// Separate invoke handlers, sharing the application's JavaScript event loop.
const empty = new Uint8Array();
function bytes(value) {
  if (!(value instanceof Uint8Array)) throw new TypeError("Invoke payload must be a Uint8Array.");
  return value;
}
function abortError(reason) {
  return reason?.name === "AbortError" ? reason : new DOMException("Invoke cancelled", "AbortError");
}
function reply(code, payload = empty) { return Object.freeze({ code, payload: bytes(payload) }); }
export const InvokeReply = Object.freeze({
  success: (payload) => reply(0, payload), busy: () => reply(1), closing: () => reply(2),
  unsupported: () => reply(3), tooLarge: () => reply(4), rejected: (payload = empty) => reply(5, payload),
});

/** Cancelling local interest is not remote cancellation or rollback. */
export class InvokeCall {
  #native;
  #completion;
  constructor(native, signal) {
    this.#native = native;
    const cancel = () => native.cancel();
    signal?.addEventListener("abort", cancel, { once: true });
    if (signal?.aborted) cancel();
    this.#completion = native.wait().catch((error) => {
      if (error?.code === "Cancelled") throw abortError(signal?.reason);
      throw error;
    }).finally(() => signal?.removeEventListener("abort", cancel));
  }
  get generation() { return this.#native.generation; }
  get completion() { return this.#completion; }
  cancel() { return this.#native.cancel(); }
}

/** One invoke connection; HTTP and invoke do not share request or response types. */
export class Invocations {
  #native;
  #active = new Set();
  #serving;
  #closing;
  #withdrawn = false;
  constructor(native) { this.#native = native; }
  call(target, operation, payload, { signal } = {}) {
    if (this.#withdrawn) throw new Error("Invoke connection is closing.");
    bytes(payload);
    if (signal?.aborted) {
      return new InvokeCall({ cancel: () => false, async wait() { throw { code: "Cancelled" }; } }, signal);
    }
    // The synchronous native submission owns bytes before this method returns.
    return new InvokeCall(this.#native.call(target, operation, payload), signal);
  }
  serve(handler) {
    if (typeof handler !== "function") throw new TypeError("Invoke handler must be a function.");
    if (this.#serving || this.#withdrawn) throw new Error("Invoke intake already started or closed.");
    this.#serving = this.#run(handler);
    return this.#serving;
  }
  async #run(handler) {
    try {
      for (;;) {
        const native = await this.#native.next();
        if (native === null) break;
        const task = answer(native, handler);
        this.#active.add(task);
        task.then(() => this.#active.delete(task), () => this.#active.delete(task));
      }
    } finally {
      this.withdraw();
      while (this.#active.size) await Promise.allSettled([...this.#active]);
      await this.#native.close();
    }
  }
  withdraw() {
    if (!this.#withdrawn) { this.#withdrawn = true; this.#native.withdraw(); }
  }
  close() {
    this.#closing ??= (async () => {
      this.withdraw();
      if (this.#serving) await this.#serving;
      else await this.#native.close();
    })();
    return this.#closing;
  }
}

async function answer(native, handler) {
  const controller = new AbortController();
  const stopped = new Promise((resolve) => controller.signal.addEventListener("abort", resolve, { once: true }));
  const watching = native.waitClosed().then((cancelled) => {
    if (cancelled) controller.abort(abortError());
  }, (error) => controller.abort(error));
  if (native.cancelled) controller.abort(abortError());
  try {
    if (controller.signal.aborted) return;
    const request = Object.freeze({
      operation: native.operation, payload: native.payload,
      generation: native.generation, signal: controller.signal,
    });
    const handled = Promise.resolve().then(() => handler(request));
    let result;
    try { result = await Promise.race([handled, stopped]); }
    catch { result = InvokeReply.rejected(); }
    if (controller.signal.aborted) return;
    if (!result || !Number.isInteger(result.code) || result.code < 0 || result.code > 5 || !(result.payload instanceof Uint8Array)) {
      result = InvokeReply.rejected();
    }
    native.reply(result.code, result.payload);
  } catch (error) {
    controller.abort(error);
  } finally {
    if (controller.signal.aborted) native.abort();
    await watching;
  }
}

export function invokeOptions(options) {
  if (!options || typeof options !== "object") throw new TypeError("Invoke options are required.");
  for (const field of ["fleet", "service"]) {
    if (typeof options[field] !== "string" || !options[field]) throw new TypeError(`Invoke ${field} is required.`);
  }
  for (const field of ["pendingCalls", "acceptedCalls", "operationBytes", "requestBytes", "responseBytes"]) {
    const minimum = ["requestBytes", "responseBytes"].includes(field) ? 0 : 1;
    if (!Number.isSafeInteger(options[field]) || options[field] < minimum || options[field] > 0xffffffff) {
      throw new TypeError(`Invoke ${field} must be an explicit bounded integer.`);
    }
  }
  if (!options.routes || typeof options.routes !== "object" || Array.isArray(options.routes)) throw new TypeError("Invoke routes must be an object.");
  const routes = Object.entries(options.routes).map(([target, service]) => {
    if (!target || typeof service !== "string" || !service) throw new TypeError("Invoke routes need nonempty target/service names.");
    return { target, service };
  });
  return { ...options, routes };
}
