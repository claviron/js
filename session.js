// One Link message session. Edge owns WebSocket/QUIC, this owns application IO.
export class Session {
  #native;
  #headers;
  #controller = new AbortController();
  #state = "pending";
  #ending;
  #parents = [];
  #closeCode;
  #closeReason;

  /** @internal Created by Link's onSession dispatcher. */
  constructor(exchange, signals = []) {
    this.#native = exchange;
    this.#headers = new Headers(exchange.headers);
    for (const signal of signals) {
      const stop = () => {
        // The dispatcher joins this same cleanup promise in its finally block.
        this.shutdown().catch(() => {});
      };
      this.#parents.push([signal, stop]);
      signal.addEventListener("abort", stop, { once: true });
    }
    if (signals.some((signal) => signal.aborted)) this.shutdown().catch(() => {});
  }

  get url() { return this.#native.url; }
  get method() { return this.#native.method; }
  get headers() { return this.#headers; }
  get httpVersion() { return this.#native.httpVersion; }
  get transport() { return this.#native.sessionKind; }
  get signal() { return this.#controller.signal; }
  get accepted() { return this.#state === "accepted"; }
  get closed() { return this.#state === "closed" || this.#state === "rejected"; }
  get closeCode() { return this.#closeCode; }
  get closeReason() { return this.#closeReason; }

  /** Explicit text/binary payload bound, in UTF-8 bytes for text. */
  async accept({ maxMessageBytes, headers = [] } = {}) {
    if (this.#state !== "pending") throw new Error("Session is already answered or stopping.");
    if (!Number.isInteger(maxMessageBytes) || maxMessageBytes <= 0 || maxMessageBytes > 0xffffffff) {
      throw new RangeError("maxMessageBytes must be a positive u32 byte count.");
    }
    const pairs = [...new Headers(headers)];
    this.#state = "accepting";
    try {
      await this.#native.acceptSession(maxMessageBytes, pairs);
      if (this.#state !== "accepting") {
        await this.#native.abort();
        throw this.signal.reason;
      }
      this.#state = "accepted";
    } catch (error) {
      await this.abort(error);
      throw error;
    }
  }

  reject(status, headers = []) {
    if (this.#state !== "pending") throw new Error("Session is already answered or stopping.");
    if (!Number.isInteger(status) || status < 300 || status > 599) {
      throw new RangeError("Session rejection requires an HTTP error or redirect status.");
    }
    const pairs = [...new Headers(headers)];
    this.#state = "rejecting";
    this.#stop("Session rejected");
    this.#ending = (async () => {
      try { await this.#native.rejectSession(status, pairs); }
      catch (error) { await this.#native.abort(); throw error; }
      finally { this.#state = "rejected"; this.#detach(); }
    })();
    return this.#ending;
  }

  /** A text string, a Uint8Array, or null on input end/close. One reader. */
  async receive() {
    if (this.closed || this.#state === "closing") return null;
    this.#requireAccepted();
    try {
      const message = await this.#native.receiveMessage();
      if (message === null) return null;
      if (message.kind === "text") return message.text;
      if (message.kind === "binary") return message.data;
      if (message.kind === "close") {
        this.#closeCode = message.code;
        this.#closeReason = message.reason;
        this.#state = "closed";
        this.#stop("Peer closed the session");
        this.#detach();
        return null;
      }
      throw new Error("Unknown native session message.");
    } catch (error) {
      if (this.closed || this.#state === "closing") return null;
      await this.abort(error);
      throw error;
    }
  }

  /** Await each send: native IO rejects concurrent sends instead of queueing. */
  async send(data) {
    this.#requireAccepted();
    if (typeof data === "string") return this.#native.sendText(data);
    if (ArrayBuffer.isView(data)) {
      return this.#native.sendBinary(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    }
    if (data instanceof ArrayBuffer) return this.#native.sendBinary(new Uint8Array(data));
    throw new TypeError("Session messages must be text or binary data.");
  }

  /** Send a protocol close, finish the current write, then release native IO. */
  close(code = 1000, reason = "") {
    if (this.#ending) return this.#ending;
    if (this.closed) return Promise.resolve();
    this.#requireAccepted();
    if (!Number.isInteger(code) || !((code >= 1000 && code <= 1003) ||
      (code >= 1007 && code <= 1014) || (code >= 3000 && code <= 4999))) {
      throw new RangeError("Invalid WebSocket close code.");
    }
    if (typeof reason !== "string" || new TextEncoder().encode(reason).byteLength > 123) {
      throw new RangeError("Close reason exceeds 123 UTF-8 bytes.");
    }
    this.#state = "closing";
    this.#closeCode = code;
    this.#closeReason = reason;
    this.#stop("Session closing");
    this.#ending = (async () => {
      try { await this.#native.closeSession(code, reason); }
      catch (error) { await this.#native.abort(); throw error; }
      finally { this.#state = "closed"; this.#detach(); }
    })();
    return this.#ending;
  }

  /** Reset this session only, waking native reads and backpressured writes. */
  abort(reason = new DOMException("Session abandoned", "AbortError")) {
    if (this.closed) return this.#ending ?? Promise.resolve();
    const ending = this.#ending;
    this.#state = "closed";
    this.#controller.abort(reason);
    this.#detach();
    this.#ending = (async () => {
      await this.#native.abort();
      // A reset also releases a graceful close parked behind backpressure.
      if (ending) await ending.catch(() => {});
    })();
    return this.#ending;
  }

  /** @internal Listener/application retirement, separate from HTTP requests. */
  shutdown() {
    if (this.#state === "rejecting") return this.#ending;
    if (this.closed) return this.#ending ?? Promise.resolve();
    if (this.#state === "pending") return this.reject(503);
    return this.abort();
  }

  /** @internal A handler owns the session until it returns. */
  finish() {
    if (this.#ending) return this.#ending;
    if (this.closed) return Promise.resolve();
    if (this.#state === "pending") return this.reject(501);
    if (this.#state === "accepted") return this.close();
    return this.abort();
  }

  async *[Symbol.asyncIterator]() {
    for (;;) {
      const message = await this.receive();
      if (message === null) return;
      yield message;
    }
  }

  #requireAccepted() {
    if (this.#state !== "accepted") throw new Error("Session is not accepted or is stopping.");
  }
  #stop(message) { this.#controller.abort(new DOMException(message, "AbortError")); }
  #detach() {
    for (const [signal, stop] of this.#parents) signal.removeEventListener("abort", stop);
    this.#parents = [];
  }
}

/** @internal Dispatch separately: CONNECT is not a Fetch Request method. */
export async function answerSession(exchange, handler, signals) {
  const session = new Session(exchange, signals);
  const stopped = Promise.withResolvers();
  const stop = () => stopped.resolve();
  session.signal.addEventListener("abort", stop, { once: true });
  try {
    if (session.signal.aborted) return;
    if (!handler) { await session.reject(501); return; }
    const handled = Promise.resolve().then(() => handler(session));
    await Promise.race([handled, stopped.promise]);
  } catch (error) {
    if (!session.signal.aborted) {
      console.error("orbit: session handler failed", error);
      if (session.accepted) await session.abort(error);
      else await session.reject(500);
    }
  } finally {
    session.signal.removeEventListener("abort", stop);
    try { await session.finish(); }
    catch (error) {
      if (!signals.some((signal) => signal.aborted)) throw error;
    }
  }
}
