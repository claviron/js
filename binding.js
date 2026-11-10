// Serve a JavaScript application behind a Claviron edge over Orbit Link.
//
// The platform-selected native module takes a lane in the fleet's Link segment and
// hands each HTTP exchange the edge writes into it to this layer, which turns
// it into a standard Request and the handler's Response back into a reply.
// Deno, Node and Bun load the same module; Deno needs --allow-ffi.

import { Invocations, invokeOptions } from "./invoke.js";
export { Invocations, InvokeCall, InvokeReply } from "./invoke.js";
import { discoverFrameworks } from "./framework.js";
import { answerSession } from "./session.js";
export { Session } from "./session.js";

/** Use the application's own native module without loading a prebuilt binary. */
export function createOrbit(native) {
  if (typeof native?.join !== "function") {
    throw new TypeError("An Orbit binding must export join(options).");
  }
  return Object.freeze({
    join: (options) => joinWith(native, options),
    serve: (options, handler) => serveWith(native, options, handler),
  });
}

/**
 * Take a lane in `fleet` under `name`. An explicit framework wins; otherwise
 * the application's package.json supplies management metadata at join.
 *
 * @param {{ fleet: string, name: string, frameworks?: string[], framework?: string, icon?: string }} options
 * @returns {Promise<Link>}
 */
async function joinWith(native, options) {
  const frameworks = options.frameworks ?? (options.framework != null
    ? [options.framework] : await discoverFrameworks());
  return new Link(await native.join({ ...options, frameworks }));
}

/** This process's place in a Link fleet: requests in, events both ways. */
export class Link {
  #native;
  #listeners = new Map();
  #pumping = false;
  #controlling = false;
  #lifecycle = new Set();
  #commands = new Map();
  #shutdown = new Set();
  #generation = new AbortController();
  #sessionClosing = new AbortController();
  #sessions = new Set();
  #http = new Set();
  #invokes = new Set();
  #closing;

  constructor(link) {
    this.#native = link;
  }

  /** Open a separate invoke Link in the same JS process and native runtime. */
  async invocations(options) {
    if (this.#closing) throw new Error("Application is closing.");
    const normalized = invokeOptions(options);
    this.#control();
    const native = await this.#native.invocations(normalized);
    const child = new Invocations(native);
    this.#invokes.add(child);
    if (this.#closing) {
      await child.close();
      throw new Error("Application closed while opening invoke Link.");
    }
    return child;
  }

  /** The lane this process holds; its address, and its node in the fleet. */
  get lane() {
    return this.#native.lane;
  }

  /**
   * Answer requests with `handler` until the link closes. Exchanges run side
   * by side: a slow one does not hold the lane. Completion joins accepted
   * HTTP responses and their cleanup after intake stops.
   *
   * @param {(request: Request) => Response | Promise<Response>} handler
   * @param {{ onSession?: (session: import("./session.js").Session) => Promise<void> }} options
   */
  async serve(handler, { onSession } = {}) {
    if (onSession !== undefined && typeof onSession !== "function") {
      throw new TypeError("onSession must be a function.");
    }
    this.#control();
    try {
      for (;;) {
        const exchange = await this.#native.next();
        if (exchange === null) {
          return;
        }
        if (exchange.isSession) {
          const task = answerSession(exchange, onSession,
            [this.#generation.signal, this.#sessionClosing.signal]);
          this.#sessions.add(task);
          task.then(() => this.#sessions.delete(task), (error) => {
            this.#sessions.delete(task);
            console.error("orbit: session cleanup failed", error);
          });
        } else {
          const task = answer(exchange, handler, this.#generation.signal);
          this.#http.add(task);
          task.then(() => this.#http.delete(task), (error) => {
            this.#http.delete(task);
            console.error("orbit: HTTP cleanup failed", error);
          });
        }
      }
    } catch (error) {
      await this.close();
      throw error;
    } finally {
      await this.#endSessions();
      while (this.#http.size) await Promise.allSettled([...this.#http]);
    }
  }

  /**
   * Whether the application is running. The edge stops, starts and restarts
   * the application, not this process: while stopped, the lane stays and
   * requests are answered 503 without reaching the handler.
   */
  get running() {
    return this.#native.running;
  }

  /**
   * Aborts when the edge stops or restarts the application. Each request's
   * own `request.signal` follows it — and aborts on its own when the edge ends
   * the reply, as it does for an event stream when it shuts down — so work a request started is cancelled
   * and its reply abandoned rather than left open. Work the application keeps
   * outside requests can follow this one.
   */
  get signal() {
    return this.#generation.signal;
  }

  /**
   * Call `listener` with `"start"`, `"stop"` or `"restart"` after the edge
   * changed the application. Stop leaves `signal` aborted until start.
   * Start after stop and restart publish a new live signal before notifying
   * listeners; previously captured signals remain aborted.
   *
   * @param {(action: "start" | "stop" | "restart") => void} listener
   */
  onLifecycle(listener) {
    this.#lifecycle.add(listener);
    this.#control();
    return this;
  }

  /**
   * Call `listener` for each command named `name` the edge forwards to this
   * application, with its payload bytes.
   *
   * @param {string} name
   * @param {(payload: Uint8Array) => void} listener
   */
  onCommand(name, listener) {
    let listeners = this.#commands.get(name);
    if (!listeners) {
      listeners = new Set();
      this.#commands.set(name, listeners);
    }
    listeners.add(listener);
    this.#control();
    return this;
  }

  /**
   * Call `listener` when the edge in front of this application announces it
   * is shutting down. The Link stays; what to do about it is the
   * application's decision.
   *
   * @param {(edge: string) => void} listener
   */
  onShutdown(listener) {
    this.#shutdown.add(listener);
    this.#control();
    return this;
  }

  /**
   * Call `listener` for each application event on `topic`, from the other
   * instances of this application and from across the Link. Subscribing is
   * what tells the far side to send it.
   *
   * @param {string} topic
   * @param {(payload: unknown, event: { id: string, topic: string }) => void} listener
   */
  on(topic, listener) {
    let listeners = this.#listeners.get(topic);
    if (!listeners) {
      listeners = new Set();
      this.#listeners.set(topic, listeners);
      this.#native.subscribe([topic]);
    }
    listeners.add(listener);
    this.#pump();
    return this;
  }

  off(topic, listener) {
    const listeners = this.#listeners.get(topic);
    if (listeners?.delete(listener) && listeners.size === 0) {
      this.#listeners.delete(topic);
      this.#native.unsubscribe([topic]);
    }
    return this;
  }

  /**
   * Publish an application event to every application that subscribes to
   * `topic`, or with `target` only to that one. Returns the event id.
   *
   * @param {string} topic
   * @param {unknown} payload JSON-compatible
   * @param {{ target?: { runtime: string, name: string } }} [options]
   */
  publish(topic, payload, options = {}) {
    const target = options.target ? [options.target.runtime, options.target.name] : undefined;
    return this.#native.publish(topic, payload ?? null, target);
  }

  /**
   * The fleet's Orbit cells, shared with every instance of this application:
   * `cells.int(0)`, `float`, `bool`, `string`, or `cells.open(id)`. A cell
   * offers `get`, `set`, atomic `add` / `append`, `version` and
   * `await wait(since)`, woken by the write itself. Scalar revision tokens are
   * Numbers; text revision tokens are BigInts. Pass tokens back unchanged.
   */
  get cells() {
    return this.#native.cells;
  }

  /**
   * Withdraw the lane and end message sessions. Await the promise returned by
   * serve() to join accepted HTTP responses; close() does not abort them.
   */
  close() {
    this.#closing ??= (async () => {
      const closing = this.#native.close();
      await Promise.all([closing, this.#endSessions(), ...[...this.#invokes].map((child) => child.close())]);
    })();
    return this.#closing;
  }

  async #endSessions() {
    this.#sessionClosing.abort(new DOMException("Link closing", "AbortError"));
    while (this.#sessions.size) await Promise.allSettled([...this.#sessions]);
  }

  async #control() {
    if (this.#controlling) {
      return;
    }
    this.#controlling = true;
    for (;;) {
      const control = await this.#native.nextControl();
      if (control === null) {
        return;
      }
      switch (control.kind) {
        case "lifecycle":
          if (control.action === "stop" || control.action === "restart") {
            this.#generation.abort(
              new DOMException(`application ${control.action}`, "AbortError"),
            );
          }
          if (control.action === "restart" ||
              (control.action === "start" && this.#generation.signal.aborted)) {
            this.#generation = new AbortController();
          }
          notify(this.#lifecycle, control.action, `lifecycle '${control.action}'`);
          break;
        case "command":
          notify(
            this.#commands.get(control.name) ?? [],
            new Uint8Array(control.payload ?? []),
            `command '${control.name}'`,
          );
          break;
        case "shutdown":
          notify(this.#shutdown, control.edge, "edge shutdown");
          break;
      }
    }
  }

  async #pump() {
    if (this.#pumping) {
      return;
    }
    this.#pumping = true;
    for (;;) {
      const event = await this.#native.nextEvent();
      if (event === null) {
        return;
      }
      for (const listener of this.#listeners.get(event.topic) ?? []) {
        try {
          listener(event.payload, { id: event.id, topic: event.topic });
        } catch (error) {
          console.error(`orbit: listener for '${event.topic}' failed`, error);
        }
      }
    }
  }
}

/**
 * Serve `handler` until `options.signal` aborts, then release the lane so the
 * edge stops choosing it. Completion waits for accepted HTTP response cleanup;
 * HTTP requests already taken still finish; accepted
 * message sessions reset so reads and backpressured writes can stop.
 *
 * @param {{ fleet: string, name: string, frameworks?: string[], framework?: string, icon?: string, signal?: AbortSignal, onSession?: (session: import("./session.js").Session) => Promise<void> }} options
 * @param {(request: Request) => Response | Promise<Response>} handler
 */
async function serveWith(native, options, handler) {
  const { signal, onSession, ...where } = options;
  if (onSession !== undefined && typeof onSession !== "function") {
    throw new TypeError("onSession must be a function.");
  }
  const link = await joinWith(native, where);
  if (signal) {
    if (signal.aborted) {
      await link.close();
      return;
    }
    signal.addEventListener("abort", () => link.close(), { once: true });
  }
  await link.serve(handler, { onSession });
}

function notify(listeners, value, what) {
  for (const listener of listeners) {
    try {
      listener(value);
    } catch (error) {
      console.error(`orbit: listener for ${what} failed`, error);
    }
  }
}

// The native output owner watches reader departure even while no response data
// is produced. Generation retirement and peer departure share request cleanup.
async function answer(exchange, handler, generation) {
  const controller = new AbortController();
  const signal = controller.signal;
  const stopped = new Promise((resolve) => {
    signal.addEventListener("abort", resolve, { once: true });
  });
  const follow = () => controller.abort(generation.reason);
  if (generation.aborted) follow();
  else generation.addEventListener("abort", follow, { once: true });
  const departed = () => controller.abort(
    new DOMException("the edge ended the reply", "AbortError"));
  const watching = exchange.waitClosed().then((cancelled) => {
    if (cancelled) departed();
  }, departed);
  let claimed = false;
  let aborting;
  const abort = () => aborting ??= exchange.abort();
  try {
    if (signal.aborted) return;
    const handled = Promise.resolve().then(() => handler(request(exchange, signal)));
    // A handler that ignores its signal may settle later. Dispose of its body
    // without retaining the bridge wait or cancelling an already claimed reader.
    handled.then((late) => {
      if (signal.aborted && !claimed) late?.body?.cancel(signal.reason).catch(() => {});
    }, () => {});
    let response;
    try {
      response = await Promise.race([handled, stopped]);
    } catch (error) {
      if (!signal.aborted) {
        console.error("orbit: handler failed", error);
        response = new Response("Internal Server Error", { status: 500 });
      }
    }
    if (signal.aborted) return;
    claimed = true;
    await reply(exchange, response, controller, stopped, abort);
  } catch (error) {
    controller.abort(error);
    console.error("orbit: response failed", error);
  } finally {
    generation.removeEventListener("abort", follow);
    if (signal.aborted) await abort();
    // Normal end and abort both settle the observer; it must not outlive work.
    await watching;
  }
}

async function reply(exchange, response, controller, stopped, abort) {
  const signal = controller.signal;
  const reader = response.body?.getReader();
  let cancelling;
  const cancel = () => {
    cancelling ??= reader?.cancel(signal.reason);
    cancelling?.catch(() => {});
    abort().catch(() => {});
  };
  signal.addEventListener("abort", cancel, { once: true });
  try {
    if (signal.aborted) { cancel(); return; }
    await exchange.respond(response.status, [...response.headers]);
    if (signal.aborted) return;
    if (reader) {
      for (;;) {
        const piece = await Promise.race([reader.read(), stopped]);
        if (signal.aborted) return;
        if (piece.done) break;
        await exchange.write(piece.value);
      }
    }
    if (!signal.aborted) await exchange.end();
  } catch (error) {
    if (!signal.aborted) controller.abort(error);
  } finally {
    signal.removeEventListener("abort", cancel);
    try { await cancelling; } catch { /* Cancellation failure still releases ownership. */ }
    reader?.releaseLock();
  }
}

function request(exchange, signal) {
  const method = exchange.method;
  const init = { method, headers: exchange.headers, signal };
  if (exchange.hasBody && method !== "GET" && method !== "HEAD") {
    init.body = new ReadableStream({
      async pull(controller) {
        const piece = await exchange.read();
        if (piece === null) {
          controller.close();
        } else {
          controller.enqueue(new Uint8Array(piece));
        }
      },
    });
    init.duplex = "half";
  }
  return new Request(exchange.url, init);
}
