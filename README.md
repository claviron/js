# claviron/js

A JavaScript library for handling Claviron application requests over same-host
Orbit Link. Your normal Node, Deno or Bun application imports the library and
supplies a standard Fetch handler. Claviron owns public connections and host
routing; your application owns its handlers and process lifetime.

## Install

```sh
npm install @claviron/orbit
# or: bun add @claviron/orbit
# or: deno add npm:@claviron/orbit
```

Native bindings are included for macOS arm64, Linux with glibc on x64 and arm64,
and FreeBSD on x64 and arm64. The package selects the right one automatically.

## Use from a JavaScript application

```js
import { serve } from "@claviron/orbit";

await serve({ fleet: "apps", name: "hello" }, () => {
  return new Response("Hello from JavaScript");
});
```

```sh
node app.mjs
# or: bun app.mjs
# or: deno run --allow-ffi --allow-read --allow-env app.mjs
```

`fleet` and `name` identify the application; a Claviron edge on the same host
routes to them. See the [package guide](npm/README.md) for events, lifecycle,
cells and invokes.

## Native execution and admission

These APIs require a native build from the current source; they have not yet
been published in an npm release.

The ready-to-load addon selects its native I/O executor before it is imported:

```sh
ORBIT_JS_EXECUTION=current_thread node app.mjs
# Or select a multi-thread executor explicitly:
ORBIT_JS_EXECUTION=multi_thread ORBIT_JS_WORKER_THREADS=4 node app.mjs
```

`current_thread` continuously drives one real Tokio current-thread runtime on
an owned native thread. It does not change the JavaScript engine's scheduling.
The default is `multi_thread`, using Tokio's worker-count configuration when
`ORBIT_JS_WORKER_THREADS` is omitted. Current-thread mode permits only one
worker. Invalid values reject native runtime initialization and are reported by
`runtimeInfo()` or the first asynchronous native call. `runtimeInfo()` returns the
actual native runtime flavor and worker count.

Applications can declare receiver admission separately from execution:

```js
import { join, runtimeInfo } from "@claviron/orbit";

console.log(runtimeInfo());
const link = await join({
  fleet: "apps", name: "hello",
  maxInFlight: 128, maxWaiting: 192,
});
await link.serve(() => new Response("Hello"));
```

Both limits must be supplied together as positive integers.
`maxInFlight` limits running exchanges; `maxWaiting` includes running and queued
exchanges and must be at least `maxInFlight`. Values must fit the native Link
geometry. Omitting both preserves unbounded admission. Queue promotion follows
completion wakes; queued requests do not block event or lifecycle intake.
Credits return when native response I/O finishes or is cancelled, even while a
JavaScript wrapper remains reachable. A caller cancelling before stream
acceptance ends that exchange without terminating the application.

`link.close()` withdraws intake and waits for accepted native I/O to finish;
`link.serve()` also joins the JavaScript handlers. Neither imposes an implicit
drain timeout. Message sessions are aborted on Link close. Process counts and
worker growth belong to the external controller, outside this binding API.

An application-owned addon may opt into the same executor by calling
`claviron_js::execution::register(RuntimeOptions { .. })` from its
`#[napi_derive::module_init]` function before Node-API activates. Otherwise it
continues to use its configured Node-API runtime. Rust-only consumers use their
caller's Tokio runtime.

## Add your own native capabilities

You can keep Orbit's bindings and export additional Rust functions from the same
`.node` module. For example, add a parser, image processing, a computation-heavy
operation, or an adapter to another Rust library. Your functions run in the
application process and are available beside Orbit's connection API.

The [application-owned addon example](examples/rust-addon) demonstrates this:

```rust
claviron_js::export_bindings!();

#[napi_derive::napi]
pub fn greeting(name: String) -> String {
    format!("Hello, {name}, from the application's Rust module!")
}
```

The addon is a Rust `cdylib` using `claviron-js` with its `napi` feature, `napi` with
`napi8`, and `napi-derive`. Its build script calls `napi_build::setup()` through
the `napi-build` build dependency. See the example's [Cargo manifest](examples/rust-addon/Cargo.toml)
and [build script](examples/rust-addon/build.rs) for the complete setup.

Load that module explicitly and pass it to the JavaScript adapter:

```js
import { createRequire } from "node:module";
import { createOrbit } from "@claviron/orbit/binding";

const require = createRequire(import.meta.url);
const native = require("./my-addon.node");
const orbit = createOrbit(native);

await orbit.serve({ fleet: "apps", name: "hello" }, () => {
  return new Response(native.greeting("JavaScript"));
});
```

The `/binding` entry point uses your supplied addon without loading a bundled
prebuilt binary. Keep `export_bindings!()` in your addon to provide the native
interface that `createOrbit()` expects. A compatible edge must route to the same
fleet and application name on the same host.

Orbit also exposes application events, shared cells, lifecycle notifications,
message sessions and application-owned invokes. Additional native exports can
implement application-specific work; changing the Orbit transport itself also requires compatibility with the
edge. Long-running native work should use an appropriate asynchronous Node-API
interface so it does not block JavaScript execution.

## HTTP response cancellation

Each HTTP exchange observes response-reader departure even while the Fetch
handler or its response body is idle. Peer cancellation aborts `request.signal`,
cancels the response reader, and releases native stream ownership without waiting
for garbage collection. A handler that returns after cancellation has its late
response body cancelled. Application work must cooperate with its AbortSignal.
Normal request-upload EOF and successful response completion are not cancellation.
The native `waitClosed()` method reports cancellation separately from successful
completion; its observer uses the output owner's readiness registration.

Response heads follow native producer backpressure. The Fetch adapter awaits
publication before body writes; low-level native callers must await `respond()`
as well. Intake and application events continue while a response waits.

Message sessions retain their separate close and lifecycle handling. Cancellation
is observable when the edge releases the response reader; network disconnect
propagation also depends on the HTTP transport and its connection policy.

## Application-owned invokes

`link.invocations(options)` opens invokes on its own Link. See the
[package guide](npm/README.md#application-owned-invokes) for the handlers and
explicit bounds.

The invoke connection uses a separate fleet from the HTTP/event connection.
It shares the application's JavaScript process, event loop and existing native
Tokio runtime. Each connection owns exactly one reader; no extra JavaScript
engine, framework, private wire protocol or request timeout is introduced.
Generation retirement uses native cancellation wakes, so it does not depend on
a JavaScript lifecycle callback running first.

Native invoke correctness has been verified on Linux x86-64 with Node and Deno.
Bun native invoke behavior has not been validated.

## Benchmarks

[Edge and Orbit application benchmarks](https://github.com/claviron/claviron/blob/main/examples/BENCHMARKS.md)
include Node.js, Deno and Bun handlers served through Claviron Edge and Orbit
Link, alongside native Rust controls and Python/PHP application paths.

The report records hardware, runtime versions, worker and lane counts,
throughput, p99 latency and errors for plain-text and JSON requests. macOS
ARM64 and Linux x86-64 campaigns are documented separately with their
configurations.

MIT licensed.
