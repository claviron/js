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
