# @claviron/orbit

**Connect your JavaScript application to a Claviron edge.**

Write a standard Fetch handler. Orbit connects it to Claviron through a native,
same-host transport. Your application handles requests; the edge handles public
HTTP connections and routing.

This release targets Claviron `0.2.0-alpha.24` and Orbitive `0.6.1`.

- **Node, Deno or Bun:** use the runtime you already know.
- **Standard web APIs:** work with `Request`, `Response` and streaming bodies.
- **Ready-made native bindings:** install the package and start your application.
- **More than HTTP:** exchange application events and handle WebSocket sessions.

## Get started

Install with your preferred tool:

```sh
npm install @claviron/orbit
# or: bun add @claviron/orbit
# or: deno add npm:@claviron/orbit
```

Save this as `app.mjs`:

```js
import { serve } from "@claviron/orbit";

await serve({ fleet: "apps", name: "hello" }, (request) => {
  const path = new URL(request.url).pathname;
  return new Response(`Hello from JavaScript! You requested ${path}`);
});
```

On the same host, start a compatible Claviron edge with the `apps` fleet and
configure a host or path to route to the application named `hello`. Then run:

```sh
node app.mjs
# or: bun app.mjs
# or: deno run --allow-ffi --allow-read --allow-env app.mjs
```

Requests to that edge route now reach your handler. `fleet` identifies the shared
application fleet; `name` identifies your application within it. Use matching
values in the edge configuration and JavaScript application.

A running, configured Claviron edge is required; this package provides the
application-side connection. Fetch-compatible framework handlers can be passed
to `serve()` in the same way.

## Let application instances talk

Use `join()` when you also want events or explicit control of the connection:

```js
import { join } from "@claviron/orbit";

const link = await join({ fleet: "apps", name: "hello" });

link.on("greeting", (message) => console.log(message.text));

await link.serve(() => {
  link.publish("greeting", { text: "Hello, other instances!" });
  return new Response("Greeting sent");
});
```

Run multiple instances with the same fleet and application name. A request to one
instance publishes a greeting to subscribed instances. Call `await link.close()`
when your application needs to release its connection.

## Supported platforms

Prebuilt Node-API 8 bindings are included for:

| Operating system | Architecture |
| --- | --- |
| macOS | arm64 |
| Linux with glibc | x64, arm64 |
| FreeBSD | x64, arm64 |

The package selects its native binding automatically. Windows, Intel macOS and
musl-based Linux are not included. Native OS/library compatibility still applies.

This is an early release. Deno 2.9.8 has a known native-callback stall under
sustained multi-process load; validate that runtime with your workload.

## Source and native builds

See [orbit-js on GitHub](https://github.com/claviron/orbit-js) to build native
bindings or add your own Rust functions. The npm package includes the prebuilt
bindings listed above.

MIT licensed.

### Lifecycle signals

After `stop`, `link.signal` remains aborted throughout the stopped state.
`start` after a stop and `restart` publish a new live signal before invoking
lifecycle listeners. Previously captured signals stay aborted, including those
attached to accepted requests. Repeated `stop` preserves the stopped signal;
`start` on an already running generation preserves its live signal.

### HTTP completion

`link.close()` withdraws the lane before returning and ends message sessions.
Await the promise returned by `link.serve()` (or the high-level `serve()`) before
releasing resources used by accepted HTTP responses: that promise joins their
response and cleanup tasks. Close does not abort accepted HTTP work or invent
a drain deadline. Peer cancellation and stop/restart still cancel only their
own requests. Arbitrary handler work that ignores cancellation remains the
application's responsibility.

### Numeric cells

JavaScript numeric cell inputs and outputs use `Number`; `BigInt` is not
supported for numeric cell values. Integer constructors, setters and deltas
require finite whole numbers in `[-2^63, 2^63)`, checked before mutation.
Float setters preserve fractional values and IEEE non-finite values. Rejected integer inputs change
neither the value nor its write count.

Only `Number.MIN_SAFE_INTEGER` through `Number.MAX_SAFE_INTEGER` guarantee
consecutive integer precision. Outside that range, inputs already reflect
JavaScript's rounding, and reading a Rust-created i64 can round the result.
Keep integer values and arithmetic within the safe range when exact JavaScript
round trips are required. Rust's `CellValue::Int` retains the full i64 domain.

### Cell revisions

For integer, float and boolean cells, `version()` returns a `Number` write count
and `wait(since)` accepts a whole `Number` in `[0, 2^32 - 1]` and resolves to a
`Number`. Text cells use `BigInt` for both the result and the argument: their
revision tokens are opaque unsigned 64-bit values. Pass them back unchanged;
do not convert them to `Number` or assume consecutive revisions.

```js
const since = cell.version();
const value = cell.get();
// Process value, then wait for a later write without missing an intervening one.
const next = await cell.wait(since);
```

The same sequence works with scalar and text cells without converting the token.
Text revision support requires a rebuilt addon compatible with the updated
text-cell protocol; older binaries use a different shared-memory table.
