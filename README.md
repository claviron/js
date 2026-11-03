# orbit-js

**Build a JavaScript native module with Orbit connectivity and your own Rust code.**

This is the source repository for [`@claviron/orbit`](https://www.npmjs.com/package/@claviron/orbit).
It provides a reusable Rust library, a Node-API addon, and JavaScript adapters for
connecting Node, Deno or Bun applications to a Claviron edge over same-host Orbit
Link. Claviron owns public HTTP connections and routing; the application handles
requests through standard `Request` and `Response` objects.

For installation and application examples, read the [npm package README](npm/README.md).

## Benchmarks

[Edge and Orbit application benchmarks](https://github.com/claviron/claviron/blob/main/examples/BENCHMARKS.md)
include Node.js, Deno and Bun handlers served through Claviron Edge and Orbit
Link, alongside native Rust controls and Python/PHP application paths.

The report records hardware, runtime versions, worker and lane counts,
throughput, p99 latency and errors for plain-text and JSON requests. macOS
ARM64 and Linux x86-64 campaigns are documented separately with their
configurations.

## Add your own native capabilities

You can keep Orbit's bindings and export additional Rust functions from the same
`.node` module. For example, add a parser, image processing, a computation-heavy
operation, or an adapter to another Rust library. Your functions run in the
application process and are available beside Orbit's connection API.

The [application-owned addon example](examples/rust-addon) demonstrates this:

```rust
orbit_js::export_bindings!();

#[napi_derive::napi]
pub fn greeting(name: String) -> String {
    format!("Hello, {name}, from the application's Rust module!")
}
```

The addon is a Rust `cdylib` using `orbit-js` with its `napi` feature, `napi` with
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

## Build from source

You need a compatible stable Rust toolchain, Node.js with npm, and the native
compiler/linker for your target. Node-API 8 is the addon ABI. There are no npm
build dependencies to install for the current source tree.

The Rust library uses Claviron `0.2.0-alpha.24` from crates.io with its `orbit`
and `application` features. It resolves Orbitive `0.6.1` and Continuo `0.11.0`
through Claviron; no sibling source checkouts are required.

For an application-owned addon, enable the Rust library's `napi` feature.
The addon example in this repository builds against the workspace library.

For a local addon on macOS arm64 or Linux x64 with glibc:

```sh
npm run build
# Or build a debug addon:
npm run build -- debug
```

This stages a local binary under `native/<platform>/orbit.node`. It does not
prepare portable release artifacts. The helper currently accepts those two build
hosts; the release preparation tool supports the five targets listed below.

To build and inspect the application-owned addon example:

```sh
cargo build --workspace --all-features
# macOS:
cp target/debug/liborbit_js_example_addon.dylib target/debug/example.node
# Linux or FreeBSD: copy liborbit_js_example_addon.so instead.
node examples/rust-addon/check.js
```

If you set `CARGO_TARGET_DIR`, use that output directory in the copy command and
place the result at `target/debug/example.node`, where the example loads it.
The check loads your Rust function and Orbit classes without joining a fleet.
`node examples/rust-addon/app.js` serves the greeting once an edge is configured.

Run the JavaScript tests with `npm test`. The Rust library can also be embedded
without Node-API by leaving its optional `napi` feature disabled.

## Customize and publish your own npm package

Fork this source and choose your own npm package name and version in
`package.json`. Update descriptions, ownership metadata and import examples for
your package. Edit `npm/README.md` for its users. Keep this root README focused
on source development. If you change the native addon, retain Orbit's exports
and add your application exports to the addon you build.

The default release includes these prebuilds:

| Package target | Rust target |
| --- | --- |
| `darwin-arm64` | `aarch64-apple-darwin` |
| `linux-x64-gnu` | `x86_64-unknown-linux-gnu` |
| `linux-arm64-gnu` | `aarch64-unknown-linux-gnu` |
| `freebsd-x64` | `x86_64-unknown-freebsd` |
| `freebsd-arm64` | `aarch64-unknown-freebsd` |

After finishing your source and package metadata, prepare each target on a
suitable build host:

```sh
bash scripts/publish.sh --prepare
# A cross build requires an installed target, linker and target sysroot:
bash scripts/publish.sh --prepare --target x86_64-unknown-freebsd
```

Preparation builds a release addon with a portable CPU baseline and writes
`prebuilds/<platform>/orbit.node` plus a `release.json` receipt. Collect each
matching pair into the release checkout. Keep all build hosts on identical
package metadata and release inputs. Changing those inputs invalidates existing
receipts; prepare them again. Local development binaries under `native/` are
kept separate from release artifacts.

### Build on several hosts

Copy `.hosts.example` to the ignored `.hosts` file and set your SSH aliases,
repository paths and release targets. Optional `jobs`, `nice` and `cache` fields
control each builder. SSH, rsync, Git, Node.js, Bash and the target's Rust toolchain
must already be available.
Commit and push the builder scripts and build inputs before starting a new revision.

```sh
bash scripts/build-all.sh --dry-run
bash scripts/build-all.sh
# Or select entries from .hosts:
bash scripts/build-all.sh --host linux --host freebsd
```

Selected hosts build concurrently from isolated clones of the current commit.
The worker script comes from that commit in the remote repository; missing
commits are fetched from its `origin` without switching the development checkout.
Their development checkouts are preserved. The committed release inputs must
match the local inputs; the remote worker checks this before compiling. Builds
continue in the background if an SSH connection closes, with their job/log paths
reported locally. A failed host does not stop the other selected builds.

The command collects only verified `orbit.node` and `release.json` pairs into
`prebuilds/`. Per-host logs and the combined result are under `target/build-all/`.
It prepares artifacts; publication remains the separate step below.

The scripts validate the target binary header, source digest and checksum for
all declared targets. This does not replace loading and testing each binary on
its intended operating system. If your fork supports a different target set,
update `native.js`, the explicit `package.json` file list and the release tests
together before preparing it.

Check the assembled release, then inspect the npm publication preview:

```sh
npm run check:package
bash scripts/publish.sh --dry-run
```

Authenticate with npm using an account authorized for **your package name**.
Once the preview is correct, publish explicitly:

```sh
bash scripts/publish.sh --publish
```

The script checks the prepared targets, copies only the explicit package file
list into a temporary directory, and installs `npm/README.md` there as the
package's `README.md`. It removes source-only npm scripts from that package and
cleans the temporary directory after success or failure. The repository README
is never replaced. Direct `npm pack` and `npm publish` from the source checkout
are blocked by lifecycle hooks; use the release script so the correct README
and checks are applied.

Native receipts, build scripts and local notes are excluded from the npm
package. Keep credentials and environment-specific release configuration in
ignored local files; do not place them in either public README.

MIT licensed.

## HTTP response cancellation

Each HTTP exchange observes response-reader departure even while the Fetch
handler or its response body is idle. Peer cancellation aborts `request.signal`,
cancels the response reader, and releases native stream ownership without waiting
for garbage collection. A handler that returns after cancellation has its late
response body cancelled. Application work must cooperate with its AbortSignal.
Normal request-upload EOF and successful response completion are not cancellation.
The native `waitClosed()` method reports cancellation separately from successful
completion; its observer uses the output owner's readiness registration.

This requires a rebuilt native addon matching the JavaScript adapter. Message
sessions retain their separate close and lifecycle handling. Cancellation is
observable when the edge releases the response reader; network disconnect
propagation also depends on the HTTP transport and its connection policy.

## Application-owned invoke support (unreleased)

The source adapter now provides `link.invocations(options)`, backed by Claviron's
existing invoke Client/Route/Driver. Rebuild your native addon together with the
JavaScript adapter before using it; previously prepared binaries do not expose
this API. See the [consumer API example](npm/README.md#application-owned-invokes-unreleased)
for the HTTP/invoke handlers and explicit bounds.

The invoke connection uses a separate fleet from the HTTP/event connection.
It shares the application's JavaScript process, event loop and existing native
Tokio runtime. Each connection owns exactly one reader; no extra JavaScript
engine, framework, private wire protocol or request timeout is introduced.
Generation retirement uses native cancellation wakes, so it does not depend on
a JavaScript lifecycle callback running first.

Run the wrapper regressions with `npm test`. For the real Node/Rust acceptance
harness, install the `orbit` CLI for test-segment cleanup, then run:

```sh
python3 scripts/test-invoke.py
# Reuse an already completed workspace/all-features/all-targets debug build:
python3 scripts/test-invoke.py --skip-build
```

This harness starts only its own processes, uses fresh application and invoke
fleet names, and removes their shared-memory segments after both processes
exit. Results are written under ignored `.local/`. It checks bidirectional
binary calls, typed errors, accepted-call and response bounds, HTTP progress
under invoke saturation, caller cancellation without remote rollback,
generation retirement in both directions, ignored late responses, stable
endpoints and graceful nested-call drain. It is a correctness test, not a load
test. The native acceptance run has been verified on Linux x86-64 with Node;
Deno and Bun native invoke behavior still needs runtime-specific validation.
