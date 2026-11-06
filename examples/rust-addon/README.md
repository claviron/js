# Application-owned Rust addon

This unpublished workspace member adds `greeting` in Rust alongside Orbit's
bindings. Both are loaded from one `.node` file. It uses the local `claviron-js`
library while developing. First configure the compatible Claviron source
dependency as described in the [repository build guide](../../README.md#build-from-source).

From the repository root on macOS:

```sh
cargo build --workspace --all-features
cp target/debug/libclaviron_js_example_addon.dylib target/debug/example.node
node examples/rust-addon/check.js
```

On Linux or FreeBSD, copy `target/debug/libclaviron_js_example_addon.so` instead. A custom
addon is built on its target platform; unsupported prebuilt platforms do not
prevent the application from explicitly building its own module.

`check.js` verifies the application's Rust function and Orbit's exported classes
without joining a fleet. `app.js` serves the Rust greeting through Orbit Link:

```sh
node examples/rust-addon/app.js
```

It joins the `example` fleet as `rust-addon`. The edge must route to that name
in the same fleet. The example uses the binary explicitly through
`createOrbit(native)`; it never loads the bundled platform module.
