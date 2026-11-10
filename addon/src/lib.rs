//! Ready-to-load Node-API module using the shared Orbit library.

claviron_js::export_bindings!();

#[napi_derive::module_init]
fn initialize_runtime() {
    claviron_js::execution::register_from_env();
}
