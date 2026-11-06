//! An application adds its own Rust operation beside Orbit's bindings.

claviron_js::export_bindings!();

#[napi_derive::napi]
pub fn greeting(name: String) -> String {
    format!("Hello, {name}, from the application's Rust module!")
}
