//! Expand Node-API invoke classes in the final addon, beside the existing HTTP adapter.
#[macro_export]
macro_rules! export_invoke_bindings {
    () => {
        #[napi(object)]
        pub struct InvokeRoute {
            pub target: String,
            pub service: String,
        }
        #[napi(object)]
        pub struct InvokeOptions {
            pub fleet: String,
            pub service: String,
            pub pending_calls: u32,
            pub accepted_calls: u32,
            pub operation_bytes: u32,
            pub request_bytes: u32,
            pub response_bytes: u32,
            pub routes: Vec<InvokeRoute>,
        }
        #[napi(object)]
        pub struct InvokeResult {
            pub code: String,
            pub payload: Buffer,
        }
        #[napi]
        pub struct Invocations {
            inner: std::sync::Arc<orbit::invoke::Invocations>,
        }
        #[napi]
        impl Invocations {
            #[napi]
            pub fn call(
                &self,
                target: String,
                operation: String,
                payload: napi::bindgen_prelude::Uint8Array,
            ) -> Result<InvokeCall> {
                self.inner
                    .call(target.into_boxed_str(), operation, payload.to_vec())
                    .map(|inner| InvokeCall { inner })
                    .map_err(failure)
            }
            #[napi]
            pub async fn next(&self) -> Result<Option<Invocation>> {
                self.inner
                    .next()
                    .await
                    .map(|value| value.map(|inner| Invocation { inner }))
                    .map_err(failure)
            }
            #[napi]
            pub fn withdraw(&self) {
                self.inner.withdraw();
            }
            #[napi]
            pub async fn close(&self) -> Result<()> {
                self.inner.close().await.map_err(failure)
            }
        }
        #[napi]
        pub struct InvokeCall {
            inner: std::sync::Arc<orbit::invoke::Call>,
        }
        #[napi]
        impl InvokeCall {
            #[napi(getter)]
            pub fn generation(&self) -> BigInt {
                self.inner.generation().into()
            }
            #[napi]
            pub fn cancel(&self) -> bool {
                self.inner.cancel()
            }
            #[napi]
            pub async fn wait(&self) -> Result<InvokeResult> {
                self.inner
                    .wait()
                    .await
                    .map(|value| InvokeResult {
                        code: value.code.into(),
                        payload: value.payload.into(),
                    })
                    .map_err(failure)
            }
        }
        #[napi]
        pub struct Invocation {
            inner: orbit::invoke::Invocation,
        }
        #[napi]
        impl Invocation {
            #[napi(getter)]
            pub fn operation(&self) -> String {
                self.inner.operation().into()
            }
            #[napi(getter)]
            pub fn payload(&self) -> Buffer {
                self.inner.payload().into()
            }
            #[napi(getter)]
            pub fn generation(&self) -> BigInt {
                self.inner.generation().into()
            }
            #[napi(getter)]
            pub fn cancelled(&self) -> bool {
                self.inner.cancelled()
            }
            #[napi]
            pub fn reply(
                &self,
                code: u32,
                payload: napi::bindgen_prelude::Uint8Array,
            ) -> Result<()> {
                self.inner.reply(code, payload.to_vec()).map_err(failure)
            }
            #[napi]
            pub fn abort(&self) {
                self.inner.abort();
            }
            #[napi]
            pub async fn wait_closed(&self) -> bool {
                self.inner.wait_closed().await
            }
        }
    };
}
