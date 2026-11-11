//! Emit the adapter in the final addon, so Node-API registration belongs to it.

/// Export Orbit's Node-API surface from the application's own `cdylib`.
///
/// Requires the `napi` feature. Call once in the final addon crate; additional
/// application functions can be exported alongside it with `#[napi]`.
#[macro_export]
macro_rules! export_bindings {
    () => {
        mod orbit_bindings {
            use napi::bindgen_prelude::{
                BigInt, Buffer, Either, Either3, Either4,
            };
            use napi::{Error, Result, Status};
            use napi_derive::napi;
            use orbit::__private::{napi, napi_derive, serde_json};
            use $crate as orbit;

            fn failure(error: orbit::Error) -> Error {
                let status = match error.kind {
                    orbit::ErrorKind::GenericFailure => Status::GenericFailure,
                    orbit::ErrorKind::InvalidArg => Status::InvalidArg,
                    orbit::ErrorKind::Cancelled => Status::Cancelled,
                };
                Error::new(status, error.message)
            }

            orbit::export_invoke_bindings!();

            #[napi(object)]
            pub struct RuntimeInfo {
                pub execution: String,
                pub worker_threads: u32,
            }

            #[napi]
            pub fn runtime_info() -> Result<RuntimeInfo> {
                let (execution, worker_threads) = orbit::execution::info()?;
                Ok(RuntimeInfo { execution, worker_threads })
            }

            #[napi(object)]
            pub struct JoinOptions {
                pub fleet: String,
                pub name: String,
                pub frameworks: Vec<String>,
                pub icon: Option<String>,
                pub max_in_flight: Option<u32>,
                pub max_queued: Option<u32>,
            }

            #[napi]
            pub async fn join(options: JoinOptions) -> Result<Link> {
                orbit::join(orbit::JoinOptions {
                    fleet: options.fleet,
                    name: options.name,
                    frameworks: options.frameworks,
                    icon: options.icon,
                    max_in_flight: options
                        .max_in_flight
                        .map(|value| value as usize),
                    max_queued: options.max_queued.map(|value| value as usize),
                })
                .await
                .map(|inner| Link { inner })
                .map_err(failure)
            }

            #[napi]
            pub struct Link {
                inner: orbit::Link,
            }

            #[napi(object)]
            pub struct DeliveredEvent {
                pub id: String,
                pub topic: String,
                pub payload: serde_json::Value,
            }

            #[napi(object)]
            pub struct ControlEvent {
                pub kind: String,
                pub action: Option<String>,
                pub name: Option<String>,
                pub payload: Option<Buffer>,
                pub edge: Option<String>,
            }

            #[napi]
            impl Link {
                #[napi]
                pub async fn invocations(
                    &self,
                    options: InvokeOptions,
                ) -> Result<Invocations> {
                    self.inner
                        .invocations(orbit::invoke::Options {
                            fleet: options.fleet.into_boxed_str(),
                            service: options.service.into_boxed_str(),
                            limits: orbit::invoke::Limits {
                                pending_calls: options.pending_calls as usize,
                                accepted_calls: options.accepted_calls as usize,
                                operation_bytes: options.operation_bytes
                                    as usize,
                                request_bytes: options.request_bytes as usize,
                                response_bytes: options.response_bytes as usize,
                            },
                            routes: options
                                .routes
                                .into_iter()
                                .map(|route| {
                                    (
                                        route.target.into_boxed_str(),
                                        route.service.into_boxed_str(),
                                    )
                                })
                                .collect(),
                        })
                        .await
                        .map(|inner| Invocations { inner })
                        .map_err(failure)
                }
                #[napi(getter)]
                pub fn lane(&self) -> u32 {
                    self.inner.lane()
                }
                #[napi(getter)]
                pub fn running(&self) -> bool {
                    self.inner.running()
                }
                #[napi]
                pub async fn next(&self) -> Result<Option<Exchange>> {
                    self.inner
                        .next()
                        .await
                        .map(|value| value.map(|inner| Exchange { inner }))
                        .map_err(failure)
                }
                #[napi]
                pub async fn close(&self) {
                    self.inner.close().await;
                }
                #[napi(getter)]
                pub fn cells(&self) -> Result<Cells> {
                    self.inner
                        .cells()
                        .map(|inner| Cells { inner })
                        .map_err(failure)
                }
                #[napi]
                pub fn subscribe(
                    &self,
                    topics: Vec<String>,
                ) {
                    self.inner.subscribe(topics);
                }
                #[napi]
                pub fn unsubscribe(
                    &self,
                    topics: Vec<String>,
                ) {
                    self.inner.unsubscribe(topics);
                }
                #[napi]
                pub fn publish(
                    &self,
                    topic: String,
                    payload: serde_json::Value,
                    target: Option<Vec<String>>,
                ) -> Result<String> {
                    self.inner.publish(topic, payload, target).map_err(failure)
                }
                #[napi]
                pub async fn next_event(&self) -> Option<DeliveredEvent> {
                    self.inner.next_event().await.map(|event| DeliveredEvent {
                        id: event.id,
                        topic: event.topic,
                        payload: event.payload,
                    })
                }
                #[napi]
                pub async fn next_control(&self) -> Option<ControlEvent> {
                    self.inner.next_control().await.map(|event| ControlEvent {
                        kind: event.kind,
                        action: event.action,
                        name: event.name,
                        payload: event.payload.map(Buffer::from),
                        edge: event.edge,
                    })
                }
            }

            #[napi]
            pub struct Exchange {
                inner: orbit::Exchange,
            }

            #[napi(object)]
            pub struct SessionMessage {
                pub kind: String,
                pub text: Option<String>,
                pub data: Option<Buffer>,
                pub code: Option<u16>,
                pub reason: Option<String>,
            }

            #[napi]
            impl Exchange {
                #[napi(getter)]
                pub fn is_session(&self) -> bool {
                    self.inner.is_session()
                }
                #[napi(getter)]
                pub fn session_kind(&self) -> Option<String> {
                    self.inner.session_kind().map(str::to_owned)
                }
                #[napi(getter)]
                pub fn http_version(&self) -> String {
                    self.inner.http_version().to_owned()
                }
                #[napi(getter)]
                pub fn method(&self) -> String {
                    self.inner.method()
                }
                #[napi(getter)]
                pub fn url(&self) -> String {
                    self.inner.url()
                }
                #[napi(getter)]
                pub fn headers(&self) -> Vec<Vec<String>> {
                    self.inner.headers()
                }
                #[napi(getter)]
                pub fn has_body(&self) -> bool {
                    self.inner.has_body()
                }
                #[napi]
                pub async fn read(&self) -> Result<Option<Buffer>> {
                    self.inner
                        .read()
                        .await
                        .map(|value| value.map(Buffer::from))
                        .map_err(failure)
                }
                #[napi]
                pub async fn respond(
                    &self,
                    status: u16,
                    headers: Vec<Vec<String>>,
                ) -> Result<()> {
                    self.inner.respond(status, headers).await.map_err(failure)
                }
                #[napi]
                pub async fn write(
                    &self,
                    chunk: Buffer,
                ) -> Result<()> {
                    self.inner.write(&chunk).await.map_err(failure)
                }
                #[napi]
                pub async fn wait_closed(&self) -> Result<bool> {
                    self.inner.wait_closed().await.map_err(failure)
                }
                #[napi]
                pub async fn end(&self) -> Result<()> {
                    self.inner.end().await.map_err(failure)
                }
                #[napi]
                pub async fn abort(&self) {
                    self.inner.abort().await;
                }
                #[napi]
                pub async fn accept_session(
                    &self,
                    max_message_bytes: u32,
                    headers: Vec<Vec<String>>,
                ) -> Result<()> {
                    self.inner
                        .accept_session(max_message_bytes, headers)
                        .await
                        .map_err(failure)
                }
                #[napi]
                pub async fn reject_session(
                    &self,
                    status: u16,
                    headers: Vec<Vec<String>>,
                ) -> Result<()> {
                    self.inner
                        .reject_session(status, headers)
                        .await
                        .map_err(failure)
                }
                #[napi]
                pub async fn receive_message(
                    &self
                ) -> Result<Option<SessionMessage>> {
                    self.inner
                        .receive_message()
                        .await
                        .map(|message| {
                            message.map(|message| {
                                let mut result = SessionMessage {
                                    kind: String::new(),
                                    text: None,
                                    data: None,
                                    code: None,
                                    reason: None,
                                };
                                match message {
                                    orbit::Message::Text(text) => {
                                        result.kind = "text".into();
                                        result.text = Some(text);
                                    }
                                    orbit::Message::Binary(data) => {
                                        result.kind = "binary".into();
                                        result.data = Some(data.into());
                                    }
                                    orbit::Message::Close { code, reason } => {
                                        result.kind = "close".into();
                                        result.code = Some(code);
                                        result.reason = Some(reason);
                                    }
                                }
                                result
                            })
                        })
                        .map_err(failure)
                }
                #[napi]
                pub async fn send_text(
                    &self,
                    text: String,
                ) -> Result<()> {
                    self.inner
                        .send_message(&orbit::Message::Text(text))
                        .await
                        .map_err(failure)
                }
                #[napi]
                pub async fn send_binary(
                    &self,
                    data: Buffer,
                ) -> Result<()> {
                    self.inner
                        .send_message(&orbit::Message::Binary(data.to_vec()))
                        .await
                        .map_err(failure)
                }
                #[napi]
                pub async fn close_session(
                    &self,
                    code: u16,
                    reason: String,
                ) -> Result<()> {
                    self.inner
                        .close_session(code, &reason)
                        .await
                        .map_err(failure)
                }
            }

            #[napi]
            pub struct Cells {
                inner: orbit::Cells,
            }

            #[napi]
            impl Cells {
                #[napi]
                pub fn int(
                    &self,
                    value: Option<f64>,
                ) -> Result<Cell> {
                    let value = value
                        .map(orbit::__private::integer_number)
                        .transpose()
                        .map_err(failure)?;
                    self.inner
                        .int(value)
                        .map(|inner| Cell { inner })
                        .map_err(failure)
                }
                #[napi]
                pub fn float(
                    &self,
                    value: Option<f64>,
                ) -> Result<Cell> {
                    self.inner
                        .float(value)
                        .map(|inner| Cell { inner })
                        .map_err(failure)
                }
                #[napi]
                pub fn bool(
                    &self,
                    value: Option<bool>,
                ) -> Result<Cell> {
                    self.inner
                        .bool(value)
                        .map(|inner| Cell { inner })
                        .map_err(failure)
                }
                #[napi]
                pub fn string(
                    &self,
                    value: Option<String>,
                ) -> Result<Cell> {
                    self.inner
                        .string(value)
                        .map(|inner| Cell { inner })
                        .map_err(failure)
                }
                #[napi]
                pub fn open(
                    &self,
                    id: String,
                ) -> Result<Cell> {
                    self.inner
                        .open(id)
                        .map(|inner| Cell { inner })
                        .map_err(failure)
                }
            }

            type Loaded = Either4<i64, f64, bool, String>;
            type Input = Either3<f64, bool, String>;
            type Revision = Either<u32, BigInt>;
            type RevisionInput = Either<f64, BigInt>;

            fn revision(
                value: u64,
                text: bool,
            ) -> Result<Revision> {
                if text {
                    Ok(Either::B(BigInt::from(value)))
                } else {
                    u32::try_from(value).map(Either::A).map_err(|_| {
                        Error::new(
                            Status::InvalidArg,
                            "scalar revision exceeds u32",
                        )
                    })
                }
            }

            fn revision_input(
                value: RevisionInput,
                text: bool,
            ) -> Result<u64> {
                match (text, value) {
                    (true, Either::B(value)) => {
                        let (negative, value, lossless) = value.get_u64();
                        if !negative && lossless {
                            return Ok(value);
                        }
                    }
                    (false, Either::A(value))
                        if value.is_finite()
                            && value.fract() == 0.0
                            && value >= 0.0
                            && value <= f64::from(u32::MAX) =>
                    {
                        return Ok(value as u64);
                    }
                    _ => {}
                }
                Err(Error::new(
                    Status::InvalidArg,
                    if text {
                        "expected a text revision BigInt in the u64 range"
                    } else {
                        "expected a scalar revision Number in the u32 range"
                    },
                ))
            }

            fn loaded(value: orbit::CellValue) -> Loaded {
                match value {
                    orbit::CellValue::Int(v) => Either4::A(v),
                    orbit::CellValue::Float(v) => Either4::B(v),
                    orbit::CellValue::Bool(v) => Either4::C(v),
                    orbit::CellValue::Text(v) => Either4::D(v),
                }
            }

            #[napi]
            pub struct Cell {
                inner: orbit::Cell,
            }

            #[napi]
            impl Cell {
                #[napi(getter)]
                pub fn id(&self) -> String {
                    self.inner.id()
                }
                #[napi(getter)]
                pub fn kind(&self) -> String {
                    self.inner.kind()
                }
                #[napi]
                pub fn get(&self) -> Result<Loaded> {
                    self.inner.get().map(loaded).map_err(failure)
                }
                #[napi]
                pub fn set(
                    &self,
                    value: Input,
                ) -> Result<()> {
                    let value = match value {
                        Either3::A(v) => orbit::CellValue::Float(v),
                        Either3::B(v) => orbit::CellValue::Bool(v),
                        Either3::C(v) => orbit::CellValue::Text(v),
                    };
                    self.inner.set(value).map_err(failure)
                }
                #[napi]
                pub fn add(
                    &self,
                    by: f64,
                ) -> Result<Loaded> {
                    self.inner.add(by).map(loaded).map_err(failure)
                }
                #[napi]
                pub fn append(
                    &self,
                    text: String,
                ) -> Result<String> {
                    self.inner.append(text).map_err(failure)
                }
                #[napi]
                pub fn version(&self) -> Result<Revision> {
                    revision(
                        self.inner.version().map_err(failure)?,
                        self.inner.kind() == "string",
                    )
                }
                #[napi]
                pub async fn wait(
                    &self,
                    since: RevisionInput,
                ) -> Result<Revision> {
                    let text = self.inner.kind() == "string";
                    let since = revision_input(since, text)?;
                    revision(
                        self.inner.wait(since).await.map_err(failure)?,
                        text,
                    )
                }
                #[napi]
                pub fn release(&self) -> Result<()> {
                    self.inner.release().map_err(failure)
                }
            }
        }
    };
}
