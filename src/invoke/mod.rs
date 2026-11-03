//! Generation-owned calls on a separate Link, using Claviron's existing protocol.
use crate::{
    Error, ErrorKind, Result,
    lifecycle::{Generation, Lifecycle},
};
pub use claviron::orbit::link::invoke::Limits;
use claviron::orbit::{
    invoke::{CallError, Client, Failure},
    link::{Connection, invoke::Driver},
};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
#[cfg(feature = "napi")]
mod bindings;

pub struct Options {
    pub fleet: Box<str>,
    pub service: Box<str>,
    pub limits: Limits,
    pub routes: Vec<(Box<str>, Box<str>)>,
}
#[derive(Debug)]
pub struct Outcome {
    pub code: &'static str,
    pub payload: Vec<u8>,
}
fn outcome(value: claviron::orbit::invoke::CallResult<Vec<u8>>) -> Outcome {
    use CallError as E;
    let (code, payload) = match value {
        Ok(bytes) => ("SUCCESS", bytes),
        Err(E::Remote(Failure::Rejected(bytes))) => ("REJECTED", bytes),
        Err(error) => (
            match error {
                E::UnknownTarget => "UNKNOWN_TARGET",
                E::AmbiguousTarget => "AMBIGUOUS_TARGET",
                E::Unavailable => "UNAVAILABLE",
                E::Busy => "BUSY",
                E::Closed => "CLOSED",
                E::OutcomeUnknown => "OUTCOME_UNKNOWN",
                E::PayloadTooLarge => "PAYLOAD_TOO_LARGE",
                E::Protocol(_) => "PROTOCOL",
                E::Transport(_) => "TRANSPORT",
                E::InvalidConfig(_) => "INVALID_CONFIG",
                E::Remote(Failure::Busy) => "REMOTE_BUSY",
                E::Remote(Failure::Closing) => "REMOTE_CLOSING",
                E::Remote(Failure::Unsupported) => "UNSUPPORTED",
                E::Remote(Failure::ResponseTooLarge) => "RESPONSE_TOO_LARGE",
                E::Remote(Failure::Rejected(_)) => unreachable!(),
            },
            Vec::new(),
        ),
    };
    Outcome { code, payload }
}
fn cancelled() -> Error {
    Error::new(ErrorKind::Cancelled, "invoke cancelled")
}
fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidArg, message)
}

pub struct Invocations {
    client: Client,
    admission: Mutex<()>,
    lifecycle: Arc<Lifecycle>,
    stop: CancellationToken,
    closed: CancellationToken,
    failure: Mutex<Option<String>>,
    receive: AsyncMutex<mpsc::Receiver<Invocation>>,
    send: mpsc::Sender<Invocation>,
    host: tokio::runtime::Handle,
    outgoing: TaskTracker,
}
impl Invocations {
    pub(crate) async fn open(
        options: Options,
        lifecycle: Arc<Lifecycle>,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        use claviron::orbit::transport::TRANSPORT_LINK_SPEC;
        options
            .limits
            .validate(TRANSPORT_LINK_SPEC.inbox.payload_capacity)
            .map_err(crate::failure)?;
        if options.limits.accepted_calls > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(invalid(
                "invoke accepted_calls exceeds native queue capacity",
            ));
        }
        let connection = Connection::open(
            &options.fleet,
            TRANSPORT_LINK_SPEC,
            &options.service,
            "invoke",
            crate::INCARNATION,
        )
        .map_err(crate::failure)?;
        let (handle, driver) =
            connection.invocations(options.limits).map_err(crate::failure)?;
        let mut client = Client::new();
        for (target, service) in options.routes {
            client
                .register(
                    target.into_string(),
                    handle
                        .route(service.into_string())
                        .map_err(crate::failure)?,
                )
                .map_err(crate::failure)?;
        }
        let (send, receive) = mpsc::channel(options.limits.accepted_calls);
        let owner = Arc::new(Self {
            client,
            admission: Mutex::new(()),
            lifecycle,
            stop,
            closed: CancellationToken::new(),
            failure: Mutex::default(),
            receive: AsyncMutex::new(receive),
            send,
            host: tokio::runtime::Handle::current(),
            outgoing: TaskTracker::new(),
        });
        let (started, ready) = oneshot::channel();
        let binding = Arc::clone(&owner);
        tokio::spawn(async move {
            binding.run(driver, started).await;
        });
        ready.await.map_err(crate::failure)??;
        Ok(owner)
    }
    async fn run(
        self: Arc<Self>,
        driver: Driver,
        started: oneshot::Sender<Result<()>>,
    ) {
        let shutdown = async {
            self.stop.cancelled().await;
            let mut queue = self.receive.lock().await;
            queue.close();
            while let Ok(request) = queue.try_recv() {
                request.abort();
            }
        };
        let service = Arc::clone(&self);
        let driving = driver.run_until(
            shutdown,
            move |call| Arc::clone(&service).accept(call),
            |_| async { Ok(()) },
        );
        tokio::pin!(driving);
        let mut started = Some(started);
        let result = std::future::poll_fn(|cx| {
            let result = std::future::Future::poll(driving.as_mut(), cx);
            if let Some(started) = started.take() {
                let _ = started.send(match &result {
                    std::task::Poll::Pending => Ok(()),
                    std::task::Poll::Ready(_) => Err(crate::failure(
                        "invoke driver ended during startup",
                    )),
                });
            }
            result
        })
        .await;
        self.withdraw();
        self.outgoing.close();
        self.outgoing.wait().await;
        if let Err(error) = result {
            *self.failure.lock().expect("invoke driver failure") =
                Some(error.to_string());
        }
        self.closed.cancel();
    }
    async fn accept(
        self: Arc<Self>,
        call: claviron::orbit::link::invoke::Call,
    ) -> std::result::Result<Vec<u8>, Failure> {
        if self.stop.is_cancelled() {
            return Err(Failure::Closing);
        }
        let generation = self.lifecycle.capture().ok_or(Failure::Closing)?;
        let (sender, receiver) = oneshot::channel();
        let request = Invocation(Arc::new(Incoming {
            operation: call.request.operation.into_boxed_str(),
            payload: call.request.payload,
            generation,
            reply: Mutex::new(Some(sender)),
            terminal: watch::channel(Terminal::Open).0,
        }));
        if self.send.try_send(request.clone()).is_err() {
            return Err(Failure::Busy);
        }
        let result = tokio::select! {
            biased;
            () = request.0.generation.cancelled.cancelled() => Err(Failure::Closing),
            value = receiver => value.unwrap_or(Err(Failure::Closing)),
        };
        request.0.end(if request.0.generation.cancelled.is_cancelled() {
            Terminal::Cancelled
        } else {
            Terminal::Replied
        });
        result
    }
    pub async fn next(&self) -> Result<Option<Invocation>> {
        let mut queue = self.receive.lock().await;
        loop {
            let request = tokio::select! {
                biased;
                () = self.stop.cancelled() => return Ok(None),
                value = queue.recv() => match value {
                    Some(request) => request,
                    None => return Ok(None),
                },
            };
            if !request.cancelled() {
                return Ok(Some(request));
            }
            request.abort();
        }
    }
    /// Accept owned bytes. Cancelling a call does not roll back remote work.
    pub fn call(
        self: &Arc<Self>,
        target: Box<str>,
        operation: String,
        payload: Vec<u8>,
    ) -> Result<Arc<Call>> {
        let _admission = self.admission.lock().expect("invoke admission");
        if self.stop.is_cancelled() {
            return Err(crate::failure("invoke connection is closing"));
        }
        let generation = self
            .lifecycle
            .capture()
            .ok_or_else(|| crate::failure("application is stopped"))?;
        let (send, receive) = oneshot::channel();
        let call = Arc::new(Call {
            generation: generation.id,
            cancel: CancellationToken::new(),
            state: Mutex::new(Some(send)),
            receive: AsyncMutex::new(Some(receive)),
        });
        let caller = Arc::clone(&call);
        let binding = Arc::clone(self);
        self.outgoing.spawn_on(
            async move {
                let result = tokio::select! {
                    biased;
                    () = caller.cancel.cancelled() => Err(cancelled()),
                    () = generation.cancelled.cancelled() => Err(cancelled()),
                    value = binding.client.call(&target, operation, payload) => Ok(outcome(value)),
                };
                caller.settle(result);
            },
            &self.host,
        );
        Ok(call)
    }
    pub fn withdraw(&self) {
        let _admission = self.admission.lock().expect("invoke admission");
        self.stop.cancel();
    }
    pub async fn close(&self) -> Result<()> {
        self.withdraw();
        self.closed.cancelled().await;
        if let Some(error) =
            self.failure.lock().expect("invoke driver failure").as_ref()
        {
            return Err(crate::failure(error));
        }
        Ok(())
    }
}
type CallSender = oneshot::Sender<Result<Outcome>>;
pub struct Call {
    generation: u64,
    cancel: CancellationToken,
    state: Mutex<Option<CallSender>>,
    receive: AsyncMutex<Option<oneshot::Receiver<Result<Outcome>>>>,
}
impl Call {
    fn settle(
        &self,
        result: Result<Outcome>,
    ) {
        if let Some(sender) = self.state.lock().expect("invoke call").take() {
            let _ = sender.send(result);
        }
    }
    pub fn cancel(&self) -> bool {
        let Some(sender) = self.state.lock().expect("invoke call").take()
        else {
            return false;
        };
        self.cancel.cancel();
        let _ = sender.send(Err(cancelled()));
        true
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub async fn wait(&self) -> Result<Outcome> {
        let receive = self
            .receive
            .lock()
            .await
            .take()
            .ok_or_else(|| invalid("invoke completion already consumed"))?;
        receive.await.map_err(crate::failure)?
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Open,
    Replied,
    Cancelled,
}
type ReplySender = oneshot::Sender<std::result::Result<Vec<u8>, Failure>>;
struct Incoming {
    operation: Box<str>,
    payload: Vec<u8>,
    generation: Arc<Generation>,
    reply: Mutex<Option<ReplySender>>,
    terminal: watch::Sender<Terminal>,
}
impl Incoming {
    fn end(
        &self,
        value: Terminal,
    ) {
        self.terminal.send_if_modified(|state| {
            if *state == Terminal::Open {
                *state = value;
                true
            } else {
                false
            }
        });
    }
}
#[derive(Clone)]
pub struct Invocation(Arc<Incoming>);
impl Invocation {
    pub fn operation(&self) -> &str {
        &self.0.operation
    }
    pub fn payload(&self) -> &[u8] {
        &self.0.payload
    }
    pub fn generation(&self) -> u64 {
        self.0.generation.id
    }
    pub fn cancelled(&self) -> bool {
        self.0.generation.cancelled.is_cancelled()
            || *self.0.terminal.borrow() == Terminal::Cancelled
    }
    pub fn reply(
        &self,
        code: u32,
        payload: Vec<u8>,
    ) -> Result<()> {
        if self.cancelled() {
            return Err(cancelled());
        }
        let value = match code {
            0 => Ok(payload),
            1 => Err(Failure::Busy),
            2 => Err(Failure::Closing),
            3 => Err(Failure::Unsupported),
            4 => Err(Failure::ResponseTooLarge),
            5 => Err(Failure::Rejected(payload)),
            _ => return Err(invalid("invalid invoke reply code")),
        };
        self.0
            .reply
            .lock()
            .expect("invoke reply")
            .take()
            .ok_or_else(|| invalid("invoke already replied"))?
            .send(value)
            .map_err(|_| cancelled())?;
        self.0.end(Terminal::Replied);
        Ok(())
    }
    pub fn abort(&self) {
        self.0.end(Terminal::Cancelled);
        if let Some(sender) = self.0.reply.lock().expect("invoke reply").take()
        {
            let _ = sender.send(Err(Failure::Closing));
        }
    }
    pub async fn wait_closed(&self) -> bool {
        let mut terminal = self.0.terminal.subscribe();
        loop {
            let state = *terminal.borrow_and_update();
            match state {
                Terminal::Replied => return false,
                Terminal::Cancelled => return true,
                Terminal::Open => {}
            }
            tokio::select! {
                () = self.0.generation.cancelled.cancelled() => return true,
                _ = terminal.changed() => {}
            }
        }
    }
}
#[cfg(test)]
#[path = "../../tests/unit/invoke.rs"]
mod tests;
