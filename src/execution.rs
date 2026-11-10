//! Explicit Node-API execution, selected before the native module is loaded.
//! Rust-only consumers continue to use their caller's Tokio runtime.
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use napi::bindgen_prelude::{
    AsyncRuntime, AsyncRuntimeRejection, AsyncRuntimeTask,
};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Execution {
    MultiThread,
    CurrentThread,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub execution: Execution,
    /// None uses Tokio's own worker-count declaration, including its env API.
    pub worker_threads: Option<usize>,
}
impl RuntimeOptions {
    pub fn from_values(
        execution: Option<&str>,
        threads: Option<&str>,
    ) -> Result<Self, String> {
        let execution = match execution.unwrap_or("multi_thread") {
            "current_thread" => Execution::CurrentThread,
            "multi_thread" => Execution::MultiThread,
            _ => {
                return Err(
                    "ORBIT_JS_EXECUTION is current_thread or multi_thread"
                        .into(),
                );
            }
        };
        let worker_threads = threads
            .map(|value| {
                value.parse::<usize>().map_err(|_| {
                    "ORBIT_JS_WORKER_THREADS must be a positive integer"
                        .to_owned()
                })
            })
            .transpose()?;
        if worker_threads == Some(0)
            || execution == Execution::CurrentThread
                && worker_threads.is_some_and(|count| count != 1)
        {
            return Err("current_thread has one thread; multi_thread needs a positive thread count".into());
        }
        Ok(Self { execution, worker_threads })
    }
}

struct Driver {
    handle: Handle,
    stop: CancellationToken,
    thread: std::thread::JoinHandle<()>,
}
struct State {
    options: Result<RuntimeOptions, String>,
    driver: Mutex<Option<Driver>>,
}
struct Backend(Arc<State>);
static STATE: OnceLock<Arc<State>> = OnceLock::new();
fn error(message: impl ToString) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, message.to_string())
}

/// Call from the application's `#[module_init]`, before Node-API activation.
/// No runtime or thread is started by registration itself.
pub fn register(options: RuntimeOptions) {
    register_result(Ok(options));
}

/// The ready-to-load addon selects its executor through these documented envs.
pub fn register_from_env() {
    let execution = std::env::var("ORBIT_JS_EXECUTION");
    let threads = std::env::var("ORBIT_JS_WORKER_THREADS");
    register_result(RuntimeOptions::from_values(
        execution.as_deref().ok(),
        threads.as_deref().ok(),
    ));
}
fn register_result(options: Result<RuntimeOptions, String>) {
    let state = Arc::new(State { options, driver: Mutex::new(None) });
    if STATE.set(Arc::clone(&state)).is_err() {
        // Let napi reject duplicate registration through its normal promise path.
        napi::bindgen_prelude::register_async_runtime(Backend(state));
        return;
    }
    napi::bindgen_prelude::register_async_runtime(Backend(state));
}

/// Actual runtime flavor and worker count, useful for validating deployment.
pub fn info() -> napi::Result<(String, u32)> {
    let state = STATE.get().ok_or_else(|| {
        error("this addon did not register an Orbit executor")
    })?;
    state.options.as_ref().map_err(error)?;
    let driver =
        state.driver.lock().map_err(|_| error("executor state poisoned"))?;
    let driver = driver
        .as_ref()
        .ok_or_else(|| error("Orbit executor is not running"))?;
    let flavor = match driver.handle.runtime_flavor() {
        tokio::runtime::RuntimeFlavor::CurrentThread => "current_thread",
        tokio::runtime::RuntimeFlavor::MultiThread => "multi_thread",
        _ => "unknown",
    };
    Ok((flavor.into(), driver.handle.metrics().num_workers() as u32))
}

// SAFETY: shutdown closes admission, wakes and joins the exact owned runtime
// thread. Its Runtime drop quiesces the tasks it owns. Native image retention
// additionally protects externally retained task/waker vtables after env cleanup,
// as required by napi's backend contract. No backend thread is detached.
unsafe impl AsyncRuntime for Backend {
    fn start(&self) -> napi::Result<()> {
        let options = self.0.options.as_ref().map_err(error)?;
        let mut slot = self
            .0
            .driver
            .lock()
            .map_err(|_| error("executor state poisoned"))?;
        if slot.is_some() {
            return Ok(());
        }
        napi::bindgen_prelude::retain_current_module_for_unload_safety();
        let mut builder = match options.execution {
            Execution::CurrentThread => {
                tokio::runtime::Builder::new_current_thread()
            }
            Execution::MultiThread => {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                if let Some(count) = options.worker_threads {
                    builder.worker_threads(count);
                }
                builder
            }
        };
        let runtime = builder.enable_all().build().map_err(error)?;
        let handle = runtime.handle().clone();
        let stop = CancellationToken::new();
        let ending = stop.clone();
        let thread = std::thread::Builder::new()
            .name("orbit-js-io".into())
            .spawn(move || runtime.block_on(ending.cancelled()))
            .map_err(error)?;
        *slot = Some(Driver { handle, stop, thread });
        Ok(())
    }
    fn spawn(
        &self,
        task: AsyncRuntimeTask,
    ) -> Result<(), AsyncRuntimeRejection<AsyncRuntimeTask>> {
        let slot = self
            .0
            .driver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot.as_ref() {
            Some(driver) => {
                driver.handle.spawn(task);
                Ok(())
            }
            None => Err(AsyncRuntimeRejection::new(
                task,
                error("Orbit executor is stopped"),
            )),
        }
    }
    fn block_on(
        &self,
        future: Pin<&mut dyn Future<Output = ()>>,
    ) -> napi::Result<()> {
        let handle = self
            .0
            .driver
            .lock()
            .map_err(|_| error("executor state poisoned"))?
            .as_ref()
            .map(|driver| driver.handle.clone())
            .ok_or_else(|| error("Orbit executor is stopped"))?;
        // The owned runtime thread continuously drives the current-thread
        // reactor; this borrowed future is polled only on this calling thread.
        handle.block_on(future);
        Ok(())
    }
    fn shutdown(&self) -> napi::Result<()> {
        let driver = self
            .0
            .driver
            .lock()
            .map_err(|_| error("executor state poisoned"))?
            .take();
        if let Some(driver) = driver {
            driver.stop.cancel();
            driver
                .thread
                .join()
                .map_err(|_| error("Orbit executor thread panicked"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/execution.rs"]
mod tests;
