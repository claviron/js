//! Receiver-owned admission and accepted-work drain, independent of GC.
use claviron::orbit::link::application::WorkExecution;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};

#[derive(Default)]
pub(crate) struct Active {
    count: AtomicUsize,
    empty: Notify,
}
impl Active {
    pub(crate) async fn drain(&self) {
        loop {
            let empty = self.empty.notified();
            tokio::pin!(empty);
            empty.as_mut().enable();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            empty.await;
        }
    }
}

pub(crate) struct Completion {
    released: AtomicBool,
    work: Mutex<Option<WorkExecution>>,
    ended: Option<mpsc::UnboundedSender<()>>,
    active: Arc<Active>,
}
impl Completion {
    pub(crate) fn new(
        work: Option<WorkExecution>,
        ended: Option<mpsc::UnboundedSender<()>>,
        active: Arc<Active>,
    ) -> Arc<Self> {
        active.count.fetch_add(1, Ordering::AcqRel);
        Arc::new(Self {
            released: AtomicBool::new(false),
            work: Mutex::new(work),
            ended,
            active,
        })
    }
    pub(crate) fn release(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.work.lock().unwrap_or_else(|error| error.into_inner()).take();
            if let Some(ended) = &self.ended {
                let _ = ended.send(());
            }
            if self.active.count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.active.empty.notify_waiters();
            }
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.release();
    }
}

/// Terminal owner of an HTTP output driver, including cancellation/unwind.
pub(crate) struct TerminalGuard(pub(crate) Option<Arc<Completion>>);
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if let Some(credit) = &self.0 {
            credit.release();
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/admission.rs"]
mod tests;
