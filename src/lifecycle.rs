//! The application inside this process, as the edge manages it.
//!
//! The process, its lane and its Link stay up; what the edge stops, starts and
//! restarts is the application — the handler answering requests — the way a
//! Claviron runner's `ApplicationManager` stops an application without its
//! process leaving. A stopped application answers requests with 503 without
//! running the handler, and JavaScript is told so it can cancel its work.
//!
//! A command reaches one lane. The instance that receives it spreads the
//! decision to every instance of the application through the fleet's event
//! ring, as a runner's master would to its workers, so the application stops
//! as a whole rather than in one process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio_util::sync::CancellationToken;

use claviron::app::fleet::{
    FleetApplicationListSnapshot, FleetApplicationPlacement,
    FleetApplicationStatus, encode_link_snapshot_rows,
};
use claviron::app::{
    ApplicationDescription, ApplicationId, ApplicationLifecycle,
    ApplicationName, ApplicationRuntime,
};
use claviron::orbit::link::application::ApplicationSegment;
use claviron::orbit::service::{FleetMember, FleetMemberRole};
use tokio::sync::mpsc;

use crate::RUNTIME;

/// The internal topic instances of one application share lifecycle decisions
/// on, addressed to that application only.
pub(crate) const LIFECYCLE_TOPIC: &str = "claviron-js.lifecycle.v1";

/// Something the edge said, for JavaScript.
pub struct ControlEvent {
    /// `lifecycle`, `command` or `shutdown`.
    pub kind: String,
    /// For `lifecycle`: `start`, `stop` or `restart`.
    pub action: Option<String>,
    /// For `command`: the application-defined name.
    pub name: Option<String>,
    pub payload: Option<Vec<u8>>,
    /// For `shutdown`: the edge that is shutting down.
    pub edge: Option<String>,
}

pub(crate) struct Lifecycle {
    description: ApplicationDescription,
    segment: Arc<ApplicationSegment>,
    name: String,
    lane: u16,
    state: Mutex<State>,
    // Keep HTTP admission lock-free; capture() synchronizes generation ownership.
    running: AtomicBool,
    notify: mpsc::UnboundedSender<ControlEvent>,
}

impl Lifecycle {
    pub(crate) fn new(
        segment: Arc<ApplicationSegment>,
        name: &str,
        lane: u16,
        frameworks: Vec<String>,
        icon: Option<String>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<ControlEvent>) {
        let (notify, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                description: ApplicationDescription {
                    frameworks,
                    icon,
                    ..ApplicationDescription::new("javascript", "fetch")
                        .runner("claviron-js")
                },
                segment,
                name: name.to_owned(),
                lane,
                state: Mutex::new(State::initial()),
                running: AtomicBool::new(true),
                notify,
            }),
            receiver,
        )
    }

    pub(crate) fn running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn capture(&self) -> Option<Arc<Generation>> {
        let state = self.state.lock().expect("application generation");
        state.running.then(|| Arc::clone(&state.generation))
    }

    pub(crate) fn id(&self) -> Option<ApplicationId> {
        Some(ApplicationId::new(
            ApplicationRuntime::new(RUNTIME).ok()?,
            ApplicationName::new(self.name.clone()).ok()?,
        ))
    }

    /// Apply a decision every instance received. A restart, or a start after
    /// a stop, is a new generation, as a runner counts it.
    pub(crate) fn apply(
        &self,
        action: &str,
    ) {
        {
            let mut state = self.state.lock().expect("application generation");
            if !matches!(action, "start" | "stop" | "restart") {
                return;
            }
            if state.apply(action).is_err() {
                eprintln!(
                    "orbit: application generation identity exhausted"
                );
                return;
            }
            self.running.store(state.running, Ordering::Release);
        }
        let _ = self.notify.send(ControlEvent {
            kind: "lifecycle".to_owned(),
            action: Some(action.to_owned()),
            name: None,
            payload: None,
            edge: None,
        });
        self.publish_state();
    }

    pub(crate) fn command(
        &self,
        name: String,
        payload: Vec<u8>,
    ) {
        let _ = self.notify.send(ControlEvent {
            kind: "command".to_owned(),
            action: None,
            name: Some(name),
            payload: Some(payload),
            edge: None,
        });
    }

    pub(crate) fn edge_shutdown(
        &self,
        edge: String,
    ) {
        let _ = self.notify.send(ControlEvent {
            kind: "shutdown".to_owned(),
            action: None,
            name: None,
            payload: None,
            edge: Some(edge),
        });
    }

    /// Tell the edges in this fleet what this instance hosts, in the same
    /// rows a runner reports, so the linked-application view shows it.
    pub(crate) fn publish_state(&self) {
        let Some(id) = self.id() else { return };
        let now = SystemTime::now();
        let state = self.state.lock().expect("application generation");
        let running = state.running;
        let generation = state.generation.id;
        drop(state);
        let snapshot = FleetApplicationListSnapshot {
            captured_at: now,
            members_expected: 1,
            members_reporting: 1,
            applications: vec![FleetApplicationStatus {
                id,
                placements: vec![FleetApplicationPlacement {
                    description: self.description.clone(),
                    member: FleetMember {
                        node_id: self.lane,
                        role: FleetMemberRole::Standalone,
                    },
                    lifecycle: if running {
                        ApplicationLifecycle::Running
                    } else {
                        ApplicationLifecycle::Stopped
                    },
                    enabled: running,
                    generation,
                    pool: None,
                    last_error: None,
                    last_error_truncated: false,
                    observed_at: now,
                }],
            }],
        };
        let member = FleetMember {
            node_id: self.lane,
            role: FleetMemberRole::Standalone,
        };
        match encode_link_snapshot_rows(&snapshot, member) {
            Ok(rows) => {
                if let Err(error) =
                    self.segment.publish_application_state(&self.name, &rows)
                {
                    eprintln!(
                        "orbit: application state was not reported: {error}"
                    );
                }
            }
            Err(error) => {
                eprintln!(
                    "orbit: application state could not be encoded: {error}"
                )
            }
        }
    }
}

pub(crate) struct Generation {
    pub id: u64,
    pub cancelled: CancellationToken,
}
struct State {
    running: bool,
    generation: Arc<Generation>,
}
impl State {
    fn initial() -> Self {
        Self {
            running: true,
            generation: Arc::new(Generation {
                id: 1,
                cancelled: CancellationToken::new(),
            }),
        }
    }
    fn apply(
        &mut self,
        action: &str,
    ) -> std::result::Result<(), ()> {
        let next =
            if action == "restart" || (action == "start" && !self.running) {
                Some(self.generation.id.checked_add(1).ok_or(())?)
            } else {
                None
            };
        if action == "stop" || action == "restart" {
            self.generation.cancelled.cancel();
        }
        if let Some(id) = next {
            self.generation = Arc::new(Generation {
                id,
                cancelled: CancellationToken::new(),
            });
        }
        self.running = action != "stop";
        Ok(())
    }
}
#[cfg(test)]
#[path = "../tests/unit/lifecycle.rs"]
mod tests;
