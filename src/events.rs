//! Application events: the same `ApplicationEvent`s Claviron's PHP, JS and
//! Rust applications publish, shared with the other instances of this
//! application through the fleet's event ring and with the edge's fleet across
//! the Link.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::{Error, ErrorKind as Status, Result};
use claviron::app::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, ApplicationId, ApplicationName,
    ApplicationRuntime, EventValue, link_routing_key,
};
use claviron::orbit::link::application::EDGE_RUNTIME;
use claviron::orbit::{ApplicationEventLayout, FleetEventCodec};
use claviron::orbitive::Fleet;
use claviron::orbitive::events::FleetEventBus;
use claviron::orbitive::link::{LinkEvents, LinkSegment, Routing};
use tokio::sync::mpsc;

use crate::RUNTIME;
use crate::lifecycle::{LIFECYCLE_TOPIC, Lifecycle};

/// One event delivered to JavaScript.
pub struct DeliveredEvent {
    pub id: String,
    pub topic: String,
    pub payload: serde_json::Value,
}

/// The event side of a Link: ring, bridge, subscriptions and the queue
/// JavaScript reads.
pub(crate) struct Events {
    bus: Arc<FleetEventBus<ApplicationEventLayout>>,
    bridge: Arc<LinkEvents<ApplicationEventLayout>>,
    topics: Mutex<HashSet<String>>,
    name: String,
    lifecycle: Arc<Lifecycle>,
    queue: mpsc::UnboundedSender<DeliveredEvent>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Events {
    pub(crate) fn join(
        fleet: Arc<Fleet>,
        name: &str,
        link: &Arc<LinkSegment>,
        lane: usize,
        lifecycle: Arc<Lifecycle>,
    ) -> Result<(Arc<Self>, mpsc::UnboundedReceiver<DeliveredEvent>)> {
        let bus = Arc::new(
            FleetEventBus::<ApplicationEventLayout>::with_layout(fleet)
                .map_err(failure)?,
        );
        let bridge = Arc::new(
            LinkEvents::for_lane(
                link,
                lane,
                Arc::clone(&bus),
                |role| role == EDGE_RUNTIME,
                Routing::Key(link_routing_key),
            )
            .map_err(failure)?,
        );
        let (queue, receiver) = mpsc::unbounded_channel();
        let events = Arc::new(Self {
            bus,
            bridge,
            topics: Mutex::new(HashSet::new()),
            name: name.to_owned(),
            lifecycle,
            queue,
            tasks: Mutex::new(Vec::new()),
        });
        let driving = Arc::clone(&events.bridge);
        let forwarding = tokio::spawn(async move {
            if let Err(error) = driving.drive().await {
                eprintln!("orbit: events stopped crossing the Link: {error}");
            }
        });
        let reading = tokio::spawn(Arc::clone(&events).read_ring());
        events.tasks.lock().expect("event tasks").extend([forwarding, reading]);
        Ok((events, receiver))
    }

    /// Deliver what the other instances of this application publish. Waits
    /// on the ring's own readiness, never on an interval.
    async fn read_ring(self: Arc<Self>) {
        let ready = match self.bus.event_fd() {
            // SAFETY: RingEventFd owns its readiness descriptor, keeps its
            // identity fixed, and is moved into AsyncFd until deregistration.
            Ok(fd) => match unsafe { tokio::io::unix::AsyncFd::register(fd) } {
                Ok(ready) => ready,
                Err(error) => {
                    eprintln!(
                        "orbit: the event ring cannot wake this process: {error}"
                    );
                    return;
                }
            },
            Err(error) => {
                eprintln!(
                    "orbit: the event ring cannot wake this process: {error}"
                );
                return;
            }
        };
        let own = self.bus.node_id().get();
        let mut cursor = self.bus.cursor_at_head();
        loop {
            for event in self.bus.poll(&mut cursor).events {
                // This process delivered its own events when it published
                // or accepted them.
                if event.id.node() != own
                    && event.topic == APPLICATION_EVENT_TOPIC
                {
                    self.deliver(&event.payload);
                }
            }
            let Ok(mut guard) = ready.readable().await else { return };
            if ready.get_ref().drain().is_err() {
                return;
            }
            guard.clear_ready();
        }
    }

    /// An event frame read from this process's lane: into the fleet's ring
    /// for the other instances, and to this process.
    pub(crate) fn accept(
        &self,
        frame: &[u8],
    ) {
        match self.bridge.accept(frame) {
            Ok((topic, payload)) if topic == APPLICATION_EVENT_TOPIC => {
                self.deliver(payload);
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("orbit: an event frame was refused: {error}")
            }
        }
    }

    pub(crate) fn subscribe(
        &self,
        topics: Vec<String>,
    ) {
        let mut held = self.topics.lock().expect("event topics");
        held.extend(topics);
        let keys: Vec<&String> = held.iter().collect();
        self.bridge.interest(&keys);
    }

    pub(crate) fn unsubscribe(
        &self,
        topics: Vec<String>,
    ) {
        let mut held = self.topics.lock().expect("event topics");
        for topic in &topics {
            held.remove(topic);
        }
        let keys: Vec<&String> = held.iter().collect();
        self.bridge.interest(&keys);
    }

    pub(crate) fn publish(
        &self,
        topic: String,
        payload: &serde_json::Value,
        target: Option<(String, String)>,
    ) -> Result<String> {
        let value = EventValue::from_json(payload);
        let event = match target {
            Some((runtime, name)) => ApplicationEvent::for_application(
                ApplicationId::new(
                    ApplicationRuntime::new(runtime).map_err(failure)?,
                    ApplicationName::new(name).map_err(failure)?,
                ),
                topic,
                value,
            ),
            None => ApplicationEvent::new(topic, value),
        }
        .map_err(failure)?;
        let encoded = event.encode_orbit();
        self.bus.publish(APPLICATION_EVENT_TOPIC, &encoded).map_err(failure)?;
        self.deliver(&encoded);
        Ok(event.id().to_string())
    }

    /// Spread a lifecycle decision to every instance of this application,
    /// this one included.
    pub(crate) fn publish_lifecycle(
        &self,
        action: &str,
    ) -> Result<()> {
        let Some(id) = self.lifecycle.id() else {
            return Err(failure("this application has no valid identity"));
        };
        let event = ApplicationEvent::for_application(
            id,
            LIFECYCLE_TOPIC,
            EventValue::String(action.as_bytes().to_vec()),
        )
        .map_err(failure)?;
        self.bus
            .publish(APPLICATION_EVENT_TOPIC, &event.encode_orbit())
            .map_err(failure)?;
        self.lifecycle.apply(action);
        Ok(())
    }

    /// Hand a decoded event to JavaScript if this process subscribed to its
    /// topic and it is addressed to everyone or to this application.
    fn deliver(
        &self,
        payload: &[u8],
    ) {
        let Ok(event) = ApplicationEvent::decode_orbit(payload) else { return };
        if event.topic().as_str() == LIFECYCLE_TOPIC {
            let ours = event.target().is_some_and(|target| {
                target.runtime().as_str() == RUNTIME
                    && target.name().as_str() == self.name
            });
            if ours && let EventValue::String(action) = event.payload() {
                self.lifecycle.apply(&String::from_utf8_lossy(action));
            }
            return;
        }
        if !self
            .topics
            .lock()
            .expect("event topics")
            .contains(event.topic().as_str())
        {
            return;
        }
        if let Some(target) = event.target()
            && (target.runtime().as_str() != RUNTIME
                || target.name().as_str() != self.name)
        {
            return;
        }
        let Ok(payload) = event.payload().to_json() else { return };
        let _ = self.queue.send(DeliveredEvent {
            id: event.id().to_string(),
            topic: event.topic().as_str().to_owned(),
            payload,
        });
    }

    pub(crate) fn flush(&self) {
        if let Err(error) = self.bridge.pump() {
            eprintln!(
                "orbit: pending events could not cross the Link: {error}"
            );
        }
    }

    pub(crate) fn stop(&self) {
        for task in self.tasks.lock().expect("event tasks").drain(..) {
            task.abort();
        }
    }
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}
