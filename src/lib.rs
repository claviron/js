//! Serve a JavaScript application behind a Claviron edge over Orbit Link.
//!
//! The process takes a lane in its fleet's Link segment under a name, the way
//! a Claviron runner does, and the edge writes HTTP exchanges into it. This
//! library reads them on its caller's Tokio runtime — the lane's bell wakes the
//! reactor, no thread is parked and nothing polls — and hands each to
//! JavaScript as a promise. The application answers with a status, headers
//! and a body; the head goes back to the edge worker that asked, the body
//! through the exchange's stream slot.
//!
//! The optional `napi` adapter runs on the addon's Node-API Tokio runtime.
//! It translates JavaScript values without owning a second transport engine.

use std::sync::{Arc, Mutex};

mod admission;
mod cells;
mod error;
mod events;
mod http;
pub mod invoke;
mod lifecycle;
mod session;
use ErrorKind as Status;
pub use cells::{Cell, CellValue, Cells};
pub use claviron::orbit::link::application::Message;
pub use error::{Error, ErrorKind, Result};
pub use events::DeliveredEvent;
pub use lifecycle::ControlEvent;

#[cfg(feature = "napi")]
mod bindings;

#[cfg(feature = "napi")]
pub mod execution;

#[cfg(feature = "napi")]
#[doc(hidden)]
pub mod __private {
    pub use crate::cells::integer_number;
    pub use napi;
    pub use napi_derive;
    pub use serde_json;
}

use claviron::orbit::link::application::{
    ApplicationBodies, ApplicationReceiver, ApplicationSegment, ControlCommand,
    Exchange as ExchangeFrame, ExchangeReply, FrameKind, Offered, Received,
    ShutdownNotice, Ticket, WorkQueue, frame_kind,
};
use claviron::orbitive::shm::LaneHold;
use claviron::orbitive::stream::{ReadHalf, WriteHalf};
use claviron::orbit::link::application::body::ResponseWriter;
use claviron::orbitive::{Fleet, NodeId};
use tokio::io::AsyncWriteExt;

/// What a lane says serves its name, so the edge's directory reads `js`.
pub(crate) const RUNTIME: &str = "js";

/// A process's first life; a supervisor would count restarts, and there is
/// none here.
const INCARNATION: u64 = 1;

/// How much one `read()` takes from a request body at most. The slot's own
/// buffer bounds what is there to take.
const READ_CHUNK: usize = 64 * 1024;

#[derive(Default)]
pub struct JoinOptions {
    /// The Link fleet the edge routes to.
    pub fleet: String,
    /// The name this process serves in that fleet.
    pub name: String,
    /// Prepared framework tags, independent of the JavaScript engine.
    pub frameworks: Vec<String>,
    /// Symbolic admin-client icon code; unknown codes fall back in the client.
    pub icon: Option<String>,
    /// Running exchanges. None with no max_queued preserves unbounded admission.
    pub max_in_flight: Option<usize>,
    /// Waiting exchanges, excluding running work; required with max_in_flight.
    pub max_queued: Option<usize>,
}

/// This process's place in a Link fleet.
pub struct Link {
    segment: Arc<ApplicationSegment>,
    fleet: Box<str>,
    shutdown: tokio_util::sync::CancellationToken,
    invokes: tokio::sync::Mutex<Vec<Arc<invoke::Invocations>>>,
    bodies: Arc<ApplicationBodies>,
    lane: u32,
    hold: Mutex<Option<LaneHold>>,
    receiver: tokio::sync::Mutex<Option<Intake>>,
    /// Set by `close()`. A `next()` parked on the lane watches it too, so a
    /// close ends that wait at once instead of waiting for a frame.
    closed: tokio::sync::watch::Sender<bool>,
    events: Arc<events::Events>,
    lifecycle: Arc<lifecycle::Lifecycle>,
    controls: tokio::sync::Mutex<
        tokio::sync::mpsc::UnboundedReceiver<lifecycle::ControlEvent>,
    >,
    name: String,
    cells: Option<cells::Cells>,
    cells_unavailable: Option<String>,
    delivered: tokio::sync::Mutex<
        tokio::sync::mpsc::UnboundedReceiver<events::DeliveredEvent>,
    >,
    completed: tokio::sync::mpsc::UnboundedSender<()>,
    queued: bool,
    active: Arc<admission::Active>,
}

struct Intake {
    receiver: ApplicationReceiver,
    queue: WorkQueue,
    finished: tokio::sync::mpsc::UnboundedReceiver<()>,
    ready:
        std::collections::VecDeque<claviron::orbit::link::application::Start>,
}

/// Take a lane in `fleet` under `name` and start reading it.
pub async fn join(options: JoinOptions) -> Result<Link> {
    let window = match (options.max_in_flight, options.max_queued) {
        (None, None) => None,
        (Some(in_flight), Some(queued)) => {
            let window = in_flight.checked_add(queued).filter(|window| {
                in_flight > 0
                    && *window < claviron::orbit::link::application::LINK_INBOX_CAPACITY
            });
            Some(window.ok_or_else(|| Error::new(
                Status::InvalidArg,
                "maxInFlight must be positive and maxInFlight + maxQueued must be below inbox capacity",
            ))?)
        }
        _ => {
            return Err(Error::new(
                Status::InvalidArg,
                "maxInFlight and maxQueued must be declared together.",
            ));
        }
    };
    let segment = Arc::new(
        ApplicationSegment::for_fleet(&options.fleet).map_err(failure)?,
    );
    let joined = segment
        .join_with_capacity(&options.name, RUNTIME, INCARNATION, window)
        .map_err(failure)?;
    if let Some(limit) = options.max_in_flight {
        if let Err(error) = segment.set_work_execution(joined.lane, limit) {
            let _ = segment.inbox().release_lane(joined.lane);
            return Err(failure(error));
        }
    }
    let bodies = (|| {
        let mut bodies = ApplicationBodies::for_segment(&segment, joined.lane, INCARNATION)?;
        bodies.prepare_http(&segment, joined.lane, INCARNATION)?;
        Ok::<_, claviron::orbit::link::application::Error>(Arc::new(bodies))
    })().map_err(|error| {
        let _ = segment.inbox().release_lane(joined.lane);
        failure(error)
    })?;
    bodies.reclaimed(&joined.reclaimed);
    let receiver =
        segment.application_receiver(joined.lane).map_err(failure)?;
    let (completed, finished) = tokio::sync::mpsc::unbounded_channel();
    let queued = options.max_queued.is_some_and(|limit| limit > 0);
    let receiver = Intake {
        receiver,
        queue: WorkQueue::new(),
        finished,
        ready: Default::default(),
    };
    // This process's lane index is its node in the fleet: a lane is held by
    // one live process, so the index is unique while it lives — what the
    // Link's "forwarded once, at its origin" rule and per-node rings need. A
    // fleet shared with a supervised Claviron runner would number its nodes
    // differently; an claviron-js application keeps a fleet of its own.
    let node = u16::try_from(joined.lane)
        .map_err(|_| failure("the lane does not fit a fleet node"))?;
    let capacity = segment.spec().fleet_capacity;
    let fleet = Arc::new(
        Fleet::join_shm_as(&options.fleet, capacity, NodeId::new(node))
            .map_err(failure)?,
    );
    let (cells, cells_unavailable) =
        match cells::Cells::attach(Arc::clone(&fleet)) {
            Ok(cells) => (Some(cells), None),
            Err(error) => (None, Some(error.to_string())),
        };
    let (lifecycle, controls) = lifecycle::Lifecycle::new(
        Arc::clone(&segment),
        &options.name,
        node,
        options.frameworks,
        options.icon,
    );
    let (events, delivered) = events::Events::join(
        fleet,
        &options.name,
        segment.link(),
        joined.lane,
        Arc::clone(&lifecycle),
    )?;
    Ok(Link {
        segment,
        fleet: options.fleet.into_boxed_str(),
        shutdown: tokio_util::sync::CancellationToken::new(),
        invokes: tokio::sync::Mutex::default(),
        bodies,
        lane: joined.lane as u32,
        hold: Mutex::new(Some(joined.hold)),
        receiver: tokio::sync::Mutex::new(Some(receiver)),
        closed: tokio::sync::watch::channel(false).0,
        events,
        lifecycle,
        controls: tokio::sync::Mutex::new(controls),
        name: options.name.clone(),
        cells,
        cells_unavailable,
        delivered: tokio::sync::Mutex::new(delivered),
        completed,
        queued,
        active: Arc::new(admission::Active::default()),
    })
}

impl Link {
    /// Open an application-owned invoke service on an isolated Link.
    pub async fn invocations(
        &self,
        options: invoke::Options,
    ) -> Result<Arc<invoke::Invocations>> {
        // Opening and parent drain share ownership even if shutdown races startup.
        let mut children = self.invokes.lock().await;
        if self.shutdown.is_cancelled() {
            return Err(failure("application is closing"));
        }
        if options.fleet.as_ref() == self.fleet.as_ref() {
            return Err(Error::new(
                Status::InvalidArg,
                "invoke fleet must differ from the HTTP/event fleet",
            ));
        }
        let child = invoke::Invocations::open(
            options,
            Arc::clone(&self.lifecycle),
            self.shutdown.child_token(),
        )
        .await?;
        children.push(Arc::clone(&child));
        if self.shutdown.is_cancelled() {
            return Err(failure(
                "application closed while opening invoke Link",
            ));
        }
        Ok(child)
    }

    /// The lane this process took; its address in the fleet.
    pub fn lane(&self) -> u32 {
        self.lane
    }

    /// The next runnable request. Queued work never blocks event/control intake.
    pub async fn next(&self) -> Result<Option<Exchange>> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return Ok(None);
        }
        let mut held = self.receiver.lock().await;
        let mut frame = Vec::new();
        loop {
            let Some(intake) = held.as_mut() else { return Ok(None) };
            let ready = if let Some(ready) = intake.ready.pop_front() {
                Some(ready)
            } else {
                let received = tokio::select! {
                    biased;
                    _ = closed.changed() => return Ok(None),
                    completed = intake.finished.recv(), if self.queued => {
                        if completed.is_some() {
                            if let Some(start) = intake.queue.finished(&self.segment, self.lane as usize) { intake.ready.push_back(start); }
                        }
                        continue;
                    }
                    received = intake.receiver.next(&mut frame) => received.map_err(failure)?,
                };
                match received {
                    Received::Event => {
                        self.events.accept(&frame);
                        continue;
                    }
                    Received::Directory(_) | Received::Capacity(_) => continue,
                    Received::Frame | Received::Work(_) => {}
                }
                match frame_kind(&frame) {
                    Ok(FrameKind::Request) => {}
                    Ok(FrameKind::Control) => {
                        let _credit = self
                            .segment
                            .accept_frame(self.lane as usize, received, &frame)
                            .map_err(failure)?;
                        if let Ok(command) = ControlCommand::decode(&frame) {
                            self.control(command);
                        }
                        continue;
                    }
                    Ok(FrameKind::ShutdownInitiated) => {
                        let _credit = self
                            .segment
                            .accept_frame(self.lane as usize, received, &frame)
                            .map_err(failure)?;
                        if let Ok(notice) = ShutdownNotice::decode(&frame) {
                            self.lifecycle.edge_shutdown(notice.edge);
                        }
                        continue;
                    }
                    _ => {
                        let _ = self.segment.accept_frame(
                            self.lane as usize,
                            received,
                            &frame,
                        );
                        continue;
                    }
                }
                if self.queued {
                    match intake
                        .queue
                        .offer(
                            &self.segment,
                            self.lane as usize,
                            received,
                            &mut frame,
                        )
                    {
                        Ok(Offered::Held) => continue,
                        Ok(Offered::Start(start)) => Some(start),
                        // A sender may withdraw after publishing but before
                        // intake accepts its reservation. Only that request
                        // has ended; this lane continues serving its peers.
                        Err(claviron::orbit::link::application::Error::StaleWork) => continue,
                        Err(error) => return Err(failure(error)),
                    }
                } else {
                    match self.segment.accept_frame(self.lane as usize, received, &frame) {
                        Ok(admission) => Some((std::mem::take(&mut frame), admission)),
                        Err(claviron::orbit::link::application::Error::StaleWork) => continue,
                        Err(error) => return Err(failure(error)),
                    }
                }
            };
            let Some((frame, admission)) = ready else { continue };
            let origin = admission.as_ref().map(|(origin, _)| *origin);
            // Move the real execution token into its terminal owner.
            let credit = Some(admission::Completion::new(
                admission.map(|(_, work)| work),
                self.queued.then(|| self.completed.clone()),
                Arc::clone(&self.active),
            ));
            let Ok(exchange) =
                ExchangeFrame::decode(tokio_util::bytes::Bytes::from(frame))
            else {
                if let Some(credit) = credit {
                    credit.release();
                }
                continue;
            };
            if origin.is_some_and(|origin| origin != exchange.origin as usize) {
                self.refuse(exchange, 400).await;
                if let Some(credit) = credit {
                    credit.release();
                }
                continue;
            }
            if !self.lifecycle.running() {
                self.refuse(exchange, 503).await;
                if let Some(credit) = credit {
                    credit.release();
                }
                continue;
            }
            // The caller may cancel between publication and stream acceptance.
            // That exchange ends; it must not tear down this application lane.
            match Exchange::new(
                exchange,
                Arc::clone(&self.segment),
                &self.bodies,
                credit,
            ) {
                Ok(exchange) => return Ok(Some(exchange)),
                Err(_) => continue,
            }
        }
    }

    /// Stop being a target: release the lane, then its hold. Requests already
    /// taken may still be answered.
    pub async fn close(&self) {
        self.closed.send_replace(true);
        self.shutdown.cancel();
        self.receiver.lock().await.take();
        let _ = self.segment.inbox().release_lane(self.lane as usize);
        // Intake is closed, but accepted bodies/sessions retain exact process
        // ownership until their terminal native owner has completed.
        self.active.drain().await;
        drop(self.hold.lock().expect("lane hold").take());
        let children = self.invokes.lock().await.clone();
        for child in children {
            if let Err(error) = child.close().await {
                eprintln!("orbit: invoke drain failed: {error}");
            }
        }
        self.events.flush();
        self.events.stop();
    }

    /// The fleet's cells, shared with every instance of this application.
    pub fn cells(&self) -> Result<cells::Cells> {
        self.cells.clone().ok_or_else(|| {
            Error::new(
                Status::GenericFailure,
                format!(
                    "Orbit cells are not available: {}",
                    self.cells_unavailable.as_deref().unwrap_or("unknown")
                ),
            )
        })
    }

    /// Receive events on these topics, from the other instances of this
    /// application and from across the Link. Subscribing is what tells the
    /// edge's side to send them.
    pub fn subscribe(
        &self,
        topics: Vec<String>,
    ) {
        self.events.subscribe(topics);
    }

    pub fn unsubscribe(
        &self,
        topics: Vec<String>,
    ) {
        self.events.unsubscribe(topics);
    }

    /// Publish an application event; returns its id. With `target`
    /// (`[runtime, name]`), only that application receives it.
    pub fn publish(
        &self,
        topic: String,
        payload: serde_json::Value,
        target: Option<Vec<String>>,
    ) -> Result<String> {
        let target = match target.as_deref() {
            None => None,
            Some([runtime, name]) => Some((runtime.clone(), name.clone())),
            Some(_) => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "target is [runtime, name]",
                ));
            }
        };
        self.events.publish(topic, &payload, target)
    }

    /// The next subscribed event, or `null` once the link is closed.
    pub async fn next_event(&self) -> Option<events::DeliveredEvent> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return None;
        }
        let mut delivered = self.delivered.lock().await;
        tokio::select! {
            event = delivered.recv() => event,
            _ = closed.changed() => None,
        }
    }

    /// The next thing the edge said — a lifecycle change, a named command or
    /// an edge shutdown — or `null` once the link is closed.
    pub async fn next_control(&self) -> Option<lifecycle::ControlEvent> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return None;
        }
        let mut controls = self.controls.lock().await;
        tokio::select! {
            event = controls.recv() => event,
            _ = closed.changed() => None,
        }
    }

    /// Whether the application is running (not stopped by the edge).
    pub fn running(&self) -> bool {
        self.lifecycle.running()
    }

    fn control(
        &self,
        command: ControlCommand,
    ) {
        if command.application != self.name {
            return;
        }
        match command.name.as_str() {
            "status" if command.target.is_none() => {
                self.lifecycle.publish_state(command.origin.map(usize::from))
            }
            "start" | "stop" | "restart" => {
                let ours = command.target.as_ref().is_some_and(|target| {
                    target.runtime == RUNTIME && target.name == self.name
                });
                if ours
                    && let Err(error) =
                        self.events.publish_lifecycle(&command.name)
                {
                    eprintln!(
                        "orbit: lifecycle command could not spread: {error}"
                    );
                }
            }
            _ => self.lifecycle.command(command.name, command.payload),
        }
    }

    /// Answer without the handler: the status, and an empty body the edge
    /// sees end, so nothing waits on a reply that will not come.
    async fn refuse(
        &self,
        exchange: ExchangeFrame,
        status: u16,
    ) {
        if let Ok(refused) = Exchange::new(
            exchange,
            Arc::clone(&self.segment),
            &self.bodies,
            None,
        ) {
            refused.refuse(status).await;
        }
    }
}

/// One request and the means to answer it.
pub struct Exchange {
    admission: Option<Arc<admission::Completion>>,
    segment: Arc<ApplicationSegment>,
    correlation: u64,
    origin: u16,
    ticket: Option<Ticket>,
    method: String,
    url: String,
    headers: Vec<Vec<String>>,
    has_body: bool,
    inline: tokio::sync::Mutex<tokio_util::bytes::Bytes>,
    streamed: bool,
    session_kind: Option<&'static str>,
    http_version: &'static str,
    session: std::sync::OnceLock<session::Session>,
    incoming: Arc<tokio::sync::Mutex<Option<ReadHalf>>>,
    http: Option<http::Output>,
    outgoing: tokio::sync::Mutex<Option<WriteHalf>>,
    replied: Mutex<bool>,
    /// Set by `abort`, so a read or write waiting on the edge gives up its
    /// half instead of holding it while the exchange is abandoned.
    aborted: tokio::sync::watch::Sender<bool>,
}

impl Drop for Exchange {
    fn drop(&mut self) {
        // Foreign collection can abandon an unclosed session. Drop its raw
        // halves before returning credit; HTTP's output driver owns its own
        // terminal guard and completes independently of this wrapper.
        let credit = self.admission.take();
        self.session.take();
        self.outgoing.get_mut().take();
        if let Some(incoming) = Arc::get_mut(&mut self.incoming) {
            incoming.get_mut().take();
        }
        self.http.take();
        drop(credit);
    }
}

impl Exchange {
    fn new(
        exchange: ExchangeFrame,
        segment: Arc<ApplicationSegment>,
        bodies: &ApplicationBodies,
        admission: Option<Arc<admission::Completion>>,
    ) -> Result<Self> {
        let (incoming, outgoing) = bodies.accept_http(&exchange).map_err(failure)?;
        let headers: Vec<Vec<String>> = exchange
            .headers
            .iter()
            .map(|(name, value)| {
                vec![
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ]
            })
            .collect();
        let url = url(&exchange, &headers);
        let session_kind = exchange.upgrade.then_some(
            if exchange.version
                == claviron::orbit::link::application::HttpVersion::Http3
            {
                "webtransport"
            } else {
                "websocket"
            },
        );
        let has_body = !matches!(
            exchange.body,
            claviron::orbit::link::application::ExchangeBody::Empty
        );
        let streamed = matches!(
            exchange.body,
            claviron::orbit::link::application::ExchangeBody::Stream
        );
        let inline = match &exchange.body {
            claviron::orbit::link::application::ExchangeBody::Inline(bytes) => {
                bytes.clone()
            }
            _ => tokio_util::bytes::Bytes::new(),
        };
        let incoming = Arc::new(tokio::sync::Mutex::new(incoming));
        let aborted = tokio::sync::watch::Sender::new(false);
        let (http, outgoing) = if session_kind.is_none() {
            (
                Some(http::Output::with_admission(
                    outgoing,
                    Arc::clone(&incoming),
                    aborted.clone(),
                    admission.clone(),
                )),
                None,
            )
        } else {
            let outgoing = match outgoing {
                Some(ResponseWriter::ByteStream(writer)) => Some(writer),
                None => None,
                Some(ResponseWriter::Payload(_)) => unreachable!("HTTP-only response ticket"),
            };
            (None, outgoing)
        };
        Ok(Self {
            admission,
            segment,
            correlation: exchange.correlation,
            origin: exchange.origin,
            ticket: exchange.stream,
            method: exchange.method,
            url,
            headers,
            has_body,
            streamed,
            inline: tokio::sync::Mutex::new(inline),
            session_kind,
            http_version: exchange.version.as_str(),
            session: std::sync::OnceLock::new(),
            incoming,
            http,
            outgoing: tokio::sync::Mutex::new(outgoing),
            replied: Mutex::new(false),
            aborted,
        })
    }
}

impl Exchange {
    /// Reply `status` with an empty, finished body, unless a reply head went
    /// out already. Returns whether this was the reply.
    async fn refuse(
        &self,
        status: u16,
    ) -> bool {
        if self.try_publish_head(status).is_err() {
            return false;
        }
        if let Some(http) = &self.http {
            let _ = http.start().await;
            let _ = http.finish().await;
        } else {
            self.incoming.lock().await.take();
            if let Some(mut outgoing) = self.outgoing.lock().await.take() {
                let _ = outgoing.shutdown().await;
            }
        }
        true
    }
}

impl Exchange {
    pub fn method(&self) -> String {
        self.method.clone()
    }

    pub fn url(&self) -> String {
        self.url.clone()
    }

    /// `[name, value]` pairs, in the order they arrived.
    pub fn headers(&self) -> Vec<Vec<String>> {
        self.headers.clone()
    }

    /// Whether the request has a body at all. Absent is not empty.
    pub fn has_body(&self) -> bool {
        self.has_body
    }

    pub fn is_session(&self) -> bool {
        self.session_kind.is_some()
    }
    pub fn session_kind(&self) -> Option<&'static str> {
        self.session_kind
    }
    pub fn http_version(&self) -> &'static str {
        self.http_version
    }

    /// The next piece of the request body, or `null` at its end.
    pub async fn read(&self) -> Result<Option<Vec<u8>>> {
        self.require_http()?;
        let mut inline = self.inline.lock().await;
        if *self.aborted.borrow() {
            return Err(abandoned());
        }
        if !inline.is_empty() {
            let length = inline.len().min(READ_CHUNK);
            return Ok(Some(inline.split_to(length).to_vec()));
        }
        if !self.streamed {
            return Ok(None);
        }
        drop(inline);
        http::read(&self.incoming, &self.aborted).await
    }

    /// Send the reply head to the edge worker that asked. Once only.
    pub async fn respond(
        &self,
        status: u16,
        headers: Vec<Vec<String>>,
    ) -> Result<()> {
        if self.is_session() && status < 300 {
            return Err(Error::new(
                Status::InvalidArg,
                "use acceptSession to accept a message session",
            ));
        }
        self.publish_head(status, headers).await
    }

    fn encode_head(
        &self,
        status: u16,
        headers: Vec<Vec<String>>,
    ) -> Result<Vec<u8>> {
        let mut replied = self.replied.lock().expect("reply flag");
        if *replied {
            return Err(Error::new(
                Status::GenericFailure,
                "this exchange was already answered",
            ));
        }
        let reply = ExchangeReply {
            correlation: self.correlation,
            status,
            headers: headers
                .into_iter()
                .filter_map(|pair| match pair.as_slice() {
                    [name, value] => Some((
                        name.as_bytes().to_vec(),
                        value.as_bytes().to_vec(),
                    )),
                    _ => None,
                })
                .collect(),
            body: if self.ticket.is_some() {
                claviron::orbit::link::application::ExchangeBody::Stream
            } else {
                claviron::orbit::link::application::ExchangeBody::Empty
            },
        };
        let frame = reply.encode().map_err(failure)?;
        *replied = true;
        Ok(frame)
    }

    // Refusals run on intake: never park the control/event reader for reply space.
    fn try_publish_head(
        &self,
        status: u16,
    ) -> Result<()> {
        let frame = self.encode_head(status, Vec::new())?;
        self.segment
            .publish_reply(self.origin as usize, &[&frame])
            .map_err(failure)
    }

    async fn publish_head(
        &self,
        status: u16,
        headers: Vec<Vec<String>>,
    ) -> Result<()> {
        if let Some(http) = &self.http {
            self.publish_head_on(status, headers, &mut None).await?;
            http.start().await
        } else {
            let mut outgoing = self.outgoing.lock().await;
            self.publish_head_on(status, headers, &mut outgoing).await
        }
    }

    async fn publish_head_on(
        &self,
        status: u16,
        headers: Vec<Vec<String>>,
        outgoing: &mut Option<WriteHalf>,
    ) -> Result<()> {
        use claviron::orbit::link::application::Error as LinkError;

        let frame = self.encode_head(status, headers)?;
        let mut stopped = None;
        loop {
            if *self.aborted.borrow() {
                return Err(abandoned());
            }
            match self.segment.publish_reply(self.origin as usize, &[&frame]) {
                Ok(()) => return Ok(()),
                Err(LinkError::ProducerBusy) => {
                    let stopped =
                        stopped.get_or_insert_with(|| self.aborted.subscribe());
                    if *stopped.borrow_and_update() {
                        return Err(abandoned());
                    }
                    tokio::select! {
                        biased;
                        _ = stopped.changed() => return Err(abandoned()),
                        _ = async {
                            if let Some(http) = &self.http { http.closed().await; }
                            else if let Some(output) = outgoing { let _ = output.peer_closed().await; }
                            else { std::future::pending::<()>().await; }
                        } => return Err(abandoned()),
                        result = self.segment.record_released() => { result.map_err(failure)?; },
                    }
                }
                Err(error) => return Err(failure(error)),
            }
        }
    }

    /// Write a piece of the reply body.
    pub async fn write(
        &self,
        chunk: &[u8],
    ) -> Result<()> {
        self.require_http()?;
        self.http.as_ref().expect("HTTP output").write(chunk).await
    }

    /// Wait for terminal HTTP ownership: true on cancellation, false on FIN.
    /// This observer does not compete with writes for the stream registration.
    pub async fn wait_closed(&self) -> Result<bool> {
        self.require_http()?;
        Ok(self.http.as_ref().expect("HTTP output").closed().await)
    }

    /// Close the reply body; the edge sees its end.
    pub async fn end(&self) -> Result<()> {
        self.require_http()?;
        self.http.as_ref().expect("HTTP output").finish().await
    }

    /// Abandon the exchange and release its halves without waiting for GC.
    pub async fn abort(&self) {
        if let Some(http) = &self.http {
            // Publish a refusal before cancellation when no head was sent.
            if self.try_publish_head(503).is_ok() {
                let _ = http.start().await;
                let _ = http.finish().await;
            } else {
                http.abort().await;
            }
            return;
        }
        self.aborted.send_replace(true);
        if let Some(session) = self.session.get() {
            session.abort().await;
            if let Some(credit) = &self.admission {
                credit.release();
            }
            return;
        }
        if !self.refuse(503).await
            && let Some(mut outgoing) = self.outgoing.lock().await.take()
        {
            outgoing.reset();
        }
        if let Some(credit) = &self.admission {
            credit.release();
        }
    }

    fn require_http(&self) -> Result<()> {
        if self.is_session() {
            return Err(Error::new(
                Status::InvalidArg,
                "message sessions use the session API, not HTTP body IO",
            ));
        }
        Ok(())
    }

    /// Accept one Link message stream with an explicit text/binary byte limit.
    /// Native H1/H2/H3 handshakes stay at the edge; 101 is the Link acknowledgment.
    pub async fn accept_session(
        &self,
        max_message_bytes: u32,
        headers: Vec<Vec<String>>,
    ) -> Result<()> {
        if !self.is_session() || max_message_bytes == 0 {
            return Err(Error::new(
                Status::InvalidArg,
                "a session and a positive maxMessageBytes are required",
            ));
        }
        let mut incoming = self.incoming.lock().await;
        let mut outgoing = self.outgoing.lock().await;
        if self.session.get().is_some() || *self.aborted.borrow() {
            return Err(abandoned());
        }
        if incoming.is_none() || outgoing.is_none() {
            return Err(Error::new(
                Status::InvalidArg,
                "message session has no duplex slot",
            ));
        }
        self.publish_head_on(101, headers, &mut outgoing).await?;
        let session = session::Session::new(
            incoming.take().unwrap(),
            outgoing.take().unwrap(),
            max_message_bytes,
            self.aborted.clone(),
        );
        self.session.set(session).map_err(|_| abandoned())?;
        drop(outgoing);
        drop(incoming);
        // An abort may have observed the old raw halves before this transfer.
        // The shared cancellation word closes that acceptance race too.
        if *self.aborted.borrow() {
            self.accepted_session()?.abort().await;
            return Err(abandoned());
        }
        Ok(())
    }

    pub async fn reject_session(
        &self,
        status: u16,
        headers: Vec<Vec<String>>,
    ) -> Result<()> {
        if !self.is_session() || !(300..=599).contains(&status) {
            return Err(Error::new(
                Status::InvalidArg,
                "session rejection requires an HTTP error or redirect status",
            ));
        }
        self.respond(status, headers).await?;
        self.incoming.lock().await.take();
        let result =
            if let Some(mut outgoing) = self.outgoing.lock().await.take() {
                outgoing.shutdown().await.map_err(failure)
            } else {
                Ok(())
            };
        if let Some(credit) = &self.admission {
            credit.release();
        }
        result
    }

    fn accepted_session(&self) -> Result<&session::Session> {
        self.session.get().ok_or_else(|| {
            Error::new(Status::InvalidArg, "session is not accepted")
        })
    }

    pub async fn receive_message(&self) -> Result<Option<Message>> {
        let result = self.accepted_session()?.receive().await;
        if matches!(result, Ok(None) | Ok(Some(Message::Close { .. })) | Err(_))
        {
            if let Some(credit) = &self.admission {
                credit.release();
            }
        }
        result
    }

    pub async fn send_message(
        &self,
        message: &Message,
    ) -> Result<()> {
        if matches!(message, Message::Close { .. }) {
            return Err(Error::new(
                Status::InvalidArg,
                "use closeSession for a close message",
            ));
        }
        self.accepted_session()?.send(message).await
    }

    pub async fn close_session(
        &self,
        code: u16,
        reason: &str,
    ) -> Result<()> {
        let result = self.accepted_session()?.close(code, reason).await;
        if let Some(credit) = &self.admission {
            credit.release();
        }
        result
    }
}

fn abandoned() -> Error {
    Error::new(Status::Cancelled, "this exchange was abandoned")
}

/// The request's absolute URL, as a `Request` needs one: scheme, the host the
/// client asked for, the path as it arrived and the query.
fn url(
    exchange: &ExchangeFrame,
    headers: &[Vec<String>],
) -> String {
    let host = headers
        .iter()
        .find(|pair| pair[0].eq_ignore_ascii_case("host"))
        .map_or("localhost", |pair| pair[1].as_str());
    let scheme =
        if exchange.scheme.is_empty() { "http" } else { &exchange.scheme };
    let path = String::from_utf8_lossy(&exchange.raw_path);
    if exchange.query.is_empty() {
        format!("{scheme}://{host}{path}")
    } else {
        format!(
            "{scheme}://{host}{path}?{}",
            String::from_utf8_lossy(&exchange.query)
        )
    }
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

#[cfg(test)]
#[path = "../tests/unit/intake.rs"]
mod intake_tests;
