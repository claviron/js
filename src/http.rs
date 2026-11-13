//! Terminal ownership of an HTTP exchange's stream halves.

use std::sync::Arc;

use claviron::orbit::link::application::body::ResponseWriter;
use claviron::orbitive::stream::ReadHalf;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Open,
    Finished,
    Cancelled,
}

enum Command {
    Start(oneshot::Sender<crate::Result<()>>),
    Write(Vec<u8>, oneshot::Sender<crate::Result<()>>),
    Finish(oneshot::Sender<crate::Result<()>>),
}

/// One driver owns output readiness, including idle reader-departure wakes.
/// Only one command may be buffered; writers await their own acknowledgement.
pub(crate) struct Output {
    commands: mpsc::Sender<Command>,
    terminal: watch::Receiver<Terminal>,
    stopped: watch::Sender<bool>,
}

impl Output {
    #[cfg(test)]
    pub(crate) fn new(
        outgoing: Option<claviron::orbitive::stream::WriteHalf>,
        incoming: Arc<Mutex<Option<ReadHalf>>>,
        stopped: watch::Sender<bool>,
    ) -> Self {
        Self::with_admission(outgoing.map(ResponseWriter::ByteStream), incoming, stopped, None)
    }

    pub(crate) fn with_admission(
        outgoing: Option<ResponseWriter>,
        incoming: Arc<Mutex<Option<ReadHalf>>>,
        stopped: watch::Sender<bool>,
        admission: Option<Arc<crate::admission::Completion>>,
    ) -> Self {
        let (commands, receiver) = mpsc::channel(1);
        let (terminal, state) = watch::channel(Terminal::Open);
        let driver_stop = stopped.clone();
        tokio::spawn(async move {
            drive(
                outgoing,
                incoming,
                driver_stop,
                receiver,
                terminal,
                crate::admission::TerminalGuard(admission),
            )
            .await;
        });
        Self { commands, terminal: state, stopped }
    }

    pub(crate) async fn start(&self) -> crate::Result<()> {
        let (done, result) = oneshot::channel();
        self.commands.send(Command::Start(done)).await.map_err(|_| crate::abandoned())?;
        result.await.map_err(|_| crate::abandoned())?
    }

    pub(crate) async fn write(
        &self,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let (done, result) = oneshot::channel();
        self.commands
            .send(Command::Write(bytes.to_vec(), done))
            .await
            .map_err(|_| crate::abandoned())?;
        result.await.map_err(|_| crate::abandoned())?
    }

    pub(crate) async fn finish(&self) -> crate::Result<()> {
        let (done, result) = oneshot::channel();
        if self.commands.send(Command::Finish(done)).await.is_err() {
            return if self.closed().await {
                Err(crate::abandoned())
            } else {
                Ok(())
            };
        }
        result.await.map_err(|_| crate::abandoned())?
    }

    pub(crate) async fn closed(&self) -> bool {
        let mut state = self.terminal.clone();
        loop {
            match *state.borrow_and_update() {
                Terminal::Finished => return false,
                Terminal::Cancelled => return true,
                Terminal::Open => {}
            }
            if state.changed().await.is_err() {
                return true;
            }
        }
    }

    pub(crate) async fn abort(&self) {
        self.stopped.send_replace(true);
        self.closed().await;
    }
}

async fn drive(
    mut outgoing: Option<ResponseWriter>,
    incoming: Arc<Mutex<Option<ReadHalf>>>,
    stopped: watch::Sender<bool>,
    mut commands: mpsc::Receiver<Command>,
    terminal: watch::Sender<Terminal>,
    completion: crate::admission::TerminalGuard,
) {
    let mut cancelled = stopped.subscribe();
    let mut finished = None;
    let state = loop {
        if *cancelled.borrow_and_update() {
            break Terminal::Cancelled;
        }
        let command = tokio::select! {
            biased;
            _ = cancelled.changed() => break Terminal::Cancelled,
            _ = async {
                match &mut outgoing {
                    Some(output) => { let _ = output.peer_closed().await; },
                    None => std::future::pending().await,
                }
            } => break Terminal::Cancelled,
            command = commands.recv() => command,
        };
        match command {
            Some(Command::Start(done)) => {
                let result = outgoing.as_mut().map_or(Ok(()), |output| output.start())
                    .map_err(crate::failure);
                let failed = result.is_err();
                let _ = done.send(result);
                if failed {
                    break Terminal::Cancelled;
                }
            }
            Some(Command::Write(bytes, done)) => {
                let result = match &mut outgoing {
                    Some(output) => tokio::select! {
                        biased;
                        _ = cancelled.changed() => Err(crate::abandoned()),
                        result = output.write(&bytes) => result.map_err(crate::failure),
                    },
                    None => Err(crate::abandoned()),
                };
                let failed = result.is_err();
                let _ = done.send(result);
                if failed {
                    break Terminal::Cancelled;
                }
            }
            Some(Command::Finish(done)) => {
                let result = match &mut outgoing {
                    Some(output) => tokio::select! {
                        biased;
                        _ = cancelled.changed() => Err(crate::abandoned()),
                        result = output.finish() => result.map_err(crate::failure),
                    },
                    None => Ok(()),
                };
                let state = if result.is_ok() {
                    Terminal::Finished
                } else {
                    Terminal::Cancelled
                };
                finished = Some((done, result));
                break state;
            }
            None => break Terminal::Cancelled,
        }
    };
    // Wake an input read before acquiring its mutex. Drop both halves before
    // acknowledging completion, even if the JavaScript wrapper stays alive.
    commands.close();
    stopped.send_replace(true);
    if state == Terminal::Cancelled && let Some(output) = outgoing.as_mut() {
        output.reset();
    }
    drop(outgoing);
    incoming.lock().await.take();
    // Credit is observable before completion or cancellation is acknowledged.
    drop(completion);
    terminal.send_replace(state);
    if let Some((done, result)) = finished {
        let _ = done.send(result);
    }
}

pub(crate) async fn read(
    incoming: &Mutex<Option<ReadHalf>>,
    stopped: &watch::Sender<bool>,
) -> crate::Result<Option<Vec<u8>>> {
    let mut held = incoming.lock().await;
    let Some(incoming) = held.as_mut() else { return Ok(None) };
    let mut stopped = stopped.subscribe();
    if *stopped.borrow_and_update() {
        return Err(crate::abandoned());
    }
    let mut piece = vec![0u8; crate::READ_CHUNK];
    let read = tokio::select! {
        read = incoming.read(&mut piece) => read.map_err(crate::failure)?,
        _ = stopped.changed() => return Err(crate::abandoned()),
    };
    if read == 0 {
        held.take();
        return Ok(None);
    }
    piece.truncate(read);
    Ok(Some(piece))
}

#[cfg(test)]
#[path = "../tests/unit/http.rs"]
mod tests;
