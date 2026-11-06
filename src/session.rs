//! Application message IO over the shared Link record protocol.

use crate::{Error, ErrorKind, Result, abandoned, failure};
use claviron::orbit::link::application::{
    Message, MessageReader, write_message,
};
use claviron::orbitive::stream::{ReadHalf, WriteHalf};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, watch};

// WebSocket control payloads have a protocol maximum of 125 bytes, even when
// an application's text/binary limit is smaller. This is not a tuning value.
const CONTROL_BYTES: usize = 125;

struct Reader {
    half: Option<ReadHalf>,
    records: MessageReader,
}

pub(crate) struct Session {
    incoming: Mutex<Reader>,
    outgoing: Mutex<Option<WriteHalf>>,
    stopped: watch::Sender<bool>,
    limit: usize,
}

impl Session {
    pub(crate) fn new(
        read: ReadHalf,
        write: WriteHalf,
        limit: u32,
        stopped: watch::Sender<bool>,
    ) -> Self {
        Self {
            incoming: Mutex::new(Reader {
                half: Some(read),
                records: MessageReader::with_limit(
                    (limit as usize).max(CONTROL_BYTES),
                ),
            }),
            outgoing: Mutex::new(Some(write)),
            stopped,
            limit: limit as usize,
        }
    }

    pub(crate) async fn receive(&self) -> Result<Option<Message>> {
        let mut reader =
            self.incoming.try_lock().map_err(|_| busy("receive"))?;
        let mut stopped = self.stopped.subscribe();
        if *stopped.borrow_and_update() {
            reader.half.take();
            return Ok(None);
        }
        let Reader { half, records } = &mut *reader;
        let Some(half) = half else { return Ok(None) };
        let result = tokio::select! {
            biased;
            _ = stopped.changed() => Ok(None),
            message = records.read(half) => message.map_err(failure),
        };
        if matches!(result, Ok(None)) {
            reader.half.take();
        }
        drop(reader);
        let result = result.and_then(|message| {
            if let Some(message) = &message {
                self.validate(message)?;
            }
            Ok(message)
        });
        match &result {
            Ok(Some(Message::Close { code, reason })) => {
                // 1005/1006 are observations, never close codes sent on wire.
                if matches!(*code, 1005 | 1006) {
                    self.finish().await?;
                } else {
                    self.close(*code, reason).await?;
                }
            }
            Err(_) => self.abort().await,
            _ => {}
        }
        result
    }

    pub(crate) async fn send(
        &self,
        message: &Message,
    ) -> Result<()> {
        self.validate(message)?;
        let mut writer = self.outgoing.try_lock().map_err(|_| busy("send"))?;
        let mut stopped = self.stopped.subscribe();
        if *stopped.borrow_and_update() {
            return Err(abandoned());
        }
        let outgoing = writer.as_mut().ok_or_else(abandoned)?;
        let result = tokio::select! {
            biased;
            _ = stopped.changed() => Err(abandoned()),
            result = write_message(outgoing, message) => result.map_err(failure),
        };
        if result.is_err() {
            if let Some(mut outgoing) = writer.take() {
                outgoing.reset();
            }
            self.stopped.send_replace(true);
            drop(writer);
            self.incoming.lock().await.half.take();
        }
        result
    }

    pub(crate) async fn close(
        &self,
        code: u16,
        reason: &str,
    ) -> Result<()> {
        validate_close(code, reason)?;
        let mut writer = self.outgoing.lock().await;
        if let Some(mut outgoing) = writer.take() {
            let mut stopped = self.stopped.subscribe();
            let result = if *stopped.borrow_and_update() {
                Err(abandoned())
            } else {
                tokio::select! {
                    biased;
                    _ = stopped.changed() => Err(abandoned()),
                    result = async {
                        write_message(&mut outgoing, &Message::Close { code, reason: reason.to_owned() }).await?;
                        outgoing.shutdown().await
                    } => result.map_err(failure),
                }
            };
            self.stopped.send_replace(true);
            drop(writer);
            self.incoming.lock().await.half.take();
            return result;
        }
        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        let mut writer = self.outgoing.lock().await;
        let result = match writer.take() {
            Some(mut outgoing) => outgoing.shutdown().await.map_err(failure),
            None => Ok(()),
        };
        self.stopped.send_replace(true);
        drop(writer);
        self.incoming.lock().await.half.take();
        result
    }

    pub(crate) async fn abort(&self) {
        self.stopped.send_replace(true);
        self.incoming.lock().await.half.take();
        if let Some(mut outgoing) = self.outgoing.lock().await.take() {
            outgoing.reset();
        }
    }

    fn validate(
        &self,
        message: &Message,
    ) -> Result<()> {
        let length = match message {
            Message::Text(text) => text.len(),
            Message::Binary(bytes) => bytes.len(),
            Message::Close { code, reason } => {
                if matches!(*code, 1005 | 1006) && reason.is_empty() {
                    return Ok(());
                }
                return validate_close(*code, reason);
            }
        };
        if length > self.limit {
            return Err(Error::new(
                ErrorKind::InvalidArg,
                "message exceeds maxMessageBytes",
            ));
        }
        Ok(())
    }
}

fn busy(operation: &str) -> Error {
    Error::new(
        ErrorKind::InvalidArg,
        format!("another {operation} is in progress; await it first"),
    )
}

pub(crate) fn validate_close(
    code: u16,
    reason: &str,
) -> Result<()> {
    if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999) {
        return Err(Error::new(
            ErrorKind::InvalidArg,
            "invalid WebSocket close code",
        ));
    }
    if reason.len() > CONTROL_BYTES - 2 {
        return Err(Error::new(
            ErrorKind::InvalidArg,
            "close reason exceeds 123 UTF-8 bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/session.rs"]
mod tests;
