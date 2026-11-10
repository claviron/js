use super::*;
use claviron::orbitive::Fleet;
use claviron::orbitive::stream::{Incarnation, StreamSpec, Streams};
use std::future::{Future, poll_fn};
use std::task::Poll;

type Input = Arc<Mutex<Option<ReadHalf>>>;

fn pair(
    name: &str
) -> (Streams, claviron::orbitive::stream::Endpoint, Input, Output) {
    let streams = Streams::with_spec(
        Arc::new(Fleet::join(name, 1).unwrap()),
        Incarnation::new(1),
        StreamSpec::new(246, 1, 256),
    )
    .unwrap();
    let (edge, ticket) = streams.create().unwrap();
    let (input, output) = streams.open(ticket).unwrap().split();
    let incoming = Arc::new(Mutex::new(Some(input)));
    let output = Output::new(
        Some(output),
        Arc::clone(&incoming),
        watch::Sender::new(false),
    );
    (streams, edge, incoming, output)
}

#[tokio::test]
async fn finished_http_reuses_a_slot_while_native_wrappers_remain_alive() {
    let streams = Streams::with_spec(
        Arc::new(Fleet::join("http-finish", 1).unwrap()),
        Incarnation::new(1),
        StreamSpec::new(246, 1, 256),
    )
    .unwrap();
    let (mut edge, ticket) = streams.create().unwrap();
    let (input, output) = streams.open(ticket).unwrap().split();
    let first = Output::new(
        Some(output),
        Arc::new(Mutex::new(Some(input))),
        watch::Sender::new(false),
    );
    let mut wrappers = vec![first];
    for _ in 0..16 {
        let output = wrappers.last().unwrap();
        output.finish().await.unwrap();
        assert!(!output.closed().await);
        edge.rearm(&streams).expect("completed HTTP must release its peer");
        // The fixture's peer ticket remains stable across rearm.
        let (input, output) = streams.open(ticket).unwrap().split();
        wrappers.push(Output::new(
            Some(output),
            Arc::new(Mutex::new(Some(input))),
            watch::Sender::new(false),
        ));
    }
    wrappers.last().unwrap().finish().await.unwrap();
    assert_eq!(wrappers.len(), 17);
}

#[tokio::test]
async fn finished_http_wakes_a_pending_body_read_before_releasing_it() {
    let (streams, mut edge, incoming, output) = pair("http-read");
    let mut reading = std::pin::pin!(read(&incoming, &output.stopped));
    poll_fn(|cx| {
        assert!(reading.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let (finished, received) = tokio::join!(output.finish(), reading);
    finished.unwrap();
    assert!(received.is_err());
    edge.rearm(&streams).unwrap();
}

#[tokio::test]
async fn idle_http_detects_reader_departure_without_another_write() {
    let (_streams, edge, incoming, output) = pair("http-idle");
    let (reader, upload) = edge.split();
    drop(reader);
    assert!(output.closed().await);
    assert!(incoming.lock().await.is_none());
    assert!(output.write(b"late").await.is_err());
    drop(upload);
}

#[tokio::test]
async fn upload_fin_is_not_response_cancellation() {
    let (_streams, edge, incoming, output) = pair("http-fin");
    let (mut reader, mut upload) = edge.split();
    upload.shutdown().await.unwrap();
    assert!(read(&incoming, &output.stopped).await.unwrap().is_none());
    output.write(b"valid response").await.unwrap();
    output.finish().await.unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"valid response");
    assert!(!output.closed().await);
}

#[tokio::test]
async fn cancellation_wakes_input_and_backpressured_output() {
    let (_streams, edge, incoming, output) = pair("http-blocked");
    let (reader, _upload) = edge.split();
    output.write(&[1; 256]).await.unwrap();
    let bytes = vec![42; 4096];
    let mut writing = std::pin::pin!(output.write(&bytes));
    let mut reading = std::pin::pin!(read(&incoming, &output.stopped));
    poll_fn(|cx| {
        assert!(writing.as_mut().poll(cx).is_pending());
        assert!(reading.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(reader);
    let (written, received, cancelled) =
        tokio::join!(writing, reading, output.closed());
    assert!(written.is_err());
    assert!(received.is_err());
    assert!(cancelled);
    assert!(incoming.lock().await.is_none());
}

#[tokio::test]
async fn local_abort_wakes_a_backpressured_write() {
    let (_streams, _edge, _incoming, output) = pair("http-abort");
    output.write(&[1; 256]).await.unwrap();
    let bytes = vec![42; 4096];
    let mut writing = std::pin::pin!(output.write(&bytes));
    poll_fn(|cx| {
        assert!(writing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let (written, ()) = tokio::join!(writing, output.abort());
    assert!(written.is_err());
    assert!(output.closed().await);
}

#[tokio::test]
async fn dropping_the_wrapper_also_retires_the_driver() {
    let (_streams, _edge, incoming, output) = pair("http-drop");
    let mut state = output.terminal.clone();
    drop(output);
    while *state.borrow_and_update() == Terminal::Open {
        state.changed().await.unwrap();
    }
    assert!(incoming.lock().await.is_none());
}

#[tokio::test]
async fn finish_returns_admission_before_its_acknowledgement() {
    let streams = Streams::with_spec(
        Arc::new(Fleet::join("http-credit", 1).unwrap()),
        Incarnation::new(1),
        StreamSpec::new(246, 1, 256),
    )
    .unwrap();
    let (_edge, ticket) = streams.create().unwrap();
    let (input, output) = streams.open(ticket).unwrap().split();
    let (finished, mut ended) = mpsc::unbounded_channel();
    let credit = crate::admission::Completion::new(
        None,
        Some(finished),
        Arc::new(crate::admission::Active::default()),
    );
    let output = Output::with_admission(
        Some(output),
        Arc::new(Mutex::new(Some(input))),
        watch::Sender::new(false),
        Some(Arc::clone(&credit)),
    );
    output.finish().await.unwrap();
    assert!(ended.try_recv().is_ok());
    credit.release();
    assert!(ended.try_recv().is_err());
}
