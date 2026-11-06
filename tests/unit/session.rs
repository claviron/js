use super::*;
use claviron::orbit::link::application::read_message;
use claviron::orbitive::Fleet;
use claviron::orbitive::stream::{
    Endpoint, Incarnation, STREAM_BUFFER_BYTES, Streams,
};
use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::task::Poll;

fn pair(limit: u32) -> (Session, Endpoint) {
    // In-memory stream storage; no server, SHM segment or process lifecycle.
    let streams = Streams::new(
        Arc::new(Fleet::join("session-unit", 2).unwrap()),
        Incarnation::new(1),
    )
    .unwrap();
    let (endpoint, ticket) = streams.create().unwrap();
    let peer = streams.open(ticket).unwrap();
    let (read, write) = endpoint.split();
    (Session::new(read, write, limit, watch::Sender::new(false)), peer)
}

#[tokio::test]
async fn message_directions_are_independent_and_preserve_boundaries() {
    let (session, peer) = pair(1024);
    let (mut input, mut output) = peer.split();
    let mut waiting = std::pin::pin!(session.receive());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let outgoing = Message::Binary(vec![0, 255, 1]);
    session.send(&outgoing).await.unwrap();
    assert_eq!(read_message(&mut input).await.unwrap(), Some(outgoing));
    write_message(&mut output, &Message::Text("hello".into())).await.unwrap();
    assert_eq!(waiting.await.unwrap(), Some(Message::Text("hello".into())));
    output.shutdown().await.unwrap();
    assert_eq!(session.receive().await.unwrap(), None);
    // A peer FIN closes only input, not the reply direction.
    session.send(&Message::Text("after FIN".into())).await.unwrap();
    assert_eq!(
        read_message(&mut input).await.unwrap(),
        Some(Message::Text("after FIN".into()))
    );
    session.close(1000, "done").await.unwrap();
    assert_eq!(
        read_message(&mut input).await.unwrap(),
        Some(Message::Close { code: 1000, reason: "done".into() })
    );
    assert_eq!(read_message(&mut input).await.unwrap(), None);
}

#[tokio::test]
async fn close_echo_and_control_limit_are_distinct_from_text_limit() {
    let (session, mut peer) = pair(1);
    let close = Message::Close { code: 1000, reason: "finished".into() };
    write_message(&mut peer, &close).await.unwrap();
    assert_eq!(session.receive().await.unwrap(), Some(close.clone()));
    assert_eq!(read_message(&mut peer).await.unwrap(), Some(close));
    assert_eq!(read_message(&mut peer).await.unwrap(), None);
    assert_eq!(session.receive().await.unwrap(), None);
}

#[tokio::test]
async fn pseudo_close_code_is_never_echoed_on_wire() {
    let (session, mut peer) = pair(64);
    let close = Message::Close { code: 1006, reason: String::new() };
    write_message(&mut peer, &close).await.unwrap();
    assert_eq!(session.receive().await.unwrap(), Some(close));
    assert_eq!(read_message(&mut peer).await.unwrap(), None);
}

#[tokio::test]
async fn oversized_or_malformed_records_reset_the_stream() {
    for record in [
        vec![1, 255, 255, 255, 255], // Only the header; never allocate its payload.
        vec![1, 2, 0, 0, 0, 0xff, 0xff], // Invalid UTF-8.
        vec![2, 2, 0, 0, 0, 1, 2], // Text/binary bound below control allowance.
    ] {
        let (session, mut peer) = pair(1);
        peer.write_all(&record).await.unwrap();
        assert!(session.receive().await.is_err());
        assert!(read_message(&mut peer).await.is_err());
    }
}

#[tokio::test]
async fn abort_wakes_reader_and_backpressured_writer_without_a_timer() {
    let (session, _peer) = pair((STREAM_BUFFER_BYTES * 2) as u32);
    let message = Message::Binary(vec![7; STREAM_BUFFER_BYTES * 2]);
    let mut read = std::pin::pin!(session.receive());
    let mut write = std::pin::pin!(session.send(&message));
    poll_fn(|cx| {
        assert!(read.as_mut().poll(cx).is_pending());
        assert!(write.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        session.receive().await.unwrap_err().message.contains("await it first")
    );
    assert!(
        session
            .send(&Message::Text("other".into()))
            .await
            .unwrap_err()
            .message
            .contains("await it first")
    );
    // Polling these futures together models independent native promises.
    let ((), read, write) = tokio::join!(session.abort(), read, write);
    assert_eq!(read.unwrap(), None);
    assert!(write.is_err());
}

#[tokio::test]
async fn cancelled_receive_keeps_partial_record_and_close_wakes_it() {
    let (session, mut peer) = pair(64);
    peer.write_all(&[1, 3, 0, 0, 0, b'a']).await.unwrap();
    {
        let mut read = std::pin::pin!(session.receive());
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    peer.write_all(b"bc").await.unwrap();
    assert_eq!(
        session.receive().await.unwrap(),
        Some(Message::Text("abc".into()))
    );
    let mut read = std::pin::pin!(session.receive());
    poll_fn(|cx| {
        assert!(read.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let (closed, received) = tokio::join!(session.close(1000, ""), read);
    closed.unwrap();
    assert_eq!(received.unwrap(), None);
}
