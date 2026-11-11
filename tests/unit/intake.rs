use super::*;
use claviron::orbit::link::application::{
    ApplicationClient, Exchange as Request,
};
use std::future::{Future, poll_fn};
use std::task::Poll;

fn request(path: &str) -> Request {
    Request {
        method: "GET".into(),
        scheme: "http".into(),
        path: path.into(),
        raw_path: path.as_bytes().to_vec(),
        ..Default::default()
    }
}

#[tokio::test]
async fn bounded_intake_promotes_on_terminal_io_and_drain_ignores_retained_wrappers()
 {
    let fleet = format!("j{:x}", std::process::id());
    let link = join(JoinOptions {
        fleet: fleet.clone(),
        name: "test".into(),
        max_in_flight: Some(1),
        max_queued: Some(1),
        ..Default::default()
    })
    .await
    .unwrap();
    let client = ApplicationClient::join(&fleet, "edge", 1).unwrap();
    let first_reply = client.send("test", request("/first")).unwrap();
    let first = link.next().await.unwrap().unwrap();
    let second_reply = client.send("test", request("/second")).unwrap();
    assert!(client.send("test", request("/full")).is_err());
    let second = link.next();
    tokio::pin!(second);
    poll_fn(|cx| {
        assert!(second.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    first.respond(200, vec![]).await.unwrap();
    first.end().await.unwrap();
    let second = second.await.unwrap().unwrap();
    assert!(second.url().ends_with("/second"));
    let closing = link.close();
    tokio::pin!(closing);
    poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    second.respond(200, vec![]).await.unwrap();
    second.end().await.unwrap();
    closing.await;
    assert!(link.next().await.unwrap().is_none());
    // Both foreign-visible request wrappers still exist after the drain.
    drop((first, second, first_reply, second_reply));
}

#[tokio::test]
async fn caller_departure_before_acceptance_does_not_end_intake() {
    let fleet = format!("k{:x}", std::process::id());
    let link = join(JoinOptions {
        fleet: fleet.clone(),
        name: "test".into(),
        max_in_flight: Some(1),
        max_queued: Some(1),
        ..Default::default()
    })
    .await
    .unwrap();
    let client = ApplicationClient::join(&fleet, "edge", 1).unwrap();
    drop(client.send("test", request("/cancelled")).unwrap());
    let valid = client.send("test", request("/valid")).unwrap();
    let accepted = link.next().await.unwrap().unwrap();
    assert!(accepted.url().ends_with("/valid"));
    accepted.respond(200, vec![]).await.unwrap();
    accepted.end().await.unwrap();
    link.close().await;
    drop((accepted, valid));
}

#[tokio::test]
async fn withdrawn_reservation_is_skipped_without_rejecting_next() {
    let fleet = format!("w{:x}", std::process::id());
    let link = join(JoinOptions {
        fleet: fleet.clone(),
        name: "test".into(),
        max_in_flight: Some(1),
        max_queued: Some(1),
        ..Default::default()
    })
    .await
    .unwrap();
    let client = ApplicationClient::join(&fleet, "edge", 1).unwrap();
    let withdrawn = client.send("test", request("/withdrawn")).unwrap();
    let (upload, body, reply, _) = withdrawn.split();
    assert!(reply.withdrawal().unwrap().withdraw());
    drop((upload, body, reply));
    let valid = client.send("test", request("/valid")).unwrap();
    let accepted = link.next().await.unwrap().unwrap();
    assert!(accepted.url().ends_with("/valid"));
    accepted.respond(200, vec![]).await.unwrap();
    accepted.end().await.unwrap();
    link.close().await;
    drop((accepted, valid));
}
