//! Separate invoke Link, existing HTTP application Link, and real JavaScript/Node-API endpoints.
use claviron::orbit::{
    invoke::{CallError, Client, Failure},
    link::application::{
        ApplicationClient, ControlCommand, ControlTarget, Exchange,
        LINK_BUFFER_BYTES, LINK_SPEC,
    },
    link::{Connection, invoke::Limits},
};
use std::{future::Future, io::Write, sync::Arc, task::Poll};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
fn signal(value: &str) -> Result<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}
async fn command(
    input: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
    expected: &str,
) -> Result<()> {
    if input.next_line().await?.as_deref() != Some(expected) {
        return Err(format!("expected {expected}").into());
    }
    Ok(())
}
async fn http(
    client: &ApplicationClient,
    status: u16,
) -> Result<()> {
    let pending = client.send(
        "demo",
        Exchange {
            method: "GET".into(),
            scheme: "http".into(),
            raw_path: b"/plain".to_vec(),
            headers: vec![(b"host".to_vec(), b"example.test".to_vec())],
            ..Default::default()
        },
    )?;
    let (_upload, mut response, reply, _) = pending.split();
    if reply.head().await?.status != status {
        return Err("HTTP status changed during invoke work".into());
    }
    let mut bytes = Vec::new();
    response.read_to_end(&mut bytes).await?;
    if status == 200 && bytes != b"http-ready" {
        return Err("HTTP path received invoke data".into());
    }
    Ok(())
}
fn action(
    client: &ApplicationClient,
    name: &str,
) -> Result<()> {
    client.command(&ControlCommand {
        application: "demo".into(),
        name: name.into(),
        payload: vec![],
        target: Some(ControlTarget {
            runtime: "js".into(),
            name: "demo".into(),
        }),
    })?;
    Ok(())
}
async fn generation(
    client: &Client,
    expected: u64,
) -> Result<()> {
    let bytes = client.call("js", "generation", vec![]).await?;
    if bytes != expected.to_string().as_bytes() {
        return Err("Invoke used a wrong generation".into());
    }
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: invoke <application-fleet> <invoke-fleet>".into());
    }
    let bound = LINK_BUFFER_BYTES * 2;
    let connection =
        Connection::open(&args[1], LINK_SPEC, "rust", "invoke", 21)?;
    let segment = Arc::clone(connection.segment());
    let limits = Limits {
        pending_calls: 4,
        accepted_calls: 4,
        operation_bytes: 32,
        request_bytes: 1024,
        response_bytes: bound,
    };
    let (handle, driver) = connection.invocations(limits)?;
    let mut client = Client::new();
    client.register("js", handle.route("js")?)?;
    let client = Arc::new(client);
    let first = Connection::open(&args[1], LINK_SPEC, "dupe", "invoke", 31)?;
    let second = Connection::open(&args[1], LINK_SPEC, "dupe", "invoke", 32)?;
    let (gate_send, mut gates) = mpsc::channel::<oneshot::Sender<Vec<u8>>>(4);
    let stop = CancellationToken::new();
    let stopping = stop.clone();
    let (started, ready) = oneshot::channel();
    let mut started = Some(started);
    let driver_task = tokio::spawn(async move {
        let running = driver.run_until(
            stopping.cancelled(),
            move |call| {
                let gates = gate_send.clone();
                async move {
                    match call.request.operation.as_str() {
                        "echo" => Ok(call.request.payload),
                        "reject" => Err(Failure::Rejected(vec![0, 255])),
                        "gate" => {
                            let (send, receive) = oneshot::channel();
                            gates
                                .send(send)
                                .await
                                .map_err(|_| Failure::Closing)?;
                            signal("NATIVE_GATE_HELD")
                                .map_err(|_| Failure::Closing)?;
                            receive.await.map_err(|_| Failure::Closing)
                        }
                        _ => Err(Failure::Unsupported),
                    }
                }
            },
            |_| async { Ok(()) },
        );
        tokio::pin!(running);
        std::future::poll_fn(|cx| {
            let result = running.as_mut().poll(cx);
            if matches!(result, Poll::Pending)
                && let Some(started) = started.take()
            {
                let _ = started.send(());
            }
            result
        })
        .await
    });
    ready.await?;
    let http_client = ApplicationClient::join(&args[0], "http-peer", 1)?;
    signal(&format!("PEER_READY {bound}"))?;
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    command(&mut input, "run").await?;
    let lane = *segment
        .inbox()
        .serving("js")
        .first()
        .ok_or("No JavaScript invoke service")?;
    let stamp =
        (segment.inbox().owner(lane)?, segment.inbox().incarnation(lane)?);
    for operation in ["echo", "echo-λ"] {
        if client.call("js", operation, vec![0, 255, 7]).await? != [0, 255, 7] {
            return Err("Invoke bytes/operation changed".into());
        }
    }
    if client.call("js", "reject", vec![]).await
        != Err(CallError::Remote(Failure::Rejected(vec![0, 255, 7])))
    {
        return Err("Domain rejection changed".into());
    }
    if client.call("js", "missing", vec![]).await
        != Err(CallError::Remote(Failure::Unsupported))
    {
        return Err("Unsupported operation changed".into());
    }
    if client.call("js", "fail", vec![]).await
        != Err(CallError::Remote(Failure::Rejected(vec![])))
    {
        return Err("Handler failure was not rejected".into());
    }
    if client.call("js", "too-big", vec![]).await
        != Err(CallError::Remote(Failure::ResponseTooLarge))
    {
        return Err("Response bound was not enforced".into());
    }
    generation(&client, 1).await?;
    http(&http_client, 200).await?;
    signal("BASIC_CHECKS_PASSED")?;
    command(&mut input, "outgoing-done").await?;
    let calls = Arc::clone(&client);
    let held =
        tokio::spawn(async move { calls.call("js", "hold", vec![]).await });
    signal("INBOUND_SENT")?;
    command(&mut input, "cancel-inbound").await?;
    if client.call("js", "echo", vec![]).await
        != Err(CallError::Remote(Failure::Busy))
    {
        return Err("Accepted invoke limit was not enforced".into());
    }
    http(&http_client, 200).await?;
    held.abort();
    let _ = held.await;
    signal("CALLER_CANCELLED")?;
    command(&mut input, "inbound-finished").await?;
    generation(&client, 1).await?;
    let calls = Arc::clone(&client);
    let held =
        tokio::spawn(async move { calls.call("js", "hold", vec![]).await });
    signal("GENERATION_INBOUND_SENT")?;
    command(&mut input, "stop-application").await?;
    action(&http_client, "stop")?;
    if held.await? != Err(CallError::Remote(Failure::Closing)) {
        return Err("Stopped generation did not reject its invoke".into());
    }
    if client.call("js", "echo", vec![]).await
        != Err(CallError::Remote(Failure::Closing))
    {
        return Err("Stopped invoke admission remained open".into());
    }
    http(&http_client, 503).await?;
    signal("STOP_CHECKS_PASSED")?;
    command(&mut input, "start-application").await?;
    action(&http_client, "start")?;
    signal("START_SENT")?;
    command(&mut input, "start-observed").await?;
    generation(&client, 2).await?;
    http(&http_client, 200).await?;
    signal("START_CHECKS_PASSED")?;
    command(&mut input, "outgoing-cancel").await?;
    let gate = gates.recv().await.ok_or("Missing outgoing gate")?;
    command(&mut input, "finish-gate").await?;
    gate.send(vec![0, 255])
        .map_err(|_| "Caller cancellation rolled back the remote handler")?;
    signal("NATIVE_GATE_FINISHED")?;
    command(&mut input, "outgoing-generation").await?;
    let gate = gates.recv().await.ok_or("Missing generation gate")?;
    command(&mut input, "restart-application").await?;
    action(&http_client, "restart")?;
    signal("RESTART_SENT")?;
    command(&mut input, "finish-gate").await?;
    gate.send(vec![0, 255]).map_err(
        |_| "Generation cancellation terminated unrelated remote work",
    )?;
    signal("NATIVE_GATE_FINISHED")?;
    generation(&client, 3).await?;
    if (segment.inbox().owner(lane)?, segment.inbox().incarnation(lane)?)
        != stamp
    {
        return Err("Invoke connection changed across generations".into());
    }
    http(&http_client, 200).await?;
    signal("GENERATION_CHECKS_PASSED")?;
    command(&mut input, "drain").await?;
    let calls = Arc::clone(&client);
    let pending =
        tokio::spawn(
            async move { calls.call("js", "drain", vec![0, 255]).await },
        );
    let gate = gates.recv().await.ok_or("Missing drain gate")?;
    command(&mut input, "release-drain").await?;
    gate.send(vec![0, 255])
        .map_err(|_| "Graceful close cancelled accepted work")?;
    if pending.await?? != [0, 255] {
        return Err("Graceful invoke drain changed result".into());
    }
    signal("INVOKE_CHECKS_PASSED")?;
    command(&mut input, "stop").await?;
    http_client.leave();
    stop.cancel();
    driver_task.await??;
    drop(first);
    drop(second);
    signal("PEER_DRAINED")?;
    Ok(())
}
