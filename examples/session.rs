//! Exercise the Link side of message sessions against an claviron-js application.
//! This sends the same records as an edge; it does not open a WebSocket/QUIC listener.
//!
//! `cargo run --example session -- <fleet> <application> <h1|h2|h3>`

use claviron::orbit::link::application::ExchangeBody;
use claviron::orbit::link::application::{
    ApplicationClient, Exchange, HttpVersion, Message, read_message,
    write_message,
};
use tokio::io::AsyncWriteExt;

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut arguments = std::env::args().skip(1);
    let usage = "usage: session <fleet> <application> <h1|h2|h3>";
    let fleet = arguments.next().ok_or(usage)?;
    let application = arguments.next().ok_or(usage)?;
    let version = match arguments.next().as_deref() {
        Some("h1") => HttpVersion::Http11,
        Some("h2") => HttpVersion::Http2,
        Some("h3") => HttpVersion::Http3,
        _ => return Err(usage.into()),
    };
    let client = ApplicationClient::join(&fleet, "session-probe", 1)?;
    if !client.serves(&application) {
        client.leave();
        return Err("application has not joined the fleet".into());
    }
    // Two independent Link slots, as two WebSockets or WT bidi streams use.
    let (first, second) = tokio::join!(
        roundtrip(&client, &application, version),
        roundtrip(&client, &application, version),
    );
    client.leave();
    first?;
    second?;
    println!("{}: two message sessions passed", version.as_str());
    Ok(())
}

async fn roundtrip(
    client: &ApplicationClient,
    application: &str,
    version: HttpVersion,
) -> Result<(), Error> {
    let pending = client.send(
        application,
        claviron::orbit::link::application::Exchange {
            body: ExchangeBody::Stream,
            ..Exchange {
                method: if version == HttpVersion::Http11 {
                    "GET"
                } else {
                    "CONNECT"
                }
                .into(),
                version,
                scheme: "https".into(),
                path: "/echo".into(),
                raw_path: b"/echo".to_vec(),
                headers: vec![(b"host".to_vec(), b"example.test".to_vec())],
                upgrade: true,
                ..Default::default()
            }
        },
    )?;
    let (mut output, mut input, reply, _) = pending.split();
    let head = reply.head().await?;
    // The edge translates this internal acknowledgment to the native response.
    if head.status != 101 {
        return Err(format!("session rejected: {}", head.status).into());
    }
    for message in [
        Message::Text("hello from Link".into()),
        Message::Binary(
            (0..300_000).map(|index| (index % 256) as u8).collect(),
        ),
    ] {
        write_message(&mut output, &message).await?;
        if read_message(&mut input).await? != Some(message) {
            return Err("message did not roundtrip".into());
        }
    }
    let close = Message::Close { code: 1000, reason: "done".into() };
    write_message(&mut output, &close).await?;
    output.shutdown().await?;
    if read_message(&mut input).await? != Some(close) {
        return Err("close did not roundtrip".into());
    }
    if read_message(&mut input).await?.is_some() {
        return Err("expected stream FIN".into());
    }
    Ok(())
}
