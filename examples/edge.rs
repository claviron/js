//! The edge's side of one exchange, for exercising a JavaScript application
//! without a running Claviron server: join the fleet as an edge, send a request
//! — with a body when one is given — and print the reply.
//!
//! ```sh
//! cargo run --example edge -- <fleet> <name> <path> [<body>]
//! ```

use claviron::orbit::link::application::{
    ApplicationClient, Exchange, HttpVersion,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let usage = "usage: edge <fleet> <name> <path> [<body>]";
    let fleet = arguments.next().ok_or(usage)?;
    let name = arguments.next().ok_or(usage)?;
    let target = arguments.next().ok_or(usage)?;
    let body = arguments.next();

    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_owned(), query.as_bytes().to_vec()),
        None => (target.clone(), Vec::new()),
    };
    let mut headers = vec![(b"host".to_vec(), b"example.test".to_vec())];
    if let Some(body) = &body {
        headers.push((
            b"content-length".to_vec(),
            body.len().to_string().into_bytes(),
        ));
        headers.push((b"content-type".to_vec(), b"text/plain".to_vec()));
    }
    let request = Exchange {
        method: if body.is_some() { "POST" } else { "GET" }.to_owned(),
        scheme: "http".to_owned(),
        version: HttpVersion::Http11,
        path: path.clone(),
        raw_path: path.into_bytes(),
        query,
        headers,
        client: Some(([192, 0, 2, 1], 54321).into()),
        server: Some(([192, 0, 2, 2], 443).into()),
        ..Default::default()
    };

    let client = ApplicationClient::join(&fleet, "claviron-js-edge", 1)?;
    if !client.serves(&name) {
        client.leave();
        return Err(
            format!("nothing serves '{name}' in fleet '{fleet}'").into()
        );
    }
    let pending = client.send(&name, request, body.is_some())?;
    let (mut request_body, mut response_body, reply) = pending.split();
    if let Some(body) = &body {
        request_body.write_all(body.as_bytes()).await?;
        request_body.shutdown().await?;
    }
    drop(request_body);

    let head = reply.head().await?;
    println!("← {}", head.status);
    let mut answer = Vec::new();
    response_body.read_to_end(&mut answer).await?;
    println!("{}", String::from_utf8_lossy(&answer));
    client.leave();
    Ok(())
}
