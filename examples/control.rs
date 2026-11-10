//! Send the edge's control commands to a JavaScript application without a
//! running Claviron server: `start`, `stop` and `restart` go to the
//! application by name, as the linked-application manager sends them; any
//! other name is forwarded as an application command with `payload`.
//!
//! ```sh
//! cargo run --example control -- <fleet> <name> <command> [<payload>]
//! ```

use claviron::orbit::link::application::{
    ApplicationClient, ControlCommand, ControlTarget,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let usage = "usage: control <fleet> <name> <command> [<payload>]";
    let fleet = arguments.next().ok_or(usage)?;
    let name = arguments.next().ok_or(usage)?;
    let command = arguments.next().ok_or(usage)?;
    let payload = arguments.next().unwrap_or_default();

    let target =
        matches!(command.as_str(), "start" | "stop" | "restart").then(|| {
            ControlTarget { runtime: "js".to_owned(), name: name.clone() }
        });
    let client = ApplicationClient::join(&fleet, "claviron-js-control", 1)?;
    let sent = client.command(&ControlCommand {
        origin: Some(client.lane() as u16),
        application: name.clone(),
        name: command.clone(),
        payload: payload.into_bytes(),
        target,
    });
    client.leave();
    sent?;
    println!("→ '{command}' accepted by '{name}' in fleet '{fleet}'");
    Ok(())
}
