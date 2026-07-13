use crate::MachineConfig;
use dashmap::DashMap;
use futures::StreamExt;
use log::{debug, error, info, warn};
use matrix_sdk::{
    Client,
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    encryption::verification::{
        Emoji, SasState, SasVerification, Verification, VerificationRequest,
        VerificationRequestState, format_emojis,
    },
    room::Room,
    ruma::{
        UserId,
        api::client::filter::FilterDefinition,
        events::{
            Mentions,
            key::verification::request::ToDeviceKeyVerificationRequestEvent,
            room::message::{OriginalSyncRoomMessageEvent, RoomMessageEventContent},
        },
    },
};
use qslib::{
    com::{CommandError, QSConnection, SendCommandError},
    commands::{
        AccessLevel, CommandBuilder, PossibleRunProgress, PowerStatus, QuickStatusQuery,
        ReceiveOkResponseError,
    },
    parser::{ErrorResponse, LogMessage},
};
use serde::{Deserialize, Serialize};
use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};
use thiserror::Error;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// The data needed to re-build a client.
#[derive(Debug, Serialize, Deserialize)]
struct ClientSession {
    /// The URL of the homeserver of the user.
    homeserver: String,

    /// The path of the database.
    db_path: PathBuf,

    /// The passphrase of the database.
    passphrase: String,
}

/// The full session to persist.
#[derive(Debug, Serialize, Deserialize)]
struct FullSession {
    /// The data to re-build the client.
    client_session: ClientSession,

    /// The Matrix user session.
    user_session: MatrixSession,

    /// The latest sync token.
    ///
    /// It is only needed to persist it when using `Client::sync_once()` and we
    /// want to make our syncs faster by not receiving all the initial sync
    /// again.
    #[serde(skip_serializing_if = "Option::is_none")]
    sync_token: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MatrixSettings {
    pub password: String,
    pub user: String,
    pub rooms: Vec<String>,
    pub host: String,
    pub session_file: PathBuf,
    pub allow_verification: bool,
    pub allow_commands: bool,
    pub allow_control: bool,
}

fn get_commands_list() -> String {
    "<b>Available commands:</b><ul>\
    <li><code>!status [machine]</code> - Show status of machine(s)</li>\
    <li><code>!protocol &lt;machine&gt;</code> - Show currently running protocol</li>\
    <li><code>!command &lt;machine&gt; &lt;command&gt;</code> - Send a command to a machine</li>\
    <li><code>!close &lt;machine&gt;</code> - Close drawer and lower cover (requires control)</li>\
    <li><code>!open &lt;machine&gt;</code> - Open drawer (requires control)</li>\
    <li><code>!abort &lt;machine&gt;</code> - Abort current run (requires control)</li>\
    <li><code>!stop &lt;machine&gt;</code> - Stop current run (requires control)</li>\
    <li><code>!power &lt;machine&gt; [on|off]</code> - Show or set machine power (control required for on/off)</li>\
    <li><code>!help [command]</code> - Show this help or detailed help for a command</li>\
    </ul>Use <code>!help &lt;command&gt;</code> for detailed documentation."
        .to_string()
}

fn get_command_help(command: &str) -> String {
    match command.to_lowercase().as_str() {
        "status" => {
            "<b>!status [machine]</b><br>\
            Shows the status of one or all machines.<br>\
            <br>\
            <b>Usage:</b><br>\
            • <code>!status</code> - Show status of all machines<br>\
            • <code>!status &lt;machine&gt;</code> - Show status of a specific machine<br>\
            <br>\
            <b>Information shown:</b><br>\
            • Power status (on/off)<br>\
            • Cover heat status and temperature<br>\
            • Current run progress (if running) or idle status".to_string()
        }
        "command" => {
            "<b>!command &lt;machine&gt; &lt;command&gt;</b><br>\
            Sends a raw command to the specified machine and returns the response.<br>\
            <br>\
            <b>Usage:</b><br>\
            <code>!command &lt;machine&gt; &lt;command&gt;</code><br>\
            <br>\
            <b>Example:</b><br>\
            <code>!command qs1 GETRUNINFO</code><br>\
            <br>\
            The command is parsed and sent to the machine. The access level is temporarily \
            elevated to Controller for the command execution.".to_string()
        }
        "close" => {
            "<b>!close &lt;machine&gt;</b><br>\
            Closes the drawer and lowers the cover on the specified machine.<br>\
            <br>\
            <b>Usage:</b><br>\
            <code>!close &lt;machine&gt;</code><br>\
            <br>\
            <b>Note:</b> This command requires control permissions to be enabled.".to_string()
        }
        "open" => {
            "<b>!open &lt;machine&gt;</b><br>\
            Opens the drawer on the specified machine.<br>\
            <br>\
            <b>Usage:</b><br>\
            <code>!open &lt;machine&gt;</code><br>\
            <br>\
            <b>Note:</b> This command requires control permissions to be enabled.".to_string()
        }
        "abort" => {
            "<b>!abort &lt;machine&gt;</b><br>\
            Aborts the currently running experiment on the specified machine.<br>\
            <br>\
            <b>Usage:</b><br>\
            <code>!abort &lt;machine&gt;</code><br>\
            <br>\
            <b>Note:</b> This command requires control permissions to be enabled. \
            The access level is temporarily elevated to Controller for the operation.".to_string()
        }
        "stop" => {
            "<b>!stop &lt;machine&gt;</b><br>\
            Stops the currently running experiment on the specified machine.<br>\
            <br>\
            <b>Usage:</b><br>\
            <code>!stop &lt;machine&gt;</code><br>\
            <br>\
            <b>Note:</b> This command requires control permissions to be enabled. \
            The access level is temporarily elevated to Controller for the operation.".to_string()
        }
        "protocol" => {
            "<b>!protocol &lt;machine&gt;</b><br>\
            Shows the currently running protocol for the specified machine.<br>\
            <br>\
            <b>Usage:</b><br>\
            <code>!protocol &lt;machine&gt;</code><br>\
            <br>\
            <b>Example:</b><br>\
            <code>!protocol qs1</code><br>\
            <br>\
            Displays the protocol in the same human-readable form as qslib's Python \
            interface: name, sample volume, run mode, default filters, and each stage's \
            steps with temperatures, durations, cycling, increments, and collection settings.<br>\
            <br>\
            If a run is in progress, the current stage and step are highlighted and the \
            current cycle is shown next to the stage.".to_string()
        }
        "power" => {
            "<b>!power &lt;machine&gt; [on|off]</b><br>\
            Shows or controls the machine's operational power (lamp, temperature control, etc.).<br>\
            <br>\
            <b>Usage:</b><br>\
            • <code>!power &lt;machine&gt;</code> - Show current power status<br>\
            • <code>!power &lt;machine&gt; on</code> - Turn power on<br>\
            • <code>!power &lt;machine&gt; off</code> - Turn power off<br>\
            <br>\
            <b>Examples:</b><br>\
            <code>!power qs1</code> - Check power status<br>\
            <code>!power qs1 on</code> - Turn power on<br>\
            <code>!power qs1 off</code> - Turn power off<br>\
            <br>\
            <b>Note:</b> Changing power (on/off) requires control permissions to be enabled. \
            Turning power off does not turn off the machine itself, just powers down the lamp and \
            temperature control systems. It will do so even if there is currently a run. \
            The access level is temporarily elevated to Controller for the operation.".to_string()
        }
        "help" => {
            "<b>!help [command]</b><br>\
            Shows help information for commands.<br>\
            <br>\
            <b>Usage:</b><br>\
            • <code>!help</code> - Show list of all available commands<br>\
            • <code>!help &lt;command&gt;</code> - Show detailed documentation for a specific command<br>\
            <br>\
            <b>Example:</b><br>\
            <code>!help status</code>".to_string()
        }
        _ => {
            format!(
                "Unknown command: <code>{}</code><br>\
                Use <code>!help</code> to see available commands.",
                command
            )
        }
    }
}

async fn handle_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    qs: &Arc<DashMap<String, (Arc<QSConnection>, MachineConfig)>>,
    settings: &MatrixSettings,
    all_machines: &[MachineConfig],
) -> Result<(), MatrixError> {
    let msg = event.content.body().to_lowercase();
    debug!("Received message: {}", msg);

    let mut parts = msg.split_whitespace();
    let command = parts.next().unwrap_or("");

    match command {
        "!status" => {
            let machine = parts.next();
            match machine {
                Some(m) => match qs.get(m) {
                    Some(x) => {
                        let (conn, _) = x.value();
                        let mut v = match QuickStatusQuery.send(conn).await {
                            Ok(v) => v,
                            Err(e) => {
                                error!("error getting status: {}", e);
                                send_matrix_message(
                                    &room,
                                    &format!("error getting status: {}", e),
                                    true,
                                )
                                .await?;
                                return Ok(());
                            }
                        };
                        let v = match v.receive_response().await {
                            Ok(v) => v,
                            Err(e) => {
                                error!("error getting status: {}", e);
                                send_matrix_message(
                                    &room,
                                    &format!("error getting status: {}", e),
                                    true,
                                )
                                .await?;
                                return Ok(());
                            }
                        };
                        match v {
                            Ok(v) => send_matrix_message(&room, &v.to_html(), false).await?,
                            Err(e) => error!("Error getting status: {}", e),
                        }
                    }
                    None => {
                        error!("Machine {} not found", m);
                        send_matrix_message(&room, &format!("Machine {} not found", m), true)
                            .await?;
                    }
                },
                None => {
                    // Get all configured machine names, sorted
                    let mut all_machine_names: Vec<_> =
                        all_machines.iter().map(|m| m.name.clone()).collect();
                    all_machine_names.sort();

                    let mut statuses = "<ul>".to_string();
                    for machine_name in all_machine_names {
                        statuses.push_str(&format!("<li><span>{}</span>: ", machine_name));

                        match qs.get(&machine_name) {
                            Some(item) => {
                                let (conn, _) = item.value();
                                let mut v = match QuickStatusQuery.send(conn).await {
                                    Ok(v) => v,
                                    Err(e) => {
                                        error!("error getting status: {}", e);
                                        statuses.push_str(&format!(
                                            "<span style='color: red;'>error getting status: {}.</span>",
                                            e
                                        ));
                                        statuses.push_str("</li>");
                                        continue;
                                    }
                                };

                                let v = match v.receive_response().await {
                                    Ok(v) => v,
                                    Err(e) => {
                                        error!("error getting status: {}", e);
                                        statuses.push_str(&format!(
                                            "<span style='color: red;'>error getting status: {}.</span>",
                                            e
                                        ));
                                        statuses.push_str("</li>");
                                        continue;
                                    }
                                };
                                match v {
                                    Ok(v) => {
                                        let runmsg = match v.runprogress {
                                            PossibleRunProgress::Running(p) => format!(
                                                "running {} (stage {}, cycle {}, step {}).",
                                                p.run_title, p.stage, p.cycle, p.step
                                            ),
                                            PossibleRunProgress::NotRunning(_) => {
                                                "idle.".to_string()
                                            }
                                        };
                                        statuses.push_str("power ");
                                        match v.power {
                                            PowerStatus::On => statuses.push_str("on"),
                                            PowerStatus::Off => statuses.push_str("off"),
                                        };
                                        match v.cover.on && v.power == PowerStatus::On {
                                            true => statuses.push_str(&format!(
                                                ", cover heat on at {:.1}°C",
                                                v.cover.temperature
                                            )),
                                            false => statuses.push_str(", cover heat off"),
                                        };
                                        statuses.push_str(&format!(", {}", runmsg));
                                    }
                                    Err(e) => {
                                        error!("error getting status: {}", e);
                                        statuses.push_str(&format!(
                                            "<span style='color: red;'>error getting status: {}.</span>",
                                            e
                                        ));
                                    }
                                }
                            }
                            None => {
                                statuses
                                    .push_str("<span style='color: orange;'>not connected.</span>");
                            }
                        }
                        statuses.push_str("</li>");
                    }
                    statuses.push_str("</ul>");
                    send_matrix_message(&room, &statuses, false).await?;
                }
            }
            Ok(())
        }
        "!command" => {
            let machine = parts.next().unwrap_or("");
            let commandstring = parts.collect::<Vec<&str>>().join(" ");

            let command = match qslib::parser::Command::parse(&mut commandstring.as_bytes()) {
                Ok(c) => c,
                Err(e) => {
                    error!("Error parsing command: {}", e);
                    return Ok(());
                }
            };

            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    conn.set_access_level(AccessLevel::Controller).await?;
                    let response = conn.send_command(command).await?.get_response().await?;
                    match response {
                        Ok(response) => {
                            send_matrix_message(&room, &response.to_string(), false).await?
                        }
                        Err(e) => {
                            send_matrix_message(&room, &format!("Error: {}", e), true).await?
                        }
                    }
                    conn.set_access_level(AccessLevel::Observer).await?;
                }
                None => error!("Machine {} not found", machine),
            }

            Ok(())
        }
        "!close" => {
            if !settings.allow_control {
                error!("Control commands not allowed");
                return Ok(());
            }
            let machine = parts.next().unwrap_or("");
            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    // Close drawer
                    match conn.send_command("CLOSE").await?.get_response().await? {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error closing drawer: {}", e);
                            return Ok(());
                        }
                    }
                    // Lower cover
                    match conn.send_command("COVerDOWN").await?.get_response().await? {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Drawer closed, but error lowering cover: {}", e);
                            return Ok(());
                        }
                    }
                    send_matrix_message(&room, "Drawer closed and cover lowered", false).await?;
                }
                None => error!("Machine {} not found", machine),
            }
            Ok(())
        }
        "!open" => {
            if !settings.allow_control {
                error!("Control commands not allowed");
                return Ok(());
            }
            let machine = parts.next().unwrap_or("");
            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    match conn.send_command("OPEN").await?.get_response().await? {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error opening drawer: {}", e);
                            return Ok(());
                        }
                    }
                    send_matrix_message(&room, "Drawer opened", false).await?;
                }
                None => error!("Machine {} not found", machine),
            }
            Ok(())
        }
        "!abort" => {
            if !settings.allow_control {
                error!("Control commands not allowed");
                return Ok(());
            }
            let machine = parts.next().unwrap_or("");
            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    conn.set_access_level(AccessLevel::Controller).await?;
                    let response = conn.abort_current_run().await?;
                    conn.set_access_level(AccessLevel::Observer).await?;
                    match response {
                        Ok(_) => {
                            send_matrix_message(&room, "Run aborted", false).await?;
                        }
                        Err(e) => {
                            send_matrix_message(&room, &format!("Error aborting run: {}", e), true)
                                .await?;
                        }
                    }
                }
                None => {
                    error!("Machine {} not found", machine);
                    send_matrix_message(&room, &format!("Machine {} not found", machine), true)
                        .await?;
                }
            }
            Ok(())
        }
        "!stop" => {
            if !settings.allow_control {
                error!("Control commands not allowed");
                return Ok(());
            }
            let machine = parts.next().unwrap_or("");
            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    conn.set_access_level(AccessLevel::Controller).await?;
                    let response = conn.stop_current_run().await?;
                    conn.set_access_level(AccessLevel::Observer).await?;
                    match response {
                        Ok(_) => {
                            send_matrix_message(&room, "Run stopped", false).await?;
                        }
                        Err(e) => {
                            send_matrix_message(&room, &format!("Error stopping run: {}", e), true)
                                .await?;
                        }
                    }
                }
                None => {
                    error!("Machine {} not found", machine);
                    send_matrix_message(&room, &format!("Machine {} not found", machine), true)
                        .await?;
                }
            }
            Ok(())
        }
        "!power" => {
            let machine = parts.next().unwrap_or("");
            let action = parts.next();

            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    match action {
                        None => {
                            // Query power status
                            let response = conn.send_command("POW?").await?.get_response().await?;
                            match response {
                                Ok(resp) => {
                                    send_matrix_message(
                                        &room,
                                        &format!("Power status for {}: {}", machine, resp),
                                        false,
                                    )
                                    .await?;
                                }
                                Err(e) => {
                                    send_matrix_message(
                                        &room,
                                        &format!("Error getting power status: {}", e),
                                        true,
                                    )
                                    .await?;
                                }
                            }
                        }
                        Some("on") | Some("off") => {
                            if !settings.allow_control {
                                error!("Control commands not allowed");
                                send_matrix_message(
                                    &room,
                                    "Control commands are not allowed",
                                    true,
                                )
                                .await?;
                                return Ok(());
                            }
                            conn.set_access_level(AccessLevel::Controller).await?;
                            let response = conn
                                .send_command(format!("POW {}", action.unwrap()))
                                .await?
                                .get_response()
                                .await?;
                            conn.set_access_level(AccessLevel::Observer).await?;
                            match response {
                                Ok(_) => {
                                    send_matrix_message(
                                        &room,
                                        &format!(
                                            "Power turned {} for {}",
                                            action.unwrap(),
                                            machine
                                        ),
                                        false,
                                    )
                                    .await?;
                                }
                                Err(e) => {
                                    send_matrix_message(
                                        &room,
                                        &format!("Error changing power: {}", e),
                                        true,
                                    )
                                    .await?;
                                }
                            }
                        }
                        Some(invalid) => {
                            send_matrix_message(
                                &room,
                                &format!("Invalid power action: {}. Use 'on' or 'off'", invalid),
                                true,
                            )
                            .await?;
                        }
                    }
                }
                None => {
                    error!("Machine {} not found", machine);
                    send_matrix_message(&room, &format!("Machine {} not found", machine), true)
                        .await?;
                }
            }
            Ok(())
        }
        "!protocol" => {
            let machine = match parts.next() {
                Some(m) => m,
                None => {
                    error!("No machine specified");
                    send_matrix_message(&room, "No machine specified", true).await?;
                    return Ok(());
                }
            };
            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    match conn.get_running_protocol().await {
                        Ok(protocol) => {
                            let (cs, ct, cc) = current_run_position(conn).await;
                            let (plain, html) = render_protocol(&protocol, cs, ct, cc);
                            send_matrix_message_with_plain(&room, &plain, &html, false).await?;
                        }
                        Err(e) => {
                            error!("Error getting protocol: {}", e);
                            send_matrix_message(
                                &room,
                                &format!("Error getting protocol: {}", e),
                                true,
                            )
                            .await?;
                        }
                    }
                }
                None => {
                    error!("Machine {} not found", machine);
                    send_matrix_message(&room, &format!("Machine {} not found.", machine), true)
                        .await?;
                }
            }
            Ok(())
        }
        "!help" => {
            let command_name = parts.next();
            match command_name {
                Some(cmd) => {
                    let cmd = cmd.strip_prefix('!').unwrap_or(cmd);
                    let help_text = get_command_help(cmd);
                    send_matrix_message(&room, &help_text, false).await?;
                }
                None => {
                    let help_text = get_commands_list();
                    send_matrix_message(&room, &help_text, false).await?;
                }
            }
            Ok(())
        }
        m if m.starts_with("!") => {
            error!("Unknown command: {} from {}", m, event.sender);
            send_matrix_message(&room, &format!("Unknown command: {}", m), true).await?;
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn wait_for_confirmation(sas: SasVerification, emoji: [Emoji; 7]) {
    println!("\nDo the emojis match: \n{}", format_emojis(emoji));
    print!("Confirm with `yes` or cancel with `no`: ");
    std::io::stdout()
        .flush()
        .expect("We should be able to flush stdout");

    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .expect("error: unable to read user input");

    match input.trim().to_lowercase().as_ref() {
        "yes" | "true" | "ok" => sas.confirm().await.unwrap(),
        _ => sas.cancel().await.unwrap(),
    }
}

async fn print_devices(user_id: &UserId, client: &Client) {
    println!("Devices of user {user_id}");

    for device in client
        .encryption()
        .get_user_devices(user_id)
        .await
        .unwrap()
        .devices()
    {
        if device.device_id()
            == client
                .device_id()
                .expect("We should be logged in now and know our device id")
        {
            continue;
        }

        println!(
            "   {:<10} {:<30} {:<}",
            device.device_id(),
            device.display_name().unwrap_or("-"),
            if device.is_verified() { "✅" } else { "❌" }
        );
    }
}

async fn sas_verification_handler(client: Client, sas: SasVerification) {
    println!(
        "Starting verification with {} {}",
        &sas.other_device().user_id(),
        &sas.other_device().device_id()
    );
    print_devices(sas.other_device().user_id(), &client).await;
    sas.accept().await.unwrap();

    let mut stream = sas.changes();

    while let Some(state) = stream.next().await {
        match state {
            SasState::KeysExchanged {
                emojis,
                decimals: _,
            } => {
                tokio::spawn(wait_for_confirmation(
                    sas.clone(),
                    emojis
                        .expect("We only support verifications using emojis")
                        .emojis,
                ));
            }
            SasState::Done { .. } => {
                let device = sas.other_device();

                println!(
                    "Successfully verified device {} {} {:?}",
                    device.user_id(),
                    device.device_id(),
                    device.local_trust_state()
                );

                print_devices(sas.other_device().user_id(), &client).await;

                break;
            }
            SasState::Cancelled(cancel_info) => {
                println!(
                    "The verification has been cancelled, reason: {}",
                    cancel_info.reason()
                );

                break;
            }
            SasState::Created { .. }
            | SasState::Started { .. }
            | SasState::Accepted { .. }
            | SasState::Confirmed => (),
        }
    }
}

async fn request_verification_handler(client: Client, request: VerificationRequest) {
    println!(
        "Accepting verification request from {}",
        request.other_user_id(),
    );
    request
        .accept()
        .await
        .expect("Can't accept verification request");

    let mut stream = request.changes();

    while let Some(state) = stream.next().await {
        match state {
            VerificationRequestState::Created { .. }
            | VerificationRequestState::Requested { .. }
            | VerificationRequestState::Ready { .. } => (),
            VerificationRequestState::Transitioned { verification } => {
                // We only support SAS verification.
                if let Verification::SasV1(s) = verification {
                    tokio::spawn(sas_verification_handler(client, s));
                    break;
                }
            }
            VerificationRequestState::Done | VerificationRequestState::Cancelled(_) => break,
        }
    }
}

async fn send_matrix_message(
    room: &Room,
    message: &str,
    important: bool,
) -> Result<(), matrix_sdk::Error> {
    let mut content = RoomMessageEventContent::text_html(message, message);
    if important {
        content.mentions = Some(Mentions::with_room_mention());
    }
    room.send(content).await?;
    Ok(())
}

/// Escape a string for inclusion in Matrix HTML message bodies.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Render a running protocol into (plain_text, html) bodies for Matrix.
///
/// `current_stage`/`current_step`/`current_cycle` are the machine's 1-based
/// position (from run status); when given, the current stage and step are
/// highlighted and the current cycle is noted on the stage. Pass `None` for an
/// idle machine.
///
/// The plain body is qslib's flat line rendering (the same human-readable form
/// as the Python interface), with a `⟵` marker on the current stage/step. The
/// HTML body is a proper nested list, with the current stage/step in bold.
fn render_protocol(
    protocol: &qslib::protocol::Protocol,
    current_stage: Option<i64>,
    current_step: Option<i64>,
    current_cycle: Option<i64>,
) -> (String, String) {
    let mut plain = String::new();
    for (i, line) in protocol
        .info_lines(current_stage, current_step, current_cycle)
        .iter()
        .enumerate()
    {
        if i > 0 {
            plain.push('\n');
        }
        plain.push_str(&line.text);
        if line.current {
            plain.push_str("  ⟵ current");
        }
    }

    let html = protocol_view_html(&protocol.view(current_stage, current_step, current_cycle));
    (plain, html)
}

/// Escape `text` and wrap it in `<b>…</b>` when `current`.
fn html_span(text: &str, current: bool) -> String {
    let escaped = html_escape(text);
    if current {
        format!("<b>{}</b>", escaped)
    } else {
        escaped
    }
}

/// Render a [`qslib::protocol::ProtocolView`] as a nested HTML list for Matrix.
///
/// Stages are an ordered list; a stage with a single step collapses onto its
/// line, otherwise its steps are a nested ordered list. The current stage and
/// step are shown in bold.
fn protocol_view_html(view: &qslib::protocol::ProtocolView) -> String {
    let mut html = String::new();

    html.push_str("<p><b>Run Protocol ");
    html.push_str(&html_escape(&view.name));
    html.push_str("</b>");
    if !view.details.is_empty() {
        html.push_str(" — ");
        html.push_str(&html_escape(&view.details.join(", ")));
    }
    html.push_str("</p>");

    if let Some(df) = &view.default_filters {
        html.push_str("<p><i>Default filters: ");
        html.push_str(&html_escape(df));
        html.push_str("</i></p>");
    }

    html.push_str("<ol>");
    for stage in &view.stages {
        html.push_str("<li>");

        let mut head = stage.summary.clone();
        if let Some(cycle) = &stage.cycle {
            head.push_str(" — ");
            head.push_str(cycle);
        }
        html.push_str(&html_span(&head, stage.current));

        if stage.steps.len() == 1 {
            // Collapse a single step onto the stage line.
            html.push_str(": ");
            html.push_str(&html_span(&stage.steps[0].text, stage.steps[0].current));
        } else if !stage.steps.is_empty() {
            html.push_str("<ol>");
            for step in &stage.steps {
                html.push_str("<li>");
                html.push_str(&html_span(&step.text, step.current));
                html.push_str("</li>");
            }
            html.push_str("</ol>");
        }

        html.push_str("</li>");
    }
    html.push_str("</ol>");

    html
}

/// Query the machine's current run position as 1-based `(stage, step, cycle)`.
///
/// Returns `(None, None, None)` when the machine is idle or on any error, so
/// protocol rendering degrades gracefully to an unmarked listing.
async fn current_run_position(conn: &Arc<QSConnection>) -> (Option<i64>, Option<i64>, Option<i64>) {
    let mut query = match QuickStatusQuery.send(conn).await {
        Ok(q) => q,
        Err(_) => return (None, None, None),
    };
    match query.receive_response().await {
        Ok(Ok(status)) => match status.runprogress {
            PossibleRunProgress::Running(rp) => (
                rp.stage.parse().ok(),
                rp.step.parse().ok(),
                rp.cycle.parse().ok(),
            ),
            PossibleRunProgress::NotRunning(_) => (None, None, None),
        },
        _ => (None, None, None),
    }
}

/// Send a message with distinct plain-text and HTML bodies. Used when the HTML
/// body carries markup (e.g. a `<pre>` block) that would be noise as plain text.
async fn send_matrix_message_with_plain(
    room: &Room,
    plain: &str,
    html: &str,
    important: bool,
) -> Result<(), matrix_sdk::Error> {
    let mut content = RoomMessageEventContent::text_html(plain, html);
    if important {
        content.mentions = Some(Mentions::with_room_mention());
    }
    room.send(content).await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum MatrixError {
    #[error("Matrix error: {0}")]
    MatrixErr(#[from] matrix_sdk::Error),
    #[error("Matrix error: {0}")]
    EchoErr(#[from] CommandError<ErrorResponse>),
    #[error("Sending error: {0}")]
    SendErr(#[from] SendCommandError),
    #[error("Receiving error: {0}")]
    ReceiveErr(#[from] ReceiveOkResponseError),
    #[error("Client build error: {0}")]
    ClientBuildError(#[from] matrix_sdk::ClientBuildError),
    #[error("Room ID parse error: {0}")]
    RoomIdParseError(#[from] matrix_sdk::IdParseError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

async fn persist_sync_token(
    session_file: &PathBuf,
    sync_token: String,
) -> Result<(), matrix_sdk::Error> {
    let serialized_session = std::fs::read_to_string(session_file)?;
    let mut full_session: FullSession = serde_json::from_str(&serialized_session)?;

    full_session.sync_token = Some(sync_token);
    let serialized_session = serde_json::to_string(&full_session)?;
    std::fs::write(session_file, serialized_session)?;

    Ok(())
}

pub async fn setup_matrix(
    settings: &MatrixSettings,
    qs_connections: Arc<DashMap<String, (Arc<QSConnection>, MachineConfig)>>,
    all_machines: Vec<MachineConfig>,
) -> Result<(), MatrixError> {
    debug!(
        "Setting up Matrix client with homeserver: {}",
        settings.host
    );

    let (client, sync_token) = if settings.session_file.exists() {
        debug!("Found existing session file at {:?}", settings.session_file);
        let session_data = std::fs::read(settings.session_file.clone()).map_err(|e| {
            MatrixError::IoError(std::io::Error::other(format!(
                "Failed to read session file: {}",
                e
            )))
        })?;
        let session: FullSession = serde_json::from_slice(&session_data).map_err(|e| {
            MatrixError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse session file: {}", e),
            ))
        })?;
        debug!(
            "Restoring session for homeserver: {}",
            session.client_session.homeserver
        );
        let client = Client::builder()
            .homeserver_url(&session.client_session.homeserver)
            .sqlite_store(
                session.client_session.db_path.clone(),
                Some(&settings.password),
            )
            .build()
            .await?;
        debug!("Restoring user session");
        client.restore_session(session.user_session).await?;
        debug!("Successfully restored existing session");
        (client, session.sync_token)
    } else {
        debug!("No existing session found, creating new session");
        let db_path = PathBuf::from(format!("{}.db", settings.session_file.to_string_lossy()));
        debug!("Creating new database at {:?}", db_path);
        let client = Client::builder()
            .homeserver_url(&settings.host)
            .sqlite_store(&db_path, Some(&settings.password))
            .build()
            .await?;

        debug!("Logging in as user {}", settings.user);
        client
            .matrix_auth()
            .login_username(&settings.user, &settings.password)
            .initial_device_display_name("qpcrbot")
            .await?;

        debug!("Creating new session file");
        let client_session = ClientSession {
            homeserver: settings.host.clone(),
            db_path,
            passphrase: settings.password.clone(),
        };
        let user_session = client.matrix_auth().session().ok_or_else(|| {
            MatrixError::IoError(std::io::Error::other("No session available after login"))
        })?;
        let serialized_session = serde_json::to_string(&FullSession {
            client_session,
            user_session,
            sync_token: None,
        })
        .map_err(|e| {
            MatrixError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize session: {}", e),
            ))
        })?;
        std::fs::write(&settings.session_file, serialized_session).map_err(|e| {
            MatrixError::IoError(std::io::Error::other(format!(
                "Failed to write session file: {}",
                e
            )))
        })?;
        debug!("Successfully created new session");
        (client, None)
    };

    debug!("Setting up sync filter with lazy loading");
    let filter = FilterDefinition::with_lazy_loading();

    let mut sync_settings = SyncSettings::default().filter(filter.into());
    if let Some(sync_token) = sync_token.as_ref() {
        debug!("Using existing sync token: {}", sync_token);
        sync_settings = sync_settings.token(sync_token);
    }

    debug!("Performing initial sync");
    let response = client.sync_once(sync_settings.clone()).await?;
    // sync_settings = sync_settings.token(response.next_batch.clone());

    debug!("Persisting sync token");
    persist_sync_token(&settings.session_file, response.next_batch.clone()).await?;

    for room in settings.rooms.iter() {
        debug!("Joining room {}", room);
        let room_id = matrix_sdk::ruma::RoomId::parse(room)?;
        client.join_room_by_id(&room_id).await?;
    }

    let log_room = client
        .get_room(&matrix_sdk::ruma::RoomId::parse(&settings.rooms[0])?)
        .ok_or_else(|| {
            MatrixError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Room {} not found after joining", settings.rooms[0]),
            ))
        })?;
    debug!("Successfully joined room");

    let _client_clone = client.clone();

    debug!("Setting up message event handler");

    let qs_connections_message_handler = qs_connections.clone();
    if settings.allow_commands {
        let settings_clone = settings.clone();
        let all_machines_clone = all_machines.clone();
        client.add_event_handler(
            move |event: OriginalSyncRoomMessageEvent, room: Room| async move {
                if let Err(e) = handle_message(
                    event,
                    room,
                    &qs_connections_message_handler,
                    &settings_clone,
                    &all_machines_clone,
                )
                .await
                {
                    error!("Error handling message: {:#}", e);
                }
            },
        );
    }

    if settings.allow_verification {
        client.add_event_handler(
            |ev: ToDeviceKeyVerificationRequestEvent, client: Client| async move {
                let request = match client
                    .encryption()
                    .get_verification_request(&ev.sender, &ev.content.transaction_id)
                    .await
                {
                    Some(req) => req,
                    None => {
                        error!("Verification request object wasn't created");
                        return;
                    }
                };

                tokio::spawn(request_verification_handler(client, request));
            },
        );
    }

    debug!("Starting sync loop");
    let matrix_client = client.clone();
    let (sync_error_tx, mut sync_error_rx) = tokio::sync::mpsc::unbounded_channel::<MatrixError>();

    tokio::spawn(async move {
        if let Err(e) = matrix_client
            .sync(SyncSettings::default().token(response.next_batch))
            .await
        {
            error!("Matrix sync loop error: {}", e);
            let _ = sync_error_tx.send(MatrixError::MatrixErr(e));
        }
    });

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(String, LogMessage)>();

    let qs_connections_for_tasks = qs_connections.clone();

    for x in qs_connections.iter() {
        let name = x.value().1.name.clone();
        let tx = tx.clone();
        let qs_connections_clone = qs_connections_for_tasks.clone();
        let task_name = name.clone();
        tokio::spawn(async move {
            let mut backoff_secs = 1u64;
            const MAX_BACKOFF_SECS: u64 = 60;
            let mut last_conn_id = None::<usize>;
            loop {
                let current_conn = qs_connections_clone.get(&task_name);
                let (conn, conn_id) = match current_conn {
                    Some(entry) => {
                        let conn = entry.value().0.clone();
                        let conn_id = Arc::as_ptr(&conn) as usize;
                        (conn, conn_id)
                    }
                    None => {
                        debug!("Machine {} not in connections map, waiting", task_name);
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        last_conn_id = None;
                        continue;
                    }
                };

                if Some(conn_id) == last_conn_id {
                    debug!(
                        "Machine {} connection unchanged, waiting for new connection",
                        task_name
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                last_conn_id = Some(conn_id);
                backoff_secs = 1;
                let mut inner_sm = conn.subscribe_log(&["Run", "Error"]).await;
                debug!("Subscribed to log stream for machine {}", task_name);

                loop {
                    match inner_sm.next().await {
                        Some((topic, Ok(msg))) => {
                            if (topic == "Run" || topic == "Error")
                                && let Err(e) = tx.send((task_name.clone(), msg))
                            {
                                error!("Failed to send message for {}: {}", task_name, e);
                                break;
                            }
                        }
                        Some((topic, Err(BroadcastStreamRecvError::Lagged(n)))) => {
                            warn!(
                                "Machine {} topic {} lagged by {} messages",
                                task_name, topic, n
                            );
                        }
                        None => {
                            warn!("Stream ended for machine {}, will reconnect", task_name);
                            last_conn_id = None;
                            break;
                        }
                    }
                }
            }
        });
    }

    info!("Matrix connected");

    loop {
        tokio::select! {
            msg_result = rx.recv() => {
                match msg_result {
                    Some((name, msg)) => {
                        handle_run_message(name, msg, &log_room).await;
                    }
                    None => {
                        warn!("Message channel closed, reconnecting");
                        break;
                    }
                }
            }
            sync_error = sync_error_rx.recv() => {
                if let Some(e) = sync_error {
                    error!("Matrix sync error received, reconnecting: {}", e);
                    return Err(e);
                } else {
                    warn!("Sync error channel closed, reconnecting");
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn handle_run_message(name: String, msg: LogMessage, room: &Room) {
    // We only want to notify if the message contains: "Error", "Ended", "Aborted", "Stopped", "Starting"
    if msg.message.contains("Error")
        || msg.message.contains("Ended")
        || msg.message.contains("Aborted")
        || msg.message.contains("Stopped")
        || msg.message.contains("Starting")
    {
        let mm = format!("{}: {}", name, msg.message);
        match send_matrix_message(room, &mm, true).await {
            Ok(_) => (),
            Err(e) => error!("Error sending message: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qslib::parser::Command;
    use qslib::protocol::Protocol;

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("a & b <c> \"d\""), "a &amp; b &lt;c&gt; \"d\"");
        assert_eq!(html_escape("plain text"), "plain text");
    }

    const MULTI_STAGE: &str = "PROT -volume=25 -runmode=standard qpcr <multiline.protocol>\n\tSTAGE 1 _HOLD <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 120\n\t\t</multiline.step>\n\t</multiline.stage>\n\tSTAGE -repeat=40 2 _CYCLE <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 15\n\t\t</multiline.step>\n\t\tSTEP 2 <multiline.step>\n\t\t\tRAMP 60\n\t\t\tHACFILT x1-m4\n\t\t\tHOLDANDCOLLECT 60\n\t\t</multiline.step>\n\t</multiline.stage>\n</multiline.protocol>";

    #[test]
    fn test_render_protocol_plain_matches_display() {
        let protocol_string = "PROTOCOL -volume=50.0 -runmode=standard test_protocol <multiline.protocol>\n\tSTAGE 1 _HOLD_1 <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 60.0 60.0 60.0 60.0 60.0 60.0\n\t\t\tHOLD 60\n\t\t</multiline.step>\n\t</multiline.stage>\n</multiline.protocol>";
        let cmd = Command::try_from(protocol_string).expect("parse command");
        let protocol = Protocol::from_scpicommand(&cmd).expect("parse protocol");

        let (plain, html) = render_protocol(&protocol, None, None, None);

        // The plain fallback is exactly qslib's Display output (Python-style).
        assert_eq!(plain, protocol.to_string());

        // The HTML is a proper nested list, not a <pre> block.
        assert!(!html.contains("<pre>"));
        assert!(html.contains("<ol>"));
        assert!(html.contains("<li>"));
        assert!(html.contains("<b>Run Protocol test_protocol</b>"));
    }

    #[test]
    fn test_render_protocol_highlights_current() {
        let cmd = Command::try_from(MULTI_STAGE).expect("parse command");
        let protocol = Protocol::from_scpicommand(&cmd).expect("parse protocol");

        // Current position: stage 2, step 2, cycle 12.
        let (plain, html) = render_protocol(&protocol, Some(2), Some(2), Some(12));

        // Cycle note on the current stage, in both bodies.
        assert!(plain.contains("cycle 12/40"));
        assert!(html.contains("<b>Stage with 40 cycles (total duration 50m) — cycle 12/40</b>"));

        // Current step: marked in plain text, bold in HTML.
        assert!(plain.contains("60.00°C for 60s/cycle (collects x1-m4)  ⟵ current"));
        assert!(html.contains("<b>60.00°C for 60s/cycle (collects x1-m4)</b>"));

        // Non-current step/stage is not marked.
        assert!(!plain.contains("15s/cycle  ⟵"));
        assert!(!html.contains("<b>95.00°C for 15s/cycle</b>"));
    }

    #[test]
    fn test_render_protocol_idle_no_marks() {
        let cmd = Command::try_from(MULTI_STAGE).expect("parse command");
        let protocol = Protocol::from_scpicommand(&cmd).expect("parse protocol");
        let (plain, html) = render_protocol(&protocol, None, None, None);
        assert!(!plain.contains("⟵"));
        // Only the title is bold; no stage or step is highlighted.
        assert_eq!(html.matches("<b>").count(), 1);
    }
}
