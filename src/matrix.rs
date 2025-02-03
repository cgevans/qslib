use dashmap::DashMap;
use futures::StreamExt;
use log::{debug, error, info, trace};
use matrix_sdk::{
    Client,
    config::SyncSettings,
    encryption::verification::{
        Emoji, SasState, SasVerification, Verification, VerificationRequest,
        VerificationRequestState, format_emojis,
    },
    matrix_auth::MatrixSession,
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
use qslib_rs::{com::{QSConnection, QSConnectionError}, commands::{QuickStatusQuery, CommandBuilder}, parser::LogMessage};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamMap;
use std::{
    io::Write, path::PathBuf, sync::Arc, time::{Duration, Instant}
};
use thiserror::Error;
use tokio::sync::broadcast::{self, Receiver};

use crate::MachineConfig;

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


async fn handle_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    qs: &Arc<DashMap<String,(Arc<QSConnection>, MachineConfig)>>,
    settings: &MatrixSettings,
) -> Result<(), MatrixError> {
    let msg = event.content.body().to_lowercase();
    debug!("Received message: {}", msg);

    let mut parts = msg.split_whitespace();
    let command = parts.next().unwrap_or("");

    match command {
        "!status" => {
            let machine = parts.next().unwrap_or("");
            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    let v = QuickStatusQuery.send(conn).await?.recv_response().await.unwrap();
                    send_matrix_message(&room, &v.to_string(), false).await?;
                }
                None => error!("Machine {} not found", machine),
            }
            Ok(())
        }
        "!command" => {
            let machine = parts.next().unwrap_or("");
            let commandstring = parts.collect::<Vec<&str>>().join(" ");

            let command = match qslib_rs::parser::Command::parse(&mut commandstring.as_bytes()) {
                Ok(c) => c,
                Err(e) => {
                    error!("Error parsing command: {}", e);
                    return Ok(());
                }
            };

            match qs.get(machine) {
                Some(x) => {
                    let (conn, _) = x.value();
                    let response = conn.send_command(command).await?.get_response().await?.unwrap();
                    send_matrix_message(&room, &response.to_string(), false).await?;
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
                    conn.send_command("CLOSE").await?.get_response().await?;
                    // Lower cover
                    conn.send_command("COVerDOWN").await?.get_response().await?;
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
                    conn.send_command("OPEN").await?.get_response().await?;
                    send_matrix_message(&room, "Drawer opened", false).await?;
                }
                None => error!("Machine {} not found", machine),
            }
            Ok(())
        }
        m if m.starts_with("!") => {
            error!("Unknown command: {}", m);
            send_matrix_message(&room, &format!("Unknown command: {}", m), true).await?;
            Ok(())
        }
        _ => {
            Ok(())
        }
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

#[derive(Debug, Error)]
pub enum MatrixError {
    #[error("Matrix error: {0}")]
    MatrixErr(#[from] matrix_sdk::Error),
    #[error("Matrix error: {0}")]
    EchoErr(#[from] QSConnectionError),
    #[error("Client build error: {0}")]
    ClientBuildError(#[from] matrix_sdk::ClientBuildError),
    #[error("Room ID parse error: {0}")]
    RoomIdParseError(#[from] matrix_sdk::IdParseError),
}

async fn persist_sync_token(session_file: &PathBuf, sync_token: String) -> Result<(), matrix_sdk::Error> {
    let serialized_session = std::fs::read_to_string(session_file)?;
    let mut full_session: FullSession = serde_json::from_str(&serialized_session)?;

    full_session.sync_token = Some(sync_token);
    let serialized_session = serde_json::to_string(&full_session)?;
    std::fs::write(session_file, serialized_session)?;

    Ok(())
}

pub async fn setup_matrix(
    settings: &MatrixSettings,
    qs_connections: Arc<DashMap<String,(Arc<QSConnection>, MachineConfig)>>,
) -> Result<(), MatrixError> {
    info!(
        "Setting up Matrix client with homeserver: {}",
        settings.host
    );

    let (client, sync_token) = if settings.session_file.exists() {
        info!("Found existing session file at {:?}", settings.session_file);
        let session: FullSession =
            serde_json::from_slice(&std::fs::read(settings.session_file.clone()).unwrap()).unwrap();
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
        info!("Successfully restored existing session");
        (client, session.sync_token)
    } else {
        info!("No existing session found, creating new session");
        let db_path = PathBuf::from(format!("{}.db", settings.session_file.to_string_lossy()));
        debug!("Creating new database at {:?}", db_path);
        let client = Client::builder()
            .homeserver_url(&settings.host)
            .sqlite_store(&db_path, Some(&settings.password))
            .build()
            .await?;

        info!("Logging in as user {}", settings.user);
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
        let user_session = client.matrix_auth().session().unwrap();
        let serialized_session = serde_json::to_string(&FullSession {
            client_session,
            user_session,
            sync_token: None,
        })
        .unwrap();
        std::fs::write(&settings.session_file, serialized_session).unwrap();
        info!("Successfully created new session");
        (client, None)
    };

    debug!("Setting up sync filter with lazy loading");
    let filter = FilterDefinition::with_lazy_loading();

    let mut sync_settings = SyncSettings::default().filter(filter.into());
    if let Some(sync_token) = sync_token.as_ref() {
        debug!("Using existing sync token: {}", sync_token);
        sync_settings = sync_settings.token(sync_token);
    }

    info!("Performing initial sync");
    let response = client.sync_once(sync_settings.clone()).await?;
    // sync_settings = sync_settings.token(response.next_batch.clone());

    debug!("Persisting sync token");
    persist_sync_token(&settings.session_file, response.next_batch.clone()).await;

    for room in settings.rooms.iter() {
        info!("Joining room {}", room);
        let room_id = matrix_sdk::ruma::RoomId::parse(&room)?;
        client.join_room_by_id(&room_id).await?;
    }
    
    let log_room = client
        .get_room(&matrix_sdk::ruma::RoomId::parse(&settings.rooms[0])?)
        .expect("Room should exist after joining");
    info!("Successfully joined room");

    let client_clone = client.clone();

    debug!("Setting up message event handler");

    let qs_connections_message_handler = qs_connections.clone();
    if settings.allow_commands {
        let settings_clone = settings.clone();
        client.add_event_handler(
            move |event: OriginalSyncRoomMessageEvent, room: Room| async move {
                if let Err(e) =
                    handle_message(event, room, &qs_connections_message_handler, &settings_clone).await
                {
                    error!("Error handling message: {:#}", e);
                }
            },
        );
    }

    if settings.allow_verification {
        client.add_event_handler(
            |ev: ToDeviceKeyVerificationRequestEvent, client: Client| async move {
                let request = client
                    .encryption()
                    .get_verification_request(&ev.sender, &ev.content.transaction_id)
                    .await
                    .expect("Request object wasn't created");

                tokio::spawn(request_verification_handler(client, request));
            },
        );
    }

    info!("Starting sync loop");
    let matrix_client = client.clone();
    tokio::spawn(async move {
        if let Err(e) = matrix_client
            .sync(SyncSettings::default().token(response.next_batch))
            .await
        {
            error!("Matrix sync loop error: {}", e);
        }
    });

    info!("Entering main event loop");

    let mut sm = StreamMap::new();

    for x in qs_connections.iter() {
        let name = x.value().1.name.clone();
        let conn = x.value().0.clone();
        sm.insert(name.clone(), conn.subscribe_log(&["Run", "Error"]).await);
    }

    loop {
        let msg = sm.next().await.unwrap();
        handle_run_message(msg.0, msg.1.1.unwrap(), &log_room).await;
    }

    Ok(())
}

async fn handle_run_message(name: String, msg: LogMessage, room: &Room) {
    let msg = format!("{}: {}", name, msg.message);
    send_matrix_message(&room, &msg, true).await;
}