//! Contract tests for the unreleased v1 API and its single managed SCPI actor.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use qslib_core::commands::AccessLevel;
use qslib_server::auth::{AuthPolicy, Role};
use qslib_server::config::Config;
use qslib_server::state::AppState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn spawn_http(state: AppState) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, qslib_server::build_router(state))
            .await
            .unwrap();
    });
    address
}

async fn spawn_fake_scpi(running: bool) -> (SocketAddr, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    spawn_fake_scpi_with_burst(running, 0).await
}

#[derive(Default)]
struct PowerResponseGate {
    used: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl PowerResponseGate {
    async fn block_first_response(&self) {
        if !self.used.swap(true, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }

    async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

async fn spawn_fake_scpi_with_burst(
    running: bool,
    temperature_burst_on_power: usize,
) -> (SocketAddr, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    spawn_fake_scpi_with_options(running, temperature_burst_on_power, None, None).await
}

async fn spawn_fake_scpi_with_options(
    running: bool,
    temperature_burst_on_power: usize,
    power_gate: Option<Arc<PowerResponseGate>>,
    experiments_root: Option<PathBuf>,
) -> (SocketAddr, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let commands = Arc::new(Mutex::new(Vec::new()));
    let recorded = commands.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            accepted.fetch_add(1, Ordering::SeqCst);
            let recorded = recorded.clone();
            let power_gate = power_gate.clone();
            let experiments_root = experiments_root.clone();
            tokio::spawn(async move {
                if socket
                    .write_all(b"READy -session=1 -product=Test -version=1.0 -build=1 -capabilities=Index\n")
                    .await
                    .is_err()
                {
                    return;
                }
                let mut pending = String::new();
                let mut access_level = "Observer".to_string();
                let mut exclusive = false;
                let mut drawer_open = false;
                let mut current_run = running.then(|| "active_run".to_string());
                let mut buffer = [0_u8; 4096];
                loop {
                    let count = match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(count) => count,
                    };
                    pending.push_str(&String::from_utf8_lossy(&buffer[..count]));
                    while let Some(end) = pending.find('\n') {
                        let line = pending[..end].trim().to_string();
                        pending = pending[end + 1..].to_string();
                        let mut parts = line.splitn(2, ' ');
                        let first = parts.next().unwrap_or_default();
                        let (identifier, command) = if first.parse::<u32>().is_ok() {
                            (Some(first), parts.next().unwrap_or_default())
                        } else {
                            (None, line.as_str())
                        };
                        recorded.lock().unwrap().push(command.to_string());
                        let body = if command == "ACC?" {
                            format!(
                                "-stealth=False -exclusive={} {}",
                                if exclusive { "True" } else { "False" },
                                access_level
                            )
                        } else if command.starts_with("TBC:ControlZones?") {
                            "6".to_string()
                        } else if command.starts_with("POW?") {
                            "ON".to_string()
                        } else if command.starts_with("BLOCK?") {
                            "ON 60".to_string()
                        } else if command.starts_with("LED:STATus?") {
                            "green on".to_string()
                        } else if command == "RAND?" {
                            "142371".to_string()
                        } else if command.eq_ignore_ascii_case("drawer?") {
                            if drawer_open {
                                "Open".to_string()
                            } else {
                                "Closed".to_string()
                            }
                        } else if command.eq_ignore_ascii_case("eng?") {
                            "Down".to_string()
                        } else if command.starts_with("RET ${RunTitle") {
                            if let Some(name) = &current_run {
                                format!("{name} 1 1 1 1 1 1 Running")
                            } else {
                                "- -1 -1 -1 -1 -1 -1 Idle".to_string()
                            }
                        } else if command == "RET ${Protocol} ${SampleVolume} ${RunMode}" {
                            "exact_protocol 35 standard".to_string()
                        } else if command == "PROT? ${Protocol}" {
                            "<quote.reply>STAGE 1 STAGE_1 <multiline.stage>\n\tSTEP 1 <multiline.step>\n\t\tRAMP 25\n\t\tHOLD 60\n\t</multiline.step>\n</multiline.stage></quote.reply>".to_string()
                        } else if command == "REMainingTime?" {
                            "-".to_string()
                        } else if command.starts_with("RET $(DRAWER?)") {
                            "Closed Down off \"25 25 25 25 25 25\" \"25 25 25 25 25 25\" 30 \"-Zone1=25 -Zone2=25 -Zone3=25 -Zone4=25 -Zone5=25 -Zone6=25\" \"-Zone1=False -Zone2=False -Zone3=False -Zone4=False -Zone5=False -Zone6=False\" 31".to_string()
                        } else {
                            if command.starts_with("ACC ") {
                                access_level = command
                                    .split_whitespace()
                                    .last()
                                    .unwrap_or("Observer")
                                    .to_string();
                                exclusive =
                                    command.to_ascii_lowercase().contains("-exclusive=true");
                            } else if command == "OPEN" {
                                drawer_open = true;
                            } else if command == "CLOSE" {
                                drawer_open = false;
                            } else if command.starts_with("EXP:NEW ") {
                                if let (Some(root), Some(name)) =
                                    (experiments_root.as_ref(), command.split_whitespace().nth(1))
                                {
                                    let name = name.trim_matches('"');
                                    let _ = std::fs::create_dir_all(root.join(name));
                                }
                            } else if command.starts_with("RP ") {
                                current_run = command
                                    .split_whitespace()
                                    .next_back()
                                    .map(|name| name.trim_matches('"').to_string());
                            }
                            String::new()
                        };
                        let response = match identifier {
                            Some(id) if body.is_empty() => format!("OK {id}\n"),
                            Some(id) => format!("OK {id} {body}\n"),
                            None if body.is_empty() => "OK\n".to_string(),
                            None => format!("OK {body}\n"),
                        };
                        if command == "POW ON" {
                            if temperature_burst_on_power > 0 {
                                // These messages exercise the event-time
                                // context carried by later replayed events.
                                for message in [
                                    "MESSage Run 1700000000.0 Starting \"burst run\"\n",
                                    "MESSage Run 1700000000.1 Stage 2\n",
                                    "MESSage Run 1700000000.2 Cycle 7\n",
                                    "MESSage Run 1700000000.3 Step 3\n",
                                    "MESSage Run 1700000000.4 Ramping -zones=Zone1,Zone2,Zone3,Zone4,Zone5,Zone6 -targets=70,71,72,73,74,75 -rates=1.6,1.6,1.6,1.6,1.6,1.6\n",
                                    "MESSage Run 1700000000.5 Collected -run=\"burst run\" -stage=2 -cycle=7 -step=3 -point=1\n",
                                ] {
                                    if socket.write_all(message.as_bytes()).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            // A paced burst deterministically exceeds the
                            // per-topic buffer if the semantic actor stops
                            // polling subscriptions while a job is in flight.
                            for index in 0..temperature_burst_on_power {
                                let message = format!(
                                    "MESSage Temperature {} -sample={}\n",
                                    1_700_000_000.0 + index as f64,
                                    20.0 + index as f64 / 100.0,
                                );
                                if socket.write_all(message.as_bytes()).await.is_err() {
                                    return;
                                }
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                        }
                        if command == "POW ON" {
                            if let Some(gate) = &power_gate {
                                gate.block_first_response().await;
                            }
                        }
                        if socket.write_all(response.as_bytes()).await.is_err() {
                            return;
                        }
                        if command.starts_with("QUIT") {
                            return;
                        }
                    }
                }
            });
        }
    });
    (address, connections, commands)
}

fn test_config(scpi_target: SocketAddr, file_root: PathBuf) -> Config {
    Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        scpi_target,
        file_root,
        auth_config: None,
        no_auth: true,
        unauthenticated_role: Role::Administrator,
        max_access: AccessLevel::Administrator,
        scpi_password: None,
        queue_capacity: 64,
        allow_file_writes: true,
        allow_controls: true,
        enable_raw_scpi: false,
        enable_scpi_tunnel: false,
        max_tunnels: 4,
        log: None,
    }
}

fn experiment_package(protocol_name: &str, marker: &str) -> Vec<u8> {
    let protocol = format!(
        "<TCProtocol><ProtocolName>{protocol_name}</ProtocolName><SampleVolume>20</SampleVolume><RunMode>Standard</RunMode><TCStage><NumOfRepetitions>1</NumOfRepetitions><TCStep><CollectionFlag>0</CollectionFlag><Temperature>60</Temperature><HoldTime>10</HoldTime></TCStep></TCStage></TCProtocol>"
    );
    let cursor = std::io::Cursor::new(Vec::new());
    let mut archive = zip::ZipWriter::new(cursor);
    for (name, contents) in [
        ("apldbio/sds/experiment.xml", "<Experiment/>"),
        ("apldbio/sds/tcprotocol.xml", protocol.as_str()),
        ("accepted-marker.txt", marker),
    ] {
        archive
            .start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        std::io::Write::write_all(&mut archive, contents.as_bytes()).unwrap();
    }
    archive.finish().unwrap().into_inner()
}

fn create_contexts(root: &std::path::Path) {
    for context in [
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ] {
        std::fs::create_dir_all(root.join(context)).unwrap();
    }
}

async fn setup(
    role: Role,
) -> (
    SocketAddr,
    tempfile::TempDir,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<String>>>,
) {
    setup_with_running(role, false).await
}

async fn setup_with_running(
    role: Role,
    running: bool,
) -> (
    SocketAddr,
    tempfile::TempDir,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<String>>>,
) {
    let (scpi, connections, commands) = spawn_fake_scpi(running).await;
    let root = tempfile::tempdir().unwrap();
    create_contexts(root.path());
    let config = test_config(scpi, root.path().to_path_buf());
    let state = AppState::new(&config, AuthPolicy::unauthenticated(role)).unwrap();
    let address = spawn_http(state).await;
    (address, root, connections, commands)
}

async fn wait_ready(address: SocketAddr) -> serde_json::Value {
    let client = reqwest::Client::new();
    for _ in 0..100 {
        let health: serde_json::Value = client
            .get(format!("http://{address}/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if health["ready"] == true {
            return health;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("managed actor did not become ready")
}

async fn wait_operation(
    client: &reqwest::Client,
    address: SocketAddr,
    initial: serde_json::Value,
) -> serde_json::Value {
    let id = initial["id"].as_str().unwrap();
    for _ in 0..200 {
        let record: serde_json::Value = client
            .get(format!("http://{address}/api/v1/operations/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            record["state"].as_str(),
            Some("succeeded" | "failed" | "unknown")
        ) {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("operation did not finish")
}

async fn wait_operation_as(
    client: &reqwest::Client,
    address: SocketAddr,
    initial: serde_json::Value,
    token: &str,
) -> serde_json::Value {
    let id = initial["id"].as_str().unwrap();
    for _ in 0..200 {
        let record: serde_json::Value = client
            .get(format!("http://{address}/api/v1/operations/{id}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            record["state"].as_str(),
            Some("succeeded" | "failed" | "unknown")
        ) {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("operation did not finish")
}

async fn setup_token_acl() -> (SocketAddr, tempfile::TempDir, AppState) {
    let (scpi, _connections, _commands) = spawn_fake_scpi(false).await;
    let root = tempfile::tempdir().unwrap();
    create_contexts(root.path());
    let auth_path = root.path().join("auth.toml");
    std::fs::write(
        &auth_path,
        format!(
            "unauthenticated_role = \"observer\"\n\n\
             [[tokens]]\nname = \"owner\"\nsha256 = \"{}\"\nrole = \"controller\"\n\n\
             [[tokens]]\nname = \"other\"\nsha256 = \"{}\"\nrole = \"observer\"\n\n\
             [[tokens]]\nname = \"admin\"\nsha256 = \"{}\"\nrole = \"administrator\"\n",
            qslib_server::state::sha256_hex(b"owner-token"),
            qslib_server::state::sha256_hex(b"other-token"),
            qslib_server::state::sha256_hex(b"admin-token"),
        ),
    )
    .unwrap();
    let auth = AuthPolicy::from_file(&auth_path).unwrap();
    let config = test_config(scpi, root.path().to_path_buf());
    let state = AppState::new(&config, auth).unwrap();
    let address = spawn_http(state.clone()).await;
    (address, root, state)
}

#[tokio::test]
async fn health_uses_the_single_managed_connection() {
    let (address, _root, connections, commands) = setup(Role::Observer).await;
    let health = wait_ready(address).await;
    assert_eq!(health["name"], "qslib-server");
    assert_eq!(health["current_access"]["level"], "observer");
    for _ in 0..5 {
        reqwest::get(format!("http://{address}/health"))
            .await
            .unwrap();
    }
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert!(commands.lock().unwrap().iter().any(|command| {
        command.starts_with("SUBS+")
            && command.contains("Temperature")
            && command.contains("Error")
            && command.contains("LEDStatus")
    }));
}

#[tokio::test]
async fn capabilities_and_status_follow_the_v1_contract() {
    let (address, _root, _connections, _commands) = setup(Role::Observer).await;
    wait_ready(address).await;
    let client = reqwest::Client::new();
    let capabilities: serde_json::Value = client
        .get(format!("http://{address}/api/v1/capabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(capabilities["api_version"], "v1");
    assert_eq!(capabilities["sse"], true);
    assert_eq!(capabilities["sse_cursor_format"], "epoch-sequence");
    assert_eq!(capabilities["sse_event_context"], true);
    assert_eq!(capabilities["sse_initial_snapshot"], true);
    assert_eq!(capabilities["raw_scpi"], false);

    let response = client
        .get(format!("http://{address}/api/v1/instrument/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("x-request-id"));
    let status: serde_json::Value = response.json().await.unwrap();
    assert_eq!(status["zone_count"], 6);
    assert_eq!(status["run"]["state"], "Idle");
}

#[tokio::test]
async fn run_and_experiment_routes_reject_encoded_unsafe_names() {
    let (address, _root, _connections, _commands) = setup(Role::Administrator).await;
    wait_ready(address).await;
    let client = reqwest::Client::new();
    let unsafe_name = "bad%5Cname";

    let requests = [
        client.get(format!("http://{address}/api/v1/experiments/{unsafe_name}")),
        client.get(format!("http://{address}/api/v1/runs/{unsafe_name}")),
        client.post(format!(
            "http://{address}/api/v1/runs/{unsafe_name}/actions/compile"
        )),
        client.get(format!("http://{address}/api/v1/runs/{unsafe_name}/eds")),
        client
            .put(format!(
                "http://{address}/api/v1/runs/{unsafe_name}/protocol"
            ))
            .json(&serde_json::json!({
                "scpi": "STAGE 1 STAGE_1\n",
                "tcprotocol_xml": "<TCProtocol/>"
            })),
    ];
    for request in requests {
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["code"], "invalid_input");
    }

    // Exercise dot-segment decoding without allowing the URL builder to
    // normalize the request path before it reaches Axum.
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /api/v1/experiments/%2E%2E HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 400 "), "{response}");
}

#[tokio::test]
async fn current_protocol_is_read_from_managed_scpi_not_tcprotocol_xml() {
    let (address, root, _connections, commands) = setup_with_running(Role::Observer, true).await;
    wait_ready(address).await;
    let display_dir = root.path().join("experiments/active_run/apldbio/sds");
    std::fs::create_dir_all(&display_dir).unwrap();
    std::fs::write(
        display_dir.join("tcprotocol.xml"),
        "<TCProtocol><ProtocolName>wrong_display_protocol</ProtocolName></TCProtocol>",
    )
    .unwrap();

    let response = reqwest::get(format!("http://{address}/api/v1/runs/current/protocol"))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let protocol: serde_json::Value = response.json().await.unwrap();
    assert_eq!(protocol["name"], "exact_protocol");
    assert_eq!(protocol["sample_volume"], 35.0);
    assert_eq!(protocol["run_mode"], "standard");
    assert!(protocol["scpi"]
        .as_str()
        .unwrap()
        .contains("STAGE 1 STAGE_1"));
    assert!(!protocol["scpi"]
        .as_str()
        .unwrap()
        .contains("wrong_display_protocol"));

    let commands = commands.lock().unwrap();
    assert!(commands
        .iter()
        .any(|command| command == "RET ${Protocol} ${SampleVolume} ${RunMode}"));
    assert!(commands
        .iter()
        .any(|command| command == "PROT? ${Protocol}"));
}

#[tokio::test]
async fn protocol_update_sends_exact_scpi_and_stores_display_xml_separately() {
    let (address, root, _connections, commands) = setup_with_running(Role::Controller, true).await;
    wait_ready(address).await;
    let run_dir = root.path().join("experiments/active_run/apldbio/sds");
    std::fs::create_dir_all(&run_dir).unwrap();
    let scpi =
        "PROT -volume=12 -runmode=fast exact_update <multiline.protocol></multiline.protocol>";
    let display = r#"<TCProtocol><ProtocolName>display_only</ProtocolName><CollectionProfile><CollectionCondition><FilterSet Emission="mm4" Excitation="xquant"/></CollectionCondition></CollectionProfile></TCProtocol>"#;

    let response = reqwest::Client::new()
        .put(format!(
            "http://{address}/api/v1/runs/active_run/protocol?mode=replace"
        ))
        .json(&serde_json::json!({
            "scpi": scpi,
            "tcprotocol_xml": display,
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        std::fs::read_to_string(run_dir.join("tcprotocol.xml")).unwrap(),
        display
    );
    let qsl = std::fs::read_to_string(run_dir.join("qsl-tcprotocol.xml")).unwrap();
    assert!(qsl.contains("exact_update"));
    assert!(!qsl.contains("display_only"));
    assert!(commands
        .lock()
        .unwrap()
        .iter()
        .any(|command| command == scpi));

    let other_dir = root.path().join("experiments/other_run/apldbio/sds");
    std::fs::create_dir_all(&other_dir).unwrap();
    let response = reqwest::Client::new()
        .put(format!(
            "http://{address}/api/v1/runs/other_run/protocol?mode=replace"
        ))
        .json(&serde_json::json!({
            "scpi": scpi,
            "tcprotocol_xml": display,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let error: serde_json::Value = response.json().await.unwrap();
    assert_eq!(error["error"]["code"], "not_running");
    assert_eq!(error["error"]["details"]["requested"], "other_run");
    assert_eq!(error["error"]["details"]["current"]["name"], "active_run");
}

#[tokio::test]
async fn protocol_update_requires_file_write_policy_as_well_as_controls() {
    let (scpi, _connections, commands) = spawn_fake_scpi(true).await;
    let root = tempfile::tempdir().unwrap();
    for context in [
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ] {
        std::fs::create_dir_all(root.path().join(context)).unwrap();
    }
    let mut config = test_config(scpi, root.path().to_path_buf());
    config.allow_file_writes = false;
    let state = AppState::new(&config, AuthPolicy::unauthenticated(Role::Controller)).unwrap();
    let address = spawn_http(state).await;
    wait_ready(address).await;
    commands.lock().unwrap().clear();

    let response = reqwest::Client::new()
        .put(format!(
            "http://{address}/api/v1/runs/active_run/protocol?mode=replace"
        ))
        .json(&serde_json::json!({
            "scpi": "PROT exact <multiline.protocol></multiline.protocol>",
            "tcprotocol_xml": "<TCProtocol/>",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "forbidden");
    assert!(commands
        .lock()
        .unwrap()
        .iter()
        .all(|command| !command.starts_with("PROT ")));
}

#[tokio::test]
async fn controller_transaction_restores_observer_on_the_managed_connection() {
    let (address, _root, connections, commands) = setup(Role::Controller).await;
    wait_ready(address).await;
    let response = reqwest::Client::new()
        .put(format!("http://{address}/api/v1/instrument/power"))
        .json(&serde_json::json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);

    let health: serde_json::Value = reqwest::get(format!("http://{address}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["current_access"]["level"], "observer");
    assert_eq!(connections.load(Ordering::SeqCst), 1);

    let commands = commands.lock().unwrap();
    let controller = commands
        .iter()
        .position(|command| command.ends_with(" Controller"))
        .expect("Controller elevation was not submitted");
    let power = commands
        .iter()
        .position(|command| command == "POW ON")
        .expect("power command was not submitted");
    let observer = commands
        .iter()
        .enumerate()
        .skip(power + 1)
        .find(|(_, command)| command.ends_with(" Observer"))
        .map(|(index, _)| index)
        .expect("Observer restoration was not submitted");
    assert!(controller < power && power < observer);
}

#[tokio::test]
async fn contextual_files_support_ranges_etags_head_and_atomic_put() {
    let (address, root, _connections, _commands) = setup(Role::Controller).await;
    let content = b"0123456789abcdef";
    std::fs::write(root.path().join("logs/data.bin"), content).unwrap();
    let client = reqwest::Client::new();
    let url = format!("http://{address}/api/v1/files/logs/data.bin");

    let response = client
        .get(&url)
        .header("Range", "bytes=4-9")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert!(response.headers().contains_key("etag"));
    assert_eq!(&response.bytes().await.unwrap()[..], b"456789");

    let response = client.head(&url).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["content-length"].to_str().unwrap(),
        content.len().to_string()
    );

    let replacement = b"replacement";
    let response = client
        .put(&url)
        .body(replacement.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        std::fs::read(root.path().join("logs/data.bin")).unwrap(),
        replacement
    );

    let created = client
        .put(format!("http://{address}/api/v1/files/logs/new.bin"))
        .body("new")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
}

#[tokio::test]
async fn role_and_feature_boundaries_are_enforced() {
    let (address, _root, _connections, _commands) = setup(Role::Observer).await;
    let client = reqwest::Client::new();
    let put = client
        .put(format!("http://{address}/api/v1/files/logs/nope"))
        .body("data")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 403);
    let header_request_id = put.headers()["x-request-id"].to_str().unwrap().to_string();
    let body: serde_json::Value = put.json().await.unwrap();
    assert_eq!(body["error"]["outcome"], "not_started");
    assert!(body["request_id"].is_string());
    assert_eq!(body["request_id"], header_request_id);

    let raw = client
        .post(format!("http://{address}/api/v1/scpi"))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(raw.status(), 404);
}

#[tokio::test]
async fn old_unversioned_routes_are_not_compatibility_aliases() {
    let (address, _root, _connections, _commands) = setup(Role::Administrator).await;
    for route in ["/file/test", "/list", "/scpi", "/upgrade"] {
        let response = reqwest::get(format!("http://{address}{route}"))
            .await
            .unwrap();
        assert_eq!(response.status(), 404, "route {route}");
    }
}

#[tokio::test]
async fn router_errors_use_the_structured_contract() {
    let (address, _root, _connections, _commands) = setup(Role::Observer).await;
    let client = reqwest::Client::new();

    for (method, route, status, code) in [
        (reqwest::Method::GET, "/missing", 404, "not_found"),
        (
            reqwest::Method::POST,
            "/api/v1/instrument/status",
            405,
            "method_not_allowed",
        ),
    ] {
        let response = client
            .request(method, format!("http://{address}{route}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let request_id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_string();
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["code"], code);
        assert_eq!(body["request_id"], request_id);
    }
}

#[tokio::test]
async fn explicit_status_requests_are_fresh_and_preserve_raw_casing() {
    let (address, _root, _connections, commands) = setup(Role::Observer).await;
    wait_ready(address).await;
    commands.lock().unwrap().clear();
    for _ in 0..2 {
        let status: serde_json::Value =
            reqwest::get(format!("http://{address}/api/v1/instrument/status"))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        assert_eq!(status["run"]["state"], "Idle");
        assert_eq!(status["drawer"], "Closed");
        assert_eq!(status["cover"], "Down");
    }
    let commands = commands.lock().unwrap();
    assert_eq!(
        commands
            .iter()
            .filter(|command| command.starts_with("RET $(DRAWER?)"))
            .count(),
        2
    );
}

#[tokio::test]
async fn led_off_and_conditional_cover_lower_emit_direct_scpi_macros() {
    let (address, _root, _connections, commands) = setup(Role::Controller).await;
    wait_ready(address).await;
    commands.lock().unwrap().clear();
    let client = reqwest::Client::new();

    let response = client
        .post(format!(
            "http://{address}/api/v1/instrument/indicator/actions/off"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    let response = client
        .put(format!("http://{address}/api/v1/instrument/cover"))
        .json(&serde_json::json!({
            "position": "down",
            "verify": true,
            "ensure_drawer": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);

    let commands = commands.lock().unwrap();
    assert!(commands.iter().any(|command| command == "LED:LightOFF"));
    assert!(commands.iter().any(|command| command == "drawer?"));
    assert!(commands.iter().any(|command| command == "COVerDOWN"));
    assert!(!commands.iter().any(|command| command == "CLOSE"));
}

#[tokio::test]
async fn key_and_restart_are_controller_authorized_at_controller_access() {
    let (scpi, _connections, commands) = spawn_fake_scpi(false).await;
    let root = tempfile::tempdir().unwrap();
    for context in [
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ] {
        std::fs::create_dir_all(root.path().join(context)).unwrap();
    }
    let mut config = test_config(scpi, root.path().to_path_buf());
    config.max_access = AccessLevel::Controller;
    let state = AppState::new(&config, AuthPolicy::unauthenticated(Role::Controller)).unwrap();
    let address = spawn_http(state).await;
    wait_ready(address).await;
    commands.lock().unwrap().clear();
    let client = reqwest::Client::new();

    let key: serde_json::Value = client
        .post(format!("http://{address}/api/v1/instrument/access-keys"))
        .header("Idempotency-Key", "key-operation")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key = wait_operation(&client, address, key).await;
    assert_eq!(key["state"], "succeeded");
    assert_eq!(key["result"]["key"], "142371");

    let restart: serde_json::Value = client
        .post(format!(
            "http://{address}/api/v1/instrument/actions/restart"
        ))
        .header("Idempotency-Key", "restart-operation")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let restart = wait_operation(&client, address, restart).await;
    assert_eq!(restart["state"], "succeeded");

    let commands = commands.lock().unwrap();
    assert!(commands.iter().any(|command| command == "RAND?"));
    assert!(commands
        .iter()
        .any(|command| command == "SYST:EXEC \"killall zygote\""));
    assert!(!commands
        .iter()
        .any(|command| command.ends_with(" Administrator")));
}

#[tokio::test]
async fn key_and_restart_obey_control_policy() {
    let (scpi, _connections, _commands) = spawn_fake_scpi(false).await;
    let root = tempfile::tempdir().unwrap();
    for context in [
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ] {
        std::fs::create_dir_all(root.path().join(context)).unwrap();
    }
    let mut config = test_config(scpi, root.path().to_path_buf());
    config.allow_controls = false;
    let state = AppState::new(&config, AuthPolicy::unauthenticated(Role::Controller)).unwrap();
    let address = spawn_http(state).await;
    wait_ready(address).await;
    let client = reqwest::Client::new();
    for route in [
        "/api/v1/instrument/access-keys",
        "/api/v1/instrument/actions/restart",
    ] {
        let response = client
            .post(format!("http://{address}{route}"))
            .header("Idempotency-Key", route)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
    }
}

#[tokio::test]
async fn preflight_and_compile_failures_use_stable_codes_and_details() {
    let (busy_address, _busy_root, _connections, _commands) =
        setup_with_running(Role::Controller, true).await;
    wait_ready(busy_address).await;
    let busy = reqwest::Client::new()
        .get(format!("http://{busy_address}/api/v1/runs/preflight"))
        .query(&[("experiment", "new_run"), ("overwrite", "false")])
        .send()
        .await
        .unwrap();
    assert_eq!(busy.status(), 409);
    let busy: serde_json::Value = busy.json().await.unwrap();
    assert_eq!(busy["error"]["code"], "machine_busy");
    assert_eq!(busy["error"]["details"]["current"]["state"], "Running");

    let (address, root, _connections, commands) = setup(Role::Controller).await;
    wait_ready(address).await;
    let experiments = root.path().join("experiments");
    for (name, attributes) in [
        ("missing_attributes", None),
        (
            "active_run",
            Some("[.]\nrun = -\nstate = Running\ncollected = False\n"),
        ),
        (
            "collected_run",
            Some("[.]\nrun = -\nstate = Completed\ncollected = True\n"),
        ),
    ] {
        let directory = experiments.join(name);
        std::fs::create_dir_all(&directory).unwrap();
        if let Some(attributes) = attributes {
            std::fs::write(directory.join(".attributes"), attributes).unwrap();
        }
    }
    commands.lock().unwrap().clear();
    let client = reqwest::Client::new();
    for (name, code) in [
        ("missing_attributes", "run_not_found"),
        ("active_run", "run_not_finished"),
        ("collected_run", "already_collected"),
    ] {
        let initial: serde_json::Value = client
            .post(format!(
                "http://{address}/api/v1/runs/{name}/actions/compile"
            ))
            .header("Idempotency-Key", format!("compile-{name}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let operation = wait_operation(&client, address, initial).await;
        assert_eq!(operation["state"], "failed");
        assert_eq!(operation["error"]["code"], code);
        assert_eq!(operation["error"]["details"]["name"], name);
    }
    assert!(!commands
        .lock()
        .unwrap()
        .iter()
        .any(|command| command.starts_with("EXP:RUN")));
}

#[tokio::test]
async fn queued_start_uses_the_exact_package_version_accepted_by_etag() {
    let root = tempfile::tempdir().unwrap();
    create_contexts(root.path());
    let gate = Arc::new(PowerResponseGate::default());
    let (scpi, _connections, commands) = spawn_fake_scpi_with_options(
        false,
        0,
        Some(gate.clone()),
        Some(root.path().join("experiments")),
    )
    .await;
    let config = test_config(scpi, root.path().to_path_buf());
    let state = AppState::new(&config, AuthPolicy::unauthenticated(Role::Controller)).unwrap();
    let address = spawn_http(state).await;
    wait_ready(address).await;
    let client = reqwest::Client::new();
    let package_url = format!("http://{address}/api/v1/experiments/queued/package");

    let accepted = client
        .put(&package_url)
        .header("Content-Type", "application/zip")
        .body(experiment_package("accepted_protocol", "accepted"))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 201);
    let accepted_etag = accepted.headers()["etag"].to_str().unwrap().to_string();

    // Hold an earlier semantic operation in the actor so the accepted start
    // remains queued while the public staging slot is replaced.
    let power_client = client.clone();
    let power = tokio::spawn(async move {
        power_client
            .put(format!("http://{address}/api/v1/instrument/power"))
            .json(&serde_json::json!({"enabled": true}))
            .send()
            .await
            .unwrap()
    });
    gate.wait_until_entered().await;

    let start_request = serde_json::json!({
        "experiment": "queued",
        "package_etag": accepted_etag,
        "overwrite": "false",
        "require_drawer_check": true,
    });
    let initial: serde_json::Value = client
        .post(format!("http://{address}/api/v1/runs"))
        .header("Idempotency-Key", "queued-start-accepted-package")
        .json(&start_request)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(initial["state"], "queued");

    let restaged = client
        .put(&package_url)
        .header("Content-Type", "application/zip")
        .body(experiment_package("restaged_protocol", "restaged"))
        .send()
        .await
        .unwrap();
    assert_eq!(restaged.status(), 201);
    let restaged_etag = restaged.headers()["etag"].to_str().unwrap().to_string();

    // Idempotent retries resolve the accepted operation before consulting the
    // now-different public staging slot.
    let retry_after_restage: serde_json::Value = client
        .post(format!("http://{address}/api/v1/runs"))
        .header("Idempotency-Key", "queued-start-accepted-package")
        .json(&start_request)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retry_after_restage["id"], initial["id"]);

    let deleted = client
        .delete(&package_url)
        .header("If-Match", restaged_etag)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let retry_after_delete: serde_json::Value = client
        .post(format!("http://{address}/api/v1/runs"))
        .header("Idempotency-Key", "queued-start-accepted-package")
        .json(&start_request)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retry_after_delete["id"], initial["id"]);

    assert!(std::fs::read_dir(root.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".qslib-server-start-input-")));
    let snapshot = std::fs::read_dir(root.path().parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with(".qslib-server-start-input-")
            }) && path
                .join(".qslib-staging/queued/accepted-marker.txt")
                .is_file()
        })
        .expect("accepted snapshot was not created outside the default context");
    let snapshot_name = snapshot.file_name().unwrap().to_string_lossy();
    let mutation = client
        .put(format!(
            "http://{address}/api/v1/files/default/%2E%2E/{snapshot_name}/.qslib-staging/queued/accepted-marker.txt"
        ))
        .body("tampered")
        .send()
        .await
        .unwrap();
    assert!(matches!(mutation.status().as_u16(), 400 | 404));
    assert_eq!(
        std::fs::read_to_string(snapshot.join(".qslib-staging/queued/accepted-marker.txt"))
            .unwrap(),
        "accepted"
    );

    gate.release();
    assert_eq!(power.await.unwrap().status(), 204);
    let operation = wait_operation(&client, address, initial).await;
    assert_eq!(operation["state"], "succeeded", "{operation:#}");
    for _ in 0..20 {
        if !snapshot.exists() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        !snapshot.exists(),
        "accepted package snapshot was not cleaned"
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("experiments/queued/accepted-marker.txt"))
            .unwrap(),
        "accepted"
    );
    let commands = commands.lock().unwrap();
    assert!(commands
        .iter()
        .any(|command| command.contains("accepted_protocol")));
    assert!(!commands
        .iter()
        .any(|command| command.contains("restaged_protocol")));
}

#[tokio::test]
async fn compile_quarantines_a_stale_output_and_waits_for_a_new_artifact() {
    let (address, root, _connections, commands) = setup(Role::Controller).await;
    wait_ready(address).await;
    let experiments = root.path().join("experiments");
    let working = experiments.join("finished");
    std::fs::create_dir_all(&working).unwrap();
    std::fs::write(
        working.join(".attributes"),
        "[.]\nrun = -\nstate = Completed\ncollected = False\n",
    )
    .unwrap();
    let generated = experiments.join("finished.eds");
    std::fs::write(&generated, b"stale artifact").unwrap();

    let client = reqwest::Client::new();
    let initial: serde_json::Value = client
        .post(format!(
            "http://{address}/api/v1/runs/finished/actions/compile"
        ))
        .header("Idempotency-Key", "compile-requires-fresh-artifact")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    for _ in 0..200 {
        if commands
            .lock()
            .unwrap()
            .iter()
            .any(|command| command.starts_with("EXP:RUN"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !generated.exists(),
        "stale output was accepted without quarantine"
    );
    assert!(!commands
        .lock()
        .unwrap()
        .iter()
        .any(|command| command.starts_with("FILE:MOVE")));
    let backup = std::fs::read_dir(&experiments)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".finished.")
                && path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with(".eds.qslib-compile-backup")
        })
        .expect("stale generated EDS was not preserved");
    assert_eq!(std::fs::read(&backup).unwrap(), b"stale artifact");

    std::fs::write(&generated, b"fresh artifact").unwrap();
    let operation = wait_operation(&client, address, initial).await;
    assert_eq!(operation["state"], "succeeded", "{operation:#}");
    assert_eq!(std::fs::read(&generated).unwrap(), b"fresh artifact");
    assert!(
        !backup.exists(),
        "stale-artifact quarantine was not cleaned"
    );
}

#[tokio::test]
async fn staged_package_delete_requires_matching_etag() {
    let (address, root, _connections, _commands) = setup(Role::Controller).await;
    wait_ready(address).await;
    let staged = root.path().join("experiments/.qslib-staging/run");
    std::fs::create_dir_all(&staged).unwrap();
    std::fs::write(staged.join(".qslib-package.etag"), "\"etag\"").unwrap();
    let client = reqwest::Client::new();
    let url = format!("http://{address}/api/v1/experiments/run/package");

    assert_eq!(client.delete(&url).send().await.unwrap().status(), 400);
    assert_eq!(
        client
            .delete(&url)
            .header("If-Match", "\"wrong\"")
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        client
            .delete(&url)
            .header("If-Match", "\"etag\"")
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(!staged.exists());
}

#[tokio::test]
async fn malformed_credentials_never_inherit_the_unauthenticated_role() {
    let (address, _root, _state) = setup_token_acl().await;
    wait_ready(address).await;
    let client = reqwest::Client::new();
    let url = format!("http://{address}/api/v1/capabilities");

    // Header absence intentionally receives the configured Observer fallback.
    assert_eq!(client.get(&url).send().await.unwrap().status(), 200);
    // Authentication schemes are case-insensitive.
    assert_eq!(
        client
            .get(&url)
            .header("Authorization", "bearer owner-token")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    for credential in ["Basic owner-token", "Bearer", "Bearer wrong-token"] {
        let response = client
            .get(&url)
            .header("Authorization", credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401, "credential {credential:?}");
        assert_eq!(
            response.headers()["www-authenticate"],
            "Bearer realm=\"qslib-server\""
        );
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["code"], "unauthorized");
    }
}

#[tokio::test]
async fn operation_results_are_owner_scoped_with_administrator_override() {
    let (address, _root, _state) = setup_token_acl().await;
    wait_ready(address).await;
    let client = reqwest::Client::new();
    let initial: serde_json::Value = client
        .post(format!("http://{address}/api/v1/instrument/access-keys"))
        .bearer_auth("owner-token")
        .header("Idempotency-Key", "private-access-key")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let completed = wait_operation_as(&client, address, initial, "owner-token").await;
    assert_eq!(completed["state"], "succeeded");
    assert_eq!(completed["result"]["key"], "142371");
    let id = completed["id"].as_str().unwrap();
    let url = format!("http://{address}/api/v1/operations/{id}");

    // A different observer sees the same response as for an unknown UUID.
    let hidden = client
        .get(&url)
        .bearer_auth("other-token")
        .send()
        .await
        .unwrap();
    assert_eq!(hidden.status(), 404);
    let hidden: serde_json::Value = hidden.json().await.unwrap();
    assert_eq!(hidden["error"]["code"], "not_found");

    let administrator = client
        .get(&url)
        .bearer_auth("admin-token")
        .send()
        .await
        .unwrap();
    assert_eq!(administrator.headers()["cache-control"], "no-store");
    let administrator: serde_json::Value = administrator.json().await.unwrap();
    assert_eq!(administrator["result"]["key"], "142371");
}

#[tokio::test]
async fn framework_rejections_use_the_structured_error_contract() {
    let (address, _root, _connections, _commands) = setup(Role::Controller).await;
    let client = reqwest::Client::new();
    let url = format!("http://{address}/api/v1/instrument/power");
    let requests = [
        (
            client
                .put(&url)
                .header("Content-Type", "application/json")
                .body("{"),
            400,
            "invalid_input",
        ),
        (client.put(&url).body("{}"), 415, "unsupported_media_type"),
        (
            client
                .put(&url)
                .header("Content-Type", "application/json")
                .body(vec![b' '; 3 * 1024 * 1024]),
            413,
            "payload_too_large",
        ),
    ];

    for (request, status, code) in requests {
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["content-type"], "application/json");
        let request_id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_string();
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["code"], code);
        assert_eq!(body["error"]["outcome"], "not_started");
        assert_eq!(body["request_id"], request_id);
    }
}

#[tokio::test]
async fn fresh_sse_stream_starts_with_a_status_snapshot_at_an_opaque_cursor() {
    let (address, _root, _connections, _commands) = setup(Role::Observer).await;
    wait_ready(address).await;
    let response = reqwest::Client::new()
        .get(format!("http://{address}/api/v1/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    let mut frame = String::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !frame.contains("\n\n") {
            let chunk = stream
                .next()
                .await
                .expect("fresh SSE stream ended before its initial snapshot")
                .unwrap();
            frame.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .expect("fresh SSE stream did not emit its initial snapshot");

    assert!(frame.lines().any(|line| line == "event: reset"), "{frame}");
    let cursor = frame
        .lines()
        .find_map(|line| line.strip_prefix("id: "))
        .expect("initial reset did not carry an SSE cursor");
    assert!(cursor.contains(':'));
    let data: serde_json::Value = frame
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str(line).unwrap())
        .expect("initial reset did not carry JSON data");
    assert_eq!(data["data"]["reason"], "initial_snapshot");
    assert_eq!(data["data"]["status"]["run"]["state"], "Idle");
}

#[tokio::test]
async fn sse_stream_ends_when_server_shutdown_begins() {
    let (scpi, _connections, _commands) = spawn_fake_scpi(false).await;
    let root = tempfile::tempdir().unwrap();
    for context in [
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ] {
        std::fs::create_dir_all(root.path().join(context)).unwrap();
    }
    let config = test_config(scpi, root.path().to_path_buf());
    let state = AppState::new(&config, AuthPolicy::unauthenticated(Role::Observer)).unwrap();
    let address = spawn_http(state.clone()).await;
    wait_ready(address).await;

    let response = reqwest::Client::new()
        .get(format!("http://{address}/api/v1/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    let mut initial = String::new();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !initial.contains("\n\n") {
            let chunk = stream
                .next()
                .await
                .expect("SSE stream ended before its initial reset")
                .expect("SSE initial reset body failed");
            initial.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .expect("SSE initial reset was not delivered");
    assert!(initial.contains("event: reset"));
    state.service.begin_shutdown();
    tokio::time::timeout(Duration::from_secs(1), async {
        // Bytes already accepted by Hyper before shutdown may still be
        // delivered; the contract is that the body reaches EOF promptly.
        while let Some(chunk) = stream.next().await {
            chunk.expect("buffered SSE body failed during shutdown");
        }
    })
    .await
    .expect("SSE body did not react to shutdown");
}

#[tokio::test]
async fn subscriptions_are_drained_while_semantic_operations_run() {
    const BURST: usize = 120;
    let (scpi, _connections, _commands) = spawn_fake_scpi_with_burst(false, BURST).await;
    let root = tempfile::tempdir().unwrap();
    for context in [
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ] {
        std::fs::create_dir_all(root.path().join(context)).unwrap();
    }
    let config = test_config(scpi, root.path().to_path_buf());
    let state = AppState::new(&config, AuthPolicy::unauthenticated(Role::Controller)).unwrap();
    let address = spawn_http(state).await;
    wait_ready(address).await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{address}/api/v1/events"))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();

    let power = client
        .put(format!("http://{address}/api/v1/instrument/power"))
        .json(&serde_json::json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(power.status(), 204);

    let mut body = String::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while body.matches("event: temperature").count() < BURST
            || !body.contains("Collected -run=")
        {
            let chunk = stream
                .next()
                .await
                .expect("SSE stream ended during subscription burst")
                .unwrap();
            body.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .expect("subscription events were dropped while an operation was running");
    assert_eq!(body.matches("event: temperature").count(), BURST);

    let data: Vec<serde_json::Value> = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).unwrap())
        .collect();
    let collected = data
        .iter()
        .find(|entry| {
            entry["data"]["message"]
                .as_str()
                .is_some_and(|message| message.starts_with("Collected"))
        })
        .expect("Run Collected event missing from SSE stream");
    let collected_context = &collected["data"]["context"];
    assert_eq!(collected_context["run_name"], "burst run");
    assert_eq!(collected_context["zone_targets_c"][0], 70.0);
    assert_eq!(collected_context["zone_targets_c"][5], 75.0);
    assert_eq!(collected_context["stage"], 2);
    assert_eq!(collected_context["cycle"], 7);
    assert_eq!(collected_context["step"], 3);

    let temperature = data
        .iter()
        .find(|entry| {
            entry["data"]["message"]
                .as_str()
                .is_some_and(|message| message.starts_with("-sample="))
        })
        .expect("Temperature event missing from SSE stream");
    assert_eq!(temperature["data"]["context"], *collected_context);
}

#[tokio::test]
async fn get_scpi_tunnel_upgrades_and_splices_bytes_bidirectionally() {
    let (scpi, connections, _commands) = spawn_fake_scpi(false).await;
    let root = tempfile::tempdir().unwrap();
    for context in [
        "experiments",
        "runs",
        "logs",
        "templates",
        "calibrations",
        "public_run_complete",
        "private_run_complete",
    ] {
        std::fs::create_dir_all(root.path().join(context)).unwrap();
    }
    let mut config = test_config(scpi, root.path().to_path_buf());
    config.enable_scpi_tunnel = true;
    let state = AppState::new(&config, AuthPolicy::unauthenticated(Role::Administrator)).unwrap();
    let address = spawn_http(state).await;
    wait_ready(address).await;

    let ordinary = reqwest::get(format!("http://{address}/api/v1/scpi/tunnel"))
        .await
        .unwrap();
    assert_eq!(ordinary.status(), 400);
    let incomplete_upgrade = reqwest::Client::new()
        .get(format!("http://{address}/api/v1/scpi/tunnel"))
        .header("Upgrade", "qslib-scpi")
        .send()
        .await
        .unwrap();
    assert_eq!(incomplete_upgrade.status(), 400);

    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!(
                "GET /api/v1/scpi/tunnel HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: qslib-scpi\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut buffer = [0_u8; 1024];
        while !String::from_utf8_lossy(&received).contains("READy -session=1") {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0, "tunnel closed before SCPI greeting");
            received.extend_from_slice(&buffer[..count]);
        }
    })
    .await
    .expect("tunnel did not return an HTTP upgrade and SCPI greeting");
    let greeting = String::from_utf8_lossy(&received);
    assert!(greeting.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
    assert!(greeting
        .to_ascii_lowercase()
        .contains("upgrade: qslib-scpi"));

    socket.write_all(b"ACC?\n").await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut buffer = [0_u8; 1024];
        while !response.contains("OK -stealth=False -exclusive=False Observer") {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0, "tunnel closed before SCPI response");
            response.push_str(&String::from_utf8_lossy(&buffer[..count]));
        }
    })
    .await
    .expect("SCPI response did not traverse upgraded tunnel");
    assert!(connections.load(Ordering::SeqCst) >= 2);
}
