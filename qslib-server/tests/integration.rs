//! Contract tests for the unreleased v1 API and its single managed SCPI actor.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
                            if running {
                                "active_run 1 1 1 1 1 1 Running".to_string()
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
                            }
                            String::new()
                        };
                        let response = match identifier {
                            Some(id) if body.is_empty() => format!("OK {id}\n"),
                            Some(id) => format!("OK {id} {body}\n"),
                            None if body.is_empty() => "OK\n".to_string(),
                            None => format!("OK {body}\n"),
                        };
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

#[tokio::test]
async fn health_uses_the_single_managed_connection() {
    let (address, _root, connections, _commands) = setup(Role::Observer).await;
    let health = wait_ready(address).await;
    assert_eq!(health["name"], "qslib-server");
    assert_eq!(health["current_access"]["level"], "observer");
    for _ in 0..5 {
        reqwest::get(format!("http://{address}/health"))
            .await
            .unwrap();
    }
    assert_eq!(connections.load(Ordering::SeqCst), 1);
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
    assert_eq!(response.status(), 201);
    assert_eq!(
        std::fs::read(root.path().join("logs/data.bin")).unwrap(),
        replacement
    );
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
