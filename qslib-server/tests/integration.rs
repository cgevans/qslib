//! End-to-end tests: a real router (and a fake SCPI server) over loopback HTTP.

use std::net::SocketAddr;
use std::path::PathBuf;

use qslib_core::commands::AccessLevel;
use qslib_server::config::Config;
use qslib_server::state::AppState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Spawn the qslib-server router on an ephemeral loopback port; return its address.
async fn spawn_server(state: AppState) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = qslib_server::build_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// Minimal fake plaintext SCPI server: sends the ready greeting, then answers
/// each `<ident> <command>` line with `OK <ident> ...`.
async fn spawn_fake_scpi() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            tokio::spawn(async move {
                socket
                    .write_all(
                        b"READy -session=1 -product=Test -version=1.0.0 -build=1 -capabilities=Index\n",
                    )
                    .await
                    .unwrap();
                let mut buf = [0u8; 1024];
                let mut line = String::new();
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    line.push_str(&String::from_utf8_lossy(&buf[..n]));
                    while let Some(pos) = line.find('\n') {
                        let l = line[..pos].trim().to_string();
                        line = line[pos + 1..].to_string();
                        let first = l.split_whitespace().next().unwrap_or("");
                        let (ident, cmd) = if first.parse::<u32>().is_ok() {
                            (
                                Some(first.to_string()),
                                l.split_once(' ')
                                    .map(|x| x.1.to_string())
                                    .unwrap_or_default(),
                            )
                        } else {
                            (None, l.clone())
                        };
                        let ok = |body: &str| match &ident {
                            Some(id) => format!("OK {id} {body}\n"),
                            None => format!("OK {body}\n"),
                        };
                        let ok_empty = || match &ident {
                            Some(id) => format!("OK {id}\n"),
                            None => "OK\n".to_string(),
                        };
                        if cmd.starts_with("QUIT") {
                            let _ = socket.write_all(ok_empty().as_bytes()).await;
                            return;
                        }
                        let resp = if cmd.starts_with("ACC") {
                            ok_empty()
                        } else if cmd.starts_with("SYST:VERS?") {
                            ok("-version=1.0.0")
                        } else if cmd.starts_with("BADCMD") {
                            match &ident {
                                Some(id) => format!("ERRor {id} [NoMatch] --> unknown command\n"),
                                None => "ERRor [NoMatch] --> unknown command\n".to_string(),
                            }
                        } else {
                            ok("done")
                        };
                        if socket.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    addr
}

fn test_config(scpi: SocketAddr, root: PathBuf, token: Option<String>) -> Config {
    let no_auth = token.is_none();
    Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        scpi_target: scpi,
        file_root: root,
        default_access: AccessLevel::Observer,
        max_access: AccessLevel::Controller,
        token,
        token_file: None,
        no_auth,
        scpi_password: None,
        pool_size: 0,
        log: None,
        scpi_timeout_ms: 5000,
        max_tunnels: 16,
        read_only: false,
    }
}

async fn setup(token: Option<&str>) -> (SocketAddr, tempfile::TempDir) {
    let scpi = spawn_fake_scpi().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(scpi, dir.path().to_path_buf(), token.map(|s| s.to_string()));
    let state = AppState::new(&cfg, cfg.resolve_token().unwrap()).unwrap();
    let addr = spawn_server(state).await;
    (addr, dir)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().build().unwrap()
}

#[tokio::test]
async fn health_reports_scpi_ok() {
    let (addr, _dir) = setup(None).await;
    let resp = client()
        .get(format!("http://{addr}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "qslib-server");
    assert_eq!(body["scpi_ok"], true);
}

#[tokio::test]
async fn file_full_download() {
    let (addr, dir) = setup(None).await;
    let content = b"0123456789abcdef".repeat(1000);
    tokio::fs::write(dir.path().join("data.bin"), &content)
        .await
        .unwrap();

    let resp = client()
        .get(format!("http://{addr}/file/data.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["accept-ranges"], "bytes");
    assert_eq!(resp.headers()["content-type"], "application/octet-stream");
    assert!(resp.headers().contains_key("etag"));
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), content.as_slice());
}

#[tokio::test]
async fn put_file_round_trips() {
    let (addr, dir) = setup(None).await;
    let content = b"<PlateSetup/>".repeat(500);

    let resp = client()
        .put(format!(
            "http://{addr}/file/exp/apldbio/sds/plate_setup.xml"
        ))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // The bytes landed on disk under the root, parent dirs created.
    let on_disk = tokio::fs::read(dir.path().join("exp/apldbio/sds/plate_setup.xml"))
        .await
        .unwrap();
    assert_eq!(on_disk, content);

    // And are served back byte-identically over GET.
    let got = client()
        .get(format!(
            "http://{addr}/file/exp/apldbio/sds/plate_setup.xml"
        ))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(got.as_ref(), content.as_slice());
}

#[tokio::test]
async fn put_file_overwrites_atomically() {
    let (addr, dir) = setup(None).await;
    tokio::fs::write(dir.path().join("f.bin"), b"old")
        .await
        .unwrap();
    let resp = client()
        .put(format!("http://{addr}/file/f.bin"))
        .body(b"new-and-longer".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    assert_eq!(
        tokio::fs::read(dir.path().join("f.bin")).await.unwrap(),
        b"new-and-longer"
    );
}

#[tokio::test]
async fn put_file_traversal_forbidden() {
    let (addr, dir) = setup(None).await;
    // Fully percent-encoded `../../escape.bin` (slashes too) so the client sends
    // one opaque segment and cannot normalize the `..` away; axum decodes it and
    // our handler must reject the traversal rather than write outside the root.
    let url = format!("http://{addr}/file/%2e%2e%2f%2e%2e%2fescape.bin");
    let resp = client().put(url).body(b"x".to_vec()).send().await.unwrap();
    assert_eq!(resp.status(), 403);
    assert!(!dir.path().parent().unwrap().join("escape.bin").exists());
}

#[tokio::test]
async fn put_file_read_only_forbidden() {
    let scpi = spawn_fake_scpi().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(scpi, dir.path().to_path_buf(), None);
    cfg.read_only = true;
    let state = AppState::new(&cfg, cfg.resolve_token().unwrap()).unwrap();
    let addr = spawn_server(state).await;

    let resp = client()
        .put(format!("http://{addr}/file/f.bin"))
        .body(b"x".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    assert!(!dir.path().join("f.bin").exists());
}

#[tokio::test]
async fn list_dir_enumerates_recursively() {
    let (addr, dir) = setup(None).await;
    let root = dir.path();
    tokio::fs::create_dir_all(root.join("run/apldbio/sds"))
        .await
        .unwrap();
    tokio::fs::write(root.join("run/top.txt"), b"top")
        .await
        .unwrap();
    tokio::fs::write(root.join("run/apldbio/sds/a.xml"), b"aaaa")
        .await
        .unwrap();
    tokio::fs::write(root.join("run/apldbio/sds/.hidden"), b"hh")
        .await
        .unwrap();

    let resp = client()
        .get(format!("http://{addr}/list/run"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let mut got: Vec<(String, u64)> = body["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["path"].as_str().unwrap().to_string(),
                e["size"].as_u64().unwrap(),
            )
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("apldbio/sds/.hidden".to_string(), 2),
            ("apldbio/sds/a.xml".to_string(), 4),
            ("top.txt".to_string(), 3),
        ]
    );
}

#[tokio::test]
async fn list_root_enumerates_the_file_root() {
    let (addr, dir) = setup(None).await;
    tokio::fs::write(dir.path().join("root.txt"), b"root")
        .await
        .unwrap();

    for suffix in ["/list", "/list/"] {
        let resp = client()
            .get(format!("http://{addr}{suffix}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["files"][0]["path"], "root.txt");
        assert_eq!(body["files"][0]["size"], 4);
    }
}

#[tokio::test]
async fn list_dir_missing_is_404() {
    let (addr, _dir) = setup(None).await;
    let resp = client()
        .get(format!("http://{addr}/list/nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn upgrade_dry_run_verifies_a_real_binary() {
    let (addr, _dir) = setup(None).await;
    let bin = std::fs::read(env!("CARGO_BIN_EXE_qslib-server")).unwrap();
    let sha = qslib_server::state::sha256_hex(&bin);

    let resp = client()
        .post(format!("http://{addr}/upgrade?dry_run=1"))
        .header("x-qslib-sha256", &sha)
        .body(bin)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "body: {text}");
    let j: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(j["status"], "verified");
    assert_eq!(j["dry_run"], true);
    assert_eq!(j["sha256"], sha);
}

#[tokio::test]
async fn upgrade_rejects_sha_mismatch() {
    let (addr, _dir) = setup(None).await;
    let bin = std::fs::read(env!("CARGO_BIN_EXE_qslib-server")).unwrap();
    let resp = client()
        .post(format!("http://{addr}/upgrade?dry_run=1"))
        .header("x-qslib-sha256", "0".repeat(64))
        .body(bin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn upgrade_requires_sha_header() {
    let (addr, _dir) = setup(None).await;
    let resp = client()
        .post(format!("http://{addr}/upgrade?dry_run=1"))
        .body(vec![0x7f, b'E', b'L', b'F', 1, 2, 3])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn upgrade_rejects_non_elf() {
    let (addr, _dir) = setup(None).await;
    let junk = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    let sha = qslib_server::state::sha256_hex(&junk);
    let resp = client()
        .post(format!("http://{addr}/upgrade?dry_run=1"))
        .header("x-qslib-sha256", &sha)
        .body(junk)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn file_range_request() {
    let (addr, dir) = setup(None).await;
    let content: Vec<u8> = (0u8..=255).collect();
    tokio::fs::write(dir.path().join("bytes.bin"), &content)
        .await
        .unwrap();

    let resp = client()
        .get(format!("http://{addr}/file/bytes.bin"))
        .header("Range", "bytes=10-19")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.headers()["content-range"], "bytes 10-19/256");
    assert_eq!(resp.headers()["content-length"], "10");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &content[10..=19]);
}

#[tokio::test]
async fn file_range_unsatisfiable() {
    let (addr, dir) = setup(None).await;
    tokio::fs::write(dir.path().join("small.bin"), b"tiny")
        .await
        .unwrap();
    let resp = client()
        .get(format!("http://{addr}/file/small.bin"))
        .header("Range", "bytes=100-200")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 416);
    assert_eq!(resp.headers()["content-range"], "bytes */4");
}

#[tokio::test]
async fn file_head_has_length_no_body() {
    let (addr, dir) = setup(None).await;
    tokio::fs::write(dir.path().join("h.bin"), b"abcde")
        .await
        .unwrap();
    let resp = client()
        .head(format!("http://{addr}/file/h.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["content-length"], "5");
    let body = resp.bytes().await.unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn file_not_found() {
    let (addr, _dir) = setup(None).await;
    let resp = client()
        .get(format!("http://{addr}/file/nope.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn file_traversal_encoded_forbidden() {
    let (addr, _dir) = setup(None).await;
    // Percent-encoded `../../etc/passwd`; axum decodes before our handler sees it.
    let url = format!("http://{addr}/file/%2e%2e%2f%2e%2e%2fetc%2fpasswd");
    let resp = client().get(url).send().await.unwrap();
    assert!(
        resp.status() == 403 || resp.status() == 404,
        "expected 403/404, got {}",
        resp.status()
    );
    assert_ne!(resp.status(), 200);
}

#[tokio::test]
async fn auth_enforced() {
    let (addr, dir) = setup(Some("s3cr3t")).await;
    tokio::fs::write(dir.path().join("a.bin"), b"x")
        .await
        .unwrap();

    // No token -> 401
    let resp = client()
        .get(format!("http://{addr}/file/a.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Wrong token -> 401
    let resp = client()
        .get(format!("http://{addr}/file/a.bin"))
        .bearer_auth("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Correct token -> 200
    let resp = client()
        .get(format!("http://{addr}/file/a.bin"))
        .bearer_auth("s3cr3t")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn scpi_oneshot_ok() {
    let (addr, _dir) = setup(None).await;
    let resp = client()
        .post(format!("http://{addr}/scpi"))
        .body("SYST:VERS?")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["x-scpi-status"], "OK");
    assert_eq!(resp.headers()["x-scpi-access"], "Observer");
    let body = resp.text().await.unwrap();
    assert!(body.contains("1.0.0"), "unexpected body: {body:?}");
}

#[tokio::test]
async fn scpi_oneshot_command_error() {
    let (addr, _dir) = setup(None).await;
    let resp = client()
        .post(format!("http://{addr}/scpi"))
        .body("BADCMD foo")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert!(resp.headers().contains_key("x-scpi-error"));
}

#[tokio::test]
async fn scpi_rejects_newline_injection() {
    let (addr, _dir) = setup(None).await;
    for body in ["SYST:VERS?\nACC Controller", "RUNTitle?\r\nPOW OFF"] {
        let resp = client()
            .post(format!("http://{addr}/scpi"))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "body {body:?} should be rejected");
    }
}

#[tokio::test]
async fn scpi_tunnel_connects_and_runs() {
    use std::time::Duration;
    let (addr, _dir) = setup(None).await;
    // Connect a real QSConnection through the qslib-server SCPI tunnel.
    let conn = qslib_core::com::QSConnection::connect_server_tunnel(
        &addr.ip().to_string(),
        addr.port(),
        None,
    )
    .await
    .expect("tunnel connect");
    let mut recv = conn
        .send_command_bytes(&b"SYST:VERS?"[..])
        .await
        .expect("send command");
    let resp = recv
        .get_response_with_timeout(Duration::from_secs(5))
        .await
        .expect("no timeout")
        .expect("ok response");
    assert!(
        resp.to_string().contains("1.0.0"),
        "unexpected tunnel response: {resp}"
    );
}

#[tokio::test]
async fn scpi_access_cap_enforced() {
    let (addr, _dir) = setup(None).await;
    let resp = client()
        .post(format!("http://{addr}/scpi?access=Full"))
        .body("SYST:VERS?")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}
