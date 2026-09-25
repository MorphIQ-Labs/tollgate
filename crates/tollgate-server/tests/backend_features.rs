//! The server binary advertises exactly the backends compiled into it.
//!
//! Running a fresh process is also the non-racy witness that `init_tracing`
//! installs the binary's global subscriber: without that install, each error
//! event asserted below would be discarded. A unit test cannot make the same
//! claim from `tracing::dispatcher::has_been_set()`, because any concurrent
//! thread-scoped `with_default` call permanently flips that process-global
//! history bit.

use std::process::{Command, Output};

// Keep diagnostic assertions independent of the invoking terminal's color policy.
fn server_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tollgate-server"));
    command.env("NO_COLOR", "1");
    command
}

fn run_with_backend(backend: &str) -> Output {
    server_command()
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
        .env("TOLLGATE_STORE", backend)
        .env("RUST_LOG", "error")
        .env_remove("TOLLGATE_PG_URL")
        .env_remove("TOLLGATE_SECURITY_CONFIG")
        .output()
        .expect("tollgate-server binary must start")
}

fn diagnostics(output: &Output) -> String {
    let mut bytes = output.stdout.clone();
    bytes.extend_from_slice(&output.stderr);
    String::from_utf8(bytes).expect("server diagnostics must be UTF-8")
}

#[test]
fn unknown_backend_lists_only_compiled_backends() {
    let output = run_with_backend("unsupported");
    assert_eq!(output.status.code(), Some(2));

    let expected = if cfg!(feature = "postgres") {
        "memory|postgres"
    } else {
        "memory"
    };
    assert!(
        diagnostics(&output).contains(&format!("unknown TOLLGATE_STORE (expected {expected})")),
        "diagnostic must list only the backends this binary can run"
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_backend_is_available_when_compiled() {
    let output = run_with_backend("postgres");
    assert_eq!(output.status.code(), Some(2));

    let diagnostic = diagnostics(&output);
    assert!(
        diagnostic.contains("TOLLGATE_STORE=postgres requires TOLLGATE_PG_URL"),
        "the compiled Postgres arm must validate its required configuration"
    );
    assert!(!diagnostic.contains("unknown TOLLGATE_STORE"));
}

#[cfg(not(feature = "postgres"))]
#[test]
fn postgres_backend_fails_closed_when_not_compiled() {
    let output = run_with_backend("postgres");
    assert_eq!(output.status.code(), Some(2));

    let diagnostic = diagnostics(&output);
    assert!(diagnostic.contains("unknown TOLLGATE_STORE (expected memory)"));
    assert!(
        !diagnostic.contains("TOLLGATE_PG_URL"),
        "an uncompiled backend must not enter Postgres configuration"
    );
}

#[test]
fn help_and_version_precede_configuration_and_respect_the_end_marker() {
    for argument in ["--help", "-h", "--version", "-V"] {
        let output = server_command()
            .args(["unknown", argument])
            .env("TOKIO_WORKER_THREADS", "0")
            .env("TOLLGATE_STORE", "invalid")
            .env_remove("TOLLGATE_SECURITY_CONFIG")
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(diagnostics(&output).contains(env!("CARGO_PKG_VERSION")));
    }
    let output = server_command()
        .args(["--", "--help"])
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(diagnostics(&output).contains("unexpected argument"));

    let output = server_command()
        .args(["--version", "--help"])
        .output()
        .unwrap();
    assert_eq!(
        diagnostics(&output),
        format!("tollgate-server {}\n", env!("CARGO_PKG_VERSION"))
    );
    let output = server_command()
        .args(["--"])
        .env("TOLLGATE_STORE", "invalid")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(diagnostics(&output).contains("unknown TOLLGATE_STORE"));
}

#[cfg(unix)]
#[test]
fn server_native_arguments_cannot_panic_or_hide_help_and_version() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    for flag in ["--help", "--version"] {
        let output = server_command()
            .arg(OsString::from_vec(vec![0xff]))
            .arg(flag)
            .env("TOLLGATE_STORE", "invalid")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
    }
    for leading in [vec![], vec!["--"]] {
        let output = server_command()
            .args(leading)
            .arg(OsString::from_vec(vec![0xff]))
            .env("TOLLGATE_STORE", "invalid")
            .env("RUST_LOG", "off")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(diagnostics(&output).contains("unexpected argument"));
        assert!(!diagnostics(&output).contains("panicked"));
    }
}

#[test]
fn credentials_are_required_even_for_loopback_and_invalid_intervals_are_rejected() {
    let output = run_with_backend("memory");
    assert_eq!(output.status.code(), Some(2));
    assert!(diagnostics(&output).contains("TOLLGATE_SECURITY_CONFIG is required"));
    for interval in ["invalid", "0"] {
        let output = server_command()
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", interval)
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(diagnostics(&output).contains("TOLLGATE_RECLAIM_INTERVAL_SECS must be"));
    }
}

#[test]
fn normal_log_verbosity_cannot_silence_the_binarys_audit_target() {
    use std::io::BufRead;
    use std::process::Stdio;
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").unwrap();
    let mut child = server_command()
        .env("TOLLGATE_STORE", "memory")
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
        .env("TOLLGATE_SECURITY_CONFIG", manifest)
        .env("RUST_LOG", "off,tollgate::audit=off")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        let result = std::io::BufReader::new(stdout).read_line(&mut line);
        sender
            .send((result, line))
            .expect("the receiver outlives the reader");
    });
    let first = received.recv_timeout(std::time::Duration::from_secs(5));
    child
        .kill()
        .expect("the configured server is still running");
    child.wait().unwrap();
    reader.join().unwrap();
    let (result, line) = first.expect("audit must remain enabled with normal logging disabled");
    result.unwrap();
    assert!(
        line.contains("administrative audit events remain enabled"),
        "{line}"
    );
}

#[test]
fn the_binary_refuses_an_exposed_plaintext_listener_before_opening_the_backend() {
    use std::process::Stdio;
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").unwrap();
    for backend in ["memory", "postgres"]
        .into_iter()
        .filter(|backend| *backend == "memory" || cfg!(feature = "postgres"))
    {
        let mut child = server_command()
            .env("TOLLGATE_STORE", backend)
            .env("TOLLGATE_PG_URL", "invalid-fixture-url")
            .env("TOLLGATE_BIND", "0.0.0.0:0")
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
            .env("TOLLGATE_SECURITY_CONFIG", &manifest)
            .env("RUST_LOG", "error")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("exposed plaintext listener must refuse startup");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success());
        assert!(diagnostics(&output).contains("non-loopback listeners require TLS"));
    }
}

#[cfg(feature = "postgres")]
#[test]
fn backend_startup_failure_never_discloses_connection_strings_or_driver_text() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").unwrap();
    // Public fixtures. An invalid port or URL fails before any connection.
    for url in [
        "postgres://user:fixture-startup-sensitive-70@localhost:invalid/db?password=fixture-startup-sensitive-70",
        "host=localhost port=invalid password=fixture-startup-sensitive-70",
        "malformed-fixture-startup-sensitive-70",
    ] {
        let output = server_command()
            .env("TOLLGATE_STORE", "postgres")
            .env("TOLLGATE_PG_URL", url)
            .env("TOLLGATE_BIND", "127.0.0.1:0")
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
            .env("TOLLGATE_SECURITY_CONFIG", &manifest)
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let diagnostic = diagnostics(&output);
        assert!(diagnostic.contains("cannot initialize postgres"));
        assert!(diagnostic.contains("operation=\"connect\""));
        assert!(!diagnostic.contains("fixture-startup-sensitive-70"));
    }
}

mod common;

/// Issuer secret for the stock-binary cases (#143). Test-only.
const BINARY_ISSUER: &str = "b143b143b143b143b143b143b143b143b143b143b143b143b143b143b143b143";

/// A manifest with instance and operator bearers and, optionally, an issuer.
fn issuer_manifest(directory: &std::path::Path, issuer: Option<&str>) -> std::path::PathBuf {
    let manifest = directory.join("security.json");
    std::fs::write(directory.join("instance.token"), common::INSTANCE).unwrap();
    std::fs::write(directory.join("operator.token"), common::OPERATOR).unwrap();
    let mut body = serde_json::json!({"bearers": [
        {"identity": "instance", "role": "instance", "token_file": "instance.token"},
        {"identity": "operator", "role": "operator", "token_file": "operator.token"},
    ]});
    if let Some(secret) = issuer {
        std::fs::write(directory.join("issuer.secret"), format!("{secret}\n")).unwrap();
        body["issuer"] = serde_json::json!({"secret_file": "issuer.secret"});
    }
    std::fs::write(&manifest, body.to_string()).unwrap();
    manifest
}

/// A running stock binary, killed on drop.
struct Running {
    child: std::process::Child,
    address: String,
}

impl Drop for Running {
    fn drop(&mut self) {
        // A child that already exited cannot be killed; either way it is reaped.
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

/// Start the memory binary and read its bound address from the `listening`
/// line: the port is chosen by the kernel and reported nowhere else.
fn start_binary(manifest: &std::path::Path) -> Running {
    use std::io::BufRead;
    use std::process::Stdio;
    let mut child = server_command()
        .env("TOLLGATE_STORE", "memory")
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
        .env("TOLLGATE_SECURITY_CONFIG", manifest)
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { return };
            if line.contains("tollgate-server listening") && sender.send(line).is_err() {
                return;
            }
        }
    });
    let line = received
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the binary reports its listener");
    let address = line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("bind="))
        .expect("the listening line carries its address")
        .to_owned();
    assert!(line.contains("issuance=\""), "{line}");
    Running { child, address }
}

async fn send(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: String,
    body: Option<serde_json::Value>,
) -> (reqwest::StatusCode, serde_json::Value) {
    let mut request = client.request(method, url).bearer_auth(common::OPERATOR);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
    )
}

/// The acceptance path a deployment depends on, against the stock binary and
/// its own configuration path (#143): issue once, a resend conflicts, a
/// verifier holding the same secret accepts the credential through the
/// `/v1/keys` projection, its policy binds by key, and revocation plus
/// withdrawal retire it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_issues_from_a_manifest_configured_issuer() {
    use reqwest::{Method, StatusCode};
    let directory = tempfile::tempdir().unwrap();
    let server = start_binary(&issuer_manifest(directory.path(), Some(BINARY_ISSUER)));
    let base = format!("http://{}/v1", server.address);
    let client = reqwest::Client::new();
    let account = format!("{:032x}", 1);
    let key = format!("{:032x}", 42);

    let (status, _) = send(
        &client,
        Method::POST,
        format!("{base}/admin/accounts"),
        Some(
            serde_json::json!({"account_id": account, "initial_balance": 100, "status": "Active"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let issue = format!("{base}/admin/accounts/{account}/keys");
    let request = serde_json::json!({"key_id": key, "max_active_keys": 3});
    let (status, issued) = send(&client, Method::POST, issue.clone(), Some(request.clone())).await;
    assert_eq!(status, StatusCode::CREATED);
    let secret = issued["secret"]
        .as_str()
        .expect("disclosed once")
        .to_owned();
    let (status, resent) = send(&client, Method::POST, issue.clone(), Some(request)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(resent["code"], "credential-exists");
    assert!(resent.get("secret").is_none());

    // A verifier configured with the same stored value, as text.
    let keys = tollgate_client::KeyManager::spawn(
        common::http(format!("http://{}", server.address)),
        BINARY_ISSUER.as_bytes(),
        std::sync::Arc::new(tollgate_store::SystemClock),
        tollgate_client::KeyManagerConfig {
            refresh_interval: std::time::Duration::from_millis(20),
            ..Default::default()
        },
    )
    .unwrap();
    let verifier = keys.verifier();
    // Presented exactly as disclosed, as a `Bearer` header carries it: no
    // decoding step, which is the form every embedder forwards.
    let presented = secret.as_bytes();
    let principal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(verified) = tollgate_auth::CredentialVerifier::verify(&verifier, presented)
            {
                break verified.principal;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the projected credential verifies under the manifest's secret");

    // Bind its policy by the handles the operator holds.
    let binding = format!("{issue}/{key}/snapshot");
    let snapshot = tollgate_core::AccountSnapshot::builder(
        tollgate_core::AccountId(1),
        tollgate_core::Generation(1),
        tollgate_core::AccountStatus::Active,
        tollgate_store::Clock::now(&tollgate_store::SystemClock)
            + jiff::SignedDuration::from_hours(1),
        tollgate_core::PermissionBits(0),
        tollgate_core::ResolvedLimits::new(1),
        std::sync::Arc::new(
            tollgate_core::CostTable::builder(
                tollgate_core::CostUnits(1),
                tollgate_core::CostUnits(1),
            )
            .build(),
        ),
    )
    .build();
    let (status, _) = send(
        &client,
        Method::PUT,
        binding.clone(),
        Some(serde_json::json!({"snapshot": snapshot})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let fetched = client
        .get(format!("{base}/snapshots/{principal}"))
        .bearer_auth(common::INSTANCE)
        .send()
        .await
        .unwrap();
    assert_eq!(fetched.status(), StatusCode::OK);

    let (status, revoked) = send(&client, Method::DELETE, format!("{issue}/{key}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(revoked["retired"], true);
    let (status, _) = send(&client, Method::DELETE, binding, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let gone = client
        .get(format!("{base}/snapshots/{principal}"))
        .bearer_auth(common::INSTANCE)
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), StatusCode::GONE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_without_an_issuer_answers_issuance_unsupported() {
    use reqwest::{Method, StatusCode};
    let directory = tempfile::tempdir().unwrap();
    let server = start_binary(&issuer_manifest(directory.path(), None));
    let base = format!("http://{}/v1", server.address);
    let client = reqwest::Client::new();
    let account = format!("{:032x}", 1);
    send(
        &client,
        Method::POST,
        format!("{base}/admin/accounts"),
        Some(serde_json::json!({"account_id": account, "initial_balance": 0, "status": "Active"})),
    )
    .await;
    let (status, problem) = send(
        &client,
        Method::POST,
        format!("{base}/admin/accounts/{account}/keys"),
        Some(serde_json::json!({"key_id": format!("{:032x}", 7), "max_active_keys": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(problem["code"], "issuance-unsupported");
}

/// A bad issuer refuses startup before the listener opens, and never echoes
/// the secret it refused.
#[test]
fn the_binary_refuses_an_invalid_issuer_before_listening() {
    let directory = tempfile::tempdir().unwrap();
    let short = &BINARY_ISSUER[..63];
    type Mutation = Box<dyn Fn(&std::path::Path)>;
    let cases: [(&str, &str, Mutation); 3] = [
        (
            "short",
            "issuer secret must be exactly 64 lowercase hexadecimal characters",
            Box::new(|_| {}),
        ),
        (
            "bearer",
            "issuer secret must differ from every bearer credential",
            Box::new(|dir| std::fs::write(dir.join("operator.token"), BINARY_ISSUER).unwrap()),
        ),
        (
            "missing",
            "cannot read security manifest or referenced credential file",
            Box::new(|dir| std::fs::remove_file(dir.join("issuer.secret")).unwrap()),
        ),
    ];
    for (case, reason, mutate) in cases {
        let manifest = issuer_manifest(
            directory.path(),
            Some(if case == "short" {
                short
            } else {
                BINARY_ISSUER
            }),
        );
        mutate(directory.path());
        let output = server_command()
            .env("TOLLGATE_STORE", "memory")
            .env("TOLLGATE_BIND", "127.0.0.1:0")
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "5")
            .env("TOLLGATE_SECURITY_CONFIG", &manifest)
            .env("RUST_LOG", "info")
            .output()
            .unwrap();
        assert!(!output.status.success(), "{case}");
        let text = diagnostics(&output);
        assert!(
            !text.contains("tollgate-server listening"),
            "{case}: {text}"
        );
        assert!(text.contains(reason), "{case}: {text}");
        assert!(!text.contains(short), "{case}: the secret is never echoed");
    }
}
