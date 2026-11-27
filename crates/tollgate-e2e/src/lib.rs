//! A deployment for tests: a PostgreSQL database, a `tollgate-server` process
//! over it, and `pricing-api` processes that embed the library against that
//! server.
//!
//! Everything a scenario does goes through public HTTP, as an operator or a
//! customer would. The one exception is creating and dropping each
//! deployment's own database.
//!
//! The binaries and database come from the environment, which
//! `./scripts/check_e2e.sh` sets; see [`Settings::from_env`].

#![allow(
    clippy::disallowed_methods,
    reason = "a harness over real processes: deadlines and snapshot validity are read from the \
              same wall clock the processes under test run on"
)]

mod link;
mod pki;

pub use link::Link;

use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jiff::{SignedDuration, Timestamp};
use reqwest::{Method, StatusCode};
use sqlx::{Connection, PgConnection};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits,
    EnforcementMode, Generation, KeyId, OpIndex, PermissionBits, ResolvedLimits,
};
use tollgate_store::wire::{
    AccountResponse, CreateAccountRequest, IssueKeyRequest, IssuedKeyResponse,
    PublishSnapshotRequest,
};

// Public test fixtures. These credentials are never deployment defaults.
const INSTANCE_TOKEN: &str = "e2e-fixture-instance-credential-only";
const OPERATOR_TOKEN: &str = "e2e-fixture-operator-credential-only";
const ISSUER_SECRET: &str = "e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0e2e0";

/// How long any single condition may take to become true.
pub const DEADLINE: Duration = Duration::from_secs(30);

/// How instances and operators reach the server, and how the server knows an
/// instance.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Transport {
    /// Loopback plaintext with a bearer credential.
    Plaintext,
    /// TLS from a private CA, with a bearer credential.
    Tls,
    /// TLS in both directions: the client certificate is the identity.
    MutualTls,
}

/// Where the processes and the database come from.
#[derive(Clone)]
pub struct Settings {
    pg_url: String,
    server: PathBuf,
    app: PathBuf,
}

impl Settings {
    /// `TOLLGATE_PG_URL`, `TOLLGATE_E2E_SERVER_BIN` and
    /// `TOLLGATE_E2E_APP_BIN`. `None`, so a scenario can skip, when any is
    /// unset.
    ///
    /// # Panics
    ///
    /// When any is unset and `TOLLGATE_REQUIRE_E2E` is set: a gate must not
    /// pass by running nothing.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let read = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let settings = (|| {
            Some(Settings {
                pg_url: read("TOLLGATE_PG_URL")?,
                server: read("TOLLGATE_E2E_SERVER_BIN")?.into(),
                app: read("TOLLGATE_E2E_APP_BIN")?.into(),
            })
        })();
        if settings.is_none() {
            assert!(
                read("TOLLGATE_REQUIRE_E2E").is_none(),
                "TOLLGATE_REQUIRE_E2E is set, but the end-to-end environment is incomplete; \
                 run ./scripts/check_e2e.sh"
            );
            eprintln!("SKIPPED: run ./scripts/check_e2e.sh for the end-to-end scenarios");
        }
        settings
    }
}

/// Poll until `check` yields a value.
///
/// # Panics
///
/// After [`DEADLINE`] (or `within`), naming `what` and the last observation.
pub async fn eventually<T, F, Fut>(what: &str, within: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let deadline = Instant::now() + within;
    loop {
        match check().await {
            Ok(value) => return value,
            Err(last) => assert!(
                Instant::now() < deadline,
                "timed out after {within:?} waiting for {what}; last: {last}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A database that exists for one deployment and is dropped with it.
struct Database {
    admin_url: String,
    name: String,
    url: String,
}

impl Database {
    async fn create(admin_url: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Unique across concurrent test threads, processes and sessions.
        let name = format!(
            "tollgate_e2e_{}_{}_{}",
            std::process::id(),
            Timestamp::now().as_microsecond(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let (base, query) = match admin_url.split_once('?') {
            Some((base, query)) => (base, format!("?{query}")),
            None => (admin_url, String::new()),
        };
        let (server, _) = base
            .rsplit_once('/')
            .expect("TOLLGATE_PG_URL names a database");
        let url = format!("{server}/{name}{query}");
        // Errors are not printed: driver text can carry the URL's credentials.
        let mut admin = PgConnection::connect(admin_url)
            .await
            .unwrap_or_else(|_| panic!("cannot connect to TOLLGATE_PG_URL"));
        sqlx::raw_sql(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .unwrap_or_else(|_| panic!("cannot create a scenario database"));
        Database {
            admin_url: admin_url.to_owned(),
            name,
            url,
        }
    }
}

impl Database {
    /// `host:port` of the database server.
    fn address(&self) -> &str {
        authority(&self.url)
    }

    /// This database's URL, reaching the server at `address`.
    fn url_via(&self, address: &str) -> String {
        self.url.replacen(self.address(), address, 1)
    }
}

/// The `host:port` of a `scheme://user:password@host:port/database` URL.
///
/// # Panics
///
/// Without an explicit port, naming neither the URL nor its credentials.
fn authority(url: &str) -> &str {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme.split(['/', '?']).next().unwrap_or_default();
    let address = authority
        .rsplit_once('@')
        .map_or(authority, |(_, address)| address);
    assert!(
        address
            .rsplit_once(':')
            .is_some_and(|(_, port)| port.parse::<u16>().is_ok()),
        "TOLLGATE_PG_URL must name a host and port"
    );
    address
}

impl Drop for Database {
    fn drop(&mut self) {
        let admin_url = self.admin_url.clone();
        let statement = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        // On its own thread and runtime, so it also runs while a failed
        // scenario unwinds inside the test's runtime.
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()?;
            runtime.block_on(async {
                let mut admin = PgConnection::connect(&admin_url).await.ok()?;
                sqlx::raw_sql(&statement).execute(&mut admin).await.ok()
            })
        })
        .join();
        if !matches!(dropped, Ok(Some(_))) {
            eprintln!("could not drop scenario database {}", self.name);
        }
    }
}

/// A child process, killed when dropped, whose output is kept for failures.
struct Process {
    name: String,
    child: Child,
    address: String,
    output: Arc<Mutex<Vec<String>>>,
}

impl Process {
    /// Start `command` and wait for its `listening` line, which carries the
    /// address the kernel chose.
    async fn start(name: &str, mut command: Command) -> Self {
        let mut child = command
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("cannot start {name}: {error}"));
        let output = Arc::new(Mutex::new(Vec::new()));
        let (sender, mut listening) = tokio::sync::mpsc::unbounded_channel();
        let pipes: [Box<dyn Read + Send>; 2] = [
            Box::new(child.stdout.take().expect("stdout is piped")),
            Box::new(child.stderr.take().expect("stderr is piped")),
        ];
        for pipe in pipes {
            let output = Arc::clone(&output);
            let sender = sender.clone();
            // Drained for the life of the process: a full pipe would block it.
            std::thread::spawn(move || {
                for line in BufReader::new(pipe).lines() {
                    let Ok(line) = line else { return };
                    if line.contains(" listening")
                        && let Some(address) = line
                            .split_whitespace()
                            .find_map(|field| field.strip_prefix("bind="))
                    {
                        // Only the first is awaited: later sends find the
                        // receiver gone, which is expected.
                        drop(sender.send(address.to_owned()));
                    }
                    output.lock().expect("output lock").push(line);
                }
            });
        }
        drop(sender);
        let mut process = Process {
            name: name.to_owned(),
            child,
            address: String::new(),
            output,
        };
        process.address = tokio::time::timeout(DEADLINE, listening.recv())
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("{name} never listened"));
        process
    }

    fn kill(&mut self) {
        // A child that already exited cannot be killed; either way reap it.
        if let Err(error) = self.child.kill() {
            eprintln!("{}: kill: {error}", self.name);
        }
        if let Err(error) = self.child.wait() {
            eprintln!("{}: wait: {error}", self.name);
        }
    }

    /// Ask the process to stop as an orchestrator would, and require a clean
    /// exit.
    async fn terminate(mut self) {
        let signalled = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("the kill utility runs");
        assert!(signalled.success(), "cannot signal {}", self.name);
        let name = self.name.clone();
        let status = eventually(&format!("{name} to exit"), DEADLINE, || {
            let exited = self.child.try_wait();
            async move {
                match exited {
                    Ok(Some(status)) => Ok(status),
                    Ok(None) => Err("still running".to_owned()),
                    Err(error) => Err(error.to_string()),
                }
            }
        })
        .await;
        assert!(status.success(), "{name} exited with {status}");
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            self.kill();
        }
        if std::thread::panicking() {
            let output = self.output.lock().expect("output lock");
            let tail = output.len().saturating_sub(60);
            eprintln!("--- {} (last lines) ---", self.name);
            for line in &output[tail..] {
                eprintln!("{line}");
            }
        }
    }
}

/// How one application instance consults the control plane.
#[derive(Clone, Copy)]
pub struct AppOptions {
    pub refresh: Duration,
    pub lease_ttl: Duration,
    pub lease_units: u64,
    pub usage_flush: Duration,
}

impl Default for AppOptions {
    fn default() -> Self {
        AppOptions {
            refresh: Duration::from_millis(100),
            lease_ttl: Duration::from_secs(60),
            lease_units: 10_000,
            usage_flush: Duration::from_millis(25),
        }
    }
}

/// One `pricing-api` process.
pub struct App {
    process: Process,
    client: reqwest::Client,
}

/// A refusal, as a customer sees it.
#[derive(Debug, PartialEq, Eq)]
pub struct Refused {
    pub status: StatusCode,
    pub code: Option<String>,
}

impl App {
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.process.address)
    }

    /// Price one contract as the customer holding `secret`. `Ok` carries the
    /// units the response says were charged.
    ///
    /// # Errors
    ///
    /// The service's refusal.
    pub async fn price(&self, secret: &str) -> Result<u64, Refused> {
        let body = serde_json::json!({"contracts": [{
            "spot": 100.0, "strike": 105.0, "rate": 0.05, "vol": 0.2, "tte_years": 0.25
        }]});
        let response = self
            .client
            .post(self.url("/v1/price"))
            .bearer_auth(secret)
            .json(&body)
            .send()
            .await
            .expect("the application answers");
        let status = response.status();
        let body: serde_json::Value = response.json().await.unwrap_or_default();
        if status == StatusCode::OK {
            Ok(body["metadata"]["units_charged"]
                .as_u64()
                .expect("a priced response states its charge"))
        } else {
            Err(Refused {
                status,
                code: body["code"].as_str().map(str::to_owned),
            })
        }
    }

    /// The balance this instance reports for the customer, if it reports one.
    pub async fn budget(&self, secret: &str) -> Option<u64> {
        let response = self
            .client
            .get(self.url("/v1/budget"))
            .bearer_auth(secret)
            .send()
            .await
            .expect("the application answers");
        if response.status() != StatusCode::OK {
            return None;
        }
        let body: serde_json::Value = response.json().await.expect("a budget body");
        body["balance"].as_u64()
    }

    /// The unfunded units this instance reports having extended, across every
    /// account.
    ///
    /// # Panics
    ///
    /// When the instance does not report the figure.
    pub async fn overage_spent(&self) -> u64 {
        let body: serde_json::Value = self
            .client
            .get(self.url("/metrics"))
            .send()
            .await
            .expect("the application answers")
            .json()
            .await
            .expect("a metrics body");
        body["total_overage_spent"]
            .as_u64()
            .expect("metrics report the overage spent")
    }

    /// Whether the readiness probe passes.
    pub async fn ready(&self) -> bool {
        self.client
            .get(self.url("/readyz"))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::OK)
    }

    /// SIGTERM, and require the instance to drain and exit cleanly.
    pub async fn terminate(self) {
        self.process.terminate().await;
    }

    /// SIGKILL: nothing is flushed or released.
    pub fn kill(mut self) {
        self.process.kill();
    }
}

/// One customer: an account, its credential, and what responses charged it.
pub struct Customer {
    pub account: AccountId,
    pub key: KeyId,
    pub secret: String,
}

struct PriceOp;
impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

/// The control plane and its database. Application instances are held by the
/// scenario; declare them after this so they stop first.
pub struct ControlPlane {
    settings: Settings,
    server: Option<Process>,
    transport: Transport,
    bind: String,
    client: reqwest::Client,
    directory: tempfile::TempDir,
    database_link: Link,
    database: Database,
}

impl ControlPlane {
    /// A fresh database, migrated by a `tollgate-server` started over it.
    pub async fn start(settings: Settings) -> Self {
        Self::start_with(settings, Transport::Plaintext).await
    }

    /// As [`Self::start`], with the control plane served over `transport`.
    pub async fn start_with(settings: Settings, transport: Transport) -> Self {
        let database = Database::create(&settings.pg_url).await;
        let directory = tempfile::tempdir().expect("a temporary directory");
        let write = |name: &str, contents: &str| {
            std::fs::write(directory.path().join(name), contents).expect("a fixture file");
        };
        write("instance.token", INSTANCE_TOKEN);
        write("operator.token", OPERATOR_TOKEN);
        write("issuer.secret", ISSUER_SECRET);
        let mut security = serde_json::json!({
            "bearers": [
                {"identity": "e2e-operator", "role": "operator",
                 "token_file": "operator.token"},
            ],
            "issuer": {"secret_file": "issuer.secret"},
        });
        if transport != Transport::MutualTls {
            security["bearers"]
                .as_array_mut()
                .expect("a bearer list")
                .push(
                    serde_json::json!({"identity": "e2e-instance", "role": "instance",
                    "token_file": "instance.token"}),
                );
        }
        let mut client = reqwest::Client::builder();
        if transport != Transport::Plaintext {
            let pki = pki::Pki::generate();
            write("ca.pem", &pki.ca);
            write("server.pem", &pki.server);
            write("server.key", &pki.server_key);
            write("instance.pem", &pki.instance.certificate);
            write("instance.identity", &pki.instance.identity());
            write("stranger.identity", &pki.stranger.identity());
            security["tls"] = serde_json::json!({
                "certificate": "server.pem", "private_key": "server.key",
            });
            if transport == Transport::MutualTls {
                security["tls"]["client_ca"] = "ca.pem".into();
                security["certificates"] = serde_json::json!([{
                    "identity": "e2e-instance", "role": "instance",
                    "certificate": "instance.pem",
                }]);
            }
            client = client.use_rustls_tls().add_root_certificate(
                reqwest::Certificate::from_pem(pki.ca.as_bytes()).expect("a CA certificate"),
            );
        }
        write("security.json", &security.to_string());
        let database_link = Link::to(database.address().to_owned()).await;
        let mut plane = ControlPlane {
            settings,
            server: None,
            transport,
            bind: "127.0.0.1:0".to_owned(),
            client: client.build().expect("an operator client"),
            directory,
            database_link,
            database,
        };
        plane.start_server().await;
        plane
    }

    /// Start the server, on the same address as before after the first time.
    pub async fn start_server(&mut self) {
        assert!(self.server.is_none(), "the server is already running");
        let mut command = Command::new(&self.settings.server);
        command
            .env_clear()
            .env("TOLLGATE_STORE", "postgres")
            .env(
                "TOLLGATE_PG_URL",
                self.database.url_via(&self.database_link.address()),
            )
            .env("TOLLGATE_BIND", &self.bind)
            .env("TOLLGATE_RECLAIM_INTERVAL_SECS", "1")
            .env(
                "TOLLGATE_SECURITY_CONFIG",
                self.directory.path().join("security.json"),
            );
        let server = Process::start("tollgate-server", command).await;
        self.bind.clone_from(&server.address);
        self.server = Some(server);
    }

    /// Kill the server: an outage, not a drain.
    pub fn kill_server(&mut self) {
        let mut server = self.server.take().expect("the server is running");
        server.kill();
    }

    /// The server's connection to its database, to break and restore.
    #[must_use]
    pub fn database_link(&self) -> &Link {
        &self.database_link
    }

    /// A separate network path to the server, for an instance a scenario will
    /// partition.
    pub async fn link_to_server(&self) -> Link {
        Link::to(self.bind.clone()).await
    }

    /// An application instance against this control plane, once ready.
    pub async fn start_app(&self, name: &str, options: AppOptions) -> App {
        self.start_app_at(name, options, &self.bind).await
    }

    /// As [`Self::start_app`], reaching the server only through `link`.
    pub async fn start_app_via(&self, name: &str, options: AppOptions, link: &Link) -> App {
        self.start_app_at(name, options, &link.address()).await
    }

    async fn start_app_at(&self, name: &str, options: AppOptions, server: &str) -> App {
        let app = self
            .spawn_app(name, options, server, "instance.identity")
            .await;
        eventually(&format!("{name} to become ready"), DEADLINE, || async {
            app.ready()
                .await
                .then_some(())
                .ok_or_else(|| "not ready".to_owned())
        })
        .await;
        app
    }

    /// An instance whose client certificate the server does not know, under
    /// [`Transport::MutualTls`]. It listens, and is not waited on.
    ///
    /// # Panics
    ///
    /// Under any other transport, where no certificate identifies an instance.
    pub async fn start_unknown_app(&self, name: &str, options: AppOptions) -> App {
        assert_eq!(self.transport, Transport::MutualTls);
        self.spawn_app(name, options, &self.bind, "stranger.identity")
            .await
    }

    fn url(&self, server: &str, path: &str) -> String {
        let scheme = match self.transport {
            Transport::Plaintext => "http",
            Transport::Tls | Transport::MutualTls => "https",
        };
        format!("{scheme}://{server}{path}")
    }

    async fn spawn_app(
        &self,
        name: &str,
        options: AppOptions,
        server: &str,
        identity: &str,
    ) -> App {
        let mut command = Command::new(&self.settings.app);
        let path = |file: &str| self.directory.path().join(file);
        command
            .env_clear()
            .env("PRICING_BIND", "127.0.0.1:0")
            .env("PRICING_CONTROL_URL", self.url(server, ""))
            .env("PRICING_CREDENTIAL_SECRET_FILE", path("issuer.secret"))
            .env(
                "PRICING_REFRESH_MS",
                options.refresh.as_millis().to_string(),
            )
            .env(
                "PRICING_LEASE_TTL_SECS",
                options.lease_ttl.as_secs().to_string(),
            )
            .env("PRICING_LEASE_UNITS", options.lease_units.to_string())
            .env(
                "PRICING_USAGE_FLUSH_MS",
                options.usage_flush.as_millis().to_string(),
            );
        if self.transport != Transport::Plaintext {
            command.env("PRICING_CONTROL_CA_FILE", path("ca.pem"));
        }
        if self.transport == Transport::MutualTls {
            command.env("PRICING_CONTROL_IDENTITY_FILE", path(identity));
        } else {
            command.env("PRICING_CONTROL_TOKEN_FILE", path("instance.token"));
        }
        App {
            process: Process::start(name, command).await,
            client: reqwest::Client::new(),
        }
    }

    /// An operator call, requiring `expected`.
    async fn operator(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
        expected: StatusCode,
    ) -> serde_json::Value {
        let mut request = self
            .client
            .request(
                method.clone(),
                self.url(&self.bind, &format!("/v1/admin{path}")),
            )
            .bearer_auth(OPERATOR_TOKEN);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("the control plane answers");
        let status = response.status();
        let text = response.text().await.expect("a response body");
        // The body is not echoed: an issuance response carries a secret.
        assert_eq!(status, expected, "{method} {path}");
        serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
    }

    /// Create a funded account, issue it a credential and publish the policy
    /// that credential authorizes against.
    pub async fn onboard(&self, account: u128, balance: u64) -> Customer {
        self.onboard_with(account, balance, EnforcementMode::Strict)
            .await
    }

    /// As [`Self::onboard`], with the account's enforcement mode.
    pub async fn onboard_with(
        &self,
        account: u128,
        balance: u64,
        mode: EnforcementMode,
    ) -> Customer {
        let account = AccountId(account);
        let key = KeyId(account.0);
        let json = |value: Result<serde_json::Value, serde_json::Error>| {
            Some(value.expect("wire types serialize"))
        };
        self.operator(
            Method::POST,
            "/accounts",
            json(serde_json::to_value(CreateAccountRequest {
                account_id: account,
                initial_balance: CostUnits(balance),
                status: AccountStatus::Active,
            })),
            StatusCode::CREATED,
        )
        .await;
        let keys = format!("/accounts/{:032x}/keys", account.0);
        let issued: IssuedKeyResponse = serde_json::from_value(
            self.operator(
                Method::POST,
                &keys,
                json(serde_json::to_value(IssueKeyRequest {
                    key_id: key,
                    max_active_keys: std::num::NonZeroUsize::MIN,
                    not_after: None,
                })),
                StatusCode::CREATED,
            )
            .await,
        )
        .unwrap_or_else(|_| panic!("an issuance response"));
        let snapshot = AccountSnapshot::builder(
            account,
            Generation(1),
            AccountStatus::Active,
            Timestamp::now() + SignedDuration::from_hours(1),
            PermissionBits::bit(0),
            ResolvedLimits::new(1_024).with_weighted_rate(5_000_000, 10_000_000),
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .capacity_class(CapacityClass::Assured)
        .enforcement_mode(mode)
        .build();
        self.operator(
            Method::PUT,
            &format!("{keys}/{:032x}/snapshot", key.0),
            json(serde_json::to_value(PublishSnapshotRequest {
                snapshot: Arc::new(snapshot),
            })),
            StatusCode::NO_CONTENT,
        )
        .await;
        Customer {
            account,
            key,
            secret: issued.secret,
        }
    }

    /// Revoke the customer's credential.
    pub async fn revoke(&self, customer: &Customer) {
        let path = format!(
            "/accounts/{:032x}/keys/{:032x}",
            customer.account.0, customer.key.0
        );
        self.operator(Method::DELETE, &path, None, StatusCode::OK)
            .await;
    }

    /// Whether the control plane answers a ledger read within `within`. A
    /// scenario that breaks its database uses this to show the fault is real.
    pub async fn ledger_answers(&self, account: AccountId, within: Duration) -> bool {
        let request = self
            .client
            .get(self.url(
                &self.bind,
                &format!("/v1/admin/accounts/{:032x}", account.0),
            ))
            .bearer_auth(OPERATOR_TOKEN)
            .send();
        matches!(
            tokio::time::timeout(within, request).await,
            Ok(Ok(response)) if response.status() == StatusCode::OK
        )
    }

    /// The ledger's view of the account.
    ///
    /// # Panics
    ///
    /// If the funding equation does not hold (INVARIANTS.md GL-1): checked on
    /// every read, so no scenario can observe a broken ledger and pass.
    pub async fn ledger(&self, account: AccountId) -> AccountResponse {
        let view: AccountResponse = serde_json::from_value(
            self.operator(
                Method::GET,
                &format!("/accounts/{:032x}", account.0),
                None,
                StatusCode::OK,
            )
            .await,
        )
        .expect("an account response");
        let funded = u128::from(view.deposited.get()) + u128::from(view.overage_recorded.get());
        let held: u128 = [
            view.balance,
            view.outstanding_lease_grants,
            view.settled_usage,
            view.expired_allowance,
            view.settlement_loss,
        ]
        .iter()
        .map(|units| u128::from(units.get()))
        .sum();
        assert_eq!(funded, held, "the ledger does not conserve: {view:?}");
        view
    }

    /// Wait until the ledger satisfies `condition`.
    pub async fn ledger_reaches(
        &self,
        what: &str,
        account: AccountId,
        within: Duration,
        condition: impl Fn(&AccountResponse) -> bool,
    ) -> AccountResponse {
        eventually(what, within, || async {
            let view = self.ledger(account).await;
            if condition(&view) {
                Ok(view)
            } else {
                Err(format!("{view:?}"))
            }
        })
        .await
    }
}
