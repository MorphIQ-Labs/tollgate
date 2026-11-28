//! A deployment for tests: a PostgreSQL database, a `tollgate-server` process
//! over it, and `pricing-api` processes that embed the library against that
//! server.
//!
//! Everything a scenario does goes through public HTTP, as an operator or a
//! customer would. There are two exceptions, both on the deployment's own
//! database: creating and dropping it, and
//! [`ControlPlane::end_budget_period`], which stands in for the passage of a
//! calendar month.
//!
//! The binaries and database come from the environment, which
//! `./scripts/check_e2e.sh` sets; see [`Settings::from_env`].
//! `./scripts/check_e2e_upgrade.sh` also builds earlier releases; see
//! [`Settings::previous`].

#![allow(
    clippy::disallowed_methods,
    reason = "a harness over real processes: deadlines and snapshot validity are read from the \
              same wall clock the processes under test run on"
)]

mod link;
mod load;
mod pki;

pub use link::Link;
pub use load::{LedgerWatch, Load, Tally, estimates_reach_ledger};

use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jiff::{SignedDuration, Timestamp};
use reqwest::{Method, StatusCode};
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, BudgetSchedule, CapacityClass, CostTable, CostUnits,
    EnforcementMode, Generation, KeyId, OpIndex, Period, PermissionBits, ResolvedLimits,
};
use tollgate_store::wire::{
    AccountResponse, CreateAccountRequest, IssueKeyRequest, IssuedKeyResponse,
    PublishSnapshotRequest, SetBudgetRequest,
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

/// The database a deployment runs over.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DatabaseTransport {
    /// Whatever `TOLLGATE_PG_URL` asks for: loopback, in this harness.
    AsConfigured,
    /// A TLS database, with `sslmode=verify-full` against its private CA.
    VerifiedTls,
}

/// A database address the server must not start over.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UntrustedDatabase {
    /// `verify-full` with a CA that did not sign the certificate.
    WrongAuthority,
    /// `verify-full` for a host name the certificate does not carry.
    WrongName,
    /// `require` of a database that has no TLS.
    NoTls,
}

/// One release's two binaries.
#[derive(Clone)]
pub struct Build {
    server: PathBuf,
    app: PathBuf,
}

impl Build {
    /// Require both binaries to report `version`, so a scenario about an
    /// earlier release cannot run something else under its name.
    ///
    /// # Panics
    ///
    /// When either binary does not run or reports another version.
    pub fn assert_version(&self, version: &str) {
        for (binary, name) in [
            (&self.server, "tollgate-server"),
            (&self.app, "pricing-api"),
        ] {
            let output = Command::new(binary)
                .arg("--version")
                .env_clear()
                .output()
                .unwrap_or_else(|error| panic!("cannot run {name} {version}: {error}"));
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).trim(),
                format!("{name} {version}"),
                "{} is not the release it is used as",
                binary.display()
            );
        }
    }
}

/// Where the processes and the databases come from.
#[derive(Clone)]
pub struct Settings {
    pg_url: String,
    pg_tls_url: String,
    pg_tls_ca: String,
    pg_tls_wrong_ca: String,
    pg_plaintext_url: String,
    current: Build,
}

impl Settings {
    /// `TOLLGATE_PG_URL`, `TOLLGATE_E2E_SERVER_BIN`,
    /// `TOLLGATE_E2E_APP_BIN` and the `TOLLGATE_PG_TLS_*` and
    /// `TOLLGATE_PG_PLAINTEXT_URL` databases that
    /// `./scripts/with_tls_postgres.sh` provides. `None`, so a scenario can
    /// skip, when any is unset.
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
                pg_tls_url: read("TOLLGATE_PG_TLS_URL")?,
                pg_tls_ca: read("TOLLGATE_PG_TLS_CA")?,
                pg_tls_wrong_ca: read("TOLLGATE_PG_TLS_WRONG_CA")?,
                pg_plaintext_url: read("TOLLGATE_PG_PLAINTEXT_URL")?,
                current: Build {
                    server: read("TOLLGATE_E2E_SERVER_BIN")?.into(),
                    app: read("TOLLGATE_E2E_APP_BIN")?.into(),
                },
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

    /// The binaries under test.
    #[must_use]
    pub fn current(&self) -> &Build {
        &self.current
    }

    /// The binaries of the release tagged `tag`, from
    /// `TOLLGATE_E2E_PREVIOUS_DIR`.
    ///
    /// # Panics
    ///
    /// When that is unset or holds no such binaries: a scenario about an
    /// earlier release must not pass without one.
    #[must_use]
    pub fn previous(&self, tag: &str) -> Build {
        let directory = std::env::var_os("TOLLGATE_E2E_PREVIOUS_DIR")
            .map(PathBuf::from)
            .expect("TOLLGATE_E2E_PREVIOUS_DIR is unset; run ./scripts/check_e2e_upgrade.sh");
        let binary = |name: &str| {
            let path = directory.join(tag).join(name);
            assert!(path.is_file(), "no {name} built for {tag}");
            path
        };
        Build {
            server: binary("tollgate-server"),
            app: binary("pricing-api"),
        }
    }
}

/// Start a `tollgate-server` over a database it cannot trust, require it to
/// give up without listening, and return what it logged.
///
/// # Panics
///
/// If the server starts, outlives [`DEADLINE`] or logs the database password.
pub async fn refused_start(settings: &Settings, database: UntrustedDatabase) -> String {
    let url = match database {
        UntrustedDatabase::WrongAuthority => format!(
            "{}?sslmode=verify-full&sslrootcert={}",
            settings.pg_tls_url, settings.pg_tls_wrong_ca
        ),
        UntrustedDatabase::WrongName => format!(
            "{}?sslmode=verify-full&sslrootcert={}&host=another-server.invalid&hostaddr=127.0.0.1",
            settings.pg_tls_url, settings.pg_tls_ca
        ),
        UntrustedDatabase::NoTls => format!("{}?sslmode=require", settings.pg_plaintext_url),
    };
    let password = authority_and_password(&url).1.to_owned();
    let directory = tempfile::tempdir().expect("a temporary directory");
    let manifest = directory.path().join("security.json");
    std::fs::write(&manifest, "{}").expect("a fixture file");
    let mut child = Command::new(&settings.current.server)
        .env_clear()
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .env("TOLLGATE_STORE", "postgres")
        .env("TOLLGATE_PG_URL", url)
        .env("TOLLGATE_BIND", "127.0.0.1:0")
        .env("TOLLGATE_SECURITY_CONFIG", &manifest)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("tollgate-server starts");
    // A refusal logs a few lines, which a pipe holds until the process exits.
    let deadline = Instant::now() + DEADLINE;
    while child.try_wait().expect("the child can be polled").is_none() {
        if Instant::now() >= deadline {
            child.kill().expect("the child can be killed");
            child.wait().expect("the child can be reaped");
            panic!("tollgate-server did not give up on {database:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let output = child.wait_with_output().expect("the child's output");
    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    assert!(
        !log.contains(&password),
        "a refusal logged the database password"
    );
    assert_eq!(output.status.code(), Some(2), "{database:?}: {log}");
    assert!(!log.contains(" listening"), "{database:?}: {log}");
    log
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

/// Move one account's period marker, and those of its active leases, back one
/// month, in one transaction. An error means nothing changed.
async fn age_period(
    database: &mut PgConnection,
    account: &[u8],
    current: i64,
) -> Result<(), sqlx::Error> {
    const PREVIOUS_MONTH: &str = "(extract(epoch FROM \
         ((to_timestamp(period_start_us / 1000000) AT TIME ZONE 'UTC') - interval '1 month') \
         AT TIME ZONE 'UTC') * 1000000)::bigint";
    let mut transaction = database.begin().await?;
    // Row locks in the server's own order, active leases and then the account,
    // queueing behind its transactions for as long as an exhausted account's
    // takes to drain.
    for lock in [
        "SELECT 1 FROM tollgate_leases WHERE account_id = $1 AND state = 0 \
         ORDER BY lease_id FOR UPDATE",
        "SELECT 1 FROM tollgate_accounts WHERE account_id = $1 FOR UPDATE",
    ] {
        sqlx::query(lock)
            .bind(account)
            .execute(&mut *transaction)
            .await?;
    }
    // A lease granted between those two could now be held by a transaction
    // waiting for the account. Giving up before the database's deadlock
    // detector would run makes this transaction the one that yields.
    sqlx::raw_sql("SET LOCAL lock_timeout = '500ms'")
        .execute(&mut *transaction)
        .await?;
    // No lease can be granted now, so these are all that will settle against
    // the new period.
    // A column expression cannot be a bind parameter. The only text written
    // into these two statements is the constant above.
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE tollgate_leases SET period_start_us = {PREVIOUS_MONTH} \
         WHERE account_id = $1 AND state = 0 AND period_start_us > 0"
    )))
    .bind(account)
    .execute(&mut *transaction)
    .await?;
    let aged = sqlx::query(AssertSqlSafe(format!(
        "UPDATE tollgate_accounts SET period_start_us = {PREVIOUS_MONTH} \
         WHERE account_id = $1 AND period_start_us = $2"
    )))
    .bind(account)
    .bind(current)
    .execute(&mut *transaction)
    .await?;
    assert_eq!(
        aged.rows_affected(),
        1,
        "an account must be in the current period before that period can end"
    );
    transaction.commit().await
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
        // A database name cannot be a bind parameter; this one is a fixed
        // prefix and numbers.
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE {name}")))
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

/// The `host:port` and the password of a
/// `scheme://user:password@host:port/database` URL.
fn authority_and_password(url: &str) -> (&str, &str) {
    let credentials = url
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('@'))
        .map_or("", |(credentials, _)| credentials);
    let password = credentials
        .split_once(':')
        .map_or("", |(_, password)| password);
    assert!(!password.is_empty(), "the database URL carries a password");
    (authority(url), password)
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
                // Built above from the harness's own database name.
                sqlx::raw_sql(AssertSqlSafe(statement))
                    .execute(&mut admin)
                    .await
                    .ok()
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

    /// What this instance reports having admitted, refused and abandoned, in
    /// that order.
    ///
    /// # Panics
    ///
    /// When the instance does not report the figures.
    pub async fn admission_counters(&self) -> [u64; 3] {
        let body: serde_json::Value = self
            .client
            .get(self.url("/metrics"))
            .send()
            .await
            .expect("the application answers")
            .json()
            .await
            .expect("a metrics body");
        ["admitted", "denied", "contexts_abandoned"].map(|name| {
            body[name]
                .as_u64()
                .unwrap_or_else(|| panic!("metrics report {name}"))
        })
    }

    /// The counter at `pointer` (a JSON pointer) in the instance's metrics.
    ///
    /// # Panics
    ///
    /// When the instance does not report it.
    pub async fn counter(&self, pointer: &str) -> u64 {
        let body: serde_json::Value = self
            .client
            .get(self.url("/metrics"))
            .send()
            .await
            .expect("the application answers")
            .json()
            .await
            .expect("a metrics body");
        body.pointer(pointer)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| panic!("metrics report no counter at {pointer}"))
    }

    /// The unfunded units this instance reports having extended, across every
    /// account.
    pub async fn overage_spent(&self) -> u64 {
        self.counter("/total_overage_spent").await
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

/// What a published snapshot says that a scenario varies.
#[derive(Clone, Copy)]
pub struct Policy {
    pub generation: u64,
    /// How long from publication the snapshot authorizes anything.
    pub valid_for: SignedDuration,
    /// The part of a request's price that does not depend on its size.
    pub request_price: u64,
    pub mode: EnforcementMode,
}

impl Policy {
    /// What [`ControlPlane::onboard`] publishes.
    pub const INITIAL: Policy = Policy {
        generation: 1,
        valid_for: SignedDuration::from_hours(1),
        request_price: 50,
        mode: EnforcementMode::Strict,
    };

    /// What one request, of one contract, costs under this policy.
    #[must_use]
    pub fn charge(&self) -> u64 {
        self.request_price + 1
    }
}

/// The control plane and its database. Application instances are held by the
/// scenario; declare them after this so they stop first.
pub struct ControlPlane {
    settings: Settings,
    /// The release the server runs, or will run when next started.
    server_build: Build,
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
        Self::start_over(settings, transport, DatabaseTransport::AsConfigured).await
    }

    /// As [`Self::start`], running `build`'s server.
    pub async fn start_on(settings: Settings, build: &Build) -> Self {
        Self::start_build(
            settings,
            build.clone(),
            Transport::Plaintext,
            DatabaseTransport::AsConfigured,
        )
        .await
    }

    /// As [`Self::start_with`], over the database reached by `database`.
    pub async fn start_over(
        settings: Settings,
        transport: Transport,
        database: DatabaseTransport,
    ) -> Self {
        let build = settings.current.clone();
        Self::start_build(settings, build, transport, database).await
    }

    async fn start_build(
        settings: Settings,
        server_build: Build,
        transport: Transport,
        database: DatabaseTransport,
    ) -> Self {
        let admin_url = match database {
            DatabaseTransport::AsConfigured => settings.pg_url.clone(),
            DatabaseTransport::VerifiedTls => format!(
                "{}?sslmode=verify-full&sslrootcert={}",
                settings.pg_tls_url, settings.pg_tls_ca
            ),
        };
        let database = Database::create(&admin_url).await;
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
            server_build,
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
        let mut command = Command::new(&self.server_build.server);
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

    /// Stop the server as an orchestrator would and start `build`'s in its
    /// place, at the same address over the same database: the control-plane
    /// step of an upgrade.
    pub async fn replace_server(&mut self, build: &Build) {
        let server = self.server.take().expect("the server is running");
        server.terminate().await;
        self.server_build = build.clone();
        self.start_server().await;
    }

    /// Kill the server: an outage, not a drain.
    pub fn kill_server(&mut self) {
        let mut server = self.server.take().expect("the server is running");
        server.kill();
    }

    /// How many connections PostgreSQL holds to this deployment's database,
    /// and how many of them it reports as encrypted.
    ///
    /// # Panics
    ///
    /// When the database cannot be asked.
    pub async fn database_sessions(&self) -> (i64, i64) {
        // Errors are not printed: driver text can carry the URL's credentials.
        let mut admin = PgConnection::connect(&self.database.admin_url)
            .await
            .unwrap_or_else(|_| panic!("cannot connect to the database server"));
        sqlx::query_as(
            "SELECT count(*), count(*) FILTER (WHERE ssl.ssl)
             FROM pg_stat_activity AS activity JOIN pg_stat_ssl AS ssl USING (pid)
             WHERE activity.datname = $1",
        )
        .bind(&self.database.name)
        .fetch_one(&mut admin)
        .await
        .unwrap_or_else(|_| panic!("cannot read the database's sessions"))
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
        self.start_app_from(&self.settings.current, name, options)
            .await
    }

    /// As [`Self::start_app`], running `build`'s instance.
    pub async fn start_app_from(&self, build: &Build, name: &str, options: AppOptions) -> App {
        self.start_app_at(build, name, options, &self.bind).await
    }

    /// As [`Self::start_app`], reaching the server only through `link`.
    pub async fn start_app_via(&self, name: &str, options: AppOptions, link: &Link) -> App {
        self.start_app_at(&self.settings.current, name, options, &link.address())
            .await
    }

    async fn start_app_at(
        &self,
        build: &Build,
        name: &str,
        options: AppOptions,
        server: &str,
    ) -> App {
        let app = self
            .spawn_app(build, name, options, server, "instance.identity")
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
        let build = &self.settings.current;
        self.spawn_app(build, name, options, &self.bind, "stranger.identity")
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
        build: &Build,
        name: &str,
        options: AppOptions,
        server: &str,
        identity: &str,
    ) -> App {
        let mut command = Command::new(&build.app);
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
        let customer = Customer {
            account,
            key,
            secret: issued.secret,
        };
        self.publish(
            &customer,
            Policy {
                mode,
                ..Policy::INITIAL
            },
        )
        .await;
        customer
    }

    /// Publish the policy the customer's credential authorizes against.
    pub async fn publish(&self, customer: &Customer, policy: Policy) {
        let price = CostUnits(policy.request_price);
        let snapshot = AccountSnapshot::builder(
            customer.account,
            Generation(policy.generation),
            AccountStatus::Active,
            Timestamp::now() + policy.valid_for,
            PermissionBits::bit(0),
            ResolvedLimits::new(1_024).with_weighted_rate(5_000_000, 10_000_000),
            Arc::new(
                CostTable::builder(price, price)
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .capacity_class(CapacityClass::Assured)
        .enforcement_mode(policy.mode)
        .build();
        let request = PublishSnapshotRequest {
            snapshot: Arc::new(snapshot),
        };
        self.operator(
            Method::PUT,
            &format!(
                "/accounts/{:032x}/keys/{:032x}/snapshot",
                customer.account.0, customer.key.0
            ),
            Some(serde_json::to_value(request).expect("wire types serialize")),
            StatusCode::NO_CONTENT,
        )
        .await;
    }

    /// Give the account a monthly allowance. The server's next maintenance
    /// pass deposits the first one.
    pub async fn set_allowance(&self, account: AccountId, allowance: u64) {
        let request = SetBudgetRequest {
            budget: Some(BudgetSchedule::monthly(CostUnits(allowance))),
        };
        self.operator(
            Method::PUT,
            &format!("/accounts/{:032x}/budget", account.0),
            Some(serde_json::to_value(request).expect("wire types serialize")),
            StatusCode::OK,
        )
        .await;
    }

    /// Make each account's current budget period the previous one, as the
    /// passage of a calendar month would. Nothing is deposited, expired or
    /// published here: the server's maintenance pass finds the accounts past
    /// their boundary and crosses it.
    ///
    /// The only period is a UTC calendar month and every process reads the
    /// host's clock, so the stored period markers move instead of the clock:
    /// each account's, and that of every lease it funded, which settlement
    /// compares with it. What this does not reproduce is an instance's own
    /// clock passing a period end.
    ///
    /// # Panics
    ///
    /// When an account is not in the current period, or the database cannot be
    /// reached.
    pub async fn end_budget_period(&self, accounts: &[AccountId]) {
        let current = Period::UtcCalendarMonth
            .start_of(Timestamp::now())
            .as_microsecond();
        let mut database = PgConnection::connect(&self.database.url)
            .await
            .unwrap_or_else(|_| panic!("cannot connect to the scenario database"));
        for account in accounts {
            let id = account.0.to_be_bytes();
            // Longer than most waits here: under load this queues behind an
            // account in constant use.
            let deadline = Instant::now() + DEADLINE * 4;
            while let Err(busy) = age_period(&mut database, &id, current).await {
                assert!(
                    Instant::now() < deadline,
                    "account {account} was never free to age: {busy}"
                );
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
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
