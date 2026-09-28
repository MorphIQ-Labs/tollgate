//! File-backed security configuration. Operators stage referenced files first,
//! then atomically replace the JSON manifest. Failed loads preserve the live generation.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tollgate_auth::{CredentialIssuer, CredentialVerifier, HmacRegistry};
use zeroize::Zeroizing;

use crate::google::GoogleVerifier;
use crate::security::{
    ControlIdentity, ProvisionerLimits, Role, SecurityError, SecurityPolicy, ServerSecurity,
};
use crate::transport::{TlsConfig, certificate_fingerprint};
use tollgate_core::CostUnits;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    tls: Option<TlsFiles>,
    #[serde(default)]
    bearers: Vec<BearerFile>,
    #[serde(default)]
    certificates: Vec<CertificateFile>,
    google: Option<GoogleConfig>,
    issuer: Option<IssuerFile>,
}

/// Customer-credential issuance authority. Distinct from every bearer: the
/// operator who administers accounts and the secret that mints credentials
/// must not collapse into one value.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssuerFile {
    secret_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsFiles {
    certificate: PathBuf,
    private_key: PathBuf,
    client_ca: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BearerFile {
    identity: String,
    role: Role,
    token_file: PathBuf,
    /// Required for, and only for, a `provisioner` (#39).
    max_budget_allowance: Option<CostUnits>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CertificateFile {
    identity: String,
    role: Role,
    certificate: PathBuf,
    /// Required for, and only for, a `provisioner` (#39).
    max_budget_allowance: Option<CostUnits>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GoogleConfig {
    audience: String,
    subjects: Vec<GoogleSubject>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GoogleSubject {
    subject: String,
    identity: String,
    role: Role,
    /// Required for, and only for, a `provisioner` (#39).
    max_budget_allowance: Option<CostUnits>,
}

/// Loader state retains Google keys for at most one hour after a successful
/// fetch. Failures retry after five minutes and never extend that deadline.
///
/// The credential issuer is fixed when the loader starts. A later manifest
/// naming a different issuer (added, removed or changed) still installs its
/// transport and control-plane credentials, but the live issuer is kept and
/// the change is reported as pending until restart: credentials already issued
/// verify only where verifiers hold the secret that minted them, so switching
/// authority under running verifiers would strand every new credential.
pub struct SecurityLoader {
    path: PathBuf,
    keys: Option<(Vec<u8>, Timestamp)>,
    next_key_attempt: tokio::time::Instant,
    digest: Option<[u8; 32]>,
    issuer: Option<Arc<HmacRegistry>>,
    issuer_fingerprint: Option<[u8; 32]>,
    pending_issuer: Option<Option<[u8; 32]>>,
}

/// A complete, validated security generation staged by
/// [`SecurityLoader::load`] and not yet live.
///
/// Pass the first one to [`SecurityLoader::start`] and later ones to
/// [`SecurityLoader::install`]. Dropping it discards the generation.
pub struct LoadedSecurity {
    policy: SecurityPolicy,
    tls: Option<TlsConfig>,
    issuer: Option<IssuerSecret>,
    digest: [u8; 32],
}

/// Exactly 64 lowercase hexadecimal characters. The HMAC key is those
/// characters as bytes, not their decoded value, so the one value an operator
/// stores configures the issuer and every verifier identically.
struct IssuerSecret(Zeroizing<[u8; ISSUER_SECRET_LEN]>);

const ISSUER_SECRET_LEN: usize = 64;

impl IssuerSecret {
    fn parse(bytes: &[u8]) -> Result<Self, SecurityError> {
        let text = bytes
            .strip_suffix(b"\n")
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
            .unwrap_or(bytes);
        let secret: [u8; ISSUER_SECRET_LEN] = text
            .try_into()
            .ok()
            .filter(|text: &[u8; ISSUER_SECRET_LEN]| {
                text.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
            })
            .ok_or(SecurityError(
                "issuer secret must be exactly 64 lowercase hexadecimal characters",
            ))?;
        Ok(Self(Zeroizing::new(secret)))
    }

    /// Compared, never logged: identifies a secret without retaining it.
    fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"tollgate-issuer-v1");
        digest.update(self.0.as_slice());
        digest.finalize().into()
    }
}

impl SecurityLoader {
    /// A loader for the JSON manifest at `path`. Nothing is read until
    /// [`load`](Self::load); relative paths inside the manifest resolve
    /// against its directory.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            keys: None,
            next_key_attempt: tokio::time::Instant::now(),
            digest: None,
            issuer: None,
            issuer_fingerprint: None,
            pending_issuer: None,
        }
    }

    /// Mark only after installation succeeds. A failed replacement must remain
    /// eligible for retry even if the source files have not changed again.
    ///
    /// This is the only place the credential issuer is set.
    pub fn start(&mut self, loaded: LoadedSecurity) -> Result<Arc<ServerSecurity>, SecurityError> {
        let security = ServerSecurity::new(loaded.policy, loaded.tls)?;
        self.issuer_fingerprint = loaded.issuer.as_ref().map(IssuerSecret::fingerprint);
        self.issuer = loaded
            .issuer
            .map(|secret| Arc::new(HmacRegistry::new(secret.0.as_slice())));
        self.pending_issuer = None;
        self.digest = Some(loaded.digest);
        Ok(security)
    }

    /// Replaces transport and control-plane credentials. A differing issuer is
    /// never applied; it is reported once per distinct staged value and
    /// remains visible through [`Self::issuer_change_pending`].
    pub fn install(
        &mut self,
        loaded: LoadedSecurity,
        security: &ServerSecurity,
    ) -> Result<(), SecurityError> {
        security.replace(loaded.policy, loaded.tls)?;
        let staged = loaded.issuer.as_ref().map(IssuerSecret::fingerprint);
        if staged == self.issuer_fingerprint {
            if self.pending_issuer.take().is_some() {
                tracing::info!(
                    reason = "issuer-change-withdrawn",
                    "staged credential issuer matches the live one again"
                );
            }
        } else if self.pending_issuer != Some(staged) {
            self.pending_issuer = Some(staged);
            tracing::warn!(
                reason = "issuer-change-requires-restart",
                "credential issuer unchanged; restart to apply the staged issuer"
            );
        }
        self.digest = Some(loaded.digest);
        Ok(())
    }

    /// The durable customer-credential issuer from the manifest's `issuer`
    /// entry, fixed at [`Self::start`]. `None` before start or when the
    /// manifest configures none; issuance then answers `501`.
    pub fn issuer(&self) -> Option<Arc<dyn CredentialIssuer + Send + Sync>> {
        self.issuer
            .as_ref()
            .map(|issuer| Arc::clone(issuer) as Arc<dyn CredentialIssuer + Send + Sync>)
    }

    /// True while the most recently installed manifest names a different
    /// issuer than the live one. Only a restart applies it.
    pub fn issuer_change_pending(&self) -> bool {
        self.pending_issuer.is_some()
    }

    /// Reads the manifest and every file it references, validates them, and
    /// stages a complete generation without making it live.
    ///
    /// Each load builds a fresh control-plane bearer verifier keyed by a new
    /// random secret, so only HMAC digests of the static tokens are retained.
    /// When the manifest configures Google identity, this also refreshes
    /// Google's signing keys if a refresh is due: after a successful fetch,
    /// at half the key set's lifetime, clamped between five seconds and five
    /// minutes; after a failure, five minutes later. A fetched key set is
    /// usable until `now` plus the lifetime its `Cache-Control`/`Age`
    /// headers allow: five minutes without cache metadata, and never more
    /// than one hour. A failed fetch keeps
    /// the previous keys and their original expiry, or fails the load when
    /// there are none. A fetched key set that fails validation fails the
    /// load and also leaves the previous keys in place.
    ///
    /// Returns `Ok(None)` when the manifest, every referenced file, and any
    /// signing keys and their expiry are unchanged since the generation last passed to
    /// [`start`](Self::start) or [`install`](Self::install).
    ///
    /// # Errors
    ///
    /// Returns a [`SecurityError`] for an unreadable file, unknown manifest
    /// fields, a malformed or out-of-bounds credential, a duplicate
    /// credential mapping, an issuer secret that is malformed or equal to a
    /// bearer token, invalid TLS material, or unavailable signing keys. The
    /// live generation is unaffected.
    pub async fn load(&mut self, now: Timestamp) -> Result<Option<LoadedSecurity>, SecurityError> {
        self.load_with_keys(now, || {
            crate::google::fetch_keys("https://www.googleapis.com/oauth2/v3/certs")
        })
        .await
    }

    async fn load_with_keys<F: Future<Output = Result<(Vec<u8>, Duration), SecurityError>>>(
        &mut self,
        now: Timestamp,
        fetch: impl FnOnce() -> F,
    ) -> Result<Option<LoadedSecurity>, SecurityError> {
        let bytes = read(&self.path).await?;
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|_| SecurityError("invalid security manifest"))?;
        let directory = self.path.parent().unwrap_or(Path::new("."));
        let mut digest = Sha256::new();
        record_digest(&mut digest, bytes.as_slice());
        let mut policy = SecurityPolicy::new();
        let mut secret = Zeroizing::new([0u8; 32]);
        getrandom::fill(secret.as_mut())
            .map_err(|_| SecurityError("control-plane verifier entropy unavailable"))?;
        let registry = Arc::new(HmacRegistry::new(secret.as_ref()));
        let mut tokens = Vec::new();
        let mut actors = Vec::new();
        for bearer in manifest.bearers {
            let bytes = read(&directory.join(bearer.token_file)).await?;
            record_digest(&mut digest, bytes.as_slice());
            let token = Zeroizing::new(
                std::str::from_utf8(&bytes)
                    .map_err(|_| SecurityError("invalid bearer file encoding"))?
                    .trim_end_matches(['\r', '\n'])
                    .to_owned(),
            );
            if token.len() < 32
                || token.len() > 16 * 1024 - 7
                || !token.bytes().all(|b| b.is_ascii_graphic())
            {
                return Err(SecurityError(
                    "static bearer credentials must contain 32..=16377 visible ASCII bytes",
                ));
            }
            actors.push(identity(
                bearer.identity,
                bearer.role,
                bearer.max_budget_allowance,
            )?);
            tokens.push(token);
        }
        registry.install_credentials(tokens.iter().map(|token| token.as_bytes()));
        let mut mapped = Vec::new();
        for (token, identity) in tokens.iter().zip(actors) {
            let principal = registry
                .verify(token.as_bytes())
                .ok_or(SecurityError("installed credential failed verification"))?
                .principal;
            mapped.push((principal, identity));
        }
        policy = policy.with_bearer(registry, mapped)?;
        let issuer = match manifest.issuer {
            Some(issuer) => {
                let bytes = read(&directory.join(issuer.secret_file)).await?;
                record_digest(&mut digest, bytes.as_slice());
                let secret = IssuerSecret::parse(&bytes)?;
                let collides = tokens
                    .iter()
                    .any(|token| bool::from(token.as_bytes().ct_eq(secret.0.as_slice())));
                if collides {
                    return Err(SecurityError(
                        "issuer secret must differ from every bearer credential",
                    ));
                }
                Some(secret)
            }
            None => None,
        };
        for certificate in manifest.certificates {
            let pem = read(&directory.join(certificate.certificate)).await?;
            record_digest(&mut digest, pem.as_slice());
            policy = policy.with_certificate(
                certificate_fingerprint(&pem)?,
                identity(
                    certificate.identity,
                    certificate.role,
                    certificate.max_budget_allowance,
                )?,
            )?;
        }
        let tls = match manifest.tls {
            Some(tls) => {
                let certificates = read(&directory.join(tls.certificate)).await?;
                let key = read(&directory.join(tls.private_key)).await?;
                let ca = match tls.client_ca {
                    Some(path) => Some(read(&directory.join(path)).await?),
                    None => None,
                };
                record_digest(&mut digest, certificates.as_slice());
                record_digest(&mut digest, key.as_slice());
                if let Some(ca) = &ca {
                    record_digest(&mut digest, ca.as_slice());
                }
                Some(TlsConfig::from_pem(
                    &certificates,
                    &key,
                    ca.as_ref().map(|ca| ca.as_slice()),
                )?)
            }
            None => None,
        };
        if let Some(google) = manifest.google {
            if tokio::time::Instant::now() >= self.next_key_attempt {
                self.next_key_attempt = tokio::time::Instant::now() + Duration::from_secs(300);
                match fetch().await {
                    Ok((keys, lifetime)) => {
                        self.next_key_attempt = tokio::time::Instant::now()
                            + (lifetime / 2)
                                .clamp(Duration::from_secs(5), Duration::from_secs(300));
                        let until = now
                            .checked_add(
                                SignedDuration::try_from(lifetime)
                                    .map_err(|_| SecurityError("signing-key lifetime overflow"))?,
                            )
                            .map_err(|_| SecurityError("signing-key expiry overflow"))?;
                        // Validate before replacing a known-good key set.
                        GoogleVerifier::from_jwks(&google.audience, &keys, until)?;
                        self.keys = Some((keys, until));
                    }
                    Err(error) if self.keys.is_some() => {
                        tracing::warn!(%error, "Google key refresh failed; previous expiry remains authoritative")
                    }
                    Err(error) => return Err(error),
                }
            }
            let (keys, until) = self
                .keys
                .as_ref()
                .ok_or(SecurityError("Google signing keys unavailable"))?;
            record_digest(&mut digest, keys);
            record_digest(&mut digest, &until.as_second().to_be_bytes());
            let verifier = Arc::new(GoogleVerifier::from_jwks(&google.audience, keys, *until)?);
            let mut mapped = Vec::new();
            for subject in google.subjects {
                if subject.subject.is_empty() {
                    return Err(SecurityError("Google subject must not be empty"));
                }
                mapped.push((
                    GoogleVerifier::principal(&subject.subject),
                    identity(subject.identity, subject.role, subject.max_budget_allowance)?,
                ));
            }
            policy = policy.with_bearer(verifier, mapped)?;
        }
        let digest = digest.finalize().into();
        if self.digest == Some(digest) {
            return Ok(None);
        }
        Ok(Some(LoadedSecurity {
            policy,
            tls,
            issuer,
            digest,
        }))
    }
}

/// One manifest entry's identity. The ceiling is required for a provisioner
/// and refused for any other role, so a misplaced field is an error rather
/// than a silently ignored limit (#39).
fn identity(
    name: String,
    role: Role,
    max_budget_allowance: Option<CostUnits>,
) -> Result<ControlIdentity, SecurityError> {
    match (role, max_budget_allowance) {
        (Role::Provisioner, Some(max)) => {
            ControlIdentity::provisioner(name, ProvisionerLimits::new(max))
        }
        (Role::Provisioner, None) => Err(SecurityError(
            "a provisioner identity requires max_budget_allowance",
        )),
        (Role::Instance | Role::Operator, Some(_)) => Err(SecurityError(
            "max_budget_allowance applies only to a provisioner identity",
        )),
        (Role::Instance | Role::Operator, None) => ControlIdentity::new(name, role),
    }
}

async fn read(path: &Path) -> Result<Zeroizing<Vec<u8>>, SecurityError> {
    tokio::fs::read(path)
        .await
        .map(Zeroizing::new)
        .map_err(|_| SecurityError("cannot read security manifest or referenced credential file"))
}

fn record_digest(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(bytes.len().to_be_bytes());
    digest.update(bytes);
}

/// Owned periodic reload; dropping it stops file reads and signing-key refresh.
#[must_use]
pub struct SecurityReloader {
    task: tokio::task::JoinHandle<()>,
    stopping: Arc<std::sync::atomic::AtomicBool>,
}

/// The reloader is owned independently of `serve`. Its exit is reported at
/// the task boundary even during unwinding; no panic payload crosses the log
/// boundary. The owner's terminal bit distinguishes deliberate cancellation.
struct ReloadExit(Arc<std::sync::atomic::AtomicBool>);

impl Drop for ReloadExit {
    fn drop(&mut self) {
        if !self.0.load(std::sync::atomic::Ordering::Acquire) {
            tracing::error!(
                operation = "security-reload",
                reason = "unexpected-exit",
                "security reload task stopped; configuration will not refresh"
            );
        }
    }
}

impl SecurityReloader {
    /// Spawns the reload task on the current Tokio runtime.
    ///
    /// Every five seconds it calls [`SecurityLoader::load`] with `clock`'s
    /// time and passes any changed generation to
    /// [`SecurityLoader::install`], which publishes it to `security`. A
    /// failed load or install is logged at `warn` and the previous generation
    /// stays live; the next attempt retries. Pass a loader that has already
    /// been [started](SecurityLoader::start) with `security`'s first
    /// generation.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn spawn(
        mut loader: SecurityLoader,
        security: Arc<ServerSecurity>,
        clock: Arc<dyn tollgate_store::Clock>,
    ) -> Self {
        let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exit = ReloadExit(Arc::clone(&stopping));
        let task = tokio::spawn(async move {
            let _exit = exit;
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                match loader.load(clock.now()).await {
                    Ok(Some(loaded)) => match loader.install(loaded, &security) {
                        Ok(()) => tracing::info!("control-plane security configuration replaced"),
                        Err(error) => {
                            tracing::warn!(%error, "security replacement refused; previous configuration retained")
                        }
                    },
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%error, "security reload failed; previous configuration retained")
                    }
                }
            }
        });
        Self { task, stopping }
    }
}

impl Drop for SecurityReloader {
    fn drop(&mut self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::Release);
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn dropping_the_reloader_releases_its_owned_task_and_clock() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("security.json");
        std::fs::write(&path, "{}").unwrap();
        let now = Timestamp::from_second(100).unwrap();
        let mut loader = SecurityLoader::new(path);
        let loaded = loader.load(now).await.unwrap().unwrap();
        let security = loader.start(loaded).unwrap();
        let clock = Arc::new(tollgate_store::ManualClock::new(now));
        let owned = Arc::downgrade(&clock);
        let reloader = SecurityReloader::spawn(loader, security, clock);
        tokio::task::yield_now().await;
        assert!(owned.upgrade().is_some());
        drop(reloader);
        tokio::task::yield_now().await;
        assert!(
            owned.upgrade().is_none(),
            "a dropped reloader cannot retain its task's clock"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn signing_key_refresh_honors_success_cadence_and_failure_backoff() {
        for (lifetime, delay) in [(2, 5), (30, 15), (3600, 300)] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("security.json");
            std::fs::write(
                &path,
                r#"{"google":{"audience":"https://control.example.test","subjects":[]}}"#,
            )
            .unwrap();
            let fixture: serde_json::Value =
                serde_json::from_str(include_str!("../tests/fixtures/google-tokens.json")).unwrap();
            let keys = serde_json::to_vec(&fixture["jwks"]).unwrap();
            let now = Timestamp::from_second(1_700_000_100).unwrap();
            let mut loader = SecurityLoader::new(path);
            let loaded = loader
                .load_with_keys(now, || async {
                    Ok((keys.clone(), Duration::from_secs(lifetime)))
                })
                .await
                .unwrap()
                .unwrap();
            let _security = loader.start(loaded).unwrap();
            tokio::time::advance(Duration::from_secs(delay - 1)).await;
            assert!(
                loader
                    .load_with_keys(now, || async { panic!("successful fetch refreshed early") })
                    .await
                    .unwrap()
                    .is_none()
            );
            tokio::time::advance(Duration::from_secs(1)).await;
            let mut attempted = false;
            assert!(
                loader
                    .load_with_keys(now, || async {
                        attempted = true;
                        Err(SecurityError("fixture outage"))
                    })
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(attempted);
            tokio::time::advance(Duration::from_secs(299)).await;
            assert!(
                loader
                    .load_with_keys(now, || async {
                        panic!("failed fetch retried before backoff")
                    })
                    .await
                    .unwrap()
                    .is_none()
            );
            tokio::time::advance(Duration::from_secs(1)).await;
            let mut retried = false;
            loader
                .load_with_keys(now, || async {
                    retried = true;
                    Ok((keys, Duration::from_secs(lifetime)))
                })
                .await
                .unwrap();
            assert!(retried);
        }
    }

    #[tokio::test]
    async fn a_google_provisioner_subject_requires_its_ceiling() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/google-tokens.json")).unwrap();
        let keys = serde_json::to_vec(&fixture["jwks"]).unwrap();
        let now = Timestamp::from_second(1_700_000_100).unwrap();
        for (subject, valid) in [
            (
                serde_json::json!({"subject": "1", "identity": "signup", "role": "provisioner", "max_budget_allowance": 1000}),
                true,
            ),
            (
                serde_json::json!({"subject": "1", "identity": "signup", "role": "provisioner"}),
                false,
            ),
            (
                serde_json::json!({"subject": "1", "identity": "ops", "role": "operator", "max_budget_allowance": 1000}),
                false,
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("security.json");
            std::fs::write(
                &path,
                serde_json::json!({"google": {"audience": "https://control.example.test", "subjects": [subject]}})
                    .to_string(),
            )
            .unwrap();
            let loaded = SecurityLoader::new(path)
                .load_with_keys(now, || async {
                    Ok((keys.clone(), Duration::from_secs(60)))
                })
                .await;
            assert_eq!(loaded.is_ok(), valid, "{subject}");
        }
    }

    #[tokio::test]
    async fn initial_signing_key_failure_preserves_the_dependency_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("security.json");
        std::fs::write(
            &path,
            r#"{"google":{"audience":"https://control.example.test","subjects":[]}}"#,
        )
        .unwrap();
        let error = SecurityLoader::new(path)
            .load_with_keys(Timestamp::from_second(100).unwrap(), || async {
                Err(SecurityError("fixture initial outage"))
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.0, "fixture initial outage");
    }

    #[tokio::test]
    async fn failed_key_refresh_never_extends_verified_identity_validity() {
        use tollgate_auth::CredentialVerifier;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("security.json");
        std::fs::write(
            &path,
            r#"{"google":{"audience":"https://control.example.test","subjects":[]}}"#,
        )
        .unwrap();
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/google-tokens.json")).unwrap();
        let keys = serde_json::to_vec(&fixture["jwks"]).unwrap();
        let token = fixture["tokens"]["valid"].as_str().unwrap();
        let now = Timestamp::from_second(1_700_000_100).unwrap();
        let mut loader = SecurityLoader::new(path);
        let loaded = loader
            .load_with_keys(now, || async {
                Ok((keys.clone(), Duration::from_secs(30)))
            })
            .await
            .unwrap()
            .unwrap();
        let security = loader.start(loaded).unwrap();
        let expiry = now.checked_add(SignedDuration::from_secs(30)).unwrap();
        let verified = |loader: &SecurityLoader| {
            let (keys, until) = loader.keys.as_ref().unwrap();
            GoogleVerifier::from_jwks("https://control.example.test", keys, *until)
                .unwrap()
                .verify(token.as_bytes())
                .unwrap()
        };
        assert!(verified(&loader).is_reusable_at(now));
        assert!(!verified(&loader).is_reusable_at(expiry));
        loader.next_key_attempt = tokio::time::Instant::now();
        assert!(
            loader
                .load_with_keys(expiry, || async { Err(SecurityError("fixture outage")) })
                .await
                .unwrap()
                .is_none()
        );
        assert!(!verified(&loader).is_reusable_at(expiry));
        // Corrupt successful responses also preserve the prior validated keys.
        loader.next_key_attempt = tokio::time::Instant::now();
        assert!(
            loader
                .load_with_keys(expiry, || async {
                    Ok((b"{}".to_vec(), Duration::from_secs(3600)))
                })
                .await
                .is_err()
        );
        assert!(!verified(&loader).is_reusable_at(expiry));
        loader.next_key_attempt = tokio::time::Instant::now();
        let loaded = loader
            .load_with_keys(expiry, || async { Ok((keys, Duration::from_secs(60))) })
            .await
            .unwrap()
            .unwrap();
        loader.install(loaded, &security).unwrap();
        assert!(verified(&loader).is_reusable_at(expiry));
        assert!(
            !verified(&loader)
                .is_reusable_at(expiry.checked_add(SignedDuration::from_secs(60)).unwrap())
        );
    }
}
