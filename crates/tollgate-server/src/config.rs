//! File-backed security configuration. Operators stage referenced files first,
//! then atomically replace the JSON manifest. Failed loads preserve the live generation.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use zeroize::Zeroizing;

use crate::google::GoogleVerifier;
use crate::security::{ControlIdentity, Role, SecurityError, SecurityPolicy, ServerSecurity};
use crate::transport::{TlsConfig, certificate_fingerprint};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    tls: Option<TlsFiles>,
    #[serde(default)]
    bearers: Vec<BearerFile>,
    #[serde(default)]
    certificates: Vec<CertificateFile>,
    google: Option<GoogleConfig>,
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
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CertificateFile {
    identity: String,
    role: Role,
    certificate: PathBuf,
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
}

/// Loader state retains Google keys for at most one hour after a successful
/// fetch. Failures retry after five minutes and never extend that deadline.
pub struct SecurityLoader {
    path: PathBuf,
    keys: Option<(Vec<u8>, Timestamp)>,
    next_key_attempt: tokio::time::Instant,
    digest: Option<[u8; 32]>,
}

pub struct LoadedSecurity {
    policy: SecurityPolicy,
    tls: Option<TlsConfig>,
    digest: [u8; 32],
}

impl SecurityLoader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            keys: None,
            next_key_attempt: tokio::time::Instant::now(),
            digest: None,
        }
    }

    /// Mark only after installation succeeds. A failed replacement must remain
    /// eligible for retry even if the source files have not changed again.
    pub fn start(&mut self, loaded: LoadedSecurity) -> Result<Arc<ServerSecurity>, SecurityError> {
        let security = ServerSecurity::new(loaded.policy, loaded.tls)?;
        self.digest = Some(loaded.digest);
        Ok(security)
    }

    pub fn install(
        &mut self,
        loaded: LoadedSecurity,
        security: &ServerSecurity,
    ) -> Result<(), SecurityError> {
        security.replace(loaded.policy, loaded.tls)?;
        self.digest = Some(loaded.digest);
        Ok(())
    }

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
            actors.push(ControlIdentity::new(bearer.identity, bearer.role)?);
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
        for certificate in manifest.certificates {
            let pem = read(&directory.join(certificate.certificate)).await?;
            record_digest(&mut digest, pem.as_slice());
            policy = policy.with_certificate(
                certificate_fingerprint(&pem)?,
                ControlIdentity::new(certificate.identity, certificate.role)?,
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
                    ControlIdentity::new(subject.identity, subject.role)?,
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
            digest,
        }))
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
