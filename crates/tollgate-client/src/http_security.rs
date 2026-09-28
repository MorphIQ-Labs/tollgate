//! Credentials and validated transport configuration for background HTTP calls.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use reqwest::header::HeaderValue;
use tollgate_store::StoreError;
use zeroize::Zeroizing;

/// An asynchronously refreshed identity source, called only by the background
/// transport. HttpStore's request budget includes this call.
#[async_trait]
pub trait BearerProvider: Send + Sync {
    async fn token(&self) -> Result<BearerToken, StoreError>;
}

/// Validated framing with redacted Debug. Raw bytes are wiped when released.
#[derive(Clone)]
pub struct BearerToken(Zeroizing<String>);

impl BearerToken {
    pub fn new(token: impl Into<String>) -> Result<Self, StoreError> {
        let token = Zeroizing::new(token.into());
        if token.is_empty()
            || token.len() > 16 * 1024 - 7
            || !token.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(StoreError("invalid bearer token framing or length".into()));
        }
        Ok(Self(token))
    }

    pub(crate) fn header(&self) -> HeaderValue {
        // Both the prefix and every token byte were validated at construction.
        let framed = Zeroizing::new(format!("Bearer {}", self.0.as_str()));
        let mut header =
            HeaderValue::from_str(&framed).expect("validated bearer token is a header value");
        header.set_sensitive(true);
        header
    }
}

impl std::fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BearerToken([redacted])")
    }
}

/// A rotatable credential. Replacement is validated before it becomes visible;
/// revocation is explicit and never turns an authenticated call anonymous.
pub struct StaticBearer(ArcSwapOption<BearerToken>);

impl StaticBearer {
    pub fn new(token: BearerToken) -> Arc<Self> {
        Arc::new(Self(ArcSwapOption::from(Some(Arc::new(token)))))
    }
    pub fn replace(&self, token: BearerToken) {
        self.0.store(Some(Arc::new(token)));
    }
    pub fn revoke(&self) {
        self.0.store(None);
    }
}

#[async_trait]
impl BearerProvider for StaticBearer {
    async fn token(&self) -> Result<BearerToken, StoreError> {
        self.0
            .load_full()
            .map(|token| (*token).clone())
            .ok_or_else(|| StoreError("control-plane credential was revoked".into()))
    }
}

/// A Google Cloud workload uses its attached service account; there is no
/// per-instance secret.
/// The metadata endpoint is fixed, never supplied by an untrusted URL or header.
pub struct GoogleIdentity {
    client: reqwest::Client,
    audience: String,
    cached: tokio::sync::Mutex<Option<(tokio::time::Instant, BearerToken)>>,
}

impl GoogleIdentity {
    pub fn new(audience: impl Into<String>) -> Result<Arc<Self>, StoreError> {
        let audience = audience.into();
        if audience.is_empty() {
            return Err(StoreError(
                "Google identity audience must not be empty".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| StoreError("cannot configure metadata client".into()))?;
        Ok(Arc::new(Self {
            client,
            audience,
            cached: tokio::sync::Mutex::new(None),
        }))
    }
}

#[async_trait]
impl BearerProvider for GoogleIdentity {
    async fn token(&self) -> Result<BearerToken, StoreError> {
        self.token_from("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity", tokio::time::Instant::now).await
    }
}

impl GoogleIdentity {
    // Private test seams exercise the full cache and HTTP protocol against a
    // local metadata fixture. The public provider always selects Google's fixed
    // endpoint and samples time after acquiring the cache guard.
    async fn token_from(
        &self,
        endpoint: &str,
        clock: impl FnOnce() -> tokio::time::Instant,
    ) -> Result<BearerToken, StoreError> {
        let mut cached = self.cached.lock().await;
        let now = clock();
        if let Some((until, token)) = &*cached
            && now < *until
        {
            return Ok(token.clone());
        }
        let mut response = self
            .client
            .get(endpoint)
            .header("Metadata-Flavor", "Google")
            .query(&[("audience", self.audience.as_str()), ("format", "full")])
            .send()
            .await
            .map_err(|_| StoreError("Google metadata identity request failed".into()))?;
        if !response.status().is_success()
            || response
                .headers()
                .get("Metadata-Flavor")
                .is_none_or(|v| v != "Google")
        {
            return Err(StoreError(
                "Google metadata identity request was refused".into(),
            ));
        }
        let mut bytes = Zeroizing::new(Vec::new());
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| StoreError("Google metadata identity response interrupted".into()))?
        {
            if bytes.len() + chunk.len() > 16 * 1024 - 7 {
                return Err(StoreError(
                    "Google metadata identity response too large".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let token = BearerToken::new(
            std::str::from_utf8(&bytes)
                .map_err(|_| StoreError("invalid metadata identity encoding".into()))?
                .to_owned(),
        )?;
        // Metadata issues a current token (~1h validity); retaining it for 60s
        // limits refresh traffic. The server independently verifies expiration,
        // issuer, audience and its current subject allowlist on every request.
        *cached = Some((now + Duration::from_secs(60), token.clone()));
        Ok(token)
    }
}

/// Authentication and TLS settings for HttpStore. A custom CA replaces public
/// roots. Identity PEM contains the certificate chain and private key.
pub struct HttpStoreConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub root_ca_pem: Option<Vec<u8>>,
    pub identity_pem: Option<Zeroizing<Vec<u8>>>,
    pub bearer: Option<Arc<dyn BearerProvider>>,
}

impl Default for HttpStoreConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
            root_ca_pem: None,
            identity_pem: None,
            bearer: None,
        }
    }
}

pub(crate) fn client(
    base: &str,
    config: &HttpStoreConfig,
) -> Result<(String, reqwest::Client), StoreError> {
    let url =
        reqwest::Url::parse(base).map_err(|_| StoreError("invalid control-plane URL".into()))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(StoreError(
            "control-plane URL must be HTTP(S), with no userinfo, query or fragment".into(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| StoreError("control-plane URL needs a host".into()))?;
    let localhost = host == "localhost";
    let loopback = localhost
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| match ip {
                IpAddr::V4(ip) => ip.is_loopback(),
                IpAddr::V6(ip) => {
                    ip.is_loopback() || ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback())
                }
            });
    if url.scheme() == "http"
        && (!loopback || config.identity_pem.is_some() || config.root_ca_pem.is_some())
    {
        return Err(StoreError(
            "plaintext is allowed only on loopback without TLS configuration".into(),
        ));
    }
    for duration in [config.connect_timeout, config.request_timeout] {
        if duration.is_zero() || std::time::Instant::now().checked_add(duration).is_none() {
            return Err(StoreError(
                "HTTP deadlines must be positive and representable".into(),
            ));
        }
    }
    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout);
    // Pin the special loopback name. Trusting its spelling while letting DNS
    // choose the address could transmit a credential to a remote plaintext peer.
    if localhost {
        builder = builder.resolve(
            "localhost",
            SocketAddr::from(([127, 0, 0, 1], url.port_or_known_default().unwrap_or(80))),
        );
    }
    if let Some(pem) = &config.root_ca_pem {
        let certificates = reqwest::Certificate::from_pem_bundle(pem)
            .map_err(|_| StoreError("invalid control-plane CA PEM".into()))?;
        if certificates.is_empty() {
            return Err(StoreError("control-plane CA PEM is empty".into()));
        }
        builder = builder.tls_built_in_root_certs(false);
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }
    if let Some(pem) = &config.identity_pem {
        builder = builder.identity(
            reqwest::Identity::from_pem(pem)
                .map_err(|_| StoreError("invalid control-plane identity PEM".into()))?,
        );
    }
    Ok((
        url.as_str().trim_end_matches('/').to_owned(),
        builder
            .build()
            .map_err(|_| StoreError("invalid HTTP/TLS client configuration".into()))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn client_validation_rejects_each_unsafe_url_component_independently() {
        for url in [
            "https://user@example.com",
            "https://:password@example.com",
            "https://example.com?query",
            "https://example.com#fragment",
            "http://example.com",
            "http://[2001:db8::1]",
            "http://[::ffff:192.0.2.1]",
        ] {
            assert!(client(url, &HttpStoreConfig::default()).is_err(), "{url}");
        }
        for url in [
            "http://localhost",
            "http://127.0.0.2",
            "http://[::1]",
            "http://[::ffff:127.0.0.1]",
        ] {
            assert!(client(url, &HttpStoreConfig::default()).is_ok(), "{url}");
            for (root, identity) in [(true, false), (false, true), (true, true)] {
                let config = HttpStoreConfig {
                    root_ca_pem: root.then(Vec::new),
                    identity_pem: identity.then(|| Zeroizing::new(Vec::new())),
                    ..Default::default()
                };
                let error = client(url, &config).err().unwrap();
                assert_eq!(
                    error.0,
                    "plaintext is allowed only on loopback without TLS configuration"
                );
            }
        }
    }

    struct Metadata {
        endpoint: String,
        response: Arc<std::sync::Mutex<Vec<u8>>>,
        requests: tokio::sync::mpsc::UnboundedReceiver<String>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Metadata {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Metadata {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!(
                "http://{}/computeMetadata/v1/instance/service-accounts/default/identity",
                listener.local_addr().unwrap()
            );
            let response = Arc::new(std::sync::Mutex::new(Vec::new()));
            let replies = response.clone();
            let (requests, received) = tokio::sync::mpsc::unbounded_channel();
            let task = tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        if stream.read(&mut byte).await.unwrap() == 0 {
                            break;
                        }
                        request.extend_from_slice(&byte);
                    }
                    requests.send(String::from_utf8(request).unwrap()).unwrap();
                    let reply = replies.lock().unwrap().clone();
                    // A size-limit refusal may close before the fixture has
                    // finished writing its intentionally oversized response.
                    if stream.write_all(&reply).await.is_err() {
                        continue;
                    }
                }
            });
            Self {
                endpoint,
                response,
                requests: received,
                task,
            }
        }
        fn reply(&self, status: u16, flavor: Option<&str>, body: &[u8]) {
            let mut reply = format!(
                "HTTP/1.1 {status} Fixture\r\nConnection: close\r\nContent-Length: {}\r\n",
                body.len()
            );
            if let Some(flavor) = flavor {
                reply.push_str(&format!("Metadata-Flavor: {flavor}\r\n"));
            }
            reply.push_str("\r\n");
            let mut bytes = reply.into_bytes();
            bytes.extend_from_slice(body);
            *self.response.lock().unwrap() = bytes;
        }
    }

    #[tokio::test]
    async fn google_metadata_cache_refreshes_at_its_exact_deadline_and_never_caches_failure() {
        assert!(GoogleIdentity::new("").is_err());
        let provider = GoogleIdentity::new("https://control.example.test").unwrap();
        let mut server = Metadata::start().await;
        server.reply(200, Some("Google"), b"fixture-token-one");
        let now = tokio::time::Instant::now();
        let first = provider.token_from(&server.endpoint, || now).await.unwrap();
        assert_eq!(first.header(), "Bearer fixture-token-one");
        let request = server.requests.recv().await.unwrap();
        let path = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let url = reqwest::Url::parse(&format!("http://metadata.fixture{path}")).unwrap();
        assert_eq!(
            url.path(),
            "/computeMetadata/v1/instance/service-accounts/default/identity"
        );
        assert_eq!(
            url.query_pairs()
                .collect::<std::collections::HashMap<_, _>>()
                .get("audience")
                .unwrap(),
            "https://control.example.test"
        );
        assert_eq!(
            url.query_pairs()
                .collect::<std::collections::HashMap<_, _>>()
                .get("format")
                .unwrap(),
            "full"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("metadata-flavor: google\r\n")
        );
        server.reply(503, Some("Google"), b"unavailable");
        let cached = provider
            .token_from(&server.endpoint, || now + Duration::from_secs(59))
            .await
            .unwrap();
        assert_eq!(cached.header(), first.header());
        assert!(server.requests.try_recv().is_err());
        assert!(
            provider
                .token_from(&server.endpoint, || now + Duration::from_secs(60))
                .await
                .is_err()
        );
        server.requests.recv().await.unwrap();
        server.reply(200, Some("Google"), b"fixture-token-two");
        let replacement = provider
            .token_from(&server.endpoint, || now + Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(replacement.header(), "Bearer fixture-token-two");
        server.requests.recv().await.unwrap();
        assert!(server.requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn metadata_requires_success_google_provenance_and_a_complete_bounded_token() {
        let server = Metadata::start().await;
        for (status, flavor, body, accepted) in [
            (200, Some("Google"), vec![b'a'; 16377], true),
            (200, Some("Google"), vec![b'a'; 16378], false),
            (200, Some("Google"), vec![b'a'; 32768], false),
            (200, Some("Google"), vec![], false),
            (200, Some("Google"), vec![0xff], false),
            (200, Some("Google"), b"token\n".to_vec(), false),
            (200, Some("Impostor"), b"token".to_vec(), false),
            (200, None, b"token".to_vec(), false),
            (503, Some("Google"), b"token".to_vec(), false),
            (302, Some("Google"), b"token".to_vec(), false),
        ] {
            server.reply(status, flavor, &body);
            let provider = GoogleIdentity::new("fixture-audience").unwrap();
            let result = provider
                .token_from(&server.endpoint, tokio::time::Instant::now)
                .await;
            assert_eq!(
                result.is_ok(),
                accepted,
                "status={status} flavor={flavor:?} length={}",
                body.len()
            );
            if body.len() > 16377 {
                assert_eq!(
                    result.unwrap_err().0,
                    "Google metadata identity response too large"
                );
            }
        }
        *server.response.lock().unwrap() = b"HTTP/1.1 200 OK\r\nMetadata-Flavor: Google\r\nContent-Length: 100\r\nConnection: close\r\n\r\ntruncated".to_vec();
        let provider = GoogleIdentity::new("fixture-audience").unwrap();
        assert!(
            provider
                .token_from(&server.endpoint, tokio::time::Instant::now)
                .await
                .is_err()
        );
    }

    #[test]
    fn bearer_framing_is_bounded_sensitive_and_redacted() {
        for invalid in ["", "one two", "one\ttwo", "one\ntwo", "\u{7f}"] {
            assert!(BearerToken::new(invalid).is_err());
        }
        assert!(BearerToken::new("x".repeat(16378)).is_err());
        let token = BearerToken::new("x".repeat(16377)).unwrap();
        assert_eq!(token.header().as_bytes().len(), 16384);
        assert!(token.header().is_sensitive());
        assert_eq!(format!("{token:?}"), "BearerToken([redacted])");
    }
}
