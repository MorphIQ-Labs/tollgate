//! TLS is owned by the listener. HTTP headers cannot manufacture a peer identity.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::Connected;
use axum::serve::{IncomingStream, Listener};
use jiff::Timestamp;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime, pem::PemObject};
use rustls::server::danger::ClientCertVerifier;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_rustls::{TlsAcceptor, server::TlsStream};

use crate::security::{SecurityError, ServerSecurity};

/// Parsed and validated before binding or atomic replacement. Private keys are
/// never rendered in diagnostics. Optional client authentication permits bearer
/// users and unauthenticated health probes on the same encrypted listener.
#[derive(Clone)]
pub struct TlsConfig {
    config: Arc<rustls::ServerConfig>,
    verifier: Option<Arc<dyn ClientCertVerifier>>,
    handshake_timeout: Duration,
    max_handshakes: NonZeroUsize,
}

impl TlsConfig {
    pub fn from_pem(
        certificates: &[u8],
        private_key: &[u8],
        client_ca: Option<&[u8]>,
    ) -> Result<Self, SecurityError> {
        let certificates = parse_certificates(certificates)?;
        let private_key = PrivateKeyDer::from_pem_slice(private_key)
            .map_err(|_| SecurityError("invalid TLS private key PEM"))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = client_ca
            .map(|pem| {
                let mut roots = rustls::RootCertStore::empty();
                for certificate in parse_certificates(pem)? {
                    roots
                        .add(certificate)
                        .map_err(|_| SecurityError("invalid client CA certificate"))?;
                }
                rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    Arc::clone(&provider),
                )
                .allow_unauthenticated()
                .build()
                .map_err(|_| SecurityError("invalid client CA configuration"))
            })
            .transpose()?;
        let builder = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| SecurityError("no safe TLS protocol version"))?;
        let builder = match &verifier {
            Some(verifier) => builder.with_client_cert_verifier(Arc::clone(verifier)),
            None => builder.with_no_client_auth(),
        };
        let mut config = builder
            .with_single_cert(certificates, private_key)
            .map_err(|_| SecurityError("TLS certificate and private key do not match"))?;
        // HTTP/1.1 matches the current control-plane server. No 0-RTT replay of
        // a non-idempotent deposit; session resumption is not enabled.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        config.max_early_data_size = 0;
        config.send_tls13_tickets = 0;
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        Ok(Self {
            config: Arc::new(config),
            verifier,
            handshake_timeout: Duration::from_secs(5),
            max_handshakes: NonZeroUsize::new(128).expect("128 is nonzero"),
        })
    }

    /// A configurable connection-burst budget, independent of accounts or body
    /// sizes. Excess connections wait in the OS backlog rather than spawning
    /// unbounded handshake tasks. Each task has its own deadline.
    pub fn with_handshake_limits(
        mut self,
        timeout: Duration,
        max_pending: NonZeroUsize,
    ) -> Result<Self, SecurityError> {
        if timeout.is_zero() || std::time::Instant::now().checked_add(timeout).is_none() {
            return Err(SecurityError(
                "TLS handshake timeout must be positive and representable",
            ));
        }
        self.handshake_timeout = timeout;
        self.max_handshakes = max_pending;
        Ok(self)
    }

    pub(crate) fn verifies_clients(&self) -> bool {
        self.verifier.is_some()
    }

    pub(crate) fn verify(
        &self,
        chain: &[CertificateDer<'static>],
        now: Timestamp,
    ) -> Result<(), SecurityError> {
        let verifier = self
            .verifier
            .as_ref()
            .ok_or(SecurityError("client CA is not configured"))?;
        let (leaf, intermediates) = chain
            .split_first()
            .ok_or(SecurityError("missing client certificate"))?;
        let seconds = u64::try_from(now.as_second())
            .map_err(|_| SecurityError("invalid certificate verification time"))?;
        verifier
            .verify_client_cert(
                leaf,
                intermediates,
                UnixTime::since_unix_epoch(Duration::from_secs(seconds)),
            )
            .map_err(|_| SecurityError("client certificate no longer valid"))?;
        Ok(())
    }
}

pub(crate) fn parse_certificates(
    pem: &[u8],
) -> Result<Vec<CertificateDer<'static>>, SecurityError> {
    let certificates: Vec<_> = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<_, _>>()
        .map_err(|_| SecurityError("invalid certificate PEM"))?;
    if certificates.is_empty() {
        return Err(SecurityError("certificate PEM is empty"));
    }
    Ok(certificates)
}

pub fn certificate_fingerprint(pem: &[u8]) -> Result<[u8; 32], SecurityError> {
    let certificates = parse_certificates(pem)?;
    if certificates.len() != 1 {
        return Err(SecurityError(
            "identity requires exactly one leaf certificate",
        ));
    }
    Ok(fingerprint(&certificates[0]))
}

pub(crate) fn fingerprint(certificate: &CertificateDer<'_>) -> [u8; 32] {
    Sha256::digest(certificate.as_ref()).into()
}

/// Includes IPv4-mapped loopback addresses; all DNS names are resolved by the
/// caller before the listener is checked. Prefixes such as `localhost.evil`
/// are never trusted.
pub fn is_loopback(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => {
            ip.is_loopback() || ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback())
        }
    }
}

#[derive(Clone)]
pub(crate) struct PeerIdentity {
    address: SocketAddr,
    pub certificates: Option<Arc<[CertificateDer<'static>]>>,
}

impl std::fmt::Debug for PeerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerIdentity")
            .field("address", &self.address)
            .field("certificate_present", &self.certificates.is_some())
            .finish()
    }
}

impl Connected<IncomingStream<'_, SecureListener>> for PeerIdentity {
    fn connect_info(stream: IncomingStream<'_, SecureListener>) -> Self {
        stream.remote_addr().clone()
    }
}

pub(crate) trait ServerIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ServerIo for T {}

pub(crate) struct SecureListener {
    listener: TcpListener,
    security: Arc<ServerSecurity>,
    handshakes: JoinSet<(io::Result<TlsStream<TcpStream>>, SocketAddr)>,
}

impl SecureListener {
    pub fn new(listener: TcpListener, security: Arc<ServerSecurity>) -> io::Result<Self> {
        if !security.encrypted() && !is_loopback(listener.local_addr()?.ip()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "non-loopback listeners require TLS",
            ));
        }
        Ok(Self {
            listener,
            security,
            handshakes: JoinSet::new(),
        })
    }
}

impl Listener for SecureListener {
    type Io = Box<dyn ServerIo>;
    type Addr = PeerIdentity;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let bundle = self.security.current.load_full();
            let capacity = bundle
                .tls
                .as_ref()
                .map_or(1, |tls| tls.max_handshakes.get());
            tokio::select! {
                biased;
                result = self.handshakes.join_next(), if !self.handshakes.is_empty() => {
                    match result {
                        Some(Ok((Ok(stream), address))) => {
                            let certificates = stream.get_ref().1.peer_certificates().map(Arc::from);
                            return (Box::new(stream), PeerIdentity { address, certificates });
                        }
                        Some(Ok((Err(error), address))) => tracing::debug!(%address, %error, "TLS handshake refused"),
                        Some(Err(error)) => tracing::warn!(%error, "TLS handshake task failed"),
                        None => {}
                    }
                }
                accepted = self.listener.accept(), if self.handshakes.len() < capacity => {
                    match accepted {
                        Ok((stream, address)) => match &self.security.current.load_full().tls {
                            None => return (Box::new(stream), PeerIdentity { address, certificates: None }),
                            Some(tls) => {
                                let acceptor = TlsAcceptor::from(Arc::clone(&tls.config));
                                let timeout = tls.handshake_timeout;
                                self.handshakes.spawn(async move {
                                    let result = tokio::time::timeout(timeout, acceptor.accept(stream)).await
                                        .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "TLS handshake deadline")));
                                    (result, address)
                                });
                            }
                        },
                        Err(error) => {
                            tracing::warn!(%error, "control-plane accept failed");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(PeerIdentity {
            address: self.listener.local_addr()?,
            certificates: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::SecurityPolicy;

    fn security(timeout: Duration, capacity: usize) -> Arc<ServerSecurity> {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let tls = TlsConfig::from_pem(
            certificate.cert.pem().as_bytes(),
            certificate.signing_key.serialize_pem().as_bytes(),
            None,
        )
        .unwrap()
        .with_handshake_limits(timeout, NonZeroUsize::new(capacity).unwrap())
        .unwrap();
        ServerSecurity::new(SecurityPolicy::new(), Some(tls)).unwrap()
    }

    #[tokio::test]
    async fn pending_tls_handshakes_are_bounded_expire_and_drop_with_the_listener() {
        use tokio::io::AsyncReadExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut listener =
            SecureListener::new(listener, security(Duration::from_millis(50), 2)).unwrap();
        let mut first = TcpStream::connect(address).await.unwrap();
        let mut second = TcpStream::connect(address).await.unwrap();
        let mut queued = TcpStream::connect(address).await.unwrap();
        // No peer sends a ClientHello, so accept remains pending. Cancellation
        // of accept retains its bounded tasks for the next call.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
        assert_eq!(listener.handshakes.len(), 2);
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), first.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), second.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
        assert_eq!(listener.handshakes.len(), 1);
        drop(listener);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), queued.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
}
