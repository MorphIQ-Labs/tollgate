//! Google service-account ID tokens. Key discovery stays off the HTTP handlers;
//! an unknown key or an expired key set fails closed until refresh succeeds.

use jiff::Timestamp;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tollgate_auth::{CredentialVerifier, Verified};
use tollgate_core::Principal;

use crate::security::SecurityError;

/// Fixed issuer, RS256 only, explicit audience. `sub` is the stable Google
/// account identifier; mutable email addresses do not grant a role.
pub struct GoogleVerifier {
    keys: HashMap<String, DecodingKey>,
    validation: Validation,
    usable_until: Timestamp,
}

impl GoogleVerifier {
    /// Builds a verifier for ID tokens issued to `audience`, from Google's
    /// published JSON Web Key Set.
    ///
    /// `usable_until` is when the key set stops being trusted. Every
    /// [`Verified`] this verifier returns expires at the earlier of the
    /// token's `exp` and this instant, so a stale key set cannot extend a
    /// token's validity. The caller compares that expiry against its own
    /// clock; this verifier reads no clock.
    ///
    /// A token verifies only with an RS256 signature under a key named by its
    /// `kid`, a Google issuer (`https://accounts.google.com` or
    /// `accounts.google.com`), exactly this audience, a nonempty `sub`, no
    /// `nbf`, and `iat` before `exp`.
    ///
    /// # Errors
    ///
    /// Returns a [`SecurityError`] for an empty audience, malformed JSON, an
    /// empty key set, a duplicate `kid`, or any key that is not an identified
    /// RSA `RS256` signing key.
    pub fn from_jwks(
        audience: &str,
        jwks: &[u8],
        usable_until: Timestamp,
    ) -> Result<Self, SecurityError> {
        #[derive(Deserialize)]
        struct Keys {
            keys: Vec<Key>,
        }
        #[derive(Deserialize)]
        struct Key {
            kid: String,
            kty: String,
            alg: String,
            #[serde(rename = "use")]
            usage: String,
            n: String,
            e: String,
        }
        if audience.is_empty() {
            return Err(SecurityError("Google audience must not be empty"));
        }
        let parsed: Keys = serde_json::from_slice(jwks)
            .map_err(|_| SecurityError("invalid Google signing key set"))?;
        let mut keys = HashMap::new();
        for key in parsed.keys {
            if key.kid.is_empty() || key.kty != "RSA" || key.alg != "RS256" || key.usage != "sig" {
                return Err(SecurityError(
                    "Google signing key is not an identified RS256 signing key",
                ));
            }
            let decoded = DecodingKey::from_rsa_components(&key.n, &key.e)
                .map_err(|_| SecurityError("invalid Google RSA key"))?;
            if keys.insert(key.kid, decoded).is_some() {
                return Err(SecurityError("duplicate Google signing key identifier"));
            }
        }
        if keys.is_empty() {
            return Err(SecurityError("Google signing key set is empty"));
        }
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["iss", "sub", "aud", "exp"]);
        // Return expiry as verified evidence; the owning server compares it to
        // its explicit Clock exactly once, with no hidden library clock/skew.
        validation.validate_exp = false;
        validation.leeway = 0;
        Ok(Self {
            keys,
            validation,
            usable_until,
        })
    }

    /// The [`Principal`] a token with this `sub` claim verifies as: a
    /// domain-separated SHA-256 digest of the subject, truncated to 128 bits.
    ///
    /// Map a service account's numeric unique ID through this to give it a
    /// role with [`SecurityPolicy::with_bearer`]. The email claim plays no
    /// part.
    ///
    /// [`SecurityPolicy::with_bearer`]: crate::security::SecurityPolicy::with_bearer
    pub fn principal(subject: &str) -> Principal {
        let mut digest = Sha256::new();
        digest.update(b"tollgate-control:google-sub:");
        digest.update(subject.as_bytes());
        let digest = digest.finalize();
        let mut principal = [0; 16];
        principal.copy_from_slice(&digest[..16]);
        Principal(u128::from_be_bytes(principal))
    }
}

impl CredentialVerifier for GoogleVerifier {
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        #[derive(Clone, Deserialize)]
        struct Claims {
            sub: String,
            exp: i64,
            iat: i64,
            nbf: Option<i64>,
        }
        let token = std::str::from_utf8(credential).ok()?;
        let header = decode_header(token).ok()?;
        if header.alg != Algorithm::RS256 {
            return None;
        }
        let key = self.keys.get(header.kid.as_ref()?)?;
        let claims = decode::<Claims>(token, key, &self.validation).ok()?.claims;
        // Google's service-account tokens carry iat/exp, not nbf. Do not
        // silently accept a future-validity restriction this seam cannot carry.
        if claims.sub.is_empty() || claims.nbf.is_some() || claims.iat >= claims.exp {
            return None;
        }
        let expiry = Timestamp::from_second(claims.exp)
            .ok()?
            .min(self.usable_until);
        Some(Verified::until(Self::principal(&claims.sub), expiry))
    }
}

/// The loader fixes Google's published endpoint; the private parameter lets
/// transport tests exercise this same bounded fetch against a local fixture.
/// Diagnostics never contain a token or response body.
pub(crate) async fn fetch_keys(
    endpoint: &str,
) -> Result<(Vec<u8>, std::time::Duration), SecurityError> {
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|_| SecurityError("cannot configure signing-key client"))?;
    let mut response = client
        .get(endpoint)
        .send()
        .await
        .map_err(|_| SecurityError("Google signing-key fetch failed"))?;
    if !response.status().is_success() {
        return Err(SecurityError("Google signing-key endpoint refused refresh"));
    }
    let lifetime = key_cache_lifetime(response.headers())?;
    // Much larger than Google's rotating RSA set; this is a transport envelope,
    // not a limit on the number of accounts or trusted service identities.
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| SecurityError("Google signing-key response interrupted"))?
    {
        if bytes.len() + chunk.len() > 1024 * 1024 {
            return Err(SecurityError("Google signing-key response exceeds 1 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok((bytes, lifetime))
}

fn key_cache_lifetime(
    headers: &reqwest::header::HeaderMap,
) -> Result<std::time::Duration, SecurityError> {
    let mut maximum: Option<u64> = None;
    for value in headers.get_all(reqwest::header::CACHE_CONTROL) {
        let value = value
            .to_str()
            .map_err(|_| SecurityError("invalid signing-key cache policy"))?;
        for directive in value.split(',').map(str::trim) {
            if directive.eq_ignore_ascii_case("no-store")
                || directive.eq_ignore_ascii_case("no-cache")
            {
                return Ok(std::time::Duration::ZERO);
            }
            if let Some((name, value)) = directive.split_once('=')
                && name.trim().eq_ignore_ascii_case("max-age")
            {
                let seconds: u64 = value
                    .trim()
                    .trim_matches('"')
                    .parse()
                    .map_err(|_| SecurityError("invalid signing-key max-age"))?;
                maximum = Some(maximum.map_or(seconds, |old| old.min(seconds)));
            }
        }
    }
    let age = headers
        .get(reqwest::header::AGE)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(SecurityError("invalid signing-key age"))
        })
        .transpose()?
        .unwrap_or(0);
    Ok(std::time::Duration::from_secs(
        maximum.unwrap_or(300).saturating_sub(age).min(3600),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_signing_key_must_independently_name_an_rs256_signature_key() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/google-tokens.json")).unwrap();
        let good = fixture["jwks"].clone();
        let until = Timestamp::from_second(1_800_000_000).unwrap();
        let decode = |audience: &str, value: &serde_json::Value| {
            GoogleVerifier::from_jwks(audience, &serde_json::to_vec(value).unwrap(), until)
        };
        assert!(decode("audience", &good).is_ok());
        assert!(decode("", &good).is_err());
        for (field, value) in [
            ("kid", ""),
            ("kty", "EC"),
            ("alg", "RS512"),
            ("use", "enc"),
            ("n", "@"),
            ("e", "@"),
        ] {
            let mut invalid = good.clone();
            invalid["keys"][0][field] = value.into();
            assert!(decode("audience", &invalid).is_err(), "field={field}");
        }
        let key = good["keys"][0].clone();
        assert!(decode("audience", &serde_json::json!({"keys":[key.clone(),key]})).is_err());
        assert!(decode("audience", &serde_json::json!({"keys":[]})).is_err());
        assert!(GoogleVerifier::from_jwks("audience", b"{", until).is_err());
    }

    #[tokio::test]
    async fn signing_key_transport_enforces_status_cache_policy_and_complete_body_bounds() {
        use axum::http::{HeaderMap, StatusCode, header};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        let response = Arc::new(Mutex::new((
            StatusCode::OK,
            HeaderMap::new(),
            Vec::<u8>::new(),
        )));
        let replies = response.clone();
        let app = axum::Router::new().route(
            "/certs",
            axum::routing::get(move || {
                let replies = replies.clone();
                async move { replies.lock().unwrap().clone() }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/certs", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    stopped.await.unwrap();
                })
                .await
                .unwrap();
        });
        let mut headers = HeaderMap::new();
        headers.insert(header::CACHE_CONTROL, "max-age=600".parse().unwrap());
        headers.insert(header::AGE, "100".parse().unwrap());
        for length in [0, 1, 1024, 1024 * 1024, 1024 * 1024 + 1, 2 * 1024 * 1024] {
            let body = vec![b'k'; length];
            *response.lock().unwrap() = (StatusCode::OK, headers.clone(), body.clone());
            let result = fetch_keys(&endpoint).await;
            if length <= 1024 * 1024 {
                let (received, lifetime) = result.unwrap();
                assert_eq!(received, body);
                assert_eq!(lifetime, Duration::from_secs(500));
            } else {
                assert_eq!(
                    result.unwrap_err(),
                    SecurityError("Google signing-key response exceeds 1 MiB")
                );
            }
        }
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::FOUND,
        ] {
            *response.lock().unwrap() = (status, HeaderMap::new(), b"refused".to_vec());
            assert_eq!(
                fetch_keys(&endpoint).await.unwrap_err(),
                SecurityError("Google signing-key endpoint refused refresh")
            );
        }
        headers.insert(header::CACHE_CONTROL, "max-age=invalid".parse().unwrap());
        *response.lock().unwrap() = (StatusCode::OK, headers, b"invalid cache".to_vec());
        assert!(fetch_keys(&endpoint).await.is_err());
        stop.send(()).unwrap();
        server.await.unwrap();

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/certs", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\ntruncated",
                )
                .await
                .unwrap();
        });
        assert_eq!(
            fetch_keys(&endpoint).await.unwrap_err(),
            SecurityError("Google signing-key response interrupted")
        );
        server.await.unwrap();
    }
    #[test]
    fn signing_keys_never_outlive_the_issuer_cache_policy_or_one_hour() {
        for (policy, age, seconds) in [
            ("public, max-age=30000", "0", 3600),
            ("max-age=600", "100", 500),
            ("max-age=10", "20", 0),
            ("no-cache, max-age=600", "0", 0),
            ("no-store", "0", 0),
            ("max-age=\"100\"", "0", 100),
        ] {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(reqwest::header::CACHE_CONTROL, policy.parse().unwrap());
            headers.insert(reqwest::header::AGE, age.parse().unwrap());
            assert_eq!(key_cache_lifetime(&headers).unwrap().as_secs(), seconds);
        }
        assert_eq!(
            key_cache_lifetime(&reqwest::header::HeaderMap::new())
                .unwrap()
                .as_secs(),
            300
        );
        let mut invalid = reqwest::header::HeaderMap::new();
        invalid.insert(
            reqwest::header::CACHE_CONTROL,
            "max-age=invalid".parse().unwrap(),
        );
        assert!(key_cache_lifetime(&invalid).is_err());
    }
}
