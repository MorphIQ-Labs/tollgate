//! A throwaway certificate authority for one deployment. Nothing here outlives
//! the scenario's temporary directory.

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

pub(crate) struct Leaf {
    pub(crate) certificate: String,
    key: String,
}

impl Leaf {
    /// The certificate and key in the one PEM a client presents.
    pub(crate) fn identity(&self) -> String {
        format!("{}{}", self.certificate, self.key)
    }
}

pub(crate) struct Pki {
    pub(crate) ca: String,
    pub(crate) server: String,
    pub(crate) server_key: String,
    /// The client certificate the server maps to the instance role.
    pub(crate) instance: Leaf,
    /// Signed by the same CA and mapped to nothing.
    pub(crate) stranger: Leaf,
}

impl Pki {
    pub(crate) fn generate() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key = KeyPair::generate().expect("a CA key");
        let ca = params.self_signed(&key).expect("a CA certificate").pem();
        let issuer = Issuer::new(params, key);
        let leaf = |name: &str, usage| {
            let mut params =
                CertificateParams::new(vec![name.to_owned()]).expect("leaf parameters");
            params.extended_key_usages = vec![usage];
            let key = KeyPair::generate().expect("a leaf key");
            Leaf {
                certificate: params
                    .signed_by(&key, &issuer)
                    .expect("a leaf certificate")
                    .pem(),
                key: key.serialize_pem(),
            }
        };
        let server = leaf("127.0.0.1", ExtendedKeyUsagePurpose::ServerAuth);
        Pki {
            ca,
            instance: leaf("e2e-instance", ExtendedKeyUsagePurpose::ClientAuth),
            stranger: leaf("e2e-stranger", ExtendedKeyUsagePurpose::ClientAuth),
            server_key: server.key,
            server: server.certificate,
        }
    }
}
