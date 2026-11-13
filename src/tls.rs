//! TLS (rustls), from librdkafka's settings: the CA from `ssl.ca.location` or `ssl.ca.pem`, else
//! the system's roots; a client certificate from `ssl.certificate.*` and `ssl.key.*`; the
//! broker's name checked unless `ssl.endpoint.identification.algorithm=none`, its chain unless
//! `enable.ssl.certificate.verification=false`.
use crate::config::Config;
use crate::error::{Error, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::sync::Arc;

fn pem_certs(k: &str, pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<_> = CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| Error::Config(format!("{k}: {e}")))?;
    if certs.is_empty() {
        return Err(Error::Config(format!("{k}: no PEM certificate in it")));
    }
    Ok(certs)
}

fn read(k: &str, path: &str) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| Error::Config(format!("{k}={path}: {e}")))
}

pub fn config(c: &Config) -> Result<Arc<rustls::ClientConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    let ca = match (c.get("ssl.ca.location"), c.get("ssl.ca.pem")) {
        (Some(_), Some(_)) => return Err(Error::Config("ssl.ca.location and ssl.ca.pem: set one".into())),
        (Some(path), None) if path != "probe" => Some(pem_certs("ssl.ca.location", &read("ssl.ca.location", path)?)?),
        (None, Some(pem)) => Some(pem_certs("ssl.ca.pem", pem.as_bytes())?),
        _ => None, // librdkafka's "probe", and its default: the system's roots
    };
    match ca {
        Some(certs) => {
            for cert in certs {
                roots.add(cert).map_err(|e| Error::Config(format!("the CA certificate: {e}")))?;
            }
        }
        None => {
            let found = rustls_native_certs::load_native_certs();
            for cert in found.certs {
                let _ = roots.add(cert); // a system root rustls cannot parse is one it would not trust
            }
            if roots.is_empty() {
                return Err(Error::Config(format!(
                    "TLS without ssl.ca.location: the system has no CA certificates ({:?})",
                    found.errors.first()
                )));
            }
        }
    }
    let roots = Arc::new(roots);
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Config(e.to_string()))?;
    let check_chain = match c.get("enable.ssl.certificate.verification") {
        None => true,
        Some(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no" | "off"),
    };
    let check_name = match c.get("ssl.endpoint.identification.algorithm").map(|v| v.trim().to_ascii_lowercase()) {
        None => true,
        Some(v) if v == "https" => true,
        Some(v) if v == "none" || v.is_empty() => false,
        Some(v) => return Err(Error::Config(format!("ssl.endpoint.identification.algorithm={v}: https or none"))),
    };
    let builder = match (check_chain, check_name) {
        (true, true) => builder.with_root_certificates(roots),
        (chain, _) => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(Verifier { roots: chain.then_some(roots), provider })),
    };
    let cert = match (c.get("ssl.certificate.location"), c.get("ssl.certificate.pem")) {
        (Some(p), None) => Some(pem_certs("ssl.certificate.location", &read("ssl.certificate.location", p)?)?),
        (None, Some(pem)) => Some(pem_certs("ssl.certificate.pem", pem.as_bytes())?),
        (None, None) => None,
        _ => return Err(Error::Config("ssl.certificate.location and ssl.certificate.pem: set one".into())),
    };
    let key = match (c.get("ssl.key.location"), c.get("ssl.key.pem")) {
        (Some(p), None) => Some(PrivateKeyDer::from_pem_slice(&read("ssl.key.location", p)?).map_err(|e| {
            Error::Config(format!("ssl.key.location: {e} (an unencrypted PKCS#8, PKCS#1 or SEC1 key)"))
        })?),
        (None, Some(pem)) => Some(
            PrivateKeyDer::from_pem_slice(pem.as_bytes())
                .map_err(|e| Error::Config(format!("ssl.key.pem: {e} (an unencrypted PKCS#8, PKCS#1 or SEC1 key)")))?,
        ),
        (None, None) => None,
        _ => return Err(Error::Config("ssl.key.location and ssl.key.pem: set one".into())),
    };
    let config = match (cert, key) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(cert, key)
            .map_err(|e| Error::Config(format!("the client certificate: {e}")))?,
        (None, None) => builder.with_no_client_auth(),
        _ => return Err(Error::Config("a client certificate needs both ssl.certificate.* and ssl.key.*".into())),
    };
    Ok(Arc::new(config))
}

/// The broker's name as TLS checks it.
pub fn server_name(host: &str) -> Result<ServerName<'static>> {
    ServerName::try_from(host.to_string()).map_err(|e| Error::Config(format!("{host}: not a TLS server name: {e}")))
}

/// A verifier that checks the chain to `roots` but not the name (`ssl.endpoint.identification.
/// algorithm=none`), or nothing at all (`enable.ssl.certificate.verification=false`, no roots):
/// the handshake's signatures are always checked.
#[derive(Debug)]
struct Verifier {
    roots: Option<Arc<RootCertStore>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        if let Some(roots) = &self.roots {
            let cert = rustls::server::ParsedCertificate::try_from(end_entity)?;
            rustls::client::verify_server_cert_signed_by_trust_anchor(
                &cert,
                roots,
                intermediates,
                now,
                self.provider.signature_verification_algorithms.all,
            )?;
        }
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}
