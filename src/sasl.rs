//! SASL mechanisms: PLAIN (RFC 4616) and SCRAM-SHA-256/512 (RFC 5802, RFC 7677), as Kafka's
//! `ScramSaslClient` speaks them (no channel binding: gs2 header `n,,`).
use crate::error::{Error, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use ring::{digest, hmac, pbkdf2};
use std::num::NonZeroU32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl Mechanism {
    pub fn parse(s: &str) -> Result<Mechanism> {
        match s.trim().to_ascii_uppercase().as_str() {
            "PLAIN" => Ok(Mechanism::Plain),
            "SCRAM-SHA-256" => Ok(Mechanism::ScramSha256),
            "SCRAM-SHA-512" => Ok(Mechanism::ScramSha512),
            other => Err(Error::Config(format!(
                "sasl.mechanisms={other}: this client speaks PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512"
            ))),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Mechanism::Plain => "PLAIN",
            Mechanism::ScramSha256 => "SCRAM-SHA-256",
            Mechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// PLAIN's one message: authzid (none), authcid, password, NUL-separated.
pub fn plain(user: &str, password: &str) -> Vec<u8> {
    [b"\0", user.as_bytes(), b"\0", password.as_bytes()].concat()
}

/// A SCRAM exchange's client side: `first()`, then `fin(server_first)`, then `verify(server_final)`.
pub struct Scram {
    mech: Mechanism,
    password: String,
    nonce: String,
    first_bare: String,
    /// the server key and auth message, which the server's signature is checked against
    server_check: Option<(hmac::Key, String)>,
}

impl std::fmt::Debug for Scram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scram").field("mech", &self.mech).finish_non_exhaustive()
        // never the password
    }
}

/// A username as SCRAM writes it: `,` and `=` escaped (RFC 5802 section 5.1).
fn saslname(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

impl Scram {
    pub fn new(mech: Mechanism, user: &str, password: &str) -> Result<Scram> {
        let mut raw = [0u8; 24];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut raw)
            .map_err(|_| Error::Auth("no randomness for the SCRAM nonce".into()))?;
        Ok(Scram::with_nonce(mech, user, password, &B64.encode(raw)))
    }

    /// With a given nonce (the RFCs' test vectors).
    pub fn with_nonce(mech: Mechanism, user: &str, password: &str, nonce: &str) -> Scram {
        assert!(mech != Mechanism::Plain, "SCRAM");
        Scram {
            mech,
            password: password.to_string(),
            nonce: nonce.to_string(),
            first_bare: format!("n={},r={nonce}", saslname(user)),
            server_check: None,
        }
    }

    /// client-first-message
    pub fn first(&self) -> Vec<u8> {
        format!("n,,{}", self.first_bare).into_bytes()
    }

    /// client-final-message, from the server-first-message.
    pub fn fin(&mut self, server_first: &[u8]) -> Result<Vec<u8>> {
        let sf = std::str::from_utf8(server_first).map_err(|_| Error::Auth("server-first-message not UTF-8".into()))?;
        let attr = |k: char| {
            sf.split(',')
                .find_map(|a| a.strip_prefix(k).and_then(|a| a.strip_prefix('=')))
                .ok_or_else(|| Error::Auth(format!("server-first-message without its {k}= attribute")))
        };
        if sf.starts_with("m=") {
            return Err(Error::Auth("the server asks for a SCRAM extension this client does not know".into()));
        }
        let nonce = attr('r')?;
        if !nonce.starts_with(&self.nonce) || nonce.len() == self.nonce.len() {
            return Err(Error::Auth("the server's nonce does not extend this client's".into()));
        }
        let salt = B64.decode(attr('s')?).map_err(|_| Error::Auth("the salt is not base64".into()))?;
        let iterations: u32 =
            attr('i')?.parse().map_err(|_| Error::Auth("the iteration count is not a number".into()))?;
        // Kafka's ScramMechanism: at least 4096 iterations; fewer would be a downgrade
        let iterations = NonZeroU32::new(iterations)
            .filter(|i| i.get() >= 4096)
            .ok_or_else(|| Error::Auth(format!("{iterations} SCRAM iterations: fewer than 4096")))?;
        let (alg, dig, len) = match self.mech {
            Mechanism::ScramSha256 => (pbkdf2::PBKDF2_HMAC_SHA256, &digest::SHA256, 32),
            _ => (pbkdf2::PBKDF2_HMAC_SHA512, &digest::SHA512, 64),
        };
        let hmac_alg = if len == 32 { hmac::HMAC_SHA256 } else { hmac::HMAC_SHA512 };
        let mut salted = vec![0u8; len];
        pbkdf2::derive(alg, iterations, &salt, self.password.as_bytes(), &mut salted);
        let salted = hmac::Key::new(hmac_alg, &salted);
        let client_key = hmac::sign(&salted, b"Client Key");
        let stored_key = digest::digest(dig, client_key.as_ref());
        let without_proof = format!("c=biws,r={nonce}"); // biws: base64 of the gs2 header "n,,"
        let auth_message = format!("{},{sf},{without_proof}", self.first_bare);
        let client_sig = hmac::sign(&hmac::Key::new(hmac_alg, stored_key.as_ref()), auth_message.as_bytes());
        let proof: Vec<u8> = client_key.as_ref().iter().zip(client_sig.as_ref()).map(|(a, b)| a ^ b).collect();
        let server_key = hmac::sign(&salted, b"Server Key");
        self.server_check = Some((hmac::Key::new(hmac_alg, server_key.as_ref()), auth_message));
        Ok(format!("{without_proof},p={}", B64.encode(proof)).into_bytes())
    }

    /// The server-final-message: the server proved it knows the password, or said why not.
    pub fn verify(&self, server_final: &[u8]) -> Result<()> {
        let s = std::str::from_utf8(server_final).map_err(|_| Error::Auth("server-final-message not UTF-8".into()))?;
        if let Some(e) = s.strip_prefix("e=") {
            return Err(Error::Auth(format!("the server refused: {e}")));
        }
        let v = s
            .split(',')
            .find_map(|a| a.strip_prefix("v="))
            .ok_or_else(|| Error::Auth("server-final-message without its signature".into()))?;
        let v = B64.decode(v).map_err(|_| Error::Auth("the server's signature is not base64".into()))?;
        let (key, auth) =
            self.server_check.as_ref().ok_or_else(|| Error::Auth("server-final before server-first".into()))?;
        // in constant time: the signature proves the server knows the password
        hmac::verify(key, auth.as_bytes(), &v).map_err(|_| {
            Error::Auth("the server's signature does not match: not the server the password is for".into())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 section 3's exchange, byte for byte (user "user", password "pencil").
    #[test]
    fn scram_sha_256_is_rfc_7677s_example() {
        let mut s = Scram::with_nonce(Mechanism::ScramSha256, "user", "pencil", "rOprNGfwEbeRWgbNEkqO");
        assert_eq!(s.first(), b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first = b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        assert_eq!(
            String::from_utf8(s.fin(server_first).unwrap()).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        s.verify(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=").unwrap();
        let shown = format!("{s:?}");
        assert!(shown.starts_with("Scram { mech: ScramSha256") && !shown.contains("pencil"), "{shown}");
        assert!(s.verify(b"v=7rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=").is_err(), "another server's signature");
        assert!(matches!(s.verify(b"e=invalid-proof"), Err(Error::Auth(m)) if m.contains("invalid-proof")));
    }

    /// RFC 5802's rules the server must keep: its nonce extends ours, it iterates enough.
    #[test]
    fn a_server_that_breaks_the_rfc_is_refused() {
        let mut s = Scram::with_nonce(Mechanism::ScramSha512, "u", "p", "abc");
        for bad in [
            &b"r=xyz123,s=QSXCR+Q6sek8bf92,i=4096"[..], // not our nonce
            b"r=abc,s=QSXCR+Q6sek8bf92,i=4096",         // nothing of its own
            b"r=abcd,s=QSXCR+Q6sek8bf92,i=1",           // a downgrade
            b"r=abcd,s=!!,i=4096",                      // not base64
            b"r=abcd,i=4096",                           // no salt
            b"m=ext,r=abcd,s=QSXCR+Q6sek8bf92,i=4096",  // a mandatory extension
        ] {
            assert!(s.fin(bad).is_err(), "{}", String::from_utf8_lossy(bad));
        }
        assert!(s.verify(b"v=AAAA").is_err(), "no server-first yet");
    }

    /// SHA-512 computes a 64-byte proof, and the user name is escaped (RFC 5802 5.1).
    #[test]
    fn scram_sha_512_and_escaped_names() {
        let mut s = Scram::with_nonce(Mechanism::ScramSha512, "a,b=c", "p", "n0");
        assert_eq!(s.first(), b"n,,n=a=2Cb=3Dc,r=n0");
        let f = String::from_utf8(s.fin(b"r=n0srv,s=c2FsdA==,i=4096").unwrap()).unwrap();
        let proof = B64.decode(f.rsplit_once(",p=").unwrap().1).unwrap();
        assert_eq!(proof.len(), 64);
        assert!(
            format!("{s:?}").find('p').is_none_or(|_| !format!("{s:?}").contains("password")),
            "no password in Debug"
        );
    }

    #[test]
    fn plain_is_rfc_4616s_message() {
        assert_eq!(plain("tim", "tanstaaftanstaaf"), b"\0tim\0tanstaaftanstaaf");
        assert_eq!(Mechanism::parse("scram-sha-256").unwrap(), Mechanism::ScramSha256);
        assert!(Mechanism::parse("GSSAPI").is_err());
        assert!(Mechanism::parse("OAUTHBEARER").is_err());
    }
}
