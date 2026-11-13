//! A broker for the tests, on a socket of this process: each request handed to a handler,
//! which answers it, drops the connection, or says nothing (a broker that stalls).
use crate::wire::{self, Cur, Put};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

/// A request as the broker read it.
#[derive(Clone, Debug)]
pub struct Req {
    pub api: i16,
    pub version: i16,
    pub corr: i32,
    pub body: Vec<u8>,
    /// which connection (0, 1, ...: in the order they were made)
    pub conn: usize,
    /// the broker's own address
    pub me: String,
    /// requests read on its connection and not yet answered, itself included: more than one where
    /// the client pipelines (plain connections only; 1 behind TLS)
    pub queued: usize,
}

pub enum Answer {
    /// the response body (after its correlation id)
    Body(Vec<u8>),
    /// the connection closed
    Drop,
    /// no answer, the connection kept
    Silence,
    /// no answer: the connection read until the client closes it, and when it did kept here
    Watch(Arc<Mutex<Option<std::time::Instant>>>),
}

type Handler = dyn Fn(&Req) -> Answer + Send + Sync;

pub struct Broker {
    pub addr: String,
    pub seen: Arc<Mutex<Vec<Req>>>,
}

/// Every API at every version this client sends.
pub fn all_versions() -> Vec<u8> {
    let mut b = vec![];
    b.i16(0);
    b.count(wire::VERSIONS.len());
    for (k, _, hi, _) in wire::VERSIONS {
        b.i16(*k);
        b.i16(0);
        b.i16(*hi + 3);
    }
    b
}

impl Broker {
    /// A broker answering ApiVersions itself (every version), the rest by `h`.
    pub fn start(h: impl Fn(&Req) -> Answer + Send + Sync + 'static) -> Broker {
        Broker::start_with(None, h)
    }

    /// The same behind TLS (`tls`: the server's config).
    pub fn start_with(
        tls: Option<Arc<rustls::ServerConfig>>,
        h: impl Fn(&Req) -> Answer + Send + Sync + 'static,
    ) -> Broker {
        Broker::start_full(tls, all_versions(), h)
    }

    /// With its ApiVersions answer `versions`.
    pub fn start_full(
        tls: Option<Arc<rustls::ServerConfig>>,
        versions: Vec<u8>,
        h: impl Fn(&Req) -> Answer + Send + Sync + 'static,
    ) -> Broker {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let me = addr.clone();
        let seen = Arc::new(Mutex::new(vec![]));
        let (h, s2): (Arc<Handler>, _) = (Arc::new(h), seen.clone());
        std::thread::spawn(move || {
            for (n, sock) in l.incoming().enumerate() {
                let Ok(sock) = sock else { return };
                let (h, seen, tls, v, me) = (h.clone(), s2.clone(), tls.clone(), versions.clone(), me.clone());
                std::thread::spawn(move || {
                    let (answered, deaf) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicBool::new(false)));
                    let _ = match tls {
                        None => {
                            // requests read as they come, ahead of the answers: what the client
                            // pipelines is seen queued
                            let (tx, rx) = mpsc::channel();
                            let (mut r, a2, d2) = (sock.try_clone().unwrap(), answered.clone(), deaf.clone());
                            std::thread::spawn(move || {
                                let mut read = 0;
                                while let Ok(b) = read_request(&mut r, &d2) {
                                    read += 1;
                                    if tx.send((b, read - a2.load(SeqCst))).is_err() {
                                        return;
                                    }
                                }
                            });
                            let served =
                                serve(sock.try_clone().unwrap(), Some(rx), &answered, &deaf, n, &me, &v, &*h, &seen);
                            let _ = sock.shutdown(std::net::Shutdown::Both);
                            served
                        }
                        Some(cfg) => {
                            let conn = rustls::ServerConnection::new(cfg).unwrap();
                            let s = rustls::StreamOwned::new(conn, sock);
                            serve(s, None, &answered, &deaf, n, &me, &v, &*h, &seen)
                        }
                    };
                });
            }
        });
        Broker { addr, seen }
    }

    pub fn requests(&self, api: i16) -> Vec<Req> {
        self.seen.lock().unwrap().iter().filter(|r| r.api == api).cloned().collect()
    }
}

/// A request read; none more once the broker is `deaf` (a broker that stopped reading: what the
/// client writes then backs up), past the next one's size.
fn read_request(s: &mut impl Read, deaf: &AtomicBool) -> std::io::Result<Vec<u8>> {
    let mut size = [0; 4];
    s.read_exact(&mut size)?;
    while deaf.load(SeqCst) {
        std::thread::sleep(Duration::from_secs(3600));
    }
    let mut b = vec![0; i32::from_be_bytes(size) as usize];
    s.read_exact(&mut b)?;
    Ok(b)
}

/// A connection's requests answered in turn: read from `s`, or from `ahead` (read as they came,
/// with how many were unanswered then).
#[allow(clippy::too_many_arguments)] // the test broker's own plumbing
fn serve(
    mut s: impl Read + Write,
    ahead: Option<mpsc::Receiver<(Vec<u8>, usize)>>,
    answered: &AtomicUsize,
    deaf: &AtomicBool,
    conn: usize,
    me: &str,
    versions: &[u8],
    h: &Handler,
    seen: &Mutex<Vec<Req>>,
) -> std::io::Result<()> {
    loop {
        let (b, queued) = match &ahead {
            Some(rx) => rx.recv().map_err(|_| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?,
            None => (read_request(&mut s, deaf)?, 1),
        };
        let mut c = Cur::new(&b);
        let (api, version, corr) = (c.i16().unwrap(), c.i16().unwrap(), c.i32().unwrap());
        c.nstr().unwrap();
        let req = Req { api, version, corr, body: b[c.at..].to_vec(), conn, me: me.to_string(), queued };
        seen.lock().unwrap().push(req.clone());
        let answer = if api == wire::API_VERSIONS { Answer::Body(versions.to_vec()) } else { h(&req) };
        match answer {
            Answer::Body(body) => {
                let mut out = vec![];
                out.i32(body.len() as i32 + 4);
                out.i32(corr);
                out.extend(body);
                // counted before it is written: the client may pipeline its next request the moment
                // it reads this answer, and that one must not count this as unanswered
                answered.fetch_add(1, SeqCst);
                s.write_all(&out)?;
                s.flush()?;
            }
            Answer::Drop => return Ok(()),
            Answer::Silence => {
                deaf.store(true, SeqCst);
                std::thread::sleep(Duration::from_secs(3600));
            }
            Answer::Watch(closed) => {
                match &ahead {
                    Some(rx) => while rx.recv().is_ok() {}, // until the reader sees the close
                    None => {
                        let mut b = [0; 4096];
                        while matches!(s.read(&mut b), Ok(1..)) {}
                    }
                }
                *closed.lock().unwrap() = Some(std::time::Instant::now());
                return Ok(());
            }
        }
    }
}

/// A plaintext client config of `addr`.
pub fn config(addr: &str) -> crate::Config {
    let mut c = crate::Config::new();
    c.set("bootstrap.servers", addr);
    c
}

/// A response of Metadata v4: brokers (id, addr), topics (name, partitions (id, leader)).
pub fn metadata(brokers: &[(i32, &str)], topics: &[(&str, &[(i32, i32)])]) -> Vec<u8> {
    let mut b = vec![];
    b.i32(0);
    b.count(brokers.len());
    for (id, addr) in brokers {
        let (host, port) = addr.rsplit_once(':').unwrap();
        b.i32(*id);
        b.str(host);
        b.i32(port.parse().unwrap());
        b.nstr(None);
    }
    b.nstr(Some("test-cluster"));
    b.i32(brokers[0].0);
    b.count(topics.len());
    for (name, parts) in topics {
        b.i16(0);
        b.str(name);
        b.i8(0);
        b.count(parts.len());
        for (p, leader) in parts.iter() {
            b.i16(if *leader < 0 { 5 } else { 0 });
            b.i32(*p);
            b.i32(*leader);
            b.count(1);
            b.i32(leader.max(&0).to_owned());
            b.count(0);
        }
    }
    b
}

/// A CA, and a broker certificate it signed for `names`.
pub struct Pki {
    pub ca_pem: String,
    pub ca_der: CertificateDer<'static>,
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
    pub ca_key: rcgen::KeyPair,
    pub ca: rcgen::Certificate,
}

pub fn pki(names: &[&str]) -> Pki {
    let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
        .unwrap()
        .signed_by(&key, &ca, &ca_key)
        .unwrap();
    Pki {
        ca_pem: ca.pem(),
        ca_der: ca.der().clone(),
        cert: cert.der().clone(),
        key: PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        ca_key,
        ca,
    }
}

pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A TLS broker of `p`'s certificate (asking for a client's of `client_ca` if given), answering by `h`.
pub fn tls_broker_with(
    p: &Pki,
    client_ca: Option<&CertificateDer<'static>>,
    h: impl Fn(&Req) -> Answer + Send + Sync + 'static,
) -> Broker {
    let b = rustls::ServerConfig::builder_with_provider(provider()).with_safe_default_protocol_versions().unwrap();
    let b = match client_ca {
        None => b.with_no_client_auth(),
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(ca.clone()).unwrap();
            let v = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                .build()
                .unwrap();
            b.with_client_cert_verifier(v)
        }
    };
    let cfg = b.with_single_cert(vec![p.cert.clone()], p.key.clone_key()).unwrap();
    Broker::start_with(Some(Arc::new(cfg)), h)
}

/// A TLS broker echoing each request's body.
pub fn tls_broker(p: &Pki, client_ca: Option<&CertificateDer<'static>>) -> Broker {
    tls_broker_with(p, client_ca, |r| Answer::Body(r.body.clone()))
}
