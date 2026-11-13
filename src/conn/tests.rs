#![allow(clippy::type_complexity)] // the tests' tuples say what each field is where they are made
use super::*;
use crate::testkit::{all_versions, config, pki, tls_broker, Answer, Broker};
use crate::wire::Put;
use std::sync::Arc;

fn net(c: &crate::Config) -> Net {
    c.net().unwrap()
}

fn echo() -> Broker {
    Broker::start(|r| Answer::Body(r.body.clone()))
}

#[test]
fn a_request_is_answered() {
    let b = echo();
    let mut c = Conn::open(&net(&config(&b.addr)), &b.addr).unwrap();
    assert_eq!(c.call(wire::METADATA, |o| o.extend(b"abc")).unwrap(), b"abc");
    assert_eq!(c.call(wire::METADATA, |o| o.extend(b"de")).unwrap(), b"de", "again, the next correlation id");
    let r = b.requests(wire::METADATA);
    assert_eq!((r[0].version, r[1].corr - r[0].corr), (4, 1));
}

/// A broker that does not speak a version this client sends is refused at once, saying which:
/// not a request it would misread later.
#[test]
fn a_broker_without_a_version_this_client_sends_is_refused() {
    let mut v = vec![];
    v.i16(0);
    v.count(wire::VERSIONS.len());
    for (k, _, hi, _) in wire::VERSIONS {
        v.i16(*k);
        // Fetch from v12 only: a broker that dropped the non-flexible versions
        v.i16(if *k == wire::FETCH { 12 } else { 0 });
        v.i16(*hi + 10);
    }
    let b = Broker::start_full(None, v, |_| Answer::Drop);
    let e = Conn::open(&net(&config(&b.addr)), &b.addr).unwrap_err();
    assert!(matches!(&e, Error::Unsupported(m) if m.contains("Fetch v11 (it speaks v12-v21)")), "{e}");
    let mut v = vec![];
    v.i16(35);
    v.count(0);
    let b = Broker::start_full(None, v, |_| Answer::Drop);
    assert!(matches!(Conn::open(&net(&config(&b.addr)), &b.addr), Err(Error::Unsupported(_))));
    assert!(!all_versions().is_empty());
}

fn sasl_broker(user: &'static str, pass: &'static str) -> Broker {
    Broker::start(move |r| {
        let mut c = wire::Cur::new(&r.body);
        let mut out = vec![];
        match r.api {
            wire::SASL_HANDSHAKE => {
                let m = c.str().unwrap();
                out.i16(if m == "PLAIN" { 0 } else { 33 });
                out.count(1);
                out.str("PLAIN");
            }
            wire::SASL_AUTHENTICATE => {
                let ok = c.bytes().unwrap() == crate::sasl::plain(user, pass);
                out.i16(if ok { 0 } else { 58 });
                out.nstr(Some(if ok { "" } else { "Authentication failed: Invalid username or password" }));
                out.bytes(b"");
            }
            _ => out.extend(b"ok"),
        }
        Answer::Body(out)
    })
}

#[test]
fn sasl_plain_authenticates_before_any_request() {
    let b = sasl_broker("svc", "s3cret");
    let mut c = config(&b.addr);
    c.set("security.protocol", "sasl_plaintext").set("sasl.mechanisms", "PLAIN").set("sasl.username", "svc");
    c.set("sasl.password", "s3cret");
    let mut conn = Conn::open(&net(&c), &b.addr).unwrap();
    assert_eq!(conn.call(wire::METADATA, |_| {}).unwrap(), b"ok");
    let apis: Vec<i16> = b.seen.lock().unwrap().iter().map(|r| r.api).collect();
    assert_eq!(apis, [wire::API_VERSIONS, wire::SASL_HANDSHAKE, wire::SASL_AUTHENTICATE, wire::METADATA]);

    c.set("sasl.password", "wrong");
    let e = Conn::open(&net(&c), &b.addr).unwrap_err();
    assert!(
        matches!(&e, Error::Auth(m) if m.contains("SASL_AUTHENTICATION_FAILED") && m.contains("Invalid username")),
        "{e}"
    );
    assert!(!e.to_string().contains("wrong"), "never the password");
    c.set("sasl.mechanisms", "SCRAM-SHA-256");
    let e = Conn::open(&net(&c), &b.addr).unwrap_err();
    assert!(
        matches!(&e, Error::Auth(m) if m.contains("UNSUPPORTED_SASL_MECHANISM") && m.contains("offers PLAIN")),
        "{e}"
    );
}

fn localhost(b: &Broker) -> String {
    format!("localhost:{}", b.addr.rsplit_once(':').unwrap().1)
}

#[test]
fn tls_checks_the_brokers_chain_and_name() {
    let p = pki(&["localhost"]);
    let b = tls_broker(&p, None);
    let addr = localhost(&b);
    let mut c = config(&addr);
    c.set("security.protocol", "ssl").set("ssl.ca.pem", p.ca_pem.clone());
    let mut conn = Conn::open(&net(&c), &addr).unwrap();
    assert_eq!(conn.call(wire::METADATA, |o| o.extend(b"over tls")).unwrap(), b"over tls");

    // another CA's: refused
    let other = pki(&["localhost"]);
    c.set("ssl.ca.pem", other.ca_pem.clone());
    assert!(matches!(Conn::open(&net(&c), &addr), Err(Error::Auth(_))), "a chain to another CA");

    // the CA's, for another name: refused, unless the name is not checked
    let p = pki(&["kafka.example"]);
    let b = tls_broker(&p, None);
    let addr = localhost(&b);
    let mut c = config(&addr);
    c.set("security.protocol", "ssl").set("ssl.ca.pem", p.ca_pem.clone());
    assert!(matches!(Conn::open(&net(&c), &addr), Err(Error::Auth(m)) if m.contains("TLS")), "another name");
    c.set("ssl.endpoint.identification.algorithm", "none");
    Conn::open(&net(&c), &addr).unwrap();
    // with the name unchecked, the chain still is
    c.set("ssl.ca.pem", other.ca_pem.clone());
    assert!(Conn::open(&net(&c), &addr).is_err(), "the chain is checked without the name");
    c.set("enable.ssl.certificate.verification", "false");
    Conn::open(&net(&c), &addr).unwrap();
}

#[test]
fn tls_presents_the_client_certificate() {
    let p = pki(&["localhost"]);
    let b = tls_broker(&p, Some(&p.ca_der));
    let addr = localhost(&b);
    let key = rcgen::KeyPair::generate().unwrap();
    let client =
        rcgen::CertificateParams::new(vec!["client".to_string()]).unwrap().signed_by(&key, &p.ca, &p.ca_key).unwrap();
    let mut c = config(&addr);
    c.set("security.protocol", "ssl").set("ssl.ca.pem", p.ca_pem.clone());
    // the broker refuses a client without one (at the first request, after the handshake in TLS 1.3)
    let refused = Conn::open(&net(&c), &addr);
    assert!(refused.is_err(), "no client certificate");
    c.set("ssl.certificate.pem", client.pem()).set("ssl.key.pem", key.serialize_pem());
    let mut conn = Conn::open(&net(&c), &addr).unwrap();
    assert_eq!(conn.call(wire::METADATA, |o| o.extend(b"mtls")).unwrap(), b"mtls");
    let dir = std::env::temp_dir().join(format!("fastka-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for (f, v) in [("ca.pem", p.ca_pem.clone()), ("c.pem", client.pem()), ("k.pem", key.serialize_pem())] {
        std::fs::write(dir.join(f), v).unwrap();
    }
    let mut c = config(&addr);
    c.set("security.protocol", "ssl");
    c.set("ssl.ca.location", dir.join("ca.pem").display().to_string());
    c.set("ssl.certificate.location", dir.join("c.pem").display().to_string());
    c.set("ssl.key.location", dir.join("k.pem").display().to_string());
    Conn::open(&net(&c), &addr).unwrap();
    c.set("ssl.key.location", "/nonexistent/k.pem");
    assert!(matches!(c.net(), Err(Error::Config(m)) if m.contains("/nonexistent/k.pem")));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_broker_that_does_not_answer_times_out() {
    let b = Broker::start(|_| Answer::Silence);
    let mut c = config(&b.addr);
    c.set("socket.timeout.ms", "200");
    let mut conn = Conn::open(&net(&c), &b.addr).unwrap();
    let t = Instant::now();
    assert!(matches!(conn.call(wire::METADATA, |_| {}), Err(Error::Timeout(_))));
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
}

#[test]
fn a_dropped_connection_and_a_refused_one_are_transport_errors() {
    let b = Broker::start(|_| Answer::Drop);
    let mut conn = Conn::open(&net(&config(&b.addr)), &b.addr).unwrap();
    assert!(matches!(conn.call(wire::METADATA, |_| {}), Err(Error::Transport(m)) if m.contains("closed")));
    let free = "127.0.0.1:1".to_string(); // nothing listens on port 1
    assert!(matches!(Conn::open(&net(&config(&free)), &free), Err(Error::Transport(m)) if m.contains(&free)));
    let e = Conn::open(&net(&config("no-such-host.invalid:9092")), "no-such-host.invalid:9092").unwrap_err();
    assert!(e.to_string().contains("resolving"), "{e}");
}

/// A response longer than `receive.message.max.bytes` is refused before it is read.
#[test]
fn a_response_past_the_limit_is_refused() {
    let b = Broker::start(|_| Answer::Body(vec![0; 5000]));
    let mut c = config(&b.addr);
    c.set("receive.message.max.bytes", "1000");
    let mut conn = Conn::open(&net(&c), &b.addr).unwrap();
    let e = conn.call(wire::METADATA, |_| {}).unwrap_err();
    assert!(e.to_string().contains("receive.message.max.bytes"), "{e}");
}

/// A frame read in pieces from a non-blocking socket is the frame read whole.
#[test]
fn a_frame_arrives_in_pieces() {
    struct Trickle(Vec<u8>, usize);
    impl Read for Trickle {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            self.1 += 1;
            if self.1.is_multiple_of(2) || self.0.is_empty() {
                return Err(ErrorKind::WouldBlock.into());
            }
            b[0] = self.0.remove(0);
            Ok(1)
        }
    }
    let mut frame = vec![];
    frame.i32(6);
    frame.extend(b"abcdef");
    let mut t = Trickle(frame.clone(), 0);
    let mut buf = vec![];
    let mut reads = 0;
    while !read_frame(&mut t, &mut buf, 100).unwrap() {
        reads += 1;
    }
    assert_eq!((buf, reads > 5), (frame, true));
}

#[test]
fn a_connection_made_off_the_calling_thread() {
    let b = echo();
    let rx = open_in_background(Arc::new(net(&config(&b.addr))), b.addr.clone());
    let mut conn = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    assert_eq!(conn.call(wire::METADATA, |o| o.extend(b"x")).unwrap(), b"x");
}

/// Each connection speaks the newest version both sides speak: Redpanda's Produce stops at v7.
#[test]
fn the_newest_version_both_speak_is_spoken() {
    let mut v = vec![];
    v.i16(0);
    v.count(wire::VERSIONS.len());
    for (k, _, hi, _) in wire::VERSIONS {
        v.i16(*k);
        v.i16(0);
        v.i16(if *k == wire::PRODUCE { 7 } else { *hi });
    }
    let b = Broker::start_full(None, v, |r| Answer::Body(r.body.clone()));
    let mut c = Conn::open(&net(&config(&b.addr)), &b.addr).unwrap();
    assert_eq!((c.version(wire::PRODUCE), c.version(wire::FETCH)), (7, 11));
    c.call(wire::PRODUCE, |_| {}).unwrap();
    assert_eq!(b.requests(wire::PRODUCE)[0].version, 7, "sent at the version negotiated");
    assert_eq!(wire::negotiate(wire::PRODUCE, &[(wire::PRODUCE, 9, 12)]), None, "none in common");
    assert_eq!(wire::negotiate(wire::PRODUCE, &[(wire::PRODUCE, 0, 12)]), Some(8));
}

/// A frame of exactly `receive.message.max.bytes` is read, one byte more refused; a read
/// interrupted by a signal is tried again.
#[test]
fn a_frame_at_the_limit_is_read_and_interrupts_are_retried() {
    struct Script(Vec<std::io::Result<Vec<u8>>>);
    impl Read for Script {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            match self.0.remove(0) {
                Ok(d) => {
                    b[..d.len()].copy_from_slice(&d);
                    Ok(d.len())
                }
                Err(e) => Err(e),
            }
        }
    }
    let mut frame = vec![];
    frame.i32(6);
    frame.extend(b"abcdef");
    let mut s = Script(vec![Err(ErrorKind::Interrupted.into()), Ok(frame[..4].to_vec()), Ok(frame[4..].to_vec())]);
    let mut buf = vec![];
    assert!(read_frame(&mut s, &mut buf, 6).unwrap(), "6 bytes, the limit");
    assert_eq!(buf, frame);
    let mut s = Script(vec![Ok(frame[..4].to_vec())]);
    let e = read_frame(&mut s, &mut vec![], 5).unwrap_err();
    assert!(e.to_string().contains("receive.message.max.bytes is 5"), "{e}");
}

/// A write taken in part, interrupted, or refused for now (a non-blocking socket) goes on from
/// where it was; its flush refused for now is tried again; any other error is the caller's.
#[test]
fn a_write_goes_on_from_where_it_was() {
    struct Script {
        writes: Vec<std::io::Result<usize>>,
        flushes: Vec<std::io::Result<()>>,
        got: Vec<u8>,
    }
    impl Write for Script {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            let n = self.writes.remove(0)?;
            self.got.extend(&b[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes.remove(0)
        }
    }
    let would_block = || Err(ErrorKind::WouldBlock.into());
    let mut s = Script {
        writes: vec![Ok(2), Err(ErrorKind::Interrupted.into()), Ok(1), would_block(), Ok(3)],
        flushes: vec![Err(ErrorKind::WouldBlock.into()), Ok(())],
        got: vec![],
    };
    let mut written = 0;
    assert!(!write_all(&mut s, b"abcdef", &mut written).unwrap(), "refused for now");
    assert_eq!(written, 3);
    assert!(!write_all(&mut s, b"abcdef", &mut written).unwrap(), "its flush refused for now");
    assert!(write_all(&mut s, b"abcdef", &mut written).unwrap());
    assert_eq!(s.got, b"abcdef");
    let mut s = Script { writes: vec![Err(ErrorKind::BrokenPipe.into())], flushes: vec![], got: vec![] };
    assert_eq!(write_all(&mut s, b"x", &mut 0).unwrap_err().kind(), ErrorKind::BrokenPipe);
    let mut s = Script { writes: vec![Ok(1)], flushes: vec![Err(ErrorKind::BrokenPipe.into())], got: vec![] };
    assert_eq!(write_all(&mut s, b"x", &mut 0).unwrap_err().kind(), ErrorKind::BrokenPipe);
}

/// A request's own timeout, shorter than `socket.timeout.ms`, is the one waited; a broker
/// trickling its answer is given up at it too; a broker that reads nothing, while sending.
#[test]
fn a_request_waits_its_own_timeout() {
    let b = Broker::start(|_| Answer::Silence);
    let mut c = config(&b.addr);
    c.set("socket.timeout.ms", "5000");
    let mut conn = Conn::open(&net(&c), &b.addr).unwrap();
    assert_eq!(format!("{:?}", conn.stream), "Stream::Plain");
    conn.set_request_timeout(Duration::from_millis(200));
    let t = Instant::now();
    assert!(matches!(conn.call(wire::METADATA, |_| {}), Err(Error::Timeout(_))));
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());

    // after ApiVersions, a byte every 50 ms of an answer that never ends
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let mut s = s.unwrap();
            std::thread::spawn(move || {
                let mut size = [0; 4];
                s.read_exact(&mut size).unwrap();
                let mut req = vec![0; i32::from_be_bytes(size) as usize];
                s.read_exact(&mut req).unwrap();
                let mut out = vec![];
                out.i32(all_versions().len() as i32 + 4);
                out.extend(&req[4..8]); // its correlation id
                out.extend(all_versions());
                s.write_all(&out).unwrap();
                s.read_exact(&mut size).unwrap(); // the next request's size: answered a byte at a time
                for b in [0u8, 0, 0x10, 0].into_iter().chain(std::iter::repeat(0)) {
                    if s.write_all(&[b]).is_err() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            });
        }
    });
    let mut conn = Conn::open(&net(&config(&addr)), &addr).unwrap();
    conn.set_request_timeout(Duration::from_millis(300));
    let t = Instant::now();
    assert!(matches!(conn.call(wire::METADATA, |_| {}), Err(Error::Timeout(_))));
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());

    let deaf = Broker::start(|_| Answer::Silence); // reads the first request, then none
    let mut conn = Conn::open(&net(&config(&deaf.addr)), &deaf.addr).unwrap();
    conn.set_request_timeout(Duration::from_millis(300));
    let _ = conn.call(wire::METADATA, |_| {}); // unanswered: the broker reads no more
    let t = Instant::now();
    let e = conn.call(wire::METADATA, |o| o.extend(vec![0u8; 64 << 20])).unwrap_err();
    assert!(matches!(&e, Error::Timeout(m) if m.contains("sending a request")), "{e}");
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
}

/// A TLS handshake the broker never answers is a timeout, said as the handshake's.
#[test]
fn a_tls_handshake_unanswered_times_out() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let _held: Vec<_> = l.incoming().collect(); // accepted, never answered
    });
    let p = pki(&["127.0.0.1"]);
    let mut c = config(&addr);
    c.set("security.protocol", "ssl").set("ssl.ca.pem", p.ca_pem.clone());
    c.set("socket.connection.setup.timeout.ms", "300");
    let t = Instant::now();
    let e = Conn::open(&net(&c), &addr).unwrap_err();
    assert!(matches!(&e, Error::Timeout(m) if m.contains("TLS handshake")), "{e}");
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
}

/// A TLS broker trickling its answer (a byte every 100 ms) holds a request far past its
/// timeout: rustls loops inside one `Until::read` until a whole record is in, each socket read
/// waiting the time left as it was when `Until::read` began.
#[test]
fn a_tls_broker_trickling_its_answer_cannot_outlive_the_timeout() {
    let p = pki(&["localhost"]);
    let cfg = rustls::ServerConfig::builder_with_provider(crate::testkit::provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![p.cert.clone()], p.key.clone_key())
        .unwrap();
    let cfg = Arc::new(cfg);
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let (mut sock, _) = l.accept().unwrap();
        let mut conn = rustls::ServerConnection::new(cfg).unwrap();
        let mut n = 0;
        loop {
            // read a request
            let mut s = rustls::Stream::new(&mut conn, &mut sock);
            let mut size = [0; 4];
            if s.read_exact(&mut size).is_err() {
                return;
            }
            let mut b = vec![0; i32::from_be_bytes(size) as usize];
            s.read_exact(&mut b).unwrap();
            let corr = i32::from_be_bytes(b[4..8].try_into().unwrap());
            let body = if n == 0 { all_versions() } else { vec![7u8; 200] };
            let mut out = vec![];
            out.i32(body.len() as i32 + 4);
            out.i32(corr);
            out.extend(body);
            conn.writer().write_all(&out).unwrap();
            let mut tls = vec![];
            while conn.wants_write() {
                conn.write_tls(&mut tls).unwrap();
            }
            if n == 0 {
                sock.write_all(&tls).unwrap();
            } else {
                for byte in tls {
                    if sock.write_all(&[byte]).is_err() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            n += 1;
        }
    });
    let addr = format!("localhost:{port}");
    let mut c = config(&addr);
    c.set("security.protocol", "ssl").set("ssl.ca.pem", p.ca_pem.clone());
    c.set("socket.timeout.ms", "500");
    let mut conn = Conn::open(&net(&c), &addr).unwrap();
    let t = Instant::now();
    let r = conn.call(wire::METADATA, |_| {});
    assert!(
        t.elapsed() < Duration::from_secs(2),
        "socket.timeout.ms=500 but the call took {:?}: {:?}",
        t.elapsed(),
        r.map(|v| v.len())
    );
}

/// `Conn`'s derived Debug prints `out`, the last request sent: after PLAIN, the password.
#[test]
fn debug_never_shows_the_password() {
    let b = sasl_broker("svc", "s3cret");
    let mut c = config(&b.addr);
    c.set("security.protocol", "sasl_plaintext").set("sasl.mechanisms", "PLAIN").set("sasl.username", "svc");
    c.set("sasl.password", "s3cret");
    let conn = Conn::open(&net(&c), &b.addr).unwrap();
    let d = format!("{conn:?}");
    let pw: String = b"s3cret".iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", ");
    assert!(!d.contains(&pw), "Debug of Conn holds the password's bytes: {d}");
}

/// A bootstrap broker answering TLS with garbage (a plaintext listener) is Error::Auth, which
/// `Client::bootstrap` returns at once, never trying the next (healthy) bootstrap broker.
#[test]
fn bootstrap_tries_the_next_broker_after_one_speaking_no_tls() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bad = format!("localhost:{}", l.local_addr().unwrap().port());
    std::thread::spawn(move || {
        for s in l.incoming() {
            let mut s = s.unwrap();
            let _ = s.write_all(b"\x00\x00\x00\x10garbage-not-tls!!!!!!!!!!");
            std::thread::sleep(Duration::from_millis(200));
        }
    });
    let p = pki(&["localhost"]);
    let good = tls_broker(&p, None);
    let good = localhost(&good);
    let mut c = config(&format!("{bad},{good}"));
    c.set("security.protocol", "ssl").set("ssl.ca.pem", p.ca_pem.clone());
    let client = crate::client::Client::new(&c).unwrap();
    let r = client.metadata(Some(&[]), Duration::from_secs(3));
    assert!(!matches!(r, Err(Error::Auth(_))), "the second bootstrap broker never tried: {r:?}");
}

/// The request header carries `client.id`, as the broker's logs and quotas know the client.
#[test]
fn requests_carry_the_client_id() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut b = [0; 4];
        s.read_exact(&mut b).unwrap();
        let mut b = vec![0; i32::from_be_bytes(b) as usize];
        s.read_exact(&mut b).unwrap();
        let mut c = wire::Cur::new(&b[8..]);
        tx.send(c.nstr().unwrap().map(str::to_string)).unwrap();
    });
    let mut c = config(&addr);
    c.set("client.id", "billing");
    let _ = Conn::open(&net(&c), &addr);
    assert_eq!(rx.recv().unwrap().as_deref(), Some("billing"));
}

/// Connecting has one deadline: SASL's steps share what is left of it, not each a fresh one.
#[test]
fn connecting_steps_share_one_deadline() {
    let b = Broker::start(|r| {
        std::thread::sleep(Duration::from_millis(600));
        let mut out = vec![];
        match r.api {
            wire::SASL_HANDSHAKE => {
                out.i16(0);
                out.count(1);
                out.str("PLAIN");
            }
            _ => {
                out.i16(0);
                out.nstr(None);
                out.bytes(b"");
            }
        }
        Answer::Body(out)
    });
    let mut c = config(&b.addr);
    c.set("security.protocol", "sasl_plaintext").set("sasl.mechanisms", "PLAIN").set("sasl.username", "u");
    c.set("sasl.password", "p").set("socket.connection.setup.timeout.ms", "1000");
    let t = Instant::now();
    let r = Conn::open(&net(&c), &b.addr);
    assert!(matches!(r, Err(Error::Timeout(_))), "two 600 ms steps within 1000 ms: {r:?}");
    assert!(t.elapsed() < Duration::from_millis(1300), "{:?}", t.elapsed());
}

/// A bootstrap broker that does not speak this client's versions is skipped for the next one.
#[test]
fn bootstrap_tries_the_next_broker_after_an_unsupported_one() {
    let mut v = vec![];
    v.i16(35);
    v.count(0);
    let bad = Broker::start_full(None, v, |_| Answer::Drop);
    let good = Broker::start(|r| match r.api {
        wire::METADATA => Answer::Body(crate::testkit::metadata(&[(0, &r.me)], &[])),
        _ => Answer::Drop,
    });
    let client = crate::client::Client::new(&config(&format!("{},{}", bad.addr, good.addr))).unwrap();
    client.metadata(Some(&[]), Duration::from_secs(3)).unwrap();
    // every one refusing: the refusal, not a transport error
    let client = crate::client::Client::new(&config(&format!("{},127.0.0.1:1", bad.addr))).unwrap();
    let r = client.metadata(Some(&[]), Duration::from_secs(1));
    assert!(matches!(r, Err(Error::Unsupported(_))), "{r:?}");
}

/// A socket's timeout is the deadline's WouldBlock on every platform (Unix says WouldBlock,
/// Windows TimedOut); any other error is itself.
#[test]
fn a_sockets_timeout_is_would_block_and_nothing_else_is() {
    let err = |k: ErrorKind| timed_out::<()>(Err(k.into())).unwrap_err().kind();
    assert_eq!(err(ErrorKind::TimedOut), ErrorKind::WouldBlock);
    assert_eq!(err(ErrorKind::WouldBlock), ErrorKind::WouldBlock);
    assert_eq!(err(ErrorKind::ConnectionReset), ErrorKind::ConnectionReset);
    assert_eq!(timed_out(Ok(3)).unwrap(), 3);
}

/// No limit (`Duration::MAX`, past what the clock counts) is a century on: never now, nor a year.
#[test]
fn no_limit_is_a_century_on() {
    let far = deadline_in(Duration::MAX).saturating_duration_since(Instant::now());
    assert!(far > Duration::from_secs(50 * 365 * 86_400), "{far:?}");
    let near = deadline_in(Duration::from_secs(2)).saturating_duration_since(Instant::now());
    assert!(near <= Duration::from_secs(2) && near > Duration::from_secs(1), "{near:?}");
}

/// A certificate refused, by this client or by the broker's alert, is Auth (a retry meets it
/// again); another TLS failure is Transport, said as TLS's; any other I/O error plain Transport.
#[test]
fn tls_refusals_are_auth_and_other_failures_transport() {
    use rustls::AlertDescription as A;
    let tls = |e: rustls::Error| tls_error("b:1", std::io::Error::new(ErrorKind::InvalidData, e));
    assert!(matches!(tls(rustls::Error::AlertReceived(A::BadCertificate)), Error::Auth(m) if m.contains("TLS")));
    assert!(matches!(tls(rustls::Error::AlertReceived(A::UnknownCA)), Error::Auth(_)));
    assert!(matches!(tls(rustls::Error::NoCertificatesPresented), Error::Auth(_)));
    assert!(
        matches!(tls(rustls::Error::AlertReceived(A::HandshakeFailure)), Error::Transport(m) if m.contains("TLS:"))
    );
    let plain = tls_error("b:1", std::io::Error::from(ErrorKind::ConnectionReset));
    assert!(matches!(&plain, Error::Transport(m) if !m.contains("TLS")), "{plain}");
}

/// A connection's descriptor is its own socket's (what the consumer waits on): two connections,
/// two descriptors, neither a standard stream.
#[test]
fn a_connections_descriptor_is_its_sockets() {
    let b = Broker::start(|_| Answer::Drop);
    let one = Conn::open(&net(&config(&b.addr)), &b.addr).unwrap();
    let two = Conn::open(&net(&config(&b.addr)), &b.addr).unwrap();
    let (a, c) = (one.stream.raw_fd(), two.stream.raw_fd());
    assert!(a > 2 && c > 2 && a != c, "descriptors {a} and {c}");
}

/// A connection's Debug names it and its broker, never its buffers (the last request may be
/// SASL's, with the password).
#[test]
fn a_connections_debug_names_it_and_hides_its_buffers() {
    let b = Broker::start(|_| Answer::Drop);
    let c = Conn::open(&net(&config(&b.addr)), &b.addr).unwrap();
    let d = format!("{c:?}");
    assert!(d.contains("Conn") && d.contains(&b.addr), "{d}");
    assert!(!d.contains("out") && !d.contains("apis"), "{d}");
}
