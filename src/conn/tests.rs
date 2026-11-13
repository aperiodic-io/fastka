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
    let dir = std::env::temp_dir().join(format!("brrrrr-kafka-tls-{}", std::process::id()));
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
