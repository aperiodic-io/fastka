#![allow(clippy::type_complexity)] // the tests' tuples say what each field is where they are made
use super::*;
use crate::testkit::{config, metadata, Answer, Broker, Req};
use crate::wire::Put;
use std::sync::atomic::{AtomicUsize, Ordering};

const T: Duration = Duration::from_secs(5);

fn list_offsets_answer(r: &Req, offset: impl Fn(i32, i64) -> (i16, i64)) -> Vec<u8> {
    let mut c = Cur::new(&r.body);
    c.i32().unwrap();
    c.i8().unwrap();
    let mut out = vec![];
    out.i32(0);
    let nt = c.i32().unwrap();
    out.i32(nt);
    for _ in 0..nt {
        let t = c.str().unwrap().to_string();
        out.str(&t);
        let np = c.i32().unwrap();
        out.i32(np);
        for _ in 0..np {
            let (p, ts) = (c.i32().unwrap(), c.i64().unwrap());
            let (code, o) = offset(p, ts);
            out.i32(p);
            out.i16(code);
            out.i64(-1);
            out.i64(o);
        }
    }
    out
}

/// Two brokers: `b1` answers metadata naming `b2` the leader of t/0 and t/1, and `b2` the offsets.
#[test]
fn offsets_are_asked_of_each_partitions_leader() {
    let b2 = Broker::start(|r| match r.api {
        wire::LIST_OFFSETS => Answer::Body(list_offsets_answer(r, |p, ts| match ts {
            wire::EARLIEST => (0, 10 * p as i64),
            wire::LATEST => (0, 100 + p as i64),
            1234 => (0, 50),
            _ => (0, -1), // nothing that recent
        })),
        _ => Answer::Drop,
    });
    let a2 = b2.addr.clone();
    let b1 = Broker::start(move |r| match r.api {
        // the leader is broker 1: never the bootstrap connection, whose id is not known
        wire::METADATA => Answer::Body(metadata(&[(2, "127.0.0.1:1"), (1, &a2)], &[("t", &[(1, 1), (0, 1)])])),
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&b1.addr)).unwrap();
    assert!(format!("{c:?}").starts_with("Client { net: "), "{c:?}");
    assert_eq!(c.partitions("t", T).unwrap(), [0, 1], "in order");
    assert_eq!(c.watermarks("t", 1, true, T).unwrap(), (10, 101));
    let got = c.list_offsets(&[("t".into(), 0, 1234), ("t".into(), 1, 9999)], false, T).unwrap();
    assert_eq!(got, [("t".into(), 0, Ok(Some(50))), ("t".into(), 1, Ok(None))]);
    assert!(b1.requests(wire::LIST_OFFSETS).is_empty(), "asked of the leader only");
    let r = b2.requests(wire::LIST_OFFSETS);
    assert_eq!(wire::Cur::new(&r[0].body).i32().unwrap(), -1, "a consumer's replica id");
    assert_eq!(r[0].body[4], 1, "read committed when asked");
    assert_eq!(r.last().unwrap().body[4], 0);
    let (leaders, brokers) = c.leaders("t", T).unwrap();
    assert_eq!((leaders, brokers.len()), (vec![(1, 1), (0, 1)], 2));
    assert_eq!(c.cluster_id(T).as_deref(), Some("test-cluster"));
}

/// A leader that moved answers NOT_LEADER_OR_FOLLOWER: the metadata is looked up again and the
/// request sent again, within the call's timeout, as librdkafka does.
#[test]
fn a_moved_leader_is_looked_up_again() {
    let tries = Arc::new(AtomicUsize::new(0));
    let t2 = tries.clone();
    let b = Broker::start(move |r| match r.api {
        wire::LIST_OFFSETS if t2.fetch_add(1, Ordering::SeqCst) < 2 => {
            Answer::Body(list_offsets_answer(r, |_, _| (6, -1)))
        }
        wire::LIST_OFFSETS => Answer::Body(list_offsets_answer(r, |_, _| (0, 7))),
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[("t", &[(0, 0)])])),
        _ => Answer::Drop,
    });
    let mut cfg = config(&b.addr);
    cfg.set("reconnect.backoff.ms", "1");
    let c = Client::new(&cfg).unwrap();
    assert_eq!(c.watermarks("t", 0, false, T).unwrap(), (7, 7));
    assert!(tries.load(Ordering::SeqCst) >= 3);
    assert!(b.requests(wire::METADATA).len() >= 3, "the metadata asked again after each NOT_LEADER_OR_FOLLOWER");
}

/// What a retry cannot mend is said at once: an unknown topic within the timeout, an
/// authorization never retried.
#[test]
fn what_a_retry_cannot_mend_fails() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => {
            let mut out = metadata(&[(0, "127.0.0.1:1")], &[]);
            // one topic: unknown
            out.truncate(out.len() - 4);
            out.i32(1);
            out.i16(3);
            out.str("nope");
            out.i8(0);
            out.i32(0);
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&b.addr)).unwrap();
    let t = Instant::now();
    let e = c.partitions("nope", Duration::from_millis(300)).unwrap_err();
    assert_eq!(e.code(), Some(Code::UNKNOWN_TOPIC_OR_PARTITION), "{e}");
    assert!(t.elapsed() < Duration::from_secs(2));
    let md = c.metadata(Some(&["nope"]), T).unwrap();
    assert_eq!(md.topic("nope").unwrap().error, Code::UNKNOWN_TOPIC_OR_PARTITION, "the metadata itself says so");

    let mut cfg = config(&b.addr);
    cfg.set("security.protocol", "sasl_plaintext").set("sasl.mechanisms", "PLAIN");
    cfg.set("sasl.username", "u").set("sasl.password", "p");
    let auth = Broker::start(|r| {
        let mut out = vec![];
        out.i16(if r.api == wire::SASL_HANDSHAKE { 33 } else { 0 });
        out.count(0);
        Answer::Body(out)
    });
    cfg.set("bootstrap.servers", auth.addr.clone());
    let c = Client::new(&cfg).unwrap();
    let t = Instant::now();
    assert!(matches!(c.metadata(None, T), Err(Error::Auth(_))));
    assert!(t.elapsed() < Duration::from_secs(1), "not retried: {:?}", t.elapsed());
}

/// The first bootstrap broker down, the next answers; a connection dropped mid-call is made again.
#[test]
fn bootstrap_failover_and_a_dropped_connection() {
    let drops = Arc::new(AtomicUsize::new(0));
    let d2 = drops.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA if d2.fetch_add(1, Ordering::SeqCst) == 0 => Answer::Drop,
        wire::METADATA => Answer::Body(metadata(&[(0, "127.0.0.1:1")], &[("t", &[(0, 0)])])),
        _ => Answer::Drop,
    });
    let dead = "127.0.0.1:1"; // nothing listens on port 1: refused at once
    let mut cfg = config(&format!("{dead},{}", b.addr));
    cfg.set("reconnect.backoff.ms", "1");
    let c = Client::new(&cfg).unwrap();
    assert_eq!(c.partitions("t", T).unwrap(), [0]);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    let conns: std::collections::HashSet<usize> = b.requests(wire::METADATA).iter().map(|r| r.conn).collect();
    assert_eq!(conns.len(), 2, "a new connection after the drop");
}

#[test]
fn topics_are_created_by_the_controller() {
    let moved = Arc::new(AtomicUsize::new(0));
    let m2 = moved.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[])), // it is the controller
        wire::CREATE_TOPICS => {
            let mut c = Cur::new(&r.body);
            let n = c.i32().unwrap();
            let mut out = vec![];
            out.i32(0);
            out.i32(n);
            for _ in 0..n {
                let name = c.str().unwrap().to_string();
                let (parts, rf) = (c.i32().unwrap(), c.i16().unwrap());
                c.i32().unwrap();
                c.i32().unwrap();
                assert_eq!((parts, rf), (1, -1));
                out.str(&name);
                let code = if m2.fetch_add(1, Ordering::SeqCst) == 0 {
                    41
                } else if name == "old" {
                    36
                } else {
                    0
                };
                out.i16(code);
                out.nstr(None);
            }
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let mut cfg = config(&b.addr);
    cfg.set("reconnect.backoff.ms", "1");
    let c = Client::new(&cfg).unwrap();
    let got = c.create_topics(&[("new".into(), 1, -1), ("old".into(), 1, -1)], T).unwrap();
    assert_eq!(got[0], ("new".into(), Ok(())));
    assert_eq!(got[1].1.as_ref().unwrap_err().code(), Some(Code::TOPIC_ALREADY_EXISTS));
    assert!(moved.load(Ordering::SeqCst) >= 3, "NOT_CONTROLLER retried");
}

#[test]
fn topic_configs_are_described() {
    let b = Broker::start(|r| match r.api {
        wire::DESCRIBE_CONFIGS => {
            let mut out = vec![];
            out.i32(0);
            out.i32(2);
            for (name, code) in [("t", 0i16), ("u", 29)] {
                out.i16(code);
                out.nstr(None);
                out.i8(2);
                out.str(name);
                out.i32(if code == 0 { 2 } else { 0 });
                if code == 0 {
                    for (k, v) in [("cleanup.policy", "delete"), ("retention.ms", "86400000")] {
                        out.str(k);
                        out.nstr(Some(v));
                        out.i8(0);
                        out.i8(5);
                        out.i8(0);
                        out.i32(0);
                    }
                }
            }
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&b.addr)).unwrap();
    let got = c.describe_topic_configs(&["t", "u"], T).unwrap();
    assert_eq!(got[0].as_ref().unwrap()["retention.ms"].as_deref(), Some("86400000"));
    assert_eq!(got[1].as_ref().unwrap_err().code(), Some(Code::TOPIC_AUTHORIZATION_FAILED));
}

/// A group's offsets go to its coordinator, found first; a coordinator still loading is asked again.
#[test]
fn offsets_are_committed_to_the_groups_coordinator() {
    let loads = Arc::new(AtomicUsize::new(0));
    let l2 = loads.clone();
    let coord = Broker::start(move |r| match r.api {
        wire::OFFSET_COMMIT => {
            let mut c = Cur::new(&r.body);
            assert_eq!((c.str().unwrap(), c.i32().unwrap(), c.str().unwrap()), ("g", -1, ""));
            let mut out = vec![];
            out.i32(1);
            out.str("t");
            out.i32(1);
            out.i32(0);
            out.i16(if l2.fetch_add(1, Ordering::SeqCst) == 0 { 14 } else { 0 });
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let a = coord.addr.clone();
    let b = Broker::start(move |r| match r.api {
        wire::FIND_COORDINATOR => {
            let (host, port) = a.rsplit_once(':').unwrap();
            let mut out = vec![];
            out.i32(0);
            out.i16(0);
            out.nstr(None);
            out.i32(7);
            out.str(host);
            out.i32(port.parse().unwrap());
            Answer::Body(out)
        }
        wire::METADATA => Answer::Body(metadata(&[(0, "127.0.0.1:1")], &[])),
        _ => Answer::Drop,
    });
    let mut cfg = config(&b.addr);
    cfg.set("reconnect.backoff.ms", "1");
    let c = Client::new(&cfg).unwrap();
    c.commit("g", &[("t".into(), 0, 42)], T).unwrap();
    assert_eq!(loads.load(Ordering::SeqCst), 2, "COORDINATOR_LOAD_IN_PROGRESS retried");
    let r = coord.requests(wire::OFFSET_COMMIT);
    let mut c = Cur::new(&r[1].body);
    for _ in 0..3 {
        let _ = c.str().map(|_| ()).or_else(|_| c.i32().map(|_| ()));
    }
    assert!(r[1].body.windows(8).any(|w| w == 42i64.to_be_bytes()), "the offset");
}

/// The client is shared between threads: calls from two at once each get their own answer.
#[test]
fn calls_from_many_threads() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, "127.0.0.1:1")], &[("t", &[(0, 0), (1, 0), (2, 0)])])),
        _ => Answer::Drop,
    });
    let c = Arc::new(Client::new(&config(&b.addr)).unwrap());
    let hs: Vec<_> = (0..8)
        .map(|_| {
            let c = c.clone();
            std::thread::spawn(move || (0..20).all(|_| c.partitions("t", T).unwrap() == [0, 1, 2]))
        })
        .collect();
    assert!(hs.into_iter().all(|h| h.join().unwrap()));
}

/// A Metadata v4 response of broker 0 at `me`: topics (topic error, name, partitions (id, leader)).
fn md_of(me: &str, topics: &[(i16, &str, &[(i32, i32)])]) -> Vec<u8> {
    let mut b = metadata(&[(0, me)], &[]);
    b.truncate(b.len() - 4);
    b.count(topics.len());
    for (code, name, parts) in topics {
        b.i16(*code);
        b.str(name);
        b.i8(0);
        b.count(parts.len());
        for (p, leader) in parts.iter() {
            b.i16(0);
            b.i32(*p);
            b.i32(*leader);
            b.count(0);
            b.count(0);
        }
    }
    b
}

/// ListOffsets says each partition's own outcome: a topic refused and a partition refused at
/// once, without waiting out the timeout, beside the partitions answered.
#[test]
fn list_offsets_says_what_a_retry_cannot_mend_at_once() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => Answer::Body(md_of(&r.me, &[(29, "denied", &[]), (0, "t", &[(0, 0), (1, 0)])])),
        wire::LIST_OFFSETS => Answer::Body(list_offsets_answer(r, |p, _| if p == 0 { (0, 5) } else { (29, -1) })),
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&b.addr)).unwrap();
    let t = Instant::now();
    let asked = [("denied".into(), 0, wire::LATEST), ("t".into(), 0, wire::LATEST), ("t".into(), 1, wire::LATEST)];
    let got = c.list_offsets(&asked, false, Duration::from_secs(3)).unwrap();
    let codes: Vec<Option<Code>> = got.iter().map(|g| g.2.as_ref().err().and_then(Error::code)).collect();
    assert_eq!(codes, [Some(Code(29)), None, Some(Code(29))]);
    assert_eq!(got[1].2.as_ref().unwrap(), &Some(5));
    assert!(t.elapsed() < Duration::from_secs(1), "not retried: {:?}", t.elapsed());
}

/// What may mend is asked again until the timeout, then said: a partition without a leader, a
/// topic's leader not available, a partition the topic does not have.
#[test]
fn list_offsets_waits_for_what_may_mend_then_says_why() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => {
            Answer::Body(md_of(&r.me, &[(0, "noleader", &[(0, -1)]), (5, "busy", &[]), (0, "t", &[(0, 0)])]))
        }
        _ => Answer::Drop,
    });
    let mut cfg = config(&b.addr);
    cfg.set("reconnect.backoff.ms", "10");
    let c = Client::new(&cfg).unwrap();
    for (topic, p, code) in [("noleader", 0, 5), ("busy", 0, 5), ("t", 3, 3)] {
        let t = Instant::now();
        let e = c.list_offsets(&[(topic.into(), p, wire::LATEST)], false, Duration::from_millis(300)).unwrap_err();
        assert_eq!(e.code(), Some(Code(code)), "{topic}: {e}");
        assert!(
            t.elapsed() >= Duration::from_millis(100),
            "{topic}: retried, until the next wait would pass the timeout"
        );
    }
}

/// A leader's connection dropped is made again and asked again; a leader this client cannot
/// speak to is said at once.
#[test]
fn list_offsets_of_a_lost_and_a_refusing_leader() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let d2 = dropped.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(md_of(&r.me, &[(0, "t", &[(0, 0)])])),
        wire::LIST_OFFSETS if d2.fetch_add(1, Ordering::SeqCst) == 0 => Answer::Drop,
        wire::LIST_OFFSETS => Answer::Body(list_offsets_answer(r, |_, _| (0, 9))),
        _ => Answer::Drop,
    });
    let mut cfg = config(&b.addr);
    cfg.set("reconnect.backoff.ms", "1");
    let c = Client::new(&cfg).unwrap();
    assert_eq!(c.watermarks("t", 0, false, T).unwrap(), (9, 9));
    assert!(dropped.load(Ordering::SeqCst) >= 2);

    let mut v = vec![];
    v.i16(0);
    v.count(wire::VERSIONS.len());
    for (k, lo, hi, _) in wire::VERSIONS {
        v.i16(*k);
        v.i16(if *k == wire::LIST_OFFSETS { hi + 1 } else { *lo });
        v.i16(hi + 5);
    }
    let old = Broker::start_full(None, v, |_| Answer::Drop);
    let at = old.addr.clone();
    let front = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me), (1, &at)], &[("t", &[(0, 1)])])),
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&front.addr)).unwrap();
    let t = Instant::now();
    let e = c.list_offsets(&[("t".into(), 0, wire::LATEST)], false, Duration::from_secs(3)).unwrap_err();
    assert!(matches!(&e, Error::Unsupported(m) if m.contains("ListOffsets")), "{e}");
    assert!(t.elapsed() < Duration::from_secs(1), "not retried: {:?}", t.elapsed());
}

/// A call that fails for now is tried again with a wait doubling from `reconnect.backoff.ms`;
/// one without time left names its request.
#[test]
fn calls_are_tried_again_with_a_growing_wait() {
    let b = Broker::start(|_| Answer::Drop);
    let mut cfg = config(&b.addr);
    cfg.set("reconnect.backoff.ms", "50").set("reconnect.backoff.max.ms", "1000");
    let c = Client::new(&cfg).unwrap();
    assert!(c.metadata(None, Duration::from_secs(1)).is_err());
    // at 0, 50, 150, 350 and 750 ms
    let n = b.requests(wire::METADATA).len();
    assert!((3..=7).contains(&n), "{n} tries in 1 s");
    let e = c.metadata(None, Duration::ZERO).unwrap_err();
    assert!(e.to_string().contains("Metadata within its timeout"), "{e}");
}

/// A broker whose ListOffsets answer omits a partition it was asked: the client panics.
#[test]
fn a_partition_left_out_of_the_answer_is_no_panic() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[("t", &[(0, 0), (1, 0)])])),
        wire::LIST_OFFSETS => {
            // answers only partition 0
            let mut out = vec![];
            out.i32(0);
            out.i32(1);
            out.str("t");
            out.i32(1);
            out.i32(0);
            out.i16(0);
            out.i64(-1);
            out.i64(42);
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&b.addr)).unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        c.list_offsets(&[("t".into(), 0, -1), ("t".into(), 1, -1)], false, T)
    }));
    assert!(r.is_ok(), "list_offsets panicked on a broker's answer");
}

/// CreateTopics is retried after a timeout: the broker created the topic the first time, the
/// retry answers TOPIC_ALREADY_EXISTS, and the caller is told its own topic already existed.
#[test]
fn create_topics_retried_after_a_lost_answer_reports_its_own_topic_created() {
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = n.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[])),
        wire::CREATE_TOPICS => {
            if n2.fetch_add(1, Ordering::SeqCst) == 0 {
                return Answer::Silence; // created, but the answer is lost/slow
            }
            let mut out = vec![];
            out.i32(0);
            out.i32(1);
            out.str("new");
            out.i16(36);
            out.nstr(None);
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let mut cfg = config(&b.addr);
    cfg.set("socket.timeout.ms", "300");
    cfg.set("reconnect.backoff.ms", "1");
    let c = Client::new(&cfg).unwrap();
    let got = c.create_topics(&[("new".into(), 1, -1)], T).unwrap();
    assert_eq!(n.load(Ordering::SeqCst), 2, "sent twice");
    assert_eq!(got[0].1, Ok(()), "the caller's own topic reported as {:?}", got[0].1);
}

/// partitions() of a topic the cluster does not have spins for the whole timeout.
#[test]
fn partitions_of_a_topic_the_cluster_has_not_fail_fast() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => {
            let mut m = vec![];
            m.i32(0);
            m.i32(1);
            m.i32(0);
            let (h, p) = r.me.rsplit_once(':').unwrap();
            m.str(h);
            m.i32(p.parse().unwrap());
            m.nstr(None);
            m.nstr(None);
            m.i32(0);
            m.i32(1);
            m.i16(3);
            m.str("nope");
            m.i8(0);
            m.i32(0);
            Answer::Body(m)
        }
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&b.addr)).unwrap();
    let t0 = Instant::now();
    let e = c.partitions("nope", Duration::from_secs(3)).unwrap_err();
    assert!(t0.elapsed() < Duration::from_secs(1), "took {:?}: {e}", t0.elapsed());
}

/// A group id over 32767 bytes panics inside the library (Put::str's expect).
#[test]
fn an_overlong_name_is_refused_not_a_panic() {
    let b = Broker::start(|_| Answer::Drop);
    let c = Client::new(&config(&b.addr)).unwrap();
    let g = "g".repeat(40_000);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        c.commit(&g, &[("t".into(), 0, 1)], Duration::from_secs(1))
    }));
    assert!(r.is_ok(), "commit panicked on a long group id");
}

/// A caller's "no limit" (Duration::MAX) panics: Instant + Duration overflows.
#[test]
fn duration_max_is_no_limit_not_a_panic() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[])),
        _ => Answer::Drop,
    });
    let c = Client::new(&config(&b.addr)).unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| c.metadata(None, Duration::MAX)));
    assert!(r.is_ok(), "metadata(None, Duration::MAX) panicked");
}
