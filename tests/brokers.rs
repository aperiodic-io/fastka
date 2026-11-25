#![allow(clippy::type_complexity)] // the tests' tuples say what each field is where they are made
//! Against real brokers: this client with itself, and with `rpk` (franz-go, an implementation of
//! its own) both ways, in every codec; the control plane; SASL. Each broker is given by an
//! environment variable, and a test whose broker is not set is skipped:
//!
//! - `FASTKA_IT=host:port` and `FASTKA_IT_RPK=<container>` (Redpanda, `rpk` in it)
//! - `FASTKA_IT_APACHE=host:port` (Apache Kafka)
//! - `FASTKA_IT_SASL=host:port` (Redpanda with SASL/SCRAM, user `it`/`it-secret` of both
//!   SHA-256 and SHA-512, `it512`/`it-secret`)
use fastka::client::Client;
use fastka::consumer::{Assignment, Consumer, Message, Offset, Polled};
use fastka::producer::{Deliveries, Delivery, Producer, Record};
use fastka::{Code, Config, Error};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

const T: Duration = Duration::from_secs(20);

fn brokers(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|v| !v.is_empty())
}

fn cfg(addr: &str) -> Config {
    let mut c = Config::new();
    c.set("bootstrap.servers", addr).set("enable.partition.eof", "true").set("fetch.wait.max.ms", "20");
    c.set("linger.ms", "2");
    c
}

fn unique(name: &str) -> String {
    let n = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
    format!("it-{name}-{n}")
}

#[derive(Default)]
struct Heard(Mutex<Vec<(Option<i32>, Option<i64>, Option<String>, u64)>>);

impl Deliveries for Heard {
    type Opaque = u64;
    fn delivery(&self, d: &Delivery<'_>, o: u64) {
        self.0.lock().unwrap().push((d.partition, d.offset, d.error.map(|e| e.to_string()), o));
    }
}

fn produce_all(c: &Config, topic: &str, values: &[String]) -> Vec<(Option<i32>, Option<i64>, Option<String>, u64)> {
    let p = Producer::new(c, Heard::default()).unwrap();
    for (i, v) in values.iter().enumerate() {
        let h = [("dedup".into(), Some(format!("d{i}").into_bytes()))];
        p.send(Record::to(topic).value(v.as_bytes()).headers(&h), i as u64).unwrap();
    }
    p.flush(T).unwrap();
    let mut h = p.context().0.lock().unwrap().clone();
    h.sort_by_key(|x| x.3);
    h
}

/// Every message of `parts` from their start, until each says its end.
fn consume_all(c: &Config, topic: &str, parts: &[i32]) -> Vec<(i32, Message)> {
    let assign = parts.iter().map(|p| Assignment::new(topic, *p, Offset::Beginning)).collect();
    let mut k = Consumer::from_config(c, assign).unwrap();
    let (mut out, mut ends) = (vec![], std::collections::HashSet::new());
    let until = Instant::now() + T;
    while ends.len() < parts.len() {
        assert!(Instant::now() < until, "{} of {} partitions at their end", ends.len(), parts.len());
        match k.next_event(Duration::from_millis(100), &mut |n| eprintln!("notice: {n}")).unwrap() {
            Some((i, Polled::Msg(m))) => out.push((parts[i], m)),
            Some((i, Polled::Eof)) => drop(ends.insert(i)),
            _ => {}
        }
    }
    out
}

fn rpk(container: &str, args: &[&str], stdin: Option<&str>) -> String {
    let mut cmd = Command::new("docker");
    cmd.args(["exec", "-i", container, "rpk"]).args(args);
    let out = match stdin {
        None => cmd.output().unwrap(),
        Some(input) => {
            use std::io::Write;
            let mut child =
                cmd.stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).spawn().unwrap();
            child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
            child.wait_with_output().unwrap()
        }
    };
    assert!(out.status.success(), "rpk {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout).unwrap()
}

/// This client with itself: created, produced in each codec, read back in order with headers,
/// the control plane's answers consistent with what was written.
fn round_trip(addr: &str) {
    let c = cfg(addr);
    let client = Client::new(&c).unwrap();
    let topic = unique("rt");
    let made = client.create_topics(&[(topic.clone(), 3, -1)], T).unwrap();
    assert_eq!(made[0].1, Ok(()));
    let again = client.create_topics(&[(topic.clone(), 3, -1)], T).unwrap();
    assert_eq!(again[0].1.as_ref().unwrap_err().code(), Some(Code::TOPIC_ALREADY_EXISTS));
    let parts = client.partitions(&topic, T).unwrap();
    assert_eq!(parts, [0, 1, 2]);
    let mut all = vec![];
    let before = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64;
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let mut pc = c.clone();
        pc.set("compression.type", codec).set("batch.num.messages", "50");
        let values: Vec<String> = (0..200).map(|i| format!("{codec}-{i:04}-{}", "x".repeat(i % 37))).collect();
        let h = produce_all(&pc, &topic, &values);
        assert!(h.iter().all(|x| x.2.is_none() && x.1.is_some()), "{codec}: {:?}", h.iter().find(|x| x.2.is_some()));
        all.extend(values.into_iter().zip(h));
    }
    let read = consume_all(&c, &topic, &parts);
    assert_eq!(read.len(), all.len(), "each once");
    for (v, (p, o, _, i)) in &all {
        let (_, m) = read.iter().find(|(rp, m)| Some(*rp) == *p && Some(m.offset) == *o).expect("at its offset");
        assert_eq!(m.value.as_deref(), Some(v.as_bytes()));
        assert_eq!(m.header("dedup"), Some(format!("d{}", i % 200).as_bytes()));
        assert!(m.timestamp >= before - 1000, "CreateTime");
    }
    // per partition, in the order sent
    for p in &parts {
        let offs: Vec<i64> = read.iter().filter(|r| r.0 == *p).map(|r| r.1.offset).collect();
        assert!(offs.windows(2).all(|w| w[1] == w[0] + 1), "partition {p}");
    }
    let total: i64 = parts.iter().map(|p| client.watermarks(&topic, *p, true, T).unwrap()).map(|(l, h)| h - l).sum();
    assert_eq!(total, 1000);
    let at = client
        .list_offsets(&[(topic.clone(), 0, before - 60_000), (topic.clone(), 1, i64::MAX / 2)], false, T)
        .unwrap();
    assert_eq!(at[0].2, Ok(Some(0)), "the first record at or after a time before them all");
    assert_eq!(at[1].2, Ok(None), "none that recent");
    let configs = client.describe_topic_configs(&[&topic], T).unwrap();
    assert!(configs[0].as_ref().unwrap().contains_key("retention.ms"));
    client.commit("it-group", &[(topic.clone(), 0, 5), (topic.clone(), 1, 7)], T).unwrap();
    assert!(client.cluster_id(T).is_some());
    let md = client.metadata(Some(&["it-no-such-topic"]), T).unwrap();
    assert_eq!(md.topic("it-no-such-topic").unwrap().error, Code::UNKNOWN_TOPIC_OR_PARTITION);
    assert!(client.metadata(None, T).unwrap().topic("it-no-such-topic").is_none(), "asking created nothing");
}

#[test]
fn redpanda_round_trip() {
    let Some(addr) = brokers("FASTKA_IT") else { return eprintln!("FASTKA_IT unset: skipped") };
    round_trip(&addr);
}

#[test]
fn apache_kafka_round_trip() {
    let Some(addr) = brokers("FASTKA_IT_APACHE") else {
        return eprintln!("FASTKA_IT_APACHE unset: skipped");
    };
    round_trip(&addr);
}

/// What rpk (franz-go) writes in each codec, this client reads; what this client writes, rpk reads
/// with the same values, headers and offsets; and the group offsets committed here are rpk's.
#[test]
fn interop_with_another_client() {
    let (Some(addr), Some(ctr)) = (brokers("FASTKA_IT"), brokers("FASTKA_IT_RPK")) else {
        return eprintln!("FASTKA_IT or FASTKA_IT_RPK unset: skipped");
    };
    let c = cfg(&addr);
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let topic = unique(&format!("rpk-{codec}"));
        rpk(&ctr, &["topic", "create", &topic, "-p", "1"], None);
        let input: String = (0..300).map(|i| format!("{codec} value {i}\n")).collect();
        rpk(
            &ctr,
            &["topic", "produce", &topic, "-z", codec, "-H", "h:v", "--max-message-bytes", "1000000"],
            Some(&input),
        );
        let read = consume_all(&c, &topic, &[0]);
        let values: Vec<String> =
            read.iter().map(|(_, m)| String::from_utf8(m.value.clone().unwrap()).unwrap()).collect();
        assert_eq!(values, (0..300).map(|i| format!("{codec} value {i}")).collect::<Vec<_>>(), "{codec}");
        assert_eq!(read[0].1.header("h"), Some(&b"v"[..]));

        let mine = unique(&format!("mine-{codec}"));
        rpk(&ctr, &["topic", "create", &mine, "-p", "1"], None);
        let mut pc = c.clone();
        pc.set("compression.type", codec);
        let values: Vec<String> = (0..100).map(|i| format!("mine {codec} {i}")).collect();
        produce_all(&pc, &mine, &values);
        let out = rpk(&ctr, &["topic", "consume", &mine, "-n", "100", "-f", "%o %v %h{%k=%v}\n"], None);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 100, "{out}");
        for (i, l) in lines.iter().enumerate() {
            assert_eq!(*l, format!("{i} mine {codec} {i} dedup=d{i}"), "{codec}");
        }
    }
    let topic = unique("group");
    rpk(&ctr, &["topic", "create", &topic, "-p", "2"], None);
    let client = Client::new(&c).unwrap();
    client.commit("it-lag", &[(topic.clone(), 0, 3), (topic.clone(), 1, 4)], T).unwrap();
    let described = rpk(&ctr, &["group", "describe", "it-lag"], None);
    assert!(described.contains(&topic), "{described}");

    // what rpk commits as a member of a group (franz-go, its own consumer), this client reads back
    let read = unique("rpk-group");
    rpk(&ctr, &["topic", "create", &read, "-p", "1"], None);
    produce_all(&c, &read, &(0..5).map(|i| i.to_string()).collect::<Vec<_>>());
    let group = unique("rpk-g");
    rpk(&ctr, &["topic", "consume", &read, "-g", &group, "-n", "3"], None);
    assert_eq!(client.committed(&group, &[(read.clone(), 0)], T).unwrap(), [(read.clone(), 0, Some(3))]);
}

/// Why: the archiver commits each partition's offset after its upload and reads them back when it
/// starts; an offset read back that is not the one committed re-archives or skips records. Against
/// each broker: what this client commits it reads back, a later commit over an earlier, and what
/// was never committed (a partition, a group, a topic the cluster has not) is `None`.
fn committed_round_trip(c: &Config) {
    let client = Client::new(c).unwrap();
    let topic = unique("committed");
    assert_eq!(client.create_topics(&[(topic.clone(), 3, -1)], T).unwrap()[0].1, Ok(()));
    let group = unique("g");
    let asked: Vec<(String, i32)> = (0..3).map(|p| (topic.clone(), p)).collect();
    assert!(client.committed(&group, &asked, T).unwrap().iter().all(|g| g.2.is_none()), "before any commit");
    client.commit(&group, &[(topic.clone(), 0, 5), (topic.clone(), 2, 0)], T).unwrap();
    client.commit(&group, &[(topic.clone(), 0, 9)], T).unwrap();
    assert_eq!(
        client.committed(&group, &asked, T).unwrap(),
        [(topic.clone(), 0, Some(9)), (topic.clone(), 1, None), (topic.clone(), 2, Some(0))]
    );
    let missing = unique("no-such-topic");
    assert_eq!(client.committed(&group, &[(missing.clone(), 0)], T).unwrap(), [(missing, 0, None)]);
}

#[test]
fn redpanda_committed_round_trip() {
    let Some(addr) = brokers("FASTKA_IT") else { return eprintln!("FASTKA_IT unset: skipped") };
    committed_round_trip(&cfg(&addr));
}

#[test]
fn apache_kafka_committed_round_trip() {
    let Some(addr) = brokers("FASTKA_IT_APACHE") else { return eprintln!("FASTKA_IT_APACHE unset: skipped") };
    committed_round_trip(&cfg(&addr));
}

/// Redpanda 26.2, the version the archivers' clusters run, here with SASL/SCRAM.
#[test]
fn redpanda_26_committed_round_trip() {
    let Some(addr) = brokers("FASTKA_IT_SASL") else { return eprintln!("FASTKA_IT_SASL unset: skipped") };
    let mut c = cfg(&addr);
    c.set("security.protocol", "sasl_plaintext").set("sasl.mechanisms", "SCRAM-SHA-256");
    c.set("sasl.username", "it").set("sasl.password", "it-secret");
    committed_round_trip(&c);
}

/// SASL/SCRAM against a broker that requires it: each mechanism with the right password, and a
/// wrong password refused as an authentication error, not retried.
#[test]
fn sasl_scram() {
    let Some(addr) = brokers("FASTKA_IT_SASL") else { return eprintln!("FASTKA_IT_SASL unset: skipped") };
    for (mech, user) in [("SCRAM-SHA-256", "it"), ("SCRAM-SHA-512", "it512")] {
        let mut c = cfg(&addr);
        c.set("security.protocol", "sasl_plaintext").set("sasl.mechanisms", mech);
        c.set("sasl.username", user).set("sasl.password", "it-secret");
        let client = Client::new(&c).unwrap();
        let topic = unique("sasl");
        assert_eq!(client.create_topics(&[(topic.clone(), 1, -1)], T).unwrap()[0].1, Ok(()));
        let h = produce_all(&c, &topic, &["secret value".to_string()]);
        assert!(h[0].2.is_none(), "{h:?}");
        assert_eq!(consume_all(&c, &topic, &[0])[0].1.value.as_deref(), Some(&b"secret value"[..]));
        c.set("sasl.password", "wrong");
        let t = Instant::now();
        let e = Client::new(&c).unwrap().metadata(None, T).unwrap_err();
        assert!(matches!(e, Error::Auth(_)), "{mech}: {e}");
        assert!(t.elapsed() < Duration::from_secs(5));
    }
    let c = cfg(&addr);
    let e = Client::new(&c).unwrap().metadata(None, Duration::from_secs(3)).unwrap_err();
    assert!(!matches!(e, Error::Protocol(_)), "a broker requiring SASL, asked without: {e}");
}

/// A frame of a Kafka connection: its bytes after the size; None at its end.
fn frame(s: &mut TcpStream) -> Option<Vec<u8>> {
    let mut size = [0; 4];
    s.read_exact(&mut size).ok()?;
    let mut f = vec![0; i32::from_be_bytes(size) as usize];
    s.read_exact(&mut f).ok()?;
    Some(f)
}

/// A proxy to `upstream` that the client reaches the cluster through (each Metadata answer it
/// passes names the proxy for every broker). The first Produce request holding `marker` goes to
/// the broker, which writes it, and its answer is lost: the connection closed instead.
struct LoseOne {
    addr: String,
    lost: Arc<AtomicBool>,
    produces: Arc<AtomicUsize>,
}

fn lose_one_answer(upstream: &str, marker: &[u8]) -> LoseOne {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let (addr, port) = (l.local_addr().unwrap().to_string(), i32::from(l.local_addr().unwrap().port()));
    let (lost, produces) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)));
    let (up, marker, lost2, produces2) = (upstream.to_string(), marker.to_vec(), lost.clone(), produces.clone());
    std::thread::spawn(move || {
        for client in l.incoming().flatten() {
            let Ok(server) = TcpStream::connect(&up) else { continue };
            // each request's (api, version, its answer to lose), by correlation id
            let asked: Arc<Mutex<HashMap<i32, (i16, i16, bool)>>> = Arc::default();
            let (mut from, mut to) = (client.try_clone().unwrap(), server.try_clone().unwrap());
            let (a, marker, lost, produces) = (asked.clone(), marker.clone(), lost2.clone(), produces2.clone());
            std::thread::spawn(move || {
                while let Some(f) = frame(&mut from) {
                    let api = i16::from_be_bytes([f[0], f[1]]);
                    let (version, corr) =
                        (i16::from_be_bytes([f[2], f[3]]), i32::from_be_bytes([f[4], f[5], f[6], f[7]]));
                    let lose = api == 0 && f.windows(marker.len()).any(|w| w == marker) && !lost.swap(true, SeqCst);
                    if api == 0 {
                        produces.fetch_add(1, SeqCst);
                    }
                    a.lock().unwrap().insert(corr, (api, version, lose));
                    if to.write_all(&(f.len() as i32).to_be_bytes()).and_then(|()| to.write_all(&f)).is_err() {
                        return;
                    }
                }
                let _ = to.shutdown(Shutdown::Both);
            });
            let (mut from, mut to) = (server, client);
            std::thread::spawn(move || {
                while let Some(f) = frame(&mut from) {
                    let corr = i32::from_be_bytes([f[0], f[1], f[2], f[3]]);
                    let Some((api, version, lose)) = asked.lock().unwrap().remove(&corr) else { break };
                    if lose {
                        break; // written by the broker, the answer lost
                    }
                    let f = if api == 3 { through(&f, version, port) } else { f };
                    if to.write_all(&(f.len() as i32).to_be_bytes()).and_then(|()| to.write_all(&f)).is_err() {
                        break;
                    }
                }
                let _ = to.shutdown(Shutdown::Both);
                let _ = from.shutdown(Shutdown::Both);
            });
        }
    });
    LoseOne { addr, lost, produces }
}

/// A Metadata answer (header v0) naming the proxy at `port` for every broker.
fn through(f: &[u8], version: i16, port: i32) -> Vec<u8> {
    use kafka_protocol::messages::MetadataResponse;
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
    let mut b = bytes::Bytes::copy_from_slice(&f[4..]);
    let mut r = MetadataResponse::decode(&mut b, version).unwrap();
    for broker in &mut r.brokers {
        (broker.host, broker.port) = (StrBytes::from_static_str("127.0.0.1"), port);
    }
    let mut out = bytes::BytesMut::from(&f[..4]);
    r.encode(&mut out, version).unwrap();
    out.to_vec()
}

/// librdkafka's duplicate (live-ingestion's staging, 2026-11-12; PR 322), against a broker: a
/// batch written, its answer lost, and another message timing out before the batch's retry. On
/// that timeout librdkafka bumps its epoch and numbers the retry from 0, and the broker writes it
/// again as new. This producer's retry keeps the batch's sequence: the broker writes it once, and
/// each message is reported at the offset it is at.
fn a_lost_answer_is_written_once(addr: &str) {
    let topic = unique("lost");
    assert_eq!(Client::new(&cfg(addr)).unwrap().create_topics(&[(topic.clone(), 1, -1)], T).unwrap()[0].1, Ok(()));
    let proxy = lose_one_answer(addr, b"a-lost");
    let mut c = cfg(&proxy.addr);
    c.set("compression.type", "none").set("message.timeout.ms", "4000");
    c.set("retry.backoff.ms", "2500").set("retry.backoff.max.ms", "2500");
    let p = Producer::new(&c, Heard::default()).unwrap();
    let send = |v: &str, t: &str, o: u64| p.send(Record::to(t).value(v.as_bytes()), o).unwrap();
    let heard = |n: usize| {
        let until = Instant::now() + T;
        while p.context().0.lock().unwrap().len() < n {
            assert!(Instant::now() < until, "{:?}", p.context().0.lock().unwrap());
            p.poll(Duration::from_millis(20));
        }
        p.context().0.lock().unwrap().clone()
    };
    send("w", &topic, 0); // the partition's leader known, and the producer id
    heard(1);
    // the cluster has no such topic: it times out at 4 s, in its queue
    send("c", &unique("missing"), 9);
    std::thread::sleep(Duration::from_millis(2000));
    // written, its answer lost; sent again 2.5 s later (4.5 s), after "c" timed out and before
    // its own timeout (6 s)
    send("a-lost", &topic, 1);
    std::thread::sleep(Duration::from_millis(200));
    send("b", &topic, 2); // queued behind it, its batch in flight
    let h = heard(4);
    let at = |o: u64| h.iter().position(|x| x.3 == o).unwrap();
    assert!(proxy.lost.load(SeqCst), "an answer lost");
    assert!(h[at(9)].2.is_some() && at(9) < at(1), "the timeout before the retry's answer: {h:?}");
    assert!(proxy.produces.load(SeqCst) >= 4, "w, a, a again, b: {}", proxy.produces.load(SeqCst));
    assert!([0, 1, 2].iter().all(|o| h[at(*o)].2.is_none()), "{h:?}");
    assert!(p.fatal_error().is_none());
    let read = consume_all(&cfg(addr), &topic, &[0]);
    let values: Vec<&[u8]> = read.iter().map(|(_, m)| m.value.as_deref().unwrap()).collect();
    assert_eq!(values, [&b"w"[..], b"a-lost", b"b"], "once each");
    // a duplicate's answer may say its offset or not (DUPLICATE_SEQUENCE_NUMBER)
    assert!(matches!(h[at(1)].1, None | Some(1)) && h[at(2)].1 == Some(2), "{h:?}");
}

#[test]
fn redpanda_lost_answer() {
    let Some(addr) = brokers("FASTKA_IT") else { return eprintln!("FASTKA_IT unset: skipped") };
    a_lost_answer_is_written_once(&addr);
}

#[test]
fn apache_kafka_lost_answer() {
    let Some(addr) = brokers("FASTKA_IT_APACHE") else {
        return eprintln!("FASTKA_IT_APACHE unset: skipped");
    };
    a_lost_answer_is_written_once(&addr);
}

/// The (producer id, base sequence) of every batch of `topic`/`partition` on the broker's log,
/// read with one raw Fetch (a one-broker cluster: it leads every partition).
fn batch_ids(addr: &str, topic: &str, partition: i32) -> Vec<(i64, i32)> {
    use fastka::wire;
    let client = Client::new(&cfg(addr)).unwrap();
    let l = wire::FetchLimits {
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 64 << 20,
        isolation: 0,
        partition_max_bytes: 64 << 20,
    };
    let frame = client.call_any(wire::FETCH, &|o| wire::fetch_request(o, &l, &[(topic, (partition, 0))]), T).unwrap();
    let fetched = wire::fetch(wire::Cur::new(&frame)).unwrap();
    let part = fetched.parts.iter().find(|p| p.topic == topic && p.partition == partition).unwrap();
    assert_eq!(part.error, Code::NONE);
    fastka::records::batches(&frame[part.records.clone()])
        .map(|b| b.unwrap())
        // the base sequence: header bytes 53..57, `checked` starting after the CRC (byte 21)
        .map(|b| (b.producer_id, i32::from_be_bytes(b.checked[53 - 21..57 - 21].try_into().unwrap())))
        .collect()
}

/// Without idempotence, against a broker: in every codec and with acks=1 and all, every batch on
/// the log carries no producer id (what a broker's dedup filter that skips idempotent batches
/// needs to see), and each message is read back once, in order, at the offset reported. With
/// idempotence (the default) the same batches carry the id the broker gave.
fn without_idempotence(addr: &str) {
    let client = Client::new(&cfg(addr)).unwrap();
    for (idempotence, acks) in [("false", "1"), ("false", "all"), ("true", "all")] {
        let topic = unique(&format!("plain-{idempotence}-{acks}"));
        assert_eq!(client.create_topics(&[(topic.clone(), 2, -1)], T).unwrap()[0].1, Ok(()));
        let mut sent = vec![];
        for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
            let mut c = cfg(addr);
            c.set("enable.idempotence", idempotence).set("acks", acks);
            c.set("compression.type", codec).set("batch.num.messages", "40");
            let values: Vec<String> = (0..100).map(|i| format!("{codec}-{i:03}")).collect();
            let h = produce_all(&c, &topic, &values);
            assert!(h.iter().all(|x| x.2.is_none() && x.1.is_some()), "{acks}/{codec}: {h:?}");
            sent.extend(values.into_iter().zip(h));
        }
        let read = consume_all(&cfg(addr), &topic, &[0, 1]);
        assert_eq!(read.len(), sent.len(), "{idempotence}/{acks}: each once");
        for (v, (p, o, _, _)) in &sent {
            let (_, m) = read.iter().find(|(rp, m)| Some(*rp) == *p && Some(m.offset) == *o).expect("at its offset");
            assert_eq!(m.value.as_deref(), Some(v.as_bytes()), "{idempotence}/{acks}");
        }
        for p in [0, 1] {
            let ids = batch_ids(addr, &topic, p);
            assert!(!ids.is_empty(), "{idempotence}/{acks}: partition {p} empty");
            if idempotence == "false" {
                assert!(ids.iter().all(|i| *i == (-1, -1)), "{acks}: partition {p}: {ids:?}");
            } else {
                assert!(ids.iter().all(|i| i.0 >= 0 && i.1 >= 0), "idempotent: partition {p}: {ids:?}");
            }
        }
    }
}

#[test]
fn redpanda_without_idempotence() {
    let Some(addr) = brokers("FASTKA_IT") else { return eprintln!("FASTKA_IT unset: skipped") };
    without_idempotence(&addr);
}

#[test]
fn apache_kafka_without_idempotence() {
    let Some(addr) = brokers("FASTKA_IT_APACHE") else {
        return eprintln!("FASTKA_IT_APACHE unset: skipped");
    };
    without_idempotence(&addr);
}

/// A topic created with configs has them: what the broker describes of it.
fn created_with_configs(addr: &str) {
    let client = Client::new(&cfg(addr)).unwrap();
    let topic = unique("compacted");
    let configs: &[(&str, &str)] = &[("cleanup.policy", "compact"), ("retention.ms", "3600000")];
    let made = client.create_topics_with(&[(topic.clone(), 1, -1, configs)], T).unwrap();
    assert_eq!(made[0].1, Ok(()));
    let got = client.describe_topic_configs(&[&topic], T).unwrap().pop().unwrap().unwrap();
    assert_eq!(got.get("cleanup.policy"), Some(&Some("compact".to_string())));
    assert_eq!(got.get("retention.ms"), Some(&Some("3600000".to_string())));
    // a config the broker does not take is the topic's own error
    let bad: &[(&str, &str)] = &[("cleanup.policy", "shred")];
    let made = client.create_topics_with(&[(unique("bad"), 1, -1, bad)], T).unwrap();
    assert!(made[0].1.is_err(), "{made:?}");
}

#[test]
fn redpanda_created_with_configs() {
    let Some(addr) = brokers("FASTKA_IT") else { return eprintln!("FASTKA_IT unset: skipped") };
    created_with_configs(&addr);
}

#[test]
fn apache_kafka_created_with_configs() {
    let Some(addr) = brokers("FASTKA_IT_APACHE") else {
        return eprintln!("FASTKA_IT_APACHE unset: skipped");
    };
    created_with_configs(&addr);
}
