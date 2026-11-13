#![allow(clippy::type_complexity)]
//! Acceptance: fastka end to end against real brokers, each scenario the way an application meets
//! it, checked against a reference rather than against fastka itself where one exists: Apache
//! Kafka's own Java client (`tests/java/Oracle.java`), the broker's log, a 3-node cluster losing a
//! leader. Each group runs when its environment is set, and is skipped otherwise:
//!
//! - `FASTKA_IT_APACHE=host:port` and `FASTKA_IT_JAVA=<dir>` (`scripts/java-oracle.sh`): the Java
//!   client's partitioner, its reads of fastka's writes and fastka's of its own, transactions it
//!   commits, aborts and holds open, quotas, group offsets.
//! - `FASTKA_IT=host:port` (Redpanda): offsets and resets, sizes at the broker's limit, many
//!   partitions and topics, pause, partitions added.
//! - `FASTKA_IT_CLUSTER=h:p,h:p,h:p` and `FASTKA_IT_CLUSTER_NODES=c0,c1,c2` (`scripts/cluster.sh`,
//!   containers by node id): producing and consuming through a leader stopped and started again.
use fastka::client::Client;
use fastka::consumer::{Assignment, Consumer, Message, Notice, Offset, Polled};
use fastka::producer::{Deliveries, Delivery, Persisted, Producer, Record};
use fastka::{Code, Config, Error};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

const T: Duration = Duration::from_secs(30);

fn env(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|v| !v.is_empty())
}

fn cfg(addr: &str) -> Config {
    let mut c = Config::new();
    c.set("bootstrap.servers", addr).set("enable.partition.eof", "true").set("fetch.wait.max.ms", "50");
    c.set("linger.ms", "2");
    c
}

fn unique(name: &str) -> String {
    let n = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
    format!("acc-{name}-{n}")
}

fn create(c: &Config, topic: &str, partitions: i32, replicas: i16) {
    let client = Client::new(c).unwrap();
    for (t, r) in client.create_topics(&[(topic.to_string(), partitions, replicas)], T).unwrap() {
        r.unwrap_or_else(|e| panic!("creating {t}: {e}"));
    }
}

/// One record's outcome: (partition, offset, error, persisted).
type Outcome = (Option<i32>, Option<i64>, Option<String>, Persisted);

#[derive(Default)]
struct Heard(Mutex<HashMap<u64, Outcome>>);
impl Deliveries for Heard {
    type Opaque = u64;
    fn delivery(&self, d: &Delivery<'_>, o: u64) {
        let prev =
            self.0.lock().unwrap().insert(o, (d.partition, d.offset, d.error.map(Error::to_string), d.persisted));
        assert!(prev.is_none(), "record {o}: a second report");
    }
}

/// Every record of `parts` from their start until each says its end (`limit`), notices kept.
fn read_all(c: &Config, topic: &str, parts: i32, limit: Duration) -> (Vec<(i32, Message)>, Vec<String>) {
    let assign = (0..parts).map(|p| Assignment::new(topic, p, Offset::Beginning)).collect();
    let mut k = Consumer::from_config(c, assign).unwrap();
    let (mut out, mut ends, mut notices) = (vec![], HashSet::new(), vec![]);
    let until = Instant::now() + limit;
    while ends.len() < parts as usize {
        assert!(Instant::now() < until, "{topic}: {} of {parts} partitions at their end", ends.len());
        match k.next_event(Duration::from_millis(100), &mut |n| notices.push(n.to_string())) {
            Ok(Some((i, Polled::Msg(m)))) => {
                ends.remove(&i);
                out.push((i as i32, m))
            }
            Ok(Some((i, Polled::Eof))) => drop(ends.insert(i)),
            Ok(_) => {}
            Err(e) => notices.push(format!("error: {e}")),
        }
    }
    (out, notices)
}

fn java(args: &[&str]) -> Vec<Value> {
    let dir = env("FASTKA_IT_JAVA").expect("FASTKA_IT_JAVA");
    let out = Command::new("java")
        .args(["-cp", &format!("{dir}:{dir}/libs/*"), "Oracle"])
        .args(args)
        .stderr(Stdio::inherit())
        .output()
        .unwrap();
    assert!(out.status.success(), "Oracle {args:?} failed");
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .filter(|l| l.starts_with('{'))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn unhex(v: &Value) -> Option<Vec<u8>> {
    let s = v.as_str()?;
    Some((0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect())
}

fn apache() -> Option<String> {
    let a = env("FASTKA_IT_APACHE")?;
    env("FASTKA_IT_JAVA").map(|_| a)
}

// ---- against Apache Kafka's own Java client ------------------------------------------------

/// A keyed record lands on the partition the Java client puts the same key on (murmur2, positive,
/// modulo the partitions): an application moving from the Java client keeps each key's partition,
/// so its order, and its consumers' assignment.
#[test]
fn keys_land_where_the_java_client_puts_them() {
    let Some(addr) = apache() else { return };
    let c = cfg(&addr);
    let (theirs, ours) = (unique("keys-java"), unique("keys-fastka"));
    create(&c, &theirs, 7, 1);
    create(&c, &ours, 7, 1);
    java(&["produce", &addr, &theirs, "none", "500"]);
    let p = Producer::new(&c, Heard::default()).unwrap();
    for i in 0..500u64 {
        let k = format!("k{}", i % 50);
        p.send(Record::to(&ours).key(k.as_bytes()).value(b"v"), i).unwrap();
    }
    p.flush(T).unwrap();
    let key_of = |m: &Message| String::from_utf8(m.key.clone().unwrap()).unwrap();
    let java_at: HashMap<String, i32> = read_all(&c, &theirs, 7, T).0.iter().map(|(p, m)| (key_of(m), *p)).collect();
    let ours_at: HashMap<String, i32> = read_all(&c, &ours, 7, T).0.iter().map(|(p, m)| (key_of(m), *p)).collect();
    assert_eq!(java_at.len(), 50);
    assert_eq!(ours_at, java_at, "each key on the Java client's partition");
    assert!(java_at.values().collect::<HashSet<_>>().len() > 3, "the keys spread");
}

/// The Java client reads what fastka writes, in every codec, exactly: keys, values, a tombstone,
/// an empty value, headers (one with no value), timestamps, the partition chosen, at the offsets
/// fastka reported.
#[test]
fn the_java_client_reads_what_fastka_writes() {
    let Some(addr) = apache() else { return };
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let mut c = cfg(&addr);
        c.set("compression.type", codec);
        let topic = unique(&format!("w-{codec}"));
        create(&c, &topic, 3, 1);
        let p = Producer::new(&c, Heard::default()).unwrap();
        let mut sent = vec![];
        for i in 0..300u64 {
            let key = format!("key-{}", i % 17);
            let value = match i % 10 {
                9 => None,
                8 => Some(vec![]),
                _ => Some(format!("{codec}-{i}-{}", "y".repeat(i as usize % 23)).into_bytes()),
            };
            let headers = vec![("i".to_string(), Some(i.to_string().into_bytes())), ("nil".to_string(), None)];
            let ts = 1_700_000_000_000 + i as i64;
            let mut r = Record::to(&topic).key(key.as_bytes()).headers(&headers).timestamp(ts);
            if let Some(v) = &value {
                r = r.value(v);
            }
            if i % 7 == 0 {
                r = r.partition(2);
            }
            p.send(r, i).unwrap();
            sent.push((key, value, headers, ts, i % 7 == 0));
        }
        p.flush(T).unwrap();
        let heard = p.context().0.lock().unwrap().clone();
        let read = java(&["consume", &addr, &topic, "3", "300", "read_uncommitted"]);
        assert_eq!(read.len(), 300, "{codec}");
        let by_at: HashMap<(i64, i64), &Value> =
            read.iter().map(|r| ((r["partition"].as_i64().unwrap(), r["offset"].as_i64().unwrap()), r)).collect();
        for (i, (key, value, headers, ts, pinned)) in sent.iter().enumerate() {
            let (part, off, err, persisted) = &heard[&(i as u64)];
            assert!(err.is_none() && *persisted == Persisted::Yes, "{codec} #{i}: {err:?}");
            if *pinned {
                assert_eq!(*part, Some(2), "{codec} #{i}: the partition named");
            }
            let r = by_at[&(part.unwrap() as i64, off.unwrap())];
            assert_eq!(unhex(&r["key"]).as_deref(), Some(key.as_bytes()), "{codec} #{i}");
            assert_eq!(unhex(&r["value"]), *value, "{codec} #{i}: value (None: a tombstone)");
            assert_eq!(r["timestamp"].as_i64(), Some(*ts), "{codec} #{i}");
            let hs: Vec<(String, Option<Vec<u8>>)> = r["headers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| (h[0].as_str().unwrap().to_string(), unhex(&h[1])))
                .collect();
            assert_eq!(&hs, headers, "{codec} #{i}: headers, in order");
        }
    }
}

/// fastka reads what the Java client writes, in every codec: the same records at the offsets
/// the Java client was told.
#[test]
fn fastka_reads_what_the_java_client_writes() {
    let Some(addr) = apache() else { return };
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let c = cfg(&addr);
        let topic = unique(&format!("r-{codec}"));
        create(&c, &topic, 3, 1);
        let told = java(&["produce", &addr, &topic, codec, "400"]);
        let (read, _) = read_all(&c, &topic, 3, T);
        assert_eq!(read.len(), 400, "{codec}");
        let at: HashMap<(i32, i64), &Message> = read.iter().map(|(p, m)| ((*p, m.offset), m)).collect();
        for t in &told {
            let i = t["i"].as_i64().unwrap();
            let m = at[&(t["partition"].as_i64().unwrap() as i32, t["offset"].as_i64().unwrap())];
            assert_eq!(m.key.as_deref(), Some(format!("k{}", i % 50).as_bytes()), "{codec} #{i}");
            let want = (i % 10 != 9).then(|| format!("v{i}-{}", "x".repeat(i as usize % 31)).into_bytes());
            assert_eq!(m.value, want, "{codec} #{i}");
            assert_eq!(m.timestamp, 1_700_000_000_000 + i);
            assert_eq!(m.header("i"), Some(i.to_string().as_bytes()));
            assert!(m.headers.iter().any(|(k, v)| k == "nil" && v.is_none()), "a header with no value");
        }
    }
}

/// Transactions the Java client commits are read, those it aborts are not, reading committed;
/// reading uncommitted gives both; neither gives a control record; the partition's end is reached
/// past the markers. The records and offsets are exactly the Java consumer's own.
#[test]
fn read_committed_is_the_java_consumers() {
    let Some(addr) = apache() else { return };
    let c = cfg(&addr);
    let topic = unique("txn");
    create(&c, &topic, 2, 1);
    java(&["produce-txn", &addr, &topic, "12", "25"]);
    for (isolation, want) in [("read_committed", 8 * 25), ("read_uncommitted", 12 * 25)] {
        let mut ci = c.clone();
        ci.set("isolation.level", isolation);
        let (read, _) = read_all(&ci, &topic, 2, T);
        let theirs = java(&["consume", &addr, &topic, "2", &want.to_string(), isolation]);
        let mut ours: Vec<(i64, i64, Vec<u8>)> =
            read.iter().map(|(p, m)| (*p as i64, m.offset, m.value.clone().unwrap())).collect();
        let mut java_read: Vec<(i64, i64, Vec<u8>)> = theirs
            .iter()
            .map(|r| (r["partition"].as_i64().unwrap(), r["offset"].as_i64().unwrap(), unhex(&r["value"]).unwrap()))
            .collect();
        ours.sort();
        java_read.sort();
        assert_eq!(ours.len(), want, "{isolation}");
        assert_eq!(ours, java_read, "{isolation}: the Java consumer's records at its offsets");
        if isolation == "read_committed" {
            assert!(ours.iter().all(|r| r.2.starts_with(b"c")), "nothing aborted");
        }
    }
}

/// An open transaction holds a read_committed consumer at its first offset (the last stable
/// offset): its records are not given and the partition's end is reached before them; once it
/// commits, they are.
#[test]
fn an_open_transaction_holds_read_committed_at_its_start() {
    let Some(addr) = apache() else { return };
    let c = cfg(&addr);
    let topic = unique("open");
    create(&c, &topic, 1, 1);
    let dir = env("FASTKA_IT_JAVA").unwrap();
    let mut held = Command::new("java")
        .args(["-cp", &format!("{dir}:{dir}/libs/*"), "Oracle", "produce-open", &addr, &topic, "20", "6000"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // its records are on the log, its transaction open
    let until = Instant::now() + T;
    loop {
        let (_, high) = Client::new(&c).unwrap().watermarks(&topic, 0, false, T).unwrap();
        if high >= 20 {
            break;
        }
        assert!(Instant::now() < until, "the open transaction's records");
        std::thread::sleep(Duration::from_millis(100));
    }
    let (read, _) = read_all(&c, &topic, 1, T);
    assert!(read.is_empty(), "nothing of an open transaction: {} records", read.len());
    assert!(held.wait().unwrap().success());
    let (read, _) = read_all(&c, &topic, 1, T);
    assert_eq!(read.len(), 20, "committed: given");
}

/// A broker's quota throttles fastka (KIP-219, the client waiting out what the broker asks) and
/// nothing fails: every record is delivered, and read, slower. `client.id` is what the quota
/// names.
#[test]
fn a_quota_throttles_and_fails_nothing() {
    let Some(addr) = apache() else { return };
    let id = format!("fastka-throttled-{}", std::process::id());
    java(&["quota", &addr, &id, "100000"]);
    std::thread::sleep(Duration::from_secs(2)); // the quota reaching the broker's cache
    let mut c = cfg(&addr);
    c.set("client.id", &id).set("batch.num.messages", "100");
    let topic = unique("quota");
    create(&c, &topic, 1, 1);
    let p = Producer::new(&c, Heard::default()).unwrap();
    let v = vec![b'q'; 1000];
    let t0 = Instant::now();
    // 1.6 MB at 100 kB/s: Kafka measures a client's rate over its quota window (11 s), so the
    // burst must outrun the quota over that span; what is past it is held (the broker mutes the
    // connection and asks the client to wait). Kafka's own Java client takes ~13 s here
    for i in 0..1600u64 {
        p.send(Record::to(&topic).value(&v), i).unwrap();
    }
    p.flush(Duration::from_secs(90)).unwrap();
    let took = t0.elapsed();
    let heard = p.context().0.lock().unwrap().clone();
    assert!(heard.values().all(|h| h.2.is_none()), "{:?}", heard.values().find(|h| h.2.is_some()));
    assert!(took > Duration::from_secs(4), "1.6 MB at 100 kB/s took {took:?}: not throttled");
    let t0 = Instant::now();
    let (read, _) = read_all(&c, &topic, 1, Duration::from_secs(90));
    assert_eq!(read.len(), 1600);
    assert!(t0.elapsed() > Duration::from_secs(4), "read in {:?}: not throttled", t0.elapsed());
}

/// Offsets fastka commits for a group are the group's offsets as the Java client's admin sees
/// them.
#[test]
fn committed_offsets_are_the_groups() {
    let Some(addr) = apache() else { return };
    let c = cfg(&addr);
    let topic = unique("group");
    create(&c, &topic, 3, 1);
    let group = unique("g");
    Client::new(&c).unwrap().commit(&group, &[(topic.clone(), 0, 11), (topic.clone(), 2, 42)], T).unwrap();
    let got: Vec<(String, i64)> = java(&["group-offsets", &addr, &group])
        .iter()
        .map(|r| (r["tp"].as_str().unwrap().to_string(), r["offset"].as_i64().unwrap()))
        .collect();
    assert_eq!(got, [(format!("{topic}/0"), 11), (format!("{topic}/2"), 42)]);
}

// ---- against Redpanda --------------------------------------------------------------------

fn redpanda() -> Option<String> {
    env("FASTKA_IT")
}

fn produce_values(c: &Config, topic: &str, partition: i32, values: &[String]) {
    let p = Producer::new(c, Heard::default()).unwrap();
    for (i, v) in values.iter().enumerate() {
        p.send(Record::to(topic).partition(partition).value(v.as_bytes()), i as u64).unwrap();
    }
    p.flush(T).unwrap();
    assert!(p.context().0.lock().unwrap().values().all(|h| h.2.is_none()));
}

/// Where a partition is read from: its end (only what comes after), an offset, an offset past its
/// end under each `auto.offset.reset` (earliest and latest move on and say so; error refuses, and
/// `seek` mends it).
#[test]
fn where_a_partition_is_read_from() {
    let Some(addr) = redpanda() else { return };
    let c = cfg(&addr);
    let topic = unique("offsets");
    create(&c, &topic, 1, 1);
    produce_values(&c, &topic, 0, &(0..10).map(|i| format!("old{i}")).collect::<Vec<_>>());
    let next = |k: &mut Consumer, notices: &mut Vec<Notice>| loop {
        match k.next_event(T, &mut |n| notices.push(n)).unwrap() {
            Some((_, Polled::Msg(m))) => return m,
            Some(_) => {}
            None => panic!("nothing in {T:?}"),
        }
    };
    // the end: only what comes after
    let mut at_end = Consumer::from_config(&c, vec![Assignment::new(&topic, 0, Offset::End)]).unwrap();
    while !matches!(at_end.next_event(T, &mut |_| {}).unwrap(), Some((_, Polled::Eof))) {}
    produce_values(&c, &topic, 0, &["new0".to_string()]);
    let m = next(&mut at_end, &mut vec![]);
    assert_eq!((m.offset, m.value.as_deref()), (10, Some(&b"new0"[..])));
    // an offset
    let mut at = Consumer::from_config(&c, vec![Assignment::new(&topic, 0, Offset::At(7))]).unwrap();
    assert_eq!(next(&mut at, &mut vec![]).offset, 7);
    // past the end
    for (reset, want) in [("earliest", Some(0)), ("latest", None)] {
        let mut cr = c.clone();
        cr.set("auto.offset.reset", reset);
        let mut k = Consumer::from_config(&cr, vec![Assignment::new(&topic, 0, Offset::At(1000))]).unwrap();
        let mut notices = vec![];
        match want {
            Some(o) => assert_eq!(next(&mut k, &mut notices).offset, o, "{reset}"),
            None => {
                while !matches!(k.next_event(T, &mut |n| notices.push(n)).unwrap(), Some((_, Polled::Eof))) {}
                assert_eq!(k.position(0), Some(11), "latest: at the end");
            }
        }
        assert!(notices.iter().any(|n| matches!(n, Notice::Reset { from: 1000, .. })), "{reset}: said: {notices:?}");
    }
    let mut ce = c.clone();
    ce.set("auto.offset.reset", "error");
    let mut k = Consumer::from_config(&ce, vec![Assignment::new(&topic, 0, Offset::At(1000))]).unwrap();
    let until = Instant::now() + T;
    let e = loop {
        assert!(Instant::now() < until, "an offset out of range refused");
        if let Err(e) = k.next_event(Duration::from_millis(100), &mut |_| {}) {
            break e;
        }
    };
    assert_eq!(e.code(), Some(Code::OFFSET_OUT_OF_RANGE), "{e}");
    k.seek(0, Offset::At(3)).unwrap();
    assert_eq!(next(&mut k, &mut vec![]).offset, 3, "seek mends it");
}

/// Records at the broker's size limit: one just under it goes both ways; one over it fails alone
/// (MESSAGE_TOO_LARGE, not persisted) and the producer goes on.
#[test]
fn records_at_the_brokers_size_limit() {
    let Some(addr) = redpanda() else { return };
    let mut c = cfg(&addr);
    c.set("message.max.bytes", "4000000").set("compression.type", "none");
    let topic = unique("sizes");
    create(&c, &topic, 1, 1);
    let p = Producer::new(&c, Heard::default()).unwrap();
    let (big, too_big) = (vec![b'b'; 1_000_000 - 200], vec![b't'; 2_000_000]);
    p.send(Record::to(&topic).value(&big), 0).unwrap();
    p.flush(T).unwrap();
    p.send(Record::to(&topic).value(&too_big), 1).unwrap();
    p.flush(T).unwrap();
    p.send(Record::to(&topic).value(b"after"), 2).unwrap();
    p.flush(T).unwrap();
    let h = p.context().0.lock().unwrap().clone();
    assert!(h[&0].2.is_none(), "just under the limit: {:?}", h[&0]);
    assert!(h[&1].2.as_deref().is_some_and(|e| e.contains("MESSAGE_TOO_LARGE")), "{:?}", h[&1]);
    assert_eq!(h[&1].3, Persisted::No);
    assert!(h[&2].2.is_none() && p.fatal_error().is_none(), "the producer goes on: {:?}", h[&2]);
    let (read, _) = read_all(&cfg(&addr), &topic, 1, T);
    assert_eq!(read.len(), 2);
    assert_eq!(read[0].1.value.as_deref(), Some(&big[..]));
    assert_eq!(read[1].1.value.as_deref(), Some(&b"after"[..]));
}

/// Many partitions of several topics at once: every record read once, each partition in the
/// order written; a paused topic gives nothing until it is resumed, then all of it.
#[test]
fn many_partitions_and_topics_and_a_pause() {
    let Some(addr) = redpanda() else { return };
    let c = cfg(&addr);
    let topics: Vec<String> = (0..3).map(|t| unique(&format!("many{t}"))).collect();
    for t in &topics {
        create(&c, t, 16, 1);
    }
    let p = Producer::new(&c, Heard::default()).unwrap();
    for i in 0..30_000u64 {
        let t = &topics[(i % 3) as usize];
        p.send(Record::to(t).key(format!("k{}", i % 997).as_bytes()).value(i.to_string().as_bytes()), i).unwrap();
    }
    p.flush(T).unwrap();
    let assign: Vec<Assignment> =
        topics.iter().flat_map(|t| (0..16).map(move |p| Assignment::new(t, p, Offset::Beginning))).collect();
    let mut k = Consumer::from_config(&c, assign).unwrap();
    k.pause(|t, _| t == topics[2]);
    let (mut got, mut last) = (HashMap::<String, usize>::new(), HashMap::<usize, i64>::new());
    let mut seen = HashSet::new();
    let read = |k: &mut Consumer,
                n: usize,
                got: &mut HashMap<String, usize>,
                last: &mut HashMap<usize, i64>,
                seen: &mut HashSet<u64>| {
        let until = Instant::now() + T;
        let mut done = 0;
        while done < n {
            assert!(Instant::now() < until, "{done} of {n}");
            if let Some((i, Polled::Msg(m))) = k.next_event(Duration::from_millis(100), &mut |_| {}).unwrap() {
                let t = k.partitions().nth(i).unwrap().0.to_string();
                assert!(m.offset > *last.get(&i).unwrap_or(&-1), "in order");
                last.insert(i, m.offset);
                assert!(
                    seen.insert(std::str::from_utf8(m.value.as_deref().unwrap()).unwrap().parse().unwrap()),
                    "once"
                );
                *got.entry(t).or_default() += 1;
                done += 1;
            }
        }
    };
    read(&mut k, 20_000, &mut got, &mut last, &mut seen);
    let quiet = Instant::now() + Duration::from_millis(500);
    while Instant::now() < quiet {
        assert!(
            !matches!(k.next_event(Duration::from_millis(50), &mut |_| {}).unwrap(), Some((_, Polled::Msg(_)))),
            "paused: nothing"
        );
    }
    assert_eq!(got.get(&topics[2]), None, "the paused topic gave nothing");
    k.resume(|t, _| t == topics[2]);
    read(&mut k, 10_000, &mut got, &mut last, &mut seen);
    assert!(topics.iter().all(|t| got[t] == 10_000), "{got:?}");
}

/// Partitions added to a topic are produced to once the metadata is refreshed
/// (`topic.metadata.refresh.interval.ms`), keyless records reaching them.
#[test]
fn partitions_added_are_produced_to() {
    let (Some(addr), Some(rpk)) = (redpanda(), env("FASTKA_IT_RPK")) else { return };
    let mut c = cfg(&addr);
    c.set("topic.metadata.refresh.interval.ms", "500").set("batch.num.messages", "10");
    let topic = unique("grow");
    create(&c, &topic, 1, 1);
    let p = Producer::new(&c, Heard::default()).unwrap();
    for i in 0..100u64 {
        p.send(Record::to(&topic).value(b"x"), i).unwrap();
    }
    p.flush(T).unwrap();
    let out = Command::new("docker")
        .args(["exec", &rpk, "rpk", "topic", "add-partitions", &topic, "-n", "3"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    std::thread::sleep(Duration::from_secs(2));
    for i in 100..2000u64 {
        p.send(Record::to(&topic).value(b"x"), i).unwrap();
        if i % 100 == 0 {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    p.flush(T).unwrap();
    let parts: HashSet<i32> = p.context().0.lock().unwrap().values().filter_map(|h| h.0).collect();
    assert!(parts.len() >= 3, "the added partitions used: {parts:?}");
}

// ---- a 3-node cluster losing a leader ------------------------------------------------------

fn cluster() -> Option<(String, Vec<String>)> {
    Some((env("FASTKA_IT_CLUSTER")?, env("FASTKA_IT_CLUSTER_NODES")?.split(',').map(str::to_string).collect()))
}

fn docker(args: &[&str]) {
    let out = Command::new("docker").args(args).output().unwrap();
    assert!(out.status.success(), "docker {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

/// Every node started again when the scenario ends, passing or not: a node left stopped would fail
/// the next scenario for a reason of its own.
struct Restart<'a>(&'a [String]);
impl Drop for Restart<'_> {
    fn drop(&mut self) {
        for n in self.0 {
            let _ = Command::new("docker").args(["start", n]).output();
        }
    }
}

/// The node leading partition 0 of `topic`, by its container.
fn leader_of(c: &Config, topic: &str, nodes: &[String]) -> (i32, String) {
    let (leaders, _) = Client::new(c).unwrap().leaders(topic, T).unwrap();
    let id = leaders.iter().find(|(p, _)| *p == 0).unwrap().1;
    (id, nodes[id as usize].clone())
}

/// Keyed records produced at a steady pace while partition 0's leader is stopped, and started
/// again: each record delivered is in the log once, at the offset reported; one reported not
/// persisted is not in the log; a key's records are in the order sent; every record has a report.
#[test]
fn producing_through_a_leader_stopped_and_started() {
    let Some((addr, nodes)) = cluster() else { return };
    let mut c = cfg(&addr);
    c.set("message.timeout.ms", "60000").set("request.timeout.ms", "5000").set("socket.timeout.ms", "5000");
    let _restart = Restart(&nodes);
    wait_cluster(&nodes);
    let topic = unique("failover-p");
    create(&c, &topic, 6, 3);
    let (_, node) = leader_of(&c, &topic, &nodes);
    let p = Producer::new(&c, Heard::default()).unwrap();
    // at ~4,000 records/s throughout: 4 s healthy; the leader stopped until its partition is led
    // elsewhere (at most 30 s), 3 s more on the new leader; the old one started, 4 s more
    #[derive(PartialEq)]
    enum Phase {
        Healthy,
        Stopped(Instant),
        Moved(Instant),
        Started(Instant),
    }
    let (t0, mut i, mut phase, mut checked) = (Instant::now(), 0u64, Phase::Healthy, Instant::now());
    loop {
        let key = format!("k{}", i % 40);
        loop {
            match p.send(Record::to(&topic).key(key.as_bytes()).value(i.to_string().as_bytes()), i) {
                Ok(()) => break,
                Err((Error::QueueFull, _)) => drop(p.poll(Duration::from_millis(5))),
                Err((e, _)) => panic!("{e}"),
            }
        }
        i += 1;
        if i % 40 == 0 {
            p.poll(Duration::ZERO);
            std::thread::sleep(Duration::from_millis(10));
        }
        let now = Instant::now();
        phase = match phase {
            Phase::Healthy if now - t0 > Duration::from_secs(4) => {
                docker(&["stop", "-t", "1", &node]);
                Phase::Stopped(now)
            }
            Phase::Stopped(at) if now - checked > Duration::from_millis(500) => {
                checked = now;
                assert!(now - at < Duration::from_secs(30), "partition 0 led elsewhere within 30 s");
                let (led, _) = leader_of(&c, &topic, &nodes);
                if nodes[led as usize] != node {
                    Phase::Moved(now)
                } else {
                    Phase::Stopped(at)
                }
            }
            Phase::Moved(at) if now - at > Duration::from_secs(3) => {
                docker(&["start", &node]);
                Phase::Started(now)
            }
            Phase::Started(at) if now - at > Duration::from_secs(4) => break,
            p => p,
        };
    }
    eprintln!("{i} records sent, {:?}", t0.elapsed());
    let n = i;
    p.flush(Duration::from_secs(120)).expect("every record reported");
    let heard = p.context().0.lock().unwrap().clone();
    assert_eq!(heard.len() as u64, n, "a report each");
    wait_cluster(&nodes);
    let (read, _) = read_all(&c, &topic, 6, Duration::from_secs(120));
    let mut at: HashMap<u64, (i32, i64)> = HashMap::new();
    for (part, m) in &read {
        let id: u64 = std::str::from_utf8(m.value.as_deref().unwrap()).unwrap().parse().unwrap();
        assert!(at.insert(id, (*part, m.offset)).is_none(), "record {id} twice");
    }
    let mut delivered = 0;
    for (id, (part, off, err, persisted)) in &heard {
        match (err, at.get(id)) {
            (None, Some(&(p, o))) => {
                delivered += 1;
                assert_eq!((*part, off.unwrap_or(o)), (Some(p), o), "record {id}: where reported");
            }
            (None, None) => panic!("record {id}: reported delivered, not in the log"),
            (Some(e), Some(_)) => assert_ne!(*persisted, Persisted::No, "record {id}: not persisted ({e}), in the log"),
            (Some(_), None) => {}
        }
    }
    assert!(delivered as u64 > n * 9 / 10, "{delivered} of {n} delivered through a leader's stop");
    for log in [0, 1, 2, 3, 4, 5] {
        let mut last: HashMap<u64, u64> = HashMap::new();
        for (_, m) in read.iter().filter(|(p, _)| *p == log) {
            let id: u64 = std::str::from_utf8(m.value.as_deref().unwrap()).unwrap().parse().unwrap();
            if let Some(prev) = last.insert(id % 40, id) {
                assert!(prev < id, "key k{}: {id} after {prev}", id % 40);
            }
        }
    }
}

/// A backlog read while partition 0's leader is stopped, then started again: every record read
/// once, in order, as a reader of the healthy cluster reads them.
#[test]
fn consuming_through_a_leader_stopped_and_started() {
    let Some((addr, nodes)) = cluster() else { return };
    let mut c = cfg(&addr);
    c.set("socket.timeout.ms", "5000").set("max.partition.fetch.bytes", "65536");
    let _restart = Restart(&nodes);
    wait_cluster(&nodes);
    let topic = unique("failover-c");
    create(&c, &topic, 3, 3);
    let p = Producer::new(&c, Heard::default()).unwrap();
    for i in 0..60_000u64 {
        p.send(Record::to(&topic).partition((i % 3) as i32).value(i.to_string().as_bytes()), i).unwrap();
        if i % 5000 == 0 {
            p.poll(Duration::ZERO);
        }
    }
    p.flush(T).unwrap();
    let (healthy, _) = read_all(&c, &topic, 3, T);
    let (_, node) = leader_of(&c, &topic, &nodes);
    let assign = (0..3).map(|p| Assignment::new(&topic, p, Offset::Beginning)).collect();
    let mut k = Consumer::from_config(&c, assign).unwrap();
    let (mut read, mut ends) = (vec![], HashSet::new());
    let until = Instant::now() + Duration::from_secs(180);
    let mut stopped_at = None;
    while ends.len() < 3 {
        assert!(Instant::now() < until, "{} of 3 at their end, {} read", ends.len(), read.len());
        match k.next_event(Duration::from_millis(50), &mut |_| {}) {
            Ok(Some((i, Polled::Msg(m)))) => {
                read.push((i as i32, m));
                if read.len() % 500 == 0 {
                    std::thread::sleep(Duration::from_millis(20)); // a reader slower than the backlog
                }
                if read.len() == 10_000 && stopped_at.is_none() {
                    docker(&["stop", "-t", "1", &node]);
                    stopped_at = Some(Instant::now());
                }
            }
            Ok(Some((i, Polled::Eof))) => drop(ends.insert(i)),
            _ => {}
        }
        if stopped_at.is_some_and(|t| t.elapsed() > Duration::from_secs(8)) {
            docker(&["start", &node]);
            stopped_at = Some(Instant::now() + Duration::from_secs(3600)); // started: not again
        }
    }
    if stopped_at.is_some_and(|t| t < Instant::now() + Duration::from_secs(1800)) {
        docker(&["start", &node]);
    }
    wait_cluster(&nodes);
    let key = |v: &Vec<(i32, Message)>| -> Vec<(i32, i64, Vec<u8>)> {
        let mut x: Vec<_> = v.iter().map(|(p, m)| (*p, m.offset, m.value.clone().unwrap())).collect();
        x.sort();
        x
    };
    assert_eq!(key(&read), key(&healthy), "every record once, as the healthy cluster has them");
}

/// Every node of the cluster up and answering again.
fn wait_cluster(nodes: &[String]) {
    for n in nodes {
        let _ = Command::new("docker").args(["start", n]).output();
    }
    let until = Instant::now() + Duration::from_secs(120);
    loop {
        // node 0's admin API (scripts/cluster.sh: node i on 4964i)
        let out = Command::new("docker")
            .args(["exec", &nodes[0], "rpk", "cluster", "health", "-X", "admin.hosts=127.0.0.1:49640"])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&out.stdout).contains("Healthy:                          true") {
            return;
        }
        assert!(Instant::now() < until, "the cluster healthy again: {}", String::from_utf8_lossy(&out.stdout));
        std::thread::sleep(Duration::from_secs(1));
    }
}

// ---- carried over from brrrrr's acceptance suite (tests/acceptance/features/kafka.feature):
// the scenarios there that exercise the client rather than the pipeline above it

/// The only broker restarts under a consumer and a producer (brrrrr: "A source broker that
/// restarts is fetched again from where it was"): the consumer reads on from where it was, no
/// record skipped or read twice; the producer delivers what it sends after, each once, in order.
#[test]
fn the_only_broker_restarting_skips_and_repeats_nothing() {
    if env("FASTKA_IT_RPK").is_none() {
        return; // docker, and an image of its own
    }
    let node = "fastka-restart";
    let _ = Command::new("docker").args(["rm", "-fv", node]).output();
    docker(&[
        "run",
        "-d",
        "--name",
        node,
        "--network",
        "host",
        "redpandadata/redpanda:v24.2.7",
        "redpanda",
        "start",
        "--mode",
        "dev-container",
        "--smp",
        "1",
        "--memory",
        "1G",
        "--reserve-memory",
        "0M",
        "--check=false",
        "--kafka-addr",
        "0.0.0.0:59292",
        "--advertise-kafka-addr",
        "127.0.0.1:59292",
        "--rpc-addr",
        "0.0.0.0:59345",
        "--advertise-rpc-addr",
        "127.0.0.1:59345",
        "--pandaproxy-addr",
        "0.0.0.0:58282",
        "--schema-registry-addr",
        "0.0.0.0:58281",
        "--set",
        r#"redpanda.admin=[{"address":"0.0.0.0","port":59744}]"#,
    ]);
    struct Gone(&'static str);
    impl Drop for Gone {
        fn drop(&mut self) {
            let _ = Command::new("docker").args(["rm", "-fv", self.0]).output();
        }
    }
    let _gone = Gone(node);
    let addr = "127.0.0.1:59292";
    let mut c = cfg(addr);
    c.set("message.timeout.ms", "120000").set("socket.timeout.ms", "5000").set("reconnect.backoff.max.ms", "1000");
    let until = Instant::now() + Duration::from_secs(120);
    while Client::new(&c).unwrap().metadata(None, Duration::from_secs(2)).is_err() {
        assert!(Instant::now() < until, "the broker up");
        std::thread::sleep(Duration::from_millis(500));
    }
    let topic = unique("restart");
    create(&c, &topic, 1, 1);
    let p = Producer::new(&c, Heard::default()).unwrap();
    let mut k = Consumer::from_config(&c, vec![Assignment::new(&topic, 0, Offset::Beginning)]).unwrap();
    let (mut got, mut notices) = (vec![], vec![]);
    let read_to = |k: &mut Consumer, n: usize, got: &mut Vec<String>, notices: &mut Vec<String>| {
        let until = Instant::now() + Duration::from_secs(120);
        while got.len() < n {
            assert!(Instant::now() < until, "{} of {n} read", got.len());
            match k.next_event(Duration::from_millis(100), &mut |x| notices.push(x.to_string())) {
                Ok(Some((_, Polled::Msg(m)))) => got.push(String::from_utf8(m.value.unwrap()).unwrap()),
                Ok(_) => {}
                Err(e) => notices.push(format!("error: {e}")),
            }
        }
    };
    for i in 0..2000u64 {
        p.send(Record::to(&topic).value(i.to_string().as_bytes()), i).unwrap();
    }
    p.flush(T).unwrap();
    read_to(&mut k, 1000, &mut got, &mut notices); // half read, the rest queued or on the log
    docker(&["restart", "-t", "1", node]);
    for i in 2000..4000u64 {
        p.send(Record::to(&topic).value(i.to_string().as_bytes()), i).unwrap();
    }
    p.flush(Duration::from_secs(120)).expect("delivered once the broker is back");
    read_to(&mut k, 4000, &mut got, &mut notices);
    let want: Vec<String> = (0..4000).map(|i| i.to_string()).collect();
    assert_eq!(got, want, "every record once, in order, across the restart");
    let heard = p.context().0.lock().unwrap().clone();
    assert!(heard.values().all(|h| h.2.is_none()), "{:?}", heard.values().find(|h| h.2.is_some()));
    assert!(notices.iter().any(|n| n.contains("broker")), "the consumer said it reconnected: {notices:?}");
}

/// Batches librdkafka wrote, in each codec (brrrrr: "Batches librdkafka compressed with <codec>
/// are read"; kcat, librdkafka's own command line client, writes them): each record read once,
/// key, value and header as written. librdkafka's LZ4 framing is the one historically misread.
#[test]
fn batches_librdkafka_compressed_are_read() {
    let Some(addr) = redpanda() else { return };
    let c = cfg(&addr);
    for codec in ["gzip", "snappy", "lz4", "zstd"] {
        let topic = unique(&format!("rdk-{codec}"));
        create(&c, &topic, 1, 1);
        let input: String = (0..3000).map(|i| format!("k{}:{codec}-{i}-{}\n", i % 13, "z".repeat(i % 41))).collect();
        let mut kcat = Command::new("docker")
            .args(["run", "--rm", "-i", "--network", "host", "edenhill/kcat:1.7.1", "-P", "-b", &addr, "-t", &topic])
            .args(["-z", codec, "-K:", "-H", "via=librdkafka", "-X", "linger.ms=50", "-X", "batch.num.messages=500"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write as _;
        kcat.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
        assert!(kcat.wait().unwrap().success(), "kcat {codec}");
        let (read, _) = read_all(&c, &topic, 1, T);
        assert_eq!(read.len(), 3000, "{codec}");
        for (i, (_, m)) in read.iter().enumerate() {
            assert_eq!(m.offset, i as i64);
            assert_eq!(m.key.as_deref(), Some(format!("k{}", i % 13).as_bytes()), "{codec} #{i}");
            assert_eq!(m.value.as_deref(), Some(format!("{codec}-{i}-{}", "z".repeat(i % 41)).as_bytes()));
            assert_eq!(m.header("via"), Some(&b"librdkafka"[..]));
        }
    }
}

/// A writer the cluster has not authorized (brrrrr: "An anonymous writer denied InitProducerId
/// reports the broker's fatal cause"): its record fails, reported not persisted, with the
/// broker's code named, and the producer says why it can deliver nothing.
#[test]
fn a_writer_not_authorized_is_told_the_brokers_cause() {
    let Some(addr) = env("FASTKA_IT_SASL") else { return };
    let mut admin = cfg(&addr);
    admin.set("security.protocol", "sasl_plaintext").set("sasl.mechanisms", "SCRAM-SHA-256");
    admin.set("sasl.username", "it").set("sasl.password", "it-secret");
    let topic = unique("denied");
    create(&admin, &topic, 1, 1);
    let mut c = admin.clone();
    c.set("sasl.username", "noacl").set("sasl.password", "noacl-secret").set("message.timeout.ms", "10000");
    let p = Producer::new(&c, Heard::default()).unwrap();
    p.send(Record::to(&topic).value(b"not mine to write"), 0).unwrap();
    p.flush(Duration::from_secs(30)).unwrap();
    let heard = p.context().0.lock().unwrap().clone();
    let (_, _, err, persisted) = &heard[&0];
    let err = err.as_deref().expect("refused");
    assert!(err.contains("AUTHORIZATION_FAILED"), "the broker's cause named: {err}");
    assert_eq!(*persisted, Persisted::No);
    let (read, _) = read_all(&admin, &topic, 1, T);
    assert!(read.is_empty(), "nothing written");
}
