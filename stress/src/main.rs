//! The stress harness: fastka's producer and consumer through a TCP proxy that breaks the
//! connections under them (cut mid-frame, stalled, delayed, refused), against a real broker, for
//! as long as asked, with every outcome checked against the log the broker kept.
//!
//! `stress/run.sh` starts a Redpanda that advertises the proxy's address, so every connection the
//! client makes (bootstrap, leaders, the producer's and the consumer's) passes through it.
//!
//! Checked, for every message the producer was given:
//! - **at most once**: no message is in the log twice (the idempotent producer, through retries);
//! - **reported truly**: a delivery reported at an offset is in the log at that offset; a message
//!   reported not persisted is not in the log at all;
//! - **in order**: a key's messages are in the log in the order sent (one producer's, until it is
//!   fatal and replaced: a new producer is a new sequence);
//! - **every outcome reported**: each message sent has exactly one delivery report;
//! - **read whole**: the consumer, reading through the same faults, reads each partition's log
//!   exactly as a reader without faults does: no record skipped, none twice, in offset order.
use fastka::consumer::{Assignment, Consumer, Offset, Polled};
use fastka::producer::{Deliveries, Delivery, Persisted, Producer, Record};
use fastka::{Config, Error};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A small xorshift: the faults are reproducible from `STRESS_SEED`.
#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn chance(&mut self, per_million: u64) -> bool {
        self.next() % 1_000_000 < per_million
    }
}

/// The proxy's faults, switched on and off between phases.
struct Chaos {
    on: AtomicBool,
    /// per million chunks: a cut, a stall; a stall's longest (ms)
    cut_ppm: AtomicU64,
    stall_ppm: AtomicU64,
    stall_ms: AtomicU64,
    seed: AtomicU64,
    cuts: AtomicU64,
    stalls: AtomicU64,
    refused: AtomicU64,
}

/// Forwards `listen` to `upstream`, breaking connections while `chaos.on`: each chunk forwarded
/// may cut both sides (mid-frame, as a broker restarting or a network partition does), stall the
/// direction for up to 3 s (a client timeout, a Fetch overdue), or be delayed a few ms; a new
/// connection may be refused.
fn proxy(listen: &str, upstream: String, chaos: Arc<Chaos>) {
    let l = TcpListener::bind(listen).unwrap_or_else(|e| panic!("binding the proxy on {listen}: {e}"));
    std::thread::spawn(move || {
        for (n, c) in l.incoming().enumerate() {
            let Ok(client) = c else { continue };
            let mut rng = Rng(chaos.seed.load(Relaxed) ^ (n as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            if chaos.on.load(Relaxed) && rng.chance(100_000) {
                chaos.refused.fetch_add(1, Relaxed);
                drop(client);
                continue;
            }
            let Ok(server) = TcpStream::connect(&upstream) else { continue };
            for (from, to, mut rng) in [
                (client.try_clone().unwrap(), server.try_clone().unwrap(), rng.clone()),
                (server, client, Rng(rng.next() | 1)),
            ] {
                let chaos = chaos.clone();
                std::thread::spawn(move || pipe(from, to, &chaos, &mut rng));
            }
        }
    });
}

fn pipe(mut from: TcpStream, mut to: TcpStream, chaos: &Chaos, rng: &mut Rng) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if chaos.on.load(Relaxed) {
            if rng.chance(chaos.cut_ppm.load(Relaxed)) {
                chaos.cuts.fetch_add(1, Relaxed);
                // part of the chunk, then nothing: a frame cut short
                let _ = to.write_all(&buf[..(rng.next() as usize % n)]);
                break;
            }
            if rng.chance(chaos.stall_ppm.load(Relaxed)) {
                chaos.stalls.fetch_add(1, Relaxed);
                std::thread::sleep(Duration::from_millis(rng.next() % chaos.stall_ms.load(Relaxed).max(1)));
            } else if rng.chance(20_000) {
                std::thread::sleep(Duration::from_millis(rng.next() % 20));
            }
        }
        if to.write_all(&buf[..n]).is_err() {
            break;
        }
    }
    let _ = from.shutdown(Shutdown::Both);
    let _ = to.shutdown(Shutdown::Both);
}

/// One message's identity and what became of it.
#[derive(Clone, Debug)]
struct Sent {
    key: u32,
    generation: u32,
    outcome: Option<(Option<i32>, Option<i64>, Option<String>, Persisted)>,
}

#[derive(Default)]
struct Ledger(Mutex<Vec<Sent>>);
/// The ledger as the producer's context (shared with the harness).
struct Reports(Arc<Ledger>);
impl Deliveries for Reports {
    type Opaque = usize;
    fn delivery(&self, d: &Delivery<'_>, id: usize) {
        let mut l = self.0 .0.lock().unwrap();
        let s = &mut l[id];
        assert!(s.outcome.is_none(), "message {id}: a second delivery report");
        s.outcome = Some((d.partition, d.offset, d.error.map(Error::to_string), d.persisted));
    }
}

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn main() {
    let proxy_addr: String = env("STRESS_PROXY", "127.0.0.1:19095".to_string());
    let upstream: String = env("STRESS_UPSTREAM", "127.0.0.1:19094".to_string());
    let topic: String = env("STRESS_TOPIC", format!("stress-{}", std::process::id()));
    let parts: i32 = env("STRESS_PARTITIONS", 6);
    let secs: u64 = env("STRESS_SECS", 60);
    let keys: u32 = env("STRESS_KEYS", 64);
    let seed: u64 = env("STRESS_SEED", 0x5eed);
    let chaos = Arc::new(Chaos {
        on: AtomicBool::new(false),
        cut_ppm: AtomicU64::new(env("STRESS_CUT_PPM", 3_000)),
        stall_ppm: AtomicU64::new(env("STRESS_STALL_PPM", 1_000)),
        stall_ms: AtomicU64::new(env("STRESS_STALL_MS", 3_000)),
        seed: AtomicU64::new(seed),
        cuts: AtomicU64::new(0),
        stalls: AtomicU64::new(0),
        refused: AtomicU64::new(0),
    });
    proxy(&proxy_addr, upstream, chaos.clone());
    let mut cfg = Config::new();
    cfg.set("bootstrap.servers", &proxy_addr)
        .set("client.id", "fastka-stress")
        .set("socket.timeout.ms", "2000")
        .set("socket.connection.setup.timeout.ms", "2000")
        .set("request.timeout.ms", "1500")
        .set("message.timeout.ms", &env("STRESS_MESSAGE_TIMEOUT_MS", 15_000u64).to_string())
        .set("linger.ms", "2")
        .set("batch.num.messages", "500")
        .set("compression.type", &env("STRESS_CODEC", "lz4".to_string()))
        .set("enable.partition.eof", "true")
        .set("fetch.wait.max.ms", "50")
        .set("isolation.level", "read_uncommitted");
    let admin = fastka::client::Client::new(&cfg).unwrap();
    for (t, r) in admin.create_topics(&[(topic.clone(), parts, 1)], Duration::from_secs(20)).unwrap() {
        r.unwrap_or_else(|e| panic!("creating {t}: {e}"));
    }

    // ---- produce through the faults ----
    eprintln!("stress: producing to {topic} ({parts} partitions, {keys} keys) for {secs} s through the faults");
    chaos.on.store(true, Relaxed);
    let ledger = Arc::new(Ledger::default());
    let until = Instant::now() + Duration::from_secs(secs);
    let (mut generation, mut fatal_producers) = (0u32, 0);
    let mut p = Producer::new(&cfg, Reports(ledger.clone())).unwrap();
    let mut rng = Rng(seed | 1);
    while Instant::now() < until {
        let id = ledger.0.lock().unwrap().len();
        let key = (rng.next() % keys as u64) as u32;
        ledger.0.lock().unwrap().push(Sent { key, generation, outcome: None });
        let (k, v) = (format!("k{key}"), format!("{id}"));
        let mut opaque = id;
        loop {
            match p.send(Record::to(&topic).key(k.as_bytes()).value(v.as_bytes()), opaque) {
                Ok(()) => break,
                Err((Error::QueueFull, o)) => {
                    opaque = o;
                    p.poll(Duration::from_millis(5));
                }
                Err((Error::Fatal(c, why), o)) => {
                    // the producer said where its sequence may have a gap: a new one goes on
                    eprintln!("stress: producer {generation} fatal: {c}: {why}");
                    p.flush(Duration::from_secs(60)).expect("a fatal producer reports every message");
                    (generation, fatal_producers) = (generation + 1, fatal_producers + 1);
                    ledger.0.lock().unwrap()[o].generation = generation;
                    p = Producer::new(&cfg, Reports(ledger.clone())).unwrap();
                    opaque = o;
                }
                Err((e, _)) => panic!("send: {e}"),
            }
        }
        if id % 64 == 0 {
            p.poll(Duration::ZERO);
            // a pace that leaves the proxy's threads a share of the CPU
            std::thread::sleep(Duration::from_micros(200));
        }
    }
    p.flush(Duration::from_secs(120)).expect("every message reported within message.timeout.ms");
    drop(p);
    let sent = std::mem::take(&mut *ledger.0.lock().unwrap());

    // ---- read the log back without faults, then through them ----
    chaos.on.store(false, Relaxed);
    let clean = read_all(&cfg, &topic, parts, Duration::from_secs(120));
    // the read through the faults at the default rates: at the harsh ones a Fetch of
    // fetch.max.bytes (~800 chunks) would almost never arrive whole, on any client
    chaos.cut_ppm.store(3_000, Relaxed);
    chaos.stall_ppm.store(1_000, Relaxed);
    chaos.stall_ms.store(3_000, Relaxed);
    chaos.on.store(true, Relaxed);
    chaos.seed.store(seed ^ 0xc0ffee, Relaxed);
    let chaotic = read_all(&cfg, &topic, parts, Duration::from_secs(300));
    chaos.on.store(false, Relaxed);

    // ---- check ----
    let mut failures = vec![];
    let mut fail = |s: String| {
        if failures.len() < 20 {
            eprintln!("FAIL: {s}");
        }
        failures.push(s);
    };
    for p in 0..parts as usize {
        if chaotic[p] != clean[p] {
            let (a, b) = (&clean[p], &chaotic[p]);
            let at = a.iter().zip(b).position(|(x, y)| x != y).unwrap_or(a.len().min(b.len()));
            fail(format!(
                "partition {p}: read through the faults differs from the log at record {at} ({} vs {} records)",
                b.len(),
                a.len()
            ));
        }
    }
    // where each message is in the log
    let mut at: HashMap<usize, (i32, i64)> = HashMap::new();
    for (p, log) in clean.iter().enumerate() {
        for &(offset, ref key, id) in log {
            let Some(s) = sent.get(id) else {
                fail(format!("partition {p} offset {offset}: message {id} was never sent"));
                continue;
            };
            if *key != format!("k{}", s.key) {
                fail(format!("message {id}: key {key}, sent with k{}", s.key));
            }
            if let Some(first) = at.insert(id, (p as i32, offset)) {
                fail(format!("message {id} written twice: at {first:?} and {:?}", (p, offset)));
            }
        }
    }
    let (mut delivered, mut failed, mut possibly) = (0, 0, 0);
    for (id, s) in sent.iter().enumerate() {
        let Some((part, offset, err, persisted)) = &s.outcome else {
            fail(format!("message {id}: no delivery report"));
            continue;
        };
        match (err, at.get(&id)) {
            (None, Some(&(p, o))) => {
                delivered += 1;
                if *part != Some(p) || offset.is_some_and(|x| x != o) {
                    fail(format!("message {id}: reported at {part:?}/{offset:?}, in the log at {p}/{o}"));
                }
            }
            (None, None) => fail(format!("message {id}: reported delivered at {part:?}/{offset:?}, not in the log")),
            (Some(e), found) => {
                failed += 1;
                match persisted {
                    Persisted::No if found.is_some() => {
                        fail(format!("message {id}: reported not persisted ({e}), in the log at {found:?}"))
                    }
                    Persisted::Possibly => possibly += 1,
                    _ => {}
                }
            }
        }
    }
    // a key's messages in the log in the order sent, within one producer's generation
    for log in &clean {
        let mut last: HashMap<(u32, u32), usize> = HashMap::new();
        for &(offset, _, id) in log {
            let Some(s) = sent.get(id) else { continue };
            if let Some(prev) = last.insert((s.key, s.generation), id) {
                if prev > id {
                    fail(format!("key k{}: message {id} at offset {offset} after message {prev}, sent later", s.key));
                }
            }
        }
    }
    let records: usize = clean.iter().map(Vec::len).sum();
    let distinct: HashSet<usize> = at.keys().copied().collect();
    println!(
        "stress: {} sent, {delivered} delivered, {failed} failed ({possibly} possibly persisted), {records} records in \
         the log ({} distinct), {fatal_producers} producers fatal; faults: {} cuts, {} stalls, {} refused",
        sent.len(),
        distinct.len(),
        chaos.cuts.load(Relaxed),
        chaos.stalls.load(Relaxed),
        chaos.refused.load(Relaxed)
    );
    if sent.is_empty() || delivered == 0 {
        fail("nothing was delivered: the harness measured nothing".into());
    }
    if !failures.is_empty() {
        eprintln!("stress: {} invariant violations", failures.len());
        std::process::exit(1);
    }
    println!("stress: ok");
}

/// Each partition's log from its start to its end: (offset, key, message id).
fn read_all(cfg: &Config, topic: &str, parts: i32, limit: Duration) -> Vec<Vec<(i64, String, usize)>> {
    let assign = (0..parts).map(|p| Assignment::new(topic, p, Offset::Beginning)).collect();
    let mut c = Consumer::from_config(cfg, assign).unwrap();
    let mut logs = vec![vec![]; parts as usize];
    let mut ends = HashSet::new();
    let until = Instant::now() + limit;
    while ends.len() < parts as usize {
        if Instant::now() >= until {
            let at: Vec<_> = (0..parts as usize).map(|i| (c.position(i), logs[i].len())).collect();
            panic!("reading back: {} of {parts} partitions at their end; (position, records read): {at:?}", ends.len());
        }
        match c.next_event(Duration::from_millis(100), &mut |n| eprintln!("stress: reading back: {n}")) {
            Ok(Some((i, Polled::Msg(m)))) => {
                let log: &mut Vec<(i64, String, usize)> = &mut logs[i];
                if let Some(&(last, _, _)) = log.last() {
                    assert!(m.offset > last, "partition {i}: offset {} after {last}", m.offset);
                }
                let key = String::from_utf8(m.key.unwrap_or_default()).unwrap();
                let id = std::str::from_utf8(m.value.as_deref().unwrap_or_default()).unwrap().parse().unwrap();
                log.push((m.offset, key, id));
                ends.remove(&i);
            }
            Ok(Some((i, Polled::Eof))) => drop(ends.insert(i)),
            Ok(_) => {}
            Err(e) => eprintln!("stress: reading back: {e} (retried)"),
        }
    }
    logs
}
