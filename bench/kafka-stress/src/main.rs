//! fastka's producer against librdkafka's, under the shape of a metrics pipeline at the top of the
//! hour: many pipelines' producers, a steady trickle, and every window closing at once. The
//! clients reach the broker through a proxy of this process, which sees every Produce: how many,
//! how big, how many partitions each carries, how many are outstanding on a connection, and how
//! long the broker took. It can also hold the broker's answers back (a stall). Afterwards every
//! partition is read back: each message delivered must be there once, in its pipeline's order.
//!
//! See README.md for the flags and how to read the output.
use fastka::client::Client;
use fastka::consumer::{Assignment, Consumer, Offset, Polled};
use fastka::producer::{Deliveries, Delivery, Producer, Record};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer as _, ProducerContext};
use rdkafka::ClientContext;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

// ---------------------------------------------------------------------------------------------
// settings

#[derive(Clone, Debug)]
struct Args {
    client: String,
    /// each broker's real address, and the proxy's port for it (its advertised one)
    upstream: Vec<String>,
    listen: Vec<u16>,
    partitions: i32,
    replicas: i16,
    pipelines: usize,
    producers: usize,
    size: usize,
    rate: f64,
    burst: usize,
    period: f64,
    first: f64,
    duration: f64,
    timeout_ms: u64,
    stalls: Vec<(f64, f64)>,
    delay_ms: f64,
    both: Vec<(String, String)>,
    fastka: Vec<(String, String)>,
    rdkafka: Vec<(String, String)>,
    csv: Option<String>,
    topic: Option<String>,
}

const USAGE: &str = "kafka-stress --client fastka|rdkafka --upstream HOST:PORT[,..] --listen PORT[,..] [options]
  (each broker advertises 127.0.0.1:<its --listen port>; the proxy forwards it to its --upstream)
  --partitions N      the topic's partitions (24)
  --replicas N        the new topic's replication factor (1)
  --pipelines N       producers, one per pipeline, each on a thread of its own (16)
  --producers N       producers the pipelines share (= --pipelines)
  --size BYTES        a message's value (200)
  --rate N            a pipeline's steady messages/s between bursts (50)
  --burst N           a pipeline's messages at each top of the hour, sent as fast as it can (20000)
  --period S          seconds between tops of the hour (20)
  --first S           the first top of the hour (5)
  --duration S        how long it sends (45)
  --timeout-ms MS     message.timeout.ms (4500: a 5 s lease fence less a tenth)
  --stall AT:SECS     the broker's answers held back SECS from AT (repeatable)
  --delay-ms MS       every answer held until its request is MS old (a cluster's replication, acks=all)
  -X key=value        a setting for both clients (repeatable)
  -F key=value        for fastka alone;  -R key=value  for librdkafka alone
  --csv PATH          each Produce the proxy saw, one row each
  --topic NAME        an existing topic, else a new one is made";

fn kv(s: &str) -> (String, String) {
    let (k, v) = s.split_once('=').unwrap_or_else(|| panic!("{s}: expected key=value"));
    (k.to_string(), v.to_string())
}

fn args() -> Args {
    let mut a = Args {
        client: String::new(),
        upstream: vec![],
        listen: vec![],
        partitions: 24,
        replicas: 1,
        pipelines: 16,
        producers: 0,
        size: 200,
        rate: 50.0,
        burst: 20_000,
        period: 20.0,
        first: 5.0,
        duration: 45.0,
        timeout_ms: 4500,
        stalls: vec![],
        delay_ms: 0.0,
        both: vec![],
        fastka: vec![],
        rdkafka: vec![],
        csv: None,
        topic: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(f) = it.next() {
        let mut v = || it.next().unwrap_or_else(|| panic!("{f}: a value\n{USAGE}"));
        match f.as_str() {
            "--client" => a.client = v(),
            "--upstream" => a.upstream = v().split(',').map(String::from).collect(),
            "--listen" => a.listen = v().split(',').map(|p| p.parse().unwrap()).collect(),
            "--partitions" => a.partitions = v().parse().unwrap(),
            "--replicas" => a.replicas = v().parse().unwrap(),
            "--pipelines" => a.pipelines = v().parse().unwrap(),
            "--producers" => a.producers = v().parse().unwrap(),
            "--size" => a.size = v().parse().unwrap(),
            "--rate" => a.rate = v().parse().unwrap(),
            "--burst" => a.burst = v().parse().unwrap(),
            "--period" => a.period = v().parse().unwrap(),
            "--first" => a.first = v().parse().unwrap(),
            "--duration" => a.duration = v().parse().unwrap(),
            "--timeout-ms" => a.timeout_ms = v().parse().unwrap(),
            "--stall" => {
                let s = v();
                let (at, secs) = s.split_once(':').expect("--stall AT:SECS");
                a.stalls.push((at.parse().unwrap(), secs.parse().unwrap()));
            }
            "--delay-ms" => a.delay_ms = v().parse().unwrap(),
            "-X" => a.both.push(kv(&v())),
            "-F" => a.fastka.push(kv(&v())),
            "-R" => a.rdkafka.push(kv(&v())),
            "--csv" => a.csv = Some(v()),
            "--topic" => a.topic = Some(v()),
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            _ => panic!("{f}?\n{USAGE}"),
        }
    }
    if a.producers == 0 {
        a.producers = a.pipelines;
    }
    assert!(a.client == "fastka" || a.client == "rdkafka", "{USAGE}");
    assert!(!a.upstream.is_empty() && a.upstream.len() == a.listen.len(), "{USAGE}");
    a
}

// ---------------------------------------------------------------------------------------------
// the proxy: the broker advertises the proxy's port, so every connection passes through it

/// A Produce as the proxy saw it.
#[derive(Clone, Debug)]
struct Seen {
    conn: usize,
    /// s from the start
    at: f64,
    bytes: usize,
    partitions: usize,
    /// Produce requests outstanding on its connection once it was sent, itself included
    outstanding: usize,
    /// ms until the broker's answer reached the proxy (None: never answered)
    ms: Option<f64>,
}

#[derive(Default)]
struct Proxy {
    seen: Mutex<Vec<Seen>>,
    /// answers held back until then
    stall_until: Mutex<Option<Instant>>,
    /// every answer held until its request is this old
    delay: Duration,
}

fn read_frame(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut n = [0u8; 4];
    s.read_exact(&mut n)?;
    let n = i32::from_be_bytes(n) as usize;
    let mut b = vec![0u8; 4 + n];
    b[..4].copy_from_slice(&(n as i32).to_be_bytes());
    s.read_exact(&mut b[4..])?;
    Ok(b)
}

/// A cursor over a request, for the few fields read here.
struct Cur<'a>(&'a [u8], usize);
impl Cur<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let b = self.0.get(self.1..self.1 + n)?;
        self.1 += n;
        Some(b)
    }
    fn i16(&mut self) -> Option<i16> {
        Some(i16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }
    fn i32(&mut self) -> Option<i32> {
        Some(i32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }
    fn uvarint(&mut self) -> Option<u64> {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let b = *self.take(1)?.first()?;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(v);
            }
            shift += 7;
        }
    }
    /// a length: an INT16/INT32 (non-flexible) or an unsigned varint less one (flexible)
    fn len(&mut self, flexible: bool, wide: bool) -> Option<i64> {
        if flexible {
            Some(self.uvarint()? as i64 - 1)
        } else if wide {
            self.i32().map(i64::from)
        } else {
            self.i16().map(i64::from)
        }
    }
    fn skip_len(&mut self, flexible: bool, wide: bool) -> Option<()> {
        let n = self.len(flexible, wide)?;
        if n > 0 {
            self.take(n as usize)?;
        }
        Some(())
    }
    fn tags(&mut self, flexible: bool) -> Option<()> {
        if flexible {
            for _ in 0..self.uvarint()? {
                self.uvarint()?;
                let n = self.uvarint()?;
                self.take(n as usize)?;
            }
        }
        Some(())
    }
}

/// The partitions a Produce request (frame, size included) carries.
fn produce_partitions(frame: &[u8], version: i16) -> Option<usize> {
    let flexible = version >= 9;
    let mut c = Cur(frame, 4 + 8);
    c.skip_len(false, false)?; // client id: a nullable string, never compact
    c.tags(flexible)?;
    if version >= 3 {
        c.skip_len(flexible, false)?; // transactional id
    }
    c.i16()?; // acks
    c.i32()?; // timeout
    let (mut n, topics) = (0, c.len(flexible, true)?);
    for _ in 0..topics.max(0) {
        c.skip_len(flexible, false)?;
        let parts = c.len(flexible, true)?;
        for _ in 0..parts.max(0) {
            c.i32()?;
            c.skip_len(flexible, true)?;
            c.tags(flexible)?;
            n += 1;
        }
        c.tags(flexible)?;
    }
    Some(n)
}

fn proxy(listen: u16, upstream: String, p: Arc<Proxy>, start: Instant) {
    let l = TcpListener::bind(("127.0.0.1", listen)).expect("the proxy's port");
    static CONNS: AtomicUsize = AtomicUsize::new(0);
    std::thread::spawn(move || {
        for client in l.incoming().flatten() {
            let id = CONNS.fetch_add(1, SeqCst);
            let Ok(server) = TcpStream::connect(&upstream) else { continue };
            let _ = (client.set_nodelay(true), server.set_nodelay(true));
            // corr -> index in `seen`, sent at
            let open: Arc<Mutex<HashMap<i32, (usize, Instant)>>> = Arc::default();
            let (mut c_in, mut s_out) = (client.try_clone().unwrap(), server.try_clone().unwrap());
            let (p1, open1) = (p.clone(), open.clone());
            std::thread::spawn(move || {
                while let Ok(f) = read_frame(&mut c_in) {
                    if f.len() >= 12 && i16::from_be_bytes([f[4], f[5]]) == 0 {
                        let version = i16::from_be_bytes([f[6], f[7]]);
                        let corr = i32::from_be_bytes([f[8], f[9], f[10], f[11]]);
                        let mut open = open1.lock().unwrap();
                        let mut seen = p1.seen.lock().unwrap();
                        let outstanding = open.len() + 1;
                        open.insert(corr, (seen.len(), Instant::now()));
                        seen.push(Seen {
                            conn: id,
                            at: start.elapsed().as_secs_f64(),
                            bytes: f.len(),
                            partitions: produce_partitions(&f, version).unwrap_or(0),
                            outstanding,
                            ms: None,
                        });
                    }
                    if s_out.write_all(&f).is_err() {
                        break;
                    }
                }
                let _ = s_out.shutdown(Shutdown::Both);
            });
            let (mut s_in, mut c_out) = (server, client);
            let p2 = p.clone();
            std::thread::spawn(move || {
                while let Ok(f) = read_frame(&mut s_in) {
                    let until = *p2.stall_until.lock().unwrap();
                    if let Some(u) = until {
                        std::thread::sleep(u.saturating_duration_since(Instant::now()));
                    }
                    let corr = i32::from_be_bytes([f[4], f[5], f[6], f[7]]);
                    let sent = open.lock().unwrap().remove(&corr);
                    if let Some((i, at)) = sent {
                        // answers come in order, their requests too: no delay adds up
                        std::thread::sleep((at + p2.delay).saturating_duration_since(Instant::now()));
                        p2.seen.lock().unwrap()[i].ms = Some(at.elapsed().as_secs_f64() * 1e3);
                    }
                    if c_out.write_all(&f).is_err() {
                        break;
                    }
                }
                let _ = c_out.shutdown(Shutdown::Both);
            });
        }
    });
}

// ---------------------------------------------------------------------------------------------
// the producers

/// What each pipeline heard: delivery latencies (ms), failures, offsets by message.
#[derive(Default)]
struct Heard {
    ms: Mutex<Vec<f64>>,
    failed: Mutex<HashMap<String, usize>>,
    delivered: AtomicUsize,
    /// (pipeline, seq) delivered
    ok: Mutex<Vec<(u32, u64)>>,
    /// each send() call, ms: how long a pipeline's thread is held by its producer
    send_ms: Mutex<Vec<f64>>,
    /// each pipeline's burst, ms from its first send to its last returning
    burst_ms: Mutex<Vec<f64>>,
}

impl Heard {
    fn heard(&self, pipeline: u32, seq: u64, sent: Instant, error: Option<String>) {
        match error {
            None => {
                self.ms.lock().unwrap().push(sent.elapsed().as_secs_f64() * 1e3);
                self.delivered.fetch_add(1, SeqCst);
                self.ok.lock().unwrap().push((pipeline, seq));
            }
            Some(e) => *self.failed.lock().unwrap().entry(e).or_default() += 1,
        }
    }
}

struct FastkaCtx(Arc<Heard>);
impl Deliveries for FastkaCtx {
    type Opaque = (u32, u64, Instant);
    fn delivery(&self, d: &Delivery<'_>, (p, s, t): Self::Opaque) {
        self.0.heard(p, s, t, d.error.map(|e| short(&e.to_string())));
    }
}

struct RdCtx(Arc<Heard>);
impl ClientContext for RdCtx {}
impl ProducerContext for RdCtx {
    type DeliveryOpaque = Box<(u32, u64, Instant)>;
    fn delivery(&self, r: &DeliveryResult<'_>, o: Self::DeliveryOpaque) {
        let (p, s, t) = *o;
        self.0.heard(p, s, t, r.as_ref().err().map(|(e, _)| short(&e.to_string())));
    }
}

/// An error's kind, not its detail (which names partitions and offsets).
fn short(e: &str) -> String {
    e.split([':', '(']).next().unwrap_or(e).trim().to_string()
}

enum Sender {
    Fastka(Producer<FastkaCtx>),
    Rd(BaseProducer<RdCtx>),
}

impl Sender {
    fn new(a: &Args, brokers: &str, heard: Arc<Heard>) -> Sender {
        let base = [
            ("bootstrap.servers".to_string(), brokers.to_string()),
            ("linger.ms".into(), "2".into()),
            ("enable.idempotence".into(), "true".into()),
            ("message.timeout.ms".into(), a.timeout_ms.to_string()),
        ];
        if a.client == "fastka" {
            let mut c = fastka::Config::new();
            for (k, v) in base.iter().chain(&a.both).chain(&a.fastka) {
                c.set(k, v);
            }
            Sender::Fastka(Producer::new(&c, FastkaCtx(heard)).expect("fastka's settings"))
        } else {
            let mut c = rdkafka::ClientConfig::new();
            for (k, v) in base.iter().chain(&a.both).chain(&a.rdkafka) {
                c.set(k, v);
            }
            Sender::Rd(c.create_with_context(RdCtx(heard)).expect("librdkafka's settings"))
        }
    }

    /// Sent, after waiting out a full queue.
    fn send(&self, topic: &str, value: &[u8], o: (u32, u64, Instant)) {
        match self {
            Sender::Fastka(p) => {
                let mut o = o;
                loop {
                    match p.send(Record::to(topic).value(value), o) {
                        Ok(()) => return,
                        Err((fastka::Error::QueueFull, back)) => {
                            o = back;
                            p.poll(Duration::from_millis(1));
                        }
                        Err((e, back)) => {
                            p.context().0.heard(back.0, back.1, back.2, Some(short(&e.to_string())));
                            return;
                        }
                    }
                }
            }
            Sender::Rd(p) => {
                let mut r = BaseRecord::<(), [u8], _>::with_opaque_to(topic, Box::new(o)).payload(value);
                loop {
                    match p.send(r) {
                        Ok(()) => return,
                        Err((e, back))
                            if e.rdkafka_error_code() == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
                        {
                            r = back;
                            p.poll(Duration::from_millis(1));
                        }
                        Err((e, back)) => {
                            let (pp, s, t) = *back.delivery_opaque;
                            p.context().0.heard(pp, s, t, Some(short(&e.to_string())));
                            return;
                        }
                    }
                }
            }
        }
    }

    fn poll(&self) {
        match self {
            Sender::Fastka(p) => drop(p.poll(Duration::ZERO)),
            Sender::Rd(p) => p.poll(Duration::ZERO),
        }
    }

    fn flush(&self, t: Duration) {
        match self {
            Sender::Fastka(p) => drop(p.flush(t)),
            Sender::Rd(p) => drop(p.flush(t)),
        }
    }
}

/// A pipeline: the trickle, a burst at each top of the hour; every message's value carries its
/// pipeline and sequence, for the read back.
fn pipeline(a: &Args, id: u32, topic: &str, s: &Sender, heard: &Heard, start: Instant, go: &AtomicBool) {
    while !go.load(SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut value = vec![0u8; a.size.max(12)];
    value[..4].copy_from_slice(&id.to_be_bytes());
    let (mut seq, mut next_tick, mut next_burst) = (0u64, 0.0, a.first);
    let gap = if a.rate > 0.0 { 1.0 / a.rate } else { f64::MAX };
    loop {
        let now = start.elapsed().as_secs_f64();
        if now >= a.duration {
            break;
        }
        if now >= next_burst {
            let (t0, mut calls) = (Instant::now(), Vec::with_capacity(a.burst));
            for _ in 0..a.burst {
                value[4..12].copy_from_slice(&seq.to_be_bytes());
                let t = Instant::now();
                s.send(topic, &value, (id, seq, t));
                calls.push(t.elapsed().as_secs_f64() * 1e3);
                seq += 1;
            }
            heard.burst_ms.lock().unwrap().push(t0.elapsed().as_secs_f64() * 1e3);
            heard.send_ms.lock().unwrap().extend(calls);
            next_burst += a.period;
        }
        if now >= next_tick {
            value[4..12].copy_from_slice(&seq.to_be_bytes());
            s.send(topic, &value, (id, seq, Instant::now()));
            seq += 1;
            next_tick += gap;
        }
        s.poll();
        std::thread::sleep(Duration::from_micros(500));
    }
}

// ---------------------------------------------------------------------------------------------
// the read back and the report

/// Every message in the topic: (pipeline, seq) in each partition's order.
fn read_back(brokers: &str, topic: &str, partitions: i32) -> Vec<(i32, u32, u64)> {
    let mut c = fastka::Config::new();
    c.set("bootstrap.servers", brokers).set("enable.partition.eof", "true").set("fetch.wait.max.ms", "20");
    let assign = (0..partitions).map(|p| Assignment::new(topic, p, Offset::Beginning)).collect();
    let mut k = Consumer::from_config(&c, assign).unwrap();
    let (mut out, mut ends) = (vec![], std::collections::HashSet::new());
    let until = Instant::now() + Duration::from_secs(120);
    while ends.len() < partitions as usize && Instant::now() < until {
        match k.next_event(Duration::from_millis(200), &mut |w| eprintln!("lost: {w}")) {
            Ok(Some((i, Polled::Msg(m)))) => {
                let v = m.value.unwrap_or_default();
                if v.len() >= 12 {
                    let id = u32::from_be_bytes(v[..4].try_into().unwrap());
                    out.push((i as i32, id, u64::from_be_bytes(v[4..12].try_into().unwrap())));
                }
            }
            Ok(Some((i, Polled::Eof))) => drop(ends.insert(i)),
            Ok(_) => {}
            Err(e) => eprintln!("read back: {e}"),
        }
    }
    out
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() - 1) as f64 * q).round() as usize]
}

fn main() {
    let a = args();
    let start = Instant::now();
    let px = Arc::new(Proxy { delay: Duration::from_secs_f64(a.delay_ms / 1e3), ..Proxy::default() });
    for (l, u) in a.listen.iter().zip(&a.upstream) {
        proxy(*l, u.clone(), px.clone(), start);
    }
    let brokers = a.listen.iter().map(|l| format!("127.0.0.1:{l}")).collect::<Vec<_>>().join(",");

    let topic = a.topic.clone().unwrap_or_else(|| {
        let n = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        let t = format!("stress-{}-{n}", a.client);
        let mut c = fastka::Config::new();
        c.set("bootstrap.servers", &brokers);
        let r =
            Client::new(&c).unwrap().create_topics(&[(t.clone(), a.partitions, a.replicas)], Duration::from_secs(20));
        r.expect("CreateTopics").into_iter().for_each(|(_, r)| r.expect("the topic"));
        std::thread::sleep(Duration::from_secs(2)); // its leaders elected
        t
    });

    for &(at, secs) in &a.stalls {
        let px = px.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs_f64(at).saturating_sub(start.elapsed()));
            *px.stall_until.lock().unwrap() = Some(Instant::now() + Duration::from_secs_f64(secs));
            eprintln!("stall: the broker's answers held back {secs} s from {at} s");
        });
    }

    let heard = Arc::new(Heard::default());
    let go = Arc::new(AtomicBool::new(false));
    let begin = Instant::now();
    let senders: Vec<Arc<Sender>> =
        (0..a.producers).map(|_| Arc::new(Sender::new(&a, &brokers, heard.clone()))).collect();
    let threads: Vec<_> = (0..a.pipelines as u32)
        .map(|id| {
            let (a, topic, go, heard) = (a.clone(), topic.clone(), go.clone(), heard.clone());
            let s = senders[id as usize % senders.len()].clone();
            std::thread::spawn(move || pipeline(&a, id, &topic, &s, &heard, begin, &go))
        })
        .collect();
    std::thread::sleep(Duration::from_secs(1)); // every producer connected
    go.store(true, SeqCst);
    for t in threads {
        t.join().unwrap();
    }
    for s in &senders {
        s.flush(Duration::from_secs(60));
    }
    let sent_secs = begin.elapsed().as_secs_f64();

    // ------------------------------------------------------------------ the read back
    let written = read_back(&brokers, &topic, a.partitions);
    let ok: std::collections::HashSet<(u32, u64)> = heard.ok.lock().unwrap().iter().copied().collect();
    let mut count: HashMap<(u32, u64), usize> = HashMap::new();
    let mut last: HashMap<(i32, u32), u64> = HashMap::new();
    let mut out_of_order = 0;
    for &(part, id, seq) in &written {
        *count.entry((id, seq)).or_default() += 1;
        if let Some(prev) = last.insert((part, id), seq) {
            if seq <= prev {
                out_of_order += 1;
            }
        }
    }
    let dups: usize = count.values().map(|n| n - 1).sum();
    let missing = ok.iter().filter(|k| !count.contains_key(k)).count();
    let failed_but_written = count.keys().filter(|k| !ok.contains(k)).count();

    // ------------------------------------------------------------------ the report
    let seen = px.seen.lock().unwrap().clone();
    let shift = begin.duration_since(start).as_secs_f64();
    if let Some(path) = &a.csv {
        let mut f = std::fs::File::create(path).unwrap();
        writeln!(f, "client,conn,at_s,bytes,partitions,outstanding,ms").unwrap();
        for s in &seen {
            let ms = s.ms.map_or(String::new(), |m| format!("{m:.2}"));
            let at = s.at - shift;
            writeln!(f, "{},{},{at:.4},{},{},{},{ms}", a.client, s.conn, s.bytes, s.partitions, s.outstanding).unwrap();
        }
    }
    let window = |s: &Seen, lo: f64, hi: f64| (lo..hi).contains(&(s.at - shift));
    let summary = |label: &str, lo: f64, hi: f64| {
        let w: Vec<&Seen> = seen.iter().filter(|s| window(s, lo, hi)).collect();
        if w.is_empty() {
            return;
        }
        let secs = hi - lo;
        let bytes: usize = w.iter().map(|s| s.bytes).sum();
        let mut ms: Vec<f64> = w.iter().filter_map(|s| s.ms).collect();
        let parts: Vec<f64> = w.iter().map(|s| s.partitions as f64).collect();
        let mut kb: Vec<f64> = w.iter().map(|s| s.bytes as f64 / 1024.0).collect();
        let max_out = w.iter().map(|s| s.outstanding).max().unwrap_or(0);
        println!(
            "  {label:<14} {:>7.1} req/s  {:>7.1} KB/req avg  {:>7.1} KB p99  {:>5.1} parts/req avg  {:>6.1} MB/s  produce p50 {:>7.1} ms  p99 {:>7.1} ms  max {:>7.1} ms  max outstanding/conn {max_out}",
            w.len() as f64 / secs,
            bytes as f64 / 1024.0 / w.len() as f64,
            pct(&mut kb, 0.99),
            parts.iter().sum::<f64>() / parts.len() as f64,
            bytes as f64 / 1048576.0 / secs,
            pct(&mut ms, 0.5),
            pct(&mut ms, 0.99),
            pct(&mut ms, 1.0),
        );
    };
    println!(
        "== {} | topic {topic} | {} pipelines on {} producers x {} partitions | {} B messages",
        a.client, a.pipelines, a.producers, a.partitions, a.size
    );
    println!(
        "   settings: -X {:?} -F {:?} -R {:?} stalls {:?} delay {} ms",
        a.both, a.fastka, a.rdkafka, a.stalls, a.delay_ms
    );
    println!(" Produce requests (as the proxy saw them):");
    summary("whole run", 0.0, sent_secs);
    let mut t = a.first;
    while t < a.duration {
        summary(&format!("burst @{t:.0}s 3s"), t, t + 3.0);
        t += a.period;
    }
    summary("steady (0-first)", 0.0, a.first);
    let mut ms = heard.ms.lock().unwrap().clone();
    let failed = heard.failed.lock().unwrap().clone();
    println!(
        " deliveries: {} ok, {} failed {:?}; delivery latency p50 {:.1} ms  p99 {:.1} ms  p99.9 {:.1} ms  max {:.1} ms",
        heard.delivered.load(SeqCst),
        failed.values().sum::<usize>(),
        failed,
        pct(&mut ms, 0.5),
        pct(&mut ms, 0.99),
        pct(&mut ms, 0.999),
        pct(&mut ms, 1.0),
    );
    let (mut sm, mut bm) = (heard.send_ms.lock().unwrap().clone(), heard.burst_ms.lock().unwrap().clone());
    println!(
        " send path: a burst enqueued in p50 {:.1} ms  max {:.1} ms; a send() call p99 {:.3} ms  p99.9 {:.3} ms  max {:.1} ms",
        pct(&mut bm, 0.5),
        pct(&mut bm, 1.0),
        pct(&mut sm, 0.99),
        pct(&mut sm, 0.999),
        pct(&mut sm, 1.0),
    );
    println!(
        " read back: {} records; {dups} duplicates, {out_of_order} out of order, {missing} delivered but missing, {failed_but_written} reported failed but written",
        written.len()
    );
    let bad = dups > 0 || out_of_order > 0 || missing > 0;
    println!(" verdict: {}", if bad { "BROKEN (exactly-once or order)" } else { "exactly once, in order" });
    std::process::exit(if bad { 1 } else { 0 });
}
