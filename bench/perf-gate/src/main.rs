//! The end-to-end performance gate's workloads: fastka's own producer and consumer against a real
//! broker, each run a process of its own, printing one JSON line (`gate.py` runs them, base against
//! head, and compares). What the instruction-count gate (`benches/gate.rs`) cannot see is measured
//! here: the threads, the waits, the syscalls, the locks: a consumer that sleeps where it should
//! wait, a producer that encodes under its lock.
//!
//!   perf-gate fill    <brokers> <topic> <codec> <n>     a backlog for the consume workloads
//!   perf-gate <workload> <brokers> <topic> <n>          one run (see WORKLOADS)
use fastka::consumer::{Assignment, Consumer, Offset, Polled};
use fastka::producer::{Deliveries, Delivery, Producer, Record};
use fastka::Config;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SIZE: usize = 100;
const PARTITIONS: i32 = 3;
const T: Duration = Duration::from_secs(120);

/// name, what it measures
const WORKLOADS: &[(&str, &str)] = &[
    ("produce_none", "keyless records, no compression: msgs/s and CPU per message"),
    ("produce_lz4", "keyless records, lz4"),
    ("produce_keyed", "64 keys, lz4: the key partitioner and per-partition batches"),
    ("consume_none", "a backlog from the start of 3 partitions, no compression"),
    ("consume_lz4", "the same, lz4"),
    ("latency", "produce to consume at 5,000 msgs/s on one partition, linger 0: p50, p99"),
];

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a.get(1).map(String::as_str) {
        Some("list") | None => WORKLOADS.iter().for_each(|(n, _)| println!("{n}")),
        Some("fill") => {
            let n = a[5].parse().unwrap();
            let got = produce(&a[2], &a[3], &a[4], n, false);
            assert_eq!(got, n);
        }
        Some(w) => {
            let (brokers, topic, n): (&str, &str, usize) = (&a[2], &a[3], a[4].parse().unwrap());
            let (cpu0, t0) = (cpu(), Instant::now());
            let mut extra = String::new();
            let got = match w {
                "produce_none" => produce(brokers, topic, "none", n, false),
                "produce_lz4" => produce(brokers, topic, "lz4", n, false),
                "produce_keyed" => produce(brokers, topic, "lz4", n, true),
                "consume_none" | "consume_lz4" => consume(brokers, topic, n),
                "latency" => {
                    let (p50, p99) = latency(brokers, topic, n);
                    extra = format!(r#","p50_us":{p50:.1},"p99_us":{p99:.1}"#);
                    n
                }
                w => panic!("no workload {w}"),
            };
            let (secs, cpu_s) = (t0.elapsed().as_secs_f64(), cpu() - cpu0);
            assert_eq!(got, n, "{w}: {got} of {n} messages");
            println!(
                r#"{{"workload":"{w}","n":{n},"msgs_per_s":{:.0},"cpu_us_per_msg":{:.4}{extra}}}"#,
                n as f64 / secs,
                cpu_s / n as f64 * 1e6
            );
        }
    }
}

fn cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

fn cfg(brokers: &str) -> Config {
    let mut c = Config::new();
    c.set("bootstrap.servers", brokers).set("isolation.level", "read_uncommitted").set("enable.partition.eof", "true");
    c
}

#[derive(Default)]
struct Count(AtomicUsize, AtomicUsize);
impl Deliveries for Count {
    type Opaque = ();
    fn delivery(&self, d: &Delivery<'_>, _: ()) {
        if d.error.is_none() { &self.0 } else { &self.1 }.fetch_add(1, Relaxed);
    }
}

fn produce(brokers: &str, topic: &str, codec: &str, n: usize, keyed: bool) -> usize {
    let mut c = cfg(brokers);
    c.set("linger.ms", "5").set("compression.type", codec);
    let p = Producer::new(&c, Count::default()).unwrap();
    let keys: Vec<String> = (0..64).map(|k| format!("key-{k}")).collect();
    let mut v = vec![b'x'; SIZE];
    for i in 0..n {
        v[..16].copy_from_slice(format!("{i:016}").as_bytes());
        loop {
            let r = Record::to(topic).value(&v);
            match p.send(if keyed { r.key(keys[i % 64].as_bytes()) } else { r }, ()) {
                Ok(()) => break,
                Err((fastka::Error::QueueFull, ())) => drop(p.poll(Duration::from_millis(1))),
                Err((e, _)) => panic!("{e}"),
            }
        }
        if i % 1024 == 0 {
            p.poll(Duration::ZERO);
        }
    }
    p.flush(T).unwrap();
    assert_eq!(p.context().1.load(Relaxed), 0, "failed deliveries");
    p.context().0.load(Relaxed)
}

fn consume(brokers: &str, topic: &str, n: usize) -> usize {
    let assign = (0..PARTITIONS).map(|p| Assignment::new(topic, p, Offset::Beginning)).collect();
    let mut k = Consumer::from_config(&cfg(brokers), assign).unwrap();
    let (mut got, mut bytes) = (0, 0);
    let until = Instant::now() + T;
    while got < n && Instant::now() < until {
        if let Some((_, Polled::Msg(m))) = k.next_event(Duration::from_millis(100), &mut |_| {}).unwrap() {
            bytes += m.value.map_or(0, |v| v.len());
            got += 1;
        }
    }
    assert_eq!(bytes, got * SIZE);
    got
}

/// `n` messages at 5,000 a second on partition 0, each carrying when it was sent (one clock: the
/// same process), read back as they come: p50 and p99 (µs).
fn latency(brokers: &str, topic: &str, n: usize) -> (f64, f64) {
    let ready = Arc::new(AtomicBool::new(false));
    let epoch = Instant::now();
    let reader = {
        let (brokers, topic, ready) = (brokers.to_string(), topic.to_string(), ready.clone());
        std::thread::spawn(move || {
            let mut c = cfg(&brokers);
            c.set("fetch.wait.max.ms", "100");
            let mut k = Consumer::from_config(&c, vec![Assignment::new(topic, 0, Offset::End)]).unwrap();
            let mut lat = Vec::with_capacity(n);
            let until = Instant::now() + T;
            while lat.len() < n && Instant::now() < until {
                match k.next_event(Duration::from_millis(100), &mut |_| {}).unwrap() {
                    Some((_, Polled::Msg(m))) => {
                        let sent = u64::from_be_bytes(m.value.unwrap()[..8].try_into().unwrap());
                        lat.push((epoch.elapsed().as_nanos() as u64).saturating_sub(sent));
                    }
                    Some((_, Polled::Eof)) => ready.store(true, Relaxed),
                    _ => {}
                }
            }
            lat
        })
    };
    while !ready.load(Relaxed) {
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut c = cfg(brokers);
    c.set("linger.ms", "0");
    let p = Producer::new(&c, Count::default()).unwrap();
    let start = Instant::now();
    for i in 0..n as u64 {
        let due = start + Duration::from_nanos(i * 200_000);
        while Instant::now() < due {
            p.poll(Duration::ZERO);
            std::hint::spin_loop();
        }
        let mut v = (epoch.elapsed().as_nanos() as u64).to_be_bytes().to_vec();
        v.resize(SIZE, 0);
        p.send(Record::to(topic).partition(0).value(&v), ()).map_err(|(e, _)| e).unwrap();
    }
    p.flush(T).unwrap();
    let mut lat = reader.join().unwrap();
    assert_eq!(lat.len(), n, "latency: {} of {n} read back", lat.len());
    lat.sort_unstable();
    let q = |f: f64| lat[((lat.len() as f64 * f) as usize).min(lat.len() - 1)] as f64 / 1000.0;
    (q(0.5), q(0.99))
}
