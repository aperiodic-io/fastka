//! One workload, one client, one process: `compare <produce|consume> <client> <brokers> <topic> <partitions> <n> <size> [codec]`.
//! Prints one JSON line: wall time, CPU time (user + system, every thread of the process) and peak RSS.
//! `run.sh` drives it. Every client is given the same batch and fetch sizes where it has the setting.
use std::time::{Duration, Instant};

struct Args {
    brokers: String,
    topic: String,
    partitions: i32,
    n: usize,
    size: usize,
    codec: String,
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 8 {
        eprintln!("usage: compare <produce|consume> <client> <brokers> <topic> <partitions> <n> <size> [codec]");
        std::process::exit(2);
    }
    let args = Args {
        brokers: a[3].clone(),
        topic: a[4].clone(),
        partitions: a[5].parse().unwrap(),
        n: a[6].parse().unwrap(),
        size: a[7].parse().unwrap(),
        codec: a.get(8).cloned().unwrap_or_else(|| "none".into()),
    };
    let (workload, client) = (a[1].as_str(), a[2].as_str());
    let (cpu0, t0) = (cpu(), Instant::now());
    let got = match (workload, client) {
        ("produce", "fastka") => fastka_produce(&args),
        ("produce", "rdkafka") => rdkafka_produce(&args),
        ("produce", "rskafka") => rskafka_produce(&args),
        ("produce", "kafka-rust") => kafka_rust_produce(&args),
        ("consume", "fastka") => fastka_consume(&args),
        ("consume", "rdkafka") => rdkafka_consume(&args),
        ("consume", "rskafka") => rskafka_consume(&args),
        ("consume", "kafka-rust") => kafka_rust_consume(&args),
        _ => panic!("unknown workload {workload} or client {client}"),
    };
    let (secs, cpu_s) = (t0.elapsed().as_secs_f64(), cpu() - cpu0);
    assert_eq!(got, args.n, "{client}: {got} messages of {}", args.n);
    println!(
        r#"{{"workload":"{workload}","client":"{client}","codec":"{}","n":{},"size":{},"secs":{secs:.4},"cpu":{cpu_s:.4},"rss_kb":{}}}"#,
        args.codec,
        args.n,
        args.size,
        rss_kb()
    );
}

fn usage() -> libc::rusage {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    u
}

fn cpu() -> f64 {
    let u = usage();
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

fn rss_kb() -> i64 {
    usage().ru_maxrss
}

fn value(i: usize, size: usize) -> Vec<u8> {
    let mut v = format!("{i:016}").into_bytes();
    v.resize(size, b'x');
    v
}

const T: Duration = Duration::from_secs(120);

/// What the harness batches for the clients without a batching producer: 10,000 messages or what
/// fits a broker's default 1 MB batch, as fastka's and librdkafka's batches are bounded.
fn per_batch(size: usize) -> usize {
    (900_000 / (size + 24)).clamp(1, 10_000)
}

// ---- fastka ----

struct Count(std::sync::atomic::AtomicUsize, std::sync::atomic::AtomicUsize);
impl fastka::producer::Deliveries for Count {
    type Opaque = ();
    fn delivery(&self, d: &fastka::producer::Delivery<'_>, _: ()) {
        let c = if d.error.is_none() { &self.0 } else { &self.1 };
        c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn fastka_cfg(a: &Args) -> fastka::Config {
    let mut c = fastka::Config::new();
    c.set("bootstrap.servers", &a.brokers).set("linger.ms", "5").set("compression.type", &a.codec);
    c.set("isolation.level", "read_uncommitted").set("enable.partition.eof", "true");
    c
}

fn fastka_produce(a: &Args) -> usize {
    let p = fastka::producer::Producer::new(&fastka_cfg(a), Count(Default::default(), Default::default())).unwrap();
    for i in 0..a.n {
        let v = value(i, a.size);
        loop {
            match p.send(fastka::producer::Record::to(&a.topic).value(&v), ()) {
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
    assert_eq!(p.context().1.load(std::sync::atomic::Ordering::Relaxed), 0, "failed deliveries");
    p.context().0.load(std::sync::atomic::Ordering::Relaxed)
}

fn fastka_consume(a: &Args) -> usize {
    use fastka::consumer::{Assignment, Consumer, Offset, Polled};
    let cfg = fastka_cfg(a);
    let assign = (0..a.partitions).map(|p| Assignment::new(a.topic.as_str(), p, Offset::Beginning)).collect();
    let mut k = Consumer::from_config(&cfg, assign).unwrap();
    let (mut n, mut bytes) = (0, 0);
    let until = Instant::now() + T;
    while n < a.n && Instant::now() < until {
        match k.next_event(Duration::from_millis(100), &mut |n| eprintln!("notice: {n}")).unwrap() {
            Some((_, Polled::Msg(m))) => {
                bytes += m.value.map_or(0, |v| v.len());
                n += 1;
            }
            _ => {}
        }
    }
    assert_eq!(bytes, n * a.size);
    n
}

// ---- rdkafka (librdkafka) ----

fn rdkafka_cfg(a: &Args) -> rdkafka::ClientConfig {
    let mut c = rdkafka::ClientConfig::new();
    c.set("bootstrap.servers", &a.brokers);
    c
}

fn rdkafka_produce(a: &Args) -> usize {
    use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    struct Ctx(AtomicUsize, AtomicUsize);
    impl rdkafka::ClientContext for Ctx {}
    impl ProducerContext for Ctx {
        type DeliveryOpaque = ();
        fn delivery(&self, r: &DeliveryResult<'_>, _: ()) {
            if r.is_ok() { &self.0 } else { &self.1 }.fetch_add(1, Relaxed);
        }
    }
    let p: BaseProducer<Ctx> = rdkafka_cfg(a)
        .set("enable.idempotence", "true")
        .set("linger.ms", "5")
        .set("batch.size", "1000000")
        .set("batch.num.messages", "10000")
        .set("compression.type", &a.codec)
        .create_with_context(Ctx(AtomicUsize::new(0), AtomicUsize::new(0)))
        .unwrap();
    for i in 0..a.n {
        let v = value(i, a.size);
        let mut r = BaseRecord::<(), [u8]>::to(&a.topic).payload(&v[..]);
        loop {
            match p.send(r) {
                Ok(()) => break,
                Err((rdkafka::error::KafkaError::MessageProduction(rdkafka::types::RDKafkaErrorCode::QueueFull), back)) => {
                    p.poll(Duration::from_millis(1));
                    r = back;
                }
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

fn rdkafka_consume(a: &Args) -> usize {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::Message;
    let c: BaseConsumer = rdkafka_cfg(a)
        .set("group.id", "fastka-compare")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .create()
        .unwrap();
    let mut tpl = rdkafka::TopicPartitionList::new();
    for p in 0..a.partitions {
        tpl.add_partition_offset(&a.topic, p, rdkafka::Offset::Beginning).unwrap();
    }
    c.assign(&tpl).unwrap();
    let (mut n, mut bytes) = (0, 0);
    let until = Instant::now() + T;
    while n < a.n && Instant::now() < until {
        if let Some(m) = c.poll(Duration::from_millis(100)) {
            bytes += m.unwrap().payload().map_or(0, |v| v.len());
            n += 1;
        }
    }
    assert_eq!(bytes, n * a.size);
    n
}

// ---- rskafka (async, tokio) ----

fn rskafka_produce(a: &Args) -> usize {
    use rskafka::client::partition::{Compression, UnknownTopicHandling};
    use rskafka::record::Record;
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = rskafka::client::ClientBuilder::new(vec![a.brokers.clone()]).build().await.unwrap();
        let compression = match a.codec.as_str() {
            "none" => Compression::NoCompression,
            "lz4" => Compression::Lz4,
            c => panic!("rskafka: codec {c} not built in"),
        };
        // rskafka's PartitionClient sends what it is given as one batch: the harness batches as
        // the others do (10,000 messages or ~1 MB a batch), one batch of a partition in flight
        let mut tasks = vec![];
        for p in 0..a.partitions {
            let pc = client.partition_client(a.topic.clone(), p, UnknownTopicHandling::Retry).await.unwrap();
            let (n, size, parts) = (a.n, a.size, a.partitions as usize);
            tasks.push(tokio::spawn(async move {
                let mut done = 0;
                let per = per_batch(size);
                let mut batch = Vec::with_capacity(per);
                for i in (p as usize..n).step_by(parts) {
                    batch.push(Record {
                        key: None,
                        value: Some(value(i, size)),
                        headers: Default::default(),
                        timestamp: chrono::Utc::now(),
                    });
                    if batch.len() == per || i + parts >= n {
                        done += pc.produce(std::mem::take(&mut batch), compression).await.unwrap().len();
                    }
                }
                done
            }));
        }
        let mut n = 0;
        for t in tasks {
            n += t.await.unwrap();
        }
        n
    })
}

fn rskafka_consume(a: &Args) -> usize {
    use rskafka::client::partition::UnknownTopicHandling;
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = rskafka::client::ClientBuilder::new(vec![a.brokers.clone()]).build().await.unwrap();
        let mut tasks = vec![];
        for p in 0..a.partitions {
            let pc = client.partition_client(a.topic.clone(), p, UnknownTopicHandling::Retry).await.unwrap();
            tasks.push(tokio::spawn(async move {
                let (mut offset, mut n, mut bytes) = (0, 0, 0);
                loop {
                    let (records, high) = pc.fetch_records(offset, 1..1_048_576, 500).await.unwrap();
                    for r in records {
                        bytes += r.record.value.map_or(0, |v| v.len());
                        n += 1;
                        offset = r.offset + 1;
                    }
                    if offset >= high {
                        return (n, bytes);
                    }
                }
            }));
        }
        let (mut n, mut bytes) = (0, 0);
        for t in tasks {
            let (k, b) = t.await.unwrap();
            (n, bytes) = (n + k, bytes + b);
        }
        assert_eq!(bytes, n * a.size);
        n
    })
}

// ---- kafka-rust (synchronous) ----

fn kafka_rust_produce(a: &Args) -> usize {
    use kafka::producer::{Producer, Record, RequiredAcks};
    assert_eq!(a.codec, "none", "kafka-rust: built without codecs");
    let mut p = Producer::from_hosts(vec![a.brokers.clone()])
        .with_ack_timeout(Duration::from_secs(30))
        .with_required_acks(RequiredAcks::All)
        .create()
        .unwrap();
    // synchronous: each send_all is one request per leader, answered before it returns
    let per = per_batch(a.size);
    let (mut done, mut batch) = (0, Vec::with_capacity(per));
    for i in 0..a.n {
        batch.push(value(i, a.size));
        if batch.len() == per || i + 1 == a.n {
            let recs: Vec<_> = batch
                .iter()
                .enumerate()
                .map(|(k, v)| Record::from_value(&a.topic, &v[..]).with_partition(((i + k) % a.partitions as usize) as i32))
                .collect();
            for r in p.send_all(&recs).unwrap() {
                for pc in r.partition_confirms {
                    pc.offset.unwrap_or_else(|e| panic!("kafka-rust: partition {}: {e:?}", pc.partition));
                }
            }
            done += recs.len();
            batch.clear();
        }
    }
    done
}

fn kafka_rust_consume(a: &Args) -> usize {
    use kafka::consumer::{Consumer, FetchOffset};
    let parts: Vec<i32> = (0..a.partitions).collect();
    let mut c = Consumer::from_hosts(vec![a.brokers.clone()])
        .with_topic_partitions(a.topic.clone(), &parts)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_fetch_max_bytes_per_partition(1_048_576)
        .with_fetch_max_wait_time(Duration::from_millis(500))
        .create()
        .unwrap();
    let (mut n, mut bytes) = (0, 0);
    let until = Instant::now() + T;
    while n < a.n && Instant::now() < until {
        for ms in c.poll().unwrap().iter() {
            for m in ms.messages() {
                bytes += m.value.len();
                n += 1;
            }
        }
    }
    assert_eq!(bytes, n * a.size);
    n
}
