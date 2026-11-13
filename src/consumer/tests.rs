#![allow(clippy::type_complexity)] // the tests' tuples say what each field is where they are made
use super::*;
use crate::config::Config;
use crate::records::{Out, Stamp};
use crate::testkit::{config, metadata, Answer, Broker, Req};
use crate::wire::Cur;
use crate::wire::Put;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// A partition's log: its batches (base offset, bytes), its log start, last stable offset (None:
/// the end), aborted transactions.
#[derive(Default, Clone)]
struct Log {
    batches: Vec<(i64, Vec<u8>)>,
    start: i64,
    lso: Option<i64>,
    aborted: Vec<(i64, i64)>,
}

impl Log {
    fn end(&self) -> i64 {
        self.batches
            .last()
            .map_or(self.start, |(base, b)| base + i32::from_be_bytes(b[23..27].try_into().unwrap()) as i64 + 1)
    }
    fn push(&mut self, codec: i16, vals: &[&str]) {
        let base = self.end();
        self.batches.push((base, batch(base, codec, vals)));
    }
}

fn batch(base: i64, codec: i16, vals: &[&str]) -> Vec<u8> {
    let h = vec![("h".to_string(), Some(b"hv".to_vec()))];
    let recs: Vec<Out<'_>> = vals
        .iter()
        .map(|v| Out { timestamp: 1000 + base, key: None, value: Some(v.as_bytes()), headers: &h })
        .collect();
    let mut out = vec![];
    records::write(&mut out, &recs, Stamp { producer_id: -1, epoch: -1, base_sequence: -1 }, codec, &mut vec![])
        .unwrap();
    out[..8].copy_from_slice(&base.to_be_bytes());
    out
}

type Logs = Arc<Mutex<HashMap<(String, i32), Log>>>;
/// A fault: given a Fetch request and how many came before it, an answer instead of the log's.
type Fault = Box<dyn Fn(&Req, usize) -> Option<Answer> + Send + Sync>;

/// A broker leading every partition of `logs`, serving them as a broker does: from the batch
/// holding the Fetch's offset, whole batches up to the partition's max bytes (the first whole
/// whatever its size).
fn log_broker(logs: Logs, fault: Fault) -> Broker {
    Broker::start(log_handler(logs, fault))
}

fn log_handler(logs: Logs, fault: Fault) -> impl Fn(&Req) -> Answer + Send + Sync + 'static {
    let fetches = AtomicUsize::new(0);
    move |r: &Req| match r.api {
        wire::METADATA => {
            let l = logs.lock().unwrap();
            let mut topics: HashMap<&str, Vec<(i32, i32)>> = HashMap::new();
            for (t, p) in l.keys() {
                topics.entry(t.as_str()).or_default().push((*p, 0));
            }
            let ts: Vec<(&str, &[(i32, i32)])> = topics.iter().map(|(t, ps)| (*t, &ps[..])).collect();
            Answer::Body(metadata(&[(0, &r.me)], &ts))
        }
        wire::LIST_OFFSETS => {
            let l = logs.lock().unwrap();
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
                    let log = &l[&(t.clone(), p)];
                    out.i32(p);
                    out.i16(0);
                    out.i64(-1);
                    out.i64(if ts == wire::EARLIEST { log.start } else { log.lso.unwrap_or(log.end()) });
                }
            }
            Answer::Body(out)
        }
        wire::FETCH => {
            let n = fetches.fetch_add(1, Ordering::SeqCst);
            if let Some(a) = fault(r, n) {
                return a;
            }
            let l = logs.lock().unwrap();
            let mut c = Cur::new(&r.body);
            for _ in 0..4 {
                c.i32().unwrap();
            }
            let committed = c.i8().unwrap() == 1;
            c.i32().unwrap(); // session
            c.i32().unwrap();
            let mut out = vec![];
            out.i32(0); // throttle
            out.i16(0);
            out.i32(0); // session
            let nt = c.i32().unwrap();
            out.i32(nt);
            for _ in 0..nt {
                let t = c.str().unwrap().to_string();
                out.str(&t);
                let np = c.i32().unwrap();
                out.i32(np);
                for _ in 0..np {
                    let p = c.i32().unwrap();
                    c.i32().unwrap(); // leader epoch
                    let offset = c.i64().unwrap();
                    c.i64().unwrap(); // log start
                    let max = c.i32().unwrap();
                    let log = &l[&(t.clone(), p)];
                    out.i32(p);
                    let limit = if committed { log.lso.unwrap_or(log.end()) } else { log.end() };
                    if offset < log.start || offset > log.end() {
                        out.i16(1);
                        out.i64(log.end());
                        out.i64(log.end());
                        out.i64(log.start);
                        out.i32(-1);
                        out.i32(-1);
                        out.i32(-1);
                        continue;
                    }
                    out.i16(0);
                    out.i64(log.end());
                    out.i64(log.lso.unwrap_or(log.end()));
                    out.i64(log.start);
                    out.count(log.aborted.len());
                    for (pid, first) in &log.aborted {
                        out.i64(*pid);
                        out.i64(*first);
                    }
                    out.i32(-1); // preferred replica
                    let mut recs = vec![];
                    for (base, b) in &log.batches {
                        let next = base + i32::from_be_bytes(b[23..27].try_into().unwrap()) as i64 + 1;
                        if next <= offset || *base >= limit {
                            continue;
                        }
                        if !recs.is_empty() && recs.len() + b.len() > max as usize {
                            break;
                        }
                        recs.extend(b);
                    }
                    out.bytes(&recs);
                }
            }
            Answer::Body(out)
        }
        _ => Answer::Drop,
    }
}

/// The partitions a Fetch asked for: (topic, partition, offset).
fn fetched(r: &Req) -> Vec<(String, i32, i64)> {
    let mut c = Cur::new(&r.body);
    for _ in 0..4 {
        c.i32().unwrap();
    }
    c.i8().unwrap();
    c.i32().unwrap();
    c.i32().unwrap();
    let mut out = vec![];
    for _ in 0..c.i32().unwrap() {
        let t = c.str().unwrap().to_string();
        for _ in 0..c.i32().unwrap() {
            let p = c.i32().unwrap();
            c.i32().unwrap();
            let o = c.i64().unwrap();
            c.i64().unwrap();
            c.i32().unwrap();
            out.push((t.clone(), p, o));
        }
    }
    out
}

/// A Fetch v11 response of one partition: (topic, partition, error, high, records).
fn fetch_answer(t: &str, p: i32, error: i16, high: i64, records: Option<&[u8]>) -> Vec<u8> {
    let mut out = vec![];
    out.i32(0);
    out.i16(0);
    out.i32(0);
    out.i32(1);
    out.str(t);
    out.i32(1);
    out.i32(p);
    out.i16(error);
    out.i64(high);
    out.i64(high);
    out.i64(0);
    out.i32(-1);
    out.i32(-1);
    match records {
        Some(r) => out.bytes(r),
        None => out.i32(-1),
    }
    out
}

fn no_fault() -> Fault {
    Box::new(|_, _| None)
}

fn logs(parts: &[(&str, i32, Log)]) -> Logs {
    Arc::new(Mutex::new(parts.iter().map(|(t, p, l)| ((t.to_string(), *p), l.clone())).collect()))
}

fn settings(c: &Config) -> ConsumerSettings {
    c.consumer().unwrap()
}

fn consumer(b: &Broker, extra: &[(&str, &str)], parts: Vec<Assignment>) -> Consumer {
    let mut c = config(&b.addr);
    c.set("enable.partition.eof", "true").set("fetch.wait.max.ms", "10").set("reconnect.backoff.ms", "1");
    for (k, v) in extra {
        c.set(*k, *v);
    }
    Consumer::new(Arc::new(Client::new(&c).unwrap()), settings(&c), parts).unwrap()
}

/// Every event until each partition said its end `ends` times, within 5 s: (partition, value or
/// "EOF"), and what `lost` heard.
fn drain(c: &mut Consumer, ends: usize) -> (Vec<(usize, String)>, Vec<String>) {
    let (mut got, mut heard, mut eofs) = (vec![], vec![], HashMap::new());
    let until = Instant::now() + Duration::from_secs(5);
    while eofs.len() < c.parts.len() || eofs.values().any(|n| *n < ends) {
        assert!(Instant::now() < until, "no end: {got:?} {heard:?}");
        match c.next_event(Duration::from_millis(50), &mut |w| heard.push(w.to_string())).unwrap() {
            Some((i, Polled::Msg(m))) => got.push((i, String::from_utf8(m.value.unwrap()).unwrap())),
            Some((i, Polled::Eof)) => {
                *eofs.entry(i).or_insert(0) += 1;
                got.push((i, "EOF".into()));
            }
            _ => {}
        }
    }
    (got, heard)
}

fn vals(got: &[(usize, String)], i: usize) -> Vec<&str> {
    got.iter().filter(|g| g.0 == i).map(|g| g.1.as_str()).collect()
}

#[test]
fn every_record_of_every_partition_then_each_end_once() {
    let (mut a, mut b) = (Log::default(), Log::default());
    for codec in 0..5 {
        a.push(codec, &[&format!("a{codec}x"), &format!("a{codec}y")]);
        b.push(codec, &[&format!("b{codec}")]);
    }
    let br = log_broker(logs(&[("t", 0, a), ("t", 1, b)]), no_fault());
    let mut c =
        consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning), Assignment::new("t", 1, Offset::At(2))]);
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["a0x", "a0y", "a1x", "a1y", "a2x", "a2y", "a3x", "a3y", "a4x", "a4y", "EOF"]);
    assert_eq!(vals(&got, 1), ["b2", "b3", "b4", "EOF"], "from offset 2");
    assert!(heard.is_empty(), "{heard:?}");
    // nothing more: no second end until something new arrives
    assert_eq!(c.next_event(Duration::from_millis(100), &mut |_| {}).unwrap(), None);
    assert_eq!(c.position(0), Some(10));
    assert_eq!(c.partitions().collect::<Vec<_>>(), [("t", 0), ("t", 1)]);
    assert_eq!(c.brokers.len(), 1, "one connection to the leader of both");
    assert_eq!(format!("{c:?}"), "Consumer { parts: 2, brokers: 1 }");
    c.pump(&mut |_| {}).unwrap();
    assert!(c.lookup.is_none(), "nothing to look up once every partition is resolved");
}

#[test]
fn headers_and_timestamps_come_with_the_messages() {
    let mut a = Log::default();
    a.push(4, &["v"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let Some((0, Polled::Msg(m))) = c.next_event(Duration::from_secs(5), &mut |_| {}).unwrap() else { panic!() };
    assert_eq!((m.offset, m.timestamp, m.header("h")), (0, 1000, Some(&b"hv"[..])));
}

/// From its end: only what is written after the consumer looked.
#[test]
fn from_the_end_only_what_comes_after() {
    let mut a = Log::default();
    a.push(0, &["old1", "old2"]);
    let l = logs(&[("t", 0, a)]);
    let br = log_broker(l.clone(), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::End)]);
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["EOF"]);
    l.lock().unwrap().get_mut(&("t".into(), 0)).unwrap().push(2, &["new"]);
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["new", "EOF"], "the end said again once past new records");
}

/// No end without `enable.partition.eof`.
#[test]
fn no_end_unless_asked() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[("enable.partition.eof", "false")], vec![Assignment::new("t", 0, Offset::Beginning)]);
    assert!(matches!(c.next_event(Duration::from_secs(5), &mut |_| {}).unwrap(), Some((0, Polled::Msg(_)))));
    assert_eq!(c.next_event(Duration::from_millis(200), &mut |_| {}).unwrap(), None);
}

/// A dropped connection, a moved leader and a broker that stops answering: each retried from
/// the next offset not given, nothing lost or given twice, each heard by `lost`.
#[test]
fn faults_are_retried_from_the_next_offset() {
    let mut a = Log::default();
    for i in 0..6 {
        a.push((i % 5) as i16, &[&format!("m{i}")]);
    }
    let l = logs(&[("t", 0, a)]);
    let fault: Fault = Box::new(|r, n| match n {
        1 => Some(Answer::Drop),
        3 => {
            let _ = r;
            Some(Answer::Body(fetch_answer("t", 0, 6, -1, None))) // NOT_LEADER_OR_FOLLOWER
        }
        5 => Some(Answer::Silence),
        _ => None,
    });
    let br = log_broker(l, fault);
    let mut c = consumer(
        &br,
        &[("fetch.message.max.bytes", "1"), ("socket.timeout.ms", "300")], // a batch a Fetch
        vec![Assignment::new("t", 0, Offset::Beginning)],
    );
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["m0", "m1", "m2", "m3", "m4", "m5", "EOF"]);
    assert!(heard.iter().any(|h| h.contains("closed")), "{heard:?}");
    assert!(heard.iter().any(|h| h.contains("NOT_LEADER_OR_FOLLOWER")), "{heard:?}");
    assert!(heard.iter().any(|h| h.contains("no answer to a Fetch")), "{heard:?}");
    // each Fetch asked from the offset after the last whole batch given: none skipped
    let asked: Vec<i64> = br.requests(wire::FETCH).iter().flat_map(fetched).map(|f| f.2).collect();
    assert!(asked.windows(2).all(|w| w[1] >= w[0] - 1), "{asked:?}");
    assert_eq!(asked[0], 0);
}

/// An offset out of range goes on from the oldest record (`earliest`), the newest (`latest`), or
/// is an error (`error`), from which a `seek` goes on.
#[test]
fn an_offset_out_of_range_follows_auto_offset_reset() {
    let mut a = Log { start: 10, ..Default::default() };
    a.push(0, &["s1", "s2"]);
    let l = logs(&[("t", 0, a)]);
    let br = log_broker(l.clone(), no_fault());
    let mut c = consumer(&br, &[("auto.offset.reset", "earliest")], vec![Assignment::new("t", 0, Offset::At(3))]);
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["s1", "s2", "EOF"]);
    assert_eq!(heard, ["t/0: offset 3 is out of range: reset to 10 (auto.offset.reset)"]);
    let mut c = consumer(&br, &[("auto.offset.reset", "latest")], vec![Assignment::new("t", 0, Offset::At(99))]);
    let mut notices = vec![];
    while !matches!(c.next_event(Duration::from_millis(10), &mut |n| notices.push(n)).unwrap(), Some((0, Polled::Eof)))
    {
    }
    let reset = Notice::Reset { topic: "t".into(), partition: 0, from: 99, to: 12 };
    assert_eq!(notices, [reset]);
    let mut c = consumer(&br, &[("auto.offset.reset", "error")], vec![Assignment::new("t", 0, Offset::At(3))]);
    let e = (0..1000).find_map(|_| c.next_event(Duration::from_millis(10), &mut |_| {}).err()).unwrap();
    assert_eq!(e.code(), Some(Code::OFFSET_OUT_OF_RANGE));
    assert_eq!(c.position(0), Some(3), "where it was");
    c.seek(0, Offset::Beginning).unwrap();
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["s1", "s2", "EOF"]);
}

/// A paused partition is not fetched until it is resumed; a full one (`queued.min.messages`)
/// not until it is drained.
#[test]
fn paused_and_full_partitions_are_not_fetched() {
    let (mut a, mut b) = (Log::default(), Log::default());
    a.push(0, &["a1", "a2"]);
    a.push(0, &["a3", "a4"]);
    b.push(0, &["b1"]);
    let br = log_broker(logs(&[("t", 0, a), ("t", 1, b)]), no_fault());
    let mut c = consumer(
        &br,
        &[("queued.min.messages", "2"), ("fetch.message.max.bytes", "1")], // a batch a Fetch
        vec![Assignment::new("t", 0, Offset::Beginning), Assignment::new("t", 1, Offset::Beginning)],
    );
    c.pause(|_, p| p == 1);
    let until = Instant::now() + Duration::from_secs(5);
    while c.parts[0].queue.len() < 2 {
        assert!(Instant::now() < until);
        c.pump(&mut |_| {}).unwrap();
    }
    for _ in 0..30 {
        c.pump(&mut |_| {}).unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(c.parts[0].queue.len(), 2, "full at queued.min.messages: not fetched again until below it");
    assert!(c.parts[1].queue.is_empty(), "paused");
    let fetched_1 = br.requests(wire::FETCH).iter().any(|r| fetched(r).iter().any(|f| f.1 == 1));
    assert!(!fetched_1, "a paused partition is in no Fetch");
    c.resume(|_, p| p == 1);
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["a1", "a2", "a3", "a4", "EOF"]);
    assert_eq!(vals(&got, 1), ["b1", "EOF"]);
}

/// A corrupt batch (its CRC) is fetched again on a new connection; one corrupt every time is an
/// error after a few tries, never records read from it.
#[test]
fn a_corrupt_batch_is_fetched_again_then_an_error() {
    let mut a = Log::default();
    a.push(0, &["good"]);
    let corrupt = |r: &Req| {
        let b = {
            let mut b = batch(0, 0, &["bad"]);
            let n = b.len();
            b[n - 2] ^= 1;
            b
        };
        let _ = r;
        Answer::Body(fetch_answer("t", 0, 0, 1, Some(&b)))
    };
    let br = log_broker(logs(&[("t", 0, a.clone())]), Box::new(move |r, n| (n == 0).then(|| corrupt(r))));
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["good", "EOF"]);
    assert!(heard.iter().any(|h| h.contains("CRC")), "{heard:?}");

    let br = log_broker(logs(&[("t", 0, a)]), Box::new(move |r, _| Some(corrupt(r))));
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let (mut seen, mut heard) = (vec![], vec![]);
    let e = (0..5000)
        .find_map(|_| match c.next_event(Duration::from_millis(5), &mut |w| heard.push(w.to_string())) {
            Err(e) => Some(e),
            Ok(Some((_, Polled::Msg(m)))) => {
                seen.push(m);
                None
            }
            _ => None,
        })
        .unwrap();
    assert!(e.to_string().contains("CRC"), "{e}");
    assert!(seen.is_empty(), "no record of a corrupt batch is given");
    assert_eq!(heard.iter().filter(|h| h.contains("CRC")).count(), CORRUPT_RETRIES as usize, "{heard:?}");
}

/// Reading committed, an aborted transaction's records are skipped and the end is the last
/// stable offset (an open transaction past it), as librdkafka reads committed.
#[test]
fn reading_committed_stops_at_the_last_stable_offset() {
    let mut a = Log::default();
    a.push(0, &["c1", "c2"]);
    a.push(0, &["open"]);
    a.lso = Some(2);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["c1", "c2", "EOF"], "the end at the LSO, before the open transaction");
    let r = br.requests(wire::FETCH);
    assert_eq!(r[0].body[16], 1, "read_committed asked of the broker");
}

/// Reading uncommitted, the end is the high watermark, past the last stable offset.
#[test]
fn reading_uncommitted_ends_at_the_high_watermark() {
    let mut a = Log::default();
    a.push(0, &["c1", "c2"]);
    a.push(0, &["open"]);
    a.lso = Some(2);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(
        &br,
        &[("isolation.level", "read_uncommitted"), ("fetch.message.max.bytes", "1")], // a batch a Fetch
        vec![Assignment::new("t", 0, Offset::Beginning)],
    );
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["c1", "c2", "open", "EOF"], "no end at the LSO");
}

/// A leader that cannot be reached (a SYN dropped) is connected to off the caller's thread:
/// `pump` returns at once while it tries.
#[test]
fn connecting_never_blocks_the_caller() {
    let b = Broker::start(|r| match r.api {
        // the leader is an address nobody answers (TEST-NET-1, RFC 5737)
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me), (1, "192.0.2.1:9092")], &[("t", &[(0, 1)])])),
        wire::LIST_OFFSETS => {
            let mut out = vec![];
            out.i32(0);
            out.i32(1);
            out.str("t");
            out.i32(1);
            out.i32(0);
            out.i16(0);
            out.i64(-1);
            out.i64(0);
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let mut c = consumer(&b, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    c.pump(&mut |_| {}).unwrap(); // the lookups (blocking, bounded) and the connect started
    let t = Instant::now();
    for _ in 0..100 {
        c.pump(&mut |_| {}).unwrap();
    }
    assert!(t.elapsed() < Duration::from_millis(100), "{:?}", t.elapsed());
}

/// The leaders unknown for a while (the metadata failing): looked up again with a growing wait,
/// then fetched once known.
#[test]
fn leaders_are_looked_up_with_backoff() {
    let fail = Arc::new(AtomicUsize::new(3));
    let f2 = fail.clone();
    let mut a = Log::default();
    a.push(0, &["x"]);
    let inner = log_broker(logs(&[("t", 0, a)]), no_fault());
    let ia = inner.addr.clone();
    let front = Broker::start(move |r| match r.api {
        wire::METADATA if f2.load(Ordering::SeqCst) > 0 => {
            f2.fetch_sub(1, Ordering::SeqCst);
            Answer::Body(metadata(&[(0, &ia)], &[("t", &[(0, -1)])])) // no leader yet
        }
        wire::METADATA => Answer::Body(metadata(&[(0, &ia)], &[("t", &[(0, 0)])])),
        _ => Answer::Drop,
    });
    let mut c = consumer(&front, &[], vec![Assignment::new("t", 0, Offset::At(0))]);
    let t = Instant::now();
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["x", "EOF"]);
    assert!(heard.is_empty() || heard.iter().all(|h| !h.contains("panicked")));
    assert!(c.lookup_backoff <= BACKOFF, "reset once found");
    assert!(t.elapsed() >= BACKOFF * 6, "three failed lookups waited 1, 2 and 4 backoffs: {:?}", t.elapsed());
}

/// The unaligned loop's order: each partition's run of up to `RUN` messages, then the next's.
#[test]
fn next_any_hands_over_runs() {
    let mut c = consumer(
        &Broker::start(|_| Answer::Drop),
        &[],
        vec![Assignment::new("t", 0, Offset::At(0)), Assignment::new("t", 1, Offset::At(0))],
    );
    for i in 0..(RUN as i64 + 5) {
        c.parts[0].queue.push_back(Message { offset: i, ..Default::default() });
        c.parts[1].queue.push_back(Message { offset: i, ..Default::default() });
    }
    let order: Vec<usize> = std::iter::from_fn(|| c.next_any().map(|(i, _)| i)).collect();
    let runs: Vec<(usize, usize)> = order.chunk_by(|a, b| a == b).map(|r| (r[0], r.len())).collect();
    assert_eq!(runs, [(0, RUN), (1, RUN), (0, 5), (1, 5)], "runs of RUN at most, each partition's in turn");
}

/// Polled with no wait at all (the standby's tail, every loop), it still fetches: what has
/// arrived is read, and given in time.
#[test]
fn a_zero_timeout_still_fetches() {
    let mut a = Log::default();
    a.push(0, &["z"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < until, "never read");
        if let Some((0, Polled::Msg(m))) = c.next_event(Duration::ZERO, &mut |_| {}).unwrap() {
            assert_eq!(m.value.as_deref(), Some(&b"z"[..]));
            break;
        }
    }
}

/// Over TLS, on the consumer's non-blocking sockets (a TLS record arrives in pieces, a write is
/// taken in part): every record read, as in plaintext, and its end. The sinks' read-back and the
/// standby's tail read a `sasl_ssl` cluster so.
#[test]
fn records_are_read_over_tls() {
    let mut a = Log::default();
    for i in 0..40 {
        // values large enough for a response of many TLS records
        a.push((i % 5) as i16, &[&format!("{i}-{}", "x".repeat(20_000)), &format!("{i}-small")]);
    }
    // the broker's metadata names it by its address: the certificate names that too
    let p = crate::testkit::pki(&["localhost", "127.0.0.1"]);
    let br = crate::testkit::tls_broker_with(&p, None, log_handler(logs(&[("t", 0, a)]), no_fault()));
    let port = br.addr.rsplit_once(':').unwrap().1;
    let mut c = config(&format!("localhost:{port}"));
    c.set("security.protocol", "ssl").set("ssl.ca.pem", p.ca_pem.clone());
    c.set("enable.partition.eof", "true").set("fetch.wait.max.ms", "10").set("fetch.message.max.bytes", "100000");
    let mut k = Consumer::from_config(&c, vec![Assignment::new("t", 0, Offset::Beginning)]).unwrap();
    let (got, heard) = drain(&mut k, 1);
    assert_eq!(vals(&got, 0).len(), 81, "{heard:?}");
    assert!(vals(&got, 0)[79].ends_with("-small") && vals(&got, 0)[80] == "EOF");
}

/// `poll` of one partition: its messages, its end once, then nothing.
#[test]
fn poll_gives_the_messages_then_the_end_once() {
    let mut a = Log::default();
    a.push(0, &["p1", "p2"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let (mut got, until) = (vec![], Instant::now() + Duration::from_secs(5));
    while !got.contains(&"EOF".to_string()) {
        assert!(Instant::now() < until, "{got:?}");
        c.pump(&mut |_| {}).unwrap();
        match c.poll(0) {
            Polled::Msg(m) => got.push(String::from_utf8(m.value.unwrap()).unwrap()),
            Polled::Eof => got.push("EOF".into()),
            Polled::Empty => {}
        }
    }
    assert_eq!(got, ["p1", "p2", "EOF"]);
    for _ in 0..20 {
        c.pump(&mut |_| {}).unwrap();
        assert_eq!(c.poll(0), Polled::Empty, "the end said once");
    }
}

/// The next Fetch is sent as soon as a response is in, before its records are read.
#[test]
fn the_next_fetch_is_pipelined() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let until = Instant::now() + Duration::from_secs(5);
    while !c.pump(&mut |_| {}).unwrap() {
        assert!(Instant::now() < until);
    }
    assert!(c.brokers[0].inflight.is_some(), "a Fetch in flight once the first is answered");
}

/// A response already in when the caller polls without waiting is read and given at once: no
/// call that comes back empty has read something it holds back. The check needs no timing (on a
/// loaded runner the broker's thread can write its answer after any fixed wait): whichever call
/// reads the answer must give it.
#[test]
fn a_zero_timeout_gives_what_has_arrived() {
    let mut a = Log::default();
    a.push(0, &["z"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[("enable.partition.eof", "false")], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < until, "never given");
        match c.next_event(Duration::ZERO, &mut |_| {}).unwrap() {
            Some((0, Polled::Msg(m))) => break assert_eq!(m.value.as_deref(), Some(&b"z"[..])),
            got => assert!(c.parts.iter().all(|p| p.queue.is_empty()), "read, and {got:?} given"),
        }
    }
}

/// A broker that asks for a wait (KIP-219) is sent no Fetch before it is over.
#[test]
fn a_throttling_broker_is_waited_for() {
    let mut a = Log::default();
    a.push(0, &["t1"]);
    a.push(0, &["t2"]);
    let l = logs(&[("t", 0, a)]);
    let inner = log_handler(l.clone(), no_fault());
    let throttled: Fault = Box::new(move |r, n| {
        (n == 0).then(|| match inner(r) {
            Answer::Body(mut b) => {
                b[..4].copy_from_slice(&300i32.to_be_bytes());
                Answer::Body(b)
            }
            a => a,
        })
    });
    let br = log_broker(l, throttled);
    let mut c = consumer(&br, &[("fetch.message.max.bytes", "1")], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let t = Instant::now();
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["t1", "t2", "EOF"]);
    assert!(t.elapsed() >= Duration::from_millis(300), "{:?}", t.elapsed());
}

/// A replica suggested in a response (`preferred_read_replica`, given only to a consumer with a
/// rack, which this one never gives) is read as a moved leader: its records are not taken.
#[test]
fn a_preferred_replica_is_a_leader_moved() {
    let mut a = Log::default();
    a.push(0, &["right"]);
    let fault: Fault = Box::new(|_, n| {
        (n == 0).then(|| {
            let mut b = fetch_answer("t", 0, 0, 1, Some(&batch(0, 0, &["wrong"])));
            let at = 4 + 2 + 4 + 4 + 3 + 4 + 4 + 2 + 8 + 8 + 8 + 4; // its preferred_read_replica
            b[at..at + 4].copy_from_slice(&0i32.to_be_bytes());
            Answer::Body(b)
        })
    });
    let br = log_broker(logs(&[("t", 0, a)]), fault);
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["right", "EOF"]);
    assert!(heard.iter().any(|h| h.contains("NOT_LEADER_OR_FOLLOWER")), "{heard:?}");
}

/// A partition's error that a retry may mend (REQUEST_TIMED_OUT) is retried; one it cannot
/// (TOPIC_AUTHORIZATION_FAILED) is the caller's error.
#[test]
fn fetch_errors_are_retried_or_given() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let timed_out: Fault = Box::new(|_, n| (n == 0).then(|| Answer::Body(fetch_answer("t", 0, 7, -1, None))));
    let br = log_broker(logs(&[("t", 0, a.clone())]), timed_out);
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["x", "EOF"]);
    assert!(heard.iter().any(|h| h.contains("REQUEST_TIMED_OUT")), "{heard:?}");

    let refused: Fault = Box::new(|_, _| Some(Answer::Body(fetch_answer("t", 0, 29, -1, None))));
    let br = log_broker(logs(&[("t", 0, a)]), refused);
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let e = (0..1000).find_map(|_| c.next_event(Duration::from_millis(5), &mut |_| {}).err()).unwrap();
    assert_eq!(e.code(), Some(Code(29)));
}

/// A Fetch unanswered is a connection lost after `socket.timeout.ms` plus the wait it asked for.
#[test]
fn an_unanswered_fetch_waits_out_its_own_wait_too() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), Box::new(|_, _| Some(Answer::Silence)));
    let mut c = consumer(
        &br,
        &[("socket.timeout.ms", "200"), ("fetch.wait.max.ms", "400")],
        vec![Assignment::new("t", 0, Offset::Beginning)],
    );
    let (mut heard, until) = (vec![], Instant::now() + Duration::from_secs(5));
    while heard.is_empty() {
        assert!(Instant::now() < until);
        c.pump(&mut |w| heard.push(w.to_string())).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(heard[0].contains("no answer to a Fetch in 600ms"), "{heard:?}");
}

/// A leader whose connections drop is connected to again with a growing wait, from
/// `reconnect.backoff.ms` up to `reconnect.backoff.max.ms`.
#[test]
fn a_lost_leader_is_reconnected_to_with_backoff() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[("t", &[(0, 0)])])),
        _ => Answer::Drop,
    });
    let mut c = consumer(
        &b,
        &[("reconnect.backoff.ms", "50"), ("reconnect.backoff.max.ms", "100")],
        vec![Assignment::new("t", 0, Offset::At(0))],
    );
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(2) {
        c.pump(&mut |_| {}).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut conns: Vec<usize> = b.requests(wire::FETCH).iter().map(|r| r.conn).collect();
    conns.dedup();
    // 0, 50, 150, 250, ... ms: about 20 in 2 s
    assert!((12..=40).contains(&conns.len()), "{} connections", conns.len());
}

/// A leader that moved to another address: its partitions follow it, not the old address.
#[test]
fn a_leader_at_a_new_address_is_followed() {
    let mut a = Log::default();
    a.push(0, &["moved"]);
    let new = log_broker(logs(&[("t", 0, a)]), no_fault());
    let (to, fetched) = (new.addr.clone(), Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let f2 = fetched.clone();
    let old = Broker::start(move |r| match r.api {
        // the same broker id, at its new address once the old one dropped a Fetch
        wire::METADATA => {
            let at = if f2.load(Ordering::SeqCst) { to.as_str() } else { &r.me };
            Answer::Body(metadata(&[(0, at)], &[("t", &[(0, 0)])]))
        }
        _ => {
            f2.store(true, Ordering::SeqCst);
            Answer::Drop
        }
    });
    let mut c = consumer(&old, &[], vec![Assignment::new("t", 0, Offset::At(0))]);
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["moved", "EOF"]);
    assert!(fetched.load(Ordering::SeqCst));
}

/// Two topics whose partition 0 each has its own leader: each fetched from its own, never from
/// the other's, also while one has no leader yet.
#[test]
fn each_partition_is_fetched_from_its_own_leader() {
    let (mut a, mut b) = (Log::default(), Log::default());
    a.push(0, &["from a"]);
    b.push(0, &["from b"]);
    let addrs = Arc::new(std::sync::OnceLock::<(String, String)>::new());
    let lookups = Arc::new(AtomicUsize::new(0));
    let leader = |logs: Logs, addrs: Arc<std::sync::OnceLock<(String, String)>>| {
        let (inner, n) = (log_handler(logs, no_fault()), lookups.clone());
        Broker::start(move |r| match r.api {
            wire::METADATA => {
                let (x, y) = addrs.get().unwrap();
                let b_leader = if n.fetch_add(1, Ordering::SeqCst) < 3 { -1 } else { 1 };
                Answer::Body(metadata(&[(0, x), (1, y)], &[("a", &[(0, 0)]), ("b", &[(0, b_leader)])]))
            }
            _ => inner(r),
        })
    };
    let ba = leader(logs(&[("a", 0, a)]), addrs.clone());
    let bb = leader(logs(&[("b", 0, b)]), addrs.clone());
    addrs.set((ba.addr.clone(), bb.addr.clone())).unwrap();
    let mut c =
        consumer(&ba, &[], vec![Assignment::new("a", 0, Offset::At(0)), Assignment::new("b", 0, Offset::At(0))]);
    let (got, heard) = drain(&mut c, 1);
    assert_eq!((vals(&got, 0), vals(&got, 1)), (vec!["from a", "EOF"], vec!["from b", "EOF"]), "{heard:?}");
    assert_eq!(c.brokers.len(), 2);
    let asked_a: Vec<String> = ba.requests(wire::FETCH).iter().flat_map(fetched).map(|f| f.0).collect();
    assert!(asked_a.iter().all(|t| t == "a"), "{asked_a:?}");
}

/// With a Fetch in flight to a broker that does not answer, `pump` returns at once: its socket
/// is non-blocking (the data thread never waits on a broker).
#[test]
fn a_fetch_unanswered_never_blocks_the_caller() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), Box::new(|_, _| Some(Answer::Silence)));
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let until = Instant::now() + Duration::from_secs(5);
    while br.requests(wire::FETCH).is_empty() {
        assert!(Instant::now() < until);
        c.pump(&mut |_| {}).unwrap();
    }
    let t = Instant::now();
    for _ in 0..100 {
        c.pump(&mut |_| {}).unwrap();
    }
    assert!(t.elapsed() < Duration::from_millis(100), "{:?}", t.elapsed());
}

/// A socket found empty is not read again for `QUIET_READ`: the data loop pumps on every pass,
/// and a read of nothing is a system call each (on a host checking each, as CI's runner does
/// with AppArmor, a tenth of the data thread's time at one a message).
#[test]
fn a_quiet_socket_is_not_read_on_every_pump() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), Box::new(|_, _| Some(Answer::Silence)));
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let until = Instant::now() + Duration::from_secs(5);
    while br.requests(wire::FETCH).is_empty() {
        assert!(Instant::now() < until);
        c.pump(&mut |_| {}).unwrap();
    }
    std::thread::sleep(QUIET_READ * 2); // its wait over: the first pump below reads
    let (t, before) = (Instant::now(), c.brokers[0].reads);
    for _ in 0..20_000 {
        c.pump(&mut |_| {}).unwrap();
    }
    let (reads, most) = (c.brokers[0].reads - before, t.elapsed().as_micros() as u64 / 100 + 2);
    assert!(reads <= most, "{reads} reads of a quiet socket in {:?}", t.elapsed());
    assert!(reads >= 1, "read again once its wait is over: {:?} of pumps", t.elapsed());
}

/// A consumer with nothing to read waits on its sockets' readiness, not on a timer: an idle
/// `next_event` reads its socket a few times (each wait's end, `WAIT`), not every millisecond, and
/// what arrives is read when it arrives (the produce-to-consume latency a 1 ms sleep floored).
#[cfg(unix)]
#[test]
fn an_idle_consumer_waits_on_its_socket_not_a_timer() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), Box::new(|_, _| Some(Answer::Silence)));
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let until = Instant::now() + Duration::from_secs(5);
    while br.requests(wire::FETCH).is_empty() {
        assert!(Instant::now() < until);
        c.pump(&mut |_| {}).unwrap();
    }
    let (t, before) = (Instant::now(), c.brokers[0].reads);
    assert_eq!(c.next_event(Duration::from_millis(300), &mut |_| {}).unwrap(), None);
    let reads = c.brokers[0].reads - before;
    let most = (t.elapsed().as_millis() / WAIT.as_millis()) as u64 * 2 + 4;
    assert!(reads <= most, "{reads} reads of an idle socket in {:?}, at most {most}", t.elapsed());
}

/// A leader cut off from the network (its Fetch unanswered, its connection open) whose
/// partitions' leadership moves: they are fetched from their new leader within seconds, not after
/// `socket.timeout.ms`. On staging, a broker cut off for 60 s held its partitions' rows that long:
/// the other partitions read on, and 896,978 rows came too late for their windows (librdkafka,
/// which follows a leader from any metadata, dropped 29,848).
#[test]
fn a_leader_that_stops_answering_is_left_for_the_new_leader() {
    let mut log = Log::default();
    log.push(0, &["before"]);
    log.push(0, &["after"]);
    let (moved, addrs) =
        (Arc::new(std::sync::atomic::AtomicBool::new(false)), Arc::new(std::sync::OnceLock::<(String, String)>::new()));
    let md = |moved: Arc<std::sync::atomic::AtomicBool>, addrs: Arc<std::sync::OnceLock<(String, String)>>| {
        move |_: &Req| {
            let (a, b) = addrs.get().unwrap();
            let leader = if moved.load(Ordering::SeqCst) { 1 } else { 0 };
            Answer::Body(metadata(&[(0, a), (1, b)], &[("t", &[(0, leader)])]))
        }
    };
    let (m1, a1) = (md(moved.clone(), addrs.clone()), log_handler(logs(&[("t", 0, log.clone())]), no_fault()));
    // the new leader: the whole log
    let new = Broker::start(move |r| if r.api == wire::METADATA { m1(r) } else { a1(r) });
    let mut first = Log::default();
    first.push(0, &["before"]);
    let (m0, flag) = (md(moved.clone(), addrs.clone()), moved.clone());
    let a0 = log_handler(
        logs(&[("t", 0, first)]),
        // its first Fetch answered, then none: cut off, and the leadership moves away
        Box::new(move |_, n| {
            (n > 0).then(|| {
                flag.store(true, Ordering::SeqCst);
                Answer::Silence
            })
        }),
    );
    let old = Broker::start(move |r| if r.api == wire::METADATA { m0(r) } else { a0(r) });
    addrs.set((old.addr.clone(), new.addr.clone())).unwrap();
    let mut c = consumer(
        &old,
        &[("socket.timeout.ms", "30000"), ("fetch.wait.max.ms", "100")],
        vec![Assignment::new("t", 0, Offset::At(0))],
    );
    let (got, heard) = drain(&mut c, 2);
    assert_eq!(vals(&got, 0), ["before", "EOF", "after", "EOF"], "{heard:?}");
    assert!(!new.requests(wire::FETCH).is_empty());
}

/// The same with the leader's id kept and its address changed (a broker's pod on a new node, the
/// old address cut off): its partitions follow the address.
#[test]
fn a_leader_that_stops_answering_at_an_address_it_left_is_left() {
    let mut log = Log::default();
    log.push(0, &["before"]);
    log.push(0, &["after"]);
    let (moved, addrs) =
        (Arc::new(std::sync::atomic::AtomicBool::new(false)), Arc::new(std::sync::OnceLock::<(String, String)>::new()));
    let md = |moved: Arc<std::sync::atomic::AtomicBool>, addrs: Arc<std::sync::OnceLock<(String, String)>>| {
        move |_: &Req| {
            let (a, b) = addrs.get().unwrap();
            let at = if moved.load(Ordering::SeqCst) { b } else { a };
            Answer::Body(metadata(&[(0, at)], &[("t", &[(0, 0)])]))
        }
    };
    let (m1, a1) = (md(moved.clone(), addrs.clone()), log_handler(logs(&[("t", 0, log.clone())]), no_fault()));
    let new = Broker::start(move |r| if r.api == wire::METADATA { m1(r) } else { a1(r) });
    let mut first = Log::default();
    first.push(0, &["before"]);
    let (m0, flag) = (md(moved.clone(), addrs.clone()), moved.clone());
    let a0 = log_handler(
        logs(&[("t", 0, first)]),
        Box::new(move |_, n| {
            (n > 0).then(|| {
                flag.store(true, Ordering::SeqCst);
                Answer::Silence
            })
        }),
    );
    let old = Broker::start(move |r| if r.api == wire::METADATA { m0(r) } else { a0(r) });
    addrs.set((old.addr.clone(), new.addr.clone())).unwrap();
    let mut c = consumer(
        &old,
        &[("socket.timeout.ms", "30000"), ("fetch.wait.max.ms", "100")],
        vec![Assignment::new("t", 0, Offset::At(0))],
    );
    let (got, heard) = drain(&mut c, 2);
    assert_eq!(vals(&got, 0), ["before", "EOF", "after", "EOF"], "{heard:?}");
}

/// A leader answering its Fetches in time is not looked up again; one slow to answer (past its
/// wait by more than `OVERDUE`) is looked up about once each `OVERDUE`, and, still the leader,
/// keeps its partitions and its connection.
#[test]
fn a_slow_leader_is_looked_up_once_a_while_and_kept() {
    let mut log = Log::default();
    log.push(0, &["x"]);
    let slow_from = Instant::now() + Duration::from_millis(2500);
    let inner = log_handler(logs(&[("t", 0, log)]), no_fault());
    let b = Broker::start(move |r| {
        if r.api == wire::FETCH && Instant::now() >= slow_from {
            std::thread::sleep(Duration::from_millis(3500)); // answered, late
        }
        inner(r)
    });
    let mut c = consumer(
        &b,
        &[("socket.timeout.ms", "30000"), ("fetch.wait.max.ms", "100")],
        vec![Assignment::new("t", 0, Offset::At(0))],
    );
    let mut heard = vec![];
    while Instant::now() < slow_from {
        c.pump(&mut |w| heard.push(w.to_string())).unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(b.requests(wire::METADATA).len(), 1, "a leader answering in time is not looked up again");
    let fetches = b.requests(wire::FETCH).len();
    while b.requests(wire::FETCH).len() < fetches + 3 {
        assert!(Instant::now() < slow_from + Duration::from_secs(15), "{heard:?}");
        c.pump(&mut |w| heard.push(w.to_string())).unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }
    // two answers 3.5 s late: overdue from 1.1 s each, looked up at about 1.1, 2.1 and 3.1 s. At
    // most one lookup a second of the time it was slow (a loaded runner's pumps take longer, and
    // its 3 Fetches more seconds), not one each pump
    let (lookups, slow_for) = (b.requests(wire::METADATA).len() - 1, slow_from.elapsed());
    assert!(lookups >= 3 && lookups as u64 <= slow_for.as_secs() + 1, "{lookups} lookups in {slow_for:?}");
    assert!(heard.is_empty(), "{heard:?}");
    assert!(b.requests(wire::FETCH).iter().all(|r| r.conn == b.requests(wire::FETCH)[0].conn), "one connection");
}

/// Overdue, a slow leader's partitions are each judged against their own topic's leaders. b/1,
/// which broker 0 still leads, is never weighed against a/1, which broker 1 leads: a leader that
/// still leads all it has keeps its connection, however many topics it leads.
#[test]
fn a_slow_leader_is_judged_by_each_partitions_own_topic() {
    let mut l = Log::default();
    l.push(0, &["x"]);
    let all = logs(&[("a", 0, l.clone()), ("a", 1, l.clone()), ("b", 1, l)]);
    let addrs = Arc::new(std::sync::OnceLock::<(String, String)>::new());
    let md = |addrs: Arc<std::sync::OnceLock<(String, String)>>| {
        move |_: &Req| {
            let (slow, fine) = addrs.get().unwrap();
            Answer::Body(metadata(&[(0, slow), (1, fine)], &[("a", &[(0, 0), (1, 1)]), ("b", &[(1, 0)])]))
        }
    };
    let slow_from = Instant::now() + Duration::from_millis(1500);
    let (m0, h0) = (md(addrs.clone()), log_handler(all.clone(), no_fault()));
    let slow = Broker::start(move |r| match r.api {
        wire::METADATA => m0(r),
        wire::FETCH if Instant::now() >= slow_from => {
            std::thread::sleep(Duration::from_millis(3500)); // answered, late
            h0(r)
        }
        _ => h0(r),
    });
    let (m1, h1) = (md(addrs.clone()), log_handler(all, no_fault()));
    let fine = Broker::start(move |r| if r.api == wire::METADATA { m1(r) } else { h1(r) });
    addrs.set((slow.addr.clone(), fine.addr.clone())).unwrap();
    let parts = vec![
        Assignment::new("a", 0, Offset::At(0)),
        Assignment::new("a", 1, Offset::At(0)),
        Assignment::new("b", 1, Offset::At(0)),
    ];
    let mut c = consumer(&slow, &[("socket.timeout.ms", "30000"), ("fetch.wait.max.ms", "100")], parts);
    let mut heard = vec![];
    // through two of its late answers: overdue, and looked up for it, more than once
    while Instant::now() < slow_from + Duration::from_secs(5) {
        c.pump(&mut |w| heard.push(w.to_string())).unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }
    let lookups = slow.requests(wire::METADATA).len() + fine.requests(wire::METADATA).len();
    assert!(lookups >= 3, "looked up while overdue: {lookups}");
    assert!(heard.is_empty(), "{heard:?}");
    let f = slow.requests(wire::FETCH);
    assert!(f.iter().all(|r| r.conn == f[0].conn), "one connection");
}

/// A corrupt batch fetched again until its retries are spent is an error; the Fetch sent from
/// past it is dropped with it: the batch is never skipped, however long the caller polls on.
#[test]
fn a_corrupt_batch_is_never_skipped_after_its_error() {
    let mut a = Log::default();
    a.push(0, &["bad"]); // offset 0, served corrupt below
    a.push(0, &["after"]); // offset 1
    let bad = {
        let mut b = batch(0, 0, &["bad"]);
        let n = b.len();
        b[n - 2] ^= 1;
        b
    };
    let fault: Fault =
        Box::new(move |r, _| (fetched(r)[0].2 == 0).then(|| Answer::Body(fetch_answer("t", 0, 0, 2, Some(&bad)))));
    let br = log_broker(logs(&[("t", 0, a)]), fault);
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let (mut errs, mut got) = (0, vec![]);
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        match c.next_event(Duration::from_millis(5), &mut |_| {}) {
            Err(_) => errs += 1,
            Ok(Some((_, Polled::Msg(m)))) => got.push(m.offset),
            _ => {}
        }
    }
    assert!(errs >= 2, "{errs} errors");
    assert!(got.is_empty(), "nothing past the corrupt batch: {got:?}");
    assert_eq!(c.position(0), Some(0));
    let asked: Vec<i64> = br.requests(wire::FETCH).iter().flat_map(fetched).map(|f| f.2).collect();
    assert!(asked.iter().all(|&o| o == 0), "never fetched from past it: {asked:?}");
}

/// One partition's error (an offset out of range with `auto.offset.reset=error`) in a response:
/// the other partitions' records are queued all the same, none lost; the consumer goes on.
#[test]
fn a_partitions_error_loses_no_other_partitions_records() {
    let (mut a, mut b) = (Log { start: 10, ..Default::default() }, Log::default());
    a.push(0, &["a"]);
    b.push(0, &["b0", "b1"]);
    let br = log_broker(logs(&[("t", 0, a), ("t", 1, b)]), no_fault());
    let mut c = consumer(
        &br,
        &[("auto.offset.reset", "error")],
        vec![Assignment::new("t", 0, Offset::At(3)), Assignment::new("t", 1, Offset::Beginning)],
    );
    let (mut got, mut errs) = (vec![], 0);
    let until = Instant::now() + Duration::from_secs(1);
    while Instant::now() < until {
        match c.next_event(Duration::from_millis(5), &mut |_| {}) {
            Err(e) => {
                assert_eq!(e.code(), Some(Code::OFFSET_OUT_OF_RANGE));
                errs += 1;
            }
            Ok(Some((i, Polled::Msg(m)))) => got.push((i, String::from_utf8(m.value.unwrap()).unwrap())),
            _ => {}
        }
    }
    assert!(errs >= 1);
    assert_eq!(got, [(1, "b0".to_string()), (1, "b1".to_string())]);
    assert_eq!((c.position(0), c.position(1)), (Some(3), Some(2)));
}

/// A response naming a partition not asked for, or one twice, is refused whole before anything
/// is taken of it: fetched again from the same offsets, nothing skipped.
#[test]
fn a_response_naming_a_partition_not_asked_for_skips_nothing() {
    for extra in [7, 0] {
        let mut a = Log::default();
        a.push(0, &["a0"]);
        a.push(0, &["a1"]);
        let rec = batch(0, 0, &["a0"]);
        let fault: Fault = Box::new(move |_, n| {
            (n == 0).then(|| {
                let mut out = vec![];
                out.i32(0);
                out.i16(0);
                out.i32(0);
                out.i32(1);
                out.str("t");
                out.i32(2);
                for (p, recs) in [(0, Some(&rec[..])), (extra, None)] {
                    out.i32(p);
                    out.i16(0);
                    out.i64(2);
                    out.i64(2);
                    out.i64(0);
                    out.i32(-1);
                    out.i32(-1);
                    match recs {
                        Some(r) => out.bytes(r),
                        None => out.i32(-1),
                    }
                }
                Answer::Body(out)
            })
        });
        let br = log_broker(logs(&[("t", 0, a)]), fault);
        let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
        let (got, heard) = drain(&mut c, 1);
        assert_eq!(vals(&got, 0), ["a0", "a1", "EOF"], "{heard:?}");
        assert!(heard[0].contains(&format!("t/{extra}: in a Fetch response, not asked for")), "{heard:?}");
    }
}

/// A paused partition gives nothing, what is queued of it included, until it is resumed.
#[test]
fn a_paused_partition_gives_nothing_queued_until_resumed() {
    let mut a = Log::default();
    a.push(0, &["a1", "a2"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning)]);
    while c.parts[0].queue.is_empty() {
        c.pump(&mut |_| {}).unwrap();
    }
    c.pause(|_, _| true);
    assert_eq!(c.next_event(Duration::from_millis(50), &mut |_| {}).unwrap(), None);
    assert_eq!((c.poll(0), c.next_any()), (Polled::Empty, None));
    assert_eq!(c.position(0), Some(0));
    c.resume(|_, _| true);
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["a1", "a2", "EOF"]);
    // paused from the start
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::Beginning).paused()]);
    assert_eq!(c.next_event(Duration::from_millis(100), &mut |_| {}).unwrap(), None);
    assert!(br.requests(wire::FETCH).is_empty());
}

/// `Duration::MAX`, a natural "until one comes", waits rather than overflowing.
#[test]
fn next_event_waits_for_duration_max() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[("enable.partition.eof", "false")], vec![Assignment::new("t", 0, Offset::Beginning)]);
    assert!(matches!(c.next_event(Duration::MAX, &mut |_| {}).unwrap(), Some((0, Polled::Msg(_)))));
}

/// A partition assigned twice (one index would get its records twice, the other none) or an
/// offset below 0 (read as the beginning) is refused; `seek` to one below 0 too, changing nothing.
#[test]
fn an_assignment_twice_or_below_offset_0_is_refused() {
    let br = Broker::start(|_| Answer::Drop);
    let c = config(&br.addr);
    let client = Arc::new(Client::new(&c).unwrap());
    let new = |parts| Consumer::new(client.clone(), settings(&c), parts).map(|_| ()).unwrap_err().to_string();
    let twice = vec![Assignment::new("t", 0, Offset::Beginning), Assignment::new("t", 0, Offset::At(4))];
    assert!(new(twice).contains("t/0: assigned twice"));
    assert!(new(vec![Assignment::new("t", 1, Offset::At(-2))]).contains("t/1: offset -2"));
    let mut k = Consumer::from_config(&c, vec![Assignment::new("t", 0, Offset::At(5))]).unwrap();
    assert!(k.seek(0, Offset::At(-1)).is_err());
    assert_eq!(k.position(0), Some(5));
}

/// KIP-74: of a Fetch, only the first partition with data gets a batch past its max bytes whole,
/// the others it cut. The partitions are asked in turn first, as Java's consumer and librdkafka
/// rotate them: a big batch behind a busy partition is not starved.
#[test]
fn a_partition_behind_a_busy_one_is_not_starved() {
    let (mut a, mut b) = (Log::default(), Log::default());
    for i in 0..300 {
        a.push(0, &[&format!("a{i}")]);
    }
    b.push(0, &[&"B".repeat(2000)]);
    let l = logs(&[("t", 0, a), ("t", 1, b)]);
    let l2 = l.clone();
    let fault: Fault = Box::new(move |r, _| {
        let l = l2.lock().unwrap();
        let mut out = vec![];
        out.i32(0);
        out.i16(0);
        out.i32(0);
        let asked = fetched(r);
        out.i32(asked.len() as i32); // one topic per entry is fine for the decoder
        let mut min_one = true;
        for (t, p, offset) in asked {
            out.str(&t);
            out.i32(1);
            let log = &l[&(t.clone(), p)];
            out.i32(p);
            out.i16(0);
            out.i64(log.end());
            out.i64(log.end());
            out.i64(0);
            out.i32(-1);
            out.i32(-1);
            let max = 300usize;
            let mut recs = vec![];
            for (base, bt) in &log.batches {
                let next = base + i32::from_be_bytes(bt[23..27].try_into().unwrap()) as i64 + 1;
                if next <= offset {
                    continue;
                }
                if recs.is_empty() && bt.len() > max && !min_one {
                    recs.extend(&bt[..max]); // partial batch, as LogSegment.read slices it
                    break;
                }
                if !recs.is_empty() && recs.len() + bt.len() > max {
                    break;
                }
                recs.extend(bt);
            }
            if !recs.is_empty() {
                min_one = false;
            }
            out.bytes(&recs);
        }
        Some(Answer::Body(out))
    });
    let br = log_broker(l, fault);
    let mut c = consumer(
        &br,
        &[("enable.partition.eof", "false"), ("fetch.message.max.bytes", "300")],
        vec![Assignment::new("t", 0, Offset::Beginning), Assignment::new("t", 1, Offset::Beginning)],
    );
    let mut order = vec![];
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until && !order.iter().any(|&(i, _)| i == 1) {
        if let Some((i, Polled::Msg(m))) = c.next_event(Duration::from_millis(5), &mut |_| {}).unwrap() {
            order.push((i, m.offset));
        }
    }
    let before = order.iter().filter(|o| o.0 == 0).count();
    assert!(before < 50, "partition 1 got its first record only after {before} of partition 0's");
}

/// A broker always corrupt beside a healthy one is an error after its own retries (the healthy
/// one's answers do not reset its count), and its reconnect backoff grows meanwhile.
#[test]
fn a_brokers_corrupt_count_and_backoff_are_its_own() {
    let (mut a, mut b) = (Log::default(), Log::default());
    a.push(0, &["a"]);
    for i in 0..3000 {
        b.push(0, &[&format!("b{i}")]); // one batch a Fetch, each answered at once
    }
    let addrs = Arc::new(std::sync::OnceLock::<(String, String)>::new());
    let md = |addrs: Arc<std::sync::OnceLock<(String, String)>>| {
        move |_: &Req| {
            let (x, y) = addrs.get().unwrap();
            Answer::Body(metadata(&[(0, x), (1, y)], &[("a", &[(0, 0)]), ("b", &[(0, 1)])]))
        }
    };
    let bad = {
        let mut b = batch(0, 0, &["bad"]);
        let n = b.len();
        b[n - 2] ^= 1;
        b
    };
    let always: Fault = Box::new(move |_, _| Some(Answer::Body(fetch_answer("a", 0, 0, 1, Some(&bad)))));
    let (m0, h0) = (md(addrs.clone()), log_handler(logs(&[("a", 0, a)]), always));
    let corrupt = Broker::start(move |r| if r.api == wire::METADATA { m0(r) } else { h0(r) });
    let (m1, h1) = (md(addrs.clone()), log_handler(logs(&[("b", 0, b)]), no_fault()));
    let fine = Broker::start(move |r| if r.api == wire::METADATA { m1(r) } else { h1(r) });
    addrs.set((corrupt.addr.clone(), fine.addr.clone())).unwrap();
    let mut c = consumer(
        &corrupt,
        &[("enable.partition.eof", "false"), ("reconnect.backoff.max.ms", "1000"), ("fetch.message.max.bytes", "1")],
        vec![Assignment::new("a", 0, Offset::At(0)), Assignment::new("b", 0, Offset::At(0))],
    );
    let until = Instant::now() + Duration::from_secs(5);
    let e = loop {
        assert!(Instant::now() < until, "a broker always corrupt is never an error");
        if let Err(e) = c.next_event(Duration::from_millis(5), &mut |_| {}) {
            break e;
        }
    };
    assert!(e.to_string().contains("CRC"), "{e}");
    let bad = c.brokers.iter().find(|b| b.addr == corrupt.addr).unwrap();
    assert!(bad.backoff >= Duration::from_millis(8), "doubled from 1 ms each time: {:?}", bad.backoff);
}

/// A Fetch whose write stalls (the broker reads nothing) times out as one unanswered does.
#[test]
fn a_fetch_whose_write_stalls_times_out() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), Box::new(|_, _| Some(Answer::Silence)));
    let mut c = consumer(&br, &[("socket.timeout.ms", "100")], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let until = Instant::now() + Duration::from_secs(5);
    while br.requests(wire::FETCH).is_empty() {
        assert!(Instant::now() < until);
        c.pump(&mut |_| {}).unwrap();
    }
    // as if the request were still being written, long ago
    c.brokers[0].written = 0;
    c.brokers[0].inflight.as_mut().unwrap().since = Instant::now() - Duration::from_secs(60);
    let e = c.receive(0, Instant::now()).unwrap_err();
    assert!(e.to_string().contains("no answer to a Fetch"), "{e}");
}

/// A partition reset (or its leader lost) while a lookup runs is looked up as soon as it ends:
/// the lookup, which did all it was for, does not count it as its failure (a backoff).
#[test]
fn a_reset_while_a_lookup_runs_is_looked_up_at_once() {
    let br = Broker::start(|_| Answer::Drop);
    let mut c =
        consumer(&br, &[], vec![Assignment::new("t", 0, Offset::At(0)), Assignment::new("t", 1, Offset::At(0))]);
    c.parts[0].broker = Some(0);
    let (tx, rx) = mpsc::channel();
    tx.send(Found { offsets: None, leaders: vec![] }).unwrap();
    (c.lookup, c.looking) = (Some(rx), vec![(0, 0, false, true), (1, 0, false, true)]);
    // their leaders looked up (found); partition 0 reset meanwhile
    c.parts[1].broker = Some(0);
    c.parts[0].next = None;
    let now = Instant::now();
    c.lookups(now, &mut |_| {});
    assert!(c.lookup_at <= now && c.lookup_backoff == BACKOFF, "the reset's lookup waits {:?}", c.lookup_at - now);
    // and one that failed at what it was for waits
    let (tx, rx) = mpsc::channel();
    tx.send(Found { offsets: None, leaders: vec![] }).unwrap();
    (c.lookup, c.looking) = (Some(rx), vec![(0, 0, true, false)]);
    c.lookups(now, &mut |_| {});
    assert!(c.lookup_at > now);
}

/// `seek`: what is queued of the partition dropped, and what a Fetch in flight brings of it from
/// the old offset; read from the new one on.
#[test]
fn seek_drops_what_is_queued_and_in_flight() {
    let mut a = Log::default();
    for i in 0..5 {
        a.push(0, &[&format!("m{i}")]);
    }
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c = consumer(&br, &[("fetch.message.max.bytes", "1")], vec![Assignment::new("t", 0, Offset::Beginning)]);
    // m0 queued, the Fetch from 1 in flight
    while c.parts[0].queue.is_empty() || c.brokers[0].inflight.is_none() {
        c.pump(&mut |_| {}).unwrap();
    }
    c.seek(0, Offset::At(3)).unwrap();
    assert_eq!(c.position(0), Some(3));
    let (got, heard) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["m3", "m4", "EOF"], "{heard:?}");
    c.seek(0, Offset::Beginning).unwrap();
    assert_eq!(c.position(0), None, "looked up");
    let (got, _) = drain(&mut c, 1);
    assert_eq!(vals(&got, 0), ["m0", "m1", "m2", "m3", "m4", "EOF"]);
}

/// `queued.max.messages.kbytes` bounds what a partition queues, not only one Fetch: a partition
/// at it is fetched no more until what it holds is taken (as librdkafka's partition queue, which
/// stops fetching at either bound), else large messages not taken grow `queued.min.messages` big.
#[test]
fn a_partitions_queue_is_bounded_by_its_bytes() {
    let mut a = Log::default();
    let big = "x".repeat(64 * 1024);
    for _ in 0..200 {
        a.push(0, &[big.as_str()]);
    }
    let br = log_broker(logs(&[("t", 0, a)]), no_fault());
    let mut c =
        consumer(&br, &[("queued.max.messages.kbytes", "1024")], vec![Assignment::new("t", 0, Offset::Beginning)]);
    let queued = |c: &Consumer| c.parts[0].queue.iter().map(|m| m.value.as_ref().map_or(0, Vec::len)).sum::<usize>();
    let until = Instant::now() + Duration::from_millis(300);
    while Instant::now() < until {
        c.pump(&mut |_| {}).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    let bytes = queued(&c);
    assert!(bytes > 0 && bytes <= 2 * 1024 * 1024, "queued.max.messages.kbytes=1024, yet {bytes} bytes queued");
    // taken, it is fetched on to the end
    let (got, _) = drain(&mut c, 1);
    assert_eq!(got.iter().filter(|g| g.1 != "EOF").count(), 200);
}

/// A partition error that persists (here NOT_LEADER_OR_FOLLOWER while the metadata still names
/// the same leader, as during an election) is looked up and fetched again only after
/// `fetch.error.backoff.ms`, not at once: the broker is not hammered.
#[test]
fn a_partition_error_that_persists_is_retried_after_a_backoff() {
    let mut a = Log::default();
    a.push(0, &["x"]);
    let br = log_broker(logs(&[("t", 0, a)]), Box::new(|_, _| Some(Answer::Body(fetch_answer("t", 0, 6, 1, None)))));
    let mut c = consumer(&br, &[], vec![Assignment::new("t", 0, Offset::At(0))]);
    let t = Instant::now();
    while t.elapsed() < Duration::from_millis(500) {
        let _ = c.next_event(Duration::from_millis(10), &mut |_| {});
    }
    let (f, m) = (br.requests(wire::FETCH).len(), br.requests(wire::METADATA).len());
    assert!(f < 20 && m < 20, "in 500 ms: {f} Fetches, {m} Metadata requests");
}
