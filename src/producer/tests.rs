#![allow(clippy::type_complexity)] // the tests' tuples say what each field is where they are made
use super::*;
use crate::records::ReadOpts;
use crate::testkit::{config, metadata, Answer, Broker, Req};
use crate::wire::Put;
use std::sync::atomic::{AtomicUsize, Ordering};

/// An outcome as a test's producer heard it: topic, partition, offset, error, its code, opaque,
/// whether it is on the log.
type Outcome = (String, Option<i32>, Option<i64>, Option<String>, Option<Code>, u64, Persisted);

#[derive(Default)]
struct Heard(Mutex<Vec<Outcome>>);

impl Deliveries for Heard {
    type Opaque = u64;
    fn delivery(&self, d: &Delivery<'_>, o: u64) {
        let e = d.error.map(|e| e.to_string());
        let c = d.error.and_then(Error::code);
        self.0.lock().unwrap().push((d.topic.to_string(), d.partition, d.offset, e, c, o, d.persisted));
    }
}

/// A partition's log as the broker keeps it: (value, header values) by offset; the idempotent
/// producer's state: its epoch, the next sequence, the last batch's (base sequence, base offset).
#[derive(Default)]
struct Log {
    values: Vec<(Vec<u8>, Vec<(String, Option<Vec<u8>>)>)>,
    /// each record's key and value as written (None: null)
    records: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>,
    epoch: i16,
    next_seq: i32,
    last: Option<(i32, i64)>,
    pid: i64,
    /// each record's (producer id, epoch)
    ids: Vec<(i64, i16)>,
    /// each record's timestamp, and how many batches were appended
    stamps: Vec<i64>,
    batches: usize,
}

type Logs = Arc<Mutex<HashMap<(String, i32), Log>>>;
type Fault = Box<dyn Fn(&Req, usize, &Logs) -> Option<Answer> + Send + Sync>;

/// A broker leading `parts` partitions of each of `topics`, as Kafka's idempotence keeps a
/// partition: a batch at its next sequence appended, the last batch again a duplicate (its offset
/// answered again), any other sequence OUT_OF_ORDER_SEQUENCE_NUMBER; a higher epoch at sequence 0
/// a new start (appended, whatever was written before), a lower one INVALID_PRODUCER_EPOCH.
fn sink(topics: &'static [&'static str], parts: i32, fault: Fault) -> (Broker, Logs) {
    let logs: Logs = Arc::default();
    let l2 = logs.clone();
    let n = AtomicUsize::new(0);
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => {
            let ps: Vec<(i32, i32)> = (0..parts).map(|p| (p, 0)).collect();
            let ts: Vec<(&str, &[(i32, i32)])> = topics.iter().map(|t| (*t, &ps[..])).collect();
            Answer::Body(metadata(&[(0, &r.me)], &ts))
        }
        wire::INIT_PRODUCER_ID => {
            let mut out = vec![];
            out.i32(0);
            out.i16(0);
            out.i64(1000);
            out.i16(0);
            Answer::Body(out)
        }
        wire::PRODUCE => {
            let i = n.fetch_add(1, Ordering::SeqCst);
            if let Some(a) = fault(r, i, &l2) {
                return a;
            }
            Answer::Body(produce(r, &l2))
        }
        _ => Answer::Drop,
    });
    (b, logs)
}

/// Appends a Produce request's batches as the broker does, and its answer.
fn produce(r: &Req, logs: &Logs) -> Vec<u8> {
    let mut c = Cur::new(&r.body);
    assert_eq!(c.nstr().unwrap(), None, "not transactional");
    assert_eq!(c.i16().unwrap(), -1, "acks=all");
    c.i32().unwrap();
    let mut out = vec![];
    let nt = c.i32().unwrap();
    out.i32(nt);
    for _ in 0..nt {
        let t = c.str().unwrap().to_string();
        out.str(&t);
        let np = c.i32().unwrap();
        out.i32(np);
        for _ in 0..np {
            let p = c.i32().unwrap();
            let batch = c.bytes().unwrap();
            let b = records::batches(batch).next().unwrap().unwrap();
            let epoch = i16::from_be_bytes(batch[51..53].try_into().unwrap());
            let seq = i32::from_be_bytes(batch[53..57].try_into().unwrap());
            let mut l = logs.lock().unwrap();
            let log = l.entry((t.clone(), p)).or_default();
            if epoch > log.epoch && seq == 0 {
                // a bumped epoch starts the partition's sequence again: nothing is a duplicate
                (log.epoch, log.next_seq, log.last) = (epoch, 0, None);
            }
            let (code, base) = if epoch < log.epoch {
                (47, -1)
            } else if epoch > log.epoch {
                (45, -1)
            } else if log.last.is_some_and(|(s, _)| s == seq) {
                (0, log.last.unwrap().1) // a duplicate: the offset it was written at
            } else if seq != log.next_seq {
                (45, -1)
            } else {
                let base = log.values.len() as i64;
                log.pid = b.producer_id;
                records::read(batch, &ReadOpts { check_crcs: true, ..Default::default() }, &mut vec![], |r| {
                    let hs = r.headers().map(|(k, v)| (k.to_string(), v.map(<[u8]>::to_vec))).collect();
                    log.values.push((r.value.unwrap_or_default().to_vec(), hs));
                    log.records.push((r.key.map(<[u8]>::to_vec), r.value.map(<[u8]>::to_vec)));
                    log.stamps.push(r.timestamp);
                    log.ids.push((b.producer_id, epoch));
                })
                .unwrap();
                log.batches += 1;
                log.next_seq = seq + b.count;
                log.last = Some((seq, base));
                (0, base)
            };
            out.i32(p);
            out.i16(code);
            out.i64(base);
            out.i64(-1);
            out.i64(0); // log start
            out.i32(0); // record errors
            out.nstr(None);
        }
    }
    out.i32(0);
    out
}

/// A Produce v8 answer of one partition: an error, no offset.
fn refusal(t: &str, p: i32, code: i16) -> Vec<u8> {
    let mut out = vec![];
    out.i32(1);
    out.str(t);
    out.i32(1);
    out.i32(p);
    out.i16(code);
    out.i64(-1);
    out.i64(-1);
    out.i64(-1);
    out.i32(0);
    out.nstr(Some("the broker's reason"));
    out.i32(0);
    out
}

fn no_fault() -> Fault {
    Box::new(|_, _, _| None)
}

fn producer(b: &Broker, extra: &[(&str, &str)]) -> Producer<Heard> {
    let mut c = config(&b.addr);
    c.set("linger.ms", "5").set("retry.backoff.ms", "5").set("reconnect.backoff.ms", "1");
    for (k, v) in extra {
        c.set(*k, *v);
    }
    Producer::new(&c, Heard::default()).unwrap()
}

fn send(p: &Producer<Heard>, topic: &str, v: &str, o: u64) {
    let h = [("dedup".into(), Some(format!("k{o}").into_bytes()))];
    p.send(Record::to(topic).value(v.as_bytes()).headers(&h), o).unwrap();
}

/// Polls until `n` outcomes are heard (within 10 s).
fn heard(p: &Producer<Heard>, n: usize) -> Vec<Outcome> {
    let until = Instant::now() + Duration::from_secs(10);
    while p.context().0.lock().unwrap().len() < n {
        assert!(Instant::now() < until, "{} of {n} heard", p.context().0.lock().unwrap().len());
        p.poll(Duration::from_millis(20));
    }
    p.context().0.lock().unwrap().clone()
}

fn values(logs: &Logs, t: &str, p: i32) -> Vec<String> {
    logs.lock()
        .unwrap()
        .get(&(t.to_string(), p))
        .map_or(vec![], |l| l.values.iter().map(|(v, _)| String::from_utf8(v.clone()).unwrap()).collect())
}

#[test]
fn messages_are_delivered_in_order_with_their_offsets_and_headers() {
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[]);
    for i in 0..50 {
        send(&p, "o", &format!("m{i}"), i);
    }
    let h = heard(&p, 50);
    assert!(h.iter().all(|x| x.3.is_none()), "{h:?}");
    let mut by_opaque: Vec<_> = h.iter().map(|x| (x.5, x.2.unwrap())).collect();
    by_opaque.sort();
    assert_eq!(by_opaque, (0..50).map(|i| (i, i as i64)).collect::<Vec<_>>(), "each message's own offset");
    assert_eq!(values(&logs, "o", 0), (0..50).map(|i| format!("m{i}")).collect::<Vec<_>>());
    assert_eq!(logs.lock().unwrap()[&("o".into(), 0)].values[7].1, [("dedup".to_string(), Some(b"k7".to_vec()))]);
    assert_eq!(logs.lock().unwrap()[&("o".into(), 0)].pid, 1000, "the producer id it was given");
    assert_eq!(p.in_flight_count(), 0);
    p.flush(Duration::from_secs(1)).unwrap();
    assert!(b.requests(wire::PRODUCE).len() < 50, "batched: {} requests", b.requests(wire::PRODUCE).len());
}

/// The broker wrote a batch but its answer was lost (the connection dropped): the batch is sent
/// again with the same sequence, and the broker writes it once.
#[test]
fn a_lost_answer_is_retried_and_written_once() {
    let fault: Fault = Box::new(|r, i, logs| {
        (i % 2 == 0).then(|| {
            let _ = produce(r, logs); // written, then the answer lost
            Answer::Drop
        })
    });
    let (b, logs) = sink(&["o"], 1, fault);
    let p = producer(&b, &[("batch.num.messages", "3")]);
    for i in 0..10 {
        send(&p, "o", &format!("m{i}"), i);
    }
    let h = heard(&p, 10);
    assert!(h.iter().all(|x| x.3.is_none()), "{h:?}");
    assert_eq!(values(&logs, "o", 0), (0..10).map(|i| format!("m{i}")).collect::<Vec<_>>(), "once each, in order");
    assert!(p.fatal_error().is_none());
}

/// librdkafka's duplicate (live-ingestion's staging, 2026-11-12): a batch written but its answer
/// lost, and another message timing out before the batch's retry. On any timeout librdkafka's
/// idempotent producer bumps its epoch and numbers what is queued from 0 again, the retry
/// included, and the broker writes it again as new: twice, both reported delivered. Here a
/// message that times out in a queue holds no sequence, and the retry keeps the batch's own: the
/// broker (which takes a higher epoch at sequence 0 as new) writes it once, under the one
/// producer id and epoch, and what is reported is what is on the log.
#[test]
fn a_batch_written_unanswered_is_written_once_though_another_message_times_out_before_its_retry() {
    let fault: Fault = Box::new(|r, i, logs| {
        (i == 1).then(|| {
            let _ = produce(r, logs); // written, then the answer lost
            Answer::Drop
        })
    });
    let (b, logs) = sink(&["o"], 1, fault);
    let p =
        producer(&b, &[("message.timeout.ms", "3000"), ("retry.backoff.ms", "2000"), ("retry.backoff.max.ms", "2000")]);
    send(&p, "o", "w", 0); // the partition's leader known, and the producer id
    heard(&p, 1);
    // never deliverable (the cluster has no such topic): times out at 3 s, in its queue
    p.send(Record::to("nope").value(b"c"), 9).unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    // sent at once, its answer lost; sent again 2 s later (3.5 s), after "c" timed out, before its
    // own timeout (4.5 s)
    send(&p, "o", "a", 1);
    std::thread::sleep(Duration::from_millis(100));
    send(&p, "o", "b", 2); // queued behind it, its batch in flight
    let h = heard(&p, 4);
    let at = |o: u64| h.iter().position(|x| x.5 == o).unwrap();
    assert_eq!(h[at(9)].3.as_deref(), Some("message timed out (message.timeout.ms)"), "{h:?}");
    assert!(at(9) < at(1), "the timeout came before the retry's answer: {h:?}");
    assert_eq!(b.requests(wire::PRODUCE).len(), 4, "w, a, a again, b");
    assert!(h[at(1)].3.is_none() && h[at(2)].3.is_none(), "{h:?}");
    assert_eq!(values(&logs, "o", 0), ["w", "a", "b"], "once each");
    assert_eq!((h[at(1)].2, h[at(2)].2), (Some(1), Some(2)), "each reported at its offset on the log");
    assert_eq!(log_of(&logs)[&("o".into(), 0)].ids, [(1000, 0); 3], "one producer id and epoch");
    assert_eq!(b.requests(wire::INIT_PRODUCER_ID).len(), 1, "the producer id asked for once");
    assert!(p.fatal_error().is_none());
}

/// The same batch written, its answer lost, and its own timeout passing before a retry is
/// answered: it is reported failed, not delivered, and the producer is fatal (the broker may have
/// written it, and a gap may follow). It is on the log once: brrrrr's read-back after a failed
/// delivery finds it there and sends it no more (`dual::Leg::recovered`, or a restart's).
#[test]
fn a_batch_written_unanswered_past_its_timeout_fails_and_is_on_the_log_once() {
    let fault: Fault = Box::new(|r, i, logs| {
        Some(match i {
            0 => {
                let _ = produce(r, logs);
                Answer::Drop
            }
            _ => Answer::Silence, // the retries unanswered
        })
    });
    let (b, logs) = sink(&["o"], 1, fault);
    let p = producer(&b, &[("message.timeout.ms", "600"), ("retry.backoff.ms", "100")]);
    send(&p, "o", "a", 1);
    std::thread::sleep(Duration::from_millis(50));
    send(&p, "o", "b", 2); // queued behind it
    let h = heard(&p, 2);
    let at = |o: u64| h.iter().position(|x| x.5 == o).unwrap();
    assert_eq!(h[at(1)].3.as_deref(), Some("message timed out (message.timeout.ms)"), "{h:?}");
    assert_eq!(h[at(1)].2, None, "not delivered");
    assert!(h[at(2)].3.is_some(), "{h:?}");
    assert_eq!(p.fatal_error().map(|f| f.0), Some(Code::MSG_TIMED_OUT));
    assert_eq!(values(&logs, "o", 0), ["a"], "written once though failed; b never");
}

/// At a batch's timeout its request still unanswered is given up: the connection it went on is
/// closed by then, and nothing is sent after (a fatal producer sends nothing). A request the
/// broker read before may still be written after, by the broker: what the lease's `2 × fence`
/// allows for past the message timeout (`run::producer_config`).
#[test]
fn at_its_timeout_a_request_unanswered_is_given_up_and_its_connection_closed() {
    let closed = Arc::new(Mutex::new(None));
    let c2 = closed.clone();
    let (b, _) = sink(&["o"], 1, Box::new(move |_, _, _| Some(Answer::Watch(c2.clone()))));
    let p = producer(&b, &[("message.timeout.ms", "500")]);
    let sent = Instant::now();
    send(&p, "o", "a", 1);
    let h = heard(&p, 1);
    assert_eq!(h[0].3.as_deref(), Some("message timed out (message.timeout.ms)"));
    assert_eq!(p.fatal_error().map(|f| f.0), Some(Code::MSG_TIMED_OUT));
    std::thread::sleep(Duration::from_millis(500));
    let at = closed.lock().unwrap().expect("its connection closed");
    let after = at.saturating_duration_since(sent);
    assert!(
        after >= Duration::from_millis(450) && after < Duration::from_millis(700),
        "closed {after:?} after sending"
    );
    assert_eq!(b.requests(wire::PRODUCE).len(), 1, "nothing sent after");
    assert!(p.send(Record::to("o").value(b"late"), 2).is_err(), "a fatal producer takes nothing more");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(b.requests(wire::PRODUCE).len(), 1);
}

/// A leader that moved (NOT_LEADER_OR_FOLLOWER) and replicas short (NOT_ENOUGH_REPLICAS) are
/// retried; the metadata is looked up again for the first.
#[test]
fn transient_refusals_are_retried() {
    let fault: Fault = Box::new(|r, i, _| {
        (i < 4).then(|| {
            let mut c = Cur::new(&r.body);
            c.nstr().unwrap();
            c.i16().unwrap();
            c.i32().unwrap();
            c.i32().unwrap();
            let t = c.str().unwrap().to_string();
            c.i32().unwrap();
            let pn = c.i32().unwrap();
            Answer::Body(refusal(&t, pn, if i % 2 == 0 { 6 } else { 19 }))
        })
    });
    let (b, logs) = sink(&["o"], 1, fault);
    let p = producer(&b, &[]);
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].2, h[0].3.as_ref()), (Some(0), None));
    assert_eq!(values(&logs, "o", 0), ["x"]);
    assert!(b.requests(wire::METADATA).len() >= 2, "looked up again after NOT_LEADER_OR_FOLLOWER");
}

/// A batch refused for good fails its messages with the broker's code, and the producer is
/// fatal: its partition's sequence has a gap the broker would refuse every later batch for.
#[test]
fn a_refused_batch_fails_and_the_producer_is_fatal() {
    let fault: Fault = Box::new(|r, _, _| {
        let mut c = Cur::new(&r.body);
        c.nstr().unwrap();
        c.i16().unwrap();
        c.i32().unwrap();
        c.i32().unwrap();
        let t = c.str().unwrap().to_string();
        Some(Answer::Body(refusal(&t, 0, 10))) // MESSAGE_TOO_LARGE
    });
    let (b, _) = sink(&["o"], 1, fault);
    let p = producer(&b, &[]);
    send(&p, "o", "big", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].1, h[0].4), (Some(0), Some(Code::MESSAGE_TOO_LARGE)));
    assert_eq!(p.fatal_error().unwrap().0, Code::MESSAGE_TOO_LARGE);
    assert!(h[0].3.as_ref().unwrap().contains("the broker's reason"), "{h:?}");
    let (e, o) = p.send(Record::to("o").value(b"after"), 2).unwrap_err();
    assert!(matches!(e, Error::Fatal(Code::MESSAGE_TOO_LARGE, _)) && o == 2, "{e}");
}

/// Without a broker, nothing is sent and every message times out (`message.timeout.ms`), not
/// fatal: no sequence was used.
#[test]
fn without_a_broker_messages_time_out() {
    let mut c = config("127.0.0.1:1");
    c.set("message.timeout.ms", "300").set("linger.ms", "1");
    let p = Producer::new(&c, Heard::default()).unwrap();
    let t = Instant::now();
    p.send(Record::to("o").value(b"x"), 1).unwrap();
    p.send(Record::to("o").value(b"y"), 2).unwrap();
    let h = heard(&p, 2);
    assert!(h.iter().all(|x| x.3.as_deref() == Some("message timed out (message.timeout.ms)")), "{h:?}");
    assert_eq!(h[0].1, None, "never given a partition");
    assert!(t.elapsed() >= Duration::from_millis(300) && t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
    assert!(p.fatal_error().is_none());
}

/// A batch in flight that times out (the broker never answers) fails, and the producer is fatal:
/// the broker may yet have written it.
#[test]
fn a_batch_in_flight_that_times_out_is_fatal() {
    let (b, _) = sink(&["o"], 1, Box::new(|_, _, _| Some(Answer::Silence)));
    let p = producer(&b, &[("message.timeout.ms", "400"), ("request.timeout.ms", "100"), ("socket.timeout.ms", "100")]);
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!(h[0].3.as_deref(), Some("message timed out (message.timeout.ms)"));
    assert_eq!(p.fatal_error().map(|f| f.0), Some(Code::MSG_TIMED_OUT));
}

#[test]
fn a_full_queue_refuses_the_message() {
    let p = Producer::new(config("127.0.0.1:1").set("queue.buffering.max.messages", "2"), Heard::default()).unwrap();
    p.send(Record::to("o").value(b"1"), 1).unwrap();
    p.send(Record::to("o").value(b"2"), 2).unwrap();
    let (e, o) = p.send(Record::to("o").value(b"3"), 3).unwrap_err();
    assert_eq!((e, o), (Error::QueueFull, 3));
    assert_eq!(p.in_flight_count(), 2);
}

/// Purging fails what is queued (Purged), and frees the queue.
#[test]
fn purging_the_queue() {
    let p = Producer::new(config("127.0.0.1:1").set("queue.buffering.max.messages", "2"), Heard::default()).unwrap();
    p.send(Record::to("o").value(b"1"), 1).unwrap();
    p.send(Record::to("o").value(b"2"), 2).unwrap();
    p.purge_queue();
    let h = heard(&p, 2);
    assert!(h.iter().all(|x| x.3.as_deref() == Some("purged from the producer's queue")));
    assert_eq!(p.in_flight_count(), 0);
    p.send(Record::to("o").value(b"3"), 3).unwrap();
}

/// A topic the cluster has not: its messages fail with UNKNOWN_TOPIC_OR_PARTITION at their
/// timeout, as librdkafka's do.
#[test]
fn an_unknown_topic_fails_its_messages() {
    let (b, _) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("message.timeout.ms", "300")]);
    p.send(Record::to("nope").value(b"x"), 1).unwrap();
    let h = heard(&p, 1);
    // the fake broker names only "o": "nope" is absent from the answer, so it never gets one
    assert!(h[0].3.is_some());
}

/// Keyless messages go to one partition a batch, then another (sticky partitioning), so every
/// partition gets some.
#[test]
fn keyless_messages_spread_over_the_partitions() {
    let (b, logs) = sink(&["o"], 4, no_fault());
    let p = producer(&b, &[("batch.num.messages", "5")]);
    for i in 0..200 {
        send(&p, "o", &format!("m{i}"), i);
    }
    heard(&p, 200);
    let counts: Vec<usize> = (0..4).map(|q| values(&logs, "o", q).len()).collect();
    assert_eq!(counts.iter().sum::<usize>(), 200);
    assert!(counts.iter().all(|c| *c > 0), "{counts:?}");
    // within a partition, in the order sent
    for q in 0..4 {
        let v: Vec<u64> = values(&logs, "o", q).iter().map(|s| s[1..].parse().unwrap()).collect();
        assert!(v.windows(2).all(|w| w[0] < w[1]), "partition {q}: {v:?}");
    }
}

#[test]
fn every_codec_is_written_as_the_broker_reads_it() {
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let (b, logs) = sink(&["o"], 1, no_fault());
        let p = producer(&b, &[("compression.type", codec)]);
        for i in 0..20 {
            send(&p, "o", &format!("{codec}{i}"), i);
        }
        heard(&p, 20);
        assert_eq!(values(&logs, "o", 0).len(), 20, "{codec}");
    }
}

/// A broker that refuses the producer an id (no IdempotentWrite permission) makes it fatal.
#[test]
fn a_producer_id_refused_is_fatal() {
    let b = Broker::start(|r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[("o", &[(0, 0)])])),
        wire::INIT_PRODUCER_ID => {
            let mut out = vec![];
            out.i32(0);
            out.i16(31); // CLUSTER_AUTHORIZATION_FAILED
            out.i64(-1);
            out.i16(-1);
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let p = producer(&b, &[]);
    // asked for at once: fatal before the first message, which is refused, or fails if it came first
    let until = Instant::now() + Duration::from_secs(5);
    while p.fatal_error().is_none() {
        assert!(Instant::now() < until, "never fatal");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(p.fatal_error().unwrap().0, Code::CLUSTER_AUTHORIZATION_FAILED);
    let (e, _) = p.send(Record::to("o").value(b"x"), 1).unwrap_err();
    assert!(matches!(e, Error::Fatal(Code::CLUSTER_AUTHORIZATION_FAILED, _)), "{e}");
}

/// Many messages over a connection that drops every few requests: each delivered once, in order.
#[test]
fn under_repeated_faults_each_message_lands_once_in_order() {
    let fault: Fault = Box::new(|r, i, logs| match i % 5 {
        1 => {
            let _ = produce(r, logs);
            Some(Answer::Drop)
        }
        3 => Some(Answer::Drop),
        _ => None,
    });
    let (b, logs) = sink(&["o"], 2, fault);
    let p = producer(&b, &[("batch.num.messages", "7"), ("linger.ms", "1")]);
    for i in 0..300 {
        send(&p, "o", &format!("m{i}"), i);
        if i % 50 == 0 {
            p.poll(Duration::ZERO);
        }
    }
    let h = heard(&p, 300);
    assert!(h.iter().all(|x| x.3.is_none()), "{:?}", h.iter().find(|x| x.3.is_some()));
    let mut all: Vec<String> = (0..2).flat_map(|q| values(&logs, "o", q)).collect();
    assert_eq!(all.len(), 300, "none twice, none lost");
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 300);
    assert!(p.fatal_error().is_none());
}

/// A producer whose threads did not run past its messages' deadline (the process stopped, then
/// resumed: SIGSTOP, a host stalled) sends none of them after, whichever of its threads runs
/// first: a broker thread takes no batch due past its first message's deadline, sealed now or
/// sent before. brrrrr's lease rests on it: nothing a holder sent lands once a standby may have
/// taken over (the acceptance suite's frozen holder).
#[test]
fn nothing_is_sent_past_its_deadline() {
    let p = Producer::new(config("127.0.0.1:1").set("linger.ms", "0"), Heard::default()).unwrap();
    let sh = &*p.shared;
    let mut st = sh.lock();
    st.pid = Some((1000, 0));
    let past = Instant::now() - Duration::from_millis(1);
    let msg = |o: u64| Pending {
        key: None,
        value: None,
        headers: vec![],
        timestamp: 0,
        partition: None,
        opaque: o,
        deadline: past,
        enqueued: past,
    };
    let mut part = Part { queue: VecDeque::new(), seq: 0, inflight: None, leader: Some(0) };
    part.queue.push_back(msg(1));
    let mut sent = Part { queue: VecDeque::new(), seq: 1, inflight: None, leader: Some(0) };
    sent.inflight =
        Some(Sent { msgs: vec![msg(2)], bytes: Arc::new(vec![]), sending: false, retry_at: past, attempts: 1 });
    let mut t =
        Topic { partitions: vec![0, 1], sticky: 0, unassigned: VecDeque::new(), parts: HashMap::new(), missing: None };
    t.parts.insert(0, part);
    t.parts.insert(1, sent);
    st.topics.insert("o".into(), t);
    (st.outstanding, st.queued, st.queued_bytes) = (2, 2, 2 * msg(0).size());
    let (req, _) = take_due(sh, &mut st, 0, Instant::now());
    assert!(
        req.is_empty(),
        "a batch past its deadline taken to send: {:?}",
        req.iter().map(|r| (&r.0, r.1)).collect::<Vec<_>>()
    );
    assert_eq!(st.reports.len(), 2, "both failed instead");
    assert!(st.reports.iter().all(|r| r.3 == Some(Error::MessageTimedOut)));
    assert!(st.fatal.is_some(), "the one sent before may have landed: fatal");
}

/// The producer's id is asked for at once, not at the first message (as librdkafka's idempotent
/// producer does): the first message waits on no more round trips than it must.
#[test]
fn the_producer_id_is_asked_for_at_once() {
    let (b, _) = sink(&["o"], 1, no_fault());
    let _p = producer(&b, &[]);
    let until = Instant::now() + Duration::from_secs(5);
    while b.requests(wire::INIT_PRODUCER_ID).is_empty() {
        assert!(Instant::now() < until, "no InitProducerId before a message");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Fatal, the producer sends nothing more, whichever of its threads runs first: what is queued
/// fails with the fatal error instead.
#[test]
fn a_fatal_producer_takes_nothing_to_send() {
    let p = Producer::new(config("127.0.0.1:1").set("linger.ms", "0"), Heard::default()).unwrap();
    let sh = &*p.shared;
    let mut st = sh.lock();
    st.pid = Some((1000, 0));
    let later = Instant::now() + Duration::from_secs(60);
    let mut part = Part { queue: VecDeque::new(), seq: 0, inflight: None, leader: Some(0) };
    let early = Instant::now() - Duration::from_secs(1);
    part.queue.push_back(Pending {
        key: None,
        value: None,
        headers: vec![],
        timestamp: 0,
        partition: None,
        opaque: 1,
        deadline: later,
        enqueued: early,
    });
    let mut t =
        Topic { partitions: vec![0], sticky: 0, unassigned: VecDeque::new(), parts: HashMap::new(), missing: None };
    t.parts.insert(0, part);
    st.topics.insert("o".into(), t);
    (st.outstanding, st.queued, st.queued_bytes) = (1, 1, 16);
    st.set_fatal(Code::OUT_OF_ORDER_SEQUENCE_NUMBER, "a gap".into());
    let (req, _) = take_due(sh, &mut st, 0, Instant::now());
    assert!(req.is_empty(), "a fatal producer sent a batch");
}

fn log_of<'a>(logs: &'a Logs) -> std::sync::MutexGuard<'a, HashMap<(String, i32), Log>> {
    logs.lock().unwrap()
}

/// Each record's timestamp is when it was sent (CreateTime, ms): what brrrrr's read-backs and
/// the lease's step-down read as when a record was written.
#[test]
fn records_carry_the_time_they_were_sent() {
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[]);
    let wall = || SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64;
    let before = wall();
    send(&p, "o", "x", 1);
    heard(&p, 1);
    let after = wall();
    let t = log_of(&logs)[&("o".into(), 0)].stamps[0];
    assert!((before..=after).contains(&t), "{before} <= {t} <= {after}");
}

/// `queue.buffering.max.kbytes` bounds what is queued by its size (value, headers, 16 bytes of a
/// record's overhead): a message that fills it exactly is taken, one byte more is refused, and
/// what a purge gives back is taken again.
#[test]
fn the_queue_is_bounded_by_bytes_and_gives_them_back() {
    let mut c = config("127.0.0.1:1");
    c.set("queue.buffering.max.kbytes", "1").set("message.timeout.ms", "300000");
    let p = Producer::new(&c, Heard::default()).unwrap();
    p.send(Record::to("o").value(&[0u8; 1024 - 16]), 1).unwrap(); // 1024 with its overhead: exactly the bound
    assert_eq!(p.send(Record::to("o").value(b""), 2).unwrap_err().0, Error::QueueFull, "16 bytes past it");
    p.purge_queue();
    heard(&p, 1);
    p.send(Record::to("o").value(&[0u8; 1024 - 16]), 3).unwrap(); // what the purge gave back
    p.purge_queue();
    heard(&p, 2);
    // headers count: 496 + 496 + 16 = 1008, so 16 bytes are left, a message with no value
    let h = vec![("k".repeat(496), Some(vec![0u8; 496]))];
    p.send(Record::to("o").value(b"").headers(&h), 4).unwrap();
    assert_eq!(p.send(Record::to("o").value(b"x"), 5).unwrap_err().0, Error::QueueFull);
    p.send(Record::to("o").value(b""), 6).unwrap();
    assert_eq!(p.in_flight_count(), 2);
}

/// A message too large for any batch (`message.max.bytes`, a batch's header included) is refused
/// at once, as the broker would refuse it (MESSAGE_TOO_LARGE), not queued to fail later.
#[test]
fn a_message_larger_than_a_batch_is_refused_at_once() {
    let mut c = config("127.0.0.1:1");
    c.set("message.max.bytes", "1000");
    let p = Producer::new(&c, Heard::default()).unwrap();
    let fits = 1000 - records::HEADER - 16;
    p.send(Record::to("o").value(&vec![0u8; fits]), 1).unwrap();
    let (e, o) = p.send(Record::to("o").value(&vec![0u8; fits + 1]), 2).unwrap_err();
    assert_eq!((e.code(), o), (Some(Code::MESSAGE_TOO_LARGE), 2));
}

/// Messages that fill a batch (`batch.size` in bytes, `batch.num.messages`) are sent at once
/// without lingering, the rest in batches of their own; one that does not fill a batch lingers
/// `linger.ms` for more.
#[test]
fn batches_are_cut_at_their_size_and_sent_full_without_lingering() {
    let (b, logs) = sink(&["o"], 1, no_fault());
    // 5 messages of 116 bytes each (100 + 16), batches of at most 250 bytes past their header: 2 a batch
    let p = producer(&b, &[("batch.size", &(records::HEADER + 2 * 116).to_string()), ("linger.ms", "60000")]);
    let t = Instant::now();
    for i in 0..4 {
        p.send(Record::to("o").value(&[b'a' + i as u8; 100]), i).unwrap();
    }
    heard(&p, 4);
    assert!(t.elapsed() < Duration::from_secs(5), "full batches lingered: {:?}", t.elapsed());
    assert_eq!(log_of(&logs)[&("o".into(), 0)].batches, 2, "two messages a batch");
    // one more: not a full batch, it lingers (here past the test), so nothing is sent yet
    p.send(Record::to("o").value(&[b'z'; 100]), 9).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(log_of(&logs)[&("o".into(), 0)].values.len(), 4, "a batch not full is sent only once lingered");
    // by count too
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("batch.num.messages", "3"), ("linger.ms", "60000")]);
    for i in 0..6 {
        p.send(Record::to("o").value(b"v"), i).unwrap();
    }
    heard(&p, 6);
    assert_eq!(log_of(&logs)[&("o".into(), 0)].batches, 2, "three messages a batch");
}

/// `flush` returns once every outcome is handed over, or says how many were not within its time.
#[test]
fn flush_waits_for_every_outcome_or_times_out() {
    let p = Producer::new(config("127.0.0.1:1").set("message.timeout.ms", "60000"), Heard::default()).unwrap();
    p.send(Record::to("o").value(b"x"), 1).unwrap();
    let t = Instant::now();
    let e = p.flush(Duration::from_millis(300)).unwrap_err();
    assert!(matches!(&e, Error::Timeout(m) if m.contains("1 messages")), "{e}");
    assert!(t.elapsed() >= Duration::from_millis(300) && t.elapsed() < Duration::from_secs(2));
    let (b, _) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[]);
    send(&p, "o", "x", 1);
    p.flush(Duration::from_secs(10)).unwrap();
    assert_eq!(p.context().0.lock().unwrap().len(), 1, "handed over by flush");
}

/// Dropped, the producer sends nothing it had queued: its threads end.
#[test]
fn a_dropped_producer_sends_nothing_more() {
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("linger.ms", "300")]);
    send(&p, "o", "late", 1);
    drop(p);
    std::thread::sleep(Duration::from_millis(800));
    assert!(log_of(&logs).values().all(|l| l.values.is_empty()), "sent after the drop");
}

/// A producer id refused for now (its coordinator not available yet) is asked for again: the
/// producer is not fatal, and its messages are delivered.
#[test]
fn a_producer_id_refused_for_now_is_asked_again() {
    let tries = Arc::new(AtomicUsize::new(0));
    let t2 = tries.clone();
    let logs: Logs = Arc::default();
    let l2 = logs.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[("o", &[(0, 0)])])),
        wire::INIT_PRODUCER_ID => {
            let mut out = vec![];
            out.i32(0);
            out.i16(if t2.fetch_add(1, Ordering::SeqCst) < 2 { 15 } else { 0 }); // COORDINATOR_NOT_AVAILABLE
            out.i64(1000);
            out.i16(0);
            Answer::Body(out)
        }
        wire::PRODUCE => Answer::Body(produce(r, &l2)),
        _ => Answer::Drop,
    });
    let p = producer(&b, &[]);
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!(h[0].3, None, "{h:?}");
    assert!(p.fatal_error().is_none());
    assert!(tries.load(Ordering::SeqCst) >= 3);
}

/// A broker that answers with a throttle time (KIP-219) gets no request before it is over.
#[test]
fn a_throttling_broker_is_waited_for() {
    let logs: Logs = Arc::default();
    let l2 = logs.clone();
    let times = Arc::new(Mutex::new(vec![]));
    let t2 = times.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[("o", &[(0, 0)])])),
        wire::INIT_PRODUCER_ID => {
            let mut out = vec![];
            out.i32(0);
            out.i16(0);
            out.i64(1000);
            out.i16(0);
            Answer::Body(out)
        }
        wire::PRODUCE => {
            t2.lock().unwrap().push(Instant::now());
            let mut out = produce(r, &l2);
            let n = out.len();
            out[n - 4..].copy_from_slice(&400i32.to_be_bytes()); // throttle_time_ms
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let p = producer(&b, &[("linger.ms", "0")]);
    send(&p, "o", "a", 1);
    heard(&p, 1);
    send(&p, "o", "b", 2);
    heard(&p, 2);
    let t = times.lock().unwrap().clone();
    assert!(t[1] - t[0] >= Duration::from_millis(400), "{:?}", t[1] - t[0]);
}

/// A DUPLICATE_SEQUENCE_NUMBER is a delivery (the batch was written before, its answer lost), at
/// an offset the broker did not say, as librdkafka takes it; the producer goes on.
#[test]
fn a_duplicate_sequence_is_delivered_at_no_offset() {
    let (b, _) = sink(
        &["o"],
        1,
        Box::new(|r, i, logs| {
            (i == 0).then(|| {
                let _ = produce(r, logs); // written, as a broker answering DUPLICATE_SEQUENCE_NUMBER has
                let mut c = Cur::new(&r.body);
                c.nstr().unwrap();
                c.i16().unwrap();
                c.i32().unwrap();
                c.i32().unwrap();
                let t = c.str().unwrap().to_string();
                let mut b = refusal(&t, 0, 46);
                // whatever offset comes with it, it is not this write's
                b[4 + 2 + t.len() + 4 + 4 + 2..][..8].copy_from_slice(&0i64.to_be_bytes());
                Answer::Body(b)
            })
        }),
    );
    let p = producer(&b, &[]);
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].1, h[0].2, h[0].3.as_ref()), (Some(0), None, None), "{h:?}");
    assert!(p.fatal_error().is_none());
    send(&p, "o", "y", 2);
    assert_eq!(heard(&p, 2)[1].2, Some(1), "the next batch after it, in sequence");
}

/// Fatal, the producer fails what it had queued with the fatal error, and sends none of it.
#[test]
fn a_fatal_producer_fails_what_it_queued() {
    let p = Producer::new(config("127.0.0.1:1").set("message.timeout.ms", "60000"), Heard::default()).unwrap();
    p.send(Record::to("o").value(b"q"), 1).unwrap();
    p.shared.lock().set_fatal(Code::PRODUCER_FENCED, "fenced".into());
    let h = heard(&p, 1);
    assert_eq!(h[0].4, Some(Code::PRODUCER_FENCED), "{h:?}");
}

/// Keyless messages go to another partition at each rotation, never the one they leave, and
/// always one the topic has; a topic of one partition stays on it.
#[test]
fn rotation_moves_to_another_partition() {
    let mut rng = 7u64;
    for n in 1..6 {
        let mut t: Topic<u64> = Topic {
            partitions: (0..n).collect(),
            sticky: 0,
            unassigned: VecDeque::new(),
            parts: HashMap::new(),
            missing: None,
        };
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let was = t.sticky;
            t.rotate(xorshift(&mut rng));
            assert!(t.sticky < n as usize);
            assert!(n == 1 || t.sticky != was, "{n} partitions: stayed on {was}");
            seen.insert(t.sticky);
        }
        assert_eq!(seen.len(), n as usize, "every partition reached");
    }
    let (a, b) = (xorshift(&mut 1), xorshift(&mut 2));
    assert_ne!(a, b);
    let mut r = 1;
    let steps: Vec<u64> = (0..4).map(|_| xorshift(&mut r)).collect();
    // the xorshift64 step: x ^= x<<13, x>>7, x<<17 (past the first, bits overlap: `^` is not `|`)
    assert_eq!(steps, [1082269761, 1152992998833853505, 11177516664432764457, 17678023832001937445]);
}

/// `queue.buffering.max.messages` counts what is queued: a message delivered frees its place.
/// One `poll` waits for an outcome. And the producer says what it has in flight.
#[test]
fn a_delivered_message_frees_its_place_in_the_queue() {
    let (b, _) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("queue.buffering.max.messages", "1")]);
    for i in 0..3 {
        send(&p, "o", "x", i);
        assert_eq!(p.poll(Duration::from_secs(5)), 1, "one poll waits for the outcome");
    }
    assert_eq!(format!("{p:?}"), "Producer { in_flight: 0, .. }");
    // and its bytes, against `queue.buffering.max.kbytes`
    let (b, _) = sink(&["o"], 1, no_fault()); // a broker of its own: the fake gives every producer one id
    let p = producer(&b, &[("queue.buffering.max.kbytes", "1")]);
    for i in 0..3 {
        p.send(Record::to("o").value(&[0u8; 900]), i).unwrap();
        assert_eq!(p.poll(Duration::from_secs(5)), 1, "the bytes of the one before given back");
    }
}

/// A topic first sent to once the producer is running (its id and first metadata had) is looked
/// up then, and its messages delivered.
#[test]
fn a_topic_sent_to_later_is_looked_up() {
    let (b, _) = sink(&["o", "later"], 1, no_fault());
    let p = producer(&b, &[]);
    send(&p, "o", "x", 1);
    heard(&p, 1);
    send(&p, "later", "y", 2);
    let h = heard(&p, 2);
    assert!(h.iter().all(|d| d.3.is_none()), "{h:?}");
    // one producer id for the producer's life: a new one would restart its sequences, which the
    // broker refuses as out of order
    assert_eq!(b.requests(wire::INIT_PRODUCER_ID).len(), 1);
}

/// A message larger than `batch.size` (and within `message.max.bytes`) goes in a batch alone.
#[test]
fn a_message_larger_than_batch_size_goes_alone() {
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("batch.size", "100")]);
    p.send(Record::to("o").value(&[b'v'; 500]), 1).unwrap();
    p.send(Record::to("o").value(&[b'w'; 500]), 2).unwrap();
    let h = heard(&p, 2);
    assert!(h.iter().all(|d| d.3.is_none()), "{h:?}");
    assert_eq!(log_of(&logs)[&("o".into(), 0)].batches, 2);
}

/// A producer id refused for now is asked again with a wait doubling from `retry.backoff.ms`.
#[test]
fn a_producer_id_is_asked_again_with_a_growing_wait() {
    let b = Broker::start(|r| match r.api {
        wire::INIT_PRODUCER_ID => {
            let mut out = vec![];
            out.i32(0);
            out.i16(15); // COORDINATOR_NOT_AVAILABLE
            out.i64(-1);
            out.i16(-1);
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let _p = producer(&b, &[("retry.backoff.ms", "50"), ("retry.backoff.max.ms", "1000")]);
    std::thread::sleep(Duration::from_secs(1));
    // at 0, 50, 150, 350 and 750 ms
    let n = b.requests(wire::INIT_PRODUCER_ID).len();
    assert!((3..=7).contains(&n), "{n} tries in 1 s");
}

/// The wait between failed lookups never holds back a message's timeout: it times out when due,
/// not at the end of a `retry.backoff.ms` longer than it.
#[test]
fn a_lookup_backoff_never_holds_back_a_timeout() {
    let mut c = config("127.0.0.1:1");
    c.set("message.timeout.ms", "300").set("retry.backoff.ms", "2000").set("retry.backoff.max.ms", "2000");
    let p = Producer::new(&c, Heard::default()).unwrap();
    std::thread::sleep(Duration::from_millis(50)); // the first lookups failed: in their backoff
    let t = Instant::now();
    p.send(Record::to("o").value(b"x"), 1).unwrap();
    // one poll, waiting long: it wakes at the message's deadline, not at the end of its own wait
    assert_eq!(p.poll(Duration::from_secs(5)), 1);
    let h = p.context().0.lock().unwrap().clone();
    assert_eq!(h[0].3.as_deref(), Some("message timed out (message.timeout.ms)"));
    assert!(t.elapsed() < Duration::from_millis(1000), "{:?}", t.elapsed());
}

/// A topic without partitions yet (being created) is looked up again after `retry.backoff.ms`,
/// not at once without end; once it has them, what was sent to it is delivered.
#[test]
fn a_topic_without_partitions_is_looked_up_with_backoff() {
    let (logs, lookups): (Logs, _) = (Arc::default(), Arc::new(AtomicUsize::new(0)));
    let (l2, n2) = (logs.clone(), lookups.clone());
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => {
            let ps: &[(i32, i32)] = if n2.fetch_add(1, Ordering::SeqCst) < 4 { &[] } else { &[(0, 0)] };
            Answer::Body(metadata(&[(0, &r.me)], &[("o", ps)]))
        }
        wire::INIT_PRODUCER_ID => {
            let mut out = vec![];
            out.i32(0);
            out.i16(0);
            out.i64(1000);
            out.i16(0);
            Answer::Body(out)
        }
        wire::PRODUCE => Answer::Body(produce(r, &l2)),
        _ => Answer::Drop,
    });
    let p = producer(&b, &[("retry.backoff.ms", "100"), ("retry.backoff.max.ms", "100")]);
    let t = Instant::now();
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].1, h[0].3.as_deref()), (Some(0), None), "{h:?}");
    assert!(t.elapsed() >= Duration::from_millis(250), "looked up 4 times, 100 ms apart: {:?}", t.elapsed());
    assert!(lookups.load(Ordering::SeqCst) <= 6, "{} lookups", lookups.load(Ordering::SeqCst));
}

/// A broker leading partition `p` of "o" for each of `answers` (its Produce answer), every
/// broker named in each one's metadata. Its answers by partition.
fn leaders(answers: Vec<Box<dyn Fn(&Req, &Logs) -> Answer + Send + Sync>>) -> (Vec<Broker>, Logs) {
    let logs: Logs = Arc::default();
    let addrs = Arc::new(std::sync::OnceLock::<Vec<String>>::new());
    let bs: Vec<Broker> = answers
        .into_iter()
        .map(|answer| {
            let (a2, l2) = (addrs.clone(), logs.clone());
            Broker::start(move |r| match r.api {
                wire::METADATA => {
                    let a = a2.get().unwrap();
                    let brokers: Vec<(i32, &str)> = a.iter().enumerate().map(|(i, x)| (i as i32, x.as_str())).collect();
                    let ps: Vec<(i32, i32)> = (0..a.len() as i32).map(|p| (p, p)).collect();
                    Answer::Body(metadata(&brokers, &[("o", &ps)]))
                }
                wire::INIT_PRODUCER_ID => {
                    let mut out = vec![];
                    out.i32(0);
                    out.i16(0);
                    out.i64(1000);
                    out.i16(0);
                    Answer::Body(out)
                }
                wire::PRODUCE => answer(r, &l2),
                _ => Answer::Drop,
            })
        })
        .collect();
    addrs.set(bs.iter().map(|b| b.addr.clone()).collect()).unwrap();
    (bs, logs)
}

/// Fatal on one broker's answer, the producer fails what it queued, not a batch another broker
/// has in flight: that one is given its own outcome when it comes (here, its timeout).
#[test]
fn fatal_spares_a_batch_in_flight_elsewhere() {
    let (bs, _) = leaders(vec![
        Box::new(|r, _| {
            let mut c = Cur::new(&r.body);
            c.nstr().unwrap();
            c.i16().unwrap();
            c.i32().unwrap();
            c.i32().unwrap();
            let t = c.str().unwrap().to_string();
            c.i32().unwrap();
            // late: both batches in flight by then
            std::thread::sleep(Duration::from_millis(300));
            Answer::Body(refusal(&t, c.i32().unwrap(), 10)) // MESSAGE_TOO_LARGE: fatal
        }),
        Box::new(|_, _| Answer::Silence),
    ]);
    let p = producer(&bs[0], &[("batch.num.messages", "1"), ("message.timeout.ms", "1000")]);
    let t = Instant::now();
    send(&p, "o", "a", 1);
    send(&p, "o", "b", 2); // a batch a partition: the other partition, the other broker
    let h = heard(&p, 1);
    assert_eq!(h[0].4, Some(Code::MESSAGE_TOO_LARGE), "{h:?}");
    let h = heard(&p, 2);
    assert!(t.elapsed() >= Duration::from_millis(900), "the other in flight until its timeout: {h:?}");
    assert_eq!(h[1].3.as_deref(), Some("message timed out (message.timeout.ms)"), "{h:?}");
}

/// One request carries the batches of several partitions: each delivery has its own
/// partition's offset.
#[test]
fn each_partition_of_a_request_has_its_own_offset() {
    let (b, logs) = sink(&["o"], 2, no_fault());
    logs.lock().unwrap().entry(("o".into(), 1)).or_default().values = vec![(vec![], vec![]); 100];
    let p = producer(&b, &[("batch.num.messages", "1"), ("linger.ms", "50")]);
    send(&p, "o", "a", 1);
    send(&p, "o", "b", 2); // the other partition
    let mut h = heard(&p, 2);
    h.sort_by_key(|x| x.1);
    assert_eq!(h.iter().map(|x| (x.1, x.2)).collect::<Vec<_>>(), [(Some(0), Some(0)), (Some(1), Some(100))]);
    assert_eq!(b.requests(wire::PRODUCE).len(), 1, "both in one request");
}

/// A keyless batch sealed (its linger over), the next messages go to another partition
/// (KIP-480); a producer's first partition is drawn at random, so producers spread.
#[test]
fn each_batch_sealed_moves_to_another_partition_and_the_first_is_random() {
    let (b, _) = sink(&["o"], 4, no_fault());
    let p = producer(&b, &[]);
    send(&p, "o", "a", 1);
    heard(&p, 1);
    send(&p, "o", "b", 2);
    let h = heard(&p, 2);
    assert_ne!(h[0].1, h[1].1, "{h:?}");
    let firsts: std::collections::HashSet<Option<i32>> = (0..8)
        .map(|i| {
            let p = producer(&b, &[]);
            send(&p, "o", "x", i);
            heard(&p, 1)[0].1
        })
        .collect();
    assert!(firsts.len() > 1, "8 producers, all first on {firsts:?}");
}

/// One thread, one connection, a leader: however many topics and lookups.
#[test]
fn one_connection_a_leader() {
    let (b, _) = sink(&["o", "p"], 2, no_fault());
    let p = producer(&b, &[]);
    send(&p, "o", "a", 1);
    heard(&p, 1);
    send(&p, "p", "b", 2);
    heard(&p, 2);
    let conns: std::collections::HashSet<usize> = b.requests(wire::PRODUCE).iter().map(|r| r.conn).collect();
    assert_eq!(conns.len(), 1, "{conns:?}");
    assert_eq!(p.shared.lock().threads, [0], "one thread for the leader, however many lookups");
}

/// A partition's error a retry may mend (NOT_ENOUGH_REPLICAS) sends its batch again after
/// `retry.backoff.ms` times its attempts; a connection lost, after a backoff doubling from it.
#[test]
fn retries_wait_their_backoff() {
    let fault: Fault = Box::new(|r, i, _| {
        (i < 2).then(|| {
            let mut c = Cur::new(&r.body);
            c.nstr().unwrap();
            c.i16().unwrap();
            c.i32().unwrap();
            c.i32().unwrap();
            Answer::Body(refusal(c.str().unwrap(), 0, 19))
        })
    });
    let (b, _) = sink(&["o"], 1, fault);
    let p = producer(&b, &[("retry.backoff.ms", "100")]);
    let t = Instant::now();
    send(&p, "o", "x", 1);
    assert_eq!(heard(&p, 1)[0].3, None);
    assert!(t.elapsed() >= Duration::from_millis(280), "100 then 200 ms: {:?}", t.elapsed());

    let (b, _) = sink(&["o"], 1, Box::new(|_, i, _| (i < 2).then_some(Answer::Drop)));
    let p = producer(&b, &[("retry.backoff.ms", "100")]);
    let t = Instant::now();
    send(&p, "o", "x", 1);
    assert_eq!(heard(&p, 1)[0].3, None);
    assert!(t.elapsed() >= Duration::from_millis(280), "100 then 200 ms: {:?}", t.elapsed());
}

/// A batch is full, and sent without lingering, once the next message would not fit, though an
/// empty record would; a message that fills the batch exactly is in it; a batch with room for
/// no more than an empty record lingers.
#[test]
fn a_batch_full_by_its_bytes() {
    let s = 100 + 16; // a message of 100 bytes, no header, with its record's overhead
    let send = |p: &Producer<Heard>, o| p.send(Record::to("o").value(&[b'v'; 100]), o).unwrap();
    let size = |b: usize| b.to_string();
    // two fit, with room for an empty record but not a third
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("linger.ms", "10000"), ("batch.size", &size(records::HEADER + 2 * s + 50))]);
    let t = Instant::now();
    for i in 0..3 {
        send(&p, i);
    }
    heard(&p, 2);
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert_eq!(log_of(&logs)[&("o".into(), 0)].values.len(), 2);
    // two that fill it exactly: one batch
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(
        &b,
        &[("linger.ms", "10000"), ("batch.num.messages", "2"), ("batch.size", &size(records::HEADER + 2 * s))],
    );
    send(&p, 1);
    send(&p, 2);
    heard(&p, 2);
    assert_eq!(log_of(&logs)[&("o".into(), 0)].batches, 1);
    // room for exactly an empty record: lingers
    let (b, _) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("linger.ms", "300"), ("batch.size", &size(records::HEADER + s + 16))]);
    let t = Instant::now();
    send(&p, 1);
    heard(&p, 1);
    assert!(t.elapsed() >= Duration::from_millis(280), "lingered: {:?}", t.elapsed());
}

/// Dropped, the producer's threads all end (each holds its shared state: none is left).
#[test]
fn a_dropped_producers_threads_end() {
    let (b, _) = sink(&["o"], 2, no_fault());
    let p = producer(&b, &[]);
    send(&p, "o", "x", 1);
    heard(&p, 1);
    let shared = Arc::downgrade(&p.shared);
    drop(p);
    let until = Instant::now() + Duration::from_secs(2);
    while shared.upgrade().is_some() {
        assert!(Instant::now() < until, "a thread still running");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Kafka's `Utils.murmur2`, as its `UtilsTest` gives it.
#[test]
fn murmur2_is_kafkas() {
    for (k, h) in [
        ("21", -973932308),
        ("foobar", -790332482),
        ("a-little-bit-long-string", -985981536),
        ("a-little-bit-longer-string", -1486304829),
        ("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
        ("abc", 479470107),
    ] {
        assert_eq!(murmur2(k.as_bytes()), h, "{k}");
    }
}

/// A keyed record goes to its key's partition, as Java's default partitioner picks it; one that
/// names its partition, to it; each record's key, value (or none: a tombstone) and timestamp as
/// given, on the log.
#[test]
fn records_go_to_their_keys_or_their_own_partition() {
    let (b, logs) = sink(&["o"], 5, no_fault());
    let p = producer(&b, &[]);
    let keys = ["21", "foobar", "abc", "a-little-bit-long-string"];
    for (i, k) in keys.iter().enumerate() {
        p.send(Record::to("o").key(k.as_bytes()).value(b"v"), i as u64).unwrap();
    }
    p.send(Record::to("o").key(b"abc").partition(4).timestamp(1234), 9).unwrap(); // a tombstone
    let h = heard(&p, 5);
    for (i, k) in keys.iter().enumerate() {
        let want = (murmur2(k.as_bytes()) & 0x7fffffff) % 5;
        let got = h.iter().find(|x| x.5 == i as u64).unwrap();
        assert_eq!((got.1, got.3.as_ref(), got.6), (Some(want), None, Persisted::Yes), "{k}: {h:?}");
    }
    // as Java's: "21" (2^31 - 973932308 = 1173551340) to partition 0 of 5, "foobar" to 1
    assert_eq!((h.iter().find(|x| x.5 == 0).unwrap().1, h.iter().find(|x| x.5 == 1).unwrap().1), (Some(0), Some(1)));
    let l = log_of(&logs);
    let p4 = &l[&("o".into(), 4)];
    let at = p4.records.iter().position(|r| r.1.is_none()).expect("the tombstone");
    assert_eq!(p4.records[at].0.as_deref(), Some(&b"abc"[..]));
    assert_eq!(p4.stamps[at], 1234);
}

/// A record naming a partition the topic has not fails (UNKNOWN_TOPIC_OR_PARTITION), once the
/// topic's partitions are known; others to the topic are delivered.
#[test]
fn a_partition_the_topic_has_not_fails_its_record() {
    let (b, _) = sink(&["o"], 2, no_fault());
    let p = producer(&b, &[]);
    p.send(Record::to("o").value(b"x").partition(7), 1).unwrap(); // its partitions not known yet
    let h = heard(&p, 1);
    assert_eq!(h[0].4, Some(Code::UNKNOWN_TOPIC_OR_PARTITION), "{h:?}");
    p.send(Record::to("o").value(b"x").partition(2), 2).unwrap(); // known now
    p.send(Record::to("o").value(b"y").partition(1), 3).unwrap();
    let h = heard(&p, 3);
    assert_eq!(h.iter().find(|x| x.5 == 2).unwrap().4, Some(Code::UNKNOWN_TOPIC_OR_PARTITION), "{h:?}");
    assert_eq!(h.iter().find(|x| x.5 == 3).unwrap().1, Some(1), "{h:?}");
}
