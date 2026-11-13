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

/// librdkafka's duplicate: a batch written but its answer
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
/// written it, and a gap may follow), possibly persisted. It is on the log once: an application
/// reading the log back after a failed delivery finds it there.
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
    assert_eq!((h[at(1)].2, h[at(1)].6), (None, Persisted::Possibly), "not delivered, maybe written");
    assert!(h[at(2)].3.is_some(), "{h:?}");
    assert_eq!(p.fatal_error().map(|f| f.0), Some(Code::MSG_TIMED_OUT));
    assert_eq!(values(&logs, "o", 0), ["a"], "written once though failed; b never");
}

/// At a batch's timeout its request still unanswered is given up: the connection it went on is
/// closed by then, and nothing is sent after (a fatal producer sends nothing). A request the
/// broker read before may still be written after, by the broker: what an application fencing on
/// the message timeout must allow for past it.
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

/// A batch refused as out of order fails its messages with the broker's code, and the producer is
/// fatal: its partition's sequence has a gap the broker would refuse every later batch for.
#[test]
fn a_batch_out_of_order_fails_and_the_producer_is_fatal() {
    let fault: Fault = Box::new(|r, _, _| {
        let mut c = Cur::new(&r.body);
        c.nstr().unwrap();
        c.i16().unwrap();
        c.i32().unwrap();
        c.i32().unwrap();
        let t = c.str().unwrap().to_string();
        Some(Answer::Body(refusal(&t, 0, 45))) // OUT_OF_ORDER_SEQUENCE_NUMBER
    });
    let (b, _) = sink(&["o"], 1, fault);
    let p = producer(&b, &[]);
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].1, h[0].4), (Some(0), Some(Code::OUT_OF_ORDER_SEQUENCE_NUMBER)));
    assert_eq!(p.fatal_error().unwrap().0, Code::OUT_OF_ORDER_SEQUENCE_NUMBER);
    assert!(h[0].3.as_ref().unwrap().contains("the broker's reason"), "{h:?}");
    let (e, o) = p.send(Record::to("o").value(b"after"), 2).unwrap_err();
    assert!(matches!(e, Error::Fatal(Code::OUT_OF_ORDER_SEQUENCE_NUMBER, _)) && o == 2, "{e}");
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
    assert_eq!((h[0].1, h[0].6), (None, Persisted::No), "never given a partition, never sent");
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
    assert_eq!((h[0].3.as_deref(), h[0].6), (Some("message timed out (message.timeout.ms)"), Persisted::Possibly));
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
/// sent before. A fence on the message timeout rests on it: nothing a stalled producer sent lands
/// once another may have taken over.
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
    sent.inflight = Some(Sent {
        msgs: vec![msg(2)],
        bytes: Arc::new(vec![]),
        seq: 0,
        deadline: past,
        sending: false,
        by: 0,
        retry_at: past,
        attempts: 1,
        maybe_written: true, // its answer lost
    });
    let mut t = Topic {
        partitions: vec![0, 1],
        sticky: 0,
        unassigned: VecDeque::new(),
        parts: HashMap::default(),
        missing: None,
    };
    t.parts.insert(0, part);
    t.parts.insert(1, sent);
    st.topics.insert("o".into(), t);
    (st.outstanding, st.queued, st.queued_bytes) = (2, 2, 2 * msg(0).size());
    let (req, fresh, _, _) = take_due(sh, &mut st, 0, 0, Instant::now());
    assert!(fresh.is_empty(), "a batch past its deadline sealed to send");
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
        Topic { partitions: vec![0], sticky: 0, unassigned: VecDeque::new(), parts: HashMap::default(), missing: None };
    t.parts.insert(0, part);
    st.topics.insert("o".into(), t);
    (st.outstanding, st.queued, st.queued_bytes) = (1, 1, 16);
    st.set_fatal(Code::OUT_OF_ORDER_SEQUENCE_NUMBER, "a gap".into());
    let (req, fresh, _, _) = take_due(sh, &mut st, 0, 0, Instant::now());
    assert!(req.is_empty() && fresh.is_empty(), "a fatal producer sent a batch");
}

fn log_of<'a>(logs: &'a Logs) -> std::sync::MutexGuard<'a, HashMap<(String, i32), Log>> {
    logs.lock().unwrap()
}

/// Each record's timestamp is when it was sent (CreateTime, ms), unless the record gives its own.
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

/// `queue.buffering.max.kbytes` bounds what is queued by its size (key, value, headers and their
/// framing, 16 bytes of a record's overhead): a message that fills it exactly is taken, one byte
/// more is refused, and what a purge gives back is taken again.
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
    // headers count, with their framing: 491 + 491 + 10 + 16 = 1008, so 16 bytes are left, a
    // message with no value
    let h = vec![("k".repeat(491), Some(vec![0u8; 491]))];
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
            parts: HashMap::default(),
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
            Answer::Body(refusal(&t, c.i32().unwrap(), 45)) // OUT_OF_ORDER_SEQUENCE_NUMBER: fatal
        }),
        Box::new(|_, _| Answer::Silence),
    ]);
    let p = producer(&bs[0], &[("batch.num.messages", "1"), ("message.timeout.ms", "1000")]);
    let t = Instant::now();
    send(&p, "o", "a", 1);
    send(&p, "o", "b", 2); // a batch a partition: the other partition, the other broker
    let h = heard(&p, 1);
    assert_eq!(h[0].4, Some(Code::OUT_OF_ORDER_SEQUENCE_NUMBER), "{h:?}");
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

/// A broker answering InitProducerId with id 1000, epoch 0.
fn pid_answer() -> Answer {
    let mut out = vec![];
    out.i32(0);
    out.i16(0);
    out.i64(1000);
    out.i16(0);
    Answer::Body(out)
}

/// Delivery reports nobody polls count against `queue.buffering.max.messages` until `poll` hands
/// them over (librdkafka counts a message until its report is served): the reports held are
/// bounded too.
#[test]
fn unpolled_reports_count_against_the_queue_bound() {
    let (b, _) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("queue.buffering.max.messages", "1")]);
    let mut accepted = 0;
    let until = Instant::now() + Duration::from_secs(1);
    while accepted < 50 && Instant::now() < until {
        if p.send(Record::to("o").value(b"x"), accepted).is_ok() {
            accepted += 1;
        } else {
            std::thread::sleep(Duration::from_millis(2)); // never polled
        }
    }
    assert_eq!(p.in_flight_count(), 1, "with a queue bound of 1");
    assert_eq!(heard(&p, 1).len(), 1);
    p.send(Record::to("o").value(b"x"), 99).unwrap(); // its place given back by poll
}

/// A Produce request holds the due batches of a broker's partitions up to `message.max.bytes` in
/// all (at least one batch), the rest in the next request.
#[test]
fn a_produce_request_holds_at_most_message_max_bytes() {
    let big = Arc::new(AtomicUsize::new(0));
    let b2 = big.clone();
    let (b, logs) = sink(
        &["o"],
        8,
        Box::new(move |r, i, _| {
            b2.fetch_max(r.body.len(), Ordering::SeqCst);
            if i == 0 {
                std::thread::sleep(Duration::from_millis(300)); // the others due meanwhile
            }
            None
        }),
    );
    let p = producer(&b, &[("message.max.bytes", "100000"), ("batch.num.messages", "1")]);
    for i in 0..8 {
        p.send(Record::to("o").value(&[7u8; 40_000]), i).unwrap();
    }
    let h = heard(&p, 8);
    assert!(h.iter().all(|x| x.3.is_none()), "{h:?}");
    let max = big.load(Ordering::SeqCst);
    assert!(max <= 100_000 + 200, "a Produce request of {max} bytes with message.max.bytes=100000");
    assert!(max > 80_000, "two batches a request still: {max}");
    assert_eq!((0..8).map(|q| values(&logs, "o", q).len()).sum::<usize>(), 8);
}

/// A header's framing (its key's and value's lengths, up to 5 bytes each) counts toward a
/// message's size: many empty headers are no free ride past `message.max.bytes`.
#[test]
fn header_framing_counts_against_message_max_bytes() {
    let big = Arc::new(AtomicUsize::new(0));
    let b2 = big.clone();
    let (b, _) = sink(
        &["o"],
        1,
        Box::new(move |r, _, _| {
            b2.fetch_max(r.body.len(), Ordering::SeqCst);
            None
        }),
    );
    let p = producer(&b, &[("message.max.bytes", "1000")]);
    let hs: Vec<(String, Option<Vec<u8>>)> = (0..600).map(|_| (String::new(), None)).collect();
    match p.send(Record::to("o").headers(&hs), 1) {
        Err((e, _)) => assert_eq!(e.code(), Some(Code::MESSAGE_TOO_LARGE), "{e}"),
        Ok(()) => {
            heard(&p, 1);
            let max = big.load(Ordering::SeqCst);
            panic!("taken: a request of {max} bytes holding one batch, message.max.bytes=1000");
        }
    }
}

/// A message larger than the whole queue (`queue.buffering.max.kbytes`) is refused for what it
/// is, not QueueFull: polling and sending it again would never help.
#[test]
fn a_message_past_the_queue_bytes_is_refused_not_queue_full() {
    let p = Producer::new(config("127.0.0.1:1").set("queue.buffering.max.kbytes", "1"), Heard::default()).unwrap();
    let (e, _) = p.send(Record::to("o").value(&[0u8; 2000]), 1).unwrap_err();
    assert_eq!(p.in_flight_count(), 0);
    assert!(matches!(e, Error::Config(_)), "{e:?}");
}

/// `poll` and `flush` take a timeout of forever (Duration::MAX).
#[test]
fn poll_and_flush_take_a_timeout_of_forever() {
    let p = Producer::new(config("127.0.0.1:1").set("message.timeout.ms", "100"), Heard::default()).unwrap();
    p.send(Record::to("o").value(b"x"), 1).unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| p.poll(Duration::MAX)));
    assert_eq!(r.ok(), Some(1), "poll(Duration::MAX) panicked or heard nothing");
    p.send(Record::to("o").value(b"x"), 2).unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| p.flush(Duration::MAX)));
    assert!(r.is_ok_and(|r| r.is_ok()), "flush(Duration::MAX)");
}

/// Keyless messages go to a partition with a leader: none sticks to one without (its messages
/// would time out while another partition is up).
#[test]
fn keyless_messages_avoid_a_partition_without_a_leader() {
    for _ in 0..8 {
        let logs: Logs = Arc::default();
        let b = Broker::start(move |r| match r.api {
            wire::METADATA => Answer::Body(metadata(&[(0, &r.me)], &[("o", &[(0, 0), (1, -1)])])),
            wire::INIT_PRODUCER_ID => pid_answer(),
            wire::PRODUCE => Answer::Body(produce(r, &logs)),
            _ => Answer::Drop,
        });
        let p = producer(&b, &[("message.timeout.ms", "2000"), ("batch.num.messages", "2")]);
        for i in 0..5 {
            send(&p, "o", "x", i);
        }
        let h = heard(&p, 5);
        assert!(h.iter().all(|x| x.3.is_none() && x.1 == Some(0)), "{h:?}");
    }
}

/// A batch never written to a socket (its leader unreachable) that times out fails as timed out,
/// not persisted, and the producer is not fatal: no sequence of it reached a broker, and the next
/// batch takes its sequence.
#[test]
fn a_batch_never_written_times_out_without_making_the_producer_fatal() {
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me), (1, "127.0.0.1:1")], &[("o", &[(0, 1)])])),
        wire::INIT_PRODUCER_ID => pid_answer(),
        _ => Answer::Drop,
    });
    let p = producer(&b, &[("message.timeout.ms", "500")]);
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].3.as_deref(), h[0].6), (Some("message timed out (message.timeout.ms)"), Persisted::No));
    assert_eq!(p.fatal_error(), None, "fatal though no byte of the batch was ever written to a socket");
    let st = p.shared.lock();
    assert_eq!(st.topics["o"].parts[&0].seq, 0, "its sequence given back");
}

/// A topic with nothing queued is not looked up again and again (an unknown one's messages long
/// timed out): only the periodic refresh asks for it.
#[test]
fn a_topic_with_nothing_queued_is_not_looked_up_again() {
    let (b, _) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("message.timeout.ms", "200"), ("retry.backoff.max.ms", "100")]);
    p.send(Record::to("nope").value(b"x"), 1).unwrap();
    heard(&p, 1);
    let before = b.requests(wire::METADATA).len();
    std::thread::sleep(Duration::from_secs(1));
    let after = b.requests(wire::METADATA).len();
    assert!(after - before < 3, "{} metadata requests in 1 s for a topic with nothing queued", after - before);
}

/// A delivery callback that panics strands nothing: the reports after it are handed over at the
/// next poll, and its own counted as handed over.
#[test]
fn a_panicking_delivery_callback_strands_nothing() {
    struct Boom(AtomicUsize);
    impl Deliveries for Boom {
        type Opaque = u64;
        fn delivery(&self, _: &Delivery<'_>, o: u64) {
            self.0.fetch_add(1, Ordering::SeqCst);
            if o == 0 {
                panic!("the application's bug");
            }
        }
    }
    let c = config("127.0.0.1:1").set("message.timeout.ms", "100").set("linger.ms", "0").clone();
    let p = Producer::new(&c, Boom(AtomicUsize::new(0))).unwrap();
    for i in 0..3 {
        p.send(Record::to("o").value(b"x"), i).map_err(|e| e.0).unwrap();
    }
    std::thread::sleep(Duration::from_millis(400));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| p.poll(Duration::from_secs(1))));
    assert!(r.is_err(), "the callback's panic goes on to poll's caller");
    assert!(p.flush(Duration::from_secs(1)).is_ok(), "in flight {} forever", p.in_flight_count());
    assert_eq!(p.context().0.load(Ordering::SeqCst), 3, "each handed over once");
}

/// A batch the broker refuses for good (MESSAGE_TOO_LARGE), answered at each attempt, fails alone:
/// it was not written, no later sequence was sent, so the next batch takes its sequence and the
/// producer goes on.
#[test]
fn a_batch_refused_for_good_fails_alone_and_its_sequence_is_used_again() {
    let fault: Fault = Box::new(|r, i, _| {
        (i < 2).then(|| {
            let mut c = Cur::new(&r.body);
            c.nstr().unwrap();
            c.i16().unwrap();
            c.i32().unwrap();
            c.i32().unwrap();
            // NOT_ENOUGH_REPLICAS (not appended), then MESSAGE_TOO_LARGE
            Answer::Body(refusal(c.str().unwrap(), 0, if i == 0 { 19 } else { 10 }))
        })
    });
    let (b, logs) = sink(&["o"], 1, fault);
    let p = producer(&b, &[]);
    send(&p, "o", "big", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].4, h[0].6), (Some(Code::MESSAGE_TOO_LARGE), Persisted::No), "{h:?}");
    assert_eq!(p.fatal_error(), None);
    send(&p, "o", "next", 2);
    let h = heard(&p, 2);
    assert_eq!((h[1].2, h[1].3.as_ref()), (Some(0), None), "at the refused batch's sequence: {h:?}");
    assert_eq!(values(&logs, "o", 0), ["next"]);
}

/// A batch refused for good after an attempt that may have been written (its answer lost, or a
/// code a broker gives after appending it) makes the producer fatal: the broker may hold its
/// sequence, and a batch at it again would be taken as a duplicate.
#[test]
fn a_batch_refused_after_an_attempt_that_may_have_been_written_is_fatal() {
    for first in [None, Some(20)] {
        let fault: Fault = Box::new(move |r, i, _| {
            let mut c = Cur::new(&r.body);
            c.nstr().unwrap();
            c.i16().unwrap();
            c.i32().unwrap();
            c.i32().unwrap();
            let t = c.str().unwrap().to_string();
            match (i, first) {
                (0, None) => Some(Answer::Drop),
                (0, Some(code)) => Some(Answer::Body(refusal(&t, 0, code))),
                _ => Some(Answer::Body(refusal(&t, 0, 29))), // TOPIC_AUTHORIZATION_FAILED
            }
        });
        let (b, _) = sink(&["o"], 1, fault);
        let p = producer(&b, &[]);
        send(&p, "o", "x", 1);
        let h = heard(&p, 1);
        assert_eq!((h[0].4, h[0].6), (Some(Code::TOPIC_AUTHORIZATION_FAILED), Persisted::Possibly), "{first:?}");
        assert_eq!(p.fatal_error().map(|f| f.0), Some(Code::TOPIC_AUTHORIZATION_FAILED), "{first:?}");
    }
}

/// A topic the metadata refuses for good (TOPIC_AUTHORIZATION_FAILED, INVALID_TOPIC_EXCEPTION)
/// fails its messages at once, not at their timeout; an empty topic name is refused by `send`.
#[test]
fn a_topic_refused_for_good_fails_its_messages_at_once() {
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => {
            let me: &[(i32, &str)] = &[(0, &r.me)];
            let mut m = metadata(me, &[("o", &[])]);
            let at = metadata(me, &[]).len();
            m[at..at + 2].copy_from_slice(&29i16.to_be_bytes());
            Answer::Body(m)
        }
        wire::INIT_PRODUCER_ID => pid_answer(),
        _ => Answer::Drop,
    });
    let p = producer(&b, &[("message.timeout.ms", "30000")]);
    let t = Instant::now();
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].4, h[0].6), (Some(Code::TOPIC_AUTHORIZATION_FAILED), Persisted::No), "{h:?}");
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    let (e, _) = p.send(Record::to("").value(b"x"), 2).unwrap_err();
    assert!(matches!(e, Error::Config(_)), "{e:?}");
}

/// The metadata is looked up again every `topic.metadata.refresh.interval.ms`: partitions added
/// are used; partitions gone (the sticky one past the end) are not.
#[test]
fn the_metadata_is_refreshed_periodically() {
    let lookups = Arc::new(AtomicUsize::new(0));
    let n2 = lookups.clone();
    let logs: Logs = Arc::default();
    let l2 = logs.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => {
            // 3 partitions, then 1, then 2
            let ps: &[(i32, i32)] = match n2.fetch_add(1, Ordering::SeqCst) {
                0 => &[(0, 0), (1, 0), (2, 0)],
                1..4 => &[(0, 0)],
                _ => &[(0, 0), (1, 0)],
            };
            Answer::Body(metadata(&[(0, &r.me)], &[("o", ps)]))
        }
        wire::INIT_PRODUCER_ID => pid_answer(),
        wire::PRODUCE => Answer::Body(produce(r, &l2)),
        _ => Answer::Drop,
    });
    let p = producer(&b, &[("topic.metadata.refresh.interval.ms", "100"), ("batch.num.messages", "1")]);
    p.send(Record::to("o").value(b"a").partition(2), 1).unwrap();
    assert_eq!(heard(&p, 1)[0].1, Some(2));
    while lookups.load(Ordering::SeqCst) < 2 {
        std::thread::sleep(Duration::from_millis(10));
    }
    for i in 0..6 {
        send(&p, "o", "k", 10 + i); // keyless, one partition left
    }
    let h = heard(&p, 7);
    assert!(h[1..].iter().all(|x| x.1 == Some(0) && x.3.is_none()), "{h:?}");
    while lookups.load(Ordering::SeqCst) < 5 {
        std::thread::sleep(Duration::from_millis(10));
    }
    p.send(Record::to("o").value(b"b").partition(1), 2).unwrap();
    let h = heard(&p, 8);
    assert_eq!((h[7].1, h[7].3.as_ref()), (Some(1), None), "{h:?}");
}

/// A lookup started near another message's deadline is given the time a broker takes to answer,
/// not cut to what that message has left: else it fails, and is made again, until that message
/// is gone.
#[test]
fn a_lookup_near_another_messages_deadline_is_given_time_to_answer() {
    let logs: Logs = Arc::default();
    let l2 = logs.clone();
    let slow = |r: &Req| r.body.windows(4).any(|w| w == b"slow");
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => {
            if slow(r) {
                std::thread::sleep(Duration::from_millis(300));
            }
            Answer::Body(metadata(&[(0, &r.me)], &[("slow", &[(0, 0)])]))
        }
        wire::INIT_PRODUCER_ID => pid_answer(),
        wire::PRODUCE => Answer::Body(produce(r, &l2)),
        _ => Answer::Drop,
    });
    let p = producer(&b, &[("message.timeout.ms", "1000"), ("retry.backoff.ms", "50"), ("retry.backoff.max.ms", "50")]);
    p.send(Record::to("nope").value(b"x"), 1).unwrap(); // looked up every 50 ms till its timeout
    std::thread::sleep(Duration::from_millis(850));
    send(&p, "slow", "y", 2); // looked up with 150 ms left of "nope"'s
    let h = heard(&p, 2);
    assert!(h.iter().any(|x| x.5 == 2 && x.3.is_none()), "{h:?}");
    let asked = b.requests(wire::METADATA).iter().filter(|r| slow(r)).count();
    assert_eq!(asked, 1, "looked up once");
}

/// A batch's retries back off as librdkafka's do: `retry.backoff.ms` doubled at each attempt,
/// never past `retry.backoff.max.ms`.
#[test]
fn retries_back_off_exponentially_to_the_max() {
    let mut c = Config::new();
    c.set("bootstrap.servers", "x:1").set("retry.backoff.ms", "100").set("retry.backoff.max.ms", "1000");
    let s = c.producer().unwrap();
    let waits: Vec<u128> = (1..=6).map(|a| super::retry_wait(&s, a).as_millis()).collect();
    assert_eq!(waits, [100, 200, 400, 800, 1000, 1000]);
    assert_eq!(super::retry_wait(&s, u32::MAX).as_millis(), 1000, "no overflow");
}

/// A reconnect in a broker thread, its batches already taken (`sending`: `poll` cannot time them
/// out meanwhile), is given what is left of their message.timeout.ms, not the whole
/// socket.connection.setup.timeout.ms: they are reported timed out on time.
#[test]
fn a_reconnect_while_sending_is_bounded_by_the_message_timeout() {
    use std::net::TcpListener;
    // the leader: its first connection closed at once, the next ones accepted and never answered
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let leader = l.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let mut held = vec![];
        for (n, s) in l.incoming().enumerate() {
            let s = s.unwrap();
            if n == 0 {
                drop(s);
            } else {
                held.push(s);
            }
        }
    });
    let leader2 = leader.clone();
    let b = Broker::start(move |r| match r.api {
        wire::METADATA => Answer::Body(metadata(&[(0, &r.me), (1, &leader2)], &[("o", &[(0, 1)])])),
        wire::INIT_PRODUCER_ID => {
            let mut out = vec![];
            out.i32(0);
            out.i16(0);
            out.i64(1000);
            out.i16(0);
            Answer::Body(out)
        }
        _ => Answer::Drop,
    });
    let p = producer(&b, &[("message.timeout.ms", "300"), ("socket.connection.setup.timeout.ms", "4000")]);
    // wait for the producer id and the leader's thread to have failed its first connection
    std::thread::sleep(Duration::from_millis(300));
    let t = Instant::now();
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    let late = t.elapsed();
    assert_eq!(h[0].3.as_deref(), Some("message timed out (message.timeout.ms)"), "{h:?}");
    assert!(late < Duration::from_millis(1500), "message.timeout.ms=300, reported after {late:?}");
}

/// A timestamp below 0 is refused at `send`, as Java's producer refuses it; any two others share a
/// batch however far apart (their delta is never an overflow, which panicked a broker thread
/// holding the state lock: its batch never reported, the broker never produced to again).
#[test]
fn negative_timestamps_are_refused_and_far_apart_ones_share_a_batch() {
    let (b, logs) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[("linger.ms", "50")]);
    let refused = p.send(Record::to("o").value(b"n").timestamp(-1), 9).unwrap_err();
    assert!(matches!(refused, (Error::Config(_), 9)), "{refused:?}");
    p.send(Record::to("o").value(b"a").timestamp(0), 1).unwrap();
    p.send(Record::to("o").value(b"b").timestamp(i64::MAX), 2).unwrap();
    let h = heard(&p, 2);
    assert!(h.iter().all(|o| o.3.is_none()), "{h:?}");
    assert_eq!(log_of(&logs)[&("o".to_string(), 0)].stamps, [0, i64::MAX]);
}

/// A batch refused for good with a code not known to come before the broker appends it (here
/// INVALID_REQUEST, and one no Kafka has) may be on the log: it fails possibly persisted, with the
/// producer fatal, its sequence never given to the next batch (which would then be taken as its
/// duplicate). Those known to be checked first (MESSAGE_TOO_LARGE above) fail alone.
#[test]
fn a_batch_refused_with_a_code_not_known_before_append_is_fatal() {
    for code in [42, 999] {
        let fault: Fault = Box::new(move |r, _, _| {
            let mut c = Cur::new(&r.body);
            c.nstr().unwrap();
            c.i16().unwrap();
            c.i32().unwrap();
            c.i32().unwrap();
            Some(Answer::Body(refusal(c.str().unwrap(), 0, code)))
        });
        let (b, _) = sink(&["o"], 1, fault);
        let p = producer(&b, &[]);
        send(&p, "o", "x", 1);
        let h = heard(&p, 1);
        assert_eq!((h[0].4, h[0].6), (Some(Code(code)), Persisted::Possibly), "{code}: {h:?}");
        assert_eq!(p.fatal_error().map(|f| f.0), Some(Code(code)), "{code}");
    }
}

/// A leader's connection refused by SASL (credentials not yet propagated to it) is retried as a
/// lost one, until the messages' deadline: not fatal, the message delivered once it authenticates.
#[test]
fn a_leader_refusing_authentication_for_a_while_is_retried() {
    let logs: Logs = Arc::default();
    // refuses its first `refusals` authentications; leads o/0 as broker 1 at `leader`
    let start = |refusals: usize, leader: Option<String>, logs: Logs| {
        let n = AtomicUsize::new(0);
        Broker::start(move |r| {
            let mut out = vec![];
            match r.api {
                wire::SASL_HANDSHAKE => {
                    out.i16(0);
                    out.count(1);
                    out.str("PLAIN");
                }
                wire::SASL_AUTHENTICATE => {
                    let ok = n.fetch_add(1, Ordering::SeqCst) >= refusals;
                    out.i16(if ok { 0 } else { 58 });
                    out.nstr(Some(if ok { "" } else { "not yet" }));
                    out.bytes(b"");
                }
                wire::METADATA => {
                    let l = leader.clone().unwrap_or_else(|| r.me.clone());
                    out = metadata(&[(0, &r.me), (1, &l)], &[("o", &[(0, 1)])]);
                }
                wire::INIT_PRODUCER_ID => {
                    out.i32(0);
                    out.i16(0);
                    out.i64(1000);
                    out.i16(0);
                }
                wire::PRODUCE => out = produce(r, &logs),
                _ => return Answer::Drop,
            }
            Answer::Body(out)
        })
    };
    let leader = start(2, None, logs.clone());
    let b = start(0, Some(leader.addr.clone()), logs.clone());
    let sasl = [
        ("security.protocol", "sasl_plaintext"),
        ("sasl.mechanisms", "PLAIN"),
        ("sasl.username", "u"),
        ("sasl.password", "p"),
    ];
    let p = producer(&b, &sasl);
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!((h[0].3.as_deref(), h[0].6), (None, Persisted::Yes), "{h:?}");
    assert_eq!(p.fatal_error(), None);
    assert_eq!(values(&logs, "o", 0), ["x"]);
}

/// A topic name longer than the protocol carries (32767 bytes) is refused at `send`: queued, it
/// would fail every Metadata request, for every topic's messages.
#[test]
fn a_topic_name_too_long_for_the_protocol_is_refused() {
    let (b, _) = sink(&["o"], 1, no_fault());
    let p = producer(&b, &[]);
    let long = "t".repeat(32_768);
    let refused = p.send(Record::to(&long).value(b"x"), 9).unwrap_err();
    assert!(matches!(refused, (Error::Config(_), 9)), "{refused:?}");
    send(&p, "o", "x", 1);
    let h = heard(&p, 1);
    assert_eq!(h[0].3, None, "{h:?}");
}

/// The partitions a Produce request carries.
fn partitions_in(r: &Req) -> usize {
    let mut c = Cur::new(&r.body);
    c.nstr().unwrap();
    c.i16().unwrap();
    c.i32().unwrap();
    let mut n = 0;
    for _ in 0..c.i32().unwrap() {
        c.str().unwrap();
        for _ in 0..c.i32().unwrap() {
            c.i32().unwrap();
            c.bytes().unwrap();
            n += 1;
        }
    }
    n
}

/// A broker slow to answer (each Produce held `ms`), and the most Produce requests it held at once.
fn slow(ms: u64) -> (Fault, Arc<AtomicUsize>) {
    let (now, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let m2 = most.clone();
    let f: Fault = Box::new(move |_, _, _| {
        m2.fetch_max(now.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(ms));
        now.fetch_sub(1, Ordering::SeqCst);
        None
    });
    (f, most)
}

/// While a broker is slow to answer, full batches of other partitions pile up: one request carries
/// them all (up to `message.max.bytes` by default), unless `max.request.size` bounds a request
/// lower; then each request carries what fits, at least one batch (librdkafka sends one partition
/// a request, the Java client bounds a request by this setting).
#[test]
fn max_request_size_bounds_a_produce_request() {
    let run = |extra: &[(&str, &str)]| {
        let (f, _) = slow(150);
        let (b, logs) = sink(&["o"], 6, f);
        let mut settings = vec![("batch.num.messages", "4"), ("linger.ms", "0")];
        settings.extend_from_slice(extra);
        let p = producer(&b, &settings);
        for i in 0..96 {
            send(&p, "o", &format!("m{i}"), i);
        }
        let h = heard(&p, 96);
        assert!(h.iter().all(|x| x.3.is_none()), "{h:?}");
        let mut all: Vec<String> = (0..6).flat_map(|q| values(&logs, "o", q)).collect();
        all.sort();
        assert_eq!(all.len(), 96, "each message once");
        all.dedup();
        assert_eq!(all.len(), 96, "each message once");
        b.requests(wire::PRODUCE).iter().map(partitions_in).max().unwrap()
    };
    assert!(run(&[]) > 1, "by default a request carries every partition due, up to message.max.bytes");
    // a batch of 4 small messages is about 200 bytes: one goes, two do not fit
    assert_eq!(run(&[("max.request.size", "200")]), 1, "bounded: a batch a request");
}

/// `max.in.flight.requests.per.connection` (librdkafka's name; `max.in.flight` its alias): that
/// many Produce requests to a broker at once, each on a connection of its own. A partition still
/// has one batch in flight, so each partition's messages land once and in order. Unbounded, the
/// first request takes every batch due and leaves the other connections nothing:
/// `max.request.size` makes room for them.
#[test]
fn max_in_flight_sends_requests_to_a_broker_side_by_side() {
    for (n, want) in [("1", 1), ("3", 3)] {
        let (f, most) = slow(200);
        let (b, logs) = sink(&["o"], 6, f);
        let settings =
            [("batch.num.messages", "2"), ("linger.ms", "0"), ("max.in.flight", n), ("max.request.size", "200")];
        let p = producer(&b, &settings);
        for i in 0..60 {
            send(&p, "o", &format!("m{i:02}"), i);
        }
        let h = heard(&p, 60);
        assert!(h.iter().all(|x| x.3.is_none()), "{h:?}");
        assert_eq!(most.load(Ordering::SeqCst), want, "max.in.flight={n}: requests held at once");
        let mut total = 0;
        for q in 0..6 {
            let v = values(&logs, "o", q);
            let mut sorted = v.clone();
            sorted.sort();
            assert_eq!(v, sorted, "partition {q} in the order sent");
            total += v.len();
        }
        assert_eq!(total, 60, "each message once");
        let conns: std::collections::HashSet<usize> = b.requests(wire::PRODUCE).iter().map(|r| r.conn).collect();
        assert_eq!(conns.len(), want, "max.in.flight={n}: a connection each");
        assert_eq!(p.shared.lock().threads.len(), want, "max.in.flight={n}: a thread each");
    }
}

/// A batch is encoded (compressed, its CRC taken) off the producer's lock: `send`, the other
/// broker threads and `poll` go on meanwhile. Here a batch of 16 MB of gzip takes far longer to
/// write than any `send` beside it may wait (bench/kafka-stress: in a burst every `send` waited
/// out each batch's encoding).
#[test]
fn a_batch_is_encoded_off_the_producers_lock() {
    let (b, _) = sink(&["o", "q"], 1, no_fault());
    let big = 16 * 1024 * 1024;
    let p = producer(
        &b,
        &[
            ("compression.type", "gzip"),
            ("message.max.bytes", &(2 * big).to_string()),
            ("batch.size", &(2 * big).to_string()),
            ("batch.num.messages", "16"),
            ("linger.ms", "0"),
        ],
    );
    let mut rng = 0x9e37_79b9_7f4a_7c15u64;
    let mut noise = |n: usize| (0..n).map(|_| xorshift(&mut rng) as u8).collect::<Vec<u8>>();
    let values: Vec<Vec<u8>> = (0..16).map(|_| noise(big / 16)).collect();
    // how long the encoding takes, as the broker thread does it
    let t = Instant::now();
    let outs: Vec<Out<'_>> =
        values.iter().map(|v| Out { timestamp: 0, key: None, value: Some(v), headers: &[] }).collect();
    let stamp = Stamp { producer_id: 1, epoch: 0, base_sequence: 0 };
    records::write(&mut vec![], &outs, stamp, records::GZIP, &mut vec![]).unwrap();
    let encoding = t.elapsed();
    send(&p, "q", "warm", 100); // the producer id and the topics looked up before the timing
    heard(&p, 1);
    for (i, v) in values.iter().enumerate() {
        p.send(Record::to("o").value(v), i as u64).unwrap();
    }
    // while the batch is written, the slowest of many sends
    let (start, mut slowest, mut i) = (Instant::now(), Duration::ZERO, 200);
    while start.elapsed() < encoding {
        let t = Instant::now();
        send(&p, "q", "small", i);
        slowest = slowest.max(t.elapsed());
        i += 1;
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(slowest < encoding / 4, "a send waited {slowest:?} while a batch was encoded (which takes {encoding:?})");
    let h = heard(&p, 17 + (i as usize - 200));
    assert!(h.iter().all(|x| x.3.is_none()), "{:?}", h.iter().find(|x| x.3.is_some()));
}

/// `max.request.size` at its edge: batches whose bytes sum to the bound exactly go in one request,
/// a byte less and the second waits for the next (counted as `size_of` counts them, a batch's
/// header included); the longest waiting first.
#[test]
fn a_request_takes_batches_up_to_max_request_size_exactly() {
    let batch = records::HEADER + size_of(None, Some(&[0; 40]), &[]);
    for (bound, taken, more) in [(2 * batch, 2, false), (2 * batch - 1, 1, true)] {
        let mut c = config("127.0.0.1:1");
        c.set("linger.ms", "0").set("max.request.size", bound.to_string());
        let p = Producer::new(&c, Heard::default()).unwrap();
        let sh = &*p.shared;
        let mut st = sh.lock();
        st.pid = Some((1000, 0));
        let (now, later) = (Instant::now(), Instant::now() + Duration::from_secs(60));
        let mut t = Topic {
            partitions: vec![0, 1],
            sticky: 0,
            unassigned: VecDeque::new(),
            parts: HashMap::default(),
            missing: None,
        };
        for (q, o) in [(0, 2), (1, 1)] {
            let mut part = Part { queue: VecDeque::new(), seq: 0, inflight: None, leader: Some(0) };
            part.queue.push_back(Pending {
                key: None,
                value: Some(vec![0; 40]),
                headers: vec![],
                timestamp: 0,
                partition: None,
                opaque: o,
                deadline: later,
                enqueued: now - Duration::from_millis(o), // partition 0 waiting the longer
            });
            t.parts.insert(q, part);
        }
        st.topics.insert("o".into(), t);
        (st.outstanding, st.queued, st.queued_bytes) = (2, 2, 2 * size_of(None, Some(&[0; 40]), &[]));
        let (req, fresh, _, left) = take_due(sh, &mut st, 0, 7, now);
        assert!(req.is_empty(), "nothing was in flight");
        let parts: Vec<i32> = fresh.iter().map(|f| f.1).collect();
        assert_eq!(parts.len(), taken, "max.request.size={bound} (a batch is {batch} bytes): {parts:?}");
        assert_eq!(parts[0], 0, "the longest waiting first");
        assert_eq!(left, more, "a batch left for the next request");
        let sent = st.topics["o"].parts[&0].inflight.as_ref().unwrap();
        assert!(sent.sending && sent.by == 7 && sent.msgs.is_empty(), "its messages out to encode, by thread 7");
        // encoded and put back: in the request, under the bound
        let written = encode(&fresh, sh.s.codec, &mut vec![]);
        let mut req = vec![];
        assert!(!encoded(sh, &mut st, &mut req, fresh, written));
        assert_eq!(req.len(), taken);
        assert!(req.iter().map(|r| r.2.len()).sum::<usize>() <= bound);
        assert_eq!(st.topics["o"].parts[&0].inflight.as_ref().unwrap().msgs.len(), 1, "its messages back");
    }
}

/// A batch that fails to encode fails its messages, not persisted, and gives its sequence back:
/// the next batch of its partition is sent at it.
#[test]
fn a_batch_that_fails_to_encode_gives_its_sequence_back() {
    let p = Producer::new(config("127.0.0.1:1").set("linger.ms", "0"), Heard::default()).unwrap();
    let sh = &*p.shared;
    let mut st = sh.lock();
    st.pid = Some((1000, 0));
    let later = Instant::now() + Duration::from_secs(60);
    let mut part = Part { queue: VecDeque::new(), seq: 5, inflight: None, leader: Some(0) };
    part.queue.push_back(Pending {
        key: None,
        value: Some(vec![1]),
        headers: vec![],
        timestamp: 0,
        partition: None,
        opaque: 1,
        deadline: later,
        enqueued: Instant::now() - Duration::from_secs(1),
    });
    let mut t =
        Topic { partitions: vec![0], sticky: 0, unassigned: VecDeque::new(), parts: HashMap::default(), missing: None };
    t.parts.insert(0, part);
    st.topics.insert("o".into(), t);
    (st.outstanding, st.queued, st.queued_bytes) = (1, 1, size_of(None, Some(&[1]), &[]));
    let (_, fresh, _, _) = take_due(sh, &mut st, 0, 0, Instant::now());
    assert_eq!(st.topics["o"].parts[&0].seq, 6, "sealed: the sequence taken");
    let mut req = vec![];
    encoded(sh, &mut st, &mut req, fresh, vec![Err(Error::Protocol("a codec's failure".into()))]);
    assert!(req.is_empty());
    let part = &st.topics["o"].parts[&0];
    assert!(part.inflight.is_none() && part.seq == 5, "its sequence given back");
    assert_eq!((st.queued, st.queued_bytes), (0, 0));
    assert!(matches!(st.reports.front(), Some((_, Some(0), None, Some(Error::Protocol(_)), Persisted::No, 1))));
}
