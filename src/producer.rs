//! The idempotent producer. `send` queues a message and returns; a broker's thread sends each
//! partition's batches to its leader on one connection, up to `max.in.flight` requests pipelined,
//! one batch of a partition in flight at a time, so a partition keeps its messages in the order
//! sent; a retry sends the same batch with the same sequence, which the broker writes once. A
//! batch is encoded by the thread that sends it, off the producer's lock. A request holds up to
//! `max.request.size` of batches, the longest waiting first. `poll` hands the outcomes to the
//! context's `delivery`, on the thread that polls.
//!
//! Like librdkafka's idempotent producer: messages are given up `message.timeout.ms` after
//! `send`, retries included; a DUPLICATE_SEQUENCE_NUMBER is a delivery (its offset unknown). A
//! batch that fails (refused for good, or timed out) though no attempt of it may have been
//! written fails alone: the broker never had its sequence, which the next batch takes. Not like
//! librdkafka: where the broker's sequence may now have a gap (a batch that may have been written
//! timed out or refused for good, or a sequence refused), librdkafka bumps the epoch and
//! renumbers; this producer is fatal instead (`fatal_error`): every message after fails, and a new
//! producer is made to go on. Each failed delivery says whether it may be on the log
//! (`Persisted`).
//!
//! The entry point is [`Producer::new`], with a [`Deliveries`] context; build each message with
//! [`Record::to`] and queue it with [`Producer::send`].
use crate::client::Client;
use crate::config::{Config, ProducerSettings};
use crate::conn::Conn;
use crate::error::{Code, Error, Result};
use crate::records::{self, Out, Stamp};
use crate::wire::{self, Cur};
use std::collections::VecDeque;
use std::hash::{BuildHasherDefault, Hasher};

/// The producer's maps, looked up for every record: keyed by the application's topic names and
/// the brokers' partition and node ids, nothing an adversary chooses, so FxHash's few
/// instructions rather than SipHash's (SipHash was 7% of a keyed send's instructions).
#[derive(Default)]
struct Fx(u64);
impl Hasher for Fx {
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            self.write_u64(u64::from_le_bytes(w));
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(5) ^ v).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
    fn write_u32(&mut self, v: u32) {
        self.write_u64(v as u64)
    }
    fn write_i32(&mut self, v: i32) {
        self.write_u64(v as u32 as u64)
    }
    fn write_u8(&mut self, v: u8) {
        self.write_u64(v as u64)
    }
    fn write_usize(&mut self, v: usize) {
        self.write_u64(v as u64)
    }
    fn finish(&self) -> u64 {
        self.0
    }
}
type HashMap<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<Fx>>;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

/// A message to send: its topic, and what the builder gives it (by default no key, no value, a
/// partition the producer picks, the time it is sent, no headers).
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    topic: &'a str,
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    partition: Option<i32>,
    timestamp: Option<i64>,
    headers: &'a [(String, Option<Vec<u8>>)],
}

impl<'a> Record<'a> {
    /// A record to `topic`: no value (a tombstone) until one is given.
    pub fn to(topic: &'a str) -> Self {
        Record { topic, key: None, value: None, partition: None, timestamp: None, headers: &[] }
    }

    /// Its value.
    pub fn value(self, v: &'a [u8]) -> Self {
        Record { value: Some(v), ..self }
    }

    /// Its key: without a partition, the partition is the key's (as Java's default partitioner
    /// picks it: murmur2 of the key, modulo the partitions).
    pub fn key(self, k: &'a [u8]) -> Self {
        Record { key: Some(k), ..self }
    }

    /// Its partition: one the topic has not fails it (UNKNOWN_TOPIC_OR_PARTITION).
    pub fn partition(self, p: i32) -> Self {
        Record { partition: Some(p), ..self }
    }

    /// Its timestamp (CreateTime, ms since the epoch), rather than when it is sent: at least 0, or
    /// [`Producer::send`] refuses it (as Java's producer does).
    pub fn timestamp(self, ms: i64) -> Self {
        Record { timestamp: Some(ms), ..self }
    }

    /// Its headers in order: (name, value; None for null). A name may repeat.
    pub fn headers(self, h: &'a [(String, Option<Vec<u8>>)]) -> Self {
        Record { headers: h, ..self }
    }
}

/// Whether a message is on the broker's log (librdkafka's message status).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Persisted {
    /// not written: never sent, or refused
    No,
    /// it may be: sent, its answer never had (a timeout, a connection lost)
    Possibly,
    /// written: the broker said so
    Yes,
}

/// A message's outcome, to `Deliveries::delivery`.
#[derive(Debug)]
#[non_exhaustive]
pub struct Delivery<'a> {
    /// The message's topic.
    pub topic: &'a str,
    /// Its partition; None: never given one (its topic's partitions never known, or purged first).
    pub partition: Option<i32>,
    /// Its offset; None: not delivered, or delivered at an offset the broker did not say (a
    /// DUPLICATE_SEQUENCE_NUMBER).
    pub offset: Option<i64>,
    /// Why it was not delivered; None: delivered.
    pub error: Option<&'a Error>,
    /// Whether it is on the broker's log.
    pub persisted: Persisted,
}

/// What hears the deliveries (librdkafka's delivery report callback).
///
/// It is the producer's context, shared with its threads; [`delivery`](Deliveries::delivery) is
/// called only from [`Producer::poll`] (and [`Producer::flush`]), on the polling thread.
pub trait Deliveries: Send + Sync + 'static {
    /// What the application attaches to each message at [`Producer::send`], handed back with
    /// its outcome.
    type Opaque: Send + 'static;
    /// Hears one message's outcome `d`, with the `opaque` it was sent with. A panic here
    /// propagates out of [`Producer::poll`]; the outcomes after it are handed over at the next
    /// poll.
    fn delivery(&self, d: &Delivery<'_>, opaque: Self::Opaque);
}

struct Pending<O> {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    timestamp: i64,
    /// the partition the application named, if it named one
    partition: Option<i32>,
    opaque: O,
    deadline: Instant,
    enqueued: Instant,
}

/// What a message counts for against `queue.buffering.max.kbytes` and in a batch: its key, value
/// and headers, a record's overhead, and each header's framing (its two lengths, at most 5 bytes
/// each: else many small headers would pass `message.max.bytes` unseen).
fn size_of(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[(String, Option<Vec<u8>>)]) -> usize {
    let hs = headers.iter().map(|(k, v)| 10 + k.len() + v.as_ref().map_or(0, Vec::len)).sum::<usize>();
    key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len) + hs + 16
}

impl<O> Pending<O> {
    fn size(&self) -> usize {
        size_of(self.key.as_deref(), self.value.as_deref(), &self.headers)
    }
}

/// A partition's batch sent (or to send again): its messages, its encoded bytes.
struct Sent<O> {
    /// empty while the broker thread that sealed it encodes them, off the lock (`Fresh`)
    msgs: Vec<Pending<O>>,
    bytes: Arc<Vec<u8>>,
    /// its first message's sequence
    seq: i32,
    /// its first message's deadline (kept here: its messages may be out being encoded)
    deadline: Instant,
    /// a broker thread has it (encoding it, or in a request): only that thread touches it then
    sending: bool,
    /// which broker thread, while `sending` (its panic guard hands back its own alone)
    by: u64,
    /// may be sent (again) from then
    retry_at: Instant,
    attempts: u32,
    /// a request of it was written and its answer never had, or answered with a code a broker
    /// gives after appending it: the broker may hold its sequence. Until then, every attempt was
    /// answered "not written", so its sequence may be given to the next batch if it fails.
    maybe_written: bool,
}

struct Part<O> {
    queue: VecDeque<Pending<O>>,
    seq: i32,
    inflight: Option<Sent<O>>,
    leader: Option<i32>,
}

struct Topic<O> {
    partitions: Vec<i32>,
    /// the partition keyless messages go to now (librdkafka's sticky partitioner)
    sticky: usize,
    unassigned: VecDeque<Pending<O>>,
    parts: HashMap<i32, Part<O>>,
    /// the metadata said the topic does not exist
    missing: Option<Code>,
}

/// An outcome for `poll` to hand over: topic, partition, offset, error, whether it is on the log,
/// the message's opaque.
type Report<O> = (Arc<str>, Option<i32>, Option<i64>, Option<Error>, Persisted, O);

struct State<O> {
    /// by name; the name shared with every report of the topic's messages (no copy a report)
    topics: HashMap<Arc<str>, Topic<O>>,
    pid: Option<(i64, i16)>,
    fatal: Option<(Code, String)>,
    /// brokers' addresses
    brokers: HashMap<i32, String>,
    /// the brokers with a thread
    threads: Vec<i32>,
    /// the next broker thread's own number (`Sent::by`)
    next_thread: u64,
    /// messages sent whose outcome `poll` has not handed over (what `queue.buffering.max.messages`
    /// bounds: a report holds its opaque until then)
    outstanding: usize,
    /// the bytes of those still in a queue or a batch (what `.kbytes` bounds)
    queued_bytes: usize,
    reports: VecDeque<Report<O>>,
    metadata_due: bool,
    /// the metadata last applied then (`topic.metadata.refresh.interval.ms` from it)
    refreshed: Instant,
    closing: bool,
    rng: u64,
}

struct Shared<C: Deliveries> {
    ctx: C,
    s: ProducerSettings,
    client: Client,
    state: Mutex<State<C::Opaque>>,
    /// a batch may be ready, metadata is due, or it closes
    work: Condvar,
    /// a report is ready
    reported: Condvar,
}

/// An idempotent producer of `C`'s messages, made with [`Producer::new`]. `send` from any thread;
/// `poll` (or `flush`) to hand outcomes to `C`. Dropping it fails what is still queued
/// ([`Error::Purged`], handed to no one) without waiting: [`flush`](Producer::flush) first.
pub struct Producer<C: Deliveries> {
    shared: Arc<Shared<C>>,
}

impl<C: Deliveries> std::fmt::Debug for Producer<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer").field("in_flight", &self.in_flight_count()).finish_non_exhaustive()
    }
}

/// A batch's wait before its retry after `attempts`: `retry.backoff.ms` doubled at each, at most
/// `retry.backoff.max.ms` (librdkafka's backoff).
pub(crate) fn retry_wait(s: &ProducerSettings, attempts: u32) -> Duration {
    s.retry_backoff.saturating_mul(1 << attempts.saturating_sub(1).min(20)).min(s.retry_backoff_max)
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

impl<C: Deliveries> Producer<C> {
    /// A producer of `cfg`'s cluster, its outcomes to `ctx`; it connects at its first message.
    /// It starts a thread to manage the producer (and later one per broker).
    ///
    /// # Errors
    ///
    /// [`Error::Config`]: what [`Config::producer`] or [`Config::net`] refuses.
    /// [`Error::Transport`]: the thread could not be started. No broker is contacted.
    pub fn new(cfg: &Config, ctx: C) -> Result<Producer<C>> {
        let s = cfg.producer()?;
        let client = Client::new(cfg)?;
        let seed = now_ms() as u64 ^ (std::process::id() as u64) << 32;
        let state = State {
            topics: HashMap::default(),
            pid: None,
            fatal: None,
            brokers: HashMap::default(),
            threads: vec![],
            next_thread: 0,
            outstanding: 0,
            queued_bytes: 0,
            reports: VecDeque::new(),
            metadata_due: false,
            refreshed: Instant::now(),
            closing: false,
            rng: seed | 1,
        };
        let shared = Arc::new(Shared {
            ctx,
            s,
            client,
            state: Mutex::new(state),
            work: Condvar::new(),
            reported: Condvar::new(),
        });
        let m = shared.clone();
        std::thread::Builder::new()
            .name("fastka-producer".into())
            .spawn(move || manage(&m))
            .map_err(|e| Error::Transport(format!("a thread for the producer: {e}")))?;
        Ok(Producer { shared })
    }

    /// Its context, as given to [`Producer::new`].
    pub fn context(&self) -> &C {
        &self.shared.ctx
    }

    /// Its cluster's control plane (metadata, offsets), on connections of its own.
    pub fn client(&self) -> &Client {
        &self.shared.client
    }

    /// Queues `r` for its topic, to its partition (the one it names, its key's, or for a record of
    /// neither one this producer picks); its outcome to `delivery` with `opaque`. QueueFull: the
    /// queue is at its bound (`queue.buffering.max.messages`, `.kbytes`): `poll` and send it again.
    /// Fatal: the producer can deliver nothing more.
    ///
    /// A partition the topic does not have is not an error here: it is reported to `delivery`
    /// (UNKNOWN_TOPIC_OR_PARTITION).
    ///
    /// # Errors
    ///
    /// The error with `opaque` handed back, the message not queued:
    /// - [`Error::QueueFull`]: at `queue.buffering.max.messages` or `.kbytes`; poll, then retry.
    /// - [`Error::Fatal`]: the producer is fatal ([`fatal_error`](Producer::fatal_error)).
    /// - [`Error::Broker`] with [`Code::MESSAGE_TOO_LARGE`]: the message cannot fit a batch of
    ///   `message.max.bytes`.
    /// - [`Error::Config`]: an empty topic or one over 32767 bytes, a timestamp below 0, or a
    ///   message larger than `queue.buffering.max.kbytes`.
    /// - [`Error::Transport`]: the producer is closing.
    pub fn send(&self, r: Record<'_>, opaque: C::Opaque) -> std::result::Result<(), (Error, C::Opaque)> {
        let sh = &*self.shared;
        let size = size_of(r.key, r.value, r.headers);
        // what the record is refused for, whatever the producer's state (refused after a fatal
        // producer's or a closing one's own error, as before)
        let refused = if r.topic.is_empty() {
            Some(Error::Config("a record without a topic".into()))
        } else if let Err(e) = wire::check_str("a topic name", r.topic) {
            // queued, a name the protocol cannot carry would fail the Metadata of every topic
            Some(e)
        } else if let Some(ms) = r.timestamp.filter(|ms| *ms < 0) {
            Some(Error::Config(format!("timestamp {ms}: a timestamp is at least 0")))
        } else if size + records::HEADER > sh.s.max_batch {
            // what no wait would ever take: refused for what it is, not QueueFull
            Some(Error::Broker(Code::MESSAGE_TOO_LARGE, format!("a message of {size} bytes")))
        } else if size > sh.s.queue_bytes {
            Some(Error::Config(format!("a message of {size} bytes, past queue.buffering.max.kbytes")))
        } else {
            None
        };
        // copied before the lock, which the broker threads, `poll` and the other senders wait on
        let copied = refused.is_none().then(|| {
            let timestamp = r.timestamp.unwrap_or_else(now_ms);
            (r.key.map(<[u8]>::to_vec), r.value.map(<[u8]>::to_vec), r.headers.to_vec(), timestamp)
        });
        let mut guard = sh.lock();
        let st = &mut *guard;
        if let Some((c, m)) = &st.fatal {
            return Err((Error::Fatal(*c, m.clone()), opaque));
        }
        if st.closing {
            return Err((Error::Transport("the producer is closing".into()), opaque));
        }
        let Some((key, value, headers, timestamp)) = copied else {
            return Err((refused.expect("refused or copied"), opaque));
        };
        if st.outstanding >= sh.s.queue_messages || st.queued_bytes + size > sh.s.queue_bytes {
            return Err((Error::QueueFull, opaque));
        }
        let now = Instant::now();
        let p = Pending {
            key,
            value,
            headers,
            timestamp,
            partition: r.partition,
            opaque,
            deadline: now + sh.s.message_timeout,
            enqueued: now,
        };
        st.outstanding += 1;
        st.queued_bytes += size;
        // looked up by the name given, no copy of it made but for a topic not seen before
        if !st.topics.contains_key(r.topic) {
            let t = Topic {
                partitions: vec![],
                sticky: 0,
                unassigned: VecDeque::new(),
                parts: HashMap::default(),
                missing: None,
            };
            st.topics.insert(Arc::from(r.topic), t);
        }
        let t = st.topics.get_mut(r.topic).expect("inserted");
        // the threads woken only when something is due sooner than they wait for: a partition's
        // first message (its linger starts), a batch filled, a topic to look up
        let mut wake = true;
        let sticky = r.partition.is_none() && r.key.is_none();
        match t.route(r.partition, r.key) {
            Ok(Some(part)) => {
                let pt = t.parts.get_mut(&part).expect("a partition of the topic");
                let q = &mut pt.queue;
                q.push_back(p);
                // not while the partition has a batch in flight: it cannot send another until that
                // one is answered, and its thread looks at what is due when it is (under keyed
                // load, most sends; each wake a futex and a thread's context switch)
                wake = pt.inflight.is_none() && (q.len() == 1 || q.len().is_multiple_of(sh.s.batch_messages));
                // a batch's worth on this partition: the next keyless go to another (KIP-480's
                // sticky partitioner, as librdkafka's)
                if sticky && q.len().is_multiple_of(sh.s.batch_messages) {
                    t.rotate(xorshift(&mut st.rng));
                }
            }
            Ok(None) => t.unassigned.push_back(p), // a topic without partitions is looked up
            Err(()) => {
                let e = Error::Broker(Code::UNKNOWN_TOPIC_OR_PARTITION, format!("{}/{:?}", r.topic, r.partition));
                st.report(&Arc::from(r.topic), r.partition, None, Some(e), Persisted::No, p);
                drop(guard);
                sh.reported.notify_all();
                return Ok(());
            }
        }
        drop(guard);
        if wake {
            sh.work.notify_all();
        }
        Ok(())
    }

    /// Hands the outcomes ready to the context's `delivery`, waiting up to `timeout` for one:
    /// how many. What is past `message.timeout.ms` is timed out here too, when due: the threads
    /// may be in a lookup, or waiting out its backoff. `Duration::MAX`: until one is ready.
    ///
    /// # Panics
    ///
    /// If the context's [`delivery`](Deliveries::delivery) panics: the panic is resumed here.
    pub fn poll(&self, timeout: Duration) -> usize {
        let sh = &*self.shared;
        // None: forever (a timeout past what an Instant holds)
        let until = Instant::now().checked_add(timeout);
        let mut st = sh.lock();
        loop {
            let now = Instant::now();
            expire(&mut st, now);
            if !st.reports.is_empty() || until.is_some_and(|u| now >= u) {
                break;
            }
            // a batch being sent past its deadline is not timed out here: its answer is waited for
            let wake = next_deadline(&st).map(|d| d.max(now + Duration::from_millis(1)));
            let wait = match (wake, until) {
                (Some(w), Some(u)) => w.min(u),
                (w, u) => w.or(u).unwrap_or(now + Duration::from_secs(1)), // forever, a second a wait
            };
            st = sh.reported.wait_timeout(st, wait.saturating_duration_since(now)).unwrap_or_else(|p| p.into_inner()).0;
        }
        let mut ready = std::mem::take(&mut st.reports).into_iter();
        drop(st);
        let n = ready.len();
        let mut handed = 0;
        while let Some((topic, partition, offset, error, persisted, opaque)) = ready.next() {
            let d = Delivery { topic: &topic, partition, offset, error: error.as_ref(), persisted };
            let heard = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sh.ctx.delivery(&d, opaque)));
            handed += 1;
            if let Err(panic) = heard {
                // the application's panic is its own: the reports after it are handed over at the
                // next poll, none left counted forever
                let mut st = sh.lock();
                st.outstanding -= handed;
                for r in ready.rev() {
                    st.reports.push_front(r);
                }
                drop(st);
                std::panic::resume_unwind(panic);
            }
        }
        // their places given back once for them all, not a lock taken beside every send for each
        sh.lock().outstanding -= n;
        n
    }

    /// The messages sent whose outcome `poll` has not handed over yet (librdkafka's outq_len).
    pub fn in_flight_count(&self) -> usize {
        self.shared.lock().outstanding
    }

    /// Polls until every message's outcome is handed over, within `timeout`
    /// (`Duration::MAX`: no limit).
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`]: messages are still in flight when `timeout` passes. A message that
    /// failed is not an error here: its outcome went to `delivery`.
    ///
    /// # Panics
    ///
    /// As [`poll`](Producer::poll) does.
    pub fn flush(&self, timeout: Duration) -> Result<()> {
        let until = Instant::now().checked_add(timeout);
        while self.in_flight_count() > 0 {
            let left = until.map_or(Duration::MAX, |u| u.saturating_duration_since(Instant::now()));
            if left.is_zero() {
                return Err(Error::Timeout(format!("{} messages not delivered", self.in_flight_count())));
            }
            self.poll(left.min(Duration::from_millis(100)));
        }
        Ok(())
    }

    /// Fails every message not yet in a request ([`Error::Purged`], [`Persisted::No`]), their
    /// outcomes handed over at the next `poll`: what is in flight may still land.
    pub fn purge_queue(&self) {
        let mut st = self.shared.lock();
        let st = &mut *st;
        for (name, t) in st.topics.iter_mut() {
            let mut gone: Vec<(Option<i32>, Pending<C::Opaque>)> = t.unassigned.drain(..).map(|p| (None, p)).collect();
            for (id, part) in t.parts.iter_mut() {
                gone.extend(part.queue.drain(..).map(|p| (Some(*id), p)));
            }
            for (partition, p) in gone {
                st.queued_bytes -= p.size();
                st.reports.push_back((name.clone(), partition, None, Some(Error::Purged), Persisted::No, p.opaque));
            }
        }
        self.shared.reported.notify_all();
    }

    /// The error that made the producer fatal, (code, why); None if it is not fatal. Once
    /// fatal, every message fails: make a new producer to go on.
    pub fn fatal_error(&self) -> Option<(Code, String)> {
        self.shared.lock().fatal.clone()
    }
}

impl<C: Deliveries> Drop for Producer<C> {
    /// Its threads end after their request in flight; what is queued is not sent.
    fn drop(&mut self) {
        self.purge_queue();
        self.shared.lock().closing = true;
        self.shared.work.notify_all();
    }
}

impl<C: Deliveries> Shared<C> {
    fn lock(&self) -> MutexGuard<'_, State<C::Opaque>> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl<O> State<O> {
    fn report(
        &mut self,
        topic: &Arc<str>,
        partition: Option<i32>,
        offset: Option<i64>,
        error: Option<Error>,
        persisted: Persisted,
        p: Pending<O>,
    ) {
        self.queued_bytes -= p.size();
        self.reports.push_back((topic.clone(), partition, offset, error, persisted, p.opaque));
    }

    fn set_fatal(&mut self, code: Code, why: String) {
        if self.fatal.is_none() {
            self.fatal = Some((code, why));
        }
    }
}

/// The next of a xorshift sequence: which partition a keyless batch goes to need not be
/// unpredictable, only spread.
fn xorshift(rng: &mut u64) -> u64 {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    *rng
}

/// Kafka's `Utils.murmur2`, which Java's default partitioner picks a key's partition with.
pub(crate) fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1e995;
    let mut h = 0x9747b28c_u32 ^ data.len() as u32;
    let mut chunks = data.chunks_exact(4);
    for c in &mut chunks {
        let mut k = u32::from_le_bytes([c[0], c[1], c[2], c[3]]).wrapping_mul(M);
        k ^= k >> 24;
        h = h.wrapping_mul(M) ^ k.wrapping_mul(M);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        for (i, b) in rest.iter().enumerate() {
            h ^= (*b as u32) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    (h ^ (h >> 15)) as i32
}

impl<O> Topic<O> {
    /// Where a message goes: its partition, none yet (the topic's partitions not known), or none
    /// at all (a partition the topic has not).
    fn route(&self, partition: Option<i32>, key: Option<&[u8]>) -> std::result::Result<Option<i32>, ()> {
        let n = self.partitions.len();
        if n == 0 {
            return Ok(None);
        }
        match (partition, key) {
            (Some(p), _) => self.partitions.binary_search(&p).map(|_| Some(p)).map_err(drop),
            // Java's default partitioner: every partition, led or not
            (None, Some(k)) => Ok(Some(self.partitions[(murmur2(k) & 0x7fffffff) as usize % n])),
            (None, None) => Ok(Some(self.partitions[self.sticky])),
        }
    }

    /// Whether the `i`th partition has a leader.
    fn led(&self, i: usize) -> bool {
        self.partitions.get(i).and_then(|p| self.parts.get(p)).is_some_and(|p| p.leader.is_some())
    }

    /// Keyless messages go to another partition from now on (`r` picks which): one with a leader
    /// if another has one (else they stay on theirs if it has one), else any other.
    fn rotate(&mut self, r: u64) {
        let n = self.partitions.len();
        let others = || (0..n).filter(|&i| i != self.sticky && self.led(i));
        self.sticky = match others().count() {
            0 if self.led(self.sticky) => self.sticky,
            0 if n > 1 => (self.sticky + 1 + (r % (n as u64 - 1)) as usize) % n,
            0 => 0,
            k => others().nth((r % k as u64) as usize).expect("one of them"),
        };
    }
}

/// The producer's own thread: the metadata, the producer id, messages given up, a thread for each
/// broker leading a partition.
fn manage<C: Deliveries>(sh: &Arc<Shared<C>>) {
    let mut backoff = sh.s.retry_backoff;
    loop {
        let mut st = sh.lock();
        if st.closing {
            return;
        }
        expire(&mut st, Instant::now());
        if st.fatal.is_some() {
            fail_queued(&mut st);
        }
        // a topic never resolved with nothing queued (unknown, its messages gone) is asked for no
        // more; one resolved is kept: its partitions' sequences go on
        st.topics.retain(|_, t| !t.parts.is_empty() || !t.unassigned.is_empty());
        sh.reported.notify_all();
        // periodically too: partitions added are used, leaders moved found before a send fails
        let stale = sh.s.metadata_refresh.is_some_and(|i| st.refreshed.elapsed() >= i) && !st.topics.is_empty();
        let need_metadata = st.metadata_due || stale || unresolved(&st);
        // at once, not at the first message (librdkafka's idempotent producer too): the first
        // message then waits on the metadata and its leader's connection alone
        let need_pid = st.pid.is_none() && st.fatal.is_none();
        if !need_metadata && !need_pid {
            let wait =
                next_deadline(&st).map_or(Duration::from_millis(100), |d| d.saturating_duration_since(Instant::now()));
            let _ = sh.work.wait_timeout(st, wait.clamp(Duration::from_millis(1), Duration::from_millis(100)));
            continue;
        }
        st.metadata_due = false;
        let asked: Vec<String> = st.topics.keys().map(|k| k.to_string()).collect();
        drop(st);
        // not cut short by a message's deadline (`poll` and the broker threads time messages out
        // meanwhile): a lookup given less than a broker's answer takes fails again and again
        let timeout = sh.s.request_timeout.min(Duration::from_secs(5));
        let mut failed = false;
        if need_metadata {
            let names: Vec<&str> = asked.iter().map(String::as_str).collect();
            match sh.client.metadata(Some(&names), timeout) {
                Ok(md) => {
                    apply_metadata(sh, &md);
                    // a topic still without partitions or a leader (being created, a leader
                    // elected) is looked up again after the backoff, not at once
                    failed = unresolved(&sh.lock());
                }
                Err(_) => failed = true,
            }
        }
        if need_pid {
            match init_producer_id(&sh.client, timeout) {
                Ok(pid) => {
                    sh.lock().pid = Some(pid);
                    sh.work.notify_all();
                }
                Err(Error::Broker(c, _)) if !c.retriable() => sh.lock().set_fatal(c, "InitProducerId refused".into()),
                Err(_) => failed = true,
            }
        }
        if failed {
            std::thread::sleep(backoff);
            backoff = (backoff * 2).min(sh.s.retry_backoff_max);
        } else {
            backoff = sh.s.retry_backoff;
        }
    }
}

/// Whether messages wait on the metadata: sent to a topic whose partitions are not known yet, or
/// to a partition without a leader.
fn unresolved<O>(st: &State<O>) -> bool {
    let waits = |p: &Part<O>| p.leader.is_none() && (!p.queue.is_empty() || p.inflight.is_some());
    st.topics.values().any(|t| !t.unassigned.is_empty() || t.parts.values().any(waits))
}

fn init_producer_id(client: &Client, timeout: Duration) -> Result<(i64, i16)> {
    let frame = client.call_any(wire::INIT_PRODUCER_ID, &wire::init_producer_id_request, timeout)?;
    match wire::init_producer_id(Cur::new(&frame))? {
        (Code::NONE, pid, epoch) => Ok((pid, epoch)),
        (c, _, _) => Err(Error::Broker(c, "InitProducerId".into())),
    }
}

/// The partitions and leaders the metadata names; a thread started for each new leader.
fn apply_metadata<C: Deliveries>(sh: &Arc<Shared<C>>, md: &wire::Metadata) {
    let mut st = sh.lock();
    let st = &mut *st;
    for b in &md.brokers {
        st.brokers.insert(b.id, format!("{}:{}", b.host, b.port));
    }
    let (mut new_leaders, mut gone) = (std::collections::BTreeSet::new(), vec![]);
    for (name, t) in st.topics.iter_mut() {
        let Some(m) = md.topic(name) else { continue };
        if m.error != Code::NONE {
            t.missing = Some(m.error);
            if matches!(m.error, Code::INVALID_TOPIC_EXCEPTION | Code::TOPIC_AUTHORIZATION_FAILED) {
                // for good: its messages fail now, not at their timeout
                let e = || Error::Broker(m.error, format!("topic {name}"));
                gone.extend(t.unassigned.drain(..).map(|p| (name.clone(), None, e(), p)));
            }
            continue;
        }
        t.missing = None;
        let mut ps: Vec<i32> = m.partitions.iter().map(|p| p.id).collect();
        ps.sort_unstable();
        if t.partitions.is_empty() && !ps.is_empty() {
            // a producer's first partition drawn at random: producers spread
            t.sticky = (xorshift(&mut st.rng) % ps.len() as u64) as usize;
        }
        t.partitions = ps;
        for p in &m.partitions {
            let part = t.parts.entry(p.id).or_insert_with(|| Part {
                queue: VecDeque::new(),
                seq: 0,
                inflight: None,
                leader: None,
            });
            part.leader = (p.leader >= 0).then_some(p.leader);
            if p.leader >= 0 {
                new_leaders.insert(p.leader);
            }
        }
        // keyless messages stick to no partition without a leader, nor one now gone
        if !t.led(t.sticky) || t.sticky >= t.partitions.len() {
            t.rotate(xorshift(&mut st.rng));
        }
        // what was sent before the topic's partitions were known, each to its partition; the
        // keyless a batch's worth a partition
        if t.partitions.is_empty() {
            continue;
        }
        let mut keyless = 0;
        for p in std::mem::take(&mut t.unassigned) {
            let sticky = p.partition.is_none() && p.key.is_none();
            match t.route(p.partition, p.key.as_deref()) {
                Ok(Some(part)) => {
                    t.parts.get_mut(&part).expect("a partition").queue.push_back(p);
                    keyless += sticky as usize;
                    if sticky && keyless % sh.s.batch_messages == 0 {
                        t.rotate(xorshift(&mut st.rng));
                    }
                }
                _ => {
                    let e = Error::Broker(Code::UNKNOWN_TOPIC_OR_PARTITION, format!("{name}/{:?}", p.partition));
                    gone.push((name.clone(), p.partition, e, p));
                }
            }
        }
    }
    for (t, p, e, m) in gone {
        st.report(&t, p, None, Some(e), Persisted::No, m);
    }
    st.refreshed = Instant::now();
    // a thread a leader, its requests pipelined on its one connection (`max.in.flight`); one that
    // ended or panicked is made again here
    for id in new_leaders {
        if st.threads.contains(&id) {
            continue;
        }
        let (sh, by) = (sh.clone(), st.next_thread);
        st.next_thread += 1;
        let spawned =
            std::thread::Builder::new().name(format!("fastka-broker-{id}")).spawn(move || broker(&sh, id, by));
        if spawned.is_ok() {
            st.threads.push(id); // else tried again at the next metadata
        }
    }
    sh.work.notify_all();
}

/// Messages queued past their deadline given up; a batch in flight past its first message's
/// deadline too, which makes the producer fatal if the broker may have written it (its sequence
/// may now have a gap), else gives its sequence to the next batch.
fn expire<O>(st: &mut State<O>, now: Instant) {
    let mut gone = vec![];
    let mut gap = None;
    for (name, t) in st.topics.iter_mut() {
        let missing = t.missing;
        let why = || match missing {
            Some(c) => Error::Broker(c, format!("topic {name}")),
            None => Error::MessageTimedOut,
        };
        while t.unassigned.front().is_some_and(|p| p.deadline <= now) {
            gone.push((name.clone(), None, why(), Persisted::No, t.unassigned.pop_front().expect("one")));
        }
        for (id, part) in t.parts.iter_mut() {
            while part.queue.front().is_some_and(|p| p.deadline <= now) {
                let p = part.queue.pop_front().expect("one");
                gone.push((name.clone(), Some(*id), Error::MessageTimedOut, Persisted::No, p));
            }
            let timed_out = part.inflight.as_ref().is_some_and(|s| !s.sending && s.deadline <= now);
            if timed_out {
                let s = part.inflight.take().expect("in flight");
                let persisted = if s.maybe_written {
                    gap = Some(format!("{name}/{id}: a batch timed out in flight (message.timeout.ms)"));
                    Persisted::Possibly
                } else {
                    part.seq = s.seq; // never written: the broker's next sequence is still its
                    Persisted::No
                };
                let timed_out = |p| (name.clone(), Some(*id), Error::MessageTimedOut, persisted, p);
                gone.extend(s.msgs.into_iter().map(timed_out));
            }
        }
    }
    for (t, p, e, persisted, m) in gone {
        st.report(&t, p, None, Some(e), persisted, m);
    }
    if let Some(why) = gap {
        st.set_fatal(Code::MSG_TIMED_OUT, why);
    }
}

fn fail_queued<O>(st: &mut State<O>) {
    let (c, m) = st.fatal.clone().expect("fatal");
    let mut gone = vec![];
    for (name, t) in st.topics.iter_mut() {
        gone.extend(t.unassigned.drain(..).map(|p| (name.clone(), None, Persisted::No, p)));
        for (id, part) in t.parts.iter_mut() {
            gone.extend(part.queue.drain(..).map(|p| (name.clone(), Some(*id), Persisted::No, p)));
            if part.inflight.as_ref().is_some_and(|s| !s.sending) {
                let s = part.inflight.take().expect("one");
                let persisted = if s.maybe_written { Persisted::Possibly } else { Persisted::No };
                gone.extend(s.msgs.into_iter().map(|p| (name.clone(), Some(*id), persisted, p)));
            }
        }
    }
    for (t, p, persisted, msg) in gone {
        st.report(&t, p, None, Some(Error::Fatal(c, m.clone())), persisted, msg);
    }
}

/// The soonest a message is due to be timed out. A batch being sent is not timed out until its
/// attempt ends (which wakes the waiters), so it is left out: else its deadline, once past, would
/// be waited for again and again at once.
fn next_deadline<O>(st: &State<O>) -> Option<Instant> {
    (st.topics.values())
        .flat_map(|t| {
            let parts = t.parts.values().flat_map(|p| {
                let sent = p.inflight.iter().filter(|s| !s.sending).map(|s| s.deadline);
                p.queue.front().map(|m| m.deadline).into_iter().chain(sent)
            });
            t.unassigned.front().map(|m| m.deadline).into_iter().chain(parts)
        })
        .min()
}

/// Whether a retriable answer to a Produce says its batch was not appended: the broker checks for
/// these before it appends. Any other (REQUEST_TIMED_OUT, NOT_ENOUGH_REPLICAS_AFTER_APPEND, a
/// leader that moved while waiting for its replicas) may come after the batch was appended.
fn unwritten(c: Code) -> bool {
    matches!(
        c,
        Code::UNKNOWN_TOPIC_OR_PARTITION
            | Code::LEADER_NOT_AVAILABLE
            | Code::NOT_ENOUGH_REPLICAS
            | Code::FENCED_LEADER_EPOCH
            | Code::UNKNOWN_LEADER_EPOCH
            | Code::THROTTLING_QUOTA_EXCEEDED
    )
}

/// Whether a refusal for good says its batch was not appended: the broker checks the request and
/// each batch for these before it appends. Any other (one this list or this crate does not know,
/// one a newer broker gives) may come after: its sequence may be the broker's.
fn refused_before_append(c: Code) -> bool {
    matches!(
        c,
        Code::CORRUPT_MESSAGE
            | Code::MESSAGE_TOO_LARGE
            | Code::INVALID_TOPIC_EXCEPTION
            | Code::RECORD_LIST_TOO_LARGE
            | Code::INVALID_REQUIRED_ACKS
            | Code::TOPIC_AUTHORIZATION_FAILED
            | Code::INVALID_TIMESTAMP
            | Code::UNSUPPORTED_FOR_MESSAGE_FORMAT
            | Code::POLICY_VIOLATION
            | Code::UNSUPPORTED_COMPRESSION_TYPE
            | Code::INVALID_RECORD
    )
}

/// Whether a refusal is about the producer's sequences or id, not the batch: whatever was sent,
/// what the broker holds for the partition no longer follows on.
fn sequence_broken(c: Code) -> bool {
    matches!(
        c,
        Code::OUT_OF_ORDER_SEQUENCE_NUMBER
            | Code::UNKNOWN_PRODUCER_ID
            | Code::INVALID_PRODUCER_EPOCH
            | Code::PRODUCER_FENCED
            | Code::CLUSTER_AUTHORIZATION_FAILED
    )
}

/// What a broker thread sends next: per partition (topic, partition, its batch's bytes, its
/// first message's deadline).
type Request = Vec<(Arc<str>, i32, Arc<Vec<u8>>, Instant)>;

/// A batch sealed, for the broker thread that took it to encode off the lock: topic, partition,
/// its messages, its stamp. Its `Sent` waits in the partition, `sending`, its messages out until
/// they come back (`encoded`).
type Fresh<O> = Vec<(Arc<str>, i32, Vec<Pending<O>>, Stamp)>;

/// The batches broker `id` leads that are due, taken by its thread `by` (marked sending): sealed
/// from their queues where none is in flight (once lingered, or full; `Fresh`, to encode), sent
/// again where one is; the longest waiting first, up to `max.request.size` a request (at least one
/// batch: what does not fit goes in the broker's next request).
fn take_due<C: Deliveries>(
    sh: &Shared<C>,
    st: &mut State<C::Opaque>,
    id: i32,
    by: u64,
    now: Instant,
) -> (Request, Fresh<C::Opaque>, Option<Instant>) {
    let s = &sh.s;
    let (mut out, mut fresh): (Request, Fresh<C::Opaque>) = (vec![], vec![]);
    let mut next: Option<Instant> = None;
    // what is past its deadline is given up here, whichever thread runs first after a stall:
    // nothing is sent past `message.timeout.ms`
    expire(st, now);
    if st.fatal.is_some() {
        // nothing more is sent: what is queued fails with the fatal error (`fail_queued`)
        fail_queued(st);
        return (out, fresh, None);
    }
    let Some((pid, epoch)) = st.pid else { return (out, fresh, None) };
    // what is due: since when, its partition, how many messages to seal (0: one to send again),
    // its bytes (a batch to seal: as `size_of` counts them, which its encoding does not pass)
    let mut due: Vec<(Instant, Arc<str>, i32, usize, usize)> = vec![];
    for (name, t) in st.topics.iter() {
        for (&pnum, part) in t.parts.iter().filter(|(_, p)| p.leader == Some(id)) {
            if let Some(sent) = &part.inflight {
                if !sent.sending && sent.retry_at <= now {
                    due.push((sent.retry_at, name.clone(), pnum, 0, sent.bytes.len()));
                } else if !sent.sending {
                    next = Some(next.map_or(sent.retry_at, |n| n.min(sent.retry_at)));
                }
                continue;
            }
            let Some(first) = part.queue.front() else { continue };
            let ready = first.enqueued + s.linger;
            let (mut n, mut bytes, limit) = (0, records::HEADER, s.batch_bytes.min(s.max_batch));
            for p in &part.queue {
                if n == s.batch_messages || (n > 0 && bytes + p.size() > limit) {
                    break;
                }
                n += 1;
                bytes += p.size();
            }
            // full: a message left for the next batch, as many as a batch takes, or no room for
            // even an empty record (`size_of`)
            let full = n < part.queue.len() || n == s.batch_messages || bytes + size_of(None, None, &[]) > limit;
            if ready > now && !full {
                next = Some(next.map_or(ready, |x| x.min(ready)));
                continue; // lingering: more may join the batch
            }
            due.push((first.enqueued, name.clone(), pnum, n, bytes));
        }
    }
    // the longest waiting first: a request's bound starves no partition
    due.sort_unstable_by_key(|d| d.0);
    let (mut sealed, mut total) = (vec![], 0);
    for (_, name, pnum, n, bytes) in due {
        if !(out.is_empty() && fresh.is_empty()) && total + bytes > s.max_request {
            break; // for the next request
        }
        total += bytes;
        let part = st.topics.get_mut(&name).and_then(|t| t.parts.get_mut(&pnum)).expect("a partition due");
        if n == 0 {
            let sent = part.inflight.as_mut().expect("a batch to send again");
            (sent.sending, sent.by) = (true, by);
            sent.attempts += 1;
            out.push((name, pnum, sent.bytes.clone(), sent.deadline));
            continue;
        }
        let msgs: Vec<_> = part.queue.drain(..n).collect();
        let seq = part.seq;
        // Kafka's sequences wrap to 0 past i32::MAX
        part.seq = part.seq.wrapping_add(n as i32) & i32::MAX;
        part.inflight = Some(Sent {
            msgs: vec![], // out, being encoded
            bytes: Arc::new(vec![]),
            seq,
            deadline: msgs[0].deadline,
            sending: true,
            by,
            retry_at: now,
            attempts: 1,
            maybe_written: false,
        });
        fresh.push((name.clone(), pnum, msgs, Stamp { producer_id: pid, epoch, base_sequence: seq }));
        sealed.push((name, pnum));
    }
    // the next keyless messages go to another partition: a partition a batch (librdkafka's sticky
    // partitioner)
    for (name, pnum) in sealed {
        let t = st.topics.get_mut(&name).expect("the topic");
        if t.partitions.get(t.sticky) == Some(&pnum) {
            t.rotate(xorshift(&mut st.rng));
        }
    }
    (out, fresh, next)
}

/// `fresh`'s batches encoded (copied, compressed, their CRCs taken), off the lock: each its bytes,
/// or why it could not be (a panic in the codec too: its messages fail, none is lost with it).
fn encode<O>(fresh: &Fresh<O>, codec: i16, scratch: &mut Vec<u8>) -> Vec<Result<Vec<u8>>> {
    let written = (fresh.iter())
        .map(|(_, _, msgs, stamp)| {
            let outs: Vec<Out<'_>> = (msgs.iter())
                .map(|p| Out {
                    timestamp: p.timestamp,
                    key: p.key.as_deref(),
                    value: p.value.as_deref(),
                    headers: &p.headers,
                })
                .collect();
            let mut b = vec![];
            let write = std::panic::AssertUnwindSafe(|| records::write(&mut b, &outs, *stamp, codec, scratch));
            match std::panic::catch_unwind(write) {
                Ok(written) => written.map(|()| b),
                Err(_) => Err(Error::Protocol("a batch's encoding panicked".into())),
            }
        })
        .collect();
    // kept for the next batch up to 1 MiB; a larger one (a burst's) given back, not held for the
    // producer's life. ponytail: a pool if the allocations show in a profile
    if scratch.capacity() > 1 << 20 {
        *scratch = vec![];
    }
    written
}

/// Encoded batches back in their partitions and in `req`, up to `max.request.size` (a batch's
/// bytes encoded may pass what `take_due` counted for it: then it is left sealed, sent in the next
/// request as early as it was due). One that failed to encode fails its messages, its sequence
/// given back: nothing was sent at it, and no later batch of its partition was sealed.
fn encoded<C: Deliveries>(
    sh: &Shared<C>,
    st: &mut State<C::Opaque>,
    req: &mut Request,
    fresh: Fresh<C::Opaque>,
    written: Vec<Result<Vec<u8>>>,
) {
    let (mut total, mut failed) = (req.iter().map(|r| r.2.len()).sum::<usize>(), vec![]);
    for ((name, pnum, msgs, _), written) in fresh.into_iter().zip(written) {
        // a partition is never removed, nor a topic with partitions
        let part = st.topics.get_mut(&name).and_then(|t| t.parts.get_mut(&pnum)).expect("a partition sending");
        match written {
            Ok(b) => {
                let sent = part.inflight.as_mut().expect("its batch, sending");
                (sent.bytes, sent.msgs) = (Arc::new(b), msgs);
                if !req.is_empty() && total + sent.bytes.len() > sh.s.max_request {
                    (sent.sending, sent.attempts, sent.retry_at) = (false, 0, sent.msgs[0].enqueued);
                    continue;
                }
                total += sent.bytes.len();
                req.push((name, pnum, sent.bytes.clone(), sent.deadline));
            }
            Err(e) => {
                let sent = part.inflight.take().expect("its batch, sending");
                part.seq = sent.seq;
                failed.push((name, pnum, msgs, e));
            }
        }
    }
    for (name, pnum, msgs, e) in failed {
        for m in msgs {
            st.report(&name, Some(pnum), None, Some(e.clone()), Persisted::No, m);
        }
    }
}

/// A Produce request sent and not yet answered: its batches, its correlation id, by when its
/// answer must come.
struct Asked {
    req: Request,
    corr: i32,
    until: Instant,
}

/// A thread that panics is no longer the broker's: a new one is started at the next metadata,
/// else its partitions would never be sent again.
struct Gone<'a, C: Deliveries>(&'a Shared<C>, i32, u64);
impl<C: Deliveries> Drop for Gone<'_, C> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let mut st = self.0.lock();
            if let Some(i) = st.threads.iter().position(|t| *t == self.1) {
                st.threads.swap_remove(i);
            }
            st.metadata_due = true;
            // its batches being sent (a thread before it of the same broker may have panicked
            // too: those are handed back by its own guard) are its successor's to send again:
            // possibly written (it may have written them), so a failure of one is fatal, never a
            // rewind
            let now = Instant::now();
            for part in st.topics.values_mut().flat_map(|t| t.parts.values_mut()) {
                let Some(sent) = part.inflight.as_mut().filter(|s| s.sending && s.by == self.2) else { continue };
                if sent.msgs.is_empty() {
                    // sealed, its messages out being encoded (`encode` catches a codec's
                    // panic, so not reached): never sent, its sequence given back
                    part.seq = sent.seq;
                    part.inflight = None;
                    continue;
                }
                (sent.sending, sent.retry_at, sent.maybe_written) = (false, now, true);
            }
            drop(st);
            self.0.work.notify_all();
            self.0.reported.notify_all();
        }
    }
}

/// Broker `id`'s thread (`by`, its own number): its partitions' batches sent as they are due,
/// encoded off the lock, on one connection with up to `max.in.flight` requests outstanding
/// (librdkafka's pipelining); the answers, which come in the order asked, applied. It connects at
/// once, before its first batch is due.
fn broker<C: Deliveries>(sh: &Arc<Shared<C>>, id: i32, by: u64) {
    let _gone = Gone(sh, id, by);
    let addr = sh.lock().brokers.get(&id).cloned();
    let mut conn: Option<Conn> = addr.and_then(|a| Conn::open(sh.client.net(), &a).ok());
    let (mut backoff, mut scratch, mut buf) = (sh.s.retry_backoff, vec![], vec![]);
    let mut asked: VecDeque<Asked> = VecDeque::new();
    // the broker asked for a wait before the next request (KIP-219): none is sent until then
    let mut throttled = Instant::now();
    loop {
        // with room for another request: when the next batch is due
        let mut due_at = None;
        if asked.len() < sh.s.max_in_flight {
            let mut st = sh.lock();
            let leads = st.topics.values().any(|t| t.parts.values().any(|p| p.leader == Some(id)));
            if st.closing || (asked.is_empty() && !leads) {
                st.threads.retain(|t| *t != id);
                return;
            }
            let now = Instant::now();
            let (mut req, fresh, next) = match throttled.saturating_duration_since(now).is_zero() {
                true => take_due(sh, &mut st, id, by, now),
                false => (vec![], vec![], Some(throttled)),
            };
            sh.reported.notify_all();
            if !req.is_empty() || !fresh.is_empty() {
                let addr = st.brokers.get(&id).cloned();
                drop(st);
                if !fresh.is_empty() {
                    // the batches sealed are encoded off the lock: `send`, `poll` and the other
                    // brokers' threads go on meanwhile; only this thread touches a batch it sends
                    let written = encode(&fresh, sh.s.codec, &mut scratch);
                    encoded(sh, &mut sh.lock(), &mut req, fresh, written);
                    sh.reported.notify_all();
                }
                if !req.is_empty() {
                    match ask(sh, &mut conn, id, addr, req) {
                        Ok(a) => asked.push_back(a),
                        // past its deadline before it was sent: its batches left to `expire`, the
                        // connection and the requests outstanding on it kept
                        Err((req, None)) => unsent(sh, &req),
                        Err((req, Some((_, written)))) => {
                            asked.push_back(Asked { req, corr: 0, until: Instant::now() });
                            failed(sh, &mut conn, &mut asked, &mut buf, &mut backoff, Some(written));
                        }
                    }
                }
                continue; // another request, while there is room
            }
            if asked.is_empty() {
                let wait = next.map_or(Duration::from_millis(100), |n| n.saturating_duration_since(now));
                let _ = sh.work.wait_timeout(st, wait.clamp(Duration::from_micros(200), Duration::from_millis(100)));
                continue;
            }
            due_at = Some(next.unwrap_or(now + Duration::from_millis(100)));
        }
        // the oldest answer read, until its deadline; with room for another request, only until
        // the next batch is due. ponytail: a message queued meanwhile is seen within linger.ms
        // (1 ms at least), the socket's read waking the thread, not `send`; a waker if it shows
        let oldest = asked.front().expect("a request outstanding").until;
        let poll = Instant::now() + sh.s.linger.max(Duration::from_millis(1));
        let until = due_at.map_or(oldest, |d| oldest.min(d).min(poll));
        let read = match conn.as_mut() {
            Some(c) => c.recv(&mut buf, until),
            None => Err(Error::Transport(format!("broker {id}: not connected"))),
        };
        match read {
            // short of its deadline (a read wakes early for what came due): read again
            Ok(false) if !oldest.saturating_duration_since(Instant::now()).is_zero() => continue,
            Ok(false) => failed(sh, &mut conn, &mut asked, &mut buf, &mut backoff, None), // no answer by its deadline
            Ok(true) => {
                let a = asked.pop_front().expect("the request answered");
                let version = conn.as_ref().map_or(wire::version(wire::PRODUCE), |c| c.version(wire::PRODUCE));
                let answer = wire::response(&buf[4..], a.corr).and_then(|c| wire::produce(c, version));
                buf.clear();
                match answer {
                    Ok((results, throttle_ms)) => {
                        backoff = sh.s.retry_backoff;
                        applied(sh, &mut sh.lock(), &a.req, results);
                        if throttle_ms > 0 {
                            let wait = Duration::from_millis(throttle_ms as u64).min(Duration::from_secs(30));
                            throttled = throttled.max(Instant::now() + wait);
                        }
                        sh.work.notify_all();
                        sh.reported.notify_all();
                    }
                    Err(_) => {
                        asked.push_front(a);
                        failed(sh, &mut conn, &mut asked, &mut buf, &mut backoff, None);
                    }
                }
            }
            Err(_) => failed(sh, &mut conn, &mut asked, &mut buf, &mut backoff, None),
        }
    }
}

/// The connection given up: every batch outstanding on it sent again, after a backoff and a
/// metadata refresh. Each was written, so may have been appended (`maybe_written`), unless
/// `written` says otherwise of the last request (it failed before anything of it went out). An
/// authentication refused (credentials reach a broker late) or versions not spoken (a broker
/// mid-upgrade) too: retried until the messages' deadline.
fn failed<C: Deliveries>(
    sh: &Shared<C>,
    conn: &mut Option<Conn>,
    asked: &mut VecDeque<Asked>,
    buf: &mut Vec<u8>,
    backoff: &mut Duration,
    written: Option<bool>,
) {
    (*conn, *buf) = (None, vec![]);
    let at = Instant::now() + *backoff;
    *backoff = (*backoff * 2).min(sh.s.retry_backoff_max);
    let last = asked.len().saturating_sub(1);
    let mut st = sh.lock();
    for (i, a) in asked.drain(..).enumerate() {
        let written = if i == last { written.unwrap_or(true) } else { true };
        for (t, p, _, _) in a.req {
            if let Some(sent) =
                st.topics.get_mut(&t).and_then(|t| t.parts.get_mut(&p)).and_then(|p| p.inflight.as_mut())
            {
                sent.sending = false;
                sent.retry_at = at;
                sent.maybe_written |= written;
            }
        }
    }
    st.metadata_due = true;
    drop(st);
    sh.work.notify_all();
    sh.reported.notify_all();
}

/// Batches taken but not sent (past their deadline first): free again, and timed out (`expire`).
fn unsent<C: Deliveries>(sh: &Shared<C>, req: &Request) {
    let mut st = sh.lock();
    for (t, p, _, _) in req {
        if let Some(sent) = st.topics.get_mut(t).and_then(|t| t.parts.get_mut(p)).and_then(|p| p.inflight.as_mut()) {
            sent.sending = false;
        }
    }
    expire(&mut st, Instant::now());
    drop(st);
    sh.reported.notify_all();
}

/// What `ask` gives back of a request it did not send: None when it was past its deadline first;
/// else the error, and whether anything of it may have been written.
type NotSent = (Request, Option<(Error, bool)>);

/// `req` sent on the broker's connection (made first if there is none, within the first
/// message's deadline), not waiting for its answer.
fn ask<C: Deliveries>(
    sh: &Shared<C>,
    conn: &mut Option<Conn>,
    id: i32,
    addr: Option<String>,
    req: Request,
) -> std::result::Result<Asked, NotSent> {
    // given up at the first message's deadline (message.timeout.ms bounds a request in flight,
    // as librdkafka bounds it): a reconnect is given no longer, the broker is asked to answer by
    // then, and the connection is dropped if it has not
    let first = req.iter().map(|r| r.3).min().expect("a batch");
    let left = first.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err((req, None)); // stalled between taking it and sending it
    }
    if conn.is_none() {
        let opened = addr
            .ok_or_else(|| Error::Transport(format!("broker {id}: no address")))
            .and_then(|a| Conn::open_within(sh.client.net(), &a, left));
        match opened {
            Ok(c) => *conn = Some(c),
            Err(e) => return Err((req, Some((e, false)))),
        }
    }
    let c = conn.as_mut().expect("connected");
    // what the connecting took
    let left = first.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err((req, None));
    }
    let timeout = sh.s.request_timeout.min(left);
    let until = Instant::now() + left.min(sh.s.request_timeout + sh.client.net().request_timeout);
    let ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX).max(1);
    let batches: Vec<(&str, (i32, &[u8]))> = req.iter().map(|(t, p, b, _)| (&**t, (*p, b.as_slice()))).collect();
    let sent = c.send_request(wire::PRODUCE, until, |o| wire::produce_request(o, -1, ms, &batches));
    drop(batches);
    match sent {
        Ok(corr) => Ok(Asked { req, corr, until }),
        // a request may be written in part, whatever went wrong: the broker may have it
        Err(e) => Err((req, Some((e, true)))),
    }
}

/// A Produce answered: each batch delivered, sent again, or failed: alone where no attempt of it
/// may have been written (its sequence then the next batch's), else with the producer fatal.
fn applied<C: Deliveries>(sh: &Shared<C>, st: &mut State<C::Opaque>, req: &Request, results: Vec<wire::Produced>) {
    let now = Instant::now();
    let (mut done, mut moved) = (vec![], vec![]);
    for (t, p, _, _) in req {
        let r = results.iter().find(|r| *r.topic == **t && r.partition == *p);
        let Some(part) = st.topics.get_mut(t).and_then(|x| x.parts.get_mut(p)) else { continue };
        let Some(mut sent) = part.inflight.take() else { continue };
        sent.sending = false;
        // a partition the answer left out: as a broker that failed it
        let code = r.map_or(Code::UNKNOWN_SERVER_ERROR, |r| r.error);
        // what the broker said, if it said more than its code (the topic and partition are the
        // delivery's own)
        let why = r.and_then(|r| r.message.clone()).unwrap_or_default();
        match code {
            Code::NONE | Code::DUPLICATE_SEQUENCE_NUMBER => {
                // a duplicate: written before, its answer lost (librdkafka: delivered, offset unknown)
                let base = r.map(|r| r.base_offset).filter(|o| *o >= 0 && code == Code::NONE);
                done.push((t.clone(), *p, sent.msgs, Ok(base)));
            }
            c if c.retriable() => {
                sent.maybe_written |= !unwritten(c);
                if c.stale_metadata() {
                    part.leader = None;
                    st.metadata_due = true;
                    moved.push((t.clone(), *p));
                }
                sent.retry_at = now + retry_wait(&sh.s, sent.attempts);
                part.inflight = Some(sent);
            }
            c => {
                // one batch a partition in flight: refused before it was appended, and no attempt
                // of it written, no later sequence was sent, and the broker's next is this batch's
                sent.maybe_written |= !refused_before_append(c) && !sequence_broken(c);
                let gap = sent.maybe_written || sequence_broken(c);
                if !gap {
                    part.seq = sent.seq;
                }
                done.push((t.clone(), *p, sent.msgs, Err((c, why, gap, sent.maybe_written))));
            }
        }
    }
    // keyless messages off a partition whose leader moved, till the metadata says where
    for (name, p) in moved {
        let t = st.topics.get_mut(&name).expect("the topic");
        if t.partitions.get(t.sticky) == Some(&p) {
            t.rotate(xorshift(&mut st.rng));
        }
    }
    for (t, p, msgs, r) in done {
        match r {
            Ok(base) => {
                for (i, m) in msgs.into_iter().enumerate() {
                    st.report(&t, Some(p), base.map(|b| b + i as i64), None, Persisted::Yes, m);
                }
            }
            Err((c, why, gap, maybe_written)) => {
                let persisted = if maybe_written { Persisted::Possibly } else { Persisted::No };
                for m in msgs {
                    st.report(&t, Some(p), None, Some(Error::Broker(c, why.clone())), persisted, m);
                }
                if !gap {
                    continue;
                }
                // the broker's sequence for the partition may have a gap now
                st.set_fatal(
                    c,
                    format!(
                        "a batch to {t}/{p} refused{}",
                        if why.is_empty() { String::new() } else { format!(": {why}") }
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
