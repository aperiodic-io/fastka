//! The consumer: each partition leader's Fetch sent from the caller's thread on a non-blocking
//! socket, the next Fetch sent before a response is parsed (the broker prepares it meanwhile),
//! each partition's records queued for the caller to take (`poll`, `next_any`). No threads hand
//! messages over and no message is an object of its own until it is taken.
//!
//! Nothing blocks the caller's thread: connecting (a resolver, a SYN dropped) and looking up
//! leaders and offsets run on threads of their own; a lookup that fails is tried again later,
//! with a doubling backoff, as librdkafka recovers by itself. A lost connection, a moved leader,
//! an offset out of range (`auto.offset.reset`), each is retried from the next offset not yet
//! given, and told to the caller's `notice` (a [`Notice`], to count and log).
//!
//! Not done: KIP-320's leader epochs (a Fetch names none, so a truncation after an unclean
//! leader election is not detected).
use crate::client::Client;
use crate::config::{Config, ConsumerSettings, Net, Reset};
use crate::conn::{self, Conn, Stream};
use crate::error::{Code, Error, Result};
use crate::records::{self, ReadOpts};
use crate::wire::{self, FetchLimits};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Where a partition is read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Offset {
    Beginning,
    End,
    /// at least 0
    At(i64),
}

fn checked(t: &str, p: i32, o: Offset) -> Result<()> {
    match o {
        Offset::At(n) if n < 0 => Err(Error::Config(format!("{t}/{p}: offset {n}: an offset is at least 0"))),
        _ => Ok(()),
    }
}

/// A message as the consumer gives it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Message {
    pub offset: i64,
    /// ms
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    /// None: a tombstone
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl Message {
    pub fn header(&self, k: &str) -> Option<&[u8]> {
        self.headers.iter().find(|(n, _)| n == k).and_then(|(_, v)| v.as_deref())
    }
}

/// What `poll` gives of a partition.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Polled {
    Msg(Message),
    /// The partition's end: every record it held when it was fetched last has been given
    /// (with `enable.partition.eof`).
    Eof,
    /// nothing yet (or the partition is paused)
    Empty,
}

/// A partition to consume: its topic, number, where from, and whether it starts paused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    topic: String,
    partition: i32,
    offset: Offset,
    paused: bool,
}

impl Assignment {
    pub fn new(topic: impl Into<String>, partition: i32, offset: Offset) -> Assignment {
        Assignment { topic: topic.into(), partition, offset, paused: false }
    }
    /// Not fetched, nor given, until `Consumer::resume`.
    pub fn paused(self) -> Assignment {
        Assignment { paused: true, ..self }
    }
}

/// What the consumer mended by itself, told to the caller's `notice` (to count and log).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Notice {
    /// The partition's offset `from` was out of range: read on from `to` (`auto.offset.reset`).
    /// The records between are not given.
    Reset { topic: String, partition: i32, from: i64, to: i64 },
    /// Something tried again (a connection lost, a leader moved, a lookup failed): no record is
    /// lost or given twice for it.
    Retrying(String),
}

impl std::fmt::Display for Notice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Notice::Reset { topic, partition, from, to } => {
                write!(f, "{topic}/{partition}: offset {from} is out of range: reset to {to} (auto.offset.reset)")
            }
            Notice::Retrying(why) => f.write_str(why),
        }
    }
}

/// A lookup's longest; one that fails is tried again later.
const LOOKUP: Duration = Duration::from_secs(2);
const BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// A Fetch unanswered this long past its own wait has its partitions' leaders looked up, and again
/// each time this long passes: a broker cut off from the network answers nothing and its
/// connection stays open until `socket.timeout.ms`, while its partitions get new leaders in
/// seconds. A partition led elsewhere now drops the connection and is fetched from its new leader.
const OVERDUE: Duration = Duration::from_secs(1);
/// A broker's responses in a row that cannot be read (a corrupt batch) fetched again before it
/// is an error.
const CORRUPT_RETRIES: u32 = 3;
/// `next_any`'s longest run of a partition before the next's.
const RUN: usize = 1000;
/// A socket found empty is read again no sooner than this. The data loop pumps on every pass:
/// at a read of nothing each, CI's runner (each system call checked by AppArmor) spent a tenth of
/// the data thread there, 500k reads in a 600k-message catch-up. What arrives waits this long at
/// most.
// ponytail: a fixed wait, not the socket's readiness (`poll`/`epoll`, a call too); a wait to
// tune if a pipeline's latency is gated below 0.1 ms
const QUIET_READ: Duration = Duration::from_micros(100);
/// `next_event`'s longest wait on its sockets before it looks at its timers again (a
/// reconnect due, a lookup, a Fetch overdue), and what a readiness missed costs at most.
const WAIT: Duration = Duration::from_millis(10);

enum Link {
    /// not connected: may connect at `.0`
    Idle(Instant),
    Connecting(mpsc::Receiver<Result<Conn>>),
    Ready(Stream),
}

struct InFlight {
    corr: i32,
    /// (partition, offset, its epoch then: one seeked since is not this Fetch's to answer)
    asked: Vec<(usize, i64, u32)>,
    since: Instant,
}

struct Broker {
    id: i32,
    addr: String,
    link: Link,
    out: Vec<u8>,
    written: usize,
    buf: Vec<u8>,
    inflight: Option<InFlight>,
    corr: i32,
    backoff: Duration,
    /// its responses in a row that could not be read
    corrupt: u32,
    /// its Fetches sent: where the next starts in its partitions (KIP-74 gives only the first
    /// partition with data a batch past its max bytes; each must be first in turn)
    rotate: usize,
    /// no request before then: the broker asked for the wait (`throttle_time_ms`, KIP-219)
    throttled: Instant,
    /// when its socket, found empty, is read again (`QUIET_READ`)
    quiet_until: Instant,
    /// when its partitions' leaders were last looked up for a Fetch overdue (`OVERDUE`)
    checked: Instant,
    /// its socket's reads, for the tests
    #[cfg(test)]
    reads: u64,
}

struct Part {
    topic: String,
    partition: i32,
    /// the next offset to fetch; None until resolved (`want`)
    next: Option<i64>,
    want: Offset,
    queue: VecDeque<Message>,
    paused: bool,
    broker: Option<usize>,
    /// the records of a response not yet queued
    incoming: usize,
    at_end: bool,
    eof: bool,
    /// one more at each `seek`: what was asked before it is not taken
    epoch: u32,
    /// the offset found out of range, until where to read on from is looked up
    reset_from: Option<i64>,
}

/// The Fetch of the partitions assigned it, on the caller's thread.
pub struct Consumer {
    client: Arc<Client>,
    net: Arc<Net>,
    s: ConsumerSettings,
    parts: Vec<Part>,
    brokers: Vec<Broker>,
    plain: Vec<u8>,
    /// when the leaders, and the offsets to resolve, may be looked up again
    lookup_at: Instant,
    lookup_backoff: Duration,
    turn: usize,
    run: usize,
    /// a lookup under way, and the partitions it is for: (index, epoch, its offset, its leader)
    lookup: Option<mpsc::Receiver<Found>>,
    looking: Vec<(usize, u32, bool, bool)>,
    /// the brokers' sockets, registered for readability (`wait`); None: waits are sleeps
    #[cfg(unix)]
    ready: Option<(mio::Poll, mio::Events)>,
}

/// A lookup's answer: the offsets asked for, each topic's leaders.
struct Found {
    offsets: Option<Result<Vec<crate::client::Listed>>>,
    leaders: Vec<(String, Result<crate::client::Leaders>)>,
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer").field("parts", &self.parts.len()).field("brokers", &self.brokers.len()).finish()
    }
}

impl Consumer {
    /// A consumer of `parts` on `client`'s cluster. An error: a partition assigned twice, or an
    /// offset below 0.
    pub fn new(client: Arc<Client>, s: ConsumerSettings, parts: Vec<Assignment>) -> Result<Consumer> {
        for (i, a) in parts.iter().enumerate() {
            checked(&a.topic, a.partition, a.offset)?;
            if parts[..i].iter().any(|b| b.topic == a.topic && b.partition == a.partition) {
                return Err(Error::Config(format!("{}/{}: assigned twice", a.topic, a.partition)));
            }
        }
        let parts = parts
            .into_iter()
            .map(|a| Part {
                next: match a.offset {
                    Offset::At(o) => Some(o),
                    _ => None,
                },
                topic: a.topic,
                partition: a.partition,
                want: a.offset,
                queue: VecDeque::new(),
                paused: a.paused,
                broker: None,
                incoming: 0,
                at_end: false,
                eof: false,
                epoch: 0,
                reset_from: None,
            })
            .collect();
        Ok(Consumer {
            net: client.net().clone(),
            client,
            s,
            parts,
            brokers: vec![],
            plain: vec![],
            lookup_at: Instant::now(),
            lookup_backoff: BACKOFF,
            turn: 0,
            run: 0,
            lookup: None,
            #[cfg(unix)]
            ready: mio::Poll::new().ok().map(|p| (p, mio::Events::with_capacity(16))),
            looking: vec![],
        })
    }

    /// A consumer of `parts`, its client and settings made of `cfg`.
    pub fn from_config(cfg: &Config, parts: Vec<Assignment>) -> Result<Consumer> {
        Consumer::new(Arc::new(Client::new(cfg)?), cfg.consumer()?, parts)
    }

    /// Its cluster's control plane.
    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }

    /// The partitions, in the order assigned (`poll`'s index).
    pub fn partitions(&self) -> impl Iterator<Item = (&str, i32)> {
        self.parts.iter().map(|p| (p.topic.as_str(), p.partition))
    }

    /// The next offset partition `i` gives (None: not resolved yet), past what is queued.
    ///
    /// Panics if `i` is not an index of `partitions`.
    pub fn position(&self, i: usize) -> Option<i64> {
        let p = &self.parts[i];
        p.queue.front().map(|m| m.offset).or(p.next)
    }

    /// Partition `i` read from `offset` on: what is queued of it, and what a Fetch in flight
    /// brings of it, dropped. After `auto.offset.reset=error`'s error, where to go on from.
    /// An error: an offset below 0 (nothing changed).
    ///
    /// Panics if `i` is not an index of `partitions`.
    pub fn seek(&mut self, i: usize, offset: Offset) -> Result<()> {
        let p = &mut self.parts[i];
        checked(&p.topic, p.partition, offset)?;
        p.epoch = p.epoch.wrapping_add(1);
        p.queue.clear();
        (p.incoming, p.at_end, p.eof, p.reset_from, p.want) = (0, false, false, None, offset);
        p.next = match offset {
            Offset::At(o) => Some(o),
            _ => None,
        };
        if p.next.is_none() {
            self.lookup_at = self.lookup_at.min(Instant::now());
        }
        Ok(())
    }

    /// Partitions that `which` names fetched and given again, or no longer (what is queued of a
    /// paused partition is kept for its resume).
    pub fn resume(&mut self, which: impl Fn(&str, i32) -> bool) {
        self.parts.iter_mut().filter(|p| which(&p.topic, p.partition)).for_each(|p| p.paused = false);
    }
    pub fn pause(&mut self, which: impl Fn(&str, i32) -> bool) {
        self.parts.iter_mut().filter(|p| which(&p.topic, p.partition)).for_each(|p| p.paused = true);
    }

    /// The offsets not resolved and the leaders of the partitions without a broker, looked up on
    /// a thread of its own once the backoff is past; its answer applied once it has one.
    fn lookups(&mut self, now: Instant, notice: &mut dyn FnMut(Notice)) {
        if let Some(rx) = &self.lookup {
            let done = match rx.try_recv() {
                Err(mpsc::TryRecvError::Empty) => return,
                Err(mpsc::TryRecvError::Disconnected) => None,
                Ok(done) => Some(done),
            };
            self.lookup = None;
            let failed = match done {
                None => {
                    notice(Notice::Retrying("looking up leaders and offsets: the thread ended".into()));
                    true
                }
                Some(d) => self.found(d, now, notice),
            };
            // only what it was for: a partition reset (or lost) while it ran is looked up at once
            let looking = std::mem::take(&mut self.looking);
            let failed = failed
                || looking.iter().any(|&(i, e, offset, leader)| {
                    let p = &self.parts[i];
                    p.epoch == e && ((offset && p.next.is_none()) || (leader && p.broker.is_none()))
                });
            if failed {
                self.lookup_at = now + self.lookup_backoff;
                self.lookup_backoff = (self.lookup_backoff * 2).min(MAX_BACKOFF);
            } else {
                (self.lookup_at, self.lookup_backoff) = (now, BACKOFF);
            }
            return;
        }
        let overdue: Vec<usize> = (0..self.brokers.len())
            .filter(|&b| self.overdue(b, now) && now >= self.brokers[b].checked + OVERDUE)
            .collect();
        if now < self.lookup_at
            || (overdue.is_empty() && self.parts.iter().all(|p| p.broker.is_some() && p.next.is_some()))
        {
            return;
        }
        for &b in &overdue {
            self.brokers[b].checked = now;
        }
        self.looking = (self.parts.iter().enumerate())
            .map(|(i, p)| (i, p.epoch, p.next.is_none(), p.broker.is_none_or(|b| overdue.contains(&b))))
            .filter(|l| l.2 || l.3)
            .collect();
        let unresolved: Vec<(String, i32, i64)> = (self.parts.iter())
            .filter(|p| p.next.is_none())
            .map(|p| (p.topic.clone(), p.partition, if p.want == Offset::End { wire::LATEST } else { wire::EARLIEST }))
            .collect();
        let mut topics: Vec<String> = (self.parts.iter())
            .filter(|p| p.broker.is_none_or(|b| overdue.contains(&b)))
            .map(|p| p.topic.clone())
            .collect();
        topics.sort_unstable();
        topics.dedup();
        let (client, committed) = (self.client.clone(), self.s.committed);
        let (tx, rx) = mpsc::channel();
        let spawned = std::thread::Builder::new().name("fastka-lookup".into()).spawn(move || {
            let offsets = (!unresolved.is_empty()).then(|| client.list_offsets(&unresolved, committed, LOOKUP));
            let leaders = topics.into_iter().map(|t| (t.clone(), client.leaders(&t, LOOKUP))).collect();
            let _ = tx.send(Found { offsets, leaders });
        });
        match spawned {
            Ok(_) => self.lookup = Some(rx),
            Err(e) => {
                notice(Notice::Retrying(format!("a thread to look up leaders and offsets: {e}")));
                self.lookup_at = now + self.lookup_backoff;
            }
        }
    }

    /// A lookup's answer applied: whether some of it failed.
    fn found(&mut self, d: Found, now: Instant, notice: &mut dyn FnMut(Notice)) -> bool {
        let mut failed = false;
        match d.offsets {
            None => {}
            Some(Ok(found)) => {
                for (t, p, r) in found {
                    let Some(i) = self.parts.iter().position(|x| x.topic == t && x.partition == p) else { continue };
                    let part = &mut self.parts[i];
                    // looked up for it as it is now: not seeked since
                    if part.next.is_some() || !self.looking.iter().any(|l| (l.0, l.1, l.2) == (i, part.epoch, true)) {
                        continue;
                    }
                    match r {
                        Ok(o) => {
                            let to = o.unwrap_or(0);
                            part.next = Some(to);
                            if let Some(from) = part.reset_from.take() {
                                notice(Notice::Reset { topic: t, partition: p, from, to });
                            }
                        }
                        Err(e) => {
                            failed = true;
                            notice(Notice::Retrying(format!("{t}/{p}: looking up where to read from: {e}")));
                        }
                    }
                }
            }
            Some(Err(e)) => {
                failed = true;
                notice(Notice::Retrying(format!("looking up where to read from: {e}")));
            }
        }
        for (t, r) in d.leaders {
            let (leaders, addrs) = match r {
                Ok(l) => l,
                Err(e) => {
                    failed = true;
                    notice(Notice::Retrying(format!("{t}: looking up its partitions' leaders: {e}")));
                    continue;
                }
            };
            let moved = (self.parts.iter()).find_map(|p| {
                let b = p.broker.filter(|&b| p.topic == t && self.overdue(b, now))?;
                let &(_, leader) = leaders.iter().find(|l| l.0 == p.partition)?;
                let (_, addr) = addrs.iter().find(|a| a.0 == leader)?;
                let br = &self.brokers[b];
                (leader != br.id || *addr != br.addr).then(|| {
                    let since = now.duration_since(br.inflight.as_ref().map_or(now, |f| f.since));
                    (
                        b,
                        format!(
                            "its Fetch unanswered for {since:?}, {t}/{} is led by broker {leader} now",
                            p.partition
                        ),
                    )
                })
            });
            if let Some((b, why)) = moved {
                self.drop_broker(b, now, why, notice);
            }
            for i in 0..self.parts.len() {
                let p = &self.parts[i];
                if p.topic != t || p.broker.is_some() {
                    continue;
                }
                let Some(&(_, leader)) = leaders.iter().find(|l| l.0 == p.partition) else { continue };
                let Some((_, addr)) = addrs.iter().find(|b| b.0 == leader) else { continue };
                let b = match self.brokers.iter().position(|b| b.id == leader && b.addr == *addr) {
                    Some(b) => b,
                    None => {
                        self.brokers.push(Broker {
                            id: leader,
                            addr: addr.clone(),
                            link: Link::Idle(now),
                            out: vec![],
                            written: 0,
                            buf: vec![],
                            inflight: None,
                            corr: 0,
                            backoff: self.net.backoff,
                            corrupt: 0,
                            rotate: 0,
                            throttled: now,
                            quiet_until: now,
                            checked: now,
                            #[cfg(test)]
                            reads: 0,
                        });
                        self.brokers.len() - 1
                    }
                };
                self.parts[i].broker = Some(b);
            }
        }
        failed
    }

    /// Whether broker `b`'s Fetch, written whole, is unanswered `OVERDUE` past its own wait.
    fn overdue(&self, b: usize, now: Instant) -> bool {
        let br = &self.brokers[b];
        let wait = Duration::from_millis(self.s.max_wait_ms as u64) + OVERDUE;
        matches!(br.link, Link::Ready(_))
            && br.written == br.out.len()
            && br.inflight.as_ref().is_some_and(|f| now.duration_since(f.since) > wait)
    }

    /// Broker `b` is lost: its partitions' leaders looked up again, its Fetch asked again (from
    /// the offsets not yet given) once it connects again.
    fn drop_broker(&mut self, b: usize, now: Instant, why: String, notice: &mut dyn FnMut(Notice)) {
        let br = &self.brokers[b];
        notice(Notice::Retrying(format!("broker {} ({}): {why}: retried in {:?}", br.id, br.addr, br.backoff)));
        self.reset_broker(b, now);
    }

    /// `drop_broker`'s work, untold.
    fn reset_broker(&mut self, b: usize, now: Instant) {
        let br = &mut self.brokers[b];
        #[cfg(unix)]
        if let (Link::Ready(s), Some((poll, _))) = (&br.link, &self.ready) {
            let _ = poll.registry().deregister(&mut mio::unix::SourceFd(&s.raw_fd()));
        }
        br.link = Link::Idle(now + br.backoff);
        br.backoff = (br.backoff * 2).min(self.net.backoff_max);
        (br.inflight, br.written) = (None, 0);
        br.out.clear();
        br.buf.clear();
        for p in self.parts.iter_mut().filter(|p| p.broker == Some(b)) {
            p.broker = None;
            p.incoming = 0;
        }
        self.lookup_at = self.lookup_at.min(now);
    }

    /// The Fetch for broker `b`'s partitions that are resolved, neither paused nor full, unless
    /// the broker asked for a wait.
    fn send(&mut self, b: usize) {
        if Instant::now() < self.brokers[b].throttled {
            return;
        }
        let s = &self.s;
        let mut asked: Vec<(usize, i64, u32)> = (self.parts.iter().enumerate())
            .filter(|(_, p)| p.broker == Some(b) && !p.paused && p.queue.len() + p.incoming < s.queued_min)
            .filter_map(|(i, p)| Some((i, p.next?, p.epoch)))
            .collect();
        if asked.is_empty() {
            return;
        }
        let br = &mut self.brokers[b];
        // each partition first in turn, as Java's consumer and librdkafka rotate theirs
        let first = br.rotate % asked.len();
        asked.rotate_left(first);
        br.rotate = br.rotate.wrapping_add(1);
        br.corr = br.corr.wrapping_add(1);
        let wire_parts: Vec<(&str, (i32, i64))> =
            asked.iter().map(|&(i, o, _)| (self.parts[i].topic.as_str(), (self.parts[i].partition, o))).collect();
        let limits = FetchLimits {
            max_wait_ms: s.max_wait_ms,
            min_bytes: s.min_bytes,
            max_bytes: s.max_bytes,
            isolation: i8::from(s.committed),
            partition_max_bytes: s.partition_max_bytes,
        };
        wire::request(&mut br.out, wire::FETCH, br.corr, &self.net.client_id, |o| {
            wire::fetch_request(o, &limits, &wire_parts)
        });
        br.written = 0;
        br.inflight = Some(InFlight { corr: br.corr, asked, since: Instant::now() });
    }

    /// Reads what has arrived and sends the Fetches due, without blocking; `notice` hears what
    /// is mended. Whether a response arrived.
    ///
    /// An error is one a retry did not mend: a partition's (an offset out of range with
    /// `auto.offset.reset=error`, a topic not authorized), or a broker's records that cannot be
    /// read (a corrupt batch, fetched again `CORRUPT_RETRIES` times first). The consumer stays
    /// usable after one, and no record is lost or skipped for it: the other partitions' records
    /// are queued, and the partition in error is fetched again from where it was (the same
    /// error again, until it mends or is `seek`ed past).
    pub fn pump(&mut self, notice: &mut dyn FnMut(Notice)) -> Result<bool> {
        let now = Instant::now();
        self.lookups(now, notice);
        let mut heard = false;
        for b in 0..self.brokers.len() {
            if !self.parts.iter().any(|p| p.broker == Some(b)) {
                continue;
            }
            match &mut self.brokers[b].link {
                Link::Idle(at) if now < *at => continue,
                Link::Idle(_) => {
                    let rx = conn::open_in_background(self.net.clone(), self.brokers[b].addr.clone());
                    self.brokers[b].link = Link::Connecting(rx);
                    continue;
                }
                Link::Connecting(rx) => match rx.try_recv() {
                    Err(mpsc::TryRecvError::Empty) => continue,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        self.drop_broker(b, now, "connecting: the thread ended".into(), notice);
                        continue;
                    }
                    Ok(Err(e)) => {
                        self.drop_broker(b, now, format!("connecting: {e}"), notice);
                        continue;
                    }
                    Ok(Ok(c)) => match c.into_stream() {
                        Ok(s) => {
                            #[cfg(unix)]
                            if let Some((poll, _)) = &self.ready {
                                // a descriptor of a socket closed unregistered may still be
                                // registered under its number: registered again, then
                                let (fd, r, t) = (s.raw_fd(), poll.registry(), mio::Token(b));
                                let mut src = mio::unix::SourceFd(&fd);
                                if r.register(&mut src, t, mio::Interest::READABLE).is_err() {
                                    let _ = r.reregister(&mut src, t, mio::Interest::READABLE);
                                }
                            }
                            self.brokers[b].link = Link::Ready(s)
                        }
                        Err(e) => {
                            self.drop_broker(b, now, format!("connecting: {e}"), notice);
                            continue;
                        }
                    },
                },
                Link::Ready(_) => {}
            }
            if self.brokers[b].inflight.is_none() {
                self.send(b);
            }
            match self.flush(b).and_then(|()| self.receive(b, now)) {
                Ok(Some(resp)) => {
                    heard = true;
                    let r = self.answered(b, &resp, notice);
                    self.brokers[b].buf = resp; // its capacity, for the next
                    self.brokers[b].buf.clear();
                    let br = &mut self.brokers[b];
                    match r {
                        Ok(Ok(())) => (br.backoff, br.corrupt) = (self.net.backoff, 0),
                        Ok(Err(e)) => self.drop_broker(b, now, e.to_string(), notice),
                        // a partition's: the response itself was read
                        Err(e @ Error::Broker(..)) => {
                            (br.backoff, br.corrupt) = (self.net.backoff, 0);
                            return Err(e);
                        }
                        // a response this client cannot read (cut, a CRC that does not match):
                        // fetched again on a new connection, but not without end. Its Fetch in
                        // flight (sent from past what was refused) is dropped either way
                        Err(e) if br.corrupt < CORRUPT_RETRIES => {
                            br.corrupt += 1;
                            self.drop_broker(b, now, e.to_string(), notice)
                        }
                        Err(e) => {
                            br.corrupt = 0; // a new error's retries
                            self.reset_broker(b, now);
                            return Err(e);
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => self.drop_broker(b, now, e.to_string(), notice),
            }
        }
        Ok(heard)
    }

    fn flush(&mut self, b: usize) -> std::io::Result<()> {
        let br = &mut self.brokers[b];
        let Link::Ready(s) = &mut br.link else { return Ok(()) };
        conn::write_all(s, &br.out, &mut br.written).map(|_| ())
    }

    /// A whole response of broker `b`, once its Fetch is written whole.
    fn receive(&mut self, b: usize, now: Instant) -> std::io::Result<Option<Vec<u8>>> {
        let (max, timeout) =
            (self.net.max_response, self.net.request_timeout + Duration::from_millis(self.s.max_wait_ms as u64));
        let br = &mut self.brokers[b];
        let Link::Ready(s) = &mut br.link else { return Ok(None) };
        let Some(inflight) = br.inflight.as_ref() else { return Ok(None) };
        // read once written whole; timed out also while its write stalls
        if br.written == br.out.len() && now >= br.quiet_until {
            #[cfg(test)]
            {
                br.reads += 1;
            }
            let had = br.buf.len();
            if conn::read_frame(s, &mut br.buf, max)? {
                return Ok(Some(std::mem::take(&mut br.buf)));
            }
            if br.buf.len() == had {
                br.quiet_until = now + QUIET_READ; // nothing there: not read again at once
            }
        }
        if now.duration_since(inflight.since) > timeout {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("no answer to a Fetch in {timeout:?}"),
            ));
        }
        Ok(None)
    }

    /// Broker `b`'s Fetch answered (`frame`: size, correlation id, body): the whole response
    /// checked first (each partition one asked for, once; its batches' headers and CRCs), then
    /// the next Fetch sent (from the offsets after the whole batches, before the records are
    /// parsed), then the records queued. No partition's next offset passes a record not queued;
    /// a partition's error is returned once the others' records are. The inner result: what
    /// sending the next Fetch met (the connection lost, not the records; a partition's error
    /// with it comes again from the Fetch asked again).
    fn answered(&mut self, b: usize, frame: &[u8], notice: &mut dyn FnMut(Notice)) -> Result<std::io::Result<()>> {
        let inflight = self.brokers[b].inflight.take().expect("a Fetch in flight");
        let body = wire::response(&frame[4..], inflight.corr)?;
        let base = 4; // ranges are of `frame[4..]`
        let got = wire::fetch(body)?;
        if got.throttle_ms > 0 {
            self.brokers[b].throttled = Instant::now() + Duration::from_millis(got.throttle_ms as u64);
        }
        if got.error != Code::NONE {
            // the response's own error (a fetch session's, none is used): asked again
            return Ok(Err(std::io::Error::other(format!("a Fetch refused: {}", got.error))));
        }
        let mut corrupt = None;
        let mut seen = vec![false; inflight.asked.len()];
        let mut answers = Vec::with_capacity(got.parts.len());
        for fp in &got.parts {
            let k = (inflight.asked.iter())
                .position(|&(i, _, _)| self.parts[i].topic == fp.topic && self.parts[i].partition == fp.partition);
            // refused before anything is changed: what was taken of it would be fetched past
            let Some(k) = k.filter(|&k| !std::mem::replace(&mut seen[k], true)) else {
                return Err(Error::Protocol(format!(
                    "{}/{}: in a Fetch response, not asked for (or twice)",
                    fp.topic, fp.partition
                )));
            };
            let (i, asked, epoch) = inflight.asked[k];
            if self.parts[i].epoch != epoch {
                continue; // seeked since: its answer is not taken
            }
            let range = fp.records.start + base..fp.records.end + base;
            // a broker suggests another replica only to a consumer that gave its rack (none
            // is): read as a leader that moved
            let error = if fp.preferred_replica >= 0 && fp.error == Code::NONE {
                Code::NOT_LEADER_OR_FOLLOWER
            } else {
                fp.error
            };
            let mut scanned = None;
            if error == Code::NONE {
                match records::scan(&frame[range.clone()], self.s.check_crcs) {
                    Ok(s) => scanned = Some(s),
                    Err(e) => drop(corrupt.get_or_insert(in_part(&self.parts[i], e))),
                }
            }
            let end = if self.s.committed && fp.last_stable >= 0 { fp.last_stable } else { fp.high };
            answers.push((i, asked, error, end, fp.aborted.clone(), range, scanned));
        }
        let mut fatal = None;
        for (i, asked, err, _, _, _, scanned) in &answers {
            let p = &mut self.parts[*i];
            match *err {
                Code::NONE => {
                    // a partition refused (`corrupt`) stays where it was asked
                    if let Some((next, count)) = *scanned {
                        p.next = Some(p.next.unwrap_or(*asked).max(next.unwrap_or(*asked)));
                        p.incoming = count;
                    }
                }
                Code::OFFSET_OUT_OF_RANGE => {
                    let to = match self.s.reset {
                        Reset::Earliest => Offset::Beginning,
                        Reset::Latest => Offset::End,
                        Reset::Error => {
                            let why = format!("{}/{}: offset {asked} (auto.offset.reset=error)", p.topic, p.partition);
                            fatal.get_or_insert(Error::Broker(*err, why));
                            continue;
                        }
                    };
                    // told once where it reads on from is found
                    (p.want, p.next, p.reset_from) = (to, None, Some(*asked));
                    self.lookup_at = Instant::now();
                }
                c if c.stale_metadata() || c.retriable() => {
                    let why = format!("{}/{}: fetch error {c}: looking up its leader again", p.topic, p.partition);
                    notice(Notice::Retrying(why));
                    p.broker = None;
                    self.lookup_at = Instant::now();
                }
                c => drop(fatal.get_or_insert(Error::Broker(c, format!("fetching {}/{}", p.topic, p.partition)))),
            }
        }
        // none from past a batch refused: the broker is dropped, and asked again, for it
        if corrupt.is_none() && self.parts.iter().any(|p| p.broker == Some(b)) {
            self.send(b); // pipelined: the broker prepares it while these are parsed
        }
        let sent = self.flush(b);
        let mut plain = std::mem::take(&mut self.plain);
        for (i, asked, _, end, aborted, range, scanned) in answers {
            if scanned.is_none() {
                continue;
            }
            let p = &mut self.parts[i];
            p.incoming = 0;
            let before = p.queue.len();
            let o = ReadOpts {
                from: asked,
                committed: self.s.committed,
                aborted: &aborted,
                check_crcs: false, // checked by `scan`
                max_decompressed: self.net.max_response,
            };
            let read = records::read(&frame[range], &o, &mut plain, |r| {
                p.queue.push_back(Message {
                    offset: r.offset,
                    timestamp: r.timestamp,
                    key: r.key.map(<[u8]>::to_vec),
                    value: r.value.map(<[u8]>::to_vec),
                    headers: r.headers().map(|(k, v)| (k.to_string(), v.map(<[u8]>::to_vec))).collect(),
                })
            });
            if let Err(e) = read {
                // fetched again from where it was asked (the Fetch sent dropped with the broker):
                // none of this response's records given
                p.queue.truncate(before);
                p.next = Some(asked);
                corrupt.get_or_insert(in_part(p, e));
                continue;
            }
            let delivered = p.queue.len() > before;
            let caught = p.next.is_some_and(|n| n >= end);
            // the end once each time a partition reaches it (librdkafka's PARTITION_EOF)
            if caught && (delivered || !p.at_end) && self.s.partition_eof {
                p.eof = true;
            }
            p.at_end = caught;
        }
        self.plain = plain;
        match (corrupt, fatal, sent) {
            (Some(e), _, _) | (None, Some(e), Ok(())) => Err(e),
            (None, _, sent) => Ok(sent),
        }
    }

    /// Partition `i`'s next message, its end once, or nothing yet (nothing while it is paused).
    ///
    /// Panics if `i` is not an index of `partitions`.
    pub fn poll(&mut self, i: usize) -> Polled {
        let p = &mut self.parts[i];
        if p.paused {
            return Polled::Empty;
        }
        match p.queue.pop_front() {
            Some(m) => Polled::Msg(m),
            None if std::mem::take(&mut p.eof) => Polled::Eof,
            None => Polled::Empty,
        }
    }

    /// The next message of any partition not paused: a partition's run, up to `RUN` messages,
    /// then the next's, so no partition waits on another's whole backlog.
    pub fn next_any(&mut self) -> Option<(usize, Message)> {
        let n = self.parts.len();
        if n == 0 {
            return None;
        }
        if self.run >= RUN {
            (self.turn, self.run) = (self.turn + 1, 0);
        }
        for k in 0..n {
            let i = (self.turn + k) % n;
            if self.parts[i].paused {
                continue;
            }
            if let Some(m) = self.parts[i].queue.pop_front() {
                if i != self.turn % n {
                    self.run = 0;
                }
                (self.turn, self.run) = (i, self.run + 1);
                return Some((i, m));
            }
        }
        None
    }

    /// Waits until a socket has something to read, at most `WAIT` and not past `until`. What no
    /// socket says (a connection or a lookup under way on a thread, a Fetch not written whole) is
    /// looked at again within 1 ms, as it was before there was a wait.
    fn wait(&mut self, until: Option<Instant>) {
        let now = Instant::now();
        let mut slice = until.map_or(WAIT, |u| u.saturating_duration_since(now).min(WAIT));
        let pending = self.lookup.is_some()
            || self
                .brokers
                .iter()
                .any(|b| matches!(b.link, Link::Connecting(_)) || b.inflight.is_some() && b.written < b.out.len());
        if pending {
            slice = slice.min(Duration::from_millis(1));
        }
        #[cfg(unix)]
        if let Some((poll, events)) = &mut self.ready {
            // edge triggered: a broker's socket is read until it would block, or its one
            // response is whole (one Fetch in flight a broker), so no readiness is left unseen;
            // one missed would cost a `WAIT`
            if poll.poll(events, Some(slice)).is_ok() {
                for e in events.iter() {
                    if let Some(br) = self.brokers.get_mut(e.token().0) {
                        br.quiet_until = now; // readable: read at the next pump
                    }
                }
                return;
            }
        }
        std::thread::sleep(slice.min(Duration::from_millis(1)));
    }

    /// The next event of any partition not paused (a message, or an end), waiting up to
    /// `timeout` for one (`Duration::MAX`: until one comes).
    ///
    /// An error is `pump`'s: the consumer stays usable, and no record is lost or skipped for it.
    pub fn next_event(&mut self, timeout: Duration, notice: &mut dyn FnMut(Notice)) -> Result<Option<(usize, Polled)>> {
        let until = Instant::now().checked_add(timeout);
        let ready = |p: &Part| !p.paused && (!p.queue.is_empty() || p.eof);
        loop {
            if let Some((i, m)) = self.next_any() {
                return Ok(Some((i, Polled::Msg(m))));
            }
            if let Some(i) = self.parts.iter_mut().position(|p| !p.paused && std::mem::take(&mut p.eof)) {
                return Ok(Some((i, Polled::Eof)));
            }
            // pumped at least once, whatever the timeout: a caller polling without waiting reads too
            let heard = self.pump(notice)?;
            if !self.parts.iter().any(ready) && until.is_some_and(|u| Instant::now() >= u) {
                return Ok(None);
            }
            if !heard {
                self.wait(until);
            }
        }
    }
}

/// A partition's read error, naming it.
fn in_part(p: &Part, e: Error) -> Error {
    match e {
        Error::Protocol(m) => Error::Protocol(format!("{}/{}: {m}", p.topic, p.partition)),
        e => e,
    }
}

#[cfg(test)]
mod tests;
