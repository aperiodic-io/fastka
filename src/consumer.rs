//! The consumer: each partition leader's Fetch sent from the caller's thread on a non-blocking
//! socket, the next Fetch sent before a response is parsed (the broker prepares it meanwhile),
//! each partition's records queued for the caller to take (`poll`, `next_any`). No threads hand
//! messages over and no message is an object of its own until it is taken (brrrrr's ADR-0005).
//!
//! Nothing blocks the caller's thread: connecting (a resolver, a SYN dropped) and looking up
//! leaders and offsets run on threads of their own; a lookup that fails is tried again later,
//! with a doubling backoff, as librdkafka recovers by itself. A lost connection, a moved leader, an offset out of range
//! (`auto.offset.reset`), each is retried from the next offset not yet given, and said to the
//! caller's `lost` (to count and log).
use crate::client::Client;
use crate::config::{ConsumerSettings, Net, Reset};
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
    At(i64),
}

/// A message as the consumer gives it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Message {
    pub offset: i64,
    /// ms
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    /// None: a tombstone
    pub value: Option<Vec<u8>>,
    /// only when the consumer was made `with_headers`
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl Message {
    pub fn header(&self, k: &str) -> Option<&[u8]> {
        self.headers.iter().find(|(n, _)| n == k).and_then(|(_, v)| v.as_deref())
    }
}

/// What `poll` gives of a partition.
#[derive(Debug, PartialEq, Eq)]
pub enum Polled {
    Msg(Message),
    /// The partition's end: every record it held when it was fetched last has been given
    /// (with `enable.partition.eof`).
    Eof,
    Empty,
}

/// A partition's assignment: (topic, partition, where from, paused).
pub type Assign = (String, i32, Offset, bool);

/// A lookup's longest; one that fails is tried again later.
const LOOKUP: Duration = Duration::from_secs(2);
const BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// A Fetch unanswered this long past its own wait has its partitions' leaders looked up, and again
/// each time this long passes: a broker cut off from the network answers nothing and its
/// connection stays open until `socket.timeout.ms`, while its partitions get new leaders in
/// seconds. A partition led elsewhere now drops the connection and is fetched from its new leader.
const OVERDUE: Duration = Duration::from_secs(1);
/// Responses in a row that cannot be read (a corrupt batch) fetched again before it is an error.
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

enum Link {
    /// not connected: may connect at `.0`
    Idle(Instant),
    Connecting(mpsc::Receiver<Result<Conn>>),
    Ready(Stream),
}

struct InFlight {
    corr: i32,
    asked: Vec<(usize, i64)>,
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
}

/// The Fetch of the partitions assigned it, on the caller's thread.
pub struct Consumer {
    client: Arc<Client>,
    net: Arc<Net>,
    s: ConsumerSettings,
    with_headers: bool,
    parts: Vec<Part>,
    brokers: Vec<Broker>,
    plain: Vec<u8>,
    /// when the leaders, and the offsets to resolve, may be looked up again
    lookup_at: Instant,
    lookup_backoff: Duration,
    turn: usize,
    run: usize,
    /// responses in a row that could not be read
    corrupt: u32,
    /// a lookup under way
    lookup: Option<mpsc::Receiver<Found>>,
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
    /// A consumer of `parts` on `client`'s cluster; `with_headers`: messages carry their headers.
    pub fn new(client: Arc<Client>, s: ConsumerSettings, parts: Vec<Assign>, with_headers: bool) -> Consumer {
        let parts = parts
            .into_iter()
            .map(|(topic, partition, want, paused)| Part {
                topic,
                partition,
                next: match want {
                    Offset::At(o) if o >= 0 => Some(o),
                    _ => None,
                },
                want,
                queue: VecDeque::new(),
                paused,
                broker: None,
                incoming: 0,
                at_end: false,
                eof: false,
            })
            .collect();
        Consumer {
            net: client.net().clone(),
            client,
            s,
            with_headers,
            parts,
            brokers: vec![],
            plain: vec![],
            lookup_at: Instant::now(),
            lookup_backoff: BACKOFF,
            turn: 0,
            run: 0,
            corrupt: 0,
            lookup: None,
        }
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
    pub fn position(&self, i: usize) -> Option<i64> {
        let p = &self.parts[i];
        p.queue.front().map(|m| m.offset).or(p.next)
    }

    /// Partitions that `which` names fetched again, or no longer.
    pub fn resume(&mut self, which: impl Fn(&str, i32) -> bool) {
        self.parts.iter_mut().filter(|p| which(&p.topic, p.partition)).for_each(|p| p.paused = false);
    }
    pub fn pause(&mut self, which: impl Fn(&str, i32) -> bool) {
        self.parts.iter_mut().filter(|p| which(&p.topic, p.partition)).for_each(|p| p.paused = true);
    }

    /// The offsets not resolved and the leaders of the partitions without a broker, looked up on
    /// a thread of its own once the backoff is past; its answer applied once it has one.
    fn lookups(&mut self, now: Instant, lost: &mut dyn FnMut(String)) {
        if let Some(rx) = &self.lookup {
            let done = match rx.try_recv() {
                Err(mpsc::TryRecvError::Empty) => return,
                Err(mpsc::TryRecvError::Disconnected) => None,
                Ok(done) => Some(done),
            };
            self.lookup = None;
            let failed = match done {
                None => {
                    lost("looking up leaders and offsets: the thread ended".into());
                    true
                }
                Some(d) => self.found(d, now, lost),
            };
            let failed = failed || self.parts.iter().any(|p| p.broker.is_none() || p.next.is_none());
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
        let spawned = std::thread::Builder::new().name("brrrrr-kafka-lookup".into()).spawn(move || {
            let offsets = (!unresolved.is_empty()).then(|| client.list_offsets(&unresolved, committed, LOOKUP));
            let leaders = topics.into_iter().map(|t| (t.clone(), client.leaders(&t, LOOKUP))).collect();
            let _ = tx.send(Found { offsets, leaders });
        });
        match spawned {
            Ok(_) => self.lookup = Some(rx),
            Err(e) => {
                lost(format!("a thread to look up leaders and offsets: {e}"));
                self.lookup_at = now + self.lookup_backoff;
            }
        }
    }

    /// A lookup's answer applied: whether some of it failed.
    fn found(&mut self, d: Found, now: Instant, lost: &mut dyn FnMut(String)) -> bool {
        let mut failed = false;
        match d.offsets {
            None => {}
            Some(Ok(found)) => {
                for (t, p, r) in found {
                    let Some(part) = self.parts.iter_mut().find(|x| x.topic == t && x.partition == p) else { continue };
                    match r {
                        // only partitions without an offset are looked up, and only a lookup gives one
                        Ok(o) => part.next = Some(o.unwrap_or(0)),
                        Err(e) => {
                            failed = true;
                            lost(format!("{t}/{p}: looking up where to read from: {e}"));
                        }
                    }
                }
            }
            Some(Err(e)) => {
                failed = true;
                lost(format!("looking up where to read from: {e}"));
            }
        }
        for (t, r) in d.leaders {
            let (leaders, addrs) = match r {
                Ok(l) => l,
                Err(e) => {
                    failed = true;
                    lost(format!("{t}: looking up its partitions' leaders: {e}"));
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
                self.drop_broker(b, now, why, lost);
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
    fn drop_broker(&mut self, b: usize, now: Instant, why: String, lost: &mut dyn FnMut(String)) {
        let br = &mut self.brokers[b];
        lost(format!("broker {} ({}): {why}: retried in {:?}", br.id, br.addr, br.backoff));
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
        let asked: Vec<(usize, i64)> = (self.parts.iter().enumerate())
            .filter(|(_, p)| p.broker == Some(b) && !p.paused && p.queue.len() + p.incoming < s.queued_min)
            .filter_map(|(i, p)| Some((i, p.next?)))
            .collect();
        if asked.is_empty() {
            return;
        }
        let br = &mut self.brokers[b];
        br.corr = br.corr.wrapping_add(1);
        let wire_parts: Vec<(&str, (i32, i64))> =
            asked.iter().map(|&(i, o)| (self.parts[i].topic.as_str(), (self.parts[i].partition, o))).collect();
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

    /// Reads what has arrived and sends the Fetches due, without blocking; `lost(why)` hears what is retried. Whether a response arrived. An error
    /// is one a retry does not mend (a record the broker should never have sent, an offset out
    /// of range with `auto.offset.reset=error`).
    pub fn pump(&mut self, lost: &mut dyn FnMut(String)) -> Result<bool> {
        let now = Instant::now();
        self.lookups(now, lost);
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
                        self.drop_broker(b, now, "connecting: the thread ended".into(), lost);
                        continue;
                    }
                    Ok(Err(e)) => {
                        self.drop_broker(b, now, format!("connecting: {e}"), lost);
                        continue;
                    }
                    Ok(Ok(c)) => match c.into_stream() {
                        Ok(s) => self.brokers[b].link = Link::Ready(s),
                        Err(e) => {
                            self.drop_broker(b, now, format!("connecting: {e}"), lost);
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
                    self.brokers[b].backoff = self.net.backoff;
                    let r = self.answered(b, &resp, lost);
                    self.brokers[b].buf = resp; // its capacity, for the next
                    self.brokers[b].buf.clear();
                    match r {
                        Ok(Ok(())) => self.corrupt = 0,
                        Ok(Err(e)) => self.drop_broker(b, now, e.to_string(), lost),
                        // a response this client cannot read (cut, a CRC that does not match):
                        // fetched again on a new connection, but not without end
                        Err(e @ Error::Protocol(_)) if self.corrupt < CORRUPT_RETRIES => {
                            self.corrupt += 1;
                            self.drop_broker(b, now, e.to_string(), lost)
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(None) => {}
                Err(e) => self.drop_broker(b, now, e.to_string(), lost),
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
        let Some(inflight) = br.inflight.as_ref().filter(|_| br.written == br.out.len()) else { return Ok(None) };
        if now >= br.quiet_until {
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

    /// Broker `b`'s Fetch answered (`frame`: size, correlation id, body): the next Fetch sent
    /// (from the offsets after the whole batches, before the records are parsed), then the
    /// records queued. The inner result: what sending the next Fetch met (the connection lost,
    /// not the records).
    fn answered(&mut self, b: usize, frame: &[u8], lost: &mut dyn FnMut(String)) -> Result<std::io::Result<()>> {
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
        let mut answers = Vec::with_capacity(got.parts.len());
        for fp in &got.parts {
            let Some(&(i, asked)) = inflight
                .asked
                .iter()
                .find(|(i, _)| self.parts[*i].topic == fp.topic && self.parts[*i].partition == fp.partition)
            else {
                return Err(Error::Protocol(format!(
                    "{}/{}: in a Fetch response, not asked for",
                    fp.topic, fp.partition
                )));
            };
            let range = fp.records.start + base..fp.records.end + base;
            // a broker suggests another replica only to a consumer that gave its rack (none
            // is): read as a leader that moved
            let error = if fp.preferred_replica >= 0 && fp.error == Code::NONE {
                Code::NOT_LEADER_OR_FOLLOWER
            } else {
                fp.error
            };
            if error == Code::NONE {
                let (next, count) = records::scan(&frame[range.clone()]);
                let p = &mut self.parts[i];
                p.next = Some(p.next.unwrap_or(asked).max(next.unwrap_or(asked)));
                p.incoming = count;
            }
            let end = if self.s.committed && fp.last_stable >= 0 { fp.last_stable } else { fp.high };
            answers.push((i, asked, error, end, fp.aborted.clone(), range));
        }
        for (i, asked, err, _, _, _) in &answers {
            let p = &mut self.parts[*i];
            match *err {
                Code::NONE => {}
                Code::OFFSET_OUT_OF_RANGE => {
                    let to = match self.s.reset {
                        Reset::Earliest => Offset::Beginning,
                        Reset::Latest => Offset::End,
                        Reset::Error => {
                            return Err(Error::Broker(
                                *err,
                                format!("{}/{}: offset {asked} (auto.offset.reset=error)", p.topic, p.partition),
                            ))
                        }
                    };
                    lost(format!(
                        "{}/{}: offset {asked} is out of range: going on from the {} (auto.offset.reset)",
                        p.topic,
                        p.partition,
                        if to == Offset::Beginning { "oldest" } else { "newest" }
                    ));
                    (p.want, p.next) = (to, None);
                    self.lookup_at = Instant::now();
                }
                c if c.stale_metadata() || c.retriable() => {
                    lost(format!("{}/{}: fetch error {c}: looking up its leader again", p.topic, p.partition));
                    p.broker = None;
                    self.lookup_at = Instant::now();
                }
                c => return Err(Error::Broker(c, format!("fetching {}/{}", p.topic, p.partition))),
            }
        }
        if self.parts.iter().any(|p| p.broker == Some(b)) {
            self.send(b); // pipelined: the broker prepares it while these are parsed
        }
        let sent = self.flush(b);
        let mut plain = std::mem::take(&mut self.plain);
        let with_headers = self.with_headers;
        let mut corrupt = None;
        for (i, asked, err, end, aborted, range) in answers {
            if err != Code::NONE {
                continue;
            }
            let p = &mut self.parts[i];
            p.incoming = 0;
            let before = p.queue.len();
            let o =
                ReadOpts { from: asked, committed: self.s.committed, aborted: &aborted, check_crcs: self.s.check_crcs };
            let read = records::read(&frame[range], &o, &mut plain, |r| {
                p.queue.push_back(Message {
                    offset: r.offset,
                    timestamp: r.timestamp,
                    key: r.key.map(<[u8]>::to_vec),
                    value: r.value.map(<[u8]>::to_vec),
                    headers: if with_headers {
                        r.headers().map(|(k, v)| (k.to_string(), v.map(<[u8]>::to_vec))).collect()
                    } else {
                        vec![]
                    },
                })
            });
            if let Err(e) = read {
                // fetched again from where it was asked: none of this response's records given
                p.queue.truncate(before);
                p.next = Some(asked);
                corrupt.get_or_insert(match e {
                    Error::Protocol(m) => Error::Protocol(format!("{}/{}: {m}", p.topic, p.partition)),
                    e => e,
                });
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
        match corrupt {
            Some(e) => Err(e),
            None => Ok(sent),
        }
    }

    /// Partition `i`'s next message, its end once, or nothing yet.
    pub fn poll(&mut self, i: usize) -> Polled {
        let p = &mut self.parts[i];
        match p.queue.pop_front() {
            Some(m) => Polled::Msg(m),
            None if std::mem::take(&mut p.eof) => Polled::Eof,
            None => Polled::Empty,
        }
    }

    /// The next message of any partition: a partition's run, up to `RUN` messages, then the
    /// next's, so no partition waits on another's whole backlog.
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

    /// The next event of any partition (a message, or an end), waiting up to `timeout` for one.
    pub fn next_event(&mut self, timeout: Duration, lost: &mut dyn FnMut(String)) -> Result<Option<(usize, Polled)>> {
        let until = Instant::now() + timeout;
        loop {
            if let Some((i, m)) = self.next_any() {
                return Ok(Some((i, Polled::Msg(m))));
            }
            if let Some(i) = self.parts.iter_mut().position(|p| std::mem::take(&mut p.eof)) {
                return Ok(Some((i, Polled::Eof)));
            }
            // pumped at least once, whatever the timeout: a caller polling without waiting reads too
            let heard = self.pump(lost)?;
            let ready = self.parts.iter().any(|p| !p.queue.is_empty() || p.eof);
            if !ready && Instant::now() >= until {
                return Ok(None);
            }
            if !heard {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

#[cfg(test)]
mod tests;
