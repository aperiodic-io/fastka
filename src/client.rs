//! The cluster's control plane: metadata, offsets, topics' settings, topics created, a group's
//! offsets committed. Each call is blocking, retried within its timeout where a retry can mend
//! it (a connection lost, a leader moved, a coordinator loading), as librdkafka retries; its
//! requests serialized on one connection a broker. `Sync`: any thread may call.
//!
//! The entry point is [`Client::new`]. A [`Consumer`](crate::consumer::Consumer) may share one
//! through an `Arc`; a [`Producer`](crate::producer::Producer) holds its own
//! ([`Producer::client`](crate::producer::Producer::client)).
use crate::config::{Config, Net};
use crate::conn::{deadline_in, Conn};
use crate::error::{Code, Error, Result};
use crate::wire::{self, Cur, Metadata};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// A blocking client of a cluster's control plane, made with [`Client::new`]. It holds one
/// connection to each broker it has called, opened at need and dropped once lost.
pub struct Client {
    net: Arc<Net>,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").field("net", &self.net).finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Inner {
    /// by broker id; -1 the bootstrap broker that answered
    conns: HashMap<i32, Conn>,
    metadata: Option<Metadata>,
}

/// Where a call goes.
#[derive(Clone, Copy, Debug)]
enum To {
    Any,
    Broker(i32),
}

impl Client {
    /// A client of `cfg`'s cluster; it connects at its first call.
    ///
    /// # Errors
    ///
    /// [`Error::Config`]: what [`Config::net`] refuses (a setting unknown or invalid, TLS
    /// material that cannot be loaded). No broker is contacted.
    pub fn new(cfg: &Config) -> Result<Client> {
        Ok(Client { net: Arc::new(cfg.net()?), inner: Mutex::new(Inner::default()) })
    }

    /// How it reaches the cluster: the settings [`Config::net`] made of its config.
    pub fn net(&self) -> &Arc<Net> {
        &self.net
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A request answered by `to`, connecting as needed, within `deadline`; a lost connection is
    /// dropped (the next call connects again).
    fn call(
        &self,
        inner: &mut Inner,
        to: To,
        deadline: Instant,
        api: i16,
        body: &dyn Fn(&mut Vec<u8>),
    ) -> Result<Vec<u8>> {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(Error::Timeout(format!("{} within its timeout", api_name(api))));
        }
        let id = match to {
            To::Broker(id) => id,
            To::Any => *inner.conns.keys().next().unwrap_or(&-1),
        };
        if !inner.conns.contains_key(&id) {
            let conn = match (to, id) {
                (To::Broker(id), _) => {
                    let addr = (inner.metadata.as_ref().and_then(|m| m.broker(id)))
                        .map(|b| format!("{}:{}", b.host, b.port))
                        .ok_or_else(|| Error::Transport(format!("broker {id}: not in the metadata")))?;
                    Conn::open_within(&self.net, &addr, left)?
                }
                _ => self.bootstrap(left)?,
            };
            inner.conns.insert(id, conn);
        }
        let conn = inner.conns.get_mut(&id).expect("connected");
        conn.set_request_timeout(left.min(self.net.request_timeout));
        let r = conn.call(api, body);
        if matches!(r, Err(Error::Transport(_) | Error::Timeout(_) | Error::Protocol(_))) {
            inner.conns.remove(&id);
        }
        r
    }

    /// The first bootstrap broker that answers.
    fn bootstrap(&self, within: Duration) -> Result<Conn> {
        let deadline = deadline_in(within);
        let mut last = Error::Transport("no bootstrap broker".into());
        // a broker refusing the client's credentials or versions refuses every retry, but another
        // may not (a listener of another kind among them): that refusal is the error if none answers
        let mut refused = None;
        for addr in &self.net.bootstrap {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            match Conn::open_within(&self.net, addr, left) {
                Ok(c) => return Ok(c),
                Err(e @ (Error::Auth(_) | Error::Unsupported(_))) => drop(refused.get_or_insert(e)),
                Err(e) => last = e,
            }
        }
        Err(refused.unwrap_or(last))
    }

    /// `f` retried while its error is transient and there is time left, the metadata looked up
    /// again between tries, the waits growing from `reconnect.backoff.ms` to its max.
    fn retried<T>(&self, timeout: Duration, mut f: impl FnMut(&mut Inner, Instant) -> Result<T>) -> Result<T> {
        let deadline = deadline_in(timeout);
        let mut wait = self.net.backoff.max(Duration::from_millis(10));
        loop {
            let mut inner = self.lock();
            let r = f(&mut inner, deadline);
            match r {
                Err(e) if transient(&e) && Instant::now() + wait < deadline => {
                    inner.metadata = None;
                    drop(inner);
                    std::thread::sleep(wait);
                    wait = (wait * 2).min(self.net.backoff_max.max(Duration::from_millis(10)));
                }
                r => return r,
            }
        }
    }

    fn refresh(&self, inner: &mut Inner, deadline: Instant, topics: Option<&[&str]>) -> Result<Metadata> {
        let frame = self.call(inner, To::Any, deadline, wire::METADATA, &|o| wire::metadata_request(o, topics))?;
        let md = wire::metadata(Cur::new(&frame))?;
        inner.metadata = Some(md.clone()); // its brokers: where later calls go
        Ok(md)
    }

    /// A request of `api` answered by any broker (InitProducerId), retried as the others are.
    /// Low level: `body` writes the request's body; the response frame is returned undecoded.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    pub fn call_any(&self, api: i16, body: &dyn Fn(&mut Vec<u8>), timeout: Duration) -> Result<Vec<u8>> {
        self.retried(timeout, |inner, deadline| self.call(inner, To::Any, deadline, api, body))
    }

    /// The cluster's metadata: of `topics`, or of every topic (`None`). A topic the cluster has
    /// not is in it with its error (UNKNOWN_TOPIC_OR_PARTITION); asking never creates one.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// [`Error::Config`]: a topic name over 32767 bytes.
    pub fn metadata(&self, topics: Option<&[&str]>, timeout: Duration) -> Result<Metadata> {
        names("topic", topics.unwrap_or_default().iter().copied())?;
        self.retried(timeout, |inner, deadline| self.refresh(inner, deadline, topics))
    }

    /// The cluster's id; None if it has none (Kafka before 0.10.1), or if the metadata could
    /// not be had within `timeout` (the error is dropped).
    pub fn cluster_id(&self, timeout: Duration) -> Option<String> {
        self.metadata(Some(&[]), timeout).ok().and_then(|m| m.cluster_id)
    }

    /// `topic`'s partitions, in order; an error if the cluster has no such topic. A topic the
    /// cluster has not (UNKNOWN_TOPIC_OR_PARTITION) is asked about once more, not until the
    /// timeout: once, as one just created may not be in every broker's metadata yet.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// [`Error::Broker`] with [`Code::UNKNOWN_TOPIC_OR_PARTITION`] if there is no such topic;
    /// [`Error::Config`]: a topic name over 32767 bytes.
    pub fn partitions(&self, topic: &str, timeout: Duration) -> Result<Vec<i32>> {
        names("topic", [topic])?;
        let mut asked = 0;
        // Ok(Err(_)): an error not retried
        self.retried(timeout, |inner, deadline| {
            asked += 1;
            let md = self.refresh(inner, deadline, Some(&[topic]))?;
            let t = md.topic(topic).ok_or_else(|| Error::Protocol(format!("{topic}: not in its own metadata")))?;
            let e = Error::Broker(t.error, format!("topic {topic}"));
            match t.error {
                Code::NONE => {}
                Code::UNKNOWN_TOPIC_OR_PARTITION if asked > 1 => return Ok(Err(e)),
                _ => return Err(e),
            }
            let mut ps: Vec<i32> = t.partitions.iter().map(|p| p.id).collect();
            ps.sort_unstable();
            Ok(Ok(ps))
        })?
    }

    /// `topic`'s partitions' leaders (partition, broker id: those with one) and the brokers'
    /// addresses (id, host:port). A partition without a leader is left out.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// [`Error::Broker`] with the topic's error if its metadata has one (e.g. no such topic).
    pub fn leaders(&self, topic: &str, timeout: Duration) -> Result<Leaders> {
        names("topic", [topic])?;
        let md = self.metadata(Some(&[topic]), timeout)?;
        let t = md.topic(topic).ok_or_else(|| Error::Protocol(format!("{topic}: not in its own metadata")))?;
        if t.error != Code::NONE {
            return Err(Error::Broker(t.error, format!("topic {topic}")));
        }
        let leaders = t.partitions.iter().filter(|p| p.leader >= 0).map(|p| (p.id, p.leader)).collect();
        let brokers = md.brokers.iter().map(|b| (b.id, format!("{}:{}", b.host, b.port))).collect();
        Ok((leaders, brokers))
    }

    /// ListOffsets of `parts` (topic, partition, timestamp ms or `wire::LATEST`/`EARLIEST`), each
    /// to its leader: per partition, its offset (`None`: no record that recent) or its error.
    /// With `committed`, read_committed: `LATEST` is the last stable offset, not the high
    /// watermark. A partition without a leader yet, or not in the metadata yet, is asked again
    /// until `timeout`.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// [`Error::Config`]: a partition named twice, or a topic name over 32767 bytes. A
    /// partition's own error is in its [`Listed`], not here.
    pub fn list_offsets(
        &self,
        parts: &[(String, i32, i64)],
        committed: bool,
        timeout: Duration,
    ) -> Result<Vec<Listed>> {
        let mut out: HashMap<(String, i32), std::result::Result<Option<i64>, Error>> = HashMap::new();
        let mut seen = std::collections::HashSet::new();
        if let Some((t, p, _)) = parts.iter().find(|(t, p, _)| !seen.insert((t, p))) {
            // a broker refuses a partition twice in one request (INVALID_REQUEST)
            return Err(Error::Config(format!("{t}/{p}: asked for twice in one ListOffsets")));
        }
        names("topic", parts.iter().map(|p| p.0.as_str()))?;
        self.retried(timeout, |inner, deadline| {
            let todo: Vec<&(String, i32, i64)> =
                parts.iter().filter(|p| !out.contains_key(&(p.0.clone(), p.1))).collect();
            if todo.is_empty() {
                return Ok(());
            }
            let topics: Vec<&str> = {
                let mut t: Vec<&str> = todo.iter().map(|p| p.0.as_str()).collect();
                t.sort_unstable();
                t.dedup();
                t
            };
            let md = self.refresh(inner, deadline, Some(&topics))?;
            let mut by_leader: HashMap<i32, Vec<Asked<'_>>> = HashMap::new();
            let mut transient = None;
            for (t, p, ts) in todo {
                let part = md
                    .topic(t)
                    .filter(|m| m.error == Code::NONE)
                    .and_then(|m| m.partitions.iter().find(|x| x.id == *p));
                match (md.topic(t).map(|m| m.error), part) {
                    (_, Some(x)) if x.leader >= 0 => {
                        by_leader.entry(x.leader).or_default().push((t.as_str(), (*p, *ts)))
                    }
                    (Some(e), _) if e != Code::NONE && !e.retriable() => {
                        out.insert((t.clone(), *p), Err(Error::Broker(e, format!("{t}/{p}"))));
                    }
                    // no such topic or partition yet, or no leader yet: asked again until the timeout
                    (Some(e), _) if e != Code::NONE => transient = Some(Error::Broker(e, format!("{t}/{p}"))),
                    (_, None) => transient = Some(Error::Broker(Code::UNKNOWN_TOPIC_OR_PARTITION, format!("{t}/{p}"))),
                    (_, Some(_)) => transient = Some(Error::Broker(Code::LEADER_NOT_AVAILABLE, format!("{t}/{p}"))),
                }
            }
            let iso = i8::from(committed);
            for (leader, ps) in by_leader {
                // a transient error: `retried` asks again, the partitions answered kept in `out`
                let frame = self.call(inner, To::Broker(leader), deadline, wire::LIST_OFFSETS, &|o| {
                    wire::list_offsets_request(o, iso, &ps)
                })?;
                let got = wire::list_offsets(Cur::new(&frame))?;
                if let Some((t, (p, _))) = ps.iter().find(|(t, (p, _))| !got.iter().any(|g| g.0 == *t && g.1 == *p)) {
                    return Err(Error::Protocol(format!(
                        "broker {leader}: ListOffsets without {t}/{p}, which it was asked"
                    )));
                }
                for (t, p, code, _, offset) in got {
                    match code {
                        Code::NONE => drop(out.insert((t, p), Ok((offset >= 0).then_some(offset)))),
                        c if c.retriable() => transient = Some(Error::Broker(c, format!("{t}/{p}"))),
                        c => drop(out.insert((t.clone(), p), Err(Error::Broker(c, format!("{t}/{p}"))))),
                    }
                }
            }
            match transient {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })?;
        Ok(parts
            .iter()
            .map(|(t, p, _)| {
                let r = out.remove(&(t.clone(), *p));
                (t.clone(), *p, r.unwrap_or_else(|| Err(Error::Protocol(format!("{t}/{p}: not answered")))))
            })
            .collect())
    }

    /// A partition's (log start, high watermark; the last stable offset reading `committed`).
    /// `timeout` covers both lookups.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// The partition's own error, as [`list_offsets`](Client::list_offsets) gives it.
    pub fn watermarks(&self, topic: &str, partition: i32, committed: bool, timeout: Duration) -> Result<(i64, i64)> {
        let deadline = deadline_in(timeout);
        let at = |ts| -> Result<Option<i64>> {
            let left = deadline.saturating_duration_since(Instant::now());
            let asked = [(topic.to_string(), partition, ts)];
            let got = self.list_offsets(&asked, committed, left)?.pop();
            got.map_or_else(|| Err(Error::Protocol(format!("{topic}/{partition}: not answered"))), |l| l.2)
        };
        let low = at(wire::EARLIEST)?.unwrap_or(0);
        let high = at(wire::LATEST)?.unwrap_or(low);
        Ok((low, high))
    }

    /// CreateTopics of `topics` (name, partitions, replication factor; -1 the broker's default),
    /// sent to the controller: per topic, what it said. A request whose answer was lost (a
    /// timeout, a connection cut) is sent again, and the broker may have created the topics the
    /// first time: on such a retry TOPIC_ALREADY_EXISTS reads as created (another client creating
    /// the same topic in that moment cannot be told apart).
    ///
    /// The broker is told to wait up to `timeout` (at most `socket.timeout.ms`, less a margin).
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// [`Error::Config`]: a topic name over 32767 bytes. A topic's own error is in its entry.
    pub fn create_topics(&self, topics: &[(String, i32, i16)], timeout: Duration) -> Result<Vec<(String, Result<()>)>> {
        names("topic", topics.iter().map(|t| t.0.as_str()))?;
        let mut lost = false;
        self.retried(timeout, |inner, deadline| {
            let md = self.refresh(inner, deadline, Some(&[]))?;
            let ts: Vec<(&str, i32, i16)> = topics.iter().map(|(n, p, r)| (n.as_str(), *p, *r)).collect();
            // the broker's wait ends before this client's, so that its answer comes back in time
            let wait = deadline.saturating_duration_since(Instant::now()).min(self.net.request_timeout);
            let ms = i32::try_from((wait - (wait / 4).min(Duration::from_secs(1))).as_millis()).unwrap_or(i32::MAX);
            let retry = lost;
            let r = self.call(inner, To::Broker(md.controller), deadline, wire::CREATE_TOPICS, &|o| {
                wire::create_topics_request(o, &ts, ms)
            });
            lost |= matches!(&r, Err(e) if transient_err(e));
            let frame = r?;
            let got = wire::create_topics(Cur::new(&frame))?;
            if let Some((_, c, _)) = got.iter().find(|(_, c, _)| *c == Code(41)) {
                return Err(Error::Broker(*c, "CreateTopics".into())); // NOT_CONTROLLER: it moved
            }
            Ok(got
                .into_iter()
                .map(|(t, code, msg)| {
                    let r = match code {
                        Code::NONE => Ok(()),
                        Code::TOPIC_ALREADY_EXISTS if retry => Ok(()),
                        c => Err(Error::Broker(c, msg.unwrap_or_default())),
                    };
                    (t, r)
                })
                .collect())
        })
    }

    /// Every setting of each of `topics` (name, value; None for a sensitive one), or its error.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// [`Error::Config`]: a topic name over 32767 bytes. A topic's own error is in its entry.
    pub fn describe_topic_configs(&self, topics: &[&str], timeout: Duration) -> Result<Vec<Result<TopicConfig>>> {
        names("topic", topics.iter().copied())?;
        self.retried(timeout, |inner, deadline| {
            let frame = self.call(inner, To::Any, deadline, wire::DESCRIBE_CONFIGS, &|o| {
                wire::describe_configs_request(o, topics)
            })?;
            let got = wire::describe_configs(Cur::new(&frame))?;
            Ok(topics
                .iter()
                .map(|t| match got.iter().find(|r| r.2 == *t) {
                    None => Err(Error::Protocol(format!("{t}: not in the answer"))),
                    Some((Code::NONE, _, _, configs)) => Ok(configs.iter().cloned().collect()),
                    Some((c, msg, _, _)) => Err(Error::Broker(*c, format!("{t}: {}", msg.clone().unwrap_or_default()))),
                })
                .collect())
        })
    }

    /// Commits `offsets` (topic, partition, offset) for `group`, from outside its membership:
    /// what lag tools read. Retried while the coordinator moves or loads.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] (or the last transient error) if `timeout` passes first;
    /// [`Error::Transport`], [`Error::Auth`] or [`Error::Unsupported`] if no broker can be
    /// reached or spoken to; [`Error::Broker`] for a code a retry does not mend.
    /// [`Error::Broker`] for the first partition the coordinator refused (e.g.
    /// [`Code::GROUP_AUTHORIZATION_FAILED`]), or if no coordinator is found;
    /// [`Error::Config`]: a group or topic name over 32767 bytes.
    pub fn commit(&self, group: &str, offsets: &[(String, i32, i64)], timeout: Duration) -> Result<()> {
        names("group.id", [group])?;
        names("topic", offsets.iter().map(|o| o.0.as_str()))?;
        self.retried(timeout, |inner, deadline| {
            let frame = self.call(inner, To::Any, deadline, wire::FIND_COORDINATOR, &|o| {
                wire::find_coordinator_request(o, group)
            })?;
            let (code, msg, b) = wire::find_coordinator(Cur::new(&frame))?;
            if code != Code::NONE {
                return Err(Error::Broker(
                    code,
                    format!("finding group {group}'s coordinator: {}", msg.unwrap_or_default()),
                ));
            }
            let md = inner.metadata.get_or_insert_with(Metadata::default);
            if md.broker(b.id).is_none() {
                md.brokers.push(b.clone());
            }
            let os: Vec<(&str, (i32, i64))> = offsets.iter().map(|(t, p, o)| (t.as_str(), (*p, *o))).collect();
            let frame = self.call(inner, To::Broker(b.id), deadline, wire::OFFSET_COMMIT, &|o| {
                wire::offset_commit_request(o, group, &os)
            })?;
            for (t, p, code) in wire::offset_commit(Cur::new(&frame))? {
                if code != Code::NONE {
                    if matches!(code, Code::NOT_COORDINATOR | Code::COORDINATOR_NOT_AVAILABLE) {
                        inner.conns.remove(&b.id);
                    }
                    return Err(Error::Broker(code, format!("committing {t}/{p} for group {group}")));
                }
            }
            Ok(())
        })
    }
}

/// A partition's offset found: (topic, partition, its offset (None: no record that recent), or why
/// not).
pub type Listed = (String, i32, Result<Option<i64>>);

/// A topic's settings: name, value (None for a sensitive one).
pub type TopicConfig = HashMap<String, Option<String>>;

/// A partition asked of its leader: (topic, (partition, timestamp)).
type Asked<'a> = (&'a str, (i32, i64));

/// The partitions' leaders (partition, broker id) and the brokers (id, host:port) of a topic.
pub type Leaders = (Vec<(i32, i32)>, Vec<(i32, String)>);

/// Names the protocol can carry, checked before any is sent (`Put::str` cannot fail).
fn names<'a>(what: &str, names: impl IntoIterator<Item = &'a str>) -> Result<()> {
    names.into_iter().try_for_each(|n| wire::check_str(what, n))
}

fn transient_err(e: &Error) -> bool {
    matches!(e, Error::Transport(_) | Error::Timeout(_))
}

fn transient(e: &Error) -> bool {
    transient_err(e) || matches!(e, Error::Broker(c, _) if c.retriable() || *c == Code(41))
}

fn api_name(api: i16) -> &'static str {
    wire::VERSIONS.iter().find(|v| v.0 == api).map_or("a request", |v| v.3)
}

#[cfg(test)]
mod tests;
