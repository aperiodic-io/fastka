//! Settings, by librdkafka's names (`CONFIGURATION.md`), so a deployment's `key=value` lines
//! mean what they meant to librdkafka. A key this client does not know is refused: a setting
//! silently ignored is how a TLS or SASL setting goes unnoticed. A key of the other role (a
//! producer's on a consumer) is taken and ignored, as librdkafka takes it.
//!
//! The entry point is [`Config`]: set its keys, then [`Config::net`], [`Config::consumer`] and
//! [`Config::producer`] check them and give the typed settings.
use crate::error::{Error, Result};
use crate::sasl::Mechanism;
use std::sync::Arc;
use std::time::Duration;

/// `key=value` settings, set in order (a later one wins). Nothing is checked until
/// [`net`](Config::net), [`consumer`](Config::consumer) or [`producer`](Config::producer) is
/// called. `Debug` shows a password's or a PEM key's value as `***`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Config {
    pairs: Vec<(String, String)>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // never a password's value
        let shown: Vec<(&str, &str)> =
            self.pairs.iter().map(|(k, v)| (k.as_str(), if secret(k) { "***" } else { v.as_str() })).collect();
        f.debug_struct("Config").field("pairs", &shown).finish()
    }
}

fn secret(k: &str) -> bool {
    k.contains("password") || k.ends_with(".pem") && k.contains("key")
}

impl Config {
    /// No settings: every key at its default (`bootstrap.servers` must still be set).
    pub fn new() -> Config {
        Config::default()
    }
    /// Sets `k` to `v`, replacing an earlier value of `k`. Never fails: an unknown key or a bad
    /// value is refused later, by [`net`](Config::net), [`consumer`](Config::consumer) or
    /// [`producer`](Config::producer).
    pub fn set(&mut self, k: impl Into<String>, v: impl Into<String>) -> &mut Config {
        let (k, v) = (k.into(), v.into());
        self.pairs.retain(|(o, _)| *o != k);
        self.pairs.push((k, v));
        self
    }
    /// `k`'s value as set; None if it is not set (its alias is not looked at).
    pub fn get(&self, k: &str) -> Option<&str> {
        self.pairs.iter().rev().find(|(o, _)| o == k).map(|(_, v)| v.as_str())
    }
}

/// Every key this client honours (librdkafka's names; its aliases too), the consumer's and the
/// producer's alike.
const KEYS: &[&str] = &[
    "bootstrap.servers",
    "metadata.broker.list",
    "client.id",
    "security.protocol",
    "sasl.mechanisms",
    "sasl.mechanism",
    "sasl.username",
    "sasl.password",
    "ssl.ca.location",
    "ssl.ca.pem",
    "ssl.certificate.location",
    "ssl.certificate.pem",
    "ssl.key.location",
    "ssl.key.pem",
    "ssl.endpoint.identification.algorithm",
    "enable.ssl.certificate.verification",
    "socket.timeout.ms",
    "socket.connection.setup.timeout.ms",
    "reconnect.backoff.ms",
    "reconnect.backoff.max.ms",
    "receive.message.max.bytes",
    "message.max.bytes",
    "allow.auto.create.topics",
    "group.id",
    "enable.auto.commit",
    "enable.auto.offset.store",
    "auto.offset.reset",
    "enable.partition.eof",
    "fetch.wait.max.ms",
    "fetch.min.bytes",
    "fetch.max.bytes",
    "fetch.message.max.bytes",
    "max.partition.fetch.bytes",
    "queued.min.messages",
    "queued.max.messages.kbytes",
    "fetch.queue.backoff.ms",
    "fetch.error.backoff.ms",
    "isolation.level",
    "check.crcs",
    "linger.ms",
    "queue.buffering.max.ms",
    "batch.size",
    "batch.num.messages",
    "queue.buffering.max.messages",
    "queue.buffering.max.kbytes",
    "message.timeout.ms",
    "delivery.timeout.ms",
    "request.timeout.ms",
    "enable.idempotence",
    "acks",
    "request.required.acks",
    "retries",
    "message.send.max.retries",
    "retry.backoff.ms",
    "retry.backoff.max.ms",
    "compression.type",
    "compression.codec",
    "compression.level",
    "sticky.partitioning.linger.ms",
    "topic.metadata.refresh.interval.ms",
    "max.in.flight.requests.per.connection",
    "max.in.flight",
    "max.request.size",
];

fn int(k: &str, v: &str, lo: i64, hi: i64) -> Result<i64> {
    let n: i64 = v.trim().parse().map_err(|_| Error::Config(format!("{k}={v}: not an integer")))?;
    if !(lo..=hi).contains(&n) {
        return Err(Error::Config(format!("{k}={n}: out of range {lo}..{hi}")));
    }
    Ok(n)
}

fn boolean(k: &str, v: &str) -> Result<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(Error::Config(format!("{k}={v}: not a boolean"))),
    }
}

fn ms(k: &str, v: &str, lo: i64, hi: i64) -> Result<Duration> {
    Ok(Duration::from_millis(int(k, v, lo, hi)? as u64))
}

/// A bootstrap broker as librdkafka takes it, as `host:port`: a scheme (any case) ignored, the
/// port 9092 if none; IPv6 bracketed (`[::1]:9093`, `[::1]`) or bare (`::1`, its port 9092: a bare
/// address cannot say where its port starts).
fn broker(s: &str) -> Result<String> {
    let bad = |why: &str| Error::Config(format!("bootstrap.servers: {s}: {why}"));
    let rest = match s.split_once("://") {
        Some((scheme, rest))
            if ["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"].contains(&scheme.to_ascii_lowercase().as_str()) =>
        {
            rest
        }
        _ => s,
    };
    let port = |p: &str| p.parse::<u16>().map_err(|_| bad("not a port"));
    let (host, port) = if let Some(v6) = rest.strip_prefix('[') {
        let (host, after) = v6.split_once(']').ok_or_else(|| bad("an IPv6 address without its ]"))?;
        match after {
            "" => (format!("[{host}]"), 9092),
            p => (format!("[{host}]"), port(p.strip_prefix(':').ok_or_else(|| bad("not a port after ]"))?)?),
        }
    } else if rest.matches(':').count() > 1 {
        (format!("[{rest}]"), 9092)
    } else {
        match rest.split_once(':') {
            Some((host, p)) => (host.to_string(), port(p)?),
            None => (rest.to_string(), 9092),
        }
    };
    Ok(format!("{host}:{port}"))
}

/// How to reach the cluster: its brokers, TLS and SASL, the sockets' timeouts. Made by
/// [`Config::net`]; `Debug` shows neither TLS material nor the SASL password.
#[derive(Clone)]
pub struct Net {
    /// The bootstrap brokers, as `host:port` (`bootstrap.servers`, or `metadata.broker.list`;
    /// port 9092 if none given, IPv6 bracketed).
    pub bootstrap: Vec<String>,
    /// The client id sent with each request (`client.id`, default `fastka`).
    pub client_id: String,
    /// TLS, if `security.protocol` is `ssl` or `sasl_ssl` (from the `ssl.*` keys); None: plaintext.
    pub tls: Option<Arc<rustls::ClientConfig>>,
    /// SASL (mechanism, username, password), if `security.protocol` is `sasl_plaintext` or
    /// `sasl_ssl` (`sasl.mechanisms`, `sasl.username`, `sasl.password`); None: no SASL.
    pub sasl: Option<(Mechanism, String, String)>,
    /// A request without its answer in this long is a connection lost (`socket.timeout.ms`,
    /// default 60000 ms).
    pub request_timeout: Duration,
    /// The longest a connection may take to set up, TCP, TLS and SASL
    /// (`socket.connection.setup.timeout.ms`, default 30000 ms).
    pub connect_timeout: Duration,
    /// The first wait before a broker is connected again, doubling up to `backoff_max`
    /// (`reconnect.backoff.ms`, default 100 ms).
    pub backoff: Duration,
    /// The longest wait between connection attempts (`reconnect.backoff.max.ms`, default
    /// 10000 ms).
    pub backoff_max: Duration,
    /// The largest response taken, bytes (`receive.message.max.bytes`, default 100000000).
    pub max_response: usize,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net")
            .field("bootstrap", &self.bootstrap)
            .field("tls", &self.tls.is_some())
            .field("sasl", &self.sasl.as_ref().map(|s| (s.0, &s.1)))
            .finish_non_exhaustive()
    }
}

/// `auto.offset.reset`: where a partition whose offset is out of range goes on from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reset {
    /// From the partition's first offset (`earliest`, `smallest`, `beginning`).
    Earliest,
    /// From the partition's end (`latest`, `largest`, `end`); the default.
    Latest,
    /// Not reset: [`Consumer::pump`](crate::consumer::Consumer::pump) returns an
    /// [`Error::Broker`] with [`OFFSET_OUT_OF_RANGE`](crate::Code::OFFSET_OUT_OF_RANGE)
    /// (`error`).
    Error,
}

/// A consumer's settings, made by `Config::consumer`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ConsumerSettings {
    /// `group.id`, if set. The consumer does not join a group; this is for
    /// [`Client::commit`](crate::client::Client::commit).
    pub group: Option<String>,
    /// Where a partition whose offset is out of range goes on from (`auto.offset.reset`,
    /// default `latest`).
    pub reset: Reset,
    /// Whether a partition's end is given as [`Polled::Eof`](crate::consumer::Polled::Eof)
    /// (`enable.partition.eof`, default false).
    pub partition_eof: bool,
    /// The longest a broker waits for `min_bytes` before it answers a Fetch, ms
    /// (`fetch.wait.max.ms`, default 500).
    pub max_wait_ms: i32,
    /// The least a broker gathers before it answers a Fetch, bytes (`fetch.min.bytes`,
    /// default 1).
    pub min_bytes: i32,
    /// The most a Fetch's response holds, bytes: `fetch.max.bytes` (default 52428800), at most
    /// `queued.max.messages.kbytes` × 1024 (as librdkafka caps it).
    pub max_bytes: i32,
    /// The most a Fetch's response holds of one partition, bytes (`max.partition.fetch.bytes`
    /// or `fetch.message.max.bytes`, default 1048576).
    pub partition_max_bytes: i32,
    /// A partition is fetched again while it holds fewer queued (`queued.min.messages`,
    /// default 100000).
    pub queued_min: usize,
    /// ... and while its queued messages' keys, values and headers are fewer bytes
    /// (`queued.max.messages.kbytes` × 1024, default 64 MiB): a bound per partition, as
    /// librdkafka's on a partition's own queue, overshot by at most one Fetch's worth.
    pub queued_max_bytes: usize,
    /// A partition whose Fetch met an error is neither looked up nor fetched again before this
    /// (`fetch.error.backoff.ms`, default 500), longer while its errors go on.
    pub fetch_error_backoff: Duration,
    /// read_committed: aborted transactions' records and control records are skipped, and a
    /// partition is read only up to its last stable offset (`isolation.level`, default
    /// `read_committed`).
    pub committed: bool,
    /// Whether each batch's CRC-32C is checked (`check.crcs`, default true here).
    pub check_crcs: bool,
}

/// A producer's settings, made by `Config::producer`. The producer is always idempotent with
/// `acks=all`.
#[derive(Clone, Debug)]
pub struct ProducerSettings {
    /// How long a partition's first queued message waits for others before its batch is sent
    /// (`linger.ms` or `queue.buffering.max.ms`, default 5 ms; less than `message_timeout`).
    pub linger: Duration,
    /// A partition's batch is sent once it holds this many bytes, or `batch_messages` messages
    /// (`batch.size`, default 1000000).
    pub batch_bytes: usize,
    /// The most messages a batch holds (`batch.num.messages`, default 10000).
    pub batch_messages: usize,
    /// The most messages queued or in flight, beyond which a send is refused with
    /// [`Error::QueueFull`] (`queue.buffering.max.messages`, default 100000; at least 1).
    pub queue_messages: usize,
    /// The most bytes queued or in flight, beyond which a send is refused with
    /// [`Error::QueueFull`]: `queue.buffering.max.kbytes` (default 1048576) × 1024.
    pub queue_bytes: usize,
    /// A message not delivered within this long of its send fails with
    /// [`Error::MessageTimedOut`] (`message.timeout.ms` or `delivery.timeout.ms`, default
    /// 300000 ms).
    pub message_timeout: Duration,
    /// The broker's wait for the replicas' acks (Produce's timeout; `request.timeout.ms`,
    /// default 30000 ms).
    pub request_timeout: Duration,
    /// The wait before a failed request is retried (`retry.backoff.ms`, default 100 ms).
    pub retry_backoff: Duration,
    /// `retry.backoff.max.ms` (default 1000 ms): the longest a retry waits, `retry.backoff.ms`
    /// doubled at each attempt up to it (a batch's retries, the metadata lookups).
    pub retry_backoff_max: Duration,
    /// The compression codec, one of [`records`](crate::records)' `NONE`, `GZIP`, `SNAPPY`,
    /// `LZ4`, `ZSTD` (`compression.type` or `compression.codec`, default `none`).
    pub codec: i16,
    /// The largest batch a broker takes, bytes (`message.max.bytes`, default 1000000).
    pub max_batch: usize,
    /// The most bytes of batches a Produce request holds (`max.request.size`, the Java client's
    /// name; default `message.max.bytes`, as before the setting). A request holds at least one
    /// batch, however large: the due batches longest waiting go first, the rest in the next
    /// request.
    pub max_request: usize,
    /// Produce requests outstanding to a broker at once, each on a connection (and a thread) of
    /// its own (`max.in.flight.requests.per.connection` or `max.in.flight`, 1 to 5 as
    /// librdkafka's idempotent producer, default 1). A partition has one batch in flight whatever
    /// this is: its messages stay in the order sent, its sequences without a gap.
    pub max_in_flight: usize,
    /// The metadata looked up again this often (`topic.metadata.refresh.interval.ms`, default
    /// 300000 ms, at least 10 ms); None for -1: never.
    pub metadata_refresh: Option<Duration>,
}

impl Config {
    /// Every key known.
    fn check(&self) -> Result<()> {
        for (k, _) in &self.pairs {
            match KEYS.iter().find(|n| *n == k) {
                Some(_) => {}
                None if k.starts_with("ssl.") || k.starts_with("sasl.") => {
                    return Err(Error::Config(format!(
                        "{k}: not a setting this client honours (TLS: ssl.ca.location/.pem, ssl.certificate.*, \
                         ssl.key.*, ssl.endpoint.identification.algorithm; SASL: sasl.mechanisms, sasl.username, \
                         sasl.password)"
                    )))
                }
                None => return Err(Error::Config(format!("{k}: not a setting this client knows"))),
            }
        }
        Ok(())
    }

    /// `k`'s value, or its alias's; both set to different values is a mistake, refused.
    fn aliased(&self, k: &str, alias: &str) -> Result<Option<&str>> {
        match (self.get(k), self.get(alias)) {
            (Some(a), Some(b)) if a.trim() != b.trim() => {
                Err(Error::Config(format!("{k}={a} and {alias}={b}: one setting by two names, set one")))
            }
            (a, b) => Ok(a.or(b)),
        }
    }

    /// How to reach the cluster: `bootstrap.servers`, `client.id`, `security.protocol` with the
    /// `ssl.*` and `sasl.*` keys, the sockets' timeouts and backoffs. Every key is checked, the
    /// other role's too. TLS certificates and keys are read here (files at their `.location`).
    ///
    /// # Errors
    ///
    /// [`Error::Config`]: a key this client does not know, a value out of range or not of its
    /// type, or a key and its alias set to different values. Also: `bootstrap.servers` not set or naming no broker,
    /// `sasl.username`/`sasl.password` missing with SASL, an `ssl.*` key without TLS, or TLS
    /// material that cannot be read or parsed.
    pub fn net(&self) -> Result<Net> {
        self.check()?;
        let servers = self
            .aliased("bootstrap.servers", "metadata.broker.list")?
            .ok_or_else(|| Error::Config("bootstrap.servers: not set".into()))?;
        let bootstrap: Vec<String> =
            servers.split(',').map(str::trim).filter(|s| !s.is_empty()).map(broker).collect::<Result<_>>()?;
        if bootstrap.is_empty() {
            return Err(Error::Config("bootstrap.servers: no broker".into()));
        }
        let protocol = self.get("security.protocol").unwrap_or("plaintext").to_ascii_lowercase();
        let (tls, sasl) = match protocol.as_str() {
            "plaintext" => (false, false),
            "ssl" => (true, false),
            "sasl_plaintext" => (false, true),
            "sasl_ssl" => (true, true),
            p => {
                return Err(Error::Config(format!(
                    "security.protocol={p}: one of plaintext, ssl, sasl_plaintext, sasl_ssl"
                )))
            }
        };
        let sasl = match sasl {
            false => None,
            true => {
                let mech = Mechanism::parse(self.aliased("sasl.mechanisms", "sasl.mechanism")?.unwrap_or("GSSAPI"))?;
                let user = self.get("sasl.username").ok_or_else(|| Error::Config("sasl.username: not set".into()))?;
                let pass = self.get("sasl.password").ok_or_else(|| Error::Config("sasl.password: not set".into()))?;
                Some((mech, user.to_string(), pass.to_string()))
            }
        };
        let tls = match tls {
            false => {
                if let Some((k, _)) = self.pairs.iter().find(|(k, _)| k.starts_with("ssl.")) {
                    return Err(Error::Config(format!("{k}: set, but security.protocol={protocol} has no TLS")));
                }
                None
            }
            true => Some(crate::tls::config(self)?),
        };
        let g = |k, d| self.get(k).unwrap_or(d);
        let client_id = g("client.id", "fastka");
        crate::wire::check_str("client.id", client_id)?;
        Ok(Net {
            bootstrap,
            client_id: client_id.to_string(),
            tls,
            sasl,
            request_timeout: ms("socket.timeout.ms", g("socket.timeout.ms", "60000"), 10, 300_000)?,
            connect_timeout: ms(
                "socket.connection.setup.timeout.ms",
                g("socket.connection.setup.timeout.ms", "30000"),
                100,
                2_147_483_647,
            )?,
            backoff: ms("reconnect.backoff.ms", g("reconnect.backoff.ms", "100"), 0, 3_600_000)?,
            backoff_max: ms("reconnect.backoff.max.ms", g("reconnect.backoff.max.ms", "10000"), 0, 3_600_000)?,
            max_response: int(
                "receive.message.max.bytes",
                g("receive.message.max.bytes", "100000000"),
                1000,
                2_147_483_647,
            )? as usize,
        })
    }

    /// A consumer's settings (`group.id`, `auto.offset.reset`, the `fetch.*` keys,
    /// `isolation.level`, ...). Every key is checked, the other role's too.
    ///
    /// # Errors
    ///
    /// [`Error::Config`]: a key this client does not know, a value out of range or not of its
    /// type, or a key and its alias set to different values. Also: `enable.auto.commit=true` (this client commits
    /// only when asked, with [`Client::commit`](crate::client::Client::commit)).
    pub fn consumer(&self) -> Result<ConsumerSettings> {
        self.check()?;
        let g = |k, d| self.get(k).unwrap_or(d);
        if let Some(v) = self.get("enable.auto.commit") {
            if boolean("enable.auto.commit", v)? {
                return Err(Error::Config("enable.auto.commit=true: this client commits only when asked".into()));
            }
        }
        let reset = match g("auto.offset.reset", "latest").to_ascii_lowercase().as_str() {
            "earliest" | "smallest" | "beginning" => Reset::Earliest,
            "latest" | "largest" | "end" => Reset::Latest,
            "error" => Reset::Error,
            v => return Err(Error::Config(format!("auto.offset.reset={v}: earliest, latest or error"))),
        };
        let fetch_max = int("fetch.max.bytes", g("fetch.max.bytes", "52428800"), 0, 2_147_483_135)?;
        let queue_kb = int("queued.max.messages.kbytes", g("queued.max.messages.kbytes", "65536"), 1, 2_097_151)?;
        let partition_max =
            self.get("max.partition.fetch.bytes").or(self.get("fetch.message.max.bytes")).unwrap_or("1048576");
        int("fetch.queue.backoff.ms", g("fetch.queue.backoff.ms", "1000"), 0, 300_000)?;
        let fetch_error_backoff = ms("fetch.error.backoff.ms", g("fetch.error.backoff.ms", "500"), 0, 300_000)?;
        let committed = match g("isolation.level", "read_committed") {
            "read_committed" => true,
            "read_uncommitted" => false,
            v => return Err(Error::Config(format!("isolation.level={v}: read_committed or read_uncommitted"))),
        };
        Ok(ConsumerSettings {
            group: self.get("group.id").map(str::to_string),
            reset,
            partition_eof: boolean("enable.partition.eof", g("enable.partition.eof", "false"))?,
            max_wait_ms: int("fetch.wait.max.ms", g("fetch.wait.max.ms", "500"), 0, 300_000)? as i32,
            min_bytes: int("fetch.min.bytes", g("fetch.min.bytes", "1"), 1, 100_000_000)? as i32,
            max_bytes: fetch_max.min(queue_kb * 1024).max(1) as i32,
            partition_max_bytes: int("fetch.message.max.bytes", partition_max, 1, 1_000_000_000)? as i32,
            queued_min: int("queued.min.messages", g("queued.min.messages", "100000"), 1, 10_000_000)? as usize,
            queued_max_bytes: queue_kb as usize * 1024,
            fetch_error_backoff,
            committed,
            check_crcs: boolean("check.crcs", g("check.crcs", "true"))?,
        })
    }

    /// A producer's settings (`linger.ms`, `batch.*`, `queue.buffering.*`, `message.timeout.ms`,
    /// `compression.type`, ...). Every key is checked, the other role's too.
    ///
    /// # Errors
    ///
    /// [`Error::Config`]: a key this client does not know, a value out of range or not of its
    /// type, or a key and its alias set to different values. Also: `enable.idempotence=false`, `acks` other than
    /// `all`/`-1`, or `linger.ms` not less than `message.timeout.ms`.
    pub fn producer(&self) -> Result<ProducerSettings> {
        self.check()?;
        let g = |k, d| self.get(k).unwrap_or(d);
        // every producer of this client is idempotent: its ordering and once-only writes rest on it
        if let Some(v) = self.get("enable.idempotence") {
            if !boolean("enable.idempotence", v)? {
                return Err(Error::Config("enable.idempotence=false: this producer is always idempotent".into()));
            }
        }
        if let Some(v) = self.get("acks").or(self.get("request.required.acks")) {
            if !matches!(v.trim(), "all" | "-1") {
                return Err(Error::Config(format!(
                    "acks={v}: an idempotent producer waits for all replicas (acks=all)"
                )));
            }
        }
        for k in ["retries", "message.send.max.retries"] {
            if let Some(v) = self.get(k) {
                int(k, v, 0, i32::MAX as i64)?; // retries are bounded by message.timeout.ms
            }
        }
        let codec = match self.get("compression.type").or(self.get("compression.codec")).unwrap_or("none") {
            "none" => crate::records::NONE,
            "gzip" => crate::records::GZIP,
            "snappy" => crate::records::SNAPPY,
            "lz4" => crate::records::LZ4,
            "zstd" => crate::records::ZSTD,
            v => return Err(Error::Config(format!("compression.type={v}: none, gzip, snappy, lz4 or zstd"))),
        };
        if let Some(v) = self.get("compression.level") {
            int("compression.level", v, -1, 12)?;
        }
        int("sticky.partitioning.linger.ms", g("sticky.partitioning.linger.ms", "10"), 0, 900_000)?;
        let timeout = self.get("message.timeout.ms").or(self.get("delivery.timeout.ms")).unwrap_or("300000");
        let linger = self.get("linger.ms").or(self.get("queue.buffering.max.ms")).unwrap_or("5");
        let (linger, message_timeout) =
            (ms("linger.ms", linger, 0, 900_000)?, ms("message.timeout.ms", timeout, 1, 2_147_483_647)?);
        if linger >= message_timeout {
            // librdkafka's own rule: every message would time out lingering
            return Err(Error::Config(format!(
                "linger.ms={} must be less than message.timeout.ms={}",
                linger.as_millis(),
                message_timeout.as_millis()
            )));
        }
        let (retry_backoff, retry_backoff_max) = (
            ms("retry.backoff.ms", g("retry.backoff.ms", "100"), 1, 300_000)?,
            ms("retry.backoff.max.ms", g("retry.backoff.max.ms", "1000"), 1, 300_000)?,
        );
        if retry_backoff_max < retry_backoff {
            // librdkafka's rule: the backoff doubles from retry.backoff.ms up to the max
            return Err(Error::Config(format!(
                "retry.backoff.max.ms={} must not be less than retry.backoff.ms={}",
                retry_backoff_max.as_millis(),
                retry_backoff.as_millis()
            )));
        }
        let max_batch = int("message.max.bytes", g("message.max.bytes", "1000000"), 1000, 1_000_000_000)? as usize;
        Ok(ProducerSettings {
            linger,
            batch_bytes: int("batch.size", g("batch.size", "1000000"), 1, 2_147_483_647)? as usize,
            batch_messages: int("batch.num.messages", g("batch.num.messages", "10000"), 1, 1_000_000)? as usize,
            queue_messages: int(
                "queue.buffering.max.messages",
                g("queue.buffering.max.messages", "100000"),
                0,
                2_147_483_647,
            )?
            .max(1) as usize,
            queue_bytes: int(
                "queue.buffering.max.kbytes",
                g("queue.buffering.max.kbytes", "1048576"),
                1,
                2_147_483_647,
            )? as usize
                * 1024,
            message_timeout,
            request_timeout: ms("request.timeout.ms", g("request.timeout.ms", "30000"), 1, 900_000)?,
            retry_backoff,
            retry_backoff_max,
            codec,
            max_batch,
            max_request: match self.get("max.request.size") {
                Some(v) => int("max.request.size", v, 1, 2_147_483_647)? as usize,
                None => max_batch,
            },
            max_in_flight: int(
                "max.in.flight.requests.per.connection",
                self.aliased("max.in.flight.requests.per.connection", "max.in.flight")?.unwrap_or("1"),
                1,
                5,
            )? as usize,
            metadata_refresh: match int(
                "topic.metadata.refresh.interval.ms",
                g("topic.metadata.refresh.interval.ms", "300000"),
                -1,
                3_600_000,
            )? {
                -1 => None,
                ms => Some(Duration::from_millis(ms.max(10) as u64)), // at least 10 ms apart
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(pairs: &[(&str, &str)]) -> Config {
        let mut c = Config::new();
        c.set("bootstrap.servers", "k1:9092,k2");
        for (k, v) in pairs {
            c.set(*k, *v);
        }
        c
    }

    /// librdkafka's defaults where this client keeps them; its names mean what they meant there.
    #[test]
    fn defaults_and_names_are_librdkafkas() {
        let c = cfg(&[]);
        let n = c.net().unwrap();
        assert_eq!(n.bootstrap, ["k1:9092", "k2:9092"], "a broker without a port is on 9092");
        assert!(n.tls.is_none() && n.sasl.is_none());
        let s = c.consumer().unwrap();
        assert_eq!((s.reset, s.partition_eof, s.max_wait_ms, s.queued_min), (Reset::Latest, false, 500, 100_000));
        assert_eq!(s.max_bytes, 52_428_800, "fetch.max.bytes under 64 MB of queue");
        assert!(s.committed, "read_committed, librdkafka's default");
        let s = cfg(&[("fetch.max.bytes", "52428800"), ("queued.max.messages.kbytes", "4096")]).consumer().unwrap();
        assert_eq!(s.max_bytes, 4 << 20, "fetch.max.bytes capped by the queue, as librdkafka caps it");
        let p = c.producer().unwrap();
        assert_eq!(
            (p.linger, p.message_timeout, p.queue_messages),
            (Duration::from_millis(5), Duration::from_secs(300), 100_000)
        );
        assert_eq!(p.metadata_refresh, Some(Duration::from_secs(300)));
        assert_eq!((p.max_in_flight, p.max_request), (1, 1_000_000), "a request bounded by message.max.bytes");
        let p = cfg(&[("message.max.bytes", "2000000")]).producer().unwrap();
        assert_eq!(p.max_request, 2_000_000, "max.request.size defaults to message.max.bytes");
        let p = cfg(&[("max.in.flight", "5"), ("max.request.size", "1048576")]).producer().unwrap();
        assert_eq!((p.max_in_flight, p.max_request), (5, 1_048_576));
        let p = cfg(&[("topic.metadata.refresh.interval.ms", "-1")]).producer().unwrap();
        assert_eq!(p.metadata_refresh, None, "-1: never");
        let p = cfg(&[("queue.buffering.max.ms", "20"), ("delivery.timeout.ms", "1500")]).producer().unwrap();
        assert_eq!(
            (p.linger, p.message_timeout),
            (Duration::from_millis(20), Duration::from_millis(1500)),
            "the aliases"
        );
    }

    /// What a misspelling or a setting this client does not honour would silently change is
    /// refused at start, naming the key, never its value.
    #[test]
    fn what_cannot_be_honoured_is_refused() {
        for (k, v, why) in [
            ("fetch.wait.max.msx", "1", "not a setting this client knows"),
            ("ssl.keystore.location", "/x", "not a setting this client honours"),
            ("sasl.kerberos.service.name", "kafka", "not a setting this client honours"),
            ("security.protocol", "ssl_plaintext", "one of plaintext"),
            ("enable.idempotence", "false", "always idempotent"),
            ("acks", "1", "acks=all"),
            ("enable.auto.commit", "true", "commits only when asked"),
            ("compression.type", "brotli", "none, gzip"),
            ("isolation.level", "dirty", "read_committed or"),
            ("linger.ms", "soon", "not an integer"),
            ("auto.offset.reset", "middle", "earliest, latest"),
            // librdkafka's idempotent producer's bound
            ("max.in.flight", "6", "out of range 1..5"),
            ("max.in.flight.requests.per.connection", "0", "out of range 1..5"),
            ("max.request.size", "0", "out of range"),
        ] {
            let c = cfg(&[(k, v)]);
            let e = c.net().and(c.consumer().map(|_| ())).and(c.producer().map(|_| ())).unwrap_err().to_string();
            assert!(e.contains(why), "{k}={v}: {e}");
        }
        let c = cfg(&[("security.protocol", "sasl_plaintext"), ("sasl.mechanisms", "PLAIN"), ("sasl.username", "u")]);
        assert!(c.net().unwrap_err().to_string().contains("sasl.password: not set"));
        let c = cfg(&[("security.protocol", "plaintext"), ("ssl.ca.location", "/ca.pem")]);
        assert!(c.net().unwrap_err().to_string().contains("has no TLS"), "a TLS setting without TLS is a mistake");
        let c = cfg(&[("security.protocol", "sasl_plaintext"), ("sasl.username", "u"), ("sasl.password", "p")]);
        assert!(c.net().unwrap_err().to_string().contains("GSSAPI"), "librdkafka's default mechanism is not spoken");
        // librdkafka's own rule: a message lingering past its timeout would always fail
        let c = cfg(&[("linger.ms", "15000"), ("message.timeout.ms", "13500")]);
        assert!(c.producer().unwrap_err().to_string().contains("linger.ms"), "linger.ms past message.timeout.ms");
    }

    /// A producer's setting on a consumer is taken (a deployment may give every client the same lines).
    #[test]
    fn the_other_roles_keys_are_taken() {
        let c = cfg(&[("linger.ms", "2"), ("fetch.wait.max.ms", "50")]);
        assert!(c.consumer().is_ok() && c.producer().is_ok());
    }

    #[test]
    fn a_password_is_never_shown() {
        let c = cfg(&[("sasl.password", "hunter2"), ("sasl.username", "svc")]);
        let d = format!("{c:?}");
        assert!(!d.contains("hunter2") && d.contains("svc"), "{d}");
        let c = cfg(&[("ssl.key.pem", "PRIVATE"), ("ssl.ca.pem", "PUBLIC")]);
        let d = format!("{c:?}");
        assert!(!d.contains("PRIVATE") && d.contains("PUBLIC"), "a key hidden, a CA shown: {d}");
        let n = cfg(&[]).net().unwrap();
        assert!(format!("{n:?}").starts_with(r#"Net { bootstrap: ["k1:9092", "k2:9092"], tls: false"#), "{n:?}");
    }

    /// librdkafka's rule: the longest retry backoff is not below the first.
    #[test]
    fn a_retry_backoff_max_below_the_backoff_is_refused() {
        let e = cfg(&[("retry.backoff.ms", "500"), ("retry.backoff.max.ms", "100")]).producer().unwrap_err();
        assert!(e.to_string().contains("retry.backoff.max.ms"), "{e}");
    }

    /// Sizes and levels as librdkafka reads them: kbytes are 1024 bytes; compression.level -1 is
    /// the codec's default, 12 the most.
    #[test]
    fn sizes_and_levels() {
        let p = cfg(&[("queue.buffering.max.kbytes", "2")]).producer().unwrap();
        assert_eq!(p.queue_bytes, 2048);
        for (level, ok) in [("-1", true), ("12", true), ("-2", false), ("13", false)] {
            assert_eq!(cfg(&[("compression.level", level)]).producer().is_ok(), ok, "compression.level={level}");
        }
    }

    /// TLS's settings: one CA (a file or PEM), "probe" the system's roots; the name checked
    /// (https), or not (none), anything else refused.
    #[test]
    fn tls_settings() {
        let tls = |pairs: &[(&str, &str)]| {
            let mut c = cfg(pairs);
            c.set("security.protocol", "ssl");
            c.net().map(|_| ())
        };
        let p = crate::testkit::pki(&["k1"]);
        let e = tls(&[("ssl.ca.location", "/x.pem"), ("ssl.ca.pem", &p.ca_pem)]).unwrap_err();
        assert!(e.to_string().contains("set one"), "{e}");
        tls(&[("ssl.ca.location", "probe")]).unwrap();
        for (v, ok) in [("https", true), ("HTTPS", true), ("none", true), ("", true), ("bogus", false)] {
            let r = tls(&[("ssl.ca.pem", &p.ca_pem), ("ssl.endpoint.identification.algorithm", v)]);
            assert_eq!(r.is_ok(), ok, "ssl.endpoint.identification.algorithm={v}: {r:?}");
        }
    }

    /// Brokers as librdkafka takes them: a scheme ignored (any case), a port or 9092, IPv6
    /// bracketed or bare; a port that is not one refused.
    #[test]
    fn bootstrap_addresses() {
        let n = |s: &str| {
            let mut c = Config::new();
            c.set("bootstrap.servers", s);
            c.net().map(|n| n.bootstrap)
        };
        assert_eq!(n("ssl://k1:1,SASL_SSL://k2, Sasl_Plaintext://k3:3").unwrap(), ["k1:1", "k2:9092", "k3:3"]);
        assert_eq!(
            n("[::1]:9093,[::1],::1,2001:db8::1").unwrap(),
            ["[::1]:9093", "[::1]:9092", "[::1]:9092", "[2001:db8::1]:9092"]
        );
        for bad in ["k1:port", "[::1", "[::1]9092", "k1:99999"] {
            assert!(matches!(n(bad), Err(Error::Config(_))), "{bad}: {:?}", n(bad));
        }
        assert_eq!(cfg(&[]).net().unwrap().client_id, "fastka");
    }

    /// A setting and its alias both set, to different values: which was meant is not known.
    #[test]
    fn an_alias_disagreeing_is_refused() {
        let c = cfg(&[("metadata.broker.list", "k3")]);
        assert!(matches!(c.net(), Err(Error::Config(m)) if m.contains("metadata.broker.list")));
        cfg(&[("metadata.broker.list", "k1:9092,k2")]).net().unwrap();
        let sasl = [("security.protocol", "sasl_plaintext"), ("sasl.username", "u"), ("sasl.password", "p")];
        let mut c = cfg(&sasl);
        c.set("sasl.mechanisms", "PLAIN").set("sasl.mechanism", "SCRAM-SHA-256");
        assert!(matches!(c.net(), Err(Error::Config(m)) if m.contains("sasl.mechanism")));
        c.set("sasl.mechanism", "PLAIN");
        c.net().unwrap();
        let c = cfg(&[("max.in.flight", "2"), ("max.in.flight.requests.per.connection", "3")]);
        assert!(matches!(c.producer(), Err(Error::Config(m)) if m.contains("max.in.flight")));
    }
}
