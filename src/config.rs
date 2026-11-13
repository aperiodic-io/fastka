//! Settings, by librdkafka's names (`CONFIGURATION.md`), so a deployment's `key=value` lines
//! mean what they meant to librdkafka. A key this client does not know is refused: a setting
//! silently ignored is how a TLS or SASL setting goes unnoticed. A key of the other role (a
//! producer's on a consumer) is taken and ignored, as librdkafka takes it.
use crate::error::{Error, Result};
use crate::sasl::Mechanism;
use std::sync::Arc;
use std::time::Duration;

/// `key=value` settings, set in order (a later one wins).
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
    pub fn new() -> Config {
        Config::default()
    }
    pub fn set(&mut self, k: impl Into<String>, v: impl Into<String>) -> &mut Config {
        let (k, v) = (k.into(), v.into());
        self.pairs.retain(|(o, _)| *o != k);
        self.pairs.push((k, v));
        self
    }
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

/// How to reach the cluster: its brokers, TLS and SASL, the sockets' timeouts.
#[derive(Clone)]
pub struct Net {
    pub bootstrap: Vec<String>,
    pub client_id: String,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub sasl: Option<(Mechanism, String, String)>,
    /// a request without its answer in this long is a connection lost (`socket.timeout.ms`)
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub backoff: Duration,
    pub backoff_max: Duration,
    /// the largest response taken (`receive.message.max.bytes`)
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
    Earliest,
    Latest,
    Error,
}

/// A consumer's settings, made by `Config::consumer`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ConsumerSettings {
    pub group: Option<String>,
    pub reset: Reset,
    pub partition_eof: bool,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    /// `fetch.max.bytes`, at most `queued.max.messages.kbytes` (as librdkafka caps it)
    pub max_bytes: i32,
    pub partition_max_bytes: i32,
    /// a partition is fetched again while it holds fewer queued (`queued.min.messages`)
    pub queued_min: usize,
    pub committed: bool,
    pub check_crcs: bool,
}

#[derive(Clone, Debug)]
pub struct ProducerSettings {
    pub linger: Duration,
    /// a partition's batch is sent once it holds this many bytes, or `batch_messages` messages
    pub batch_bytes: usize,
    pub batch_messages: usize,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub message_timeout: Duration,
    /// the broker's wait for the replicas' acks (Produce's timeout)
    pub request_timeout: Duration,
    pub retry_backoff: Duration,
    pub retry_backoff_max: Duration,
    pub codec: i16,
    /// the largest batch a broker takes (`message.max.bytes`)
    pub max_batch: usize,
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

    pub fn net(&self) -> Result<Net> {
        self.check()?;
        let servers = self
            .get("bootstrap.servers")
            .or(self.get("metadata.broker.list"))
            .ok_or_else(|| Error::Config("bootstrap.servers: not set".into()))?;
        let bootstrap: Vec<String> = servers
            .split(',')
            .map(|s| {
                s.trim()
                    .trim_start_matches("PLAINTEXT://")
                    .trim_start_matches("SSL://")
                    .trim_start_matches("SASL_SSL://")
                    .trim_start_matches("SASL_PLAINTEXT://")
            })
            .filter(|s| !s.is_empty())
            .map(|s| {
                if s.rsplit_once(':').is_some_and(|(_, p)| p.parse::<u16>().is_ok()) {
                    s.to_string()
                } else {
                    format!("{s}:9092")
                }
            })
            .collect();
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
                let mech =
                    Mechanism::parse(self.get("sasl.mechanisms").or(self.get("sasl.mechanism")).unwrap_or("GSSAPI"))?;
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
        Ok(Net {
            bootstrap,
            client_id: g("client.id", "brrrrr").to_string(),
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
        int("fetch.error.backoff.ms", g("fetch.error.backoff.ms", "500"), 0, 300_000)?;
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
            committed,
            check_crcs: boolean("check.crcs", g("check.crcs", "true"))?,
        })
    }

    pub fn producer(&self) -> Result<ProducerSettings> {
        self.check()?;
        let g = |k, d| self.get(k).unwrap_or(d);
        // every producer of this client is idempotent: what brrrrr's ordering rests on
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
            retry_backoff: ms("retry.backoff.ms", g("retry.backoff.ms", "100"), 1, 300_000)?,
            retry_backoff_max: ms("retry.backoff.max.ms", g("retry.backoff.max.ms", "1000"), 1, 300_000)?,
            codec,
            max_batch: int("message.max.bytes", g("message.max.bytes", "1000000"), 1000, 1_000_000_000)? as usize,
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

    /// librdkafka's defaults where brrrrr leaves them; its names mean what they meant there.
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

    /// A producer's setting on a consumer is taken (brrrrr gives every client the same lines).
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
}
