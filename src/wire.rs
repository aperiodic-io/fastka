//! The Kafka protocol's framing and the requests this client sends, each at one version of
//! Apache Kafka's message schemas (`clients/src/main/resources/common/message/*.json`), the
//! fields in the schema's order. `VERSIONS` names them; a connection checks the broker speaks
//! each (`conn.rs`, ApiVersions). Every version here is non-flexible (no tagged fields): request
//! header v1, response header v0. The tests check each encoding against the `kafka-protocol`
//! crate, generated from those schemas.
//!
//! Decoding never trusts a length: a string, bytes or array longer than what is left of the
//! response is an error before anything is allocated for it.
use crate::error::{Code, Error, Result};
use std::ops::Range;

/// The APIs this client sends: (key, the oldest and the newest version it speaks, its name). A
/// connection speaks the newest the broker speaks too (`conn.rs`, ApiVersions).
pub const VERSIONS: &[(i16, i16, i16, &str)] = &[
    // Produce v7+ and Fetch v10+ carry zstd (KIP-110); Produce v8 (Kafka 2.4) adds the records'
    // own errors, which Redpanda (v7 at most) does not send; Fetch v11 is the last non-flexible
    (PRODUCE, 7, 8, "Produce"),
    (FETCH, 11, 11, "Fetch"),
    (LIST_OFFSETS, 2, 2, "ListOffsets"),
    (METADATA, 4, 4, "Metadata"),
    (OFFSET_COMMIT, 2, 2, "OffsetCommit"),
    (FIND_COORDINATOR, 1, 1, "FindCoordinator"),
    (SASL_HANDSHAKE, 1, 1, "SaslHandshake"),
    (API_VERSIONS, 0, 0, "ApiVersions"),
    (CREATE_TOPICS, 4, 4, "CreateTopics"),
    (INIT_PRODUCER_ID, 0, 0, "InitProducerId"),
    (DESCRIBE_CONFIGS, 1, 1, "DescribeConfigs"),
    (SASL_AUTHENTICATE, 0, 0, "SaslAuthenticate"),
];

pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const LIST_OFFSETS: i16 = 2;
pub const METADATA: i16 = 3;
pub const OFFSET_COMMIT: i16 = 8;
pub const FIND_COORDINATOR: i16 = 10;
pub const SASL_HANDSHAKE: i16 = 17;
pub const API_VERSIONS: i16 = 18;
pub const CREATE_TOPICS: i16 = 19;
pub const INIT_PRODUCER_ID: i16 = 22;
pub const DESCRIBE_CONFIGS: i16 = 32;
pub const SASL_AUTHENTICATE: i16 = 36;

/// The newest version this client speaks of `api`.
pub fn version(api: i16) -> i16 {
    VERSIONS.iter().find(|v| v.0 == api).map(|v| v.2).expect("an API of VERSIONS")
}

/// The version of `api` a broker speaking `broker` (key, min, max of each API) and this client
/// both speak, the newest.
pub fn negotiate(api: i16, broker: &[(i16, i16, i16)]) -> Option<i16> {
    let &(_, lo, hi, _) = VERSIONS.iter().find(|v| v.0 == api)?;
    let &(_, bmin, bmax) = broker.iter().find(|b| b.0 == api)?;
    let v = hi.min(bmax);
    (v >= lo.max(bmin)).then_some(v)
}

/// The big-endian primitives of the protocol, appended.
pub trait Put {
    fn i8(&mut self, v: i8);
    fn i16(&mut self, v: i16);
    fn i32(&mut self, v: i32);
    fn i64(&mut self, v: i64);
    fn str(&mut self, s: &str);
    fn nstr(&mut self, s: Option<&str>);
    fn bytes(&mut self, b: &[u8]);
    fn count(&mut self, n: usize);
}

impl Put for Vec<u8> {
    fn i8(&mut self, v: i8) {
        self.push(v as u8);
    }
    fn i16(&mut self, v: i16) {
        self.extend(v.to_be_bytes());
    }
    fn i32(&mut self, v: i32) {
        self.extend(v.to_be_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.extend(v.to_be_bytes());
    }
    fn str(&mut self, s: &str) {
        self.i16(i16::try_from(s.len()).expect("a string of at most 32767 bytes"));
        self.extend(s.as_bytes());
    }
    fn nstr(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.str(s),
            None => self.i16(-1),
        }
    }
    fn bytes(&mut self, b: &[u8]) {
        self.count(b.len());
        self.extend(b);
    }
    fn count(&mut self, n: usize) {
        self.i32(i32::try_from(n).expect("at most 2^31-1 elements"));
    }
}

/// A cursor over a response, bounded by its bytes.
#[derive(Debug, Clone)]
pub struct Cur<'a> {
    pub b: &'a [u8],
    pub at: usize,
}

fn short() -> Error {
    Error::Protocol("a response cut short".into())
}

impl<'a> Cur<'a> {
    pub fn new(b: &'a [u8]) -> Cur<'a> {
        Cur { b, at: 0 }
    }
    pub fn left(&self) -> usize {
        self.b.len() - self.at
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        // the error built only when there is one: this runs for every byte of a record's varints
        let s = self.b.get(self.at..self.at.checked_add(n).ok_or_else(short)?).ok_or_else(short)?;
        self.at += n;
        Ok(s)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("N bytes"))
    }
    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.take(1)?[0] as i8)
    }
    pub fn bool(&mut self) -> Result<bool> {
        Ok(self.i8()? != 0)
    }
    pub fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.array()?))
    }
    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    pub fn code(&mut self) -> Result<Code> {
        Ok(Code(self.i16()?))
    }
    pub fn nstr(&mut self) -> Result<Option<&'a str>> {
        match self.i16()? {
            -1 => Ok(None),
            n => std::str::from_utf8(
                self.take(usize::try_from(n).map_err(|_| Error::Protocol(format!("a string of length {n}")))?)?,
            )
            .map(Some)
            .map_err(|_| Error::Protocol("a string not UTF-8".into())),
        }
    }
    pub fn str(&mut self) -> Result<&'a str> {
        self.nstr()?.ok_or_else(|| Error::Protocol("a null string where the schema has none".into()))
    }
    /// Nullable bytes: their range in the response.
    pub fn nbytes(&mut self) -> Result<Option<Range<usize>>> {
        match self.i32()? {
            -1 => Ok(None),
            n => {
                let start = self.at;
                self.take(usize::try_from(n).map_err(|_| Error::Protocol(format!("bytes of length {n}")))?)?;
                Ok(Some(start..self.at))
            }
        }
    }
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let r = self.nbytes()?.ok_or_else(|| Error::Protocol("null bytes where the schema has none".into()))?;
        Ok(&self.b[r])
    }
    /// An array's length, each of its elements at least `min` bytes: one longer than what is left
    /// is refused before its elements are (a corrupt length allocates nothing). A null array is
    /// empty.
    pub fn count(&mut self, min: usize) -> Result<usize> {
        match self.i32()? {
            -1 => Ok(0),
            // below -1, a length past every response
            n if (n as usize).saturating_mul(min.max(1)) > self.left() => {
                Err(Error::Protocol(format!("an array of {n} elements in {} bytes", self.left())))
            }
            n => Ok(n as usize),
        }
    }
    /// An array of elements of at least `min` bytes, each read by `f`.
    pub fn vec<T>(&mut self, min: usize, mut f: impl FnMut(&mut Cur<'a>) -> Result<T>) -> Result<Vec<T>> {
        let n = self.count(min)?;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(f(self)?);
        }
        Ok(out)
    }
    /// A zigzag varint (a record's lengths, deltas and counts).
    pub fn var(&mut self) -> Result<i64> {
        let (mut z, mut shift) = (0u64, 0);
        loop {
            let b = self.take(1)?[0];
            z |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok((z >> 1) as i64 ^ -((z & 1) as i64));
            }
            shift += 7;
            if shift > 63 {
                return Err(Error::Protocol("a varint too long".into()));
            }
        }
    }
    /// The whole response was read: a field left over means another version's layout.
    pub fn end(&self) -> Result<()> {
        match self.left() {
            0 => Ok(()),
            n => Err(Error::Protocol(format!("{n} bytes left after the response"))),
        }
    }
}

/// A zigzag varint, appended.
pub fn put_var(out: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    while z >= 0x80 {
        out.push(z as u8 | 0x80);
        z >>= 7;
    }
    out.push(z as u8);
}

/// A request of `api` at its newest version (`request_at`).
pub fn request(out: &mut Vec<u8>, api: i16, corr: i32, client_id: &str, body: impl FnOnce(&mut Vec<u8>)) {
    request_at(out, api, version(api), corr, client_id, body)
}

/// A request of `api` at `v` into `out`: its size, header v1 (key, version, correlation id,
/// client id), then `body`.
pub fn request_at(out: &mut Vec<u8>, api: i16, v: i16, corr: i32, client_id: &str, body: impl FnOnce(&mut Vec<u8>)) {
    out.clear();
    out.i32(0); // its size, below
    out.i16(api);
    out.i16(v);
    out.i32(corr);
    out.nstr(Some(client_id));
    body(out);
    let n = i32::try_from(Vec::len(out) - 4).expect("a request under 2 GiB");
    out[..4].copy_from_slice(&n.to_be_bytes());
}

/// A response's body (after its size): its correlation id checked (response header v0).
pub fn response(frame: &[u8], corr: i32) -> Result<Cur<'_>> {
    let mut c = Cur::new(frame);
    let got = c.i32()?;
    if got != corr {
        return Err(Error::Protocol(format!("the answer to request {got}, not to {corr}")));
    }
    Ok(c)
}

/// Items of (topic, rest) grouped by topic, in the order each topic first appears (the
/// requests' topic arrays).
pub fn by_topic<'a, T: Copy>(items: &[(&'a str, T)]) -> Vec<(&'a str, Vec<T>)> {
    let mut out: Vec<(&str, Vec<T>)> = vec![];
    for &(t, x) in items {
        match out.iter_mut().find(|(o, _)| *o == t) {
            Some((_, v)) => v.push(x),
            None => out.push((t, vec![x])),
        }
    }
    out
}

/// An API a broker speaks: (key, min version, max version).
pub type ApiRange = (i16, i16, i16);

// ApiVersions v0 -------------------------------------------------------------------------------

/// ApiVersions v0's request has no fields. Its answer: the broker's (key, min, max) of each API.
pub fn api_versions(mut c: Cur<'_>) -> Result<(Code, Vec<ApiRange>)> {
    let code = c.code()?;
    let apis = c.vec(6, |c| Ok((c.i16()?, c.i16()?, c.i16()?)))?;
    // a broker that does not speak v0 answers v0 all the same, with UNSUPPORTED_VERSION
    Ok((code, apis))
}

// Metadata v4 ----------------------------------------------------------------------------------

/// Metadata v4: the topics named (all of them, `None`), none created by the asking.
pub fn metadata_request(out: &mut Vec<u8>, topics: Option<&[&str]>) {
    match topics {
        None => out.i32(-1),
        Some(ts) => {
            out.count(ts.len());
            for t in ts {
                out.str(t);
            }
        }
    }
    out.i8(0); // allow_auto_topic_creation
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionMeta {
    pub id: i32,
    pub error: Code,
    /// -1 while it has none
    pub leader: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicMeta {
    pub name: String,
    pub error: Code,
    pub partitions: Vec<PartitionMeta>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    pub brokers: Vec<Broker>,
    pub cluster_id: Option<String>,
    pub controller: i32,
    pub topics: Vec<TopicMeta>,
}

impl Metadata {
    pub fn broker(&self, id: i32) -> Option<&Broker> {
        self.brokers.iter().find(|b| b.id == id)
    }
    pub fn topic(&self, name: &str) -> Option<&TopicMeta> {
        self.topics.iter().find(|t| t.name == name)
    }
}

pub fn metadata(mut c: Cur<'_>) -> Result<Metadata> {
    c.i32()?; // throttle
    let brokers = c.vec(12, |c| {
        let b = Broker { id: c.i32()?, host: c.str()?.to_string(), port: c.i32()? };
        c.nstr()?; // rack
        Ok(b)
    })?;
    let cluster_id = c.nstr()?.map(str::to_string);
    let controller = c.i32()?;
    let topics = c.vec(9, |c| {
        let error = c.code()?;
        let name = c.str()?.to_string();
        c.bool()?; // internal
        let partitions = c.vec(18, |c| {
            let p = PartitionMeta { error: c.code()?, id: c.i32()?, leader: c.i32()? };
            c.vec(4, |c| c.i32())?; // replicas
            c.vec(4, |c| c.i32())?; // in sync
            Ok(p)
        })?;
        Ok(TopicMeta { name, error, partitions })
    })?;
    c.end()?;
    Ok(Metadata { brokers, cluster_id, controller, topics })
}

// Fetch v11 ------------------------------------------------------------------------------------

/// What a Fetch asks of the broker, but its partitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchLimits {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    /// 0 read uncommitted, 1 read committed
    pub isolation: i8,
    pub partition_max_bytes: i32,
}

/// Fetch v11 of `parts` (topic, partition, offset), outside a fetch session (KIP-227: id 0,
/// epoch -1, every partition asked each time), no leader epoch checked, no rack.
pub fn fetch_request(out: &mut Vec<u8>, l: &FetchLimits, parts: &[(&str, (i32, i64))]) {
    out.i32(-1); // replica id: a consumer
    out.i32(l.max_wait_ms);
    out.i32(l.min_bytes);
    out.i32(l.max_bytes);
    out.i8(l.isolation);
    out.i32(0); // session id
    out.i32(-1); // session epoch: no session
    let topics = by_topic(parts);
    out.count(topics.len());
    for (t, ps) in topics {
        out.str(t);
        out.count(ps.len());
        for (p, offset) in ps {
            out.i32(p);
            out.i32(-1); // current leader epoch
            out.i64(offset);
            out.i64(-1); // log start offset: a follower's
            out.i32(l.partition_max_bytes);
        }
    }
    out.count(0); // forgotten topics
    out.str(""); // rack id
}

/// A Fetch response's partition; its records as a range of the response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedPartition<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub error: Code,
    pub high: i64,
    pub last_stable: i64,
    /// (producer id, first offset) of each aborted transaction in the records' range
    pub aborted: Vec<(i64, i64)>,
    /// -1, or the replica the broker would have this consumer read from (KIP-392)
    pub preferred_replica: i32,
    pub records: Range<usize>,
}

/// A Fetch response: how long the broker asks the client to wait before its next request
/// (KIP-219), the response's own error (a fetch session's), its partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetched<'a> {
    pub throttle_ms: i32,
    pub error: Code,
    pub parts: Vec<FetchedPartition<'a>>,
}

pub fn fetch(mut c: Cur<'_>) -> Result<Fetched<'_>> {
    let throttle_ms = c.i32()?;
    let error = c.code()?;
    c.i32()?; // session id
    let mut parts = vec![];
    for _ in 0..c.count(6)? {
        let topic = c.str()?;
        // partition 4, error 2, high 8, last stable 8, log start 8, aborted 4, replica 4,
        // records 4 (both null)
        for _ in 0..c.count(42)? {
            let partition = c.i32()?;
            let error = c.code()?;
            let high = c.i64()?;
            let last_stable = c.i64()?;
            c.i64()?; // log start
            let aborted = c.vec(16, |c| Ok((c.i64()?, c.i64()?)))?;
            let preferred_replica = c.i32()?;
            let records = c.nbytes()?.unwrap_or(c.at..c.at);
            parts.push(FetchedPartition {
                topic,
                partition,
                error,
                high,
                last_stable,
                aborted,
                preferred_replica,
                records,
            });
        }
    }
    c.end()?;
    Ok(Fetched { throttle_ms, error, parts })
}

// ListOffsets v2 -------------------------------------------------------------------------------

/// ListOffsets' timestamps for the latest offset (the high watermark, or the last stable offset
/// reading committed) and the earliest (the log start).
pub const LATEST: i64 = -1;
pub const EARLIEST: i64 = -2;

/// ListOffsets v2 of `parts` (topic, (partition, timestamp ms or LATEST or EARLIEST)).
pub fn list_offsets_request(out: &mut Vec<u8>, isolation: i8, parts: &[(&str, (i32, i64))]) {
    out.i32(-1);
    out.i8(isolation);
    let topics = by_topic(parts);
    out.count(topics.len());
    for (t, ps) in topics {
        out.str(t);
        out.count(ps.len());
        for (p, ts) in ps {
            out.i32(p);
            out.i64(ts);
        }
    }
}

/// (topic, partition, error, timestamp, offset): the offset -1 where no record is that recent.
pub type ListedOffset = (String, i32, Code, i64, i64);

pub fn list_offsets(mut c: Cur<'_>) -> Result<Vec<ListedOffset>> {
    c.i32()?; // throttle
    let mut out = vec![];
    for _ in 0..c.count(6)? {
        let topic = c.str()?;
        for _ in 0..c.count(22)? {
            out.push((topic.to_string(), c.i32()?, c.code()?, c.i64()?, c.i64()?));
        }
    }
    c.end()?;
    Ok(out)
}

// Produce v7, v8 --------------------------------------------------------------------------------

/// Produce v7 or v8, the same request (not transactional) of `batches` (topic, (partition, a record batch)).
pub fn produce_request(out: &mut Vec<u8>, acks: i16, timeout_ms: i32, batches: &[(&str, (i32, &[u8]))]) {
    out.nstr(None);
    out.i16(acks);
    out.i32(timeout_ms);
    let topics = by_topic(batches);
    out.count(topics.len());
    for (t, ps) in topics {
        out.str(t);
        out.count(ps.len());
        for (p, batch) in ps {
            out.i32(p);
            out.bytes(batch);
        }
    }
}

/// A partition's answer: topic, partition, error, base offset, and why (the broker's message
/// and its records' own errors, KIP-467), if it said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Produced {
    pub topic: String,
    pub partition: i32,
    pub error: Code,
    pub base_offset: i64,
    pub message: Option<String>,
}

/// The partitions' answers to Produce `v` (7 or 8), and how long the broker asks the client to
/// wait (KIP-219).
pub fn produce(mut c: Cur<'_>, v: i16) -> Result<(Vec<Produced>, i32)> {
    let mut out = vec![];
    for _ in 0..c.count(6)? {
        let topic = c.str()?;
        // index 4, error 2, base 8, append time 8, log start 8 (v8: record errors 4, message 2)
        for _ in 0..c.count(if v >= 8 { 36 } else { 30 })? {
            let (partition, error, base_offset) = (c.i32()?, c.code()?, c.i64()?);
            c.i64()?; // log append time
            c.i64()?; // log start
            let (records, msg) = match v {
                8.. => (c.vec(6, |c| Ok((c.i32()?, c.nstr()?.map(str::to_string))))?, c.nstr()?),
                _ => (vec![], None),
            };
            let msg = msg.filter(|m| !m.is_empty()).map(str::to_string);
            let rec: Vec<String> =
                records.into_iter().map(|(i, m)| format!("record {i}: {}", m.unwrap_or_default())).collect();
            let message = match (msg, rec.is_empty()) {
                (m, true) => m,
                (m, false) => Some(format!("{}{}", m.map(|m| m + ": ").unwrap_or_default(), rec.join(", "))),
            };
            out.push(Produced { topic: topic.to_string(), partition, error, base_offset, message });
        }
    }
    let throttle = c.i32()?;
    c.end()?;
    Ok((out, throttle))
}

// InitProducerId v0 ----------------------------------------------------------------------------

/// InitProducerId v0 for an idempotent producer (no transactional id).
pub fn init_producer_id_request(out: &mut Vec<u8>) {
    out.nstr(None);
    out.i32(i32::MAX); // the transaction timeout: none is begun
}

/// (error, producer id, epoch)
pub fn init_producer_id(mut c: Cur<'_>) -> Result<(Code, i64, i16)> {
    c.i32()?; // throttle
    let r = (c.code()?, c.i64()?, c.i16()?);
    c.end()?;
    Ok(r)
}

// CreateTopics v4 ------------------------------------------------------------------------------

/// CreateTopics v4 of `topics` (name, partitions, replication factor; -1: the broker's default).
pub fn create_topics_request(out: &mut Vec<u8>, topics: &[(&str, i32, i16)], timeout_ms: i32) {
    out.count(topics.len());
    for (name, partitions, replication) in topics {
        out.str(name);
        out.i32(*partitions);
        out.i16(*replication);
        out.count(0); // assignments
        out.count(0); // configs
    }
    out.i32(timeout_ms);
    out.i8(0); // validate only
}

/// (topic, error, message)
pub fn create_topics(mut c: Cur<'_>) -> Result<Vec<(String, Code, Option<String>)>> {
    c.i32()?; // throttle
    let r = c.vec(6, |c| Ok((c.str()?.to_string(), c.code()?, c.nstr()?.map(str::to_string))))?;
    c.end()?;
    Ok(r)
}

// DescribeConfigs v1 ---------------------------------------------------------------------------

/// The resource type of a topic (`ConfigResource.Type.TOPIC`).
pub const TOPIC_RESOURCE: i8 = 2;

/// DescribeConfigs v1 of every setting of `topics`.
pub fn describe_configs_request(out: &mut Vec<u8>, topics: &[&str]) {
    out.count(topics.len());
    for t in topics {
        out.i8(TOPIC_RESOURCE);
        out.str(t);
        out.i32(-1); // every key
    }
    out.i8(0); // include synonyms
}

/// Per resource: (error, message, name, its settings (name, value)).
pub type Described = (Code, Option<String>, String, Vec<(String, Option<String>)>);

pub fn describe_configs(mut c: Cur<'_>) -> Result<Vec<Described>> {
    c.i32()?; // throttle
              // error 2, message 2, type 1, name 2, configs 4
    let r = c.vec(11, |c| {
        let (code, msg) = (c.code()?, c.nstr()?.map(str::to_string));
        c.i8()?;
        let name = c.str()?.to_string();
        let configs = c.vec(11, |c| {
            let kv = (c.str()?.to_string(), c.nstr()?.map(str::to_string));
            c.bool()?; // read only
            c.i8()?; // source
            c.bool()?; // sensitive
            c.vec(5, |c| {
                c.str()?;
                c.nstr()?;
                c.i8()
            })?;
            Ok(kv)
        })?;
        Ok((code, msg, name, configs))
    })?;
    c.end()?;
    Ok(r)
}

// FindCoordinator v1 ---------------------------------------------------------------------------

/// FindCoordinator v1 of consumer group `group`.
pub fn find_coordinator_request(out: &mut Vec<u8>, group: &str) {
    out.str(group);
    out.i8(0); // a group
}

/// (error, message, broker)
pub fn find_coordinator(mut c: Cur<'_>) -> Result<(Code, Option<String>, Broker)> {
    c.i32()?; // throttle
    let (code, msg) = (c.code()?, c.nstr()?.map(str::to_string));
    let b = Broker { id: c.i32()?, host: c.str()?.to_string(), port: c.i32()? };
    c.end()?;
    Ok((code, msg, b))
}

// OffsetCommit v2 ------------------------------------------------------------------------------

/// OffsetCommit v2 of `offsets` (topic, (partition, offset)) for `group`, as a client outside the
/// group's membership commits (generation -1, no member id): the group's offsets for lag tools.
pub fn offset_commit_request(out: &mut Vec<u8>, group: &str, offsets: &[(&str, (i32, i64))]) {
    out.str(group);
    out.i32(-1);
    out.str("");
    out.i64(-1); // the broker's retention
    let topics = by_topic(offsets);
    out.count(topics.len());
    for (t, ps) in topics {
        out.str(t);
        out.count(ps.len());
        for (p, o) in ps {
            out.i32(p);
            out.i64(o);
            out.nstr(None);
        }
    }
}

/// (topic, partition, error)
pub fn offset_commit(mut c: Cur<'_>) -> Result<Vec<(String, i32, Code)>> {
    let mut out = vec![];
    for _ in 0..c.count(6)? {
        let topic = c.str()?;
        for _ in 0..c.count(6)? {
            out.push((topic.to_string(), c.i32()?, c.code()?));
        }
    }
    c.end()?;
    Ok(out)
}

// SASL -----------------------------------------------------------------------------------------

/// SaslHandshake v1: the mechanism, its exchange then in SaslAuthenticate requests.
pub fn sasl_handshake_request(out: &mut Vec<u8>, mechanism: &str) {
    out.str(mechanism);
}

/// (error, the broker's mechanisms)
pub fn sasl_handshake(mut c: Cur<'_>) -> Result<(Code, Vec<String>)> {
    let code = c.code()?;
    let m = c.vec(2, |c| Ok(c.str()?.to_string()))?;
    c.end()?;
    Ok((code, m))
}

pub fn sasl_authenticate_request(out: &mut Vec<u8>, auth: &[u8]) {
    out.bytes(auth);
}

/// (error, message, the broker's bytes)
pub fn sasl_authenticate(mut c: Cur<'_>) -> Result<(Code, Option<String>, Vec<u8>)> {
    let (code, msg) = (c.code()?, c.nstr()?.map(str::to_string));
    let auth = c.bytes()?.to_vec();
    c.end()?;
    Ok((code, msg, auth))
}

#[cfg(test)]
mod tests;
