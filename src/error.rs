//! Errors: a broker's error code (Kafka's `Errors.java`), and what the client met itself.
use std::fmt;

/// A Kafka error code, as a broker returns it (`org.apache.kafka.common.protocol.Errors`).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Code(pub i16);

impl Code {
    // librdkafka's local codes (negative, below -1), for what no broker said, come first.
    /// librdkafka's `_TRANSPORT`: a connection to a broker failed or was lost (local, no broker sent it).
    pub const TRANSPORT: Code = Code(-195);
    /// librdkafka's `_MSG_TIMED_OUT`: a message not delivered within `message.timeout.ms` (local).
    pub const MSG_TIMED_OUT: Code = Code(-192);
    /// librdkafka's `_AUTHENTICATION`: TLS or SASL authentication failed (local).
    pub const AUTHENTICATION: Code = Code(-169);
    /// The broker met an unexpected error; retriable.
    pub const UNKNOWN_SERVER_ERROR: Code = Code(-1);
    /// No error.
    pub const NONE: Code = Code(0);
    /// The offset asked for is not in the partition's log (before its start or past its end).
    pub const OFFSET_OUT_OF_RANGE: Code = Code(1);
    /// A record failed its CRC check or is otherwise corrupt.
    pub const CORRUPT_MESSAGE: Code = Code(2);
    /// This broker does not host the topic or partition (it may not exist yet).
    pub const UNKNOWN_TOPIC_OR_PARTITION: Code = Code(3);
    /// The partition has no leader right now (an election is in progress).
    pub const LEADER_NOT_AVAILABLE: Code = Code(5);
    /// This broker is not the partition's leader (or a follower, for a follower fetch): metadata is stale.
    pub const NOT_LEADER_OR_FOLLOWER: Code = Code(6);
    /// The broker did not complete the request in time (for a Produce, replication within its timeout).
    pub const REQUEST_TIMED_OUT: Code = Code(7);
    /// A record batch is larger than the topic's `max.message.bytes`.
    pub const MESSAGE_TOO_LARGE: Code = Code(10);
    /// The broker's connection to another broker failed while serving the request.
    pub const NETWORK_EXCEPTION: Code = Code(13);
    /// The group coordinator is still loading the group's state.
    pub const COORDINATOR_LOAD_IN_PROGRESS: Code = Code(14);
    /// The group coordinator is not available (the offsets topic not ready).
    pub const COORDINATOR_NOT_AVAILABLE: Code = Code(15);
    /// This broker is not the group's coordinator.
    pub const NOT_COORDINATOR: Code = Code(16);
    /// The topic name is not valid, or the request may not write to it (an internal topic).
    pub const INVALID_TOPIC_EXCEPTION: Code = Code(17);
    /// A Produce request's records exceed the broker's segment size.
    pub const RECORD_LIST_TOO_LARGE: Code = Code(18);
    /// Fewer in-sync replicas than `min.insync.replicas`: the write was refused.
    pub const NOT_ENOUGH_REPLICAS: Code = Code(19);
    /// The write was appended, but fewer in-sync replicas than `min.insync.replicas` acknowledged it.
    pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: Code = Code(20);
    /// Not authorized to access the topic.
    pub const TOPIC_AUTHORIZATION_FAILED: Code = Code(29);
    /// Not authorized to access the consumer group.
    pub const GROUP_AUTHORIZATION_FAILED: Code = Code(30);
    /// Not authorized for a cluster-level operation.
    pub const CLUSTER_AUTHORIZATION_FAILED: Code = Code(31);
    /// The broker does not enable the SASL mechanism asked for.
    pub const UNSUPPORTED_SASL_MECHANISM: Code = Code(33);
    /// The broker does not support the version of the API asked for.
    pub const UNSUPPORTED_VERSION: Code = Code(35);
    /// A topic of that name already exists.
    pub const TOPIC_ALREADY_EXISTS: Code = Code(36);
    /// The request breaks a broker-side policy (e.g. a create-topic policy).
    pub const POLICY_VIOLATION: Code = Code(44);
    /// The idempotent producer's sequence number is not the next the broker expects: records may be lost.
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: Code = Code(45);
    /// The broker already has a batch of this sequence number (a duplicate).
    pub const DUPLICATE_SEQUENCE_NUMBER: Code = Code(46);
    /// The producer's epoch is older than the broker's (another instance has fenced it).
    pub const INVALID_PRODUCER_EPOCH: Code = Code(47);
    /// A disk error on the broker for this partition's log; retriable on another leader.
    pub const KAFKA_STORAGE_ERROR: Code = Code(56);
    /// SASL authentication failed (bad credentials).
    pub const SASL_AUTHENTICATION_FAILED: Code = Code(58);
    /// The broker no longer knows the producer id (its state expired or was removed).
    pub const UNKNOWN_PRODUCER_ID: Code = Code(59);
    /// The leader epoch in the request is older than the broker's: metadata is stale.
    pub const FENCED_LEADER_EPOCH: Code = Code(74);
    /// The leader epoch in the request is newer than the broker's: the broker is behind.
    pub const UNKNOWN_LEADER_EPOCH: Code = Code(75);
    /// The leader cannot serve the offset yet (its high watermark not caught up after an election).
    pub const OFFSET_NOT_AVAILABLE: Code = Code(78);
    /// The broker refused a record as invalid (e.g. a compacted topic's record without a key).
    pub const INVALID_RECORD: Code = Code(87);
    /// A quota on the operation's rate was exceeded; retry later.
    pub const THROTTLING_QUOTA_EXCEEDED: Code = Code(89);
    /// A newer producer with the same transactional id has fenced this one.
    pub const PRODUCER_FENCED: Code = Code(90);

    /// Whether a request that met it may succeed if sent again, to the partition's leader as the
    /// metadata names it then (the codes whose Java exceptions are `RetriableException`s, and the
    /// broker's unknown error, which librdkafka retries too).
    pub fn retriable(self) -> bool {
        matches!(self.0, -1 | 3 | 5 | 6 | 7 | 8 | 9 | 13 | 14 | 15 | 16 | 19 | 20 | 56 | 74 | 75 | 78 | 89)
    }

    /// Whether it means the partition's leader may have moved: its metadata is looked up again.
    pub fn stale_metadata(self) -> bool {
        matches!(self.0, -1 | 3 | 5 | 6 | 9 | 13 | 56 | 74 | 75 | 78)
    }

    /// Its name in `Errors.java`, if this crate knows it.
    pub fn name(self) -> Option<&'static str> {
        NAMES.iter().find(|(c, _)| *c == self.0).map(|(_, n)| *n)
    }
}

impl fmt::Debug for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(n) => write!(f, "{n} ({})", self.0),
            None => write!(f, "error code {}", self.0),
        }
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// `Errors.java`'s names, by code (Apache Kafka trunk, 2026), and librdkafka's local ones.
const NAMES: &[(i16, &str)] = &[
    (-195, "_TRANSPORT"),
    (-192, "_MSG_TIMED_OUT"),
    (-169, "_AUTHENTICATION"),
    (-1, "UNKNOWN_SERVER_ERROR"),
    (0, "NONE"),
    (1, "OFFSET_OUT_OF_RANGE"),
    (2, "CORRUPT_MESSAGE"),
    (3, "UNKNOWN_TOPIC_OR_PARTITION"),
    (4, "INVALID_FETCH_SIZE"),
    (5, "LEADER_NOT_AVAILABLE"),
    (6, "NOT_LEADER_OR_FOLLOWER"),
    (7, "REQUEST_TIMED_OUT"),
    (8, "BROKER_NOT_AVAILABLE"),
    (9, "REPLICA_NOT_AVAILABLE"),
    (10, "MESSAGE_TOO_LARGE"),
    (11, "STALE_CONTROLLER_EPOCH"),
    (12, "OFFSET_METADATA_TOO_LARGE"),
    (13, "NETWORK_EXCEPTION"),
    (14, "COORDINATOR_LOAD_IN_PROGRESS"),
    (15, "COORDINATOR_NOT_AVAILABLE"),
    (16, "NOT_COORDINATOR"),
    (17, "INVALID_TOPIC_EXCEPTION"),
    (18, "RECORD_LIST_TOO_LARGE"),
    (19, "NOT_ENOUGH_REPLICAS"),
    (20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND"),
    (21, "INVALID_REQUIRED_ACKS"),
    (22, "ILLEGAL_GENERATION"),
    (24, "INVALID_GROUP_ID"),
    (25, "UNKNOWN_MEMBER_ID"),
    (27, "REBALANCE_IN_PROGRESS"),
    (28, "INVALID_COMMIT_OFFSET_SIZE"),
    (29, "TOPIC_AUTHORIZATION_FAILED"),
    (30, "GROUP_AUTHORIZATION_FAILED"),
    (31, "CLUSTER_AUTHORIZATION_FAILED"),
    (32, "INVALID_TIMESTAMP"),
    (33, "UNSUPPORTED_SASL_MECHANISM"),
    (34, "ILLEGAL_SASL_STATE"),
    (35, "UNSUPPORTED_VERSION"),
    (36, "TOPIC_ALREADY_EXISTS"),
    (37, "INVALID_PARTITIONS"),
    (38, "INVALID_REPLICATION_FACTOR"),
    (39, "INVALID_REPLICA_ASSIGNMENT"),
    (40, "INVALID_CONFIG"),
    (41, "NOT_CONTROLLER"),
    (42, "INVALID_REQUEST"),
    (43, "UNSUPPORTED_FOR_MESSAGE_FORMAT"),
    (44, "POLICY_VIOLATION"),
    (45, "OUT_OF_ORDER_SEQUENCE_NUMBER"),
    (46, "DUPLICATE_SEQUENCE_NUMBER"),
    (47, "INVALID_PRODUCER_EPOCH"),
    (48, "INVALID_TXN_STATE"),
    (49, "INVALID_PRODUCER_ID_MAPPING"),
    (53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"),
    (54, "SECURITY_DISABLED"),
    (56, "KAFKA_STORAGE_ERROR"),
    (58, "SASL_AUTHENTICATION_FAILED"),
    (59, "UNKNOWN_PRODUCER_ID"),
    (72, "LISTENER_NOT_FOUND"),
    (74, "FENCED_LEADER_EPOCH"),
    (75, "UNKNOWN_LEADER_EPOCH"),
    (76, "UNSUPPORTED_COMPRESSION_TYPE"),
    (78, "OFFSET_NOT_AVAILABLE"),
    (87, "INVALID_RECORD"),
    (89, "THROTTLING_QUOTA_EXCEEDED"),
    (90, "PRODUCER_FENCED"),
    (93, "UNACCEPTABLE_CREDENTIAL"),
];

/// What a call of this crate failed of.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Error {
    /// A broker answered with this code (what it was asked, and its message if it gave one).
    Broker(Code, String),
    /// A connection could not be made, or was lost.
    Transport(String),
    /// A response that does not follow the protocol (cut short, a length past its end, an answer
    /// to another request).
    Protocol(String),
    /// A setting this client does not know, or a value it cannot honour.
    Config(String),
    /// The broker does not speak a version of an API this client needs.
    Unsupported(String),
    /// Authentication failed (TLS or SASL).
    Auth(String),
    /// No answer within the time given.
    Timeout(String),
    /// The producer's queue is full (`queue.buffering.max.messages` or `.kbytes`).
    QueueFull,
    /// A message not delivered within `message.timeout.ms`.
    MessageTimedOut,
    /// A message taken back out of the queue before it was sent (`Producer::purge_queue`).
    Purged,
    /// The idempotent producer can deliver nothing more (its id refused, a sequence out of order).
    Fatal(Code, String),
}

impl Error {
    /// The broker's code, if a broker's answer is why.
    pub fn code(&self) -> Option<Code> {
        match self {
            Error::Broker(c, _) | Error::Fatal(c, _) => Some(*c),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Broker(c, what) if what.is_empty() => write!(f, "broker: {c}"),
            Error::Broker(c, what) => write!(f, "{what}: broker: {c}"),
            Error::Transport(s) => write!(f, "transport: {s}"),
            Error::Protocol(s) => write!(f, "protocol: {s}"),
            Error::Config(s) => write!(f, "config: {s}"),
            Error::Unsupported(s) => write!(f, "unsupported: {s}"),
            Error::Auth(s) => write!(f, "authentication: {s}"),
            Error::Timeout(s) => write!(f, "timed out: {s}"),
            Error::QueueFull => write!(f, "the producer's queue is full"),
            Error::MessageTimedOut => write!(f, "message timed out (message.timeout.ms)"),
            Error::Purged => write!(f, "purged from the producer's queue"),
            Error::Fatal(c, s) => write!(f, "producer fatal error {c}: {s}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Transport(e.to_string())
    }
}

/// This crate's result: an [`Error`] on failure.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    /// The names are Kafka's own: a diagnostic names the code as the broker's logs do.
    #[test]
    fn codes_are_named_as_kafka_names_them() {
        assert_eq!(format!("{:?}", Code::PRODUCER_FENCED), "PRODUCER_FENCED (90)");
        assert_eq!(format!("{}", Code(999)), "error code 999");
        assert!(NAMES.windows(2).all(|w| w[0].0 < w[1].0), "sorted, no duplicates");
        // the local codes as librdkafka numbers them (rdkafka.h's RD_KAFKA_RESP_ERR__*)
        let local = [Code::TRANSPORT, Code::MSG_TIMED_OUT, Code::AUTHENTICATION, Code::UNKNOWN_SERVER_ERROR];
        assert_eq!(
            local.map(|c| format!("{c:?}")),
            ["_TRANSPORT (-195)", "_MSG_TIMED_OUT (-192)", "_AUTHENTICATION (-169)", "UNKNOWN_SERVER_ERROR (-1)"]
        );
        assert_eq!(Error::Broker(Code::NONE, String::new()).to_string(), "broker: NONE (0)");
        assert_eq!(Error::Broker(Code::NONE, "a Fetch".into()).to_string(), "a Fetch: broker: NONE (0)");
    }

    /// A leader looked up again only for a code that says it may have moved, not for any retry.
    #[test]
    fn only_a_moved_leader_is_looked_up_again() {
        assert!(Code::NOT_LEADER_OR_FOLLOWER.stale_metadata());
        assert!(Code::REQUEST_TIMED_OUT.retriable() && !Code::REQUEST_TIMED_OUT.stale_metadata());
    }

    /// What is retried: a leader that moved or a broker going away, never a record the broker
    /// refused, nor an authorization (a retry would loop on it).
    #[test]
    fn only_transient_codes_are_retried() {
        for c in [
            Code::NOT_LEADER_OR_FOLLOWER,
            Code::LEADER_NOT_AVAILABLE,
            Code::NOT_ENOUGH_REPLICAS,
            Code::REQUEST_TIMED_OUT,
        ] {
            assert!(c.retriable(), "{c}");
        }
        for c in [
            Code::MESSAGE_TOO_LARGE,
            Code::CORRUPT_MESSAGE,
            Code::INVALID_RECORD,
            Code::TOPIC_AUTHORIZATION_FAILED,
            Code::OUT_OF_ORDER_SEQUENCE_NUMBER,
            Code::DUPLICATE_SEQUENCE_NUMBER,
            Code::PRODUCER_FENCED,
            Code::OFFSET_OUT_OF_RANGE,
        ] {
            assert!(!c.retriable(), "{c}");
        }
    }
}
