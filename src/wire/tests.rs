#![allow(clippy::type_complexity)] // the tests' tuples say what each field is where they are made
//! Conformance: each request this client encodes is decoded by `kafka-protocol` (generated from
//! Apache Kafka's message schemas) at the version sent, and each response it decodes is one that
//! codec encoded. Then robustness: no response, cut anywhere or corrupted, panics or allocates
//! past its own length.
use super::*;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages as km;
use kafka_protocol::protocol::{Decodable, Encodable, Message, StrBytes};

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

fn topic(v: &str) -> km::TopicName {
    km::TopicName(s(v))
}

/// The request `out` holds decoded by the oracle: its header checked, its body as `R`.
fn oracle_request<R: Decodable>(out: &[u8], api: i16, corr: i32) -> R {
    let size = i32::from_be_bytes(out[..4].try_into().unwrap()) as usize;
    assert_eq!(size, out.len() - 4, "the size prefix counts what follows it");
    let mut b = Bytes::copy_from_slice(&out[4..]);
    let h = km::RequestHeader::decode(&mut b, 1).unwrap();
    assert_eq!((h.request_api_key, h.request_api_version, h.correlation_id), (api, version(api), corr));
    assert_eq!(h.client_id.as_deref(), Some("fastka"));
    let r = R::decode(&mut b, version(api)).unwrap();
    assert!(b.is_empty(), "{} bytes the schema has no field for", b.len());
    r
}

/// A response encoded by the oracle at the newest version this client speaks, after its header v0.
fn oracle_response<R: Encodable>(r: &R, api: i16, corr: i32) -> Vec<u8> {
    oracle_response_at(r, version(api), corr)
}

fn oracle_response_at<R: Encodable>(r: &R, v: i16, corr: i32) -> Vec<u8> {
    let mut b = BytesMut::new();
    km::ResponseHeader::default().with_correlation_id(corr).encode(&mut b, 0).unwrap();
    r.encode(&mut b, v).unwrap();
    b.to_vec()
}

/// Every prefix of a response short of the whole is an error, never a panic.
fn every_cut_fails<T: std::fmt::Debug>(frame: &[u8], decode: impl Fn(Cur<'_>) -> Result<T>) {
    for n in 4..frame.len() {
        let c = response(&frame[..n], i32::from_be_bytes(frame[..4].try_into().unwrap())).unwrap();
        assert!(decode(c).is_err(), "cut at {n} of {}", frame.len());
    }
}

macro_rules! in_spec {
    ($($api:ident => $req:ty, $resp:ty;)*) => {
        /// Each version this client sends is one the schemas define, for the request and its response.
        #[test]
        fn every_version_sent_is_in_the_schemas() {
            $(
                let &(_, lo, hi, _) = VERSIONS.iter().find(|v| v.0 == $api).unwrap();
                for v in lo..=hi {
                    assert!((<$req>::VERSIONS.min..=<$req>::VERSIONS.max).contains(&v), "{} v{v}", stringify!($req));
                    assert!((<$resp>::VERSIONS.min..=<$resp>::VERSIONS.max).contains(&v), "{} v{v}", stringify!($resp));
                }
            )*
            assert_eq!(VERSIONS.len(), [$(stringify!($api)),*].len());
        }
    };
}

in_spec! {
    PRODUCE => km::ProduceRequest, km::ProduceResponse;
    FETCH => km::FetchRequest, km::FetchResponse;
    LIST_OFFSETS => km::ListOffsetsRequest, km::ListOffsetsResponse;
    METADATA => km::MetadataRequest, km::MetadataResponse;
    OFFSET_COMMIT => km::OffsetCommitRequest, km::OffsetCommitResponse;
    FIND_COORDINATOR => km::FindCoordinatorRequest, km::FindCoordinatorResponse;
    SASL_HANDSHAKE => km::SaslHandshakeRequest, km::SaslHandshakeResponse;
    API_VERSIONS => km::ApiVersionsRequest, km::ApiVersionsResponse;
    CREATE_TOPICS => km::CreateTopicsRequest, km::CreateTopicsResponse;
    INIT_PRODUCER_ID => km::InitProducerIdRequest, km::InitProducerIdResponse;
    DESCRIBE_CONFIGS => km::DescribeConfigsRequest, km::DescribeConfigsResponse;
    SASL_AUTHENTICATE => km::SaslAuthenticateRequest, km::SaslAuthenticateResponse;
}

/// The versions sent are non-flexible, so the headers are v1 and v0 (no tagged fields), which is
/// what `request` and `response` write and read.
#[test]
fn every_version_sent_has_the_headers_this_client_writes() {
    use kafka_protocol::messages::ApiKey;
    for &(api, lo, hi, name) in VERSIONS {
        let key = ApiKey::try_from(api).unwrap();
        for v in lo..=hi {
            assert_eq!(key.request_header_version(v), 1, "{name} v{v}");
            assert_eq!(key.response_header_version(v), 0, "{name} v{v}");
        }
    }
}

#[test]
fn api_versions() {
    let mut out = vec![];
    request(&mut out, API_VERSIONS, 7, "fastka", |_| {});
    let _: km::ApiVersionsRequest = oracle_request(&out, API_VERSIONS, 7);
    let r = km::ApiVersionsResponse::default().with_error_code(0).with_api_keys(vec![
        km::api_versions_response::ApiVersion::default().with_api_key(1).with_min_version(4).with_max_version(17),
        km::api_versions_response::ApiVersion::default().with_api_key(3).with_min_version(0).with_max_version(12),
    ]);
    let frame = oracle_response(&r, API_VERSIONS, 7);
    assert_eq!(super::api_versions(response(&frame, 7).unwrap()).unwrap(), (Code::NONE, vec![(1, 4, 17), (3, 0, 12)]));
}

#[test]
fn metadata() {
    for topics in [None, Some(&["a", "b.c"][..])] {
        let mut out = vec![];
        request(&mut out, METADATA, 9, "fastka", |o| metadata_request(o, topics));
        let r: km::MetadataRequest = oracle_request(&out, METADATA, 9);
        let names = r.topics.map(|ts| ts.into_iter().map(|t| t.name.unwrap().0.to_string()).collect::<Vec<_>>());
        assert_eq!(names, topics.map(|ts| ts.iter().map(|t| t.to_string()).collect()));
        assert!(!r.allow_auto_topic_creation, "asking never creates a topic");
    }
    use km::metadata_response::*;
    let part = |i, leader, e| {
        MetadataResponsePartition::default()
            .with_partition_index(i)
            .with_leader_id(km::BrokerId(leader))
            .with_error_code(e)
            .with_replica_nodes(vec![km::BrokerId(1), km::BrokerId(2)])
            .with_isr_nodes(vec![km::BrokerId(1)])
    };
    let r = km::MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default().with_node_id(km::BrokerId(1)).with_host(s("k1")).with_port(9092),
            MetadataResponseBroker::default()
                .with_node_id(km::BrokerId(2))
                .with_host(s("k2"))
                .with_port(9093)
                .with_rack(Some(s("r"))),
        ])
        .with_cluster_id(Some(s("cid")))
        .with_controller_id(km::BrokerId(2))
        .with_topics(vec![
            MetadataResponseTopic::default()
                .with_name(Some(topic("t")))
                .with_partitions(vec![part(0, 1, 0), part(1, -1, 5)]),
            MetadataResponseTopic::default().with_name(Some(topic("gone"))).with_error_code(3),
        ]);
    let frame = oracle_response(&r, METADATA, 9);
    let m = super::metadata(response(&frame, 9).unwrap()).unwrap();
    assert_eq!(
        m,
        Metadata {
            brokers: vec![
                Broker { id: 1, host: "k1".into(), port: 9092 },
                Broker { id: 2, host: "k2".into(), port: 9093 }
            ],
            cluster_id: Some("cid".into()),
            controller: 2,
            topics: vec![
                TopicMeta {
                    name: "t".into(),
                    error: Code::NONE,
                    partitions: vec![
                        PartitionMeta { id: 0, error: Code::NONE, leader: 1 },
                        PartitionMeta { id: 1, error: Code::LEADER_NOT_AVAILABLE, leader: -1 }
                    ]
                },
                TopicMeta { name: "gone".into(), error: Code::UNKNOWN_TOPIC_OR_PARTITION, partitions: vec![] },
            ],
        }
    );
    every_cut_fails(&frame, super::metadata);
}

#[test]
fn fetch() {
    let l =
        FetchLimits { max_wait_ms: 50, min_bytes: 1, max_bytes: 4 << 20, isolation: 1, partition_max_bytes: 1 << 20 };
    let mut out = vec![];
    request(&mut out, FETCH, 3, "fastka", |o| fetch_request(o, &l, &[("a", (0, 5)), ("b", (2, 0)), ("a", (1, 9))]));
    let r: km::FetchRequest = oracle_request(&out, FETCH, 3);
    assert_eq!((r.replica_id.0, r.max_wait_ms, r.min_bytes, r.max_bytes, r.isolation_level), (-1, 50, 1, 4 << 20, 1));
    assert_eq!((r.session_id, r.session_epoch, r.rack_id.as_str()), (0, -1, ""), "no fetch session, no rack");
    assert!(r.forgotten_topics_data.is_empty());
    assert!(r
        .topics
        .iter()
        .flat_map(|t| &t.partitions)
        .all(|p| p.current_leader_epoch == -1 && p.log_start_offset == -1));
    let got: Vec<(String, i32, i64, i32)> = (r.topics.iter())
        .flat_map(|t| {
            t.partitions.iter().map(|p| (t.topic.0.to_string(), p.partition, p.fetch_offset, p.partition_max_bytes))
        })
        .collect();
    assert_eq!(got, [("a".into(), 0, 5, 1 << 20), ("a".into(), 1, 9, 1 << 20), ("b".into(), 2, 0, 1 << 20)]);

    use km::fetch_response::*;
    let r =
        km::FetchResponse::default().with_throttle_time_ms(4).with_responses(vec![FetchableTopicResponse::default()
            .with_topic(topic("a"))
            .with_partitions(vec![
                PartitionData::default()
                    .with_partition_index(0)
                    .with_high_watermark(10)
                    .with_last_stable_offset(8)
                    .with_aborted_transactions(Some(vec![AbortedTransaction::default()
                        .with_producer_id(km::ProducerId(77))
                        .with_first_offset(6)]))
                    .with_records(Some(Bytes::from_static(b"RECORDS"))),
                PartitionData::default().with_partition_index(1).with_error_code(1).with_high_watermark(-1),
            ])]);
    let frame = oracle_response(&r, FETCH, 3);
    let got = super::fetch(response(&frame, 3).unwrap()).unwrap();
    assert_eq!((got.throttle_ms, got.error), (4, Code::NONE));
    let parts = got.parts;
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].preferred_replica, -1, "the oracle's default: none");
    assert_eq!((parts[0].topic, parts[0].partition, parts[0].high, parts[0].last_stable), ("a", 0, 10, 8));
    assert_eq!(parts[0].aborted, [(77, 6)]);
    assert_eq!(&frame[parts[0].records.clone()], b"RECORDS", "the records' range in the frame");
    assert_eq!((parts[1].error, parts[1].records.len()), (Code::OFFSET_OUT_OF_RANGE, 0), "null records: none");
    every_cut_fails(&frame, |c| super::fetch(c).map(|v| v.parts.len()));
}

#[test]
fn list_offsets() {
    let mut out = vec![];
    request(&mut out, LIST_OFFSETS, 5, "fastka", |o| {
        list_offsets_request(o, 1, &[("t", (0, LATEST)), ("t", (1, 1234))])
    });
    let r: km::ListOffsetsRequest = oracle_request(&out, LIST_OFFSETS, 5);
    assert_eq!((r.replica_id.0, r.isolation_level), (-1, 1));
    let ps: Vec<(i32, i64)> = r.topics[0].partitions.iter().map(|p| (p.partition_index, p.timestamp)).collect();
    assert_eq!((r.topics[0].name.0.as_str(), ps), ("t", vec![(0, -1), (1, 1234)]));
    use km::list_offsets_response::*;
    let r = km::ListOffsetsResponse::default().with_topics(vec![ListOffsetsTopicResponse::default()
        .with_name(topic("t"))
        .with_partitions(vec![
            ListOffsetsPartitionResponse::default().with_partition_index(0).with_timestamp(-1).with_offset(42),
            ListOffsetsPartitionResponse::default().with_partition_index(1).with_error_code(6).with_offset(-1),
        ])]);
    let frame = oracle_response(&r, LIST_OFFSETS, 5);
    assert_eq!(
        super::list_offsets(response(&frame, 5).unwrap()).unwrap(),
        [("t".into(), 0, Code::NONE, -1, 42), ("t".into(), 1, Code::NOT_LEADER_OR_FOLLOWER, -1, -1)]
    );
    every_cut_fails(&frame, super::list_offsets);
}

#[test]
fn produce() {
    let mut out = vec![];
    request(&mut out, PRODUCE, 11, "fastka", |o| {
        produce_request(o, -1, 30_000, &[("o", (0, b"B0")), ("o", (3, b"B3"))])
    });
    let r: km::ProduceRequest = oracle_request(&out, PRODUCE, 11);
    assert_eq!((r.transactional_id, r.acks, r.timeout_ms), (None, -1, 30_000));
    let ps: Vec<(i32, Bytes)> =
        r.topic_data[0].partition_data.iter().map(|p| (p.index, p.records.clone().unwrap())).collect();
    assert_eq!(ps, [(0, Bytes::from_static(b"B0")), (3, Bytes::from_static(b"B3"))]);
    use km::produce_response::*;
    let r =
        km::ProduceResponse::default().with_throttle_time_ms(1).with_responses(vec![TopicProduceResponse::default()
            .with_name(topic("o"))
            .with_partition_responses(vec![
                PartitionProduceResponse::default()
                    .with_index(0)
                    .with_base_offset(100)
                    .with_log_append_time_ms(-1)
                    .with_error_message(None),
                PartitionProduceResponse::default()
                    .with_index(3)
                    .with_error_code(87)
                    .with_base_offset(-1)
                    .with_error_message(Some(s("bad")))
                    .with_record_errors(vec![BatchIndexAndErrorMessage::default()
                        .with_batch_index(2)
                        .with_batch_index_error_message(Some(s("no key")))]),
            ])]);
    let frame = oracle_response(&r, PRODUCE, 11);
    let (got, throttle) = super::produce(response(&frame, 11).unwrap(), 8).unwrap();
    assert_eq!(throttle, 1);
    assert_eq!(
        got,
        [
            Produced { topic: "o".into(), partition: 0, error: Code::NONE, base_offset: 100, message: None },
            Produced {
                topic: "o".into(),
                partition: 3,
                error: Code::INVALID_RECORD,
                base_offset: -1,
                message: Some("bad: record 2: no key".into())
            }
        ]
    );
    every_cut_fails(&frame, |c| super::produce(c, 8).map(|r| r.0.len()));
    // v7, as Redpanda answers: no records' errors, no message
    let frame = oracle_response_at(&r, 7, 11);
    let (got, _) = super::produce(response(&frame, 11).unwrap(), 7).unwrap();
    assert_eq!((got[1].error, got[1].message.as_deref()), (Code::INVALID_RECORD, None));
    every_cut_fails(&frame, |c| super::produce(c, 7).map(|r| r.0.len()));
    // the request is the same at both versions
    let mut v7 = vec![];
    request_at(&mut v7, PRODUCE, 7, 11, "fastka", |o| produce_request(o, -1, 30_000, &[("o", (0, b"B0"))]));
    let mut b = Bytes::copy_from_slice(&v7[4..]);
    km::RequestHeader::decode(&mut b, 1).unwrap();
    km::ProduceRequest::decode(&mut b, 7).unwrap();
    assert!(b.is_empty());
}

#[test]
fn init_producer_id() {
    let mut out = vec![];
    request(&mut out, INIT_PRODUCER_ID, 2, "fastka", init_producer_id_request);
    let r: km::InitProducerIdRequest = oracle_request(&out, INIT_PRODUCER_ID, 2);
    assert_eq!(r.transactional_id, None, "idempotent, not transactional");
    let r = km::InitProducerIdResponse::default().with_producer_id(km::ProducerId(9001)).with_producer_epoch(3);
    let frame = oracle_response(&r, INIT_PRODUCER_ID, 2);
    assert_eq!(super::init_producer_id(response(&frame, 2).unwrap()).unwrap(), (Code::NONE, 9001, 3));
    every_cut_fails(&frame, super::init_producer_id);
}

#[test]
fn create_topics() {
    let mut out = vec![];
    request(&mut out, CREATE_TOPICS, 4, "fastka", |o| create_topics_request(o, &[("x", 1, -1), ("y", 3, 2)], 30_000));
    let r: km::CreateTopicsRequest = oracle_request(&out, CREATE_TOPICS, 4);
    let ts: Vec<(String, i32, i16)> =
        r.topics.iter().map(|t| (t.name.0.to_string(), t.num_partitions, t.replication_factor)).collect();
    assert_eq!(ts, [("x".into(), 1, -1), ("y".into(), 3, 2)]);
    assert_eq!((r.timeout_ms, r.validate_only), (30_000, false));
    use km::create_topics_response::*;
    let r = km::CreateTopicsResponse::default().with_topics(vec![
        CreatableTopicResult::default().with_name(topic("x")).with_error_message(None),
        CreatableTopicResult::default().with_name(topic("y")).with_error_code(36).with_error_message(Some(s("exists"))),
    ]);
    let frame = oracle_response(&r, CREATE_TOPICS, 4);
    assert_eq!(
        super::create_topics(response(&frame, 4).unwrap()).unwrap(),
        [("x".into(), Code::NONE, None), ("y".into(), Code::TOPIC_ALREADY_EXISTS, Some("exists".into()))]
    );
    every_cut_fails(&frame, super::create_topics);
}

#[test]
fn describe_configs() {
    let mut out = vec![];
    request(&mut out, DESCRIBE_CONFIGS, 6, "fastka", |o| describe_configs_request(o, &["t"]));
    let r: km::DescribeConfigsRequest = oracle_request(&out, DESCRIBE_CONFIGS, 6);
    assert_eq!(
        (
            r.resources[0].resource_type,
            r.resources[0].resource_name.as_str(),
            r.resources[0].configuration_keys.is_none()
        ),
        (2, "t", true)
    );
    use km::describe_configs_response::*;
    let r = km::DescribeConfigsResponse::default().with_results(vec![DescribeConfigsResult::default()
        .with_resource_type(2)
        .with_error_message(None)
        .with_resource_name(s("t"))
        .with_configs(vec![
            DescribeConfigsResourceResult::default().with_name(s("retention.ms")).with_value(Some(s("3600000"))),
            DescribeConfigsResourceResult::default()
                .with_name(s("sasl.jaas.config"))
                .with_value(None)
                .with_is_sensitive(true),
        ])]);
    let frame = oracle_response(&r, DESCRIBE_CONFIGS, 6);
    assert_eq!(
        super::describe_configs(response(&frame, 6).unwrap()).unwrap(),
        [(
            Code::NONE,
            None,
            "t".into(),
            vec![("retention.ms".into(), Some("3600000".into())), ("sasl.jaas.config".into(), None)]
        )]
    );
    every_cut_fails(&frame, super::describe_configs);
}

#[test]
fn find_coordinator_and_offset_commit() {
    let mut out = vec![];
    request(&mut out, FIND_COORDINATOR, 1, "fastka", |o| find_coordinator_request(o, "g"));
    let r: km::FindCoordinatorRequest = oracle_request(&out, FIND_COORDINATOR, 1);
    assert_eq!((r.key.as_str(), r.key_type), ("g", 0));
    let r = km::FindCoordinatorResponse::default().with_node_id(km::BrokerId(3)).with_host(s("k3")).with_port(9094);
    let frame = oracle_response(&r, FIND_COORDINATOR, 1);
    let (code, _, b) = super::find_coordinator(response(&frame, 1).unwrap()).unwrap();
    assert_eq!((code, b), (Code::NONE, Broker { id: 3, host: "k3".into(), port: 9094 }));
    every_cut_fails(&frame, super::find_coordinator);

    request(&mut out, OFFSET_COMMIT, 2, "fastka", |o| offset_commit_request(o, "g", &[("t", (0, 10)), ("t", (1, 20))]));
    let r: km::OffsetCommitRequest = oracle_request(&out, OFFSET_COMMIT, 2);
    assert_eq!(
        (r.group_id.0.as_str(), r.generation_id_or_member_epoch, r.member_id.as_str(), r.retention_time_ms),
        ("g", -1, "", -1),
        "a commit from outside the group's membership"
    );
    let ps: Vec<(i32, i64)> = r.topics[0].partitions.iter().map(|p| (p.partition_index, p.committed_offset)).collect();
    assert_eq!(ps, [(0, 10), (1, 20)]);
    use km::offset_commit_response::*;
    let r = km::OffsetCommitResponse::default().with_topics(vec![OffsetCommitResponseTopic::default()
        .with_name(topic("t"))
        .with_partitions(vec![
            OffsetCommitResponsePartition::default().with_partition_index(0),
            OffsetCommitResponsePartition::default().with_partition_index(1).with_error_code(30),
        ])]);
    let frame = oracle_response(&r, OFFSET_COMMIT, 2);
    assert_eq!(
        super::offset_commit(response(&frame, 2).unwrap()).unwrap(),
        [("t".into(), 0, Code::NONE), ("t".into(), 1, Code::GROUP_AUTHORIZATION_FAILED)]
    );
    every_cut_fails(&frame, super::offset_commit);
}

#[test]
fn sasl() {
    let mut out = vec![];
    request(&mut out, SASL_HANDSHAKE, 1, "fastka", |o| sasl_handshake_request(o, "SCRAM-SHA-256"));
    let r: km::SaslHandshakeRequest = oracle_request(&out, SASL_HANDSHAKE, 1);
    assert_eq!(r.mechanism.as_str(), "SCRAM-SHA-256");
    let r = km::SaslHandshakeResponse::default().with_mechanisms(vec![s("PLAIN"), s("SCRAM-SHA-512")]);
    let frame = oracle_response(&r, SASL_HANDSHAKE, 1);
    assert_eq!(
        super::sasl_handshake(response(&frame, 1).unwrap()).unwrap(),
        (Code::NONE, vec!["PLAIN".to_string(), "SCRAM-SHA-512".to_string()])
    );
    every_cut_fails(&frame, super::sasl_handshake);

    request(&mut out, SASL_AUTHENTICATE, 2, "fastka", |o| sasl_authenticate_request(o, b"n,,n=u,r=x"));
    let r: km::SaslAuthenticateRequest = oracle_request(&out, SASL_AUTHENTICATE, 2);
    assert_eq!(&r.auth_bytes[..], b"n,,n=u,r=x");
    let r = km::SaslAuthenticateResponse::default()
        .with_error_code(58)
        .with_error_message(Some(s("bad")))
        .with_auth_bytes(Bytes::new());
    let frame = oracle_response(&r, SASL_AUTHENTICATE, 2);
    assert_eq!(
        super::sasl_authenticate(response(&frame, 2).unwrap()).unwrap(),
        (Code::SASL_AUTHENTICATION_FAILED, Some("bad".into()), vec![])
    );
    every_cut_fails(&frame, super::sasl_authenticate);
}

/// A response to another request is refused, not mistaken for this one's.
#[test]
fn a_response_to_another_request_is_refused() {
    let frame = oracle_response(&km::InitProducerIdResponse::default(), INIT_PRODUCER_ID, 5);
    assert!(matches!(response(&frame, 6), Err(Error::Protocol(_))));
}

/// A length larger than the response is refused before anything is allocated for it: an array
/// of 2^31-1 brokers in a few bytes, a string of 32767.
#[test]
fn a_length_past_the_end_allocates_nothing() {
    let mut b = vec![];
    b.i32(0); // throttle
    b.i32(i32::MAX); // brokers
    assert!(super::metadata(Cur::new(&b)).unwrap_err().to_string().contains("2147483647 elements"));
    let mut b = vec![];
    b.i16(0);
    b.i16(i16::MAX);
    assert!(super::sasl_handshake(Cur::new(&b)).is_err());
    let mut b = vec![];
    b.i32(-7);
    assert!(Cur::new(&b).count(1).is_err(), "a negative length");
}

/// The varints are Kafka's zigzag (ByteUtils.writeVarlong): each value round-trips, and the
/// encoding of the spec's examples is the spec's.
#[test]
fn varints_are_zigzag() {
    for (v, bytes) in [(0i64, &[0u8][..]), (-1, &[1]), (1, &[2]), (-64, &[0x7f]), (64, &[0x80, 1]), (300, &[0xd8, 4])] {
        let mut out = vec![];
        put_var(&mut out, v);
        assert_eq!(out, bytes, "{v}");
    }
    for v in [i64::MIN, i64::MAX, -1234567, 98765432] {
        let mut out = vec![];
        put_var(&mut out, v);
        assert_eq!(Cur::new(&out).var().unwrap(), v);
    }
    assert!(Cur::new(&[0xff; 11]).var().is_err(), "past 64 bits");
}

proptest::proptest! {
    /// No bytes, whatever they hold, make a decoder panic.
    #[test]
    fn no_response_panics(b in proptest::collection::vec(proptest::num::u8::ANY, 0..256)) {
        let _ = super::metadata(Cur::new(&b));
        let _ = super::fetch(Cur::new(&b));
        let _ = super::list_offsets(Cur::new(&b));
        let _ = super::produce(Cur::new(&b), 7);
        let _ = super::produce(Cur::new(&b), 8);
        let _ = super::describe_configs(Cur::new(&b));
        let _ = super::create_topics(Cur::new(&b));
        let _ = super::offset_commit(Cur::new(&b));
        let _ = super::api_versions(Cur::new(&b));
        let _ = super::find_coordinator(Cur::new(&b));
        let _ = super::sasl_handshake(Cur::new(&b));
        let _ = super::sasl_authenticate(Cur::new(&b));
    }
}

/// Each array's elements at their smallest (every nullable field null, every array empty, every
/// string empty), as the oracle encodes them: decoded, not refused as "longer than the response"
/// (the bound each array's length is checked against is the smallest element the schema allows).
#[test]
fn the_smallest_elements_decode() {
    use km::fetch_response::*;
    let r = km::FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(topic(""))
        .with_partitions(vec![PartitionData::default()
            .with_error_code(1)
            .with_aborted_transactions(None)
            .with_records(None)])]);
    let frame = oracle_response(&r, FETCH, 1);
    assert_eq!(super::fetch(response(&frame, 1).unwrap()).unwrap().parts[0].error, Code::OFFSET_OUT_OF_RANGE);

    use km::describe_configs_response::*;
    let r = km::DescribeConfigsResponse::default().with_results(vec![DescribeConfigsResult::default()
        .with_error_code(29)
        .with_error_message(None)
        .with_resource_name(s(""))
        .with_configs(vec![DescribeConfigsResourceResult::default().with_name(s("")).with_value(None)])]);
    let frame = oracle_response(&r, DESCRIBE_CONFIGS, 1);
    assert_eq!(super::describe_configs(response(&frame, 1).unwrap()).unwrap()[0].0, Code::TOPIC_AUTHORIZATION_FAILED);

    use km::metadata_response::*;
    let r = km::MetadataResponse::default()
        .with_brokers(vec![MetadataResponseBroker::default().with_host(s("")).with_rack(None)])
        .with_cluster_id(None)
        .with_topics(vec![MetadataResponseTopic::default()
            .with_name(Some(topic("")))
            .with_partitions(vec![MetadataResponsePartition::default()])]);
    let frame = oracle_response(&r, METADATA, 1);
    assert_eq!(super::metadata(response(&frame, 1).unwrap()).unwrap().topics[0].partitions.len(), 1);

    let r = km::CreateTopicsResponse::default().with_topics(vec![
        km::create_topics_response::CreatableTopicResult::default().with_name(topic("")).with_error_message(None),
    ]);
    let frame = oracle_response(&r, CREATE_TOPICS, 1);
    assert_eq!(super::create_topics(response(&frame, 1).unwrap()).unwrap().len(), 1);

    let r = km::ListOffsetsResponse::default().with_topics(vec![
        km::list_offsets_response::ListOffsetsTopicResponse::default()
            .with_name(topic(""))
            .with_partitions(vec![km::list_offsets_response::ListOffsetsPartitionResponse::default()]),
    ]);
    let frame = oracle_response(&r, LIST_OFFSETS, 1);
    assert_eq!(super::list_offsets(response(&frame, 1).unwrap()).unwrap().len(), 1);

    let r = km::ProduceResponse::default().with_responses(vec![km::produce_response::TopicProduceResponse::default()
        .with_name(topic(""))
        .with_partition_responses(vec![
            km::produce_response::PartitionProduceResponse::default().with_error_message(None)
        ])]);
    let frame = oracle_response(&r, PRODUCE, 1);
    assert_eq!(super::produce(response(&frame, 1).unwrap(), 8).unwrap().0.len(), 1);

    let r = km::OffsetCommitResponse::default().with_topics(vec![
        km::offset_commit_response::OffsetCommitResponseTopic::default()
            .with_name(topic(""))
            .with_partitions(vec![km::offset_commit_response::OffsetCommitResponsePartition::default()]),
    ]);
    let frame = oracle_response(&r, OFFSET_COMMIT, 1);
    assert_eq!(super::offset_commit(response(&frame, 1).unwrap()).unwrap().len(), 1);

    let r = km::SaslHandshakeResponse::default().with_mechanisms(vec![s("")]);
    let frame = oracle_response(&r, SASL_HANDSHAKE, 1);
    assert_eq!(super::sasl_handshake(response(&frame, 1).unwrap()).unwrap().1, [""]);
}

/// The primitives as the protocol defines them: a boolean any byte but 0, null as length -1 and
/// any other negative length refused, no byte left after a response, ListOffsets' special
/// timestamps (`ListOffsetsRequest.LATEST_TIMESTAMP`, `EARLIEST_TIMESTAMP`).
#[test]
fn the_primitives_read_as_the_protocol_says() {
    let mut c = Cur::new(&[0, 1, 2]);
    assert_eq!([c.bool().unwrap(), c.bool().unwrap(), c.bool().unwrap()], [false, true, true]);
    assert!(c.end().is_ok());
    assert_eq!(Cur::new(&[0xff, 0xff]).nstr().unwrap(), None);
    let e = Cur::new(&[0xff, 0xfe, b'x']).nstr().unwrap_err();
    assert!(e.to_string().contains("a string of length -2"), "{e}");
    assert_eq!(Cur::new(&[0xff, 0xff, 0xff, 0xff]).nbytes().unwrap(), None);
    let e = Cur::new(&[0xff, 0xff, 0xff, 0xfe, 0]).nbytes().unwrap_err();
    assert!(e.to_string().contains("bytes of length -2"), "{e}");
    let e = Cur::new(&[0]).end().unwrap_err();
    assert!(e.to_string().contains("1 bytes left"), "{e}");
    let mut ok = vec![];
    ok.i32(0);
    ok.i16(0);
    ok.i64(7);
    ok.i16(1);
    assert!(super::init_producer_id(Cur::new(&ok)).is_ok());
    ok.push(0);
    assert!(super::init_producer_id(Cur::new(&ok)).is_err(), "a byte past the response");
    assert_eq!((LATEST, EARLIEST), (-1, -2));
}

/// A string longer than its i16 length can say is a Config error where it is handed in, not a
/// panic where it is written.
#[test]
fn an_overlong_string_is_refused_before_it_is_written() {
    check_str("group.id", &"g".repeat(32767)).unwrap();
    let e = check_str("group.id", &"g".repeat(32768)).unwrap_err();
    assert!(matches!(&e, Error::Config(m) if m.starts_with("group.id: 32768 bytes")), "{e}");
}

/// The version both sides speak is the newest within both ranges, of that API's own range, and
/// none where the ranges do not meet or the broker lacks the API.
#[test]
fn negotiate_takes_the_newest_version_both_speak() {
    let &(_, lo, hi, _) = VERSIONS.iter().find(|v| v.0 == PRODUCE).unwrap();
    let other = (FETCH, 0, 100); // another API's range, first: not taken for Produce's
    assert_eq!(negotiate(PRODUCE, &[other, (PRODUCE, 0, hi + 5)]), Some(hi), "the client's newest");
    assert_eq!(negotiate(PRODUCE, &[other, (PRODUCE, 0, lo)]), Some(lo), "the broker's newest");
    assert_eq!(negotiate(PRODUCE, &[other, (PRODUCE, hi + 1, hi + 5)]), None, "ranges apart");
    assert_eq!(negotiate(PRODUCE, &[other]), None, "the API not spoken");
    let &(_, flo, fhi, _) = VERSIONS.iter().find(|v| v.0 == FETCH).unwrap();
    assert_eq!(negotiate(FETCH, &[(PRODUCE, 0, 100), (FETCH, flo, fhi)]), Some(fhi), "Fetch's own range");
}

/// A metadata answer's broker and topic, looked up by id and name: that one, or none.
#[test]
fn metadata_finds_its_broker_and_topic_by_id_and_name() {
    let b = |id| Broker { id, host: format!("h{id}"), port: 9092 };
    let t = |name: &str| TopicMeta { name: name.into(), error: Code::NONE, partitions: vec![] };
    let md = Metadata { brokers: vec![b(1), b(2)], topics: vec![t("a"), t("b")], ..Metadata::default() };
    assert_eq!(md.broker(2).map(|b| b.host.as_str()), Some("h2"));
    assert!(md.broker(3).is_none());
    assert_eq!(md.topic("b").map(|t| t.name.as_str()), Some("b"));
    assert!(md.topic("c").is_none());
}
