#![allow(clippy::type_complexity)] // the tests' tuples say what each field is where they are made
//! The record batch codec against the spec's: batches this crate writes are read by
//! `kafka-protocol`'s decoder (which checks their CRC-32C), batches it writes are read here, in
//! every codec; then what Kafka's consumer skips, and what no bytes may do.
use super::*;
use bytes::{Bytes, BytesMut};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records as kr;

const CODECS: [i16; 5] = [NONE, GZIP, SNAPPY, LZ4, ZSTD];

fn oracle_codec(c: i16) -> kr::Compression {
    [kr::Compression::None, kr::Compression::Gzip, kr::Compression::Snappy, kr::Compression::Lz4, kr::Compression::Zstd]
        [c as usize]
}

fn stamp() -> Stamp {
    Stamp { producer_id: 4242, epoch: 3, base_sequence: 17 }
}

fn all(
    records: &[u8],
    o: &ReadOpts<'_>,
) -> Result<Vec<(i64, i64, Option<Vec<u8>>, Option<Vec<u8>>, Vec<(String, Option<Vec<u8>>)>)>> {
    let mut out = vec![];
    read(records, o, &mut vec![], |r| {
        let hs = r.headers().map(|(k, v)| (k.to_string(), v.map(<[u8]>::to_vec))).collect();
        out.push((r.offset, r.timestamp, r.key.map(<[u8]>::to_vec), r.value.map(<[u8]>::to_vec), hs))
    })?;
    Ok(out)
}

fn opts() -> ReadOpts<'static> {
    ReadOpts { check_crcs: true, ..Default::default() }
}

/// A batch written here as the broker stores it: its base offset set.
fn at(mut b: Vec<u8>, base: i64) -> Vec<u8> {
    b[..8].copy_from_slice(&base.to_be_bytes());
    b
}

fn written(codec: i16, records: &[Out<'_>]) -> Vec<u8> {
    let mut out = vec![];
    write(&mut out, records, stamp(), codec, &mut vec![]).unwrap();
    out
}

#[test]
fn what_this_writes_the_spec_reads_in_every_codec() {
    let h = vec![("dedup".to_string(), Some(b"w|1".to_vec())), ("n".to_string(), None)];
    let recs = [
        Out { timestamp: 1000, key: None, value: Some(b"first"), headers: &h },
        Out { timestamp: 990, key: Some(b"k"), value: None, headers: &[] },
        Out { timestamp: 1500, key: Some(b""), value: Some(&[7u8; 300]), headers: &[] },
    ];
    for codec in CODECS {
        let b = written(codec, &recs);
        let set =
            kr::RecordBatchDecoder::decode(&mut Bytes::from(b.clone())).unwrap_or_else(|e| panic!("{codec}: {e}"));
        assert_eq!(set.compression, oracle_codec(codec));
        let r = &set.records;
        assert_eq!(r.len(), 3);
        assert_eq!((r[0].offset, r[1].offset, r[2].offset), (0, 1, 2));
        assert_eq!((r[0].timestamp, r[1].timestamp, r[2].timestamp), (1000, 990, 1500), "deltas may be negative");
        assert_eq!((r[0].producer_id, r[0].producer_epoch), (4242, 3));
        assert_eq!((r[0].sequence, r[2].sequence), (17, 19));
        assert!(!r[0].transactional && !r[0].control);
        assert_eq!(r[0].key, None);
        assert_eq!(r[0].value.as_deref(), Some(&b"first"[..]));
        assert_eq!(
            r[0].headers.get(&StrBytes::from_static_str("dedup")).cloned().flatten().as_deref(),
            Some(&b"w|1"[..])
        );
        assert_eq!(r[0].headers.get(&StrBytes::from_static_str("n")), Some(&None), "a null header value");
        assert_eq!((r[1].key.as_deref(), r[1].value.as_deref()), (Some(&b"k"[..]), None), "a tombstone");
        assert_eq!(r[2].key.as_deref(), Some(&b""[..]), "an empty key is not a null one");
        // and read back here
        let got = all(&at(b, 50), &opts()).unwrap();
        assert_eq!(got.iter().map(|g| g.0).collect::<Vec<_>>(), [50, 51, 52]);
        assert_eq!(got[0].4, h);
        assert_eq!(got[1].3, None);
    }
}

#[test]
fn what_the_spec_writes_this_reads_in_every_codec() {
    let rec = |offset, ts, v: &'static [u8]| kr::Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: 5,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kr::TimestampType::Creation,
        offset,
        sequence: -1,
        timestamp: ts,
        key: None,
        value: Some(Bytes::from_static(v)),
        headers: [(StrBytes::from_static_str("h"), Some(Bytes::from_static(b"x")))].into_iter().collect(),
    };
    for codec in CODECS {
        let recs = [rec(100, 7, b"a"), rec(101, 9, b"bb"), rec(102, 8, b"")];
        let mut b = BytesMut::new();
        kr::RecordBatchEncoder::encode(
            &mut b,
            &recs,
            &kr::RecordEncodeOptions { version: 2, compression: oracle_codec(codec) },
        )
        .unwrap();
        let got = all(&b, &opts()).unwrap_or_else(|e| panic!("{codec}: {e}"));
        assert_eq!(
            got,
            [
                (100, 7, None, Some(b"a".to_vec()), vec![("h".into(), Some(b"x".to_vec()))]),
                (101, 9, None, Some(b"bb".to_vec()), vec![("h".into(), Some(b"x".to_vec()))]),
                (102, 8, None, Some(vec![]), vec![("h".into(), Some(b"x".to_vec()))]),
            ],
            "{codec}"
        );
        assert_eq!(scan(&b), (Some(103), 3));
    }
}

/// Kafka's own LZ4 reader takes only frames of independent blocks of 64 KB to 4 MB
/// (`Lz4BlockInputStream`, `Lz4BlockOutputStream.FLG.validate`): what this writes is one.
#[test]
fn lz4_frames_are_the_kind_kafka_reads() {
    let b = written(LZ4, &[Out { timestamp: 0, key: None, value: Some(&[1; 100_000]), headers: &[] }]);
    let frame = &b[HEADER..];
    assert_eq!(&frame[..4], &0x184D2204u32.to_le_bytes(), "the frame's magic");
    let (flg, bd) = (frame[4], frame[5]);
    assert_eq!(flg >> 6, 1, "version 01");
    assert_ne!(flg & 0x20, 0, "independent blocks");
    assert_eq!(flg & 0x03, 0, "reserved bits");
    assert!((4..=7).contains(&((bd >> 4) & 7)), "block size 64 KB to 4 MB");
}

/// A Fetch's records end in part of a batch where they reach the Fetch's max bytes: it is left
/// for the next Fetch, from the offset after the whole ones.
#[test]
fn a_batch_cut_short_is_left_for_the_next_fetch() {
    let one = at(written(NONE, &[Out { timestamp: 1, key: None, value: Some(b"v"), headers: &[] }; 2]), 10);
    let two = at(written(ZSTD, &[Out { timestamp: 1, key: None, value: Some(b"w"), headers: &[] }; 3]), 12);
    let both = [one.clone(), two.clone()].concat();
    for cut in [one.len() + 1, one.len() + 11, one.len() + 12, both.len() - 1] {
        assert_eq!(scan(&both[..cut]), (Some(12), 2), "cut at {cut}");
        assert_eq!(all(&both[..cut], &opts()).unwrap().len(), 2, "cut at {cut}");
    }
    assert_eq!(scan(&both), (Some(15), 5));
    assert_eq!(scan(&both[..5]), (None, 0), "not even a whole header");
}

/// A compressed batch comes whole, from its base offset: the records before the Fetch's offset
/// are not given again.
#[test]
fn records_before_the_fetchs_offset_are_skipped() {
    let b = at(written(GZIP, &[Out { timestamp: 1, key: None, value: Some(b"v"), headers: &[] }; 5]), 20);
    let got = all(&b, &ReadOpts { from: 23, ..opts() }).unwrap();
    assert_eq!(got.iter().map(|g| g.0).collect::<Vec<_>>(), [23, 24]);
}

/// With LogAppendTime, every record's timestamp is the broker's, the batch's max timestamp.
#[test]
fn log_append_time_is_the_batchs_max_timestamp() {
    let mut b = written(NONE, &[Out { timestamp: 5, key: None, value: Some(b"v"), headers: &[] }; 2]);
    b[21..23].copy_from_slice(&LOG_APPEND_TIME.to_be_bytes());
    b[35..43].copy_from_slice(&9_999i64.to_be_bytes()); // max timestamp
    let crc = crc32c::crc32c(&b[CRC_FROM..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    assert_eq!(all(&b, &opts()).unwrap().iter().map(|g| g.1).collect::<Vec<_>>(), [9_999, 9_999]);
}

/// A batch changed after it was written is refused (its CRC-32C), not read as other records.
#[test]
fn a_corrupt_batch_is_refused() {
    let b = written(NONE, &[Out { timestamp: 5, key: None, value: Some(b"payload"), headers: &[] }]);
    for i in CRC_FROM..b.len() {
        let mut c = b.clone();
        c[i] ^= 0x40;
        assert!(all(&c, &opts()).is_err(), "byte {i} flipped");
    }
    let mut c = b.clone();
    c[b.len() - 3] ^= 1;
    assert!(all(&c, &ReadOpts { check_crcs: false, ..opts() }).is_ok(), "unchecked, it is read as it is");
}

/// A v0/v1 message set (Kafka before 0.11) is refused, saying so.
#[test]
fn old_message_formats_are_refused() {
    let mut b = written(NONE, &[Out { timestamp: 5, key: None, value: Some(b"v"), headers: &[] }]);
    b[16] = 1;
    assert!(matches!(all(&b, &opts()), Err(Error::Unsupported(m)) if m.contains("magic 1")));
}

/// A transaction's batches, and its marker: (producer id, transactional, control, abort).
fn txn(base: i64, pid: i64, attrs: i16, values: &[&[u8]]) -> Vec<u8> {
    let recs: Vec<Out<'_>> =
        values.iter().map(|v| Out { timestamp: 1, key: None, value: Some(v), headers: &[] }).collect();
    let mut b = at(written(NONE, &recs), base);
    b[21..23].copy_from_slice(&attrs.to_be_bytes());
    b[43..51].copy_from_slice(&pid.to_be_bytes());
    let crc = crc32c::crc32c(&b[CRC_FROM..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    b
}

fn marker(base: i64, pid: i64, abort: bool) -> Vec<u8> {
    let key = [0u8, 0, 0, if abort { 0 } else { 1 }];
    let recs = [Out { timestamp: 1, key: Some(&key), value: Some(&[0, 0, 0, 0, 0, 0]), headers: &[] }];
    let mut b = at(written(NONE, &recs), base);
    b[21..23].copy_from_slice(&(TRANSACTIONAL | CONTROL).to_be_bytes());
    b[43..51].copy_from_slice(&pid.to_be_bytes());
    let crc = crc32c::crc32c(&b[CRC_FROM..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    b
}

/// Reading committed, an aborted transaction's records are skipped and a committed one's read,
/// whichever interleaves with which, as Kafka's consumer reads them; the markers never are.
/// Reading uncommitted, both are read and the markers skipped.
#[test]
fn reading_committed_skips_aborted_transactions_as_kafka_does() {
    let log = [
        txn(0, 7, TRANSACTIONAL, &[b"a1", b"a2"]), // producer 7, aborted below
        txn(2, 8, TRANSACTIONAL, &[b"c1"]),        // producer 8, committed below
        txn(3, -1, 0, &[b"plain"]),                // not transactional
        marker(4, 7, true),
        txn(5, 7, TRANSACTIONAL, &[b"n1"]), // producer 7 again, a new transaction, committed
        marker(6, 8, false),
        marker(7, 7, false),
    ]
    .concat();
    let aborted = [(7, 0)];
    let vals = |o: &ReadOpts<'_>| -> Vec<Vec<u8>> { all(&log, o).unwrap().into_iter().map(|g| g.3.unwrap()).collect() };
    let committed = ReadOpts { committed: true, aborted: &aborted, ..opts() };
    assert_eq!(vals(&committed), [b"c1".to_vec(), b"plain".to_vec(), b"n1".to_vec()]);
    assert_eq!(vals(&opts()).len(), 5, "uncommitted: all but the markers");
    assert_eq!(scan(&log).0, Some(8), "the markers' offsets are read past");
}

proptest::proptest! {
    /// Any records written here read back the same, whatever the codec.
    #[test]
    fn records_round_trip(
        vals in proptest::collection::vec((proptest::option::of(proptest::collection::vec(0u8..=255, 0..40)), -5000i64..5000), 1..30),
        codec in 0i16..5,
    ) {
        let recs: Vec<Out<'_>> = vals.iter().map(|(v, t)| Out { timestamp: 1_700_000_000_000 + t, key: None, value: v.as_deref(), headers: &[] }).collect();
        let got = all(&written(codec, &recs), &opts()).unwrap();
        proptest::prop_assert_eq!(got.len(), vals.len());
        for (g, (v, t)) in got.iter().zip(&vals) {
            proptest::prop_assert_eq!(&g.3, v);
            proptest::prop_assert_eq!(g.1, 1_700_000_000_000 + t);
        }
    }

    /// No bytes make the reader panic, the CRC checked or not.
    #[test]
    fn no_bytes_panic(b in proptest::collection::vec(0u8..=255, 0..400), check in proptest::bool::ANY) {
        let _ = all(&b, &ReadOpts { check_crcs: check, committed: true, aborted: &[(1, 0)], from: 0 });
        let _ = scan(&b);
    }

    /// A valid batch with any one byte changed, its CRC not checked, never panics either: the
    /// lengths inside it are checked as the response's are.
    #[test]
    fn a_batch_changed_anywhere_does_not_panic(i in 0usize..200, x in 1u8..=255, codec in 0i16..5) {
        let h = vec![("k".to_string(), Some(b"v".to_vec()))];
        let mut b = written(codec, &[Out { timestamp: 1, key: Some(b"key"), value: Some(b"value"), headers: &h }; 3]);
        let i = i % b.len();
        b[i] ^= x;
        let _ = all(&b, &ReadOpts { check_crcs: false, ..opts() });
    }
}

/// A record's headers by name: the first of a name, a null value as none, a name absent none.
#[test]
fn a_header_is_found_by_its_name() {
    let h = vec![
        ("a".to_string(), Some(b"1".to_vec())),
        ("n".into(), None),
        ("a".into(), Some(b"2".to_vec())),
        ("e".into(), Some(vec![])),
    ];
    let b = written(NONE, &[Out { timestamp: 5, key: None, value: Some(b"v"), headers: &h }]);
    let mut got = vec![];
    read(&b, &opts(), &mut vec![], |r| {
        got.push((
            r.header("a").map(<[u8]>::to_vec),
            r.header("n").is_some(),
            r.header("x").is_some(),
            r.header("e").map(<[u8]>::to_vec),
        ))
    })
    .unwrap();
    assert_eq!(got, [(Some(b"1".to_vec()), false, false, Some(vec![]))], "an empty value is not a null one");
    assert_eq!(&b[12..16], &(-1i32).to_be_bytes(), "no partition leader epoch: the broker sets it");
}

/// A record whose key length is below -1 (null) is refused, saying so.
#[test]
fn a_negative_length_is_refused() {
    let mut b = written(NONE, &[Out { timestamp: 5, key: None, value: Some(b"v"), headers: &[] }]);
    // the record: its length, attributes, timestamp delta, offset delta, then the key's length
    assert_eq!(b[HEADER + 4], 1, "-1 in zigzag");
    b[HEADER + 4] = 3; // -2
    let e = all(&b, &ReadOpts { check_crcs: false, ..opts() }).unwrap_err();
    assert!(e.to_string().contains("a length out of range"), "{e}");
}

/// A batch emptied by compaction (its header alone, no record) is read past; a length shorter
/// than a header is refused, as a v0/v1 message set when its magic says so.
#[test]
fn an_empty_batch_is_read_past_and_a_short_one_refused() {
    let mut b = written(NONE, &[Out { timestamp: 5, key: None, value: Some(b"v"), headers: &[] }]);
    b.truncate(HEADER);
    b[8..12].copy_from_slice(&(HEADER as i32 - 12).to_be_bytes());
    b[57..61].copy_from_slice(&0i32.to_be_bytes());
    let crc = crc32c::crc32c(&b[CRC_FROM..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    assert_eq!(all(&b, &opts()).unwrap(), []);
    assert_eq!(scan(&b), (Some(1), 0), "the offset past it, no record");
    for (magic, old) in [(0, true), (1, true), (2, false)] {
        let mut s = vec![0u8; 30];
        s[8..12].copy_from_slice(&18i32.to_be_bytes());
        s[16] = magic;
        let e = batches(&s).next().unwrap().err().unwrap();
        match old {
            true => assert!(matches!(&e, Error::Unsupported(m) if m.contains(&format!("magic {magic}"))), "{e}"),
            false => assert!(e.to_string().contains("a record batch of length 18"), "{e}"),
        }
    }
}

/// Reading committed, only transactional batches of an aborted producer are skipped (an
/// idempotent one's are not), and a marker emptied by compaction is read past; uncommitted, the
/// aborted list is not read.
#[test]
fn only_an_aborted_transactions_batches_are_skipped() {
    let mut empty_marker = marker(2, 7, true);
    empty_marker.truncate(HEADER);
    empty_marker[8..12].copy_from_slice(&(HEADER as i32 - 12).to_be_bytes());
    empty_marker[23..27].copy_from_slice(&0i32.to_be_bytes());
    empty_marker[57..61].copy_from_slice(&0i32.to_be_bytes());
    let crc = crc32c::crc32c(&empty_marker[CRC_FROM..]);
    empty_marker[17..21].copy_from_slice(&crc.to_be_bytes());
    let log = [txn(0, 7, TRANSACTIONAL, &[b"t"]), txn(1, 7, 0, &[b"idem"]), empty_marker].concat();
    let aborted = [(7, 0)];
    let vals = |o: &ReadOpts<'_>| -> Vec<Vec<u8>> { all(&log, o).unwrap().into_iter().map(|g| g.3.unwrap()).collect() };
    assert_eq!(vals(&ReadOpts { committed: true, aborted: &aborted, ..opts() }), [b"idem".to_vec()]);
    assert_eq!(vals(&ReadOpts { aborted: &aborted, ..opts() }), [b"t".to_vec(), b"idem".to_vec()]);
}

/// Reading committed, checked against Kafka's rule for every log of up to 8 batches from two
/// transactional producers and a non-transactional one, read from every offset: a record is given
/// iff it is outside a transaction or its transaction was committed; no marker is ever given. The
/// aborted transactions are those a broker's Fetch answer lists: each aborted one ending (its
/// marker) at or after the offset fetched, by its first offset.
#[test]
fn reading_committed_is_kafkas_rule_on_every_small_log() {
    #[derive(Clone, Copy, Debug)]
    enum Ev {
        Data(i64),
        Plain,
        End(i64, bool), // a producer's marker: abort?
    }
    // every sequence of events up to 8 long where a marker ends an open transaction and none is
    // left open (a Fetch stops at the last stable offset, before an open one)
    fn logs(at: Vec<Ev>, open: [bool; 2], out: &mut Vec<Vec<Ev>>) {
        if !open.contains(&true) && !at.is_empty() {
            out.push(at.clone());
        }
        if at.len() == 8 {
            return;
        }
        for p in [1, 2] {
            let mut o = open;
            o[p as usize - 1] = true;
            logs([&at[..], &[Ev::Data(p)]].concat(), o, out);
            if open[p as usize - 1] {
                for abort in [false, true] {
                    let mut o = open;
                    o[p as usize - 1] = false;
                    logs([&at[..], &[Ev::End(p, abort)]].concat(), o, out);
                }
            }
        }
        logs([&at[..], &[Ev::Plain]].concat(), open, out);
    }
    let mut all = vec![];
    logs(vec![], [false, false], &mut all);
    eprintln!("{} logs", all.len());
    assert!(all.len() > 50_000, "{}", all.len());
    for evs in &all {
        let bytes: Vec<u8> = (evs.iter().enumerate())
            .flat_map(|(i, e)| match *e {
                Ev::Data(p) => txn(i as i64, p, TRANSACTIONAL, &[format!("r{i}").as_bytes()]),
                Ev::Plain => txn(i as i64, -1, 0, &[format!("r{i}").as_bytes()]),
                Ev::End(p, abort) => marker(i as i64, p, abort),
            })
            .collect();
        // each transaction: its producer, first offset, marker's offset, aborted?
        let (mut txns, mut first) = (vec![], [None::<i64>; 2]);
        for (i, e) in evs.iter().enumerate() {
            match *e {
                Ev::Data(p) => drop(first[p as usize - 1].get_or_insert(i as i64)),
                Ev::End(p, abort) => txns.push((p, first[p as usize - 1].take().unwrap(), i as i64, abort)),
                Ev::Plain => {}
            }
        }
        let visible = |i: usize| match evs[i] {
            Ev::Plain => true,
            Ev::Data(p) => txns.iter().any(|t| t.0 == p && t.1 <= i as i64 && i as i64 <= t.2 && !t.3),
            Ev::End(..) => false,
        };
        for from in 0..=evs.len() as i64 {
            let aborted: Vec<(i64, i64)> = txns.iter().filter(|t| t.3 && t.2 >= from).map(|t| (t.0, t.1)).collect();
            let o = ReadOpts { from, committed: true, aborted: &aborted, ..opts() };
            let got: Vec<i64> = all_offsets(&bytes, &o);
            let want: Vec<i64> = (from as usize..evs.len()).filter(|i| visible(*i)).map(|i| i as i64).collect();
            assert_eq!(got, want, "{evs:?} from {from}, aborted {aborted:?}");
            // uncommitted: every record, no marker
            let got = all_offsets(&bytes, &ReadOpts { from, ..opts() });
            let want: Vec<i64> =
                (from as usize..evs.len()).filter(|i| !matches!(evs[*i], Ev::End(..))).map(|i| i as i64).collect();
            assert_eq!(got, want, "uncommitted: {evs:?} from {from}");
        }
    }
}

fn all_offsets(b: &[u8], o: &ReadOpts<'_>) -> Vec<i64> {
    let mut out = vec![];
    read(b, o, &mut vec![], |r| out.push(r.offset)).unwrap();
    out
}
