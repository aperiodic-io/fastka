//! The benchmark gate's workloads: the client's own work on the hot paths, single threaded and
//! deterministic, so an instruction count (`bench/gate/gate.py`, Valgrind) measures them to well
//! under 0.1% and a regression of a few percent is seen on any runner. What runs on the network,
//! the broker or other threads is the stress harness's and `bench/compare`'s to measure.
//!
//! `gate <workload>` runs one; `gate list` names them; `gate noop` is the baseline subtracted.
use fastka::records::{self, Out, ReadOpts, Stamp};
use fastka::wire::{self, Cur};
use std::hint::black_box;

const RECORDS: usize = 10_000;
const ROUNDS: usize = 20;

fn values() -> Vec<Vec<u8>> {
    (0..RECORDS)
        .map(|i| {
            let mut v = format!("{{\"seq\":{i},\"px\":{}.{:02},\"side\":\"{}\"}}", 1000 + i % 97, i % 100, i % 2).into_bytes();
            v.resize(100, b' ');
            v
        })
        .collect()
}

fn batch(codec: i16, values: &[Vec<u8>]) -> Vec<u8> {
    let hdrs = [("trace".to_string(), Some(b"0123456789abcdef".to_vec()))];
    let outs: Vec<Out<'_>> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Out { timestamp: 1_700_000_000_000 + i as i64, key: Some(&v[..8]), value: Some(v), headers: &hdrs })
        .collect();
    let mut out = vec![];
    // a Fetch's records: batches of 1000 records, as a producer of batch.num.messages=1000 writes
    for (k, chunk) in outs.chunks(1000).enumerate() {
        let mut b = vec![];
        records::write(&mut b, chunk, Stamp { producer_id: 7, epoch: 0, base_sequence: (k * 1000) as i32 }, codec, &mut vec![])
            .unwrap();
        b[..8].copy_from_slice(&((k * 1000) as i64).to_be_bytes()); // base offset, as the broker sets it
        out.extend(b);
    }
    out
}

fn read(codec: i16) {
    let b = batch(codec, &values());
    let o = ReadOpts { check_crcs: true, ..Default::default() };
    let mut plain = vec![];
    for _ in 0..ROUNDS {
        let mut sum = 0usize;
        records::read(black_box(&b), &o, &mut plain, |r| sum += r.value.map_or(0, <[u8]>::len) + r.headers().count())
            .unwrap();
        assert_eq!(sum, RECORDS * 101);
    }
}

fn write(codec: i16) {
    let v = values();
    for _ in 0..ROUNDS / 4 {
        black_box(batch(codec, black_box(&v)));
    }
}

/// A Fetch response of 12 partitions, each 64 KB of records, decoded (not the records).
fn fetch_decode() {
    let records = batch(records::NONE, &values()[..600]);
    let mut body = vec![];
    let put = |b: &mut Vec<u8>, x: &[u8]| b.extend_from_slice(x);
    put(&mut body, &0i32.to_be_bytes()); // throttle
    put(&mut body, &0i16.to_be_bytes()); // error
    put(&mut body, &0i32.to_be_bytes()); // session
    put(&mut body, &3i32.to_be_bytes()); // topics
    for t in ["l1-book", "trades", "quotes"] {
        put(&mut body, &(t.len() as i16).to_be_bytes());
        put(&mut body, t.as_bytes());
        put(&mut body, &4i32.to_be_bytes());
        for p in 0..4i32 {
            put(&mut body, &p.to_be_bytes());
            put(&mut body, &0i16.to_be_bytes());
            for _ in 0..3 {
                put(&mut body, &1_000_000i64.to_be_bytes()); // high, last stable, log start
            }
            put(&mut body, &(-1i32).to_be_bytes()); // aborted: null
            put(&mut body, &(-1i32).to_be_bytes()); // preferred replica
            put(&mut body, &(records.len() as i32).to_be_bytes());
            put(&mut body, &records);
        }
    }
    for _ in 0..ROUNDS * 50 {
        let f = wire::fetch(Cur::new(black_box(&body))).unwrap();
        assert_eq!(f.parts.len(), 12);
        black_box(f);
    }
}

const WORKLOADS: &[(&str, fn())] = &[
    ("noop", || {}),
    ("read_none", || read(records::NONE)),
    ("read_gzip", || read(records::GZIP)),
    ("read_snappy", || read(records::SNAPPY)),
    ("read_lz4", || read(records::LZ4)),
    ("read_zstd", || read(records::ZSTD)),
    ("write_none", || write(records::NONE)),
    ("write_lz4", || write(records::LZ4)),
    ("write_zstd", || write(records::ZSTD)),
    ("fetch_decode", fetch_decode),
];

fn main() {
    let arg = std::env::args().nth(1).unwrap_or_else(|| "list".into());
    if arg == "list" || arg == "--bench" {
        WORKLOADS.iter().for_each(|(n, _)| println!("{n}"));
        return;
    }
    let (_, f) = WORKLOADS.iter().find(|(n, _)| *n == arg).unwrap_or_else(|| panic!("no workload {arg}"));
    f();
}
