//! Kani proofs (`cargo kani`): properties of the decoders every byte of a broker's response goes
//! through, proved for every input up to the bounds given, not sampled as the property tests and
//! fuzzers do.
use crate::records;
use crate::wire::{self, put_var, Cur};

/// The decoders' error messages, formatted from symbolic integers, are what makes CBMC slow and
/// none of the properties: stubbed to an empty string (`-Z stubbing`).
#[allow(dead_code)] // named by the stubs only
fn no_format(_: std::fmt::Arguments<'_>) -> String {
    String::new()
}

/// A varint written is read back as itself, its every byte consumed, whatever the i64.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
#[kani::unwind(12)]
fn varint_round_trips() {
    let v: i64 = kani::any();
    let mut out = Vec::with_capacity(10);
    put_var(&mut out, v);
    assert!(out.len() <= 10);
    let mut c = Cur::new(&out);
    assert_eq!(c.var().ok(), Some(v));
    assert_eq!(c.left(), 0);
}

/// No 11 bytes make the varint decoder panic or read past 10 of them.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
#[kani::unwind(12)]
fn varint_never_panics_nor_overreads() {
    let b: [u8; 11] = kani::any();
    let n: usize = kani::any_where(|n| *n <= 11);
    let mut c = Cur::new(&b[..n]);
    let r = c.var();
    assert!(c.at <= 10 && c.at <= n);
    if r.is_ok() {
        assert!(c.at >= 1);
    }
}

/// `take` hands back exactly what was asked, within the bytes, or refuses: never a panic, whatever
/// the length (a corrupt one past every response included).
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
fn take_stays_in_bounds() {
    let b: [u8; 8] = kani::any();
    let len: usize = kani::any_where(|l| *l <= 8);
    let at: usize = kani::any_where(|a| *a <= len);
    let n: usize = kani::any();
    let mut c = Cur { b: &b[..len], at };
    match c.take(n) {
        Ok(s) => assert!(s.len() == n && c.at == at + n && c.at <= len),
        Err(_) => assert!(c.at == at && at.checked_add(n).is_none_or(|e| e > len)),
    }
}

/// An array's length is never trusted past the bytes left: `count(min)` admits only what `min`
/// bytes an element could fit, so nothing is allocated for a length a response cannot hold.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
fn array_lengths_are_bounded_by_the_bytes() {
    let b: [u8; 12] = kani::any();
    let min: usize = kani::any_where(|m| *m <= 64);
    let mut c = Cur::new(&b);
    if let Ok(n) = c.count(min) {
        assert!(n.saturating_mul(min.max(1)) <= c.left());
    }
}

/// The version negotiated is the newest both sides speak, and none when their ranges do not meet.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
fn negotiation_picks_the_newest_common_version() {
    let (api, lo, hi, _) = wire::VERSIONS[kani::any_where(|i: &usize| *i < wire::VERSIONS.len())];
    let (bmin, bmax): (i16, i16) = (kani::any(), kani::any());
    kani::assume(bmin <= bmax);
    match wire::negotiate(api, &[(api, bmin, bmax)]) {
        Some(v) => assert!(v >= lo && v <= hi && v >= bmin && v <= bmax && (v == hi || v == bmax)),
        None => assert!(bmax < lo || bmin > hi),
    }
    assert_eq!(wire::negotiate(api, &[]), None, "an API the broker lacks");
}

/// No bytes make the batch iterator panic (an overflowing offset, a length past the end), and every
/// batch it yields lies within the bytes, its records after its 61-byte header.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
#[kani::unwind(3)]
fn batch_headers_never_panic() {
    let b: [u8; 70] = kani::any();
    let n: usize = kani::any_where(|n| *n <= 70);
    for x in records::batches(&b[..n]).take(2) {
        if let Ok(batch) = x {
            assert!(batch.records.len() + records::HEADER <= n);
            let _ = batch.next_offset();
        }
    }
}

/// Every key of up to 9 bytes (two whole words and a tail) hashes without a panic, and lands on a
/// partition the topic has, whatever its count.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
#[kani::unwind(10)]
fn keys_land_on_a_partition_the_topic_has() {
    let key: [u8; 9] = kani::any();
    let len: usize = kani::any_where(|l| *l <= 9);
    let n: usize = kani::any_where(|n| *n >= 1 && *n <= 1 << 20);
    let h = crate::producer::murmur2(&key[..len]);
    assert!(((h & 0x7fffffff) as usize % n) < n);
}
