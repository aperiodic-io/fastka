//! Record batches, magic v2 (Kafka 0.11+, `DefaultRecordBatch.java`, `DefaultRecord.java`):
//! read where they lie in a Fetch response, and written for a Produce request.
//!
//! A batch: base offset (8), length (4), partition leader epoch (4), magic (1), CRC-32C (4) of
//! what follows it, attributes (2: bits 0-2 the codec, 3 LogAppendTime, 4 transactional,
//! 5 control), last offset delta (4), base and max timestamp (8 + 8), producer id (8), epoch (2),
//! base sequence (4), the record count (4), then the records, compressed as a whole.
//! A record: its length, attributes (1), timestamp delta, offset delta, key, value and headers,
//! lengths as zigzag varints, -1 for null.
use crate::error::{Error, Result};
use crate::wire::{put_var, Cur, Put};
use std::io::{Read, Write};

/// The size of a batch's header, base offset to record count.
pub const HEADER: usize = 61;
const CRC_FROM: usize = 21; // the CRC covers the attributes onwards

pub const NONE: i16 = 0;
pub const GZIP: i16 = 1;
pub const SNAPPY: i16 = 2;
pub const LZ4: i16 = 3;
pub const ZSTD: i16 = 4;

const LOG_APPEND_TIME: i16 = 0x08;
const TRANSACTIONAL: i16 = 0x10;
const CONTROL: i16 = 0x20;

/// A batch's header, its records not read yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Batch<'a> {
    pub base_offset: i64,
    pub last_offset_delta: i32,
    pub attributes: i16,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub count: i32,
    pub crc: u32,
    /// attributes onwards: what the CRC covers
    pub checked: &'a [u8],
    /// the records, compressed if the attributes say
    pub records: &'a [u8],
    pub magic: i8,
}

impl Batch<'_> {
    pub fn next_offset(&self) -> i64 {
        self.base_offset + self.last_offset_delta as i64 + 1
    }
    pub fn codec(&self) -> i16 {
        self.attributes & 7
    }
    pub fn transactional(&self) -> bool {
        self.attributes & TRANSACTIONAL != 0
    }
    pub fn control(&self) -> bool {
        self.attributes & CONTROL != 0
    }
}

/// The whole batches of `b` (a Fetch's records): a response cut at its max bytes ends in part
/// of one, which is left for the next Fetch. An error: a batch that cannot be one.
pub fn batches(mut b: &[u8]) -> impl Iterator<Item = Result<Batch<'_>>> {
    std::iter::from_fn(move || {
        let len = i32::from_be_bytes(b.get(8..12)?.try_into().ok()?);
        // a whole batch has its header, the record count's 4 bytes included
        if len < (HEADER - 12) as i32 {
            // a v0/v1 message set's length can be that short: say so rather than misread it
            let magic = b.get(16).copied();
            b = &[];
            return Some(Err(match magic {
                Some(m) if m < 2 => old_magic(m as i8),
                _ => Error::Protocol(format!("a record batch of length {len}")),
            }));
        }
        let all = b.get(..12 + len as usize)?;
        b = &b[all.len()..];
        Some(parse_header(all))
    })
}

fn old_magic(m: i8) -> Error {
    Error::Unsupported(format!(
        "a message set of magic {m}: this client reads record batches v2 (Kafka 0.11 and later)"
    ))
}

fn parse_header(all: &[u8]) -> Result<Batch<'_>> {
    let mut c = Cur::new(all);
    let base_offset = c.i64()?;
    c.i32()?; // length
    c.i32()?; // partition leader epoch
    let magic = c.i8()?;
    if magic != 2 {
        return Err(old_magic(magic));
    }
    let crc = c.u32()?;
    let attributes = c.i16()?;
    let last_offset_delta = c.i32()?;
    let base_timestamp = c.i64()?;
    let max_timestamp = c.i64()?;
    let producer_id = c.i64()?;
    c.i16()?; // epoch
    c.i32()?; // base sequence
    let count = c.i32()?;
    Ok(Batch {
        base_offset,
        last_offset_delta,
        attributes,
        base_timestamp,
        max_timestamp,
        producer_id,
        count,
        crc,
        checked: &all[CRC_FROM..],
        records: &all[HEADER..],
        magic,
    })
}

/// The offset after the last whole batch of `records`, and the records they hold, from the
/// batches' headers alone (the next Fetch is sent from there before these are parsed).
pub fn scan(records: &[u8]) -> (Option<i64>, usize) {
    let (mut next, mut count) = (None, 0);
    for b in batches(records) {
        let Ok(b) = b else { break };
        next = Some(b.next_offset());
        count += b.count.max(0) as usize;
    }
    (next, count)
}

/// A record, its key, value and headers where they lie (in the response, or in the batch's
/// decompressed records).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record<'a> {
    pub offset: i64,
    /// ms: the producer's (CreateTime) or the broker's (LogAppendTime)
    pub timestamp: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    headers: &'a [u8],
    header_count: usize,
}

impl<'a> Record<'a> {
    /// Its headers: (key, value or None for null).
    pub fn headers(&self) -> impl Iterator<Item = (&'a str, Option<&'a [u8]>)> + 'a {
        let mut c = Cur::new(self.headers);
        (0..self.header_count).map_while(move |_| {
            let k = c.var().ok()?;
            let k = std::str::from_utf8(c.take(usize::try_from(k).ok()?).ok()?).ok()?;
            let v = match c.var().ok()? {
                n if n < 0 => None,
                n => Some(c.take(n as usize).ok()?),
            };
            Some((k, v))
        })
    }
    /// The value of its first header named `key`.
    pub fn header(&self, key: &str) -> Option<&'a [u8]> {
        self.headers().find(|(k, _)| *k == key).and_then(|(_, v)| v)
    }
}

/// How a partition's records are read: at or after `from`, uncommitted or committed (then the
/// aborted transactions of the Fetch's answer, (producer id, first offset), are skipped).
#[derive(Clone, Debug, Default)]
pub struct ReadOpts<'a> {
    pub from: i64,
    pub committed: bool,
    pub aborted: &'a [(i64, i64)],
    /// a batch whose CRC-32C does not match is refused (`check.crcs`, on by default here)
    pub check_crcs: bool,
}

/// Every record of `records`' whole batches at or after `o.from`, to `f`; control batches and,
/// reading committed, aborted transactions skipped as Kafka's consumer skips them
/// (`CompletedFetch.nextFetchedRecord`). `plain` holds a batch decompressed (reused).
pub fn read(records: &[u8], o: &ReadOpts<'_>, plain: &mut Vec<u8>, mut f: impl FnMut(Record<'_>)) -> Result<()> {
    let mut aborted: Vec<(i64, i64)> = o.aborted.to_vec();
    aborted.sort_by_key(|a| std::cmp::Reverse(a.1)); // popped from the end, by first offset
    let mut aborting: Vec<i64> = vec![];
    for b in batches(records) {
        let b = b?;
        if o.check_crcs && crc32c::crc32c(b.checked) != b.crc {
            return Err(Error::Protocol(format!(
                "the record batch at offset {}: its CRC does not match",
                b.base_offset
            )));
        }
        if o.committed && b.producer_id >= 0 {
            // the aborted transactions begun by the batch's last offset
            while aborted.last().is_some_and(|a| a.1 < b.next_offset()) {
                let (pid, _) = aborted.pop().expect("one");
                aborting.push(pid);
            }
            if b.control() {
                if is_abort_marker(&b, plain)? {
                    aborting.retain(|p| *p != b.producer_id);
                }
                continue;
            }
            if b.transactional() && aborting.contains(&b.producer_id) {
                continue;
            }
        }
        if b.control() {
            continue; // a transaction's marker, read uncommitted
        }
        let body = decompressed(&b, plain)?;
        let mut c = Cur::new(body);
        for _ in 0..b.count.max(0) {
            let len = c.var()?;
            let mut r = Cur::new(c.take(usize::try_from(len).map_err(|_| bad(&b, "a record's length"))?)?);
            r.i8()?; // attributes
            let ts_delta = r.var()?;
            let offset = b.base_offset + r.var()?;
            let key = nullable(&mut r, &b)?;
            let value = nullable(&mut r, &b)?;
            let header_count = usize::try_from(r.var()?).map_err(|_| bad(&b, "a header count"))?;
            let headers = &r.b[r.at..];
            for _ in 0..header_count {
                // walked once here, so `Record::headers` meets none it cannot read
                let k = usize::try_from(r.var()?).map_err(|_| bad(&b, "a header key's length"))?;
                std::str::from_utf8(r.take(k)?).map_err(|_| bad(&b, "a header key (not UTF-8)"))?;
                nullable(&mut r, &b)?;
            }
            if offset < o.from {
                continue; // before the Fetch's offset (a compressed batch comes whole)
            }
            let timestamp =
                if b.attributes & LOG_APPEND_TIME != 0 { b.max_timestamp } else { b.base_timestamp + ts_delta };
            f(Record { offset, timestamp, key, value, headers, header_count });
        }
    }
    Ok(())
}

fn bad(b: &Batch<'_>, what: &str) -> Error {
    Error::Protocol(format!("the record batch at offset {}: {what} out of range", b.base_offset))
}

fn nullable<'a>(r: &mut Cur<'a>, b: &Batch<'_>) -> Result<Option<&'a [u8]>> {
    match r.var()? {
        -1 => Ok(None),
        n => Ok(Some(r.take(usize::try_from(n).map_err(|_| bad(b, "a length"))?)?)),
    }
}

/// A control batch's marker: its one record's key is (version, type), type 0 an abort.
fn is_abort_marker(b: &Batch<'_>, plain: &mut Vec<u8>) -> Result<bool> {
    let body = decompressed(b, plain)?;
    let mut c = Cur::new(body);
    if b.count < 1 {
        return Ok(false);
    }
    let len = c.var()?;
    let mut r = Cur::new(c.take(usize::try_from(len).map_err(|_| bad(b, "a record's length"))?)?);
    r.i8()?;
    r.var()?;
    r.var()?;
    let key = nullable(&mut r, b)?.ok_or_else(|| bad(b, "a control record's key"))?;
    let mut k = Cur::new(key);
    k.i16()?; // version
    Ok(k.i16()? == 0)
}

/// The batch's records, decompressed into `plain` if they are compressed.
fn decompressed<'r: 'p, 'p>(b: &Batch<'r>, plain: &'p mut Vec<u8>) -> Result<&'p [u8]> {
    if b.codec() == NONE {
        return Ok(b.records);
    }
    plain.clear();
    decompress(b.codec(), b.records, plain).map_err(|e| {
        Error::Protocol(format!("the record batch at offset {}, codec {}: {e}", b.base_offset, b.codec()))
    })?;
    Ok(plain)
}

/// Records compressed by `codec` (1 gzip, 2 snappy, 3 lz4, 4 zstd), into `out`.
pub fn decompress(codec: i16, b: &[u8], out: &mut Vec<u8>) -> std::result::Result<(), String> {
    let e = |e: &dyn std::fmt::Display| e.to_string();
    match codec {
        GZIP => drop(flate2::read::MultiGzDecoder::new(b).read_to_end(out).map_err(|x| e(&x))?),
        // Java's producers frame snappy as xerial's snappy-java does: a magic, then blocks
        SNAPPY if b.starts_with(b"\x82SNAPPY\x00") => {
            let mut c = Cur::new(b);
            c.take(16).map_err(|x| e(&x))?; // magic, version, compatible version
            let mut d = snap::raw::Decoder::new();
            while c.left() > 0 {
                let n = c.i32().map_err(|x| e(&x))?;
                let block = c.take(usize::try_from(n).map_err(|_| "a snappy block's length")?).map_err(|x| e(&x))?;
                out.extend(d.decompress_vec(block).map_err(|x| e(&x))?);
            }
        }
        SNAPPY => out.extend(snap::raw::Decoder::new().decompress_vec(b).map_err(|x| e(&x))?),
        LZ4 => drop(lz4_flex::frame::FrameDecoder::new(b).read_to_end(out).map_err(|x| e(&x))?),
        ZSTD => zstd::stream::copy_decode(b, &mut *out).map_err(|x| e(&x))?,
        c => return Err(format!("compression {c}: not one Kafka writes")),
    }
    Ok(())
}

/// `plain` compressed by `codec` into `out`, as Kafka's own producer writes each (snappy raw,
/// which xerial's reader takes too; lz4 in frames of independent 64 KB blocks, the only kind
/// Kafka's `Lz4BlockInputStream` reads).
pub fn compress(codec: i16, plain: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let io = |e: std::io::Error| Error::Config(format!("compressing: {e}"));
    match codec {
        NONE => out.extend(plain),
        GZIP => {
            let mut w = flate2::write::GzEncoder::new(&mut *out, flate2::Compression::default());
            w.write_all(plain).map_err(io)?;
            w.finish().map_err(io)?;
        }
        SNAPPY => out.extend(snap::raw::Encoder::new().compress_vec(plain).map_err(|e| Error::Config(e.to_string()))?),
        LZ4 => {
            let info = lz4_flex::frame::FrameInfo::new()
                .block_mode(lz4_flex::frame::BlockMode::Independent)
                .block_size(lz4_flex::frame::BlockSize::Max64KB);
            let mut w = lz4_flex::frame::FrameEncoder::with_frame_info(info, &mut *out);
            w.write_all(plain).map_err(io)?;
            w.finish().map_err(|e| Error::Config(format!("compressing: {e}")))?;
        }
        ZSTD => zstd::stream::copy_encode(plain, &mut *out, 3).map_err(io)?,
        c => return Err(Error::Config(format!("compression {c}: not one Kafka reads"))),
    }
    Ok(())
}

/// A record to write: its timestamp (ms), key, value and headers.
#[derive(Clone, Copy, Debug)]
pub struct Out<'a> {
    pub timestamp: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [(String, Option<Vec<u8>>)],
}

/// The idempotent producer's stamp on a batch: its id, epoch and the first record's sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub producer_id: i64,
    pub epoch: i16,
    pub base_sequence: i32,
}

/// A batch of `records` (at least one) into `out`, base offset 0 (the broker assigns it), its
/// records compressed by `codec`; `scratch` is reused.
pub fn write(out: &mut Vec<u8>, records: &[Out<'_>], stamp: Stamp, codec: i16, scratch: &mut Vec<u8>) -> Result<()> {
    assert!(!records.is_empty(), "a batch holds a record");
    let base_ts = records[0].timestamp;
    let max_ts = records.iter().map(|r| r.timestamp).max().unwrap_or(base_ts);
    scratch.clear();
    let mut rec = vec![];
    for (i, r) in records.iter().enumerate() {
        rec.clear();
        rec.push(0); // attributes
        put_var(&mut rec, r.timestamp - base_ts);
        put_var(&mut rec, i as i64);
        for b in [r.key, r.value] {
            match b {
                Some(b) => {
                    put_var(&mut rec, b.len() as i64);
                    rec.extend(b);
                }
                None => put_var(&mut rec, -1),
            }
        }
        put_var(&mut rec, r.headers.len() as i64);
        for (k, v) in r.headers {
            put_var(&mut rec, k.len() as i64);
            rec.extend(k.as_bytes());
            match v {
                Some(v) => {
                    put_var(&mut rec, v.len() as i64);
                    rec.extend(v);
                }
                None => put_var(&mut rec, -1),
            }
        }
        put_var(scratch, rec.len() as i64);
        scratch.extend(&rec);
    }
    out.clear();
    out.i64(0); // base offset
    out.i32(0); // length, below
    out.i32(-1); // partition leader epoch
    out.i8(2);
    out.i32(0); // crc, below
    out.i16(codec);
    out.i32(records.len() as i32 - 1);
    out.i64(base_ts);
    out.i64(max_ts);
    out.i64(stamp.producer_id);
    out.i16(stamp.epoch);
    out.i32(stamp.base_sequence);
    out.i32(records.len() as i32);
    compress(codec, scratch, out)?;
    let len = i32::try_from(out.len() - 12).map_err(|_| Error::Config("a batch over 2 GiB".into()))?;
    out[8..12].copy_from_slice(&len.to_be_bytes());
    let crc = crc32c::crc32c(&out[CRC_FROM..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());
    Ok(())
}

#[cfg(test)]
mod tests;
