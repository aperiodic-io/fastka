//! A Fetch's records, whatever bytes they hold (fastka's `records`): read uncommitted or
//! committed, the CRC checked or not, each record's headers walked, never a panic; and what a
//! batch written here holds reads back the same.
#![no_main]
use fastka::records::{self, Out, ReadOpts, Stamp};

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Some((&flags, b)) = data.split_first() else { return };
    let aborted = [(1, 0), (7, 3)];
    let o = ReadOpts {
        from: (flags >> 4) as i64,
        committed: flags & 1 != 0,
        aborted: &aborted,
        check_crcs: flags & 2 != 0,
        max_decompressed: 1 << 20,
    };
    let mut plain = vec![];
    let _ = records::read(b, &o, &mut plain, |r| {
        let _ = r.headers().count();
    });
    let _ = records::scan(b, flags & 2 != 0);
    // the bytes as records of a batch: written, then read back the same
    let values: Vec<&[u8]> = b.chunks(7).collect();
    if values.is_empty() {
        return;
    }
    let outs: Vec<Out<'_>> = values.iter().map(|v| Out { timestamp: 1, key: None, value: Some(v), headers: &[] }).collect();
    let mut out = vec![];
    let stamp = Stamp { producer_id: 1, epoch: 0, base_sequence: 0 };
    records::write(&mut out, &outs, stamp, (flags % 5) as i16, &mut vec![]).expect("a batch");
    let mut got = vec![];
    records::read(&out, &ReadOpts { check_crcs: true, ..Default::default() }, &mut plain, |r| got.push(r.value.unwrap().to_vec()))
        .expect("what was written");
    assert_eq!(got, values);
});
