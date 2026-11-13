//! A broker's responses, whatever bytes they hold (brrrrr-kafka's `wire`): each decoder fails or
//! decodes, never panics, and allocates no more than its bytes allow (a length past the end is
//! refused before anything is allocated for it). The first byte picks the decoder.
#![no_main]
use fastka::wire::{self, Cur};

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Some((&which, b)) = data.split_first() else { return };
    let c = Cur::new(b);
    let _ = match which % 13 {
        0 => wire::metadata(c).map(drop),
        1 => wire::fetch(c).map(drop),
        2 => wire::list_offsets(c).map(drop),
        3 => wire::produce(c, 7).map(drop),
        4 => wire::produce(c, 8).map(drop),
        5 => wire::init_producer_id(c).map(drop),
        6 => wire::create_topics(c).map(drop),
        7 => wire::describe_configs(c).map(drop),
        8 => wire::find_coordinator(c).map(drop),
        9 => wire::offset_commit(c).map(drop),
        10 => wire::sasl_handshake(c).map(drop),
        11 => wire::sasl_authenticate(c).map(drop),
        _ => wire::api_versions(c).map(drop),
    };
});
