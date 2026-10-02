#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| wt_proto::fuzz::protocol(data));
