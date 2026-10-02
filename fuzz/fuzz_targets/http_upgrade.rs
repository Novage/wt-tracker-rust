#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| wt_server::fuzz::http_upgrade(data));
