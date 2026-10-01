#![allow(dead_code)]

use wt_core::{ConnId, Shard};
use wt_proto::{Backend, Encoder, Message, ProtoError, SerdeJson};

pub type ParseFn = for<'a> fn(&'a [u8]) -> Result<Message<'a>, ProtoError>;
pub type HandleFn = fn(&mut Shard, u32, ConnId, &[u8], &mut Encoder) -> Result<(), ProtoError>;

/// Every compiled-in parser backend.
pub fn backends() -> Vec<(&'static str, ParseFn, HandleFn)> {
    #[allow(unused_mut)]
    let mut v: Vec<(&'static str, ParseFn, HandleFn)> = vec![(
        "serde_json",
        <SerdeJson as Backend>::parse,
        wt_proto::handle_with::<SerdeJson>,
    )];
    #[cfg(feature = "sonic")]
    v.push((
        "sonic",
        <wt_proto::Sonic as Backend>::parse,
        wt_proto::handle_with::<wt_proto::Sonic>,
    ));
    v
}

/// Messages of an encoder as `(conn, text)`.
pub fn texts(out: &Encoder) -> Vec<(u64, String)> {
    out.messages()
        .map(|(to, m)| (to.0, String::from_utf8(m.to_vec()).unwrap()))
        .collect()
}
