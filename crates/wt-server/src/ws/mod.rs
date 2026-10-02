//! Own WebSocket implementation ("native" transport): sans-IO framing (`codec`), permessage-deflate
//! (`deflate`) and the connection driver (`driver`) reading into one shared buffer per worker
//! thread.

pub(crate) mod codec;
pub(crate) mod deflate;
pub(crate) mod driver;
