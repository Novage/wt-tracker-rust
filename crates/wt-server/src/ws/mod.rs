//! Own WebSocket implementation ("native" transport): sans-IO framing (`codec`) and the
//! connection driver (`driver`) reading into one shared buffer per worker thread.

pub(crate) mod codec;
pub(crate) mod driver;
