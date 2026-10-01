//! Sans-IO WebTorrent tracker core: the peers / swarms bookkeeping and offer routing of
//! wt-tracker's `FastTracker`, as a single-threaded [`Shard`].

mod key;
mod request;
mod shard;

pub use key::{Key, MAX_KEY_LEN};
pub use request::{AnnounceEvent, ConnId, NullOutbox, Outbox, Request, ScrapeTarget, TrackerError};
pub use shard::{OfferSelection, Settings, Shard, SwarmStats};
