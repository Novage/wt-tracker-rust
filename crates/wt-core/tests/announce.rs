//! Port of wt-tracker test/announce.test.ts.

mod common;

use common::{Recorder, announce, swarm_peers};
use wt_core::{AnnounceEvent, Settings, Shard};

#[test]
fn should_add_peers_to_swarms_on_announce() {
    let mut shard = Shard::new(Settings::default(), 1);
    let mut out = Recorder::default();
    let (peer0, peer1, peer2, peer3) = (0, 1, 2, 3);

    announce(
        &mut shard,
        &mut out,
        0,
        peer0,
        "swarm1",
        "0",
        AnnounceEvent::Started,
    )
    .unwrap();
    assert_eq!(shard.swarm_count(), 1);
    assert_eq!(swarm_peers(&shard, "swarm1"), ["0"]);

    announce(
        &mut shard,
        &mut out,
        0,
        peer1,
        "swarm1",
        "1",
        AnnounceEvent::None,
    )
    .unwrap();
    assert_eq!(shard.swarm_count(), 1);
    assert_eq!(swarm_peers(&shard, "swarm1"), ["0", "1"]);

    announce(
        &mut shard,
        &mut out,
        0,
        peer1,
        "swarm1",
        "1",
        AnnounceEvent::Started,
    )
    .unwrap();
    assert_eq!(shard.swarm_count(), 1);
    assert_eq!(swarm_peers(&shard, "swarm1"), ["0", "1"]);

    announce(
        &mut shard,
        &mut out,
        0,
        peer2,
        "swarm2",
        "2_0",
        AnnounceEvent::Completed,
    )
    .unwrap();
    assert_eq!(shard.swarm_count(), 2);
    assert_eq!(swarm_peers(&shard, "swarm1"), ["0", "1"]);
    assert_eq!(swarm_peers(&shard, "swarm2"), ["2_0"]);

    announce(
        &mut shard,
        &mut out,
        0,
        peer3,
        "swarm2",
        "2_1",
        AnnounceEvent::Completed,
    )
    .unwrap();
    assert_eq!(shard.swarm_count(), 2);
    assert_eq!(swarm_peers(&shard, "swarm1"), ["0", "1"]);
    assert_eq!(swarm_peers(&shard, "swarm2"), ["2_0", "2_1"]);

    // The same peer joins a second swarm over the same connection.
    announce(
        &mut shard,
        &mut out,
        0,
        peer1,
        "swarm2",
        "1",
        AnnounceEvent::Completed,
    )
    .unwrap();
    assert_eq!(shard.swarm_count(), 2);
    assert_eq!(swarm_peers(&shard, "swarm1"), ["0", "1"]);
    assert_eq!(swarm_peers(&shard, "swarm2"), ["1", "2_0", "2_1"]);

    shard.check_invariants().unwrap();
}
