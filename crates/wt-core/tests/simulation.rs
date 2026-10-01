//! Port of wt-tracker test/simulation.test.ts, plus a full invariant check after every step.

mod common;

use common::{Recorder, announce, stop};
use wt_core::{AnnounceEvent, ConnId, Settings, Shard};

struct PeerData {
    info_hash: Option<String>,
    peer_id: String,
}

fn run_simulation(seed: u64) {
    const SIMULATIONS: usize = 1000;
    const TORRENTS: u64 = 2;
    const PEERS: usize = 200;
    const SAME_ID_PEERS_RATIO: f64 = 0.1;

    let mut rng = fastrand::Rng::with_seed(seed);
    let mut shard = Shard::new(Settings::default(), seed);
    let mut out = Recorder::default();

    let distinct_ids = (PEERS as f64 * SAME_ID_PEERS_RATIO) as usize;
    let mut next_conn = PEERS as u64;
    let mut sockets: Vec<u64> = (0..PEERS as u64).collect();
    let mut peers_data: Vec<PeerData> = (0..PEERS)
        .map(|i| PeerData {
            info_hash: None,
            peer_id: (i % distinct_ids).to_string(),
        })
        .collect();

    for _ in 0..SIMULATIONS {
        let i = rng.usize(..PEERS);
        let conn = sockets[i];
        let data = &mut peers_data[i];

        match data.info_hash.clone() {
            Some(info_hash) => {
                let random = rng.f64();
                if random < 0.05 {
                    // leave torrent
                    stop(&mut shard, &mut out, conn, &info_hash, &data.peer_id);
                    data.info_hash = None;
                } else if random < 0.06 {
                    // disconnect
                    shard.disconnect(ConnId(conn), &mut out);
                    data.info_hash = None;
                    sockets[i] = next_conn;
                    next_conn += 1;
                } else {
                    // announce on the same torrent
                    announce(
                        &mut shard,
                        &mut out,
                        0,
                        conn,
                        &info_hash,
                        &data.peer_id,
                        AnnounceEvent::None,
                    )
                    .unwrap();
                }
            }
            None => {
                // assign the peer to a torrent
                let info_hash = rng.u64(..TORRENTS).to_string();
                announce(
                    &mut shard,
                    &mut out,
                    0,
                    conn,
                    &info_hash,
                    &data.peer_id,
                    AnnounceEvent::None,
                )
                .unwrap();
                data.info_hash = Some(info_hash);
            }
        }

        shard.check_invariants().unwrap();
    }

    for (info_hash, stats) in shard.swarms() {
        assert!(stats.peers > 0);
        let info_hash = std::str::from_utf8(info_hash.as_bytes()).unwrap();
        for peer_id in shard.swarm_peer_ids(info_hash.as_bytes()).unwrap() {
            let peer_id = std::str::from_utf8(peer_id.as_bytes()).unwrap();
            assert!(
                peers_data
                    .iter()
                    .any(|d| d.peer_id == peer_id && d.info_hash.as_deref() == Some(info_hash)),
                "peer {peer_id} in swarm {info_hash} has no matching simulated peer"
            );
        }
    }
}

#[test]
fn should_pass_random_simulations() {
    for seed in 0..50 {
        run_simulation(seed);
    }
}
