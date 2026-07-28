//! In-memory election model for a three-node leader transition.
//!
//! This does not exercise transports, replicated logs, archive recovery, or concurrent load.

use aeron_cluster::codecs::ClusterRole;
use aeron_cluster::consensus_module::ConsensusModule;
use aeron_cluster::state_machine::ClusterMember;

#[test]
fn should_model_a_leader_transition_after_a_new_election() {
    let members = vec![
        ClusterMember::new(0, "aeron:ipc", "aeron:ipc"),
        ClusterMember::new(1, "aeron:ipc", "aeron:ipc"),
        ClusterMember::new(2, "aeron:ipc", "aeron:ipc"),
    ];

    let mut node0 = ConsensusModule::new(0, members.clone());
    let mut node1 = ConsensusModule::new(1, members.clone());
    let mut node2 = ConsensusModule::new(2, members.clone());

    // 1. Initial election: Node 0 becomes Leader
    node0.start_election();
    node0.on_vote_response(1, 1, true);
    let _ = node0.do_work();
    assert_eq!(node0.state_machine().role(), ClusterRole::Leader);

    // Nodes 1 & 2 acknowledge Node 0 as Leader
    let header = aeron_cluster::codecs::ClusterHeader {
        leader_member_id: 0,
        _pad: 0,
        term_id: 1,
        log_position: 100,
        timestamp: 1000,
    };

    node1.on_heartbeat(&header);
    node2.on_heartbeat(&header);

    assert_eq!(node1.state_machine().role(), ClusterRole::Follower);
    assert_eq!(node2.state_machine().role(), ClusterRole::Follower);

    // 2. Failover trigger: Leader (Node 0) is dropped/killed.
    // Node 1 detects heartbeat timeout and triggers election for Term 2.
    node1.start_election();
    assert_eq!(node1.state_machine().role(), ClusterRole::Candidate);
    assert_eq!(node1.state_machine().term_id(), 2);

    // Node 2 votes for Node 1
    let vote_granted = node2.on_vote_request(1, 2);
    assert!(vote_granted);
    node1.on_vote_response(2, 2, true);

    // Node 1 steps and becomes the new Leader for Term 2
    let _ = node1.do_work();
    assert_eq!(node1.state_machine().role(), ClusterRole::Leader);
    assert_eq!(node1.state_machine().leader_member_id(), 1);
}
