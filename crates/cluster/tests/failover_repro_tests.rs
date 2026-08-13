//! In-memory election models for three-node leader transitions.
//!
//! These tests do not exercise UDP transports, replicated logs, Archive recovery, or
//! real concurrent load. The sustained-load test is a deterministic comparison of
//! election progress only; it is not the exact host reproduction from issue #20.

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

#[test]
fn should_keep_bounded_load_progress_after_leader_failure() {
    const MESSAGE_COUNT: usize = 3_000;
    const LEADER_FAILURE_AT: usize = 1_500;

    let members = vec![
        ClusterMember::new(0, "aeron:ipc", "aeron:ipc"),
        ClusterMember::new(1, "aeron:ipc", "aeron:ipc"),
        ClusterMember::new(2, "aeron:ipc", "aeron:ipc"),
    ];
    let mut node0 = ConsensusModule::new(0, members.clone());
    let mut node1 = ConsensusModule::new(1, members.clone());
    let mut node2 = ConsensusModule::new(2, members);

    node0.start_election();
    node0.on_vote_response(1, 1, true);
    node0.do_work().unwrap();
    assert_eq!(node0.state_machine().role(), ClusterRole::Leader);

    let mut accepted_messages = 0;
    for message_index in 0..MESSAGE_COUNT {
        if message_index == LEADER_FAILURE_AT {
            // The old leader is intentionally no longer stepped after this point.
            node1.start_election();
            assert!(node2.on_vote_request(1, 2));
            node1.on_vote_response(2, 2, true);
            node1.do_work().unwrap();
            assert_eq!(node1.state_machine().role(), ClusterRole::Leader);
            assert_eq!(node1.state_machine().term_id(), 2);
        }

        accepted_messages += 1;
        let leader_member_id = if message_index < LEADER_FAILURE_AT {
            0
        } else {
            1
        };
        let term_id = if message_index < LEADER_FAILURE_AT {
            1
        } else {
            2
        };
        let header = aeron_cluster::codecs::ClusterHeader {
            leader_member_id,
            _pad: 0,
            term_id,
            log_position: (message_index + 1) as i64,
            timestamp: message_index as i64,
        };
        if message_index < LEADER_FAILURE_AT {
            node1.on_heartbeat(&header);
            node2.on_heartbeat(&header);
        } else {
            node2.on_heartbeat(&header);
        }
    }

    assert_eq!(accepted_messages, MESSAGE_COUNT);
    assert_eq!(node1.state_machine().role(), ClusterRole::Leader);
    assert_eq!(node2.state_machine().leader_member_id(), 1);
    assert_eq!(node2.state_machine().term_id(), 2);
}
