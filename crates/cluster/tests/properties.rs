//! Property-based tests for aeron-cluster Raft quorum size and member count properties.

use aeron_cluster::consensus_module::ConsensusModule;
use aeron_cluster::state_machine::ClusterMember;
use proptest::prelude::*;

proptest! {
    // 1. Quorum size property: quorum must be strictly majority (len / 2) + 1
    #[test]
    fn prop_quorum_size_is_majority(
        member_count in 1usize..20usize,
    ) {
        let members: Vec<_> = (0..member_count)
            .map(|id| ClusterMember::new(id as i32, "aeron:ipc", "aeron:ipc"))
            .collect();

        let consensus = ConsensusModule::new(0, members);
        let expected_quorum = (member_count / 2) + 1;

        prop_assert_eq!(consensus.quorum_size(), expected_quorum);
        prop_assert!(consensus.quorum_size() * 2 > member_count);
    }
}
