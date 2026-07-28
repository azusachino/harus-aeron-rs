//! ClusterMember state machine and role transitions.
//!
//! Mirrors `io.aeron.cluster.ClusterMember.java`.

use crate::codecs::ClusterRole;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMember {
    pub member_id: i32,
    pub ingress_channel: String,
    pub consensus_channel: String,
    pub is_leader: bool,
}

impl ClusterMember {
    pub fn new(member_id: i32, ingress_channel: &str, consensus_channel: &str) -> Self {
        Self {
            member_id,
            ingress_channel: ingress_channel.to_string(),
            consensus_channel: consensus_channel.to_string(),
            is_leader: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusModuleState {
    Init,
    Active,
    Suspended,
    Closed,
}

pub struct ClusterStateMachine {
    member_id: i32,
    role: ClusterRole,
    state: ConsensusModuleState,
    term_id: i64,
    leader_member_id: i32,
}

impl ClusterStateMachine {
    pub fn new(member_id: i32) -> Self {
        Self {
            member_id,
            role: ClusterRole::Follower,
            state: ConsensusModuleState::Init,
            term_id: 0,
            leader_member_id: -1,
        }
    }

    pub fn role(&self) -> ClusterRole {
        self.role
    }

    pub fn member_id(&self) -> i32 {
        self.member_id
    }

    pub fn state(&self) -> ConsensusModuleState {
        self.state
    }

    pub fn term_id(&self) -> i64 {
        self.term_id
    }

    pub fn leader_member_id(&self) -> i32 {
        self.leader_member_id
    }

    pub fn transition_to_candidate(&mut self) {
        self.role = ClusterRole::Candidate;
        self.term_id += 1;
    }

    pub fn transition_to_leader(&mut self) {
        self.role = ClusterRole::Leader;
        self.leader_member_id = self.member_id;
        self.state = ConsensusModuleState::Active;
    }

    pub fn transition_to_follower(&mut self, leader_member_id: i32, term_id: i64) {
        self.role = ClusterRole::Follower;
        self.leader_member_id = leader_member_id;
        self.term_id = term_id;
        self.state = ConsensusModuleState::Active;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_transition_cluster_roles() {
        let mut sm = ClusterStateMachine::new(0);
        assert_eq!(sm.role(), ClusterRole::Follower);

        sm.transition_to_candidate();
        assert_eq!(sm.role(), ClusterRole::Candidate);
        assert_eq!(sm.term_id(), 1);

        sm.transition_to_leader();
        assert_eq!(sm.role(), ClusterRole::Leader);
        assert_eq!(sm.leader_member_id(), 0);

        sm.transition_to_follower(1, 2);
        assert_eq!(sm.role(), ClusterRole::Follower);
        assert_eq!(sm.leader_member_id(), 1);
        assert_eq!(sm.term_id(), 2);
    }
}
