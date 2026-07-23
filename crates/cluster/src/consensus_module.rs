//! ConsensusModule agent managing Raft consensus elections and heartbeats.
//!
//! Mirrors `io.aeron.cluster.ConsensusModule.java`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::codecs::{ClusterHeader, ClusterRole};

use crate::state_machine::{ClusterMember, ClusterStateMachine};

pub struct ConsensusModule {
    state_machine: ClusterStateMachine,
    members: HashMap<i32, ClusterMember>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    last_heartbeat: Instant,
    votes_received: usize,
}

impl ConsensusModule {
    pub fn new(member_id: i32, members: Vec<ClusterMember>) -> Self {
        let mut member_map = HashMap::new();
        for m in members {
            member_map.insert(m.member_id, m);
        }

        Self {
            state_machine: ClusterStateMachine::new(member_id),
            members: member_map,
            election_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
            last_heartbeat: Instant::now(),
            votes_received: 0,
        }
    }

    pub fn state_machine(&self) -> &ClusterStateMachine {
        &self.state_machine
    }

    pub fn quorum_size(&self) -> usize {
        (self.members.len() / 2) + 1
    }

    pub fn do_work(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let mut work_count = 0;

        match self.state_machine.role() {
            ClusterRole::Follower => {
                if self.last_heartbeat.elapsed() > self.election_timeout {
                    self.start_election();
                    work_count += 1;
                }
            }
            ClusterRole::Candidate => {
                if self.votes_received >= self.quorum_size() {
                    self.state_machine.transition_to_leader();
                    work_count += 1;
                } else if self.last_heartbeat.elapsed() > self.election_timeout {
                    self.start_election();
                    work_count += 1;
                }
            }
            ClusterRole::Leader => {
                if self.last_heartbeat.elapsed() > self.heartbeat_interval {
                    self.send_heartbeat();
                    self.last_heartbeat = Instant::now();
                    work_count += 1;
                }
            }
        }

        Ok(work_count)
    }

    pub fn start_election(&mut self) {
        self.state_machine.transition_to_candidate();
        self.votes_received = 1; // vote for self
        self.last_heartbeat = Instant::now();
    }

    pub fn send_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    pub fn on_heartbeat(&mut self, header: &ClusterHeader) {
        if header.term_id >= self.state_machine.term_id() {
            self.state_machine
                .transition_to_follower(header.leader_member_id, header.term_id);
            self.last_heartbeat = Instant::now();
        }
    }

    pub fn on_vote_request(&mut self, candidate_id: i32, term_id: i64) -> bool {
        if term_id > self.state_machine.term_id() {
            self.state_machine
                .transition_to_follower(candidate_id, term_id);
            self.last_heartbeat = Instant::now();
            true
        } else {
            false
        }
    }

    pub fn on_vote_response(&mut self, _voter_id: i32, term_id: i64, vote_granted: bool) {
        if self.state_machine.role() == ClusterRole::Candidate
            && term_id == self.state_machine.term_id()
            && vote_granted
        {
            self.votes_received += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_elect_leader_when_quorum_reached() {
        let members = vec![
            ClusterMember::new(0, "aeron:ipc", "aeron:ipc"),
            ClusterMember::new(1, "aeron:ipc", "aeron:ipc"),
            ClusterMember::new(2, "aeron:ipc", "aeron:ipc"),
        ];

        let mut consensus = ConsensusModule::new(0, members);
        assert_eq!(consensus.quorum_size(), 2);

        consensus.start_election();
        assert_eq!(consensus.state_machine().role(), ClusterRole::Candidate);

        consensus.on_vote_response(1, 1, true);
        let _ = consensus.do_work();

        assert_eq!(consensus.state_machine().role(), ClusterRole::Leader);
    }
}
