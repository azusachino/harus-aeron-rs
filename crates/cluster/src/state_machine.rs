//! ClusterMember state machine and role transitions.
//!
//! Mirrors `io.aeron.cluster.ClusterMember.java`.

use crate::codecs::ClusterRole;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMember {
    pub member_id: i32,
    pub ingress_channel: String,
    pub consensus_channel: String,
    pub log_channel: String,
    pub catchup_channel: String,
    pub archive_channel: String,
    pub archive_response_channel: Option<String>,
    pub egress_response_channel: Option<String>,
    endpoints: String,
    pub is_leader: bool,
    leadership_term_id: i64,
    log_position: i64,
    time_of_last_append_position_ns: i64,
}

impl ClusterMember {
    pub fn new(member_id: i32, ingress_channel: &str, consensus_channel: &str) -> Self {
        Self {
            member_id,
            ingress_channel: ingress_channel.to_string(),
            consensus_channel: consensus_channel.to_string(),
            log_channel: String::new(),
            catchup_channel: String::new(),
            archive_channel: String::new(),
            archive_response_channel: None,
            egress_response_channel: None,
            endpoints: format!("{ingress_channel},{consensus_channel}"),
            is_leader: false,
            leadership_term_id: -1,
            log_position: -1,
            time_of_last_append_position_ns: -1,
        }
    }

    pub fn parse(value: &str) -> Result<Vec<Self>, String> {
        if value.is_empty() {
            return Ok(Vec::new());
        }

        value
            .split('|')
            .filter(|member| !member.is_empty())
            .map(|member| {
                let fields = member.split(',').collect::<Vec<_>>();
                if !(6..=8).contains(&fields.len()) {
                    return Err(format!("invalid member value: {member}"));
                }
                let member_id = fields[0]
                    .parse::<i32>()
                    .map_err(|_| format!("invalid cluster member id: {}", fields[0]))?;
                let mut parsed = Self {
                    member_id,
                    ingress_channel: fields[1].into(),
                    consensus_channel: fields[2].into(),
                    log_channel: fields[3].into(),
                    catchup_channel: fields[4].into(),
                    archive_channel: fields[5].into(),
                    archive_response_channel: fields.get(6).map(|value| (*value).into()),
                    egress_response_channel: fields.get(7).map(|value| (*value).into()),
                    endpoints: fields[1..].join(","),
                    is_leader: false,
                    leadership_term_id: -1,
                    log_position: -1,
                    time_of_last_append_position_ns: -1,
                };
                parsed.endpoints = fields[1..].join(",");
                Ok(parsed)
            })
            .collect()
    }

    pub fn endpoints(&self) -> &str {
        &self.endpoints
    }

    pub fn log_position(&mut self, position: i64) -> &mut Self {
        self.log_position = position;
        self
    }

    pub fn log_position_value(&self) -> i64 {
        self.log_position
    }

    pub fn leadership_term_id(&mut self, term_id: i64) -> &mut Self {
        self.leadership_term_id = term_id;
        self
    }

    pub fn leadership_term_id_value(&self) -> i64 {
        self.leadership_term_id
    }

    pub fn time_of_last_append_position_ns(&mut self, timestamp_ns: i64) -> &mut Self {
        self.time_of_last_append_position_ns = timestamp_ns;
        self
    }

    pub fn is_active(&self, now_ns: i64, timeout_ns: i64) -> bool {
        self.time_of_last_append_position_ns + timeout_ns > now_ns
    }

    pub fn quorum_threshold(member_count: usize) -> usize {
        (member_count / 2) + 1
    }

    pub fn compare_log(lhs: &Self, rhs: &Self) -> std::cmp::Ordering {
        lhs.leadership_term_id
            .cmp(&rhs.leadership_term_id)
            .then(lhs.log_position.cmp(&rhs.log_position))
    }

    pub fn quorum_position(members: &[Self], now_ns: i64, timeout_ns: i64) -> i64 {
        let mut positions = members
            .iter()
            .filter(|member| member.is_active(now_ns, timeout_ns))
            .map(|member| member.log_position)
            .collect::<Vec<_>>();
        positions.sort_unstable_by(|lhs, rhs| rhs.cmp(lhs));
        positions
            .get(Self::quorum_threshold(members.len()).saturating_sub(1))
            .copied()
            .unwrap_or(0)
    }

    pub fn has_quorum_at_position(
        members: &[Self],
        leadership_term_id: i64,
        position: i64,
        now_ns: i64,
        timeout_ns: i64,
    ) -> bool {
        members
            .iter()
            .filter(|member| {
                member.is_active(now_ns, timeout_ns)
                    && member.leadership_term_id == leadership_term_id
                    && member.log_position >= position
            })
            .count()
            >= Self::quorum_threshold(members.len())
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

    #[test]
    fn should_parse_member_endpoints_and_optional_response_channels() {
        let members = ClusterMember::parse(
            "0,ingress,consensus,log,catchup,archive,archive-response,egress-response|",
        )
        .unwrap();

        assert_eq!(members[0].member_id, 0);
        assert_eq!(
            members[0].endpoints(),
            "ingress,consensus,log,catchup,archive,archive-response,egress-response"
        );
        assert_eq!(
            members[0].archive_response_channel.as_deref(),
            Some("archive-response")
        );
        assert_eq!(
            members[0].egress_response_channel.as_deref(),
            Some("egress-response")
        );
    }

    #[test]
    fn should_rank_active_member_positions_for_a_quorum() {
        let mut members = (0..3)
            .map(|id| ClusterMember::new(id, "ingress", "consensus"))
            .collect::<Vec<_>>();
        members[0]
            .log_position(100)
            .leadership_term_id(2)
            .time_of_last_append_position_ns(9);
        members[1]
            .log_position(80)
            .leadership_term_id(2)
            .time_of_last_append_position_ns(9);
        members[2]
            .log_position(20)
            .leadership_term_id(2)
            .time_of_last_append_position_ns(0);

        assert_eq!(ClusterMember::quorum_threshold(3), 2);
        assert_eq!(
            ClusterMember::compare_log(&members[0], &members[1]),
            std::cmp::Ordering::Greater
        );
        assert_eq!(ClusterMember::quorum_position(&members, 10, 2), 80);
        assert!(ClusterMember::has_quorum_at_position(
            &members, 2, 80, 10, 2
        ));
        assert!(!ClusterMember::has_quorum_at_position(
            &members, 1, 80, 10, 2
        ));
    }
}
