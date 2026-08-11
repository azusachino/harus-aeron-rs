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
    candidate_term_id: i64,
    vote: Option<bool>,
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
            candidate_term_id: -1,
            vote: None,
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
                let parsed = Self {
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
                    candidate_term_id: -1,
                    vote: None,
                };
                Ok(parsed)
            })
            .collect()
    }

    pub fn parse_endpoints(member_id: i32, endpoints: &str) -> Result<Self, String> {
        let fields = endpoints.split(',').collect::<Vec<_>>();
        if fields.len() != 5 {
            return Err(format!("invalid member value: {endpoints}"));
        }

        Ok(Self {
            member_id,
            ingress_channel: fields[0].into(),
            consensus_channel: fields[1].into(),
            log_channel: fields[2].into(),
            catchup_channel: fields[3].into(),
            archive_channel: fields[4].into(),
            archive_response_channel: None,
            egress_response_channel: None,
            endpoints: endpoints.into(),
            is_leader: false,
            leadership_term_id: -1,
            log_position: -1,
            time_of_last_append_position_ns: -1,
            candidate_term_id: -1,
            vote: None,
        })
    }

    pub fn encode_as_string(members: &[Self]) -> String {
        members
            .iter()
            .map(|member| format!("{},{}", member.member_id, member.endpoints))
            .collect::<Vec<_>>()
            .join("|")
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

    pub fn candidate_term_id(&mut self, term_id: i64) -> &mut Self {
        self.candidate_term_id = term_id;
        self
    }

    pub fn candidate_term_id_value(&self) -> i64 {
        self.candidate_term_id
    }

    pub fn vote(&mut self, vote: Option<bool>) -> &mut Self {
        self.vote = vote;
        self
    }

    pub fn vote_value(&self) -> Option<bool> {
        self.vote
    }

    pub fn is_active(&self, now_ns: i64, timeout_ns: i64) -> bool {
        self.time_of_last_append_position_ns + timeout_ns > now_ns
    }

    pub fn has_active_quorum(members: &[Self], now_ns: i64, timeout_ns: i64) -> bool {
        members
            .iter()
            .filter(|member| member.is_active(now_ns, timeout_ns))
            .count()
            >= Self::quorum_threshold(members.len())
    }

    pub fn reset(&mut self) {
        self.is_leader = false;
        self.vote = None;
        self.candidate_term_id = -1;
        self.leadership_term_id = -1;
        self.log_position = -1;
    }

    pub fn quorum_threshold(member_count: usize) -> usize {
        (member_count / 2) + 1
    }

    pub fn compare_log(lhs: &Self, rhs: &Self) -> std::cmp::Ordering {
        Self::compare_log_values(
            lhs.leadership_term_id,
            lhs.log_position,
            rhs.leadership_term_id,
            rhs.log_position,
        )
    }

    pub fn quorum_position(members: &[Self], now_ns: i64, timeout_ns: i64) -> i64 {
        let mut positions = members
            .iter()
            .filter(|member| member.is_active(now_ns, timeout_ns))
            .map(|member| member.log_position.max(0))
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

    pub fn compare_log_values(
        lhs_term_id: i64,
        lhs_position: i64,
        rhs_term_id: i64,
        rhs_position: i64,
    ) -> std::cmp::Ordering {
        lhs_term_id
            .cmp(&rhs_term_id)
            .then(lhs_position.cmp(&rhs_position))
    }

    pub fn will_vote_for(&self, candidate: &Self) -> bool {
        self.log_position != -1 && Self::compare_log(self, candidate) != std::cmp::Ordering::Greater
    }

    pub fn has_reached_position(
        &self,
        leadership_term_id: i64,
        position: i64,
        now_ns: i64,
        timeout_ns: i64,
    ) -> bool {
        self.is_active(now_ns, timeout_ns)
            && self.leadership_term_id == leadership_term_id
            && self.log_position >= position
    }

    pub fn is_unanimous_candidate(
        members: &[Self],
        candidate: &Self,
        graceful_closed_leader_id: i32,
    ) -> bool {
        let possible_votes = members
            .iter()
            .filter(|member| member.member_id != graceful_closed_leader_id)
            .filter(|member| member.will_vote_for(candidate))
            .count();
        possible_votes >= Self::quorum_threshold(members.len())
            && members
                .iter()
                .filter(|member| member.member_id != graceful_closed_leader_id)
                .all(|member| member.will_vote_for(candidate))
    }

    pub fn is_quorum_candidate(members: &[Self], candidate: &Self) -> bool {
        members
            .iter()
            .filter(|member| member.will_vote_for(candidate))
            .count()
            >= Self::quorum_threshold(members.len())
    }

    pub fn is_unanimous_leader(
        members: &[Self],
        candidate_term_id: i64,
        graceful_closed_leader_id: i32,
    ) -> bool {
        let mut votes = 0;
        for member in members
            .iter()
            .filter(|member| member.member_id != graceful_closed_leader_id)
        {
            if member.candidate_term_id != candidate_term_id || member.vote != Some(true) {
                return false;
            }
            votes += 1;
        }
        votes >= Self::quorum_threshold(members.len())
    }

    pub fn is_quorum_leader(members: &[Self], candidate_term_id: i64) -> bool {
        let mut votes = 0;
        for member in members {
            if member.candidate_term_id == candidate_term_id {
                if member.vote == Some(false) {
                    return false;
                }
                if member.vote == Some(true) {
                    votes += 1;
                }
            }
        }
        votes >= Self::quorum_threshold(members.len())
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

        let parsed =
            ClusterMember::parse_endpoints(7, "ingress,consensus,log,catchup,archive").unwrap();
        assert_eq!(parsed.member_id, 7);
        assert_eq!(parsed.endpoints(), "ingress,consensus,log,catchup,archive");
        assert_eq!(
            ClusterMember::encode_as_string(&[parsed]),
            "7,ingress,consensus,log,catchup,archive"
        );
        assert!(ClusterMember::parse_endpoints(7, "ingress,consensus").is_err());
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

    #[test]
    fn should_match_upstream_vote_and_election_rules() {
        let candidate = ClusterMember::new(2, "ingress", "consensus")
            .log_position(800)
            .leadership_term_id(10)
            .clone();
        let members = vec![
            ClusterMember::new(10, "ingress", "consensus")
                .log_position(100)
                .leadership_term_id(2)
                .clone(),
            ClusterMember::new(20, "ingress", "consensus")
                .log_position(600)
                .leadership_term_id(18)
                .clone(),
            ClusterMember::new(30, "ingress", "consensus")
                .log_position(800)
                .leadership_term_id(10)
                .clone(),
            ClusterMember::new(40, "ingress", "consensus")
                .log_position(800)
                .leadership_term_id(9)
                .clone(),
            ClusterMember::new(50, "ingress", "consensus")
                .log_position(700)
                .leadership_term_id(10)
                .clone(),
        ];

        assert!(ClusterMember::is_quorum_candidate(&members, &candidate));
        assert!(!ClusterMember::is_unanimous_candidate(
            &members, &candidate, -1
        ));
        assert!(members[0].will_vote_for(&candidate));
        assert!(!members[1].will_vote_for(&candidate));
        assert_eq!(
            ClusterMember::compare_log_values(5, 1000, 3, 999_999),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn should_require_matching_terms_and_positive_votes_for_leadership() {
        let mut members = (1..=3)
            .map(|id| ClusterMember::new(id, "ingress", "consensus"))
            .collect::<Vec<_>>();
        for member in &mut members {
            member.candidate_term_id(42).vote(Some(true));
        }

        assert!(ClusterMember::is_quorum_leader(&members, 42));
        assert!(ClusterMember::is_unanimous_leader(&members, 42, -1));

        members[1].vote(Some(false));
        assert!(!ClusterMember::is_quorum_leader(&members, 42));
        assert!(!ClusterMember::is_unanimous_leader(&members, 42, -1));

        members[1].vote(Some(true)).candidate_term_id(43);
        assert!(!ClusterMember::is_unanimous_leader(&members, 42, -1));

        members[0].candidate_term_id(42).vote(Some(true));
        members[1].candidate_term_id(42).vote(Some(true));
        members[2].candidate_term_id(42).vote(Some(true));
        assert!(ClusterMember::is_unanimous_leader(&members, 42, 1));
    }

    #[test]
    fn should_check_reached_position_with_activity_and_term() {
        let member = ClusterMember::new(1, "ingress", "consensus")
            .leadership_term_id(42)
            .log_position(500)
            .time_of_last_append_position_ns(10)
            .clone();

        assert!(member.has_reached_position(42, 100, 12, 5));
        assert!(!member.has_reached_position(42, 600, 12, 5));
        assert!(!member.has_reached_position(7, 100, 12, 5));
        assert!(!member.has_reached_position(42, 100, 16, 5));
    }

    #[test]
    fn should_reset_election_state_and_detect_active_quorum() {
        let mut members = (0..3)
            .map(|id| ClusterMember::new(id, "ingress", "consensus"))
            .collect::<Vec<_>>();
        members[0].is_leader = true;
        members[0]
            .candidate_term_id(42)
            .leadership_term_id(42)
            .log_position(100)
            .vote(Some(true))
            .time_of_last_append_position_ns(9);
        members[1].time_of_last_append_position_ns(9);
        members[2].time_of_last_append_position_ns(0);

        assert!(ClusterMember::has_active_quorum(&members, 10, 2));
        assert!(!ClusterMember::has_active_quorum(&members, 10, 0));

        members[0].reset();
        assert!(!members[0].is_leader);
        assert_eq!(members[0].candidate_term_id_value(), -1);
        assert_eq!(members[0].leadership_term_id_value(), -1);
        assert_eq!(members[0].log_position_value(), -1);
        assert_eq!(members[0].vote_value(), None);
    }
}
