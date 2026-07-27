//! Aeron Cluster Raft consensus protocol codecs and flyweights.
//!
//! Mirrors `io.aeron.cluster.codecs`.

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterRole {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMessageType {
    SessionConnect = 1,
    SessionClose = 2,
    Heartbeat = 3,
    VoteRequest = 4,
    VoteResponse = 5,
    NewLeader = 6,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ClusterHeader {
    pub leader_member_id: i32,
    pub _pad: i32,
    pub term_id: i64,
    pub log_position: i64,
    pub timestamp: i64,
}

impl ClusterHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

const _: () = assert!(std::mem::size_of::<ClusterHeader>() == 32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_validate_cluster_header_size() {
        assert_eq!(ClusterHeader::SIZE, 32);
    }
}
