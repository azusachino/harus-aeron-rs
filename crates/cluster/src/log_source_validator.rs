//! Validate whether a cluster log source is acceptable for a backup.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSourceType {
    Leader,
    Follower,
    Any,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogSourceValidator {
    source_type: LogSourceType,
}

impl LogSourceValidator {
    pub fn new(source_type: LogSourceType) -> Self {
        Self { source_type }
    }

    pub fn is_acceptable(&self, leader_member_id: i64, member_id: i64) -> bool {
        match self.source_type {
            LogSourceType::Leader => leader_member_id != -1 && leader_member_id == member_id,
            LogSourceType::Follower => leader_member_id == -1 || leader_member_id != member_id,
            LogSourceType::Any => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_source_only_accepts_the_known_leader() {
        let validator = LogSourceValidator::new(LogSourceType::Leader);
        assert!(validator.is_acceptable(123, 123));
        assert!(!validator.is_acceptable(123, 456));
        assert!(!validator.is_acceptable(-1, -1));
        assert!(!validator.is_acceptable(123, -1));
        assert!(!validator.is_acceptable(-1, 456));
    }

    #[test]
    fn follower_source_accepts_followers_and_unknown_leaders() {
        let validator = LogSourceValidator::new(LogSourceType::Follower);
        assert!(!validator.is_acceptable(123, 123));
        assert!(validator.is_acceptable(123, 456));
        assert!(validator.is_acceptable(-1, -1));
        assert!(validator.is_acceptable(123, -1));
        assert!(validator.is_acceptable(-1, 456));
    }

    #[test]
    fn any_source_accepts_every_member_pair() {
        let validator = LogSourceValidator::new(LogSourceType::Any);
        assert!(validator.is_acceptable(123, 123));
        assert!(validator.is_acceptable(123, 456));
        assert!(validator.is_acceptable(-1, -1));
    }
}
