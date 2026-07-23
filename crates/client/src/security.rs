//! Authorization policies for client/driver commands.

/// Authorization policy for a command and its optional credentials.
pub trait AuthorisationService {
    fn is_authorised(
        &self,
        protocol_id: i32,
        action_id: i32,
        command: Option<&[u8]>,
        encoded_credentials: &[u8],
    ) -> bool;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAll;

pub const ALLOW_ALL: AllowAll = AllowAll;
pub const DENY_ALL: DenyAll = DenyAll;

impl AuthorisationService for AllowAll {
    fn is_authorised(&self, _: i32, _: i32, _: Option<&[u8]>, _: &[u8]) -> bool {
        true
    }
}

impl AuthorisationService for DenyAll {
    fn is_authorised(&self, _: i32, _: i32, _: Option<&[u8]>, _: &[u8]) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_all_authorizes_any_command() {
        assert!(ALLOW_ALL.is_authorised(77, 1, None, &[1, 2, 3]));
    }

    #[test]
    fn deny_all_rejects_any_command() {
        assert!(!DENY_ALL.is_authorised(77, 1, None, &[1, 2, 3]));
    }
}
