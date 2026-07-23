//! Learner stubs for authorization policies.

pub trait AuthorisationService {
    fn is_authorised(&self) -> bool;
}

pub struct AllowAll;
pub struct DenyAll;
