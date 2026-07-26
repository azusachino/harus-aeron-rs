//! Optional driver extensions used by fault-injection and congestion-control tooling.

pub mod fixed_loss_generator;
pub mod multi_gap_loss_generator;

pub use fixed_loss_generator::FixedLossGenerator;
pub use multi_gap_loss_generator::MultiGapLossGenerator;
