//! MediaDriver: Conductor, Sender, Receiver duty-cycle agents, UDP transport.
//!
//! Mirrors upstream `io.aeron.driver`'s package layout — depends on [`aeron_client`]
//! for wire types and the log buffer, same as upstream `aeron-driver` depending on
//! `aeron-client`. Orchestrates the log buffer, IPC command/status buffers, and UDP
//! transport into the standalone media driver process. Skeleton crate — not yet
//! implemented.

pub mod conductor;
pub mod configuration;
pub mod driver;
pub mod error;
pub mod flow_control;
pub mod loss_detector;
pub mod media;
pub mod network_publication;
pub mod publication_image;
pub mod publication_params;
pub mod receiver;
pub mod receiver_liveness_tracker;
pub mod retransmit_handler;
pub mod sender;
