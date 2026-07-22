//! Aeron client: wire protocol, log buffer, `Publication`/`Subscription` API.
//!
//! Mirrors upstream `aeron-client`'s package layout (`io.aeron.protocol`,
//! `io.aeron.logbuffer`, plus the client API and `ClientConductor` at the crate root) —
//! depends only on [`agrona`], same as upstream. `aeron-driver`, `aeron-archive`, and
//! `aeron-cluster` all depend on this crate rather than reimplementing the wire types.
//! Skeleton crate — not yet implemented.

pub mod logbuffer;
pub mod protocol;
