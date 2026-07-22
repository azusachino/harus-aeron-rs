//! Aeron client: wire protocol, log buffer, `Publication`/`Subscription` API.
//!
//! Mirrors upstream `aeron-client`'s package layout (`io.aeron.protocol`,
//! `io.aeron.logbuffer`, plus the client API and `ClientConductor` at the crate root) —
//! depends only on [`agrona`], same as upstream. `aeron-driver`, `aeron-archive`, and
//! `aeron-cluster` all depend on this crate rather than reimplementing the wire types.
//!
//! `protocol` (`HeaderFlyweight`/`ErrorFlyweight`/`DataHeaderFlyweight`) and `logbuffer`
//! (`LogBufferDescriptor` layout, `Header`/`HeaderWriter`, term scan/read/rebuild/unblock
//! utilities) are implemented as of v0.0.3. `Publication`/`Subscription`/`ClientConductor`
//! at the crate root are not yet implemented — see `docs/plan.md`'s v0.0.4 milestone.

pub mod logbuffer;
pub mod protocol;
