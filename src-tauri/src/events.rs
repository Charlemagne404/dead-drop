//! Names for the Rust-to-frontend event boundary.
//!
//! Payload types are defined by the emitting subsystem; keeping the names in
//! one place prevents a UI rename from silently leaving a stale emitter.

pub(crate) const PEERS_UPDATED: &str = "peers-updated";
pub(crate) const TRANSFER_UPDATE: &str = "transfer-update";
pub(crate) const INCOMING_TRANSFER: &str = "incoming-transfer";
pub(crate) const TRUST_REQUEST: &str = "trust-request";
pub(crate) const DISCOVERY_STATUS: &str = "discovery-status";
pub(crate) const CONNECTIVITY_DIAGNOSTICS: &str = "connectivity-diagnostics";
