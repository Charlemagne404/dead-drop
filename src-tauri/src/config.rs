//! Compile-time limits shared by the v1 protocol and transfer engine.
//!
//! These values are part of the current application contract. Keeping them in
//! one small module makes the limits discoverable without turning them into a
//! runtime configuration surface.

use std::time::Duration;

pub(crate) const PROTOCOL_VERSION: u16 = 1;
pub(crate) const DROP_SERVICE_PORT: u16 = 39_821;
pub(crate) const MAX_TRANSFER_FILES: usize = 256;
pub(crate) const MAX_FILENAME_BYTES: usize = 255;
pub(crate) const MAX_TRANSFER_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
pub(crate) const TRANSFER_CHUNK_SIZE: usize = 96 * 1024;
pub(crate) const TRANSFER_PROGRESS_INTERVAL: Duration = Duration::from_millis(120);
