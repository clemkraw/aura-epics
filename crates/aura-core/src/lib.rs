//! # aura-core
//!
//! Shared types for the AURA-EPICS archiving system.
//! Full EPICS 7 PVAccess Normative Types support.
//! ```

pub mod error;

// ── Re-exports ───────────────────────────────────────────────────────
pub use error::{AuraError, AuraResult};