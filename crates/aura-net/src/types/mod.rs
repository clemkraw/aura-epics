//! PVA type conversion layer.
//!
//! Bridges the gap between raw PVA wire types (decoded by the codec) and AURA's type-safe
//! system (defined in aura-core).
//!
//! - `pva_value`: intermediate representation (PvaValue, PvaScalar)
//! - `to_normative`: PvaValue → aura_core::NormativeType conversion

pub mod pva_value;
pub mod to_normative;

pub use pva_value::{PvaScalar, PvaValue, decode_scalar};
pub use to_normative::to_normative;
