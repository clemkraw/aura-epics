//! PVA wire protocol codec.
//!
//! Implements the PVAccess Protocol Specification encoding:
//! - `header`: 8-byte message header (magic, version, flags, command, size)
//! - `commands`: protocol command constants (CMD_BEACON, CMD_MONITOR, etc.)
//! - `pvdata`: primitive type encoding (integers, floats, strings, arrays, sizes)
//! - `bitset`: compact bit array for delta-encoded monitor updates
//! - `field_desc`: type introspection / structure descriptions
//! - `framing`: TCP stream framing + segmented message reassembly

pub mod header;
pub mod commands;
pub mod pvdata;
pub mod bitset;
pub mod field_desc;
pub mod framing;

pub use header::{PvaHeader, ByteOrder, Segmentation, HEADER_SIZE, PVA_MAGIC, PVA_VERSION};
pub use commands::*;
pub use pvdata::{PvaReader, PvaWriter, DecodeError};
pub use bitset::PvaBitSet;
pub use field_desc::{FieldDesc, FieldType, NamedField, FieldCategory, IntrospectionRegistry};
pub use framing::{PvaCodec, PvaFrame, CodecError};