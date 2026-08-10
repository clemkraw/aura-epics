//! PVA wire protocol codec.
//!
//! Implements the PVAccess Protocol Specification encoding:
//! - `header`: 8-byte message header (magic, version, flags, command, size)
//! - `commands`: protocol command constants (CMD_BEACON, CMD_MONITOR, etc.)
//! - `pvdata`: primitive type encoding (integers, floats, strings, arrays, sizes)
//! - `bitset`: compact bit array for delta-encoded monitor updates
//! - `field_desc`: type introspection / structure descriptions
//! - `framing`: TCP stream framing + segmented message reassembly

pub mod bitset;
pub mod commands;
pub mod field_desc;
pub mod framing;
pub mod header;
pub mod pvdata;

pub use bitset::PvaBitSet;
pub use commands::*;
pub use field_desc::{FieldCategory, FieldDesc, FieldType, IntrospectionRegistry, NamedField};
pub use framing::{CodecError, PvaCodec, PvaFrame};
pub use header::{ByteOrder, HEADER_SIZE, PVA_MAGIC, PVA_VERSION, PvaHeader, Segmentation};
pub use pvdata::{DecodeError, PvaReader, PvaWriter};
