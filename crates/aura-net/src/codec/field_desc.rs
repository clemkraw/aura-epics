//! PVA field description / introspection data.
//!
//! Every PVA structure has a `FieldDesc` that describes its shape: field names, types,
//! and nesting. The server sends the FieldDesc once on monitor INIT, then subsequent updates
//! only send changed values (identified by BitSet indices mapping to FieldDesc fields).

use super::pvdata::{DecodeError, PvaReader, PvaWriter};
use aura_core::pva::scalars::ScalarType;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub const NULL_TYPE_CODE: u8 = 0xFF;
pub const ONLY_ID_TYPE_CODE: u8 = 0xFE;
pub const FULL_WITH_ID_CODE: u8 = 0xFD;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldCategory {
    Scalar,
    ScalarArray,
    Structure,
    StructureArray,
    Union,
    UnionArray,
    BoundedString,
}

impl FieldCategory {
    pub fn from_type_code(code: u8) -> Option<Self> {
        match (code >> 3) & 0x07 {
            0 => Some(Self::Scalar),
            1 => Some(Self::ScalarArray),
            2 => Some(Self::Structure),
            3 => Some(Self::StructureArray),
            4 => Some(Self::Union),
            5 => Some(Self::UnionArray),
            6 => Some(Self::BoundedString),
            _ => None,
        }
    }

    pub const fn to_bits(self) -> u8 {
        match self {
            Self::Scalar => 0,
            Self::ScalarArray => 1,
            Self::Structure => 2,
            Self::StructureArray => 3,
            Self::Union => 4,
            Self::UnionArray => 5,
            Self::BoundedString => 6,
        }
    }

    pub const ALL: [Self; 7] = [
        Self::Scalar,
        Self::ScalarArray,
        Self::Structure,
        Self::StructureArray,
        Self::Union,
        Self::UnionArray,
        Self::BoundedString,
    ];
}

impl fmt::Display for FieldCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Scalar => "scalar",
            Self::ScalarArray => "scalar[]",
            Self::Structure => "structure",
            Self::StructureArray => "structure[]",
            Self::Union => "union",
            Self::UnionArray => "union[]",
            Self::BoundedString => "bounded_string",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Scalar(ScalarType),
    ScalarArray(ScalarType),
    BoundedString(usize),
    Structure,
    StructureArray,
    Union,
    UnionArray,
    VariantUnion,
    VariantUnionArray,
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(s) => write!(f, "{s}"),
            Self::ScalarArray(s) => write!(f, "{s}[]"),
            Self::BoundedString(n) => write!(f, "string<{n}>"),
            Self::Structure => write!(f, "structure"),
            Self::StructureArray => write!(f, "structure[]"),
            Self::Union => write!(f, "union"),
            Self::UnionArray => write!(f, "union[]"),
            Self::VariantUnion => write!(f, "any"),
            Self::VariantUnionArray => write!(f, "any[]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDesc {
    pub field_type: FieldType,
    pub type_id: String,
    pub fields: Vec<NamedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedField {
    pub name: std::sync::Arc<str>,
    pub desc: FieldDesc,
}

/// Map ScalarType to PVA wire type code byte.
fn scalar_to_wire(s: ScalarType) -> u8 {
    match s {
        ScalarType::Boolean => 0x00,
        ScalarType::Byte => 0x20,
        ScalarType::Short => 0x21,
        ScalarType::Int => 0x22,
        ScalarType::Long => 0x23,
        ScalarType::UByte => 0x24,
        ScalarType::UShort => 0x25,
        ScalarType::UInt => 0x26,
        ScalarType::ULong => 0x27,
        ScalarType::Float => 0x42,
        ScalarType::Double => 0x43,
        ScalarType::String => 0x60,
    }
}

impl FieldDesc {
    pub fn scalar(stype: ScalarType) -> Self {
        Self {
            field_type: FieldType::Scalar(stype),
            type_id: String::new(),
            fields: Vec::new(),
        }
    }
    pub fn scalar_array(stype: ScalarType) -> Self {
        Self {
            field_type: FieldType::ScalarArray(stype),
            type_id: String::new(),
            fields: Vec::new(),
        }
    }
    pub fn structure(type_id: impl Into<String>, fields: Vec<NamedField>) -> Self {
        Self {
            field_type: FieldType::Structure,
            type_id: type_id.into(),
            fields,
        }
    }
    pub fn structure_array(type_id: impl Into<String>, fields: Vec<NamedField>) -> Self {
        Self {
            field_type: FieldType::StructureArray,
            type_id: type_id.into(),
            fields,
        }
    }
    pub fn union_type(type_id: impl Into<String>, fields: Vec<NamedField>) -> Self {
        Self {
            field_type: FieldType::Union,
            type_id: type_id.into(),
            fields,
        }
    }
    pub fn variant_union() -> Self {
        Self {
            field_type: FieldType::VariantUnion,
            type_id: String::new(),
            fields: Vec::new(),
        }
    }
    pub fn bounded_string(max_len: usize) -> Self {
        Self {
            field_type: FieldType::BoundedString(max_len),
            type_id: String::new(),
            fields: Vec::new(),
        }
    }

    pub fn is_structure(&self) -> bool {
        matches!(self.field_type, FieldType::Structure)
    }
    pub fn is_scalar(&self) -> bool {
        matches!(self.field_type, FieldType::Scalar(_))
    }
    pub fn is_array(&self) -> bool {
        matches!(
            self.field_type,
            FieldType::ScalarArray(_)
                | FieldType::StructureArray
                | FieldType::UnionArray
                | FieldType::VariantUnionArray
        )
    }

    pub fn is_normative_type(&self) -> bool {
        self.type_id.starts_with("epics:nt/")
    }

    pub fn nt_type_name(&self) -> Option<&str> {
        if !self.is_normative_type() {
            return None;
        }
        self.type_id["epics:nt/".len()..].split(':').next()
    }

    pub fn find_field(&self, name: &str) -> Option<&FieldDesc> {
        self.fields
            .iter()
            .find(|f| f.name == Arc::from(name))
            .map(|f| &f.desc)
    }

    pub fn flat_field_count(&self) -> usize {
        1 + self
            .fields
            .iter()
            .map(|f| f.desc.flat_field_count())
            .sum::<usize>()
    }

    pub fn decode(
        reader: &mut PvaReader<'_>,
        registry: &mut IntrospectionRegistry,
    ) -> Result<Option<Self>, DecodeError> {
        let tag = reader.read_u8()?;
        match tag {
            NULL_TYPE_CODE => Ok(None),
            ONLY_ID_TYPE_CODE => {
                let id = reader.read_i16()?;
                registry
                    .get(id)
                    .cloned()
                    .map(Some)
                    .ok_or_else(|| DecodeError::Protocol(format!("unknown introspection ID: {id}")))
            }
            FULL_WITH_ID_CODE => {
                let id = reader.read_i16()?;
                let desc = Self::decode_full(reader)?;
                registry.register(id, desc.clone());
                Ok(Some(desc))
            }
            code => Self::decode_type_code(code, reader).map(Some),
        }
    }

    pub fn encode(&self, writer: &mut PvaWriter) {
        match &self.field_type {
            FieldType::Scalar(s) => writer.write_u8(scalar_to_wire(*s)),
            FieldType::ScalarArray(s) => writer.write_u8(scalar_to_wire(*s) | 0x08),
            FieldType::BoundedString(bound) => {
                writer.write_u8(0x86);
                writer.write_size(*bound);
            }
            FieldType::VariantUnion => writer.write_u8(0x81),
            FieldType::VariantUnionArray => writer.write_u8(0x83),
            FieldType::Structure
            | FieldType::StructureArray
            | FieldType::Union
            | FieldType::UnionArray => {
                let code = match self.field_type {
                    FieldType::Structure => 0x80u8,
                    FieldType::StructureArray => 0x88,
                    FieldType::Union => 0x82,
                    FieldType::UnionArray => 0x8A,
                    _ => unreachable!(),
                };
                writer.write_u8(code);
                writer.write_string(&self.type_id);
                writer.write_size(self.fields.len());
                for f in &self.fields {
                    writer.write_string(&f.name);
                    f.desc.encode(writer);
                }
            }
        }
    }

    fn decode_full(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let tag = reader.read_u8()?;
        match tag {
            NULL_TYPE_CODE => {
                Err(DecodeError::Protocol(
                    "unexpected NULL_TYPE_CODE in nested field".into(),
                ))
            }
            ONLY_ID_TYPE_CODE => {
                let _id = reader.read_i16()?;
                Err(DecodeError::Protocol(format!(
                    "ONLY_ID_TYPE_CODE {_id} in nested field (registry not available)"
                )))
            }
            FULL_WITH_ID_CODE => {
                let _id = reader.read_i16()?;
                let type_code = reader.read_u8()?;
                Self::decode_type_code(type_code, reader)
            }
            code => Self::decode_type_code(code, reader),
        }
    }

    fn decode_type_code(type_code: u8, reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        // PVA type byte encoding (from pvxs TypeCode::code_t and EPICS spec):
        //
        // Complex types (bit 7 set):
        //   0x80 = Structure       0x88 = StructureArray
        //   0x81 = VariantUnion    0x89 = VariantUnionArray
        //   0x82 = Union           0x8A = UnionArray
        //
        // Scalar types (bit 3 = array flag):
        //   0x00 = Boolean         0x08 = Boolean[]
        //   0x20 = Int8  (Byte)    0x28 = Int8[]
        //   0x21 = Int16 (Short)   0x29 = Int16[]
        //   0x22 = Int32 (Int)     0x2A = Int32[]
        //   0x23 = Int64 (Long)    0x2B = Int64[]
        //   0x24 = UInt8 (UByte)   0x2C = UInt8[]
        //   0x25 = UInt16(UShort)  0x2D = UInt16[]
        //   0x26 = UInt32(UInt)    0x2E = UInt32[]
        //   0x27 = UInt64(ULong)   0x2F = UInt64[]
        //   0x42 = Float32(Float)  0x4A = Float32[]
        //   0x43 = Float64(Double) 0x4B = Float64[]
        //   0x60 = String          0x68 = String[]

        if type_code & 0x80 != 0 {
            return Self::decode_complex_type(type_code, reader);
        }

        let is_array = type_code & 0x08 != 0;
        let base = type_code & !0x08;

        let scalar_type = match base {
            0x00 => ScalarType::Boolean,
            0x20 => ScalarType::Byte,   // Int8
            0x21 => ScalarType::Short,  // Int16
            0x22 => ScalarType::Int,    // Int32
            0x23 => ScalarType::Long,   // Int64
            0x24 => ScalarType::UByte,  // UInt8
            0x25 => ScalarType::UShort, // UInt16
            0x26 => ScalarType::UInt,   // UInt32
            0x27 => ScalarType::ULong,  // UInt64
            0x42 => ScalarType::Float,  // Float32
            0x43 => ScalarType::Double, // Float64
            0x60 => ScalarType::String,
            _ => return Err(DecodeError::UnknownTypeCode(type_code)),
        };

        if is_array {
            Ok(Self::scalar_array(scalar_type))
        } else {
            Ok(Self::scalar(scalar_type))
        }
    }

    /// Decode complex types: structures, unions, variant unions + their arrays.
    fn decode_complex_type(type_code: u8, reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        match type_code {
            0x80 | 0x88 => {
                let type_id = reader.read_string()?;
                let count = reader.read_size_non_null()?;
                let mut fields = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    fields.push(NamedField {
                        name: reader.read_string()?.into(),
                        desc: Self::decode_full(reader)?,
                    });
                }
                let ft = if type_code == 0x80 {
                    FieldType::Structure
                } else {
                    FieldType::StructureArray
                };
                Ok(Self {
                    field_type: ft,
                    type_id,
                    fields,
                })
            }
            0x82 | 0x8A => {
                let type_id = reader.read_string()?;
                let count = reader.read_size_non_null()?;
                let mut fields = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    fields.push(NamedField {
                        name: reader.read_string()?.into(),
                        desc: Self::decode_full(reader)?,
                    });
                }
                let ft = if type_code == 0x82 {
                    FieldType::Union
                } else {
                    FieldType::UnionArray
                };
                Ok(Self {
                    field_type: ft,
                    type_id,
                    fields,
                })
            }
            0x86 => {
                let bound = reader.read_size_non_null()?;
                Ok(Self { field_type: FieldType::BoundedString(bound), type_id: String::new(), fields: Vec::new() })
            }
            0x81 => Ok(Self {
                field_type: FieldType::VariantUnion,
                type_id: String::new(),
                fields: Vec::new(),
            }),
            0x83 | 0x89 => Ok(Self {
                field_type: FieldType::VariantUnionArray,
                type_id: String::new(),
                fields: Vec::new(),
            }),
            _ => Err(DecodeError::UnknownTypeCode(type_code)),
        }
    }
}

impl fmt::Display for FieldDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.type_id.is_empty() {
            write!(f, "{} \"{}\"", self.field_type, self.type_id)?;
        } else {
            write!(f, "{}", self.field_type)?;
        }
        if !self.fields.is_empty() {
            write!(f, " ({} fields)", self.fields.len())?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct IntrospectionRegistry {
    by_id: HashMap<i16, FieldDesc>,
    max_entries: usize,
}

impl IntrospectionRegistry {
    pub fn new(max_entries: usize) -> Self {
        Self {
            by_id: HashMap::with_capacity(max_entries.min(1024)),
            max_entries,
        }
    }
    pub fn register(&mut self, id: i16, desc: FieldDesc) -> bool {
        if self.by_id.len() >= self.max_entries {
            return false;
        }
        self.by_id.insert(id, desc);
        true
    }
    pub fn get(&self, id: i16) -> Option<&FieldDesc> {
        self.by_id.get(&id)
    }
    pub fn len(&self) -> usize {
        self.by_id.len()
    }
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
    pub fn is_full(&self) -> bool {
        self.by_id.len() >= self.max_entries
    }
    pub fn clear(&mut self) {
        self.by_id.clear();
    }
}

impl Default for IntrospectionRegistry {
    fn default() -> Self {
        Self::new(128)
    }
}

pub mod nt_ids {
    pub const NT_SCALAR: &str = "epics:nt/NTScalar:1.0";
    pub const NT_SCALAR_ARRAY: &str = "epics:nt/NTScalarArray:1.0";
    pub const NT_ENUM: &str = "epics:nt/NTEnum:1.0";
    pub const NT_TABLE: &str = "epics:nt/NTTable:1.0";
    pub const NT_NDARRAY: &str = "epics:nt/NTNDArray:1.0";
    pub const NT_MATRIX: &str = "epics:nt/NTMatrix:1.0";
    pub const NT_HISTOGRAM: &str = "epics:nt/NTHistogram:1.0";
    pub const NT_CONTINUUM: &str = "epics:nt/NTContinuum:1.0";
    pub const NT_NAME_VALUE: &str = "epics:nt/NTNameValue:1.0";
    pub const NT_MULTI_CHANNEL: &str = "epics:nt/NTMultiChannel:1.0";
    pub const NT_AGGREGATE: &str = "epics:nt/NTAggregate:1.0";
    pub const NT_UNION: &str = "epics:nt/NTUnion:1.0";

    pub const ALL: [&str; 12] = [
        NT_SCALAR,
        NT_SCALAR_ARRAY,
        NT_ENUM,
        NT_TABLE,
        NT_NDARRAY,
        NT_MATRIX,
        NT_HISTOGRAM,
        NT_CONTINUUM,
        NT_NAME_VALUE,
        NT_MULTI_CHANNEL,
        NT_AGGREGATE,
        NT_UNION,
    ];
}

#[cfg(test)]
mod tests {
    use super::super::header::ByteOrder;
    use super::*;

    fn le_w() -> PvaWriter {
        PvaWriter::new(ByteOrder::LittleEndian)
    }
    fn le_r(d: &[u8]) -> PvaReader<'_> {
        PvaReader::new(d, ByteOrder::LittleEndian)
    }
    fn be_w() -> PvaWriter {
        PvaWriter::new(ByteOrder::BigEndian)
    }
    fn be_r(d: &[u8]) -> PvaReader<'_> {
        PvaReader::new(d, ByteOrder::BigEndian)
    }

    fn nf(name: &str, desc: FieldDesc) -> NamedField {
        NamedField {
            name: name.into(),
            desc,
        }
    }

    #[test]
    fn test_stc_values() {
        assert_eq!(ScalarType::Boolean as u8, 0);
        assert_eq!(ScalarType::Byte as u8, 1);
        assert_eq!(ScalarType::Short as u8, 2);
        assert_eq!(ScalarType::Int as u8, 3);
        assert_eq!(ScalarType::Long as u8, 4);
        assert_eq!(ScalarType::UByte as u8, 5);
        assert_eq!(ScalarType::UShort as u8, 6);
        assert_eq!(ScalarType::UInt as u8, 7);
        assert_eq!(ScalarType::ULong as u8, 8);
        assert_eq!(ScalarType::Float as u8, 9);
        assert_eq!(ScalarType::Double as u8, 10);
        assert_eq!(ScalarType::String as u8, 11);
    }
    #[test]
    fn test_stc_from_u8_all() {
        for s in ScalarType::ALL {
            assert_eq!(ScalarType::from_wire(s as u8), Some(s));
        }
    }
    #[test]
    fn test_stc_from_u8_invalid() {
        assert!(ScalarType::from_wire(12).is_none());
        assert!(ScalarType::from_wire(255).is_none());
    }
    #[test]
    fn test_stc_byte_size() {
        assert_eq!(ScalarType::Boolean.wire_size(), 1);
        assert_eq!(ScalarType::Byte.wire_size(), 1);
        assert_eq!(ScalarType::UByte.wire_size(), 1);
        assert_eq!(ScalarType::Short.wire_size(), 2);
        assert_eq!(ScalarType::UShort.wire_size(), 2);
        assert_eq!(ScalarType::Int.wire_size(), 4);
        assert_eq!(ScalarType::UInt.wire_size(), 4);
        assert_eq!(ScalarType::Float.wire_size(), 4);
        assert_eq!(ScalarType::Long.wire_size(), 8);
        assert_eq!(ScalarType::ULong.wire_size(), 8);
        assert_eq!(ScalarType::Double.wire_size(), 8);
        assert_eq!(ScalarType::String.wire_size(), 0);
    }
    #[test]
    fn test_stc_is_integer() {
        assert!(ScalarType::Int.is_integer());
        assert!(!ScalarType::Float.is_integer());
        assert!(!ScalarType::Boolean.is_integer());
        assert!(!ScalarType::String.is_integer());
    }
    #[test]
    fn test_stc_is_floating() {
        assert!(ScalarType::Float.is_floating());
        assert!(ScalarType::Double.is_floating());
        assert!(!ScalarType::Int.is_floating());
    }
    #[test]
    fn test_stc_is_numeric() {
        assert!(ScalarType::Int.is_numeric());
        assert!(ScalarType::Double.is_numeric());
        assert!(!ScalarType::Boolean.is_numeric());
        assert!(!ScalarType::String.is_numeric());
    }
    #[test]
    fn test_stc_is_unsigned() {
        assert!(ScalarType::UInt.is_unsigned());
        assert!(!ScalarType::Int.is_unsigned());
    }
    #[test]
    fn test_stc_name_all() {
        for s in ScalarType::ALL {
            assert!(!s.name().is_empty());
        }
    }
    #[test]
    fn test_stc_display() {
        assert_eq!(ScalarType::Double.to_string(), "double");
        assert_eq!(ScalarType::Boolean.to_string(), "boolean");
    }
    #[test]
    fn test_stc_copy() {
        let a = ScalarType::Int;
        let b = a;
        assert_eq!(a, b);
    }
    #[test]
    fn test_stc_hash() {
        use std::collections::HashSet;
        assert_eq!(ScalarType::ALL.iter().collect::<HashSet<_>>().len(), 12);
    }
    #[test]
    fn test_stc_all_count() {
        assert_eq!(ScalarType::ALL.len(), 12);
    }

    #[test]
    fn test_cat_from_code_all() {
        assert_eq!(
            FieldCategory::from_type_code(0x00),
            Some(FieldCategory::Scalar)
        );
        assert_eq!(
            FieldCategory::from_type_code(0x08),
            Some(FieldCategory::ScalarArray)
        );
        assert_eq!(
            FieldCategory::from_type_code(0x10),
            Some(FieldCategory::Structure)
        );
        assert_eq!(
            FieldCategory::from_type_code(0x18),
            Some(FieldCategory::StructureArray)
        );
        assert_eq!(
            FieldCategory::from_type_code(0x20),
            Some(FieldCategory::Union)
        );
        assert_eq!(
            FieldCategory::from_type_code(0x28),
            Some(FieldCategory::UnionArray)
        );
        assert_eq!(
            FieldCategory::from_type_code(0x30),
            Some(FieldCategory::BoundedString)
        );
    }
    #[test]
    fn test_cat_from_code_invalid() {
        assert_eq!(FieldCategory::from_type_code(0x38), None);
    }
    #[test]
    fn test_cat_to_bits_roundtrip() {
        for c in FieldCategory::ALL {
            assert_eq!(FieldCategory::from_type_code(c.to_bits() << 3), Some(c));
        }
    }
    #[test]
    fn test_cat_display() {
        assert_eq!(FieldCategory::Structure.to_string(), "structure");
        assert_eq!(FieldCategory::ScalarArray.to_string(), "scalar[]");
    }
    #[test]
    fn test_cat_all_count() {
        assert_eq!(FieldCategory::ALL.len(), 7);
    }

    #[test]
    fn test_ft_display_scalar() {
        assert_eq!(FieldType::Scalar(ScalarType::Int).to_string(), "int");
    }
    #[test]
    fn test_ft_display_scalar_array() {
        assert_eq!(
            FieldType::ScalarArray(ScalarType::Double).to_string(),
            "double[]"
        );
    }
    #[test]
    fn test_ft_display_bounded() {
        assert_eq!(FieldType::BoundedString(256).to_string(), "string<256>");
    }
    #[test]
    fn test_ft_display_structure() {
        assert_eq!(FieldType::Structure.to_string(), "structure");
    }
    #[test]
    fn test_ft_display_struct_array() {
        assert_eq!(FieldType::StructureArray.to_string(), "structure[]");
    }
    #[test]
    fn test_ft_display_union() {
        assert_eq!(FieldType::Union.to_string(), "union");
    }
    #[test]
    fn test_ft_display_union_array() {
        assert_eq!(FieldType::UnionArray.to_string(), "union[]");
    }
    #[test]
    fn test_ft_display_variant() {
        assert_eq!(FieldType::VariantUnion.to_string(), "any");
    }
    #[test]
    fn test_ft_display_variant_array() {
        assert_eq!(FieldType::VariantUnionArray.to_string(), "any[]");
    }

    #[test]
    fn test_fd_scalar() {
        let fd = FieldDesc::scalar(ScalarType::Double);
        assert!(fd.is_scalar());
        assert!(!fd.is_structure());
        assert!(fd.type_id.is_empty());
    }
    #[test]
    fn test_fd_scalar_array() {
        assert!(FieldDesc::scalar_array(ScalarType::Float).is_array());
    }
    #[test]
    fn test_fd_structure() {
        let fd = FieldDesc::structure("test", vec![]);
        assert!(fd.is_structure());
        assert!(!fd.is_scalar());
    }
    #[test]
    fn test_fd_structure_array() {
        assert!(FieldDesc::structure_array("test", vec![]).is_array());
    }
    #[test]
    fn test_fd_variant_union() {
        assert_eq!(
            FieldDesc::variant_union().field_type,
            FieldType::VariantUnion
        );
    }
    #[test]
    fn test_fd_bounded_string() {
        assert_eq!(
            FieldDesc::bounded_string(100).field_type,
            FieldType::BoundedString(100)
        );
    }

    #[test]
    fn test_fd_is_nt() {
        assert!(FieldDesc::structure("epics:nt/NTScalar:1.0", vec![]).is_normative_type());
    }
    #[test]
    fn test_fd_not_nt() {
        assert!(!FieldDesc::structure("alarm_t", vec![]).is_normative_type());
    }
    #[test]
    fn test_fd_nt_name() {
        assert_eq!(
            FieldDesc::structure("epics:nt/NTNDArray:1.0", vec![]).nt_type_name(),
            Some("NTNDArray")
        );
    }
    #[test]
    fn test_fd_nt_name_none() {
        assert_eq!(FieldDesc::structure("alarm_t", vec![]).nt_type_name(), None);
    }
    #[test]
    fn test_fd_nt_name_scalar() {
        assert_eq!(FieldDesc::scalar(ScalarType::Int).nt_type_name(), None);
    }

    #[test]
    fn test_fd_find() {
        let fd = FieldDesc::structure(
            "",
            vec![
                nf("value", FieldDesc::scalar(ScalarType::Double)),
                nf("ts", FieldDesc::scalar(ScalarType::Long)),
            ],
        );
        assert!(fd.find_field("value").is_some());
        assert!(fd.find_field("ts").is_some());
        assert!(fd.find_field("nope").is_none());
    }
    #[test]
    fn test_fd_find_empty() {
        assert!(FieldDesc::structure("", vec![]).find_field("x").is_none());
    }

    #[test]
    fn test_fd_flat_scalar() {
        assert_eq!(FieldDesc::scalar(ScalarType::Double).flat_field_count(), 1);
    }
    #[test]
    fn test_fd_flat_struct_2() {
        assert_eq!(
            FieldDesc::structure(
                "",
                vec![
                    nf("a", FieldDesc::scalar(ScalarType::Int)),
                    nf("b", FieldDesc::scalar(ScalarType::Float))
                ]
            )
            .flat_field_count(),
            3
        );
    }
    #[test]
    fn test_fd_flat_nested() {
        let fd = FieldDesc::structure(
            "",
            vec![
                nf(
                    "inner",
                    FieldDesc::structure(
                        "",
                        vec![
                            nf("x", FieldDesc::scalar(ScalarType::Double)),
                            nf("y", FieldDesc::scalar(ScalarType::Double)),
                        ],
                    ),
                ),
                nf("z", FieldDesc::scalar(ScalarType::Int)),
            ],
        );
        assert_eq!(fd.flat_field_count(), 5);
    }
    #[test]
    fn test_fd_flat_deep() {
        let fd = FieldDesc::structure(
            "",
            vec![nf(
                "a",
                FieldDesc::structure(
                    "",
                    vec![nf(
                        "b",
                        FieldDesc::structure("", vec![nf("c", FieldDesc::scalar(ScalarType::Int))]),
                    )],
                ),
            )],
        );
        assert_eq!(fd.flat_field_count(), 4);
    }

    #[test]
    fn test_rt_scalar_all_le() {
        for st in ScalarType::ALL {
            let o = FieldDesc::scalar(st);
            let mut w = le_w();
            o.encode(&mut w);
            assert_eq!(FieldDesc::decode_full(&mut le_r(w.as_bytes())).unwrap(), o);
        }
    }
    #[test]
    fn test_rt_scalar_all_be() {
        for st in ScalarType::ALL {
            let o = FieldDesc::scalar(st);
            let mut w = be_w();
            o.encode(&mut w);
            assert_eq!(FieldDesc::decode_full(&mut be_r(w.as_bytes())).unwrap(), o);
        }
    }
    #[test]
    fn test_rt_scalar_array_all() {
        for st in ScalarType::ALL {
            let o = FieldDesc::scalar_array(st);
            let mut w = le_w();
            o.encode(&mut w);
            assert_eq!(FieldDesc::decode_full(&mut le_r(w.as_bytes())).unwrap(), o);
        }
    }

    #[test]
    fn test_rt_empty_struct() {
        let o = FieldDesc::structure("", vec![]);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(FieldDesc::decode_full(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_rt_nt_scalar() {
        let o = FieldDesc::structure(
            "epics:nt/NTScalar:1.0",
            vec![
                nf("value", FieldDesc::scalar(ScalarType::Double)),
                nf(
                    "alarm",
                    FieldDesc::structure(
                        "alarm_t",
                        vec![
                            nf("severity", FieldDesc::scalar(ScalarType::Int)),
                            nf("status", FieldDesc::scalar(ScalarType::Int)),
                            nf("message", FieldDesc::scalar(ScalarType::String)),
                        ],
                    ),
                ),
                nf(
                    "timeStamp",
                    FieldDesc::structure(
                        "time_t",
                        vec![
                            nf("secondsPastEpoch", FieldDesc::scalar(ScalarType::Long)),
                            nf("nanoseconds", FieldDesc::scalar(ScalarType::Int)),
                        ],
                    ),
                ),
            ],
        );
        let mut w = le_w();
        o.encode(&mut w);
        let d = FieldDesc::decode_full(&mut le_r(w.as_bytes())).unwrap();
        assert_eq!(d, o);
    }

    #[test]
    fn test_rt_nt_scalar_be() {
        let o = FieldDesc::structure(
            "epics:nt/NTScalar:1.0",
            vec![nf("value", FieldDesc::scalar(ScalarType::Double))],
        );
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(FieldDesc::decode_full(&mut be_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_rt_bounded_string() {
        let o = FieldDesc::bounded_string(256);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(FieldDesc::decode_full(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_decode_null() {
        assert!(
            FieldDesc::decode(
                &mut le_r(&[NULL_TYPE_CODE]),
                &mut IntrospectionRegistry::default()
            )
            .unwrap()
            .is_none()
        );
    }
    #[test]
    fn test_decode_only_id_found() {
        let mut w = le_w();
        w.write_u8(ONLY_ID_TYPE_CODE);
        w.write_i16(7);
        let mut reg = IntrospectionRegistry::default();
        reg.register(7, FieldDesc::scalar(ScalarType::Double));
        assert_eq!(
            FieldDesc::decode(&mut le_r(w.as_bytes()), &mut reg)
                .unwrap()
                .unwrap(),
            FieldDesc::scalar(ScalarType::Double)
        );
    }
    #[test]
    fn test_decode_only_id_missing() {
        let mut w = le_w();
        w.write_u8(ONLY_ID_TYPE_CODE);
        w.write_i16(42);
        assert!(
            FieldDesc::decode(
                &mut le_r(w.as_bytes()),
                &mut IntrospectionRegistry::default()
            )
            .is_err()
        );
    }
    #[test]
    fn test_decode_full_with_id() {
        let mut w = le_w();
        w.write_u8(FULL_WITH_ID_CODE);
        w.write_i16(99);
        FieldDesc::scalar(ScalarType::Int).encode(&mut w);
        let result = FieldDesc::decode(
            &mut le_r(w.as_bytes()),
            &mut IntrospectionRegistry::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.field_type, FieldType::Scalar(ScalarType::Int));
    }

    #[test]
    fn test_decode_empty() {
        assert!(FieldDesc::decode(&mut le_r(&[]), &mut IntrospectionRegistry::default()).is_err());
    }
    #[test]
    fn test_decode_bad_type_code() {
        assert!(FieldDesc::decode_type_code(0x38, &mut le_r(&[])).is_err());
    } // category 7 invalid
    #[test]
    fn test_decode_truncated_struct() {
        let mut w = le_w();
        w.write_u8(2 << 3); // structure, but no type_id or fields
        assert!(FieldDesc::decode_full(&mut le_r(w.as_bytes())).is_err());
    }

    #[test]
    fn test_reg_new() {
        let r = IntrospectionRegistry::new(64);
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
        assert!(!r.is_full());
    }
    #[test]
    fn test_reg_default() {
        assert_eq!(IntrospectionRegistry::default().len(), 0);
    }
    #[test]
    fn test_reg_register_get() {
        let mut r = IntrospectionRegistry::new(64);
        r.register(1, FieldDesc::scalar(ScalarType::Double));
        assert_eq!(r.len(), 1);
        assert!(r.get(1).is_some());
        assert!(r.get(2).is_none());
    }
    #[test]
    fn test_reg_overwrite() {
        let mut r = IntrospectionRegistry::new(64);
        r.register(1, FieldDesc::scalar(ScalarType::Int));
        r.register(1, FieldDesc::scalar(ScalarType::Double));
        assert_eq!(r.len(), 1);
        assert_eq!(
            r.get(1).unwrap().field_type,
            FieldType::Scalar(ScalarType::Double)
        );
    }
    #[test]
    fn test_reg_full() {
        let mut r = IntrospectionRegistry::new(2);
        assert!(r.register(1, FieldDesc::scalar(ScalarType::Int)));
        assert!(r.register(2, FieldDesc::scalar(ScalarType::Float)));
        assert!(!r.register(3, FieldDesc::scalar(ScalarType::Double))); // rejected
        assert_eq!(r.len(), 2);
        assert!(r.is_full());
    }
    #[test]
    fn test_reg_clear() {
        let mut r = IntrospectionRegistry::new(64);
        r.register(1, FieldDesc::scalar(ScalarType::Int));
        r.clear();
        assert!(r.is_empty());
    }
    #[test]
    fn test_reg_negative_id() {
        let mut r = IntrospectionRegistry::new(64);
        r.register(-1, FieldDesc::scalar(ScalarType::Int));
        assert!(r.get(-1).is_some());
    }

    #[test]
    fn test_nt_all_start_with_prefix() {
        for id in nt_ids::ALL {
            assert!(id.starts_with("epics:nt/"), "{id}");
        }
    }
    #[test]
    fn test_nt_all_have_version() {
        for id in nt_ids::ALL {
            assert!(id.ends_with(":1.0"), "{id}");
        }
    }
    #[test]
    fn test_nt_all_unique() {
        use std::collections::HashSet;
        assert_eq!(nt_ids::ALL.iter().collect::<HashSet<_>>().len(), 12);
    }
    #[test]
    fn test_nt_scalar() {
        assert_eq!(nt_ids::NT_SCALAR, "epics:nt/NTScalar:1.0");
    }
    #[test]
    fn test_nt_ndarray() {
        assert!(nt_ids::NT_NDARRAY.contains("NTNDArray"));
    }
    #[test]
    fn test_nt_enum() {
        assert!(nt_ids::NT_ENUM.contains("NTEnum"));
    }

    #[test]
    fn test_fd_display_scalar() {
        assert_eq!(FieldDesc::scalar(ScalarType::Double).to_string(), "double");
    }
    #[test]
    fn test_fd_display_struct() {
        let s = FieldDesc::structure(
            "epics:nt/NTScalar:1.0",
            vec![nf("v", FieldDesc::scalar(ScalarType::Double))],
        )
        .to_string();
        assert!(s.contains("structure"));
        assert!(s.contains("NTScalar"));
        assert!(s.contains("1 fields"));
    }
    #[test]
    fn test_fd_display_no_type_id() {
        assert_eq!(FieldDesc::structure("", vec![]).to_string(), "structure");
    }
    #[test]
    fn test_fd_clone() {
        let a = FieldDesc::scalar(ScalarType::Int);
        assert_eq!(a.clone(), a);
    }
    #[test]
    fn test_fd_debug() {
        assert!(format!("{:?}", FieldDesc::scalar(ScalarType::Int)).contains("FieldDesc"));
    }
}