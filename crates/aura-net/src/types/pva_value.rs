//! Runtime PVA value representation.
//!
//! Uses `ScalarValue` from aura-core directly — no duplication.
//!
//! ## Data flow (no intermediate type)
//!
//! ```text
//! wire bytes -> PvaReader -> ScalarValue (aura-core) -> MonitorEvent -> Bus
//! ```

use crate::codec::FieldType;
use crate::codec::field_desc::FieldDesc;
use crate::codec::pvdata::{DecodeError, PvaReader};
use aura_core::pva::scalars::{ScalarType, ScalarValue};
use std::fmt;

/// Decode a scalar from PVA wire → aura-core ScalarValue directly.
pub fn decode_scalar(
    reader: &mut PvaReader<'_>,
    stype: ScalarType,
) -> Result<ScalarValue, DecodeError> {
    Ok(match stype {
        ScalarType::Boolean => ScalarValue::Boolean(reader.read_bool()?),
        ScalarType::Byte => ScalarValue::Byte(reader.read_i8()?),
        ScalarType::UByte => ScalarValue::UByte(reader.read_u8()?),
        ScalarType::Short => ScalarValue::Short(reader.read_i16()?),
        ScalarType::UShort => ScalarValue::UShort(reader.read_u16()?),
        ScalarType::Int => ScalarValue::Int(reader.read_i32()?),
        ScalarType::UInt => ScalarValue::UInt(reader.read_u32()?),
        ScalarType::Long => ScalarValue::Long(reader.read_i64()?),
        ScalarType::ULong => ScalarValue::ULong(reader.read_u64()?),
        ScalarType::Float => ScalarValue::Float(reader.read_f32()?),
        ScalarType::Double => ScalarValue::Double(reader.read_f64()?),
        ScalarType::String => ScalarValue::String(reader.read_string()?),
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum PvaValue {
    Scalar(ScalarValue),
    ScalarArray(Vec<ScalarValue>),
    Structure(Vec<(std::sync::Arc<str>, PvaValue)>),
    Union(std::sync::Arc<str>, Box<PvaValue>),
    Null,
}

impl PvaValue {
    pub fn decode_from(reader: &mut PvaReader<'_>, desc: &FieldDesc) -> Result<Self, DecodeError> {
        match &desc.field_type {
            FieldType::Scalar(st) => Ok(Self::Scalar(decode_scalar(reader, *st)?)),
            FieldType::ScalarArray(st) => {
                let len = reader.read_size_non_null()?;
                if len == 0 {
                    return Ok(Self::ScalarArray(Vec::new()));
                }
                if *st == ScalarType::UByte {
                    let bytes = reader.read_bytes(len)?;
                    return Ok(Self::ScalarArray(
                        bytes.iter().map(|&b| ScalarValue::UByte(b)).collect(),
                    ));
                }
                let mut arr = Vec::with_capacity(len.min(65536));
                for _ in 0..len {
                    arr.push(decode_scalar(reader, *st)?);
                }
                Ok(Self::ScalarArray(arr))
            }
            FieldType::Structure | FieldType::StructureArray => {
                let mut fields = Vec::with_capacity(desc.fields.len());
                for nf in &desc.fields {
                    fields.push((nf.name.clone(), Self::decode_from(reader, &nf.desc)?));
                }
                Ok(Self::Structure(fields))
            }
            FieldType::BoundedString(max_len) => {
                let s = reader.read_string()?;
                if s.len() > *max_len {
                    return Err(DecodeError::Protocol(format!(
                        "bounded string exceeds max {max_len}: got {}",
                        s.len()
                    )));
                }
                Ok(Self::Scalar(ScalarValue::String(s)))
            }
            FieldType::Union | FieldType::VariantUnion => {
                let selector = reader.read_i32()?;
                if selector < 0 {
                    return Ok(Self::Null);
                }
                if matches!(desc.field_type, FieldType::VariantUnion) {
                    let tag = reader.read_u8()?;
                    if let Some(st) = ScalarType::from_wire(tag & 0x07) {
                        return Ok(Self::Scalar(decode_scalar(reader, st)?));
                    }
                    return Ok(Self::Null);
                }
                let idx = selector as usize;
                if idx >= desc.fields.len() {
                    return Err(DecodeError::Protocol(format!(
                        "union selector {idx} out of range (max {})",
                        desc.fields.len()
                    )));
                }
                let nf = &desc.fields[idx];
                Ok(Self::Union(
                    nf.name.clone(),
                    Box::new(Self::decode_from(reader, &nf.desc)?),
                ))
            }
            _ => Ok(Self::Null),
        }
    }

    #[inline]
    pub fn as_scalar(&self) -> Option<&ScalarValue> {
        match self {
            Self::Scalar(s) => Some(s),
            _ => None,
        }
    }
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        self.as_scalar().and_then(|s| s.as_f64())
    }
    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        self.as_scalar().and_then(|s| s.as_i64())
    }
    #[inline]
    pub fn as_i32(&self) -> Option<i32> {
        self.as_scalar().and_then(|s| s.as_i32())
    }
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::Scalar(ScalarValue::String(s)) => Some(s),
            _ => None,
        }
    }
    pub fn as_scalar_array(&self) -> Option<&[ScalarValue]> {
        match self {
            Self::ScalarArray(a) => Some(a),
            _ => None,
        }
    }

    pub fn field(&self, name: &str) -> Option<&PvaValue> {
        match self {
            Self::Structure(f) => f.iter().find(|(n, _)| &**n == name).map(|(_, v)| v),
            _ => None,
        }
    }
    #[inline]
    pub fn field_by_index(&self, idx: usize) -> Option<&PvaValue> {
        match self {
            Self::Structure(f) => f.get(idx).map(|(_, v)| v),
            _ => None,
        }
    }
    #[inline]
    pub fn field_by_index_mut(&mut self, idx: usize) -> Option<&mut PvaValue> {
        match self {
            Self::Structure(f) => f.get_mut(idx).map(|(_, v)| v),
            _ => None,
        }
    }
    pub fn field_mut(&mut self, name: &str) -> Option<&mut PvaValue> {
        match self {
            Self::Structure(f) => f.iter_mut().find(|(n, _)| &**n == name).map(|(_, v)| v),
            _ => None,
        }
    }
    pub fn field_f64(&self, name: &str) -> Option<f64> {
        self.field(name).and_then(|v| v.as_f64())
    }
    pub fn field_i32(&self, name: &str) -> Option<i32> {
        self.field(name).and_then(|v| v.as_i32())
    }
    pub fn field_i64(&self, name: &str) -> Option<i64> {
        self.field(name).and_then(|v| v.as_i64())
    }
    pub fn field_string(&self, name: &str) -> Option<&str> {
        self.field(name).and_then(|v| v.as_string())
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
    #[inline]
    pub fn is_structure(&self) -> bool {
        matches!(self, Self::Structure(_))
    }
    #[inline]
    pub fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(_))
    }
    #[inline]
    pub fn is_array(&self) -> bool {
        matches!(self, Self::ScalarArray(_))
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Structure(f) => f.len(),
            Self::ScalarArray(a) => a.len(),
            _ => 0,
        }
    }
    pub fn is_empty_value(&self) -> bool {
        self.len() == 0 && !self.is_scalar() && !self.is_null()
    }
}

impl fmt::Display for PvaValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(s) => write!(f, "{s}"),
            Self::ScalarArray(a) => write!(f, "[{} elements]", a.len()),
            Self::Structure(fields) => write!(f, "{{{} fields}}", fields.len()),
            Self::Union(name, val) => write!(f, "union({name}: {val})"),
            Self::Null => write!(f, "null"),
        }
    }
}

/// Backwards-compatible alias: `PvaScalar` = `ScalarValue` from aura-core.
pub type PvaScalar = ScalarValue;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::field_desc::NamedField;
    use crate::codec::header::ByteOrder;
    use crate::codec::pvdata::PvaWriter;
    use std::sync::Arc;

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
    fn nf(n: &str, d: FieldDesc) -> NamedField {
        NamedField {
            name: n.into(),
            desc: d,
        }
    }

    #[test]
    fn test_dec_bool_t() {
        assert_eq!(
            decode_scalar(&mut le_r(&[1]), ScalarType::Boolean).unwrap(),
            ScalarValue::Boolean(true)
        );
    }
    #[test]
    fn test_dec_bool_f() {
        assert_eq!(
            decode_scalar(&mut le_r(&[0]), ScalarType::Boolean).unwrap(),
            ScalarValue::Boolean(false)
        );
    }
    #[test]
    fn test_dec_byte() {
        assert_eq!(
            decode_scalar(&mut le_r(&[0xFF]), ScalarType::Byte).unwrap(),
            ScalarValue::Byte(-1)
        );
    }
    #[test]
    fn test_dec_ubyte() {
        assert_eq!(
            decode_scalar(&mut le_r(&[0xFF]), ScalarType::UByte).unwrap(),
            ScalarValue::UByte(255)
        );
    }
    #[test]
    fn test_dec_short() {
        let mut w = le_w();
        w.write_i16(-1000);
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::Short).unwrap(),
            ScalarValue::Short(-1000)
        );
    }
    #[test]
    fn test_dec_ushort() {
        let mut w = le_w();
        w.write_u16(60000);
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::UShort).unwrap(),
            ScalarValue::UShort(60000)
        );
    }
    #[test]
    fn test_dec_int() {
        let mut w = le_w();
        w.write_i32(42);
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::Int).unwrap(),
            ScalarValue::Int(42)
        );
    }
    #[test]
    fn test_dec_uint() {
        let mut w = le_w();
        w.write_u32(0xDEADBEEF);
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::UInt).unwrap(),
            ScalarValue::UInt(0xDEADBEEF)
        );
    }
    #[test]
    fn test_dec_long() {
        let mut w = le_w();
        w.write_i64(i64::MIN);
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::Long).unwrap(),
            ScalarValue::Long(i64::MIN)
        );
    }
    #[test]
    fn test_dec_ulong() {
        let mut w = le_w();
        w.write_u64(u64::MAX);
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::ULong).unwrap(),
            ScalarValue::ULong(u64::MAX)
        );
    }
    #[test]
    fn test_dec_float() {
        assert!(
            (decode_scalar(
                &mut le_r(
                    &{
                        let mut w = le_w();
                        w.write_f32(3.14);
                        w
                    }
                    .as_bytes()
                ),
                ScalarType::Float
            )
            .unwrap()
            .as_f64()
            .unwrap()
                - 3.14)
                .abs()
                < 0.01
        );
    }
    #[test]
    fn test_dec_double() {
        let mut w = le_w();
        w.write_f64(std::f64::consts::PI);
        assert!(
            (decode_scalar(&mut le_r(w.as_bytes()), ScalarType::Double)
                .unwrap()
                .as_f64()
                .unwrap()
                - std::f64::consts::PI)
                .abs()
                < 1e-15
        );
    }
    #[test]
    fn test_dec_string() {
        let mut w = le_w();
        w.write_string("café");
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::String).unwrap(),
            ScalarValue::String("café".into())
        );
    }
    #[test]
    fn test_dec_string_empty() {
        let mut w = le_w();
        w.write_string("");
        assert_eq!(
            decode_scalar(&mut le_r(w.as_bytes()), ScalarType::String).unwrap(),
            ScalarValue::String(String::new())
        );
    }
    #[test]
    fn test_dec_int_be() {
        let mut w = be_w();
        w.write_i32(42);
        assert_eq!(
            decode_scalar(&mut be_r(w.as_bytes()), ScalarType::Int).unwrap(),
            ScalarValue::Int(42)
        );
    }
    #[test]
    fn test_dec_double_be() {
        let mut w = be_w();
        w.write_f64(2.718);
        assert!(
            (decode_scalar(&mut be_r(w.as_bytes()), ScalarType::Double)
                .unwrap()
                .as_f64()
                .unwrap()
                - 2.718)
                .abs()
                < 1e-10
        );
    }
    #[test]
    fn test_dec_err_empty() {
        assert!(decode_scalar(&mut le_r(&[]), ScalarType::Int).is_err());
    }
    #[test]
    fn test_dec_err_truncated() {
        assert!(decode_scalar(&mut le_r(&[1, 2]), ScalarType::Int).is_err());
    }

    #[test]
    fn test_vdec_scalar() {
        let mut w = le_w();
        w.write_f64(2.718);
        assert!(
            (PvaValue::decode_from(
                &mut le_r(w.as_bytes()),
                &FieldDesc::scalar(ScalarType::Double)
            )
            .unwrap()
            .as_f64()
            .unwrap()
                - 2.718)
                .abs()
                < 1e-10
        );
    }
    #[test]
    fn test_vdec_scalar_be() {
        let mut w = be_w();
        w.write_i32(99);
        assert_eq!(
            PvaValue::decode_from(&mut be_r(w.as_bytes()), &FieldDesc::scalar(ScalarType::Int))
                .unwrap()
                .as_i32(),
            Some(99)
        );
    }
    #[test]
    fn test_vdec_array() {
        let mut w = le_w();
        w.write_size(3);
        w.write_i32(10);
        w.write_i32(20);
        w.write_i32(30);
        assert_eq!(
            PvaValue::decode_from(
                &mut le_r(w.as_bytes()),
                &FieldDesc::scalar_array(ScalarType::Int)
            )
            .unwrap()
            .len(),
            3
        );
    }
    #[test]
    fn test_vdec_array_empty() {
        let mut w = le_w();
        w.write_size(0);
        assert_eq!(
            PvaValue::decode_from(
                &mut le_r(w.as_bytes()),
                &FieldDesc::scalar_array(ScalarType::Double)
            )
            .unwrap()
            .len(),
            0
        );
    }
    #[test]
    fn test_vdec_array_trunc() {
        let mut w = le_w();
        w.write_size(100);
        w.write_i32(1);
        assert!(
            PvaValue::decode_from(
                &mut le_r(w.as_bytes()),
                &FieldDesc::scalar_array(ScalarType::Int)
            )
            .is_err()
        );
    }
    #[test]
    fn test_vdec_struct() {
        let d = FieldDesc::structure(
            "",
            vec![
                nf("x", FieldDesc::scalar(ScalarType::Double)),
                nf("y", FieldDesc::scalar(ScalarType::Int)),
            ],
        );
        let mut w = le_w();
        w.write_f64(1.5);
        w.write_i32(42);
        let v = PvaValue::decode_from(&mut le_r(w.as_bytes()), &d).unwrap();
        assert!(v.is_structure());
        assert_eq!(v.field_i32("y"), Some(42));
    }
    #[test]
    fn test_vdec_nested() {
        let d = FieldDesc::structure(
            "",
            vec![nf(
                "a",
                FieldDesc::structure("", vec![nf("s", FieldDesc::scalar(ScalarType::Int))]),
            )],
        );
        let mut w = le_w();
        w.write_i32(2);
        assert_eq!(
            PvaValue::decode_from(&mut le_r(w.as_bytes()), &d)
                .unwrap()
                .field("a")
                .unwrap()
                .field_i32("s"),
            Some(2)
        );
    }
    #[test]
    fn test_vdec_bounded_ok() {
        let mut w = le_w();
        w.write_string("hi");
        assert_eq!(
            PvaValue::decode_from(&mut le_r(w.as_bytes()), &FieldDesc::bounded_string(10))
                .unwrap()
                .as_string(),
            Some("hi")
        );
    }
    #[test]
    fn test_vdec_bounded_err() {
        let mut w = le_w();
        w.write_string("toolong");
        assert!(
            PvaValue::decode_from(&mut le_r(w.as_bytes()), &FieldDesc::bounded_string(3)).is_err()
        );
    }
    #[test]
    fn test_vdec_union() {
        let d = FieldDesc::union_type(
            "",
            vec![
                nf("a", FieldDesc::scalar(ScalarType::Int)),
                nf("b", FieldDesc::scalar(ScalarType::Double)),
            ],
        );
        let mut w = le_w();
        w.write_i32(1);
        w.write_f64(3.14);
        match PvaValue::decode_from(&mut le_r(w.as_bytes()), &d).unwrap() {
            PvaValue::Union(n, v) => {
                assert_eq!(n, Arc::from("b"));
                assert!((v.as_f64().unwrap() - 3.14).abs() < 1e-10);
            }
            _ => panic!(),
        }
    }
    #[test]
    fn test_vdec_union_neg() {
        let d = FieldDesc::union_type("", vec![nf("a", FieldDesc::scalar(ScalarType::Int))]);
        let mut w = le_w();
        w.write_i32(-1);
        assert!(
            PvaValue::decode_from(&mut le_r(w.as_bytes()), &d)
                .unwrap()
                .is_null()
        );
    }
    #[test]
    fn test_vdec_union_oor() {
        let d = FieldDesc::union_type("", vec![nf("a", FieldDesc::scalar(ScalarType::Int))]);
        let mut w = le_w();
        w.write_i32(5);
        assert!(PvaValue::decode_from(&mut le_r(w.as_bytes()), &d).is_err());
    }

    #[test]
    fn test_as_scalar() {
        assert_eq!(
            PvaValue::Scalar(ScalarValue::Int(42)).as_scalar(),
            Some(&ScalarValue::Int(42))
        );
    }
    #[test]
    fn test_as_scalar_none() {
        assert!(PvaValue::Null.as_scalar().is_none());
    }
    #[test]
    fn test_as_f64() {
        assert_eq!(
            PvaValue::Scalar(ScalarValue::Double(3.14)).as_f64(),
            Some(3.14)
        );
    }
    #[test]
    fn test_as_i64() {
        assert_eq!(PvaValue::Scalar(ScalarValue::Long(99)).as_i64(), Some(99));
    }
    #[test]
    fn test_as_i32() {
        assert_eq!(PvaValue::Scalar(ScalarValue::Short(7)).as_i32(), Some(7));
    }
    #[test]
    fn test_as_string() {
        assert_eq!(
            PvaValue::Scalar(ScalarValue::String("hi".into())).as_string(),
            Some("hi")
        );
    }
    #[test]
    fn test_as_string_none() {
        assert!(PvaValue::Scalar(ScalarValue::Int(42)).as_string().is_none());
    }
    #[test]
    fn test_as_array() {
        assert_eq!(
            PvaValue::ScalarArray(vec![ScalarValue::Int(1)])
                .as_scalar_array()
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn test_field() {
        assert!(
            PvaValue::Structure(vec![("x".into(), PvaValue::Scalar(ScalarValue::Int(1)))])
                .field("x")
                .is_some()
        );
    }
    #[test]
    fn test_field_miss() {
        assert!(PvaValue::Structure(vec![]).field("x").is_none());
    }
    #[test]
    fn test_field_by_idx() {
        let v = PvaValue::Structure(vec![("a".into(), PvaValue::Scalar(ScalarValue::Int(10)))]);
        assert_eq!(v.field_by_index(0).unwrap().as_i32(), Some(10));
    }
    #[test]
    fn test_field_by_idx_mut() {
        let mut v = PvaValue::Structure(vec![("a".into(), PvaValue::Scalar(ScalarValue::Int(1)))]);
        *v.field_by_index_mut(0).unwrap() = PvaValue::Scalar(ScalarValue::Int(99));
        assert_eq!(v.field_by_index(0).unwrap().as_i32(), Some(99));
    }
    #[test]
    fn test_field_mut() {
        let mut v = PvaValue::Structure(vec![(
            "x".into(),
            PvaValue::Scalar(ScalarValue::Double(1.0)),
        )]);
        *v.field_mut("x").unwrap() = PvaValue::Scalar(ScalarValue::Double(9.9));
        assert_eq!(v.field_f64("x"), Some(9.9));
    }
    #[test]
    fn test_byte_array() {
        let mut w = le_w();
        w.write_size(4);
        w.write_raw(&[0xAA, 0xBB, 0xCC, 0xDD]);
        let v = PvaValue::decode_from(
            &mut le_r(w.as_bytes()),
            &FieldDesc::scalar_array(ScalarType::UByte),
        )
        .unwrap();
        assert_eq!(v.as_scalar_array().unwrap()[0], ScalarValue::UByte(0xAA));
    }

    #[test]
    fn test_is_null() {
        assert!(PvaValue::Null.is_null());
    }
    #[test]
    fn test_is_struct() {
        assert!(PvaValue::Structure(vec![]).is_structure());
    }
    #[test]
    fn test_is_scalar() {
        assert!(PvaValue::Scalar(ScalarValue::Int(0)).is_scalar());
    }
    #[test]
    fn test_is_array() {
        assert!(PvaValue::ScalarArray(vec![]).is_array());
    }
    #[test]
    fn test_len() {
        assert_eq!(
            PvaValue::Structure(vec![("a".into(), PvaValue::Null)]).len(),
            1
        );
    }
    #[test]
    fn test_empty_val() {
        assert!(PvaValue::Structure(vec![]).is_empty_value());
        assert!(!PvaValue::Null.is_empty_value());
    }

    #[test]
    fn test_display() {
        assert_eq!(PvaValue::Scalar(ScalarValue::Int(42)).to_string(), "42");
        assert_eq!(PvaValue::Null.to_string(), "null");
    }
    #[test]
    fn test_clone() {
        let a = PvaValue::Scalar(ScalarValue::Int(42));
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_alias() {
        let v: PvaScalar = PvaScalar::Double(3.14);
        assert_eq!(v.as_f64(), Some(3.14));
    }
}
