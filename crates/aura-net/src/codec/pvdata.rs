//! pvData encoding/decoding — primitive types, sizes, strings, arrays.
//!
//! This module implements the pvData wire format from the PVAccess
//! Protocol Specification. It is the serialization layer for all structured data on the wire.
//!
//! ## Encoding rules
//!
//! - No alignment for primitives (contiguous byte stream).
//! - Sizes: <254 = 1 byte, 254 = 4-byte i32 follows, 255 = null.
//! - Strings: size + UTF-8 bytes (no null terminator).
//! - Byte order: determined per-connection (negotiated at handshake).

use super::header::ByteOrder;

/// Special size value indicating null/absent data.
pub const SIZE_NULL: u8 = 255;

/// Threshold for extended size encoding.
const SIZE_EXTENDED: u8 = 254;

/// Reader for pvData-encoded bytes.
pub struct PvaReader<'a> {
    buf: &'a [u8],
    pos: usize,
    order: ByteOrder,
}

impl<'a> PvaReader<'a> {
    /// Create a reader over a byte slice.
    pub fn new(buf: &'a [u8], order: ByteOrder) -> Self {
        Self { buf, pos: 0, order }
    }

    /// Remaining bytes available.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Current cursor position.
    #[inline]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Whether all bytes have been consumed.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    /// Peek at the next byte without advancing.
    #[inline]
    pub fn peek(&self) -> Result<u8, DecodeError> {
        if self.pos < self.buf.len() {
            Ok(self.buf[self.pos])
        } else {
            Err(DecodeError::UnexpectedEnd {
                needed: 1,
                available: 0,
            })
        }
    }

    /// Skip N bytes.
    #[inline]
    pub fn skip(&mut self, n: usize) -> Result<(), DecodeError> {
        self.ensure(n)?;
        self.pos += n;
        Ok(())
    }

    /// Read a raw byte slice.
    #[inline]
    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        self.ensure(n)?;
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }


    #[inline]
    pub fn read_u8(&mut self) -> Result<u8, DecodeError> {
        self.ensure(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    #[inline]
    pub fn read_i8(&mut self) -> Result<i8, DecodeError> {
        Ok(self.read_u8()? as i8)
    }

    #[inline]
    pub fn read_bool(&mut self) -> Result<bool, DecodeError> {
        Ok(self.read_u8()? != 0)
    }

    #[inline]
    pub fn read_i16(&mut self) -> Result<i16, DecodeError> {
        let bytes = self.read_fixed::<2>()?;
        Ok(self.order.read_i16(bytes))
    }

    #[inline]
    pub fn read_u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.read_fixed::<2>()?;
        Ok(self.order.read_u16(bytes))
    }

    #[inline]
    pub fn read_i32(&mut self) -> Result<i32, DecodeError> {
        let bytes = self.read_fixed::<4>()?;
        Ok(self.order.read_i32(bytes))
    }

    #[inline]
    pub fn read_u32(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.read_fixed::<4>()?;
        Ok(self.order.read_u32(bytes))
    }

    #[inline]
    pub fn read_i64(&mut self) -> Result<i64, DecodeError> {
        let bytes = self.read_fixed::<8>()?;
        Ok(self.order.read_i64(bytes))
    }

    #[inline]
    pub fn read_u64(&mut self) -> Result<u64, DecodeError> {
        let bytes = self.read_fixed::<8>()?;
        Ok(self.order.read_u64(bytes))
    }

    #[inline]
    pub fn read_f32(&mut self) -> Result<f32, DecodeError> {
        let bytes = self.read_fixed::<4>()?;
        Ok(self.order.read_f32(bytes))
    }

    #[inline]
    pub fn read_f64(&mut self) -> Result<f64, DecodeError> {
        let bytes = self.read_fixed::<8>()?;
        Ok(self.order.read_f64(bytes))
    }

    /// Read a PVA size value.
    ///
    /// Returns `None` for null (255), `Some(n)` otherwise.
    /// - byte < 254: size is that byte
    /// - byte == 254: size is next i32
    /// - byte == 255: null
    pub fn read_size(&mut self) -> Result<Option<usize>, DecodeError> {
        let tag = self.read_u8()?;
        match tag {
            SIZE_NULL => Ok(None),
            SIZE_EXTENDED => {
                let n = self.read_i32()?;
                if n < 0 {
                    return Err(DecodeError::InvalidSize(n as i64));
                }
                Ok(Some(n as usize))
            }
            n => Ok(Some(n as usize)),
        }
    }

    /// Read a non-null size (returns error on null).
    pub fn read_size_non_null(&mut self) -> Result<usize, DecodeError> {
        self.read_size()?.ok_or(DecodeError::UnexpectedNull)
    }

    /// Read a PVA string (size + UTF-8 bytes).
    pub fn read_string(&mut self) -> Result<String, DecodeError> {
        let len = self.read_size_non_null()?;
        if len == 0 {
            return Ok(String::new());
        }
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|e| DecodeError::InvalidUtf8(e.to_string()))
    }

    /// Read an optional string (null = None).
    pub fn read_string_opt(&mut self) -> Result<Option<String>, DecodeError> {
        match self.read_size()? {
            None => Ok(None),
            Some(0) => Ok(Some(String::new())),
            Some(len) => {
                let bytes = self.read_bytes(len)?;
                let s = String::from_utf8(bytes.to_vec())
                    .map_err(|e| DecodeError::InvalidUtf8(e.to_string()))?;
                Ok(Some(s))
            }
        }
    }

    /// Read an array of f64 values.
    pub fn read_f64_array(&mut self) -> Result<Vec<f64>, DecodeError> {
        let len = self.read_size_non_null()?;
        let mut arr = Vec::with_capacity(len.min(65536));
        for _ in 0..len {
            arr.push(self.read_f64()?);
        }
        Ok(arr)
    }

    /// Read an array of i32 values.
    pub fn read_i32_array(&mut self) -> Result<Vec<i32>, DecodeError> {
        let len = self.read_size_non_null()?;
        let mut arr = Vec::with_capacity(len.min(65536));
        for _ in 0..len {
            arr.push(self.read_i32()?);
        }
        Ok(arr)
    }

    /// Read an array of u8 values (byte array — bulk read).
    pub fn read_byte_array(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.read_size_non_null()?;
        let bytes = self.read_bytes(len)?;
        Ok(bytes.to_vec())
    }

    /// Read an array of strings.
    pub fn read_string_array(&mut self) -> Result<Vec<String>, DecodeError> {
        let len = self.read_size_non_null()?;
        let mut arr = Vec::with_capacity(len.min(4096));
        for _ in 0..len {
            arr.push(self.read_string()?);
        }
        Ok(arr)
    }

    #[inline]
    fn ensure(&self, n: usize) -> Result<(), DecodeError> {
        if self.pos + n > self.buf.len() {
            Err(DecodeError::UnexpectedEnd {
                needed: n,
                available: self.buf.len() - self.pos,
            })
        } else {
            Ok(())
        }
    }

    #[inline]
    fn read_fixed<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        self.ensure(N)?;
        let mut arr = [0u8; N];
        arr.copy_from_slice(&self.buf[self.pos..self.pos + N]);
        self.pos += N;
        Ok(arr)
    }
}

/// Writer for pvData-encoded bytes.
pub struct PvaWriter {
    buf: Vec<u8>,
    order: ByteOrder,
}

impl PvaWriter {
    /// Create a writer with initial capacity.
    pub fn new(order: ByteOrder) -> Self {
        Self {
            buf: Vec::with_capacity(256),
            order,
        }
    }

    /// Create with specific capacity.
    pub fn with_capacity(capacity: usize, order: ByteOrder) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            order,
        }
    }

    /// Get the accumulated bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Current length of written data.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether no data has been written.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Reference to the written bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Clear the buffer for reuse (retains capacity).
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    #[inline]
    pub fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    #[inline]
    pub fn write_i8(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    #[inline]
    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    #[inline]
    pub fn write_i16(&mut self, v: i16) {
        let bytes = match self.order {
            ByteOrder::LittleEndian => v.to_le_bytes(),
            ByteOrder::BigEndian => v.to_be_bytes(),
        };
        self.buf.extend_from_slice(&bytes);
    }

    #[inline]
    pub fn write_u16(&mut self, v: u16) {
        let bytes = match self.order {
            ByteOrder::LittleEndian => v.to_le_bytes(),
            ByteOrder::BigEndian => v.to_be_bytes(),
        };
        self.buf.extend_from_slice(&bytes);
    }

    #[inline]
    pub fn write_i32(&mut self, v: i32) {
        let bytes = self.order.write_i32(v);
        self.buf.extend_from_slice(&bytes);
    }

    #[inline]
    pub fn write_u32(&mut self, v: u32) {
        let bytes = match self.order {
            ByteOrder::LittleEndian => v.to_le_bytes(),
            ByteOrder::BigEndian => v.to_be_bytes(),
        };
        self.buf.extend_from_slice(&bytes);
    }

    #[inline]
    pub fn write_i64(&mut self, v: i64) {
        let bytes = match self.order {
            ByteOrder::LittleEndian => v.to_le_bytes(),
            ByteOrder::BigEndian => v.to_be_bytes(),
        };
        self.buf.extend_from_slice(&bytes);
    }

    #[inline]
    pub fn write_u64(&mut self, v: u64) {
        let bytes = match self.order {
            ByteOrder::LittleEndian => v.to_le_bytes(),
            ByteOrder::BigEndian => v.to_be_bytes(),
        };
        self.buf.extend_from_slice(&bytes);
    }

    #[inline]
    pub fn write_f32(&mut self, v: f32) {
        let bytes = match self.order {
            ByteOrder::LittleEndian => v.to_le_bytes(),
            ByteOrder::BigEndian => v.to_be_bytes(),
        };
        self.buf.extend_from_slice(&bytes);
    }

    #[inline]
    pub fn write_f64(&mut self, v: f64) {
        let bytes = match self.order {
            ByteOrder::LittleEndian => v.to_le_bytes(),
            ByteOrder::BigEndian => v.to_be_bytes(),
        };
        self.buf.extend_from_slice(&bytes);
    }

    /// Write a PVA size value.
    pub fn write_size(&mut self, size: usize) {
        if size < SIZE_EXTENDED as usize {
            self.write_u8(size as u8);
        } else {
            self.write_u8(SIZE_EXTENDED);
            self.write_i32(size as i32);
        }
    }

    /// Write a null size (255).
    pub fn write_size_null(&mut self) {
        self.write_u8(SIZE_NULL);
    }

    /// Write a PVA string (size + UTF-8 bytes).
    pub fn write_string(&mut self, s: &str) {
        self.write_size(s.len());
        self.buf.extend_from_slice(s.as_bytes());
    }

    /// Write an optional string (None = null size).
    pub fn write_string_opt(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.write_string(s),
            None => self.write_size_null(),
        }
    }

    /// Write an f64 array.
    pub fn write_f64_array(&mut self, arr: &[f64]) {
        self.write_size(arr.len());
        for &v in arr {
            self.write_f64(v);
        }
    }

    /// Write an i32 array.
    pub fn write_i32_array(&mut self, arr: &[i32]) {
        self.write_size(arr.len());
        for &v in arr {
            self.write_i32(v);
        }
    }

    /// Write a byte array (bulk write).
    pub fn write_byte_array(&mut self, arr: &[u8]) {
        self.write_size(arr.len());
        self.buf.extend_from_slice(arr);
    }

    /// Write a string array.
    pub fn write_string_array(&mut self, arr: &[String]) {
        self.write_size(arr.len());
        for s in arr {
            self.write_string(s);
        }
    }

    /// Write raw bytes without size prefix.
    pub fn write_raw(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }
}

/// Decoding errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Not enough bytes in the buffer.
    UnexpectedEnd { needed: usize, available: usize },
    /// Got null (255) where a value was expected.
    UnexpectedNull,
    /// Invalid size value.
    InvalidSize(i64),
    /// Invalid UTF-8 in a string.
    InvalidUtf8(String),
    /// Unknown type code.
    UnknownTypeCode(u8),
    /// Generic protocol error.
    Protocol(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEnd { needed, available } => {
                write!(f, "unexpected end: need {needed} bytes, have {available}")
            }
            Self::UnexpectedNull => write!(f, "unexpected null value"),
            Self::InvalidSize(s) => write!(f, "invalid size: {s}"),
            Self::InvalidUtf8(e) => write!(f, "invalid UTF-8: {e}"),
            Self::UnknownTypeCode(c) => write!(f, "unknown type code: 0x{c:02X}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn le_reader(data: &[u8]) -> PvaReader<'_> {
        PvaReader::new(data, ByteOrder::LittleEndian)
    }
    fn be_reader(data: &[u8]) -> PvaReader<'_> {
        PvaReader::new(data, ByteOrder::BigEndian)
    }
    fn le_writer() -> PvaWriter {
        PvaWriter::new(ByteOrder::LittleEndian)
    }
    fn be_writer() -> PvaWriter {
        PvaWriter::new(ByteOrder::BigEndian)
    }

    #[test]
    fn test_read_u8() {
        let mut r = le_reader(&[0x42]);
        assert_eq!(r.read_u8().unwrap(), 0x42);
        assert!(r.is_empty());
    }

    #[test]
    fn test_read_i8() {
        let mut r = le_reader(&[0xFF]);
        assert_eq!(r.read_i8().unwrap(), -1);
    }

    #[test]
    fn test_read_bool() {
        let mut r = le_reader(&[0x00, 0x01, 0xFF]);
        assert!(!r.read_bool().unwrap());
        assert!(r.read_bool().unwrap());
        assert!(r.read_bool().unwrap());
    }

    #[test]
    fn test_read_i16_le() {
        let mut r = le_reader(&[0x34, 0x12]);
        assert_eq!(r.read_i16().unwrap(), 0x1234);
    }

    #[test]
    fn test_read_i16_be() {
        let mut r = be_reader(&[0x12, 0x34]);
        assert_eq!(r.read_i16().unwrap(), 0x1234);
    }

    #[test]
    fn test_read_u16() {
        let mut r = le_reader(&[0xFF, 0xFF]);
        assert_eq!(r.read_u16().unwrap(), 0xFFFF);
    }

    #[test]
    fn test_read_i32_le() {
        let mut r = le_reader(&[0x78, 0x56, 0x34, 0x12]);
        assert_eq!(r.read_i32().unwrap(), 0x12345678);
    }

    #[test]
    fn test_read_i32_be() {
        let mut r = be_reader(&[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(r.read_i32().unwrap(), 0x12345678);
    }

    #[test]
    fn test_read_i64_le() {
        let mut r = le_reader(&[0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11]);
        assert_eq!(r.read_i64().unwrap(), 0x1122334455667788);
    }

    #[test]
    fn test_read_f32() {
        let pi = std::f32::consts::PI;
        let bytes = pi.to_le_bytes();
        let mut r = le_reader(&bytes);
        let v = r.read_f32().unwrap();
        assert!((v - pi).abs() < f32::EPSILON);
    }

    #[test]
    fn test_read_f64() {
        let pi = std::f64::consts::PI;
        let bytes = pi.to_le_bytes();
        let mut r = le_reader(&bytes);
        let v = r.read_f64().unwrap();
        assert!((v - pi).abs() < f64::EPSILON);
    }

    #[test]
    fn test_read_underflow() {
        let mut r = le_reader(&[0x01]);
        assert!(r.read_i32().is_err());
    }

    #[test]
    fn test_read_empty() {
        let mut r = le_reader(&[]);
        assert!(r.read_u8().is_err());
        assert!(r.is_empty());
    }

    #[test]
    fn test_read_size_small() {
        let mut r = le_reader(&[42]);
        assert_eq!(r.read_size().unwrap(), Some(42));
    }

    #[test]
    fn test_read_size_zero() {
        let mut r = le_reader(&[0]);
        assert_eq!(r.read_size().unwrap(), Some(0));
    }

    #[test]
    fn test_read_size_253() {
        let mut r = le_reader(&[253]);
        assert_eq!(r.read_size().unwrap(), Some(253));
    }

    #[test]
    fn test_read_size_extended() {
        let mut data = vec![254];
        data.extend_from_slice(&1000i32.to_le_bytes());
        let mut r = le_reader(&data);
        assert_eq!(r.read_size().unwrap(), Some(1000));
    }

    #[test]
    fn test_read_size_null() {
        let mut r = le_reader(&[255]);
        assert_eq!(r.read_size().unwrap(), None);
    }

    #[test]
    fn test_read_size_non_null_ok() {
        let mut r = le_reader(&[42]);
        assert_eq!(r.read_size_non_null().unwrap(), 42);
    }

    #[test]
    fn test_read_size_non_null_err() {
        let mut r = le_reader(&[255]);
        assert!(r.read_size_non_null().is_err());
    }

    #[test]
    fn test_read_size_negative_extended() {
        let mut data = vec![254];
        data.extend_from_slice(&(-1i32).to_le_bytes());
        let mut r = le_reader(&data);
        assert!(matches!(r.read_size(), Err(DecodeError::InvalidSize(_))));
    }

    #[test]
    fn test_read_string_empty() {
        let mut r = le_reader(&[0]); // size=0
        assert_eq!(r.read_string().unwrap(), "");
    }

    #[test]
    fn test_read_string_hello() {
        let mut data = vec![5]; // size=5
        data.extend_from_slice(b"hello");
        let mut r = le_reader(&data);
        assert_eq!(r.read_string().unwrap(), "hello");
    }

    #[test]
    fn test_read_string_utf8() {
        let s = "café";
        let bytes = s.as_bytes();
        let mut data = vec![bytes.len() as u8];
        data.extend_from_slice(bytes);
        let mut r = le_reader(&data);
        assert_eq!(r.read_string().unwrap(), "café");
    }

    #[test]
    fn test_read_string_opt_null() {
        let mut r = le_reader(&[255]);
        assert_eq!(r.read_string_opt().unwrap(), None);
    }

    #[test]
    fn test_read_string_opt_some() {
        let mut data = vec![3];
        data.extend_from_slice(b"abc");
        let mut r = le_reader(&data);
        assert_eq!(r.read_string_opt().unwrap(), Some("abc".to_string()));
    }

    #[test]
    fn test_read_f64_array() {
        let mut w = le_writer();
        w.write_f64_array(&[1.0, 2.0, 3.0]);
        let mut r = le_reader(w.as_bytes());
        let arr = r.read_f64_array().unwrap();
        assert_eq!(arr, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_read_i32_array() {
        let mut w = le_writer();
        w.write_i32_array(&[10, 20, 30]);
        let mut r = le_reader(w.as_bytes());
        let arr = r.read_i32_array().unwrap();
        assert_eq!(arr, vec![10, 20, 30]);
    }

    #[test]
    fn test_read_byte_array() {
        let mut w = le_writer();
        w.write_byte_array(&[0xAA, 0xBB, 0xCC]);
        let mut r = le_reader(w.as_bytes());
        let arr = r.read_byte_array().unwrap();
        assert_eq!(arr, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_read_string_array() {
        let mut w = le_writer();
        w.write_string_array(&["hello".into(), "world".into()]);
        let mut r = le_reader(w.as_bytes());
        let arr = r.read_string_array().unwrap();
        assert_eq!(arr, vec!["hello", "world"]);
    }

    #[test]
    fn test_read_empty_array() {
        let mut w = le_writer();
        w.write_f64_array(&[]);
        let mut r = le_reader(w.as_bytes());
        let arr = r.read_f64_array().unwrap();
        assert!(arr.is_empty());
    }

    #[test]
    fn test_reader_remaining() {
        let r = le_reader(&[1, 2, 3, 4]);
        assert_eq!(r.remaining(), 4);
        assert_eq!(r.position(), 0);
    }

    #[test]
    fn test_reader_skip() {
        let mut r = le_reader(&[1, 2, 3, 4]);
        r.skip(2).unwrap();
        assert_eq!(r.position(), 2);
        assert_eq!(r.remaining(), 2);
    }

    #[test]
    fn test_reader_skip_overflow() {
        let mut r = le_reader(&[1, 2]);
        assert!(r.skip(10).is_err());
    }

    #[test]
    fn test_reader_peek() {
        let mut r = le_reader(&[0x42, 0x43]);
        assert_eq!(r.peek().unwrap(), 0x42);
        assert_eq!(r.position(), 0); // peek doesn't advance
        r.read_u8().unwrap();
        assert_eq!(r.peek().unwrap(), 0x43);
    }

    #[test]
    fn test_reader_read_bytes() {
        let mut r = le_reader(&[1, 2, 3, 4, 5]);
        let slice = r.read_bytes(3).unwrap();
        assert_eq!(slice, &[1, 2, 3]);
        assert_eq!(r.remaining(), 2);
    }

    #[test]
    fn test_write_u8() {
        let mut w = le_writer();
        w.write_u8(0x42);
        assert_eq!(w.as_bytes(), &[0x42]);
    }

    #[test]
    fn test_write_bool() {
        let mut w = le_writer();
        w.write_bool(true);
        w.write_bool(false);
        assert_eq!(w.as_bytes(), &[1, 0]);
    }

    #[test]
    fn test_write_i16_le() {
        let mut w = le_writer();
        w.write_i16(0x1234);
        assert_eq!(w.as_bytes(), &[0x34, 0x12]);
    }

    #[test]
    fn test_write_i16_be() {
        let mut w = be_writer();
        w.write_i16(0x1234);
        assert_eq!(w.as_bytes(), &[0x12, 0x34]);
    }

    #[test]
    fn test_write_i32_le() {
        let mut w = le_writer();
        w.write_i32(0x12345678);
        assert_eq!(w.as_bytes(), &[0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_write_i64() {
        let mut w = le_writer();
        w.write_i64(0x0102030405060708);
        assert_eq!(w.len(), 8);
    }

    #[test]
    fn test_write_f64() {
        let mut w = le_writer();
        w.write_f64(std::f64::consts::PI);
        assert_eq!(w.len(), 8);
    }

    #[test]
    fn test_write_size_small() {
        let mut w = le_writer();
        w.write_size(42);
        assert_eq!(w.as_bytes(), &[42]);
    }

    #[test]
    fn test_write_size_253() {
        let mut w = le_writer();
        w.write_size(253);
        assert_eq!(w.as_bytes(), &[253]);
    }

    #[test]
    fn test_write_size_254() {
        let mut w = le_writer();
        w.write_size(254);
        assert_eq!(w.len(), 5); // tag + i32
        assert_eq!(w.as_bytes()[0], SIZE_EXTENDED);
    }

    #[test]
    fn test_write_size_large() {
        let mut w = le_writer();
        w.write_size(100_000);
        assert_eq!(w.len(), 5);
    }

    #[test]
    fn test_write_size_null() {
        let mut w = le_writer();
        w.write_size_null();
        assert_eq!(w.as_bytes(), &[SIZE_NULL]);
    }

    #[test]
    fn test_write_string_empty() {
        let mut w = le_writer();
        w.write_string("");
        assert_eq!(w.as_bytes(), &[0]); // size=0
    }

    #[test]
    fn test_write_string_hello() {
        let mut w = le_writer();
        w.write_string("hello");
        assert_eq!(w.as_bytes(), &[5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_write_string_opt_none() {
        let mut w = le_writer();
        w.write_string_opt(None);
        assert_eq!(w.as_bytes(), &[SIZE_NULL]);
    }

    #[test]
    fn test_write_string_opt_some() {
        let mut w = le_writer();
        w.write_string_opt(Some("hi"));
        assert_eq!(w.as_bytes(), &[2, b'h', b'i']);
    }

    #[test]
    fn test_write_f64_array() {
        let mut w = le_writer();
        w.write_f64_array(&[1.0, 2.0]);
        assert_eq!(w.len(), 1 + 2 * 8); // size(1) + 2 * f64(8)
    }

    #[test]
    fn test_write_byte_array() {
        let mut w = le_writer();
        w.write_byte_array(&[0xAA, 0xBB]);
        assert_eq!(w.as_bytes(), &[2, 0xAA, 0xBB]);
    }

    #[test]
    fn test_writer_clear() {
        let mut w = le_writer();
        w.write_i32(42);
        assert_eq!(w.len(), 4);
        w.clear();
        assert_eq!(w.len(), 0);
        assert!(w.is_empty());
    }

    #[test]
    fn test_writer_raw() {
        let mut w = le_writer();
        w.write_raw(&[1, 2, 3]);
        assert_eq!(w.as_bytes(), &[1, 2, 3]);
    }

    #[test]
    fn test_writer_into_bytes() {
        let mut w = le_writer();
        w.write_u8(42);
        let bytes = w.into_bytes();
        assert_eq!(bytes, vec![42]);
    }

    #[test]
    fn test_roundtrip_all_primitives() {
        for order in [ByteOrder::LittleEndian, ByteOrder::BigEndian] {
            let mut w = PvaWriter::new(order);
            w.write_bool(true);
            w.write_u8(0xAB);
            w.write_i8(-42);
            w.write_i16(-1234);
            w.write_u16(0xCAFE);
            w.write_i32(0x12345678);
            w.write_u32(0xDEADBEEF);
            w.write_i64(0x0102030405060708);
            w.write_u64(0xFFFFFFFFFFFFFFFF);
            w.write_f32(3.14);
            w.write_f64(2.718281828);

            let mut r = PvaReader::new(w.as_bytes(), order);
            assert!(r.read_bool().unwrap());
            assert_eq!(r.read_u8().unwrap(), 0xAB);
            assert_eq!(r.read_i8().unwrap(), -42);
            assert_eq!(r.read_i16().unwrap(), -1234);
            assert_eq!(r.read_u16().unwrap(), 0xCAFE);
            assert_eq!(r.read_i32().unwrap(), 0x12345678);
            assert_eq!(r.read_u32().unwrap(), 0xDEADBEEF);
            assert_eq!(r.read_i64().unwrap(), 0x0102030405060708);
            assert_eq!(r.read_u64().unwrap(), 0xFFFFFFFFFFFFFFFF);
            assert!((r.read_f32().unwrap() - 3.14).abs() < 0.001);
            assert!((r.read_f64().unwrap() - 2.718281828).abs() < 1e-9);
            assert!(r.is_empty());
        }
    }

    #[test]
    fn test_roundtrip_size() {
        for order in [ByteOrder::LittleEndian, ByteOrder::BigEndian] {
            for size in [0, 1, 42, 253, 254, 255, 1000, 100_000] {
                let mut w = PvaWriter::new(order);
                if size == 255 {
                    w.write_size_null();
                } else {
                    w.write_size(size);
                }
                let mut r = PvaReader::new(w.as_bytes(), order);
                let read = r.read_size().unwrap();
                if size == 255 {
                    assert_eq!(read, None);
                } else {
                    assert_eq!(read, Some(size));
                }
            }
        }
    }

    #[test]
    fn test_roundtrip_string() {
        let long_str = "a".repeat(300);
        let strings: Vec<&str> = vec!["", "hello", "café", "日本語", &long_str];
        for s in &strings {
            let mut w = le_writer();
            w.write_string(s);
            let mut r = le_reader(w.as_bytes());
            assert_eq!(r.read_string().unwrap(), *s);
        }
    }

    #[test]
    fn test_roundtrip_f64_array() {
        let data = vec![1.0, -2.5, 0.0, f64::MAX, f64::MIN, f64::NAN.copysign(1.0)];
        for order in [ByteOrder::LittleEndian, ByteOrder::BigEndian] {
            let mut w = PvaWriter::new(order);
            w.write_f64_array(&data);
            let mut r = PvaReader::new(w.as_bytes(), order);
            let result = r.read_f64_array().unwrap();
            assert_eq!(result.len(), data.len());
            for (a, b) in data.iter().zip(result.iter()) {
                if a.is_nan() {
                    assert!(b.is_nan());
                } else {
                    assert_eq!(a, b);
                }
            }
        }
    }

    #[test]
    fn test_roundtrip_string_array() {
        let data = vec!["one".into(), "two".into(), "three".into()];
        let mut w = le_writer();
        w.write_string_array(&data);
        let mut r = le_reader(w.as_bytes());
        assert_eq!(r.read_string_array().unwrap(), data);
    }

    #[test]
    fn test_error_display() {
        assert!(
            DecodeError::UnexpectedEnd {
                needed: 4,
                available: 2
            }
            .to_string()
            .contains("need 4")
        );
        assert!(DecodeError::UnexpectedNull.to_string().contains("null"));
        assert!(DecodeError::InvalidSize(-1).to_string().contains("-1"));
        assert!(
            DecodeError::InvalidUtf8("bad".into())
                .to_string()
                .contains("bad")
        );
        assert!(
            DecodeError::UnknownTypeCode(0xFF)
                .to_string()
                .contains("0xFF")
        );
        assert!(
            DecodeError::Protocol("oops".into())
                .to_string()
                .contains("oops")
        );
    }

    #[test]
    fn test_error_is_error() {
        let e: Box<dyn std::error::Error> = Box::new(DecodeError::UnexpectedNull);
        assert!(!e.to_string().is_empty());
    }
}