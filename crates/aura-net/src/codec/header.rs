//! PVA message header (8 bytes, fixed layout).
//!
//! Every PVAccess message starts with this header:
//! ```text
//! ┌──────┬─────────┬───────┬─────────┬──────────────┐
//! │ magic│ version │ flags │ command │ payload_size │
//! │  1B  │   1B    │  1B   │   1B    │     4B       │
//! └──────┴─────────┴───────┴─────────┴──────────────┘
//!   0xCA   2         see      CMD_*    i32 (LE/BE)
//!                   below
//! ```
//!
//! ## Flags byte
//!
//! ```text
//! bit 0:   0 = application message, 1 = control message
//! bit 1-3: reserved (must be 0)
//! bit 4-5: segmentation (00=none, 01=first, 10=last, 11=middle)
//! bit 6:   0 = from client, 1 = from server
//! bit 7:   0 = little-endian, 1 = big-endian
//! ```

use std::fmt;

/// PVA protocol magic byte. Every message starts with this.
pub const PVA_MAGIC: u8 = 0xCA;

/// Current protocol version (v2).
pub const PVA_VERSION: u8 = 2;

/// Minimum supported protocol version.
pub const PVA_VERSION_MIN: u8 = 1;

/// Header size in bytes (fixed).
pub const HEADER_SIZE: usize = 8;

/// Maximum payload size (16 MB — safety limit, not in spec).
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// Flags bit masks.
pub mod flags {
    /// Bit 0: message category.
    pub const CONTROL: u8 = 0x01;
    /// Bits 4-5: segmentation.
    pub const SEG_MASK: u8 = 0x30;
    pub const SEG_NONE: u8 = 0x00;
    pub const SEG_FIRST: u8 = 0x10;
    pub const SEG_LAST: u8 = 0x20;
    pub const SEG_MIDDLE: u8 = 0x30;
    /// Bit 6: direction.
    pub const FROM_SERVER: u8 = 0x40;
    /// Bit 7: byte order.
    pub const BIG_ENDIAN: u8 = 0x80;
}

/// Segmentation state of a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Segmentation {
    None,
    First,
    Last,
    Middle,
}

impl Segmentation {
    pub const fn from_flags(f: u8) -> Self {
        match f & flags::SEG_MASK {
            flags::SEG_NONE => Self::None,
            flags::SEG_FIRST => Self::First,
            flags::SEG_LAST => Self::Last,
            flags::SEG_MIDDLE => Self::Middle,
            _ => Self::None,
        }
    }

    pub const fn to_bits(self) -> u8 {
        match self {
            Self::None => flags::SEG_NONE,
            Self::First => flags::SEG_FIRST,
            Self::Last => flags::SEG_LAST,
            Self::Middle => flags::SEG_MIDDLE,
        }
    }

    pub const fn is_segmented(self) -> bool {
        !matches!(self, Self::None)
    }
}

impl fmt::Display for Segmentation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "complete",
            Self::First => "first",
            Self::Last => "last",
            Self::Middle => "middle",
        })
    }
}

/// Wire byte order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ByteOrder {
    #[default]
    LittleEndian,
    BigEndian,
}

impl ByteOrder {
    pub const fn from_flags(f: u8) -> Self {
        if f & flags::BIG_ENDIAN != 0 {
            Self::BigEndian
        } else {
            Self::LittleEndian
        }
    }

    pub const fn to_bits(self) -> u8 {
        match self {
            Self::LittleEndian => 0,
            Self::BigEndian => flags::BIG_ENDIAN,
        }
    }

    /// Read a i32 from bytes in this byte order.
    #[inline]
    pub fn read_i32(self, bytes: [u8; 4]) -> i32 {
        match self {
            Self::LittleEndian => i32::from_le_bytes(bytes),
            Self::BigEndian => i32::from_be_bytes(bytes),
        }
    }

    /// Write a i32 to bytes in this byte order.
    #[inline]
    pub fn write_i32(self, val: i32) -> [u8; 4] {
        match self {
            Self::LittleEndian => val.to_le_bytes(),
            Self::BigEndian => val.to_be_bytes(),
        }
    }

    /// Read a u16 from bytes in this byte order.
    #[inline]
    pub fn read_u16(self, bytes: [u8; 2]) -> u16 {
        match self {
            Self::LittleEndian => u16::from_le_bytes(bytes),
            Self::BigEndian => u16::from_be_bytes(bytes),
        }
    }

    /// Read a i16 from bytes in this byte order.
    #[inline]
    pub fn read_i16(self, bytes: [u8; 2]) -> i16 {
        match self {
            Self::LittleEndian => i16::from_le_bytes(bytes),
            Self::BigEndian => i16::from_be_bytes(bytes),
        }
    }

    /// Read a u32 from bytes in this byte order.
    #[inline]
    pub fn read_u32(self, bytes: [u8; 4]) -> u32 {
        match self {
            Self::LittleEndian => u32::from_le_bytes(bytes),
            Self::BigEndian => u32::from_be_bytes(bytes),
        }
    }

    /// Read a i64 from bytes in this byte order.
    #[inline]
    pub fn read_i64(self, bytes: [u8; 8]) -> i64 {
        match self {
            Self::LittleEndian => i64::from_le_bytes(bytes),
            Self::BigEndian => i64::from_be_bytes(bytes),
        }
    }

    /// Read a u64 from bytes in this byte order.
    #[inline]
    pub fn read_u64(self, bytes: [u8; 8]) -> u64 {
        match self {
            Self::LittleEndian => u64::from_le_bytes(bytes),
            Self::BigEndian => u64::from_be_bytes(bytes),
        }
    }

    /// Read a f32 from bytes in this byte order.
    #[inline]
    pub fn read_f32(self, bytes: [u8; 4]) -> f32 {
        match self {
            Self::LittleEndian => f32::from_le_bytes(bytes),
            Self::BigEndian => f32::from_be_bytes(bytes),
        }
    }

    /// Read a f64 from bytes in this byte order.
    #[inline]
    pub fn read_f64(self, bytes: [u8; 8]) -> f64 {
        match self {
            Self::LittleEndian => f64::from_le_bytes(bytes),
            Self::BigEndian => f64::from_be_bytes(bytes),
        }
    }
}

impl fmt::Display for ByteOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LittleEndian => "LE",
            Self::BigEndian => "BE",
        })
    }
}

/// PVA message header (8 bytes, Copy, zero heap).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PvaHeader {
    pub version: u8,
    pub flags: u8,
    pub command: u8,
    pub payload_size: i32,
}

impl PvaHeader {
    /// Create a new application message header (client → server, LE).
    pub const fn app(command: u8, payload_size: i32) -> Self {
        Self {
            version: PVA_VERSION,
            flags: 0, // app, client, LE, no segmentation
            command,
            payload_size,
        }
    }

    /// Create a server response header.
    pub const fn server(command: u8, payload_size: i32) -> Self {
        Self {
            version: PVA_VERSION,
            flags: flags::FROM_SERVER,
            command,
            payload_size,
        }
    }

    /// Create a control message header.
    pub const fn ctrl(command: u8, payload_value: i32) -> Self {
        Self {
            version: PVA_VERSION,
            flags: flags::CONTROL,
            command,
            payload_size: payload_value,
        }
    }

    /// Set byte order.
    pub const fn with_byte_order(mut self, order: ByteOrder) -> Self {
        self.flags = (self.flags & !flags::BIG_ENDIAN) | order.to_bits();
        self
    }

    /// Set segmentation.
    pub const fn with_segmentation(mut self, seg: Segmentation) -> Self {
        self.flags = (self.flags & !flags::SEG_MASK) | seg.to_bits();
        self
    }

    #[inline]
    pub const fn is_control(&self) -> bool {
        self.flags & flags::CONTROL != 0
    }
    #[inline]
    pub const fn is_application(&self) -> bool {
        !self.is_control()
    }
    #[inline]
    pub const fn is_from_server(&self) -> bool {
        self.flags & flags::FROM_SERVER != 0
    }
    #[inline]
    pub const fn is_from_client(&self) -> bool {
        !self.is_from_server()
    }
    #[inline]
    pub const fn byte_order(&self) -> ByteOrder {
        ByteOrder::from_flags(self.flags)
    }
    #[inline]
    pub const fn segmentation(&self) -> Segmentation {
        Segmentation::from_flags(self.flags)
    }
    #[inline]
    pub const fn is_segmented(&self) -> bool {
        self.segmentation().is_segmented()
    }

    /// Decode from an 8-byte slice. Returns None if magic is wrong.
    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Option<Self> {
        if buf[0] != PVA_MAGIC {
            return None;
        }

        let version = buf[1];
        let f = buf[2];
        let command = buf[3];
        let order = ByteOrder::from_flags(f);
        let payload_size = order.read_i32([buf[4], buf[5], buf[6], buf[7]]);

        Some(Self {
            version,
            flags: f,
            command,
            payload_size,
        })
    }

    /// Encode to an 8-byte array.
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let order = self.byte_order();
        let size_bytes = order.write_i32(self.payload_size);
        [
            PVA_MAGIC,
            self.version,
            self.flags,
            self.command,
            size_bytes[0],
            size_bytes[1],
            size_bytes[2],
            size_bytes[3],
        ]
    }

    /// Encode into a mutable byte slice (must be >= 8 bytes).
    pub fn encode_into(&self, buf: &mut [u8]) {
        let encoded = self.encode();
        buf[..HEADER_SIZE].copy_from_slice(&encoded);
    }

    /// Validate the header (version check, payload bounds).
    pub fn validate(&self) -> Result<(), HeaderError> {
        if self.version < PVA_VERSION_MIN {
            return Err(HeaderError::UnsupportedVersion(self.version));
        }
        if self.payload_size < 0 && self.is_application() {
            return Err(HeaderError::NegativePayload(self.payload_size));
        }
        if self.is_application() && self.payload_size as u32 > MAX_PAYLOAD_SIZE {
            return Err(HeaderError::PayloadTooLarge(self.payload_size as u32));
        }
        Ok(())
    }
}

impl fmt::Debug for PvaHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PvaHeader")
            .field("version", &self.version)
            .field("type", &if self.is_control() { "ctrl" } else { "app" })
            .field(
                "direction",
                &if self.is_from_server() {
                    "server"
                } else {
                    "client"
                },
            )
            .field("order", &self.byte_order())
            .field("segmentation", &self.segmentation())
            .field("command", &format!("0x{:02X}", self.command))
            .field("payload_size", &self.payload_size)
            .finish()
    }
}

impl fmt::Display for PvaHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.is_control() { "CTRL" } else { "APP" };
        let dir = if self.is_from_server() { "S" } else { "C" };
        write!(
            f,
            "[{kind}/{dir} v{} {} cmd=0x{:02X} {}B]",
            self.version,
            self.byte_order(),
            self.command,
            self.payload_size
        )
    }
}

/// Header validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderError {
    BadMagic(u8),
    UnsupportedVersion(u8),
    NegativePayload(i32),
    PayloadTooLarge(u32),
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic(m) => write!(f, "bad magic: 0x{m:02X} (expected 0xCA)"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported version: {v}"),
            Self::NegativePayload(s) => write!(f, "negative payload: {s}"),
            Self::PayloadTooLarge(s) => write!(f, "payload too large: {s} bytes"),
        }
    }
}

impl std::error::Error for HeaderError {}

#[cfg(test)]
mod tests {
    use super::flags::*;
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(PVA_MAGIC, 0xCA);
        assert_eq!(PVA_VERSION, 2);
        assert_eq!(HEADER_SIZE, 8);
    }

    #[test]
    fn test_byte_order_from_flags() {
        assert_eq!(ByteOrder::from_flags(0x00), ByteOrder::LittleEndian);
        assert_eq!(ByteOrder::from_flags(0x80), ByteOrder::BigEndian);
        assert_eq!(ByteOrder::from_flags(0xFF), ByteOrder::BigEndian);
    }

    #[test]
    fn test_byte_order_to_bits() {
        assert_eq!(ByteOrder::LittleEndian.to_bits(), 0x00);
        assert_eq!(ByteOrder::BigEndian.to_bits(), 0x80);
    }

    #[test]
    fn test_byte_order_default() {
        assert_eq!(ByteOrder::default(), ByteOrder::LittleEndian);
    }

    #[test]
    fn test_byte_order_read_i32_le() {
        let val = ByteOrder::LittleEndian.read_i32([0x78, 0x56, 0x34, 0x12]);
        assert_eq!(val, 0x12345678);
    }

    #[test]
    fn test_byte_order_read_i32_be() {
        let val = ByteOrder::BigEndian.read_i32([0x12, 0x34, 0x56, 0x78]);
        assert_eq!(val, 0x12345678);
    }

    #[test]
    fn test_byte_order_write_i32_le() {
        assert_eq!(
            ByteOrder::LittleEndian.write_i32(0x12345678),
            [0x78, 0x56, 0x34, 0x12]
        );
    }

    #[test]
    fn test_byte_order_write_i32_be() {
        assert_eq!(
            ByteOrder::BigEndian.write_i32(0x12345678),
            [0x12, 0x34, 0x56, 0x78]
        );
    }

    #[test]
    fn test_byte_order_roundtrip() {
        for val in [0i32, 1, -1, i32::MAX, i32::MIN, 42, 0x12345678] {
            for order in [ByteOrder::LittleEndian, ByteOrder::BigEndian] {
                let bytes = order.write_i32(val);
                assert_eq!(order.read_i32(bytes), val);
            }
        }
    }

    #[test]
    fn test_byte_order_read_u16() {
        assert_eq!(ByteOrder::LittleEndian.read_u16([0x34, 0x12]), 0x1234);
        assert_eq!(ByteOrder::BigEndian.read_u16([0x12, 0x34]), 0x1234);
    }

    #[test]
    fn test_byte_order_read_i64() {
        let val =
            ByteOrder::LittleEndian.read_i64([0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11]);
        assert_eq!(val, 0x1122334455667788);
    }

    #[test]
    fn test_byte_order_read_f64() {
        let pi_bytes = std::f64::consts::PI.to_le_bytes();
        let val = ByteOrder::LittleEndian.read_f64(pi_bytes);
        assert!((val - std::f64::consts::PI).abs() < f64::EPSILON);
    }

    #[test]
    fn test_byte_order_display() {
        assert_eq!(ByteOrder::LittleEndian.to_string(), "LE");
        assert_eq!(ByteOrder::BigEndian.to_string(), "BE");
    }

    #[test]
    fn test_segmentation_from_flags() {
        assert_eq!(Segmentation::from_flags(0x00), Segmentation::None);
        assert_eq!(Segmentation::from_flags(0x10), Segmentation::First);
        assert_eq!(Segmentation::from_flags(0x20), Segmentation::Last);
        assert_eq!(Segmentation::from_flags(0x30), Segmentation::Middle);
    }

    #[test]
    fn test_segmentation_to_bits() {
        assert_eq!(Segmentation::None.to_bits(), 0x00);
        assert_eq!(Segmentation::First.to_bits(), 0x10);
        assert_eq!(Segmentation::Last.to_bits(), 0x20);
        assert_eq!(Segmentation::Middle.to_bits(), 0x30);
    }

    #[test]
    fn test_segmentation_is_segmented() {
        assert!(!Segmentation::None.is_segmented());
        assert!(Segmentation::First.is_segmented());
        assert!(Segmentation::Last.is_segmented());
        assert!(Segmentation::Middle.is_segmented());
    }

    #[test]
    fn test_segmentation_roundtrip() {
        for seg in [
            Segmentation::None,
            Segmentation::First,
            Segmentation::Last,
            Segmentation::Middle,
        ] {
            assert_eq!(Segmentation::from_flags(seg.to_bits()), seg);
        }
    }

    #[test]
    fn test_segmentation_display() {
        assert_eq!(Segmentation::None.to_string(), "complete");
        assert_eq!(Segmentation::First.to_string(), "first");
    }

    #[test]
    fn test_header_app() {
        let h = PvaHeader::app(0x0D, 100);
        assert!(h.is_application());
        assert!(h.is_from_client());
        assert_eq!(h.command, 0x0D);
        assert_eq!(h.payload_size, 100);
        assert_eq!(h.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(h.segmentation(), Segmentation::None);
    }

    #[test]
    fn test_header_server() {
        let h = PvaHeader::server(0x0D, 200);
        assert!(h.is_application());
        assert!(h.is_from_server());
    }

    #[test]
    fn test_header_ctrl() {
        let h = PvaHeader::ctrl(0x02, 0);
        assert!(h.is_control());
    }

    #[test]
    fn test_header_with_byte_order() {
        let h = PvaHeader::app(0x03, 50).with_byte_order(ByteOrder::BigEndian);
        assert_eq!(h.byte_order(), ByteOrder::BigEndian);
        assert!(h.is_application()); // other flags preserved
    }

    #[test]
    fn test_header_with_segmentation() {
        let h = PvaHeader::app(0x0D, 1000).with_segmentation(Segmentation::First);
        assert_eq!(h.segmentation(), Segmentation::First);
        assert!(h.is_segmented());
    }

    #[test]
    fn test_header_copy() {
        let a = PvaHeader::app(0x03, 42);
        let b = a; // Copy
        assert_eq!(a, b);
    }

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<PvaHeader>(), 8);
    }

    #[test]
    fn test_encode_le() {
        let h = PvaHeader::app(0x03, 42);
        let bytes = h.encode();
        assert_eq!(bytes[0], PVA_MAGIC);
        assert_eq!(bytes[1], PVA_VERSION);
        assert_eq!(bytes[2], 0x00); // flags: app, client, LE
        assert_eq!(bytes[3], 0x03); // command
        // payload_size = 42 in LE
        assert_eq!(bytes[4..8], [42, 0, 0, 0]);
    }

    #[test]
    fn test_encode_be() {
        let h = PvaHeader::server(0x0D, 1000).with_byte_order(ByteOrder::BigEndian);
        let bytes = h.encode();
        assert_eq!(bytes[0], PVA_MAGIC);
        assert_eq!(bytes[2] & BIG_ENDIAN, BIG_ENDIAN);
        assert_eq!(bytes[2] & FROM_SERVER, FROM_SERVER);
        // payload 1000 in BE = 0x000003E8
        assert_eq!(bytes[4..8], [0x00, 0x00, 0x03, 0xE8]);
    }

    #[test]
    fn test_decode_le() {
        let bytes: [u8; 8] = [0xCA, 0x02, 0x00, 0x03, 42, 0, 0, 0];
        let h = PvaHeader::decode(&bytes).unwrap();
        assert_eq!(h.version, 2);
        assert_eq!(h.command, 0x03);
        assert_eq!(h.payload_size, 42);
        assert!(h.is_application());
        assert!(h.is_from_client());
    }

    #[test]
    fn test_decode_be() {
        let bytes: [u8; 8] = [0xCA, 0x02, 0xC0, 0x0D, 0x00, 0x00, 0x03, 0xE8];
        let h = PvaHeader::decode(&bytes).unwrap();
        assert!(h.is_from_server());
        assert_eq!(h.byte_order(), ByteOrder::BigEndian);
        assert_eq!(h.payload_size, 1000);
    }

    #[test]
    fn test_decode_bad_magic() {
        let bytes: [u8; 8] = [0xFF, 0x02, 0x00, 0x03, 0, 0, 0, 0];
        assert!(PvaHeader::decode(&bytes).is_none());
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        for cmd in [0x00u8, 0x03, 0x07, 0x0D, 0x14] {
            for size in [0i32, 1, 100, 65535, 1_000_000] {
                for order in [ByteOrder::LittleEndian, ByteOrder::BigEndian] {
                    let original = PvaHeader::app(cmd, size).with_byte_order(order);
                    let bytes = original.encode();
                    let decoded = PvaHeader::decode(&bytes).unwrap();
                    assert_eq!(original, decoded);
                }
            }
        }
    }

    #[test]
    fn test_encode_into() {
        let h = PvaHeader::app(0x03, 42);
        let mut buf = [0u8; 16];
        h.encode_into(&mut buf);
        assert_eq!(buf[0], PVA_MAGIC);
        assert_eq!(buf[3], 0x03);
    }

    #[test]
    fn test_validate_ok() {
        assert!(PvaHeader::app(0x03, 100).validate().is_ok());
    }

    #[test]
    fn test_validate_version_too_old() {
        let h = PvaHeader {
            version: 0,
            flags: 0,
            command: 0,
            payload_size: 0,
        };
        assert_eq!(h.validate(), Err(HeaderError::UnsupportedVersion(0)));
    }

    #[test]
    fn test_validate_negative_payload() {
        let h = PvaHeader::app(0x03, -1);
        assert_eq!(h.validate(), Err(HeaderError::NegativePayload(-1)));
    }

    #[test]
    fn test_validate_payload_too_large() {
        let h = PvaHeader::app(0x03, (MAX_PAYLOAD_SIZE + 1) as i32);
        assert!(matches!(h.validate(), Err(HeaderError::PayloadTooLarge(_))));
    }

    #[test]
    fn test_validate_ctrl_negative_ok() {
        // Control messages use payload_size as a value, not a size.
        let h = PvaHeader::ctrl(0x02, -1);
        assert!(h.validate().is_ok());
    }

    #[test]
    fn test_header_display() {
        let s = PvaHeader::app(0x03, 42).to_string();
        assert!(s.contains("APP/C"));
        assert!(s.contains("LE"));
        assert!(s.contains("cmd=0x03"));
        assert!(s.contains("42B"));
    }

    #[test]
    fn test_header_display_server() {
        let s = PvaHeader::server(0x0D, 100).to_string();
        assert!(s.contains("APP/S"));
    }

    #[test]
    fn test_header_debug() {
        let d = format!("{:?}", PvaHeader::app(0x03, 42));
        assert!(d.contains("PvaHeader"));
        assert!(d.contains("command"));
    }

    #[test]
    fn test_error_display() {
        assert!(HeaderError::BadMagic(0xFF).to_string().contains("0xFF"));
        assert!(
            HeaderError::UnsupportedVersion(0)
                .to_string()
                .contains("version")
        );
        assert!(HeaderError::NegativePayload(-5).to_string().contains("-5"));
        assert!(
            HeaderError::PayloadTooLarge(999)
                .to_string()
                .contains("999")
        );
    }

    #[test]
    fn test_error_is_error() {
        // Verify std::error::Error is implemented
        let e: Box<dyn std::error::Error> = Box::new(HeaderError::BadMagic(0));
        assert!(!e.to_string().is_empty());
    }
}
