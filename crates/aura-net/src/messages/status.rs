//! PVA status — success/warning/error result for protocol operations.
//!
//! Every PVA response includes a `PvaStatus` indicating success or failure. Wire format:
//!
//! ```text
//! u8 type_code:
//!   0xFF = OK (no message follows)
//!   0-6  = type + message string follows
//!
//! If type_code != 0xFF:
//!   string message
//!   string call_tree (if type has HAS_CALL_TREE_BIT set)
//! ```

use crate::codec::pvdata::{DecodeError, PvaReader, PvaWriter};
use std::fmt;

/// Status severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StatusType {
    Ok,
    Warning,
    Error,
    Fatal,
}

impl StatusType {
    pub fn from_code(code: u8) -> Option<Self> {
        match code & 0x07 {
            0 => Some(Self::Ok),
            1 => Some(Self::Warning),
            2 => Some(Self::Error),
            3 => Some(Self::Fatal),
            _ => None,
        }
    }

    pub const fn to_code(self) -> u8 {
        match self {
            Self::Ok => 0,
            Self::Warning => 1,
            Self::Error => 2,
            Self::Fatal => 3,
        }
    }

    pub const fn is_error(self) -> bool {
        matches!(self, Self::Error | Self::Fatal)
    }
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Ok | Self::Warning)
    }

    pub const ALL: [Self; 4] = [Self::Ok, Self::Warning, Self::Error, Self::Fatal];
}

impl fmt::Display for StatusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ok => "OK",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
        })
    }
}

const STATUS_OK_CODE: u8 = 0xFF;
const HAS_CALL_TREE_BIT: u8 = 0x08;

/// Protocol result (success, warning, or error with message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PvaStatus {
    pub status_type: StatusType,
    pub message: String,
    pub call_tree: Option<String>,
}

impl PvaStatus {
    pub fn ok() -> Self {
        Self {
            status_type: StatusType::Ok,
            message: String::new(),
            call_tree: None,
        }
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            status_type: StatusType::Warning,
            message: message.into(),
            call_tree: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status_type: StatusType::Error,
            message: message.into(),
            call_tree: None,
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            status_type: StatusType::Fatal,
            message: message.into(),
            call_tree: None,
        }
    }

    pub fn with_call_tree(mut self, tree: impl Into<String>) -> Self {
        self.call_tree = Some(tree.into());
        self
    }

    pub fn is_success(&self) -> bool {
        self.status_type.is_success()
    }

    pub fn is_error(&self) -> bool {
        self.status_type.is_error()
    }

    pub fn is_ok(&self) -> bool {
        self.status_type == StatusType::Ok && self.message.is_empty()
    }

    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let code = reader.read_u8()?;
        if code == STATUS_OK_CODE {
            return Ok(Self::ok());
        }

        let status_type = StatusType::from_code(code).ok_or(DecodeError::Protocol(format!(
            "unknown status type: 0x{code:02X}"
        )))?;
        let has_call_tree = code & HAS_CALL_TREE_BIT != 0;
        let message = reader.read_string()?;
        let call_tree = if has_call_tree {
            Some(reader.read_string()?)
        } else {
            None
        };
        Ok(Self {
            status_type,
            message,
            call_tree,
        })
    }

    pub fn encode(&self, writer: &mut PvaWriter) {
        if self.is_ok() {
            writer.write_u8(STATUS_OK_CODE);
            return;
        }
        let mut code = self.status_type.to_code();
        if self.call_tree.is_some() {
            code |= HAS_CALL_TREE_BIT;
        }
        writer.write_u8(code);
        writer.write_string(&self.message);
        if let Some(ref tree) = self.call_tree {
            writer.write_string(tree);
        }
    }

    /// Wire size in bytes (for pre-allocation).
    pub fn wire_size(&self) -> usize {
        if self.is_ok() {
            return 1;
        }
        1 + 1 + self.message.len() // code + size + msg
            + self.call_tree.as_ref().map_or(0, |t| 1 + t.len()) // size + tree
    }
}

impl fmt::Display for PvaStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ok() {
            write!(f, "OK")
        } else if self.message.is_empty() {
            write!(f, "{}", self.status_type)
        } else {
            write!(f, "{}: {}", self.status_type, self.message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::header::ByteOrder;

    fn le_writer() -> PvaWriter {
        PvaWriter::new(ByteOrder::LittleEndian)
    }
    fn le_reader(data: &[u8]) -> PvaReader<'_> {
        PvaReader::new(data, ByteOrder::LittleEndian)
    }

    #[test]
    fn test_type_from_code_all() {
        assert_eq!(StatusType::from_code(0), Some(StatusType::Ok));
        assert_eq!(StatusType::from_code(1), Some(StatusType::Warning));
        assert_eq!(StatusType::from_code(2), Some(StatusType::Error));
        assert_eq!(StatusType::from_code(3), Some(StatusType::Fatal));
    }

    #[test]
    fn test_type_from_code_invalid() {
        for c in 4..=7 {
            assert_eq!(StatusType::from_code(c), None);
        }
    }

    #[test]
    fn test_type_from_code_masks_upper_bits() {
        // Only bits 0-2 matter.
        assert_eq!(StatusType::from_code(0x08), Some(StatusType::Ok)); // 0x08 & 0x07 = 0
        assert_eq!(StatusType::from_code(0xF1), Some(StatusType::Warning)); // 0xF1 & 0x07 = 1
    }

    #[test]
    fn test_type_to_code_roundtrip() {
        for t in StatusType::ALL {
            assert_eq!(StatusType::from_code(t.to_code()), Some(t));
        }
    }

    #[test]
    fn test_type_is_error() {
        assert!(!StatusType::Ok.is_error());
        assert!(!StatusType::Warning.is_error());
        assert!(StatusType::Error.is_error());
        assert!(StatusType::Fatal.is_error());
    }

    #[test]
    fn test_type_is_success() {
        assert!(StatusType::Ok.is_success());
        assert!(StatusType::Warning.is_success());
        assert!(!StatusType::Error.is_success());
        assert!(!StatusType::Fatal.is_success());
    }

    #[test]
    fn test_type_ord() {
        assert!(StatusType::Ok < StatusType::Warning);
        assert!(StatusType::Warning < StatusType::Error);
        assert!(StatusType::Error < StatusType::Fatal);
    }

    #[test]
    fn test_type_display() {
        assert_eq!(StatusType::Ok.to_string(), "OK");
        assert_eq!(StatusType::Warning.to_string(), "WARNING");
        assert_eq!(StatusType::Error.to_string(), "ERROR");
        assert_eq!(StatusType::Fatal.to_string(), "FATAL");
    }

    #[test]
    fn test_type_copy() {
        let a = StatusType::Error;
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_type_hash() {
        use std::collections::HashSet;
        assert_eq!(
            StatusType::ALL
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len(),
            4
        );
    }

    #[test]
    fn test_ok() {
        let s = PvaStatus::ok();
        assert!(s.is_ok());
        assert!(s.is_success());
        assert!(!s.is_error());
        assert!(s.message.is_empty());
        assert!(s.call_tree.is_none());
    }

    #[test]
    fn test_warning() {
        let s = PvaStatus::warning("slow");
        assert!(s.is_success());
        assert!(!s.is_error());
        assert!(!s.is_ok());
        assert_eq!(s.message, "slow");
    }

    #[test]
    fn test_error() {
        let s = PvaStatus::error("not found");
        assert!(s.is_error());
        assert!(!s.is_success());
        assert_eq!(s.message, "not found");
    }

    #[test]
    fn test_fatal() {
        let s = PvaStatus::fatal("oom");
        assert!(s.is_error());
        assert_eq!(s.status_type, StatusType::Fatal);
    }

    #[test]
    fn test_with_call_tree() {
        let s = PvaStatus::error("bad").with_call_tree("stack trace");
        assert_eq!(s.call_tree.as_deref(), Some("stack trace"));
    }

    #[test]
    fn test_ok_with_message_is_not_is_ok() {
        let s = PvaStatus {
            status_type: StatusType::Ok,
            message: "info".into(),
            call_tree: None,
        };
        assert!(!s.is_ok());
        assert!(s.is_success());
    }

    #[test]
    fn test_warning_empty_message() {
        let s = PvaStatus {
            status_type: StatusType::Warning,
            message: String::new(),
            call_tree: None,
        };
        assert!(!s.is_ok());
        assert!(s.is_success());
    }

    #[test]
    fn test_encode_ok_single_byte() {
        let mut w = le_writer();
        PvaStatus::ok().encode(&mut w);
        assert_eq!(w.as_bytes(), &[0xFF]);
    }

    #[test]
    fn test_encode_warning_has_type_and_message() {
        let mut w = le_writer();
        PvaStatus::warning("x").encode(&mut w);
        assert_eq!(w.as_bytes()[0], 1); // Warning type code
    }

    #[test]
    fn test_encode_error_with_call_tree_has_bit() {
        let mut w = le_writer();
        PvaStatus::error("e").with_call_tree("t").encode(&mut w);
        assert_eq!(w.as_bytes()[0], 2 | HAS_CALL_TREE_BIT); // Error + tree bit
    }

    #[test]
    fn test_roundtrip_ok() {
        let o = PvaStatus::ok();
        let mut w = le_writer();
        o.encode(&mut w);
        let d = PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap();
        assert_eq!(d, o);
    }

    #[test]
    fn test_roundtrip_warning() {
        let o = PvaStatus::warning("timeout");
        let mut w = le_writer();
        o.encode(&mut w);
        assert_eq!(PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_roundtrip_error() {
        let o = PvaStatus::error("channel not found");
        let mut w = le_writer();
        o.encode(&mut w);
        assert_eq!(PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_roundtrip_fatal() {
        let o = PvaStatus::fatal("segfault");
        let mut w = le_writer();
        o.encode(&mut w);
        assert_eq!(PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_roundtrip_error_with_call_tree() {
        let o = PvaStatus::error("NPE").with_call_tree("at Foo.java:10\nat Bar.java:20");
        let mut w = le_writer();
        o.encode(&mut w);
        let d = PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap();
        assert_eq!(d, o);
        assert!(d.call_tree.is_some());
    }

    #[test]
    fn test_roundtrip_ok_with_message() {
        // Ok type with a message (unusual but valid).
        let o = PvaStatus {
            status_type: StatusType::Ok,
            message: "info".into(),
            call_tree: None,
        };
        let mut w = le_writer();
        o.encode(&mut w);
        let d = PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap();
        assert_eq!(d, o);
    }

    #[test]
    fn test_roundtrip_empty_message() {
        let o = PvaStatus {
            status_type: StatusType::Error,
            message: String::new(),
            call_tree: None,
        };
        let mut w = le_writer();
        o.encode(&mut w);
        assert_eq!(PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_roundtrip_long_message() {
        let o = PvaStatus::error("x".repeat(1000));
        let mut w = le_writer();
        o.encode(&mut w);
        assert_eq!(PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_roundtrip_utf8_message() {
        let o = PvaStatus::error("erreur café 日本語");
        let mut w = le_writer();
        o.encode(&mut w);
        assert_eq!(PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_roundtrip_all_types() {
        for t in StatusType::ALL {
            let o = PvaStatus {
                status_type: t,
                message: format!("msg_{t}"),
                call_tree: None,
            };
            let mut w = le_writer();
            o.encode(&mut w);
            let d = PvaStatus::decode(&mut le_reader(w.as_bytes())).unwrap();
            assert_eq!(d.status_type, t);
            assert_eq!(d.message, format!("msg_{t}"));
        }
    }

    #[test]
    fn test_roundtrip_both_byte_orders() {
        for order in [ByteOrder::LittleEndian, ByteOrder::BigEndian] {
            let o = PvaStatus::error("test");
            let mut w = PvaWriter::new(order);
            o.encode(&mut w);
            let d = PvaStatus::decode(&mut PvaReader::new(w.as_bytes(), order)).unwrap();
            assert_eq!(d, o);
        }
    }

    #[test]
    fn test_decode_empty() {
        assert!(PvaStatus::decode(&mut le_reader(&[])).is_err());
    }

    #[test]
    fn test_decode_truncated_message() {
        // Type code but no message string.
        assert!(PvaStatus::decode(&mut le_reader(&[0x01])).is_err());
    }

    #[test]
    fn test_decode_unknown_type() {
        let mut w = le_writer();
        w.write_u8(7);
        w.write_string("msg");
        assert!(PvaStatus::decode(&mut le_reader(w.as_bytes())).is_err());
    }

    #[test]
    fn test_wire_size_ok() {
        assert_eq!(PvaStatus::ok().wire_size(), 1);
    }

    #[test]
    fn test_wire_size_error() {
        let s = PvaStatus::error("abc");
        assert!(s.wire_size() > 1);
    }

    #[test]
    fn test_wire_size_with_tree() {
        let s = PvaStatus::error("e").with_call_tree("t");
        let without = PvaStatus::error("e").wire_size();
        assert!(s.wire_size() > without);
    }

    #[test]
    fn test_display_ok() {
        assert_eq!(PvaStatus::ok().to_string(), "OK");
    }

    #[test]
    fn test_display_error_msg() {
        assert_eq!(PvaStatus::error("bad").to_string(), "ERROR: bad");
    }

    #[test]
    fn test_display_warning_empty() {
        let s = PvaStatus {
            status_type: StatusType::Warning,
            message: String::new(),
            call_tree: None,
        };
        assert_eq!(s.to_string(), "WARNING");
    }

    #[test]
    fn test_clone() {
        let a = PvaStatus::error("x").with_call_tree("y");
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_debug() {
        assert!(format!("{:?}", PvaStatus::ok()).contains("PvaStatus"));
    }
}
