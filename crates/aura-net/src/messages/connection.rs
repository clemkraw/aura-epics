//! CMD_CONNECTION_VALIDATION (0x01) / CMD_CONNECTION_VALIDATED (0x09) - TCP handshake.

use crate::codec::pvdata::{DecodeError, PvaReader, PvaWriter};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionValidation {
    pub server_buffer_size: i32,
    pub server_registry_size: i16,
    pub auth_methods: Vec<String>,
}

impl ConnectionValidation {
    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let server_buffer_size = reader.read_i32()?;
        let server_registry_size = reader.read_i16()?;
        // Auth method count uses PVA "size" encoding (variable-length), not a fixed u16.
        // Typically, 1 byte for small counts.
        let count = reader.read_size_non_null()?;
        let mut auth_methods = Vec::with_capacity(count.min(16));
        for _ in 0..count {
            auth_methods.push(reader.read_string()?);
        }
        Ok(Self {
            server_buffer_size,
            server_registry_size,
            auth_methods,
        })
    }
    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_i32(self.server_buffer_size);
        writer.write_i16(self.server_registry_size);
        writer.write_size(self.auth_methods.len());
        for m in &self.auth_methods {
            writer.write_string(m);
        }
    }
    pub fn supports_anonymous(&self) -> bool {
        self.auth_methods.iter().any(|m| m == "anonymous")
    }
    pub fn supports(&self, method: &str) -> bool {
        self.auth_methods.iter().any(|m| m == method)
    }
}

impl fmt::Display for ConnectionValidation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConnValidation[buf={}B, reg={}, auth={:?}]",
            self.server_buffer_size, self.server_registry_size, self.auth_methods
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionValidated {
    pub client_buffer_size: i32,
    pub client_registry_size: i16,
    pub connection_qos: u16,
    pub auth_method: String,
}

impl ConnectionValidated {
    pub fn anonymous(buffer_size: i32, registry_size: i16) -> Self {
        Self {
            client_buffer_size: buffer_size,
            client_registry_size: registry_size,
            connection_qos: 0,
            auth_method: "anonymous".into(),
        }
    }
    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let client_buffer_size = reader.read_i32()?;
        let client_registry_size = reader.read_i16()?;
        let connection_qos = reader.read_u16()?;
        let auth_method = reader.read_string()?;
        Ok(Self {
            client_buffer_size,
            client_registry_size,
            connection_qos,
            auth_method,
        })
    }
    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_i32(self.client_buffer_size);
        writer.write_i16(self.client_registry_size);
        writer.write_u16(self.connection_qos);
        writer.write_string(&self.auth_method);
        writer.write_u8(0xFF); // null auth data
    }
}

impl fmt::Display for ConnectionValidated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConnValidated[buf={}B, reg={}, auth={}]",
            self.client_buffer_size, self.client_registry_size, self.auth_method
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::header::ByteOrder;

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

    fn make_validation() -> ConnectionValidation {
        ConnectionValidation {
            server_buffer_size: 65536,
            server_registry_size: 128,
            auth_methods: vec!["anonymous".into(), "ca".into()],
        }
    }

    #[test]
    fn test_val_roundtrip() {
        let o = make_validation();
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            ConnectionValidation::decode(&mut le_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_val_roundtrip_be() {
        let o = make_validation();
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(
            ConnectionValidation::decode(&mut be_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_val_roundtrip_empty_auth() {
        let o = ConnectionValidation {
            server_buffer_size: 32768,
            server_registry_size: 64,
            auth_methods: vec![],
        };
        let mut w = le_w();
        o.encode(&mut w);
        assert!(
            ConnectionValidation::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .auth_methods
                .is_empty()
        );
    }

    #[test]
    fn test_val_roundtrip_single_auth() {
        let o = ConnectionValidation {
            server_buffer_size: 65536,
            server_registry_size: 128,
            auth_methods: vec!["x509".into()],
        };
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            ConnectionValidation::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .auth_methods,
            vec!["x509"]
        );
    }

    #[test]
    fn test_val_supports_anonymous() {
        assert!(make_validation().supports_anonymous());
    }

    #[test]
    fn test_val_no_anonymous() {
        let v = ConnectionValidation {
            server_buffer_size: 0,
            server_registry_size: 0,
            auth_methods: vec!["ca".into()],
        };
        assert!(!v.supports_anonymous());
    }

    #[test]
    fn test_val_supports() {
        assert!(make_validation().supports("ca"));
        assert!(!make_validation().supports("x509"));
    }

    #[test]
    fn test_val_decode_empty() {
        assert!(ConnectionValidation::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_val_display() {
        let s = make_validation().to_string();
        assert!(s.contains("65536"));
        assert!(s.contains("anonymous"));
    }

    #[test]
    fn test_val_clone() {
        let a = make_validation();
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_val_debug() {
        assert!(format!("{:?}", make_validation()).contains("ConnectionValidation"));
    }

    #[test]
    fn test_vd_anonymous() {
        let v = ConnectionValidated::anonymous(65536, 128);
        assert_eq!(v.auth_method, "anonymous");
        assert_eq!(v.connection_qos, 0);
    }

    #[test]
    fn test_vd_roundtrip() {
        let o = ConnectionValidated::anonymous(65536, 128);
        let mut w = le_w();
        o.encode(&mut w);
        let d = ConnectionValidated::decode(&mut le_r(w.as_bytes())).unwrap();
        assert_eq!(d.client_buffer_size, 65536);
        assert_eq!(d.auth_method, "anonymous");
    }

    #[test]
    fn test_vd_roundtrip_be() {
        let o = ConnectionValidated::anonymous(32768, 64);
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(
            ConnectionValidated::decode(&mut be_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_vd_roundtrip_custom_auth() {
        let o = ConnectionValidated {
            client_buffer_size: 65536,
            client_registry_size: 128,
            connection_qos: 5,
            auth_method: "x509".into(),
        };
        let mut w = le_w();
        o.encode(&mut w);
        let d = ConnectionValidated::decode(&mut le_r(w.as_bytes())).unwrap();
        assert_eq!(d.auth_method, "x509");
        assert_eq!(d.connection_qos, 5);
    }

    #[test]
    fn test_vd_decode_empty() {
        assert!(ConnectionValidated::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_vd_display() {
        let s = ConnectionValidated::anonymous(32768, 64).to_string();
        assert!(s.contains("32768"));
        assert!(s.contains("anonymous"));
    }

    #[test]
    fn test_vd_clone() {
        let a = ConnectionValidated::anonymous(65536, 128);
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_vd_debug() {
        assert!(
            format!("{:?}", ConnectionValidated::anonymous(0, 0)).contains("ConnectionValidated")
        );
    }
}
