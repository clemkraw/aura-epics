//! PVA TCP handshake sequence.
//!
//! The PVA handshake is:
//! 1. Server sends CTRL_SET_BYTE_ORDER
//! 2. Client echoes CTRL_SET_BYTE_ORDER (same order)
//! 3. Server sends CMD_CONNECTION_VALIDATION
//! 4. Client sends CMD_CONNECTION_VALIDATED (anonymous auth)
//! 5. Server sends CMD_CONNECTION_VALIDATED (connection ready)

use super::tcp::{PvaTcp, TransportError};
use crate::codec::commands::{
    CMD_CONNECTION_VALIDATED, CMD_CONNECTION_VALIDATION, CTRL_SET_BYTE_ORDER,
};
use crate::codec::header::{ByteOrder, flags};
use crate::codec::pvdata::{PvaReader, PvaWriter};
use crate::messages::{ConnectionValidated, ConnectionValidation};

/// Handshake error.
#[derive(Debug)]
pub enum HandshakeError {
    Transport(TransportError),
    UnexpectedMessage { expected: &'static str, got: u8 },
    Protocol(String),
}

impl From<TransportError> for HandshakeError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::UnexpectedMessage { expected, got } => {
                write!(f, "expected {expected}, got cmd 0x{got:02X}")
            }
            Self::Protocol(msg) => write!(f, "protocol: {msg}"),
        }
    }
}

impl std::error::Error for HandshakeError {}

/// Result of a successful handshake.
#[derive(Debug)]
pub struct HandshakeResult {
    pub byte_order: ByteOrder,
    pub server_buffer_size: i32,
    pub server_registry_size: i16,
}

/// Perform the PVA handshake on an established TCP connection.
pub async fn perform_handshake(
    tcp: &mut PvaTcp,
    client_buffer_size: i32,
    client_registry_size: i16,
) -> Result<HandshakeResult, HandshakeError> {
    let frame = tcp.recv_frame().await?;
    if !frame.is_control() || frame.header.command != CTRL_SET_BYTE_ORDER {
        return Err(HandshakeError::UnexpectedMessage {
            expected: "CTRL_SET_BYTE_ORDER",
            got: frame.header.command,
        });
    }
    let byte_order = if frame.header.flags & flags::BIG_ENDIAN != 0 {
        ByteOrder::BigEndian
    } else {
        ByteOrder::LittleEndian
    };
    tcp.set_byte_order(byte_order);

    let order_flag = match byte_order {
        ByteOrder::BigEndian => 0x80i32,
        ByteOrder::LittleEndian => 0x00i32,
    };
    tcp.send_control(CTRL_SET_BYTE_ORDER, order_flag).await?;

    let frame = tcp.recv_frame().await?;
    tracing::trace!(
        cmd = frame.header.command,
        flags = format_args!("0x{:02X}", frame.header.flags),
        payload_size = frame.header.payload_size,
        payload_len = frame.payload.len(),
        payload_hex = format_args!("{:02x?}", &frame.payload),
        "received frame in handshake step 3"
    );
    if frame.header.command != CMD_CONNECTION_VALIDATION {
        return Err(HandshakeError::UnexpectedMessage {
            expected: "CMD_CONNECTION_VALIDATION",
            got: frame.header.command,
        });
    }
    let mut reader = PvaReader::new(&frame.payload, byte_order);
    let validation = ConnectionValidation::decode(&mut reader)
        .map_err(|e| HandshakeError::Protocol(format!("decode CONNECTION_VALIDATION: {e}")))?;

    let validated = ConnectionValidated::anonymous(client_buffer_size, client_registry_size);
    let mut writer = PvaWriter::new(byte_order);
    validated.encode(&mut writer);
    tcp.send_msg(CMD_CONNECTION_VALIDATION, writer.as_bytes())
        .await?;

    let frame = tcp.recv_frame().await?;
    if frame.header.command != CMD_CONNECTION_VALIDATED {
        tracing::warn!(
            cmd = frame.header.command,
            "expected CONNECTION_VALIDATED (0x09) from server"
        );
    }

    Ok(HandshakeResult {
        byte_order,
        server_buffer_size: validation.server_buffer_size,
        server_registry_size: validation.server_registry_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::header::PvaHeader;
    use crate::codec::pvdata::PvaWriter;
    use crate::messages::ConnectionValidation;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Write a raw PVA frame (header + payload) to a TcpStream.
    async fn write_frame(stream: &mut tokio::net::TcpStream, header: PvaHeader, payload: &[u8]) {
        stream.write_all(&header.encode()).await.unwrap();
        if !payload.is_empty() {
            stream.write_all(payload).await.unwrap();
        }
    }

    /// Build a ConnectionValidation payload (what the server sends at step 3).
    fn validation_payload(buf_size: i32, reg_size: i16) -> Vec<u8> {
        let val = ConnectionValidation {
            server_buffer_size: buf_size,
            server_registry_size: reg_size,
            auth_methods: vec!["anonymous".into()],
        };
        let mut w = PvaWriter::new(ByteOrder::LittleEndian);
        val.encode(&mut w);
        w.into_bytes()
    }

    /// Run a mock PVA server: sends the 3 server frames (step 1, 3, 5).
    async fn mock_server(mut stream: tokio::net::TcpStream, buf_size: i32, reg_size: i16) {
        // Step 1: CTRL_SET_BYTE_ORDER (LE)
        let hdr = PvaHeader::ctrl(CTRL_SET_BYTE_ORDER, 0);
        write_frame(&mut stream, hdr, &[]).await;

        // Step 2: read client echo (consume it)
        let mut tmp = [0u8; 8];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut tmp)
            .await
            .unwrap();

        // Step 3: CMD_CONNECTION_VALIDATION
        let payload = validation_payload(buf_size, reg_size);
        let hdr = PvaHeader::app(CMD_CONNECTION_VALIDATION, payload.len() as i32);
        write_frame(&mut stream, hdr, &payload).await;

        // Step 4: read client CMD_CONNECTION_VALIDATED (consume it)
        let mut buf = vec![0u8; 256];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf)
            .await
            .unwrap();

        // Step 5: CMD_CONNECTION_VALIDATED
        let hdr = PvaHeader::app(CMD_CONNECTION_VALIDATED, 0);
        write_frame(&mut stream, hdr, &[]).await;
    }

    async fn connect_pair(addr: std::net::SocketAddr) -> PvaTcp {
        PvaTcp::connect(addr, Duration::from_secs(2)).await.unwrap()
    }

    #[test]
    fn error_display_transport() {
        let e = HandshakeError::Transport(TransportError::Timeout);
        assert!(e.to_string().contains("timeout"));
    }

    #[test]
    fn error_display_unexpected() {
        let e = HandshakeError::UnexpectedMessage {
            expected: "CTRL",
            got: 0x42,
        };
        let s = e.to_string();
        assert!(s.contains("CTRL"));
        assert!(s.contains("0x42"));
    }

    #[test]
    fn error_display_protocol() {
        let e = HandshakeError::Protocol("bad size".into());
        assert!(e.to_string().contains("bad size"));
    }

    #[test]
    fn error_from_transport() {
        let e: HandshakeError = TransportError::Closed.into();
        assert!(matches!(
            e,
            HandshakeError::Transport(TransportError::Closed)
        ));
    }

    #[test]
    fn error_debug() {
        let e = HandshakeError::Protocol("test".into());
        assert!(format!("{:?}", e).contains("Protocol"));
    }

    #[tokio::test]
    async fn handshake_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            mock_server(stream, 65536, 32).await;
        });

        let mut tcp = connect_pair(addr).await;
        let result = perform_handshake(&mut tcp, 16384, 16).await.unwrap();

        assert_eq!(result.byte_order, ByteOrder::LittleEndian);
        assert_eq!(result.server_buffer_size, 65536);
        assert_eq!(result.server_registry_size, 32);

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_wrong_step1() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Send an app message instead of control
            let hdr = PvaHeader::app(0xFF, 0);
            write_frame(&mut stream, hdr, &[]).await;
        });

        let mut tcp = connect_pair(addr).await;
        let err = perform_handshake(&mut tcp, 16384, 16).await.unwrap_err();
        assert!(matches!(
            err,
            HandshakeError::UnexpectedMessage {
                expected: "CTRL_SET_BYTE_ORDER",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn handshake_wrong_step3() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Step 1: correct CTRL_SET_BYTE_ORDER
            write_frame(&mut stream, PvaHeader::ctrl(CTRL_SET_BYTE_ORDER, 0), &[]).await;
            // Step 2: consume echo
            let mut tmp = [0u8; 8];
            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut tmp)
                .await
                .unwrap();
            // Step 3: wrong command (0xEE instead of CONNECTION_VALIDATION)
            write_frame(&mut stream, PvaHeader::app(0xEE, 0), &[]).await;
        });

        let mut tcp = connect_pair(addr).await;
        let err = perform_handshake(&mut tcp, 16384, 16).await.unwrap_err();
        assert!(matches!(
            err,
            HandshakeError::UnexpectedMessage {
                expected: "CMD_CONNECTION_VALIDATION",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn handshake_server_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream); // close immediately
        });

        let mut tcp = connect_pair(addr).await;
        let err = perform_handshake(&mut tcp, 16384, 16).await.unwrap_err();
        assert!(matches!(
            err,
            HandshakeError::Transport(TransportError::Closed)
        ));
    }

    #[test]
    fn result_debug() {
        let r = HandshakeResult {
            byte_order: ByteOrder::LittleEndian,
            server_buffer_size: 65536,
            server_registry_size: 32,
        };
        let d = format!("{:?}", r);
        assert!(d.contains("HandshakeResult"));
        assert!(d.contains("65536"));
    }
}
