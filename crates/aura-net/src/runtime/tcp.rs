//! Raw async TCP connection with PVA framing.
//!
//! Wraps a `TcpStream` with the PVA codec for framed read/write.

use bytes::BytesMut;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::codec::framing::{CodecError, PvaCodec, PvaFrame};
use crate::codec::header::{ByteOrder, HEADER_SIZE, PvaHeader};

/// Errors from the TCP transport layer.
#[derive(Debug)]
pub enum TransportError {
    Io(std::io::Error),
    Codec(CodecError),
    Timeout,
    Closed,
}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<CodecError> for TransportError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::Codec(e) => write!(f, "codec: {e}"),
            Self::Timeout => write!(f, "timeout"),
            Self::Closed => write!(f, "connection closed"),
        }
    }
}

impl std::error::Error for TransportError {}

/// A PVA TCP connection with framed I/O.
pub struct PvaTcp {
    pub(crate) stream: TcpStream,
    codec: PvaCodec,
    read_buf: BytesMut,
    pub(crate) write_buf: BytesMut,
    pub addr: SocketAddr,
    bytes_read: u64,
    bytes_written: u64,
}

impl PvaTcp {
    /// Connect to an IOC with timeout.
    pub async fn connect(addr: SocketAddr, timeout: Duration) -> Result<Self, TransportError> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(TransportError::Io)?;

        // Disable Nagle for low latency.
        stream.set_nodelay(true)?;

        // Increase TCP buffers for bulk operations (100k+ PVs).
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = stream.as_raw_fd();
            let buf_size: libc::c_int = 1_048_576; // 1 MB
            unsafe {
                let rc = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    &buf_size as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
                if rc != 0 {
                    tracing::warn!(addr = %addr, "SO_SNDBUF 1MB failed (using kernel default)");
                }
                let rc = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &buf_size as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
                if rc != 0 {
                    tracing::warn!(addr = %addr, "SO_RCVBUF 1MB failed (using kernel default)");
                }
            }
        }

        Ok(Self {
            stream,
            codec: PvaCodec::new(),
            read_buf: BytesMut::with_capacity(1_048_576),
            write_buf: BytesMut::with_capacity(65536),
            addr,
            bytes_read: 0,
            bytes_written: 0,
        })
    }

    /// Read the next complete PVA frame.
    /// Blocks until a full frame is available or the connection closes.
    pub async fn recv_frame(&mut self) -> Result<PvaFrame, TransportError> {
        loop {
            if let Some(frame) = self.codec.decode_frame(&mut self.read_buf)? {
                return Ok(frame);
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(TransportError::Closed);
            }
            self.bytes_read += n as u64;
        }
    }

    /// Send a raw control message (no payload — header only).
    pub async fn send_control(
        &mut self,
        command: u8,
        payload_data: i32,
    ) -> Result<(), TransportError> {
        let header = PvaHeader::ctrl(command, payload_data);
        let mut buf = [0u8; HEADER_SIZE];
        header.encode_into(&mut buf);
        self.stream.write_all(&buf).await?;
        self.bytes_written += HEADER_SIZE as u64;
        Ok(())
    }

    /// Send a message: header + payload in a single write syscall.
    pub async fn send_msg(&mut self, command: u8, payload: &[u8]) -> Result<(), TransportError> {
        let header = PvaHeader::app(command, payload.len() as i32);
        self.write_buf.clear();
        self.write_buf.extend_from_slice(&header.encode());
        self.write_buf.extend_from_slice(payload);
        self.stream.write_all(&self.write_buf).await?;
        self.bytes_written += self.write_buf.len() as u64;
        Ok(())
    }

    /// Buffer a message without flushing to TCP.
    #[inline]
    pub fn buffer_msg(&mut self, command: u8, payload: &[u8]) {
        let header = PvaHeader::app(command, payload.len() as i32);
        self.write_buf.extend_from_slice(&header.encode());
        self.write_buf.extend_from_slice(payload);
    }

    /// Flush all buffered messages to TCP in a single write syscall.
    pub async fn flush_writes(&mut self) -> Result<(), TransportError> {
        if self.write_buf.is_empty() {
            return Ok(());
        }
        self.stream.write_all(&self.write_buf).await?;
        self.bytes_written += self.write_buf.len() as u64;
        self.write_buf.clear();
        Ok(())
    }

    /// Get/set codec byte order (updated after handshake).
    pub fn byte_order(&self) -> ByteOrder {
        self.codec.byte_order()
    }
    pub fn set_byte_order(&mut self, order: ByteOrder) {
        self.codec.set_byte_order(order);
    }
}

impl std::fmt::Debug for PvaTcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PvaTcp")
            .field("addr", &self.addr)
            .field("byte_order", &self.byte_order())
            .field("rx", &self.bytes_read)
            .field("tx", &self.bytes_written)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Bind a loopback listener on a random port, return (listener, addr).
    async fn loopback() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    /// Connect a PvaTcp to a loopback listener, return (client, server_stream).
    async fn pair() -> (PvaTcp, TcpStream) {
        let (listener, addr) = loopback().await;
        let (client, server) = tokio::join!(
            PvaTcp::connect(addr, Duration::from_secs(2)),
            listener.accept()
        );
        (client.unwrap(), server.unwrap().0)
    }

    #[test]
    fn error_display_io() {
        let e = TransportError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken",
        ));
        assert!(e.to_string().contains("broken"));
    }

    #[test]
    fn error_display_timeout() {
        assert_eq!(TransportError::Timeout.to_string(), "timeout");
    }

    #[test]
    fn error_display_closed() {
        assert_eq!(TransportError::Closed.to_string(), "connection closed");
    }

    #[test]
    fn error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let e: TransportError = io_err.into();
        matches!(e, TransportError::Io(_));
    }

    #[test]
    fn error_debug() {
        let e = TransportError::Timeout;
        assert!(format!("{:?}", e).contains("Timeout"));
    }

    #[tokio::test]
    async fn connect_loopback() {
        let (tcp, _server) = pair().await;
        assert_eq!(tcp.bytes_read, 0);
        assert_eq!(tcp.bytes_written, 0);
        assert_eq!(tcp.byte_order(), ByteOrder::LittleEndian);
    }

    #[tokio::test]
    async fn connect_timeout_unreachable() {
        // 192.0.2.1 = TEST-NET, guaranteed unreachable.
        let result =
            PvaTcp::connect("192.0.2.1:9999".parse().unwrap(), Duration::from_millis(50)).await;
        assert!(matches!(result, Err(TransportError::Timeout)));
    }

    #[tokio::test]
    async fn send_recv_roundtrip() {
        let (listener, addr) = loopback().await;

        // Client sends, server-side PvaTcp receives.
        let client_handle = tokio::spawn(async move {
            let mut tcp = PvaTcp::connect(addr, Duration::from_secs(2)).await.unwrap();
            tcp.send_msg(0x42, b"hello PVA").await.unwrap();
            assert!(tcp.bytes_written > 0);
        });

        let (server_stream, _) = listener.accept().await.unwrap();
        let server_addr = server_stream.peer_addr().unwrap();
        let mut server_tcp = PvaTcp {
            stream: server_stream,
            codec: PvaCodec::new(),
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
            addr: server_addr,
            bytes_read: 0,
            bytes_written: 0,
        };

        let frame = server_tcp.recv_frame().await.unwrap();
        assert_eq!(frame.header.command, 0x42);
        assert_eq!(frame.payload, b"hello PVA");
        assert!(server_tcp.bytes_read > 0);

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn buffer_and_flush() {
        let (listener, addr) = loopback().await;

        let client_handle = tokio::spawn(async move {
            let mut tcp = PvaTcp::connect(addr, Duration::from_secs(2)).await.unwrap();
            // Buffer 3 messages, flush once.
            tcp.buffer_msg(0x10, b"one");
            tcp.buffer_msg(0x11, b"two");
            tcp.buffer_msg(0x12, b"three");
            assert!(!tcp.write_buf.is_empty());
            tcp.flush_writes().await.unwrap();
            assert!(tcp.write_buf.is_empty());
            assert!(tcp.bytes_written > 0);
        });

        let (server_stream, _) = listener.accept().await.unwrap();
        let server_addr = server_stream.peer_addr().unwrap();
        let mut server = PvaTcp {
            stream: server_stream,
            codec: PvaCodec::new(),
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
            addr: server_addr,
            bytes_read: 0,
            bytes_written: 0,
        };

        let f1 = server.recv_frame().await.unwrap();
        let f2 = server.recv_frame().await.unwrap();
        let f3 = server.recv_frame().await.unwrap();
        assert_eq!(f1.header.command, 0x10);
        assert_eq!(f2.header.command, 0x11);
        assert_eq!(f3.header.command, 0x12);
        assert_eq!(f1.payload, b"one");
        assert_eq!(f2.payload, b"two");
        assert_eq!(f3.payload, b"three");

        client_handle.await.unwrap();
    }

    #[tokio::test]
    async fn flush_empty_is_noop() {
        let (mut tcp, _server) = pair().await;
        assert!(tcp.write_buf.is_empty());
        tcp.flush_writes().await.unwrap();
        assert_eq!(tcp.bytes_written, 0);
    }

    #[tokio::test]
    async fn send_control_writes_header_only() {
        let (mut tcp, _server) = pair().await;
        tcp.send_control(0x02, 0).await.unwrap();
        assert_eq!(tcp.bytes_written, HEADER_SIZE as u64);
    }

    #[tokio::test]
    async fn byte_order_default_le() {
        let (tcp, _server) = pair().await;
        assert_eq!(tcp.byte_order(), ByteOrder::LittleEndian);
    }

    #[tokio::test]
    async fn set_byte_order() {
        let (mut tcp, _server) = pair().await;
        tcp.set_byte_order(ByteOrder::BigEndian);
        assert_eq!(tcp.byte_order(), ByteOrder::BigEndian);
    }

    #[tokio::test]
    async fn recv_detects_closed() {
        let (mut tcp, server) = pair().await;
        drop(server); // close server side
        let result = tcp.recv_frame().await;
        assert!(matches!(result, Err(TransportError::Closed)));
    }

    #[tokio::test]
    async fn debug_format() {
        let (tcp, _server) = pair().await;
        let d = format!("{:?}", tcp);
        assert!(d.contains("PvaTcp"));
        assert!(d.contains("127.0.0.1"));
        assert!(d.contains("rx"));
        assert!(d.contains("tx"));
    }

    #[tokio::test]
    async fn send_recv_large_payload() {
        let (listener, addr) = loopback().await;
        let payload = vec![0xABu8; 100_000]; // 100 KB

        let payload_clone = payload.clone();
        let client_handle = tokio::spawn(async move {
            let mut tcp = PvaTcp::connect(addr, Duration::from_secs(2)).await.unwrap();
            tcp.send_msg(0x99, &payload_clone).await.unwrap();
        });

        let (server_stream, _) = listener.accept().await.unwrap();
        let server_addr = server_stream.peer_addr().unwrap();
        let mut server = PvaTcp {
            stream: server_stream,
            codec: PvaCodec::new(),
            read_buf: BytesMut::with_capacity(1_048_576),
            write_buf: BytesMut::with_capacity(4096),
            addr: server_addr,
            bytes_read: 0,
            bytes_written: 0,
        };

        let frame = server.recv_frame().await.unwrap();
        assert_eq!(frame.payload.len(), 100_000);
        assert!(frame.payload.iter().all(|&b| b == 0xAB));

        client_handle.await.unwrap();
    }
}
