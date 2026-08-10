//! PVA runtime — async TCP/UDP I/O for real IOC communication.
//!
//! ```text
//! driver.rs   — Top-level API: search → connect → subscribe
//! session.rs  — One TCP connection: channels + monitors
//! handshake.rs — PVA handshake sequence
//! tcp.rs      — Raw async TCP with PVA framing
//! ```

pub mod driver;
pub mod handshake;
pub mod session;
pub mod tcp;

pub use driver::PvaDriver;
pub use session::PvaSession;
pub use tcp::PvaTcp;
