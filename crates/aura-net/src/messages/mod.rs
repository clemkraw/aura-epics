//! PVA protocol messages — typed request/response structs.
//!
//! Each struct corresponds to one CMD_* command from the PVA spec.
//! All messages implement encode/decode via the codec layer.
//!
//! - `status`: PvaStatus (success/error result)
//! - `search`: CMD_SEARCH / CMD_SEARCH_RESPONSE (PV discovery)
//! - `connection`: CMD_CONNECTION_VALIDATION / VALIDATED (handshake)
//! - `channel`: CMD_CREATE_CHANNEL / DESTROY_CHANNEL
//! - `monitor`: CMD_MONITOR (subscribe to value changes — hot path)

pub mod channel;
pub mod connection;
pub mod monitor;
pub mod search;
pub mod status;

pub use channel::{CreateChannelRequest, CreateChannelResponse, DestroyChannel};
pub use connection::{ConnectionValidated, ConnectionValidation};
pub use monitor::{MonitorRequest, MonitorResponseHeader, MonitorSubCommand};
pub use search::{SearchRequest, SearchResponse};
pub use status::{PvaStatus, StatusType};
