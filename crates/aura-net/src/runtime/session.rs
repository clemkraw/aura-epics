//! PVA session - async I/O on a single TCP connection.
//!
//! `PvaSession` owns the TCP transport and delegates channel state management to
//! `ConnectionState` from `client/connection.rs`. This ensures a single source of truth for
//! channel lifecycle and a single atomic ID generator across the codebase.
//!
//! Responsibilities:
//! - **session.rs**: async TCP I/O, frame dispatch, monitor event loop
//! - **connection.rs**: channel state, introspection registry, stats
//! - **subscription.rs**: delta decoding logic

use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::client::connection::{ConnectionState, next_client_id};
use crate::codec::bitset::PvaBitSet;
use crate::codec::commands::*;
use crate::codec::field_desc::FieldDesc;
use crate::codec::header::ByteOrder;
use crate::codec::pvdata::{PvaReader, PvaWriter};
use crate::messages::SearchResponse;
use crate::messages::channel::{CreateChannelRequest, CreateChannelResponse};
use crate::messages::monitor::{MonitorRequest, MonitorResponseHeader, MonitorSubCommand};
use crate::messages::status::PvaStatus;
use crate::monitor::bus::MonitorBusTx;
use crate::monitor::handle::MonitorHandle;
use crate::monitor::subscription::{MonitorEvent, MonitorSubscription};
use crate::types::pva_value::PvaValue;

/// Global PV index counter — each PV gets a unique u32 index for bus routing.
static NEXT_PV_INDEX: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
fn next_pv_index() -> u32 {
    NEXT_PV_INDEX.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

use super::handshake::{HandshakeError, perform_handshake};
use super::tcp::{PvaTcp, TransportError};
use crate::codec::field_desc::FieldType;
use crate::types::pva_value::decode_scalar;
use aura_core::pva::scalars::ScalarType;

// In PVA monitor, the server sends only the changed fields.
// The `changed` BitSet indicates which fields (by flat index) have
// new data in the payload. The flat index is computed by a depth-first
// walk of the FieldDesc tree:
//   idx 0 = the top-level structure itself
//   idx 1 = first field (e.g. "value")
//   idx 2 = second field or first sub-field of a nested struct
//   ...

/// Decode a PVA delta-encoded structure from the wire.
///
/// Only reads fields whose bit is set in `changed`. Returns a partial
/// `PvaValue::Structure` with `PvaValue::Null` for unchanged fields.
fn decode_delta_structure(
    reader: &mut PvaReader<'_>,
    desc: &FieldDesc,
    changed: &PvaBitSet,
    flat_idx: &mut usize,
) -> Result<PvaValue, crate::codec::pvdata::DecodeError> {
    let my_idx = *flat_idx;
    *flat_idx += 1;

    match &desc.field_type {
        FieldType::Scalar(st) => {
            if changed.is_set(my_idx) {
                let pos_before = reader.position();
                let val = decode_scalar(reader, *st)?;
                tracing::trace!(
                    idx = my_idx, scalar_type = ?st,
                    pos_before, pos_after = reader.position(),
                    value = ?val.as_f64(),
                    "delta: read scalar"
                );
                Ok(PvaValue::Scalar(val))
            } else {
                Ok(PvaValue::Null)
            }
        }
        FieldType::ScalarArray(st) => {
            if changed.is_set(my_idx) {
                let len = reader.read_size_non_null()?;
                tracing::trace!(idx = my_idx, scalar_type = ?st, array_len = len, "delta: read scalar array");
                if len == 0 {
                    return Ok(PvaValue::ScalarArray(Vec::new()));
                }
                if *st == ScalarType::UByte {
                    let bytes = reader.read_bytes(len)?;
                    return Ok(PvaValue::ScalarArray(
                        bytes
                            .iter()
                            .map(|&b| aura_core::pva::scalars::ScalarValue::UByte(b))
                            .collect(),
                    ));
                }
                let mut arr = Vec::with_capacity(len.min(65536));
                for _ in 0..len {
                    arr.push(decode_scalar(reader, *st)?);
                }
                Ok(PvaValue::ScalarArray(arr))
            } else {
                Ok(PvaValue::Null)
            }
        }
        FieldType::BoundedString(_) => {
            if changed.is_set(my_idx) {
                let s = reader.read_string()?;
                tracing::trace!(idx = my_idx, value = %s, "delta: read bounded string");
                Ok(PvaValue::Scalar(
                    aura_core::pva::scalars::ScalarValue::String(s),
                ))
            } else {
                Ok(PvaValue::Null)
            }
        }
        FieldType::Structure | FieldType::StructureArray => {
            let start_idx = my_idx;
            let end_idx = start_idx + desc.flat_field_count();
            let any_child_set = (start_idx..end_idx).any(|i| changed.is_set(i));

            if !any_child_set {
                tracing::trace!(idx = my_idx, range = ?(start_idx..end_idx), type_id = %desc.type_id, "delta: skip struct (no bits set)");
                for nf in &desc.fields {
                    skip_flat_indices(&nf.desc, flat_idx);
                }
                return Ok(PvaValue::Null);
            }

            tracing::trace!(idx = my_idx, range = ?(start_idx..end_idx), type_id = %desc.type_id, n_fields = desc.fields.len(), "delta: recurse struct");
            let mut fields = Vec::with_capacity(desc.fields.len());
            for nf in &desc.fields {
                let child_val = decode_delta_structure(reader, &nf.desc, changed, flat_idx)?;
                fields.push((nf.name.clone(), child_val));
            }
            Ok(PvaValue::Structure(fields))
        }
        FieldType::VariantUnion | FieldType::VariantUnionArray => {
            if changed.is_set(my_idx) {
                let _tag = reader.read_u8()?;
                tracing::trace!(idx = my_idx, "delta: read variant union (skipped)");
                Ok(PvaValue::Null)
            } else {
                Ok(PvaValue::Null)
            }
        }
        FieldType::Union | FieldType::UnionArray => {
            if changed.is_set(my_idx) {
                let selector = reader.read_i32()?;
                tracing::trace!(idx = my_idx, selector, "delta: read union");
                if selector < 0 || (selector as usize) >= desc.fields.len() {
                    return Ok(PvaValue::Null);
                }
                let nf = &desc.fields[selector as usize];
                let v = PvaValue::decode_from(reader, &nf.desc)?;
                Ok(PvaValue::Union(nf.name.clone(), Box::new(v)))
            } else {
                Ok(PvaValue::Null)
            }
        }
    }
}

/// Advance flat_idx past all fields in a FieldDesc sub-tree without reading data.
fn skip_flat_indices(desc: &FieldDesc, flat_idx: &mut usize) {
    *flat_idx += 1; // this node
    if matches!(
        desc.field_type,
        FieldType::Structure | FieldType::StructureArray | FieldType::Union | FieldType::UnionArray
    ) {
        for nf in &desc.fields {
            skip_flat_indices(&nf.desc, flat_idx);
        }
    }
}

/// Errors from the session layer.
#[derive(Debug)]
pub enum SessionError {
    Transport(TransportError),
    Handshake(HandshakeError),
    ChannelCreateFailed(String),
    MonitorFailed(String),
    Protocol(String),
}

impl From<TransportError> for SessionError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}

impl From<HandshakeError> for SessionError {
    fn from(e: HandshakeError) -> Self {
        Self::Handshake(e)
    }
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::Handshake(e) => write!(f, "handshake: {e}"),
            Self::ChannelCreateFailed(msg) => write!(f, "channel create: {msg}"),
            Self::MonitorFailed(msg) => write!(f, "monitor: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol: {msg}"),
        }
    }
}

/// Pre-computed bit layout for zero-alloc NTScalar decode.
#[derive(Clone, Debug)]
struct FastScalarLayout {
    /// Bit index of the "value" field.
    value_bit: u8,
    /// How to read the value and convert to f64.
    value_type: ScalarType,
    /// Bit indices for alarm fields.
    severity_bit: u8,
    status_bit: u8,
    message_bit: u8,
    /// Bit indices for timeStamp fields.
    seconds_bit: u8,
    nanos_bit: u8,
    user_tag_bit: u8,
    /// Highest bit we understand - if any higher bit is set, fall back to generic decode.
    max_known_bit: u8,
}

impl FastScalarLayout {
    /// Detect NTScalar and compute bit layout from FieldDesc.
    /// Returns None for non-NTScalar types.
    fn detect(desc: &FieldDesc) -> Option<Self> {
        if !desc.type_id.starts_with("epics:nt/NTScalar") {
            return None;
        }
        if desc.fields.len() < 3 {
            return None;
        }

        // Walk the field tree depth-first, assigning flat bit indices.
        // bit 0 = root structure
        // bit 1 = first child, etc.
        let mut bit = 1u8;

        // Field 0: value (must be Scalar)
        let value_type = match &desc.fields[0].desc.field_type {
            FieldType::Scalar(st) => *st,
            _ => return None,
        };
        let value_bit = bit;
        bit += 1; // value is a leaf — 1 bit

        // Field 1: alarm (structure with severity, status, message)
        let alarm_fields = &desc.fields[1].desc.fields;
        if alarm_fields.len() < 3 {
            return None;
        }
        bit += 1; // alarm structure bit
        let severity_bit = bit;
        bit += 1;
        let status_bit = bit;
        bit += 1;
        let message_bit = bit;
        bit += 1;

        // Field 2: timeStamp (structure with secondsPastEpoch, nanoseconds, userTag)
        let ts_fields = &desc.fields[2].desc.fields;
        if ts_fields.len() < 3 {
            return None;
        }
        bit += 1; // timeStamp structure bit
        let seconds_bit = bit;
        bit += 1;
        let nanos_bit = bit;
        bit += 1;
        let user_tag_bit = bit;
        bit += 1;

        Some(FastScalarLayout {
            value_bit,
            value_type,
            severity_bit,
            status_bit,
            message_bit,
            seconds_bit,
            nanos_bit,
            user_tag_bit,
            max_known_bit: bit - 1,
        })
    }

    /// Read the value field as f64 from the reader.
    #[inline]
    fn read_value(&self, reader: &mut PvaReader) -> Result<f64, crate::codec::pvdata::DecodeError> {
        match self.value_type {
            ScalarType::Double => reader.read_f64(),
            ScalarType::Float => reader.read_f32().map(|v| v as f64),
            ScalarType::Int => reader.read_i32().map(|v| v as f64),
            ScalarType::Long => reader.read_i64().map(|v| v as f64),
            ScalarType::Short => reader.read_i16().map(|v| v as f64),
            ScalarType::Byte => reader.read_i8().map(|v| v as f64),
            ScalarType::UInt => reader.read_u32().map(|v| v as f64),
            ScalarType::ULong => reader.read_u64().map(|v| v as f64),
            ScalarType::UShort => reader.read_u16().map(|v| v as f64),
            ScalarType::UByte => reader.read_u8().map(|v| v as f64),
            ScalarType::Boolean => reader.read_u8().map(|v| if v != 0 { 1.0 } else { 0.0 }),
            ScalarType::String => Err(crate::codec::pvdata::DecodeError::InvalidUtf8(
                "string value in NTScalar".into(),
            )),
        }
    }
}

/// Pre-computed bit layout for zero-intermediate-alloc NTScalarArray decode.
/// Same approach as FastScalarLayout but for array value fields.
#[derive(Clone, Debug)]
struct FastArrayLayout {
    value_bit: u8,
    element_type: ScalarType,
    severity_bit: u8,
    status_bit: u8,
    message_bit: u8,
    seconds_bit: u8,
    nanos_bit: u8,
    user_tag_bit: u8,
    max_known_bit: u8,
}

impl FastArrayLayout {
    fn detect(desc: &FieldDesc) -> Option<Self> {
        if desc.type_id != "epics:nt/NTScalarArray:1.0" {
            return None;
        }
        if desc.fields.len() < 3 {
            return None;
        }

        let element_type = match &desc.fields[0].desc.field_type {
            FieldType::ScalarArray(st) => *st,
            _ => return None,
        };
        // String arrays go through slow path (rare, variable-length).
        if element_type == ScalarType::String {
            return None;
        }

        let mut bit = 1u8;
        let value_bit = bit;
        bit += 1;

        let alarm_fields = &desc.fields[1].desc.fields;
        if alarm_fields.len() < 3 {
            return None;
        }
        bit += 1; // alarm structure
        let severity_bit = bit;
        bit += 1;
        let status_bit = bit;
        bit += 1;
        let message_bit = bit;
        bit += 1;

        let ts_fields = &desc.fields[2].desc.fields;
        if ts_fields.len() < 3 {
            return None;
        }
        bit += 1; // timeStamp structure
        let seconds_bit = bit;
        bit += 1;
        let nanos_bit = bit;
        bit += 1;
        let user_tag_bit = bit;
        bit += 1;

        Some(FastArrayLayout {
            value_bit,
            element_type,
            severity_bit,
            status_bit,
            message_bit,
            seconds_bit,
            nanos_bit,
            user_tag_bit,
            max_known_bit: bit - 1,
        })
    }

    /// Read array elements as f64 directly from the wire. Single allocation.
    #[inline]
    fn read_array(
        &self,
        reader: &mut PvaReader,
    ) -> Result<Vec<f64>, crate::codec::pvdata::DecodeError> {
        use aura_core::pva::scalars::ScalarType;
        let len = reader.read_size_non_null()?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::with_capacity(len.min(65536));
        match self.element_type {
            ScalarType::Double => {
                for _ in 0..len {
                    values.push(reader.read_f64()?);
                }
            }
            ScalarType::Float => {
                for _ in 0..len {
                    values.push(reader.read_f32()? as f64);
                }
            }
            ScalarType::Int => {
                for _ in 0..len {
                    values.push(reader.read_i32()? as f64);
                }
            }
            ScalarType::Long => {
                for _ in 0..len {
                    values.push(reader.read_i64()? as f64);
                }
            }
            ScalarType::Short => {
                for _ in 0..len {
                    values.push(reader.read_i16()? as f64);
                }
            }
            ScalarType::Byte => {
                for _ in 0..len {
                    values.push(reader.read_i8()? as f64);
                }
            }
            ScalarType::UInt => {
                for _ in 0..len {
                    values.push(reader.read_u32()? as f64);
                }
            }
            ScalarType::ULong => {
                for _ in 0..len {
                    values.push(reader.read_u64()? as f64);
                }
            }
            ScalarType::UShort => {
                for _ in 0..len {
                    values.push(reader.read_u16()? as f64);
                }
            }
            ScalarType::UByte => {
                let bytes = reader.read_bytes(len)?;
                values.extend(bytes.iter().map(|&b| b as f64));
                return Ok(values);
            }
            ScalarType::Boolean => {
                for _ in 0..len {
                    values.push(if reader.read_u8()? != 0 { 1.0 } else { 0.0 });
                }
            }
            ScalarType::String => {
                return Err(crate::codec::pvdata::DecodeError::Protocol(
                    "string array in FastArrayLayout".into(),
                ));
            }
        }
        Ok(values)
    }
}

/// Pre-computed bit layout for zero-alloc NTEnum decode.
#[derive(Clone, Debug)]
struct FastEnumLayout {
    /// Bit index of the "value" structure (parent of index/choices).
    value_struct_bit: u8,
    /// Bit index of "value.index" - the actual enum value.
    index_bit: u8,
    /// Bit index of "value.choices" - string array, skipped.
    choices_bit: u8,
    severity_bit: u8,
    status_bit: u8,
    message_bit: u8,
    seconds_bit: u8,
    nanos_bit: u8,
    user_tag_bit: u8,
    max_known_bit: u8,
}

impl FastEnumLayout {
    fn detect(desc: &FieldDesc) -> Option<Self> {
        if desc.type_id != "epics:nt/NTEnum:1.0" {
            return None;
        }
        if desc.fields.len() < 3 {
            return None;
        }

        let mut bit = 1u8;

        // Field 0: value (must be a Structure with index + choices)
        if !desc.fields[0].desc.is_structure() {
            return None;
        }
        let value_fields = &desc.fields[0].desc.fields;
        if value_fields.len() < 2 {
            return None;
        }
        let value_struct_bit = bit;
        bit += 1; // value structure marker

        // value.index (Int scalar)
        let index_bit = bit;
        bit += 1;

        // value.choices (String array)
        let choices_bit = bit;
        bit += 1;

        // Field 1: alarm
        let alarm_fields = &desc.fields[1].desc.fields;
        if alarm_fields.len() < 3 {
            return None;
        }
        bit += 1; // alarm structure
        let severity_bit = bit;
        bit += 1;
        let status_bit = bit;
        bit += 1;
        let message_bit = bit;
        bit += 1;

        // Field 2: timeStamp
        let ts_fields = &desc.fields[2].desc.fields;
        if ts_fields.len() < 3 {
            return None;
        }
        bit += 1; // timeStamp structure
        let seconds_bit = bit;
        bit += 1;
        let nanos_bit = bit;
        bit += 1;
        let user_tag_bit = bit;
        bit += 1;

        Some(FastEnumLayout {
            value_struct_bit,
            index_bit,
            choices_bit,
            severity_bit,
            status_bit,
            message_bit,
            seconds_bit,
            nanos_bit,
            user_tag_bit,
            max_known_bit: bit - 1,
        })
    }
}

/// Runtime state for an active monitor (async - owns mpsc::Sender).
#[allow(dead_code)]
struct MonitorEntry {
    request_id: i32,
    subscription: MonitorSubscription,
    tx: mpsc::Sender<MonitorEvent>,
    /// Numeric index for bus routing (avoids Arc clone per event).
    pv_index: u32,
    /// Pre-computed shard for bus routing (avoids hash per event).
    bus_shard: usize,
    /// Cached pv_id from pv_lookup (resolved lazily, 0 = unknown).
    pv_id: i32,
    /// Zero-alloc fast decode layout for NTScalar PVs.
    fast_layout: Option<FastScalarLayout>,
    /// Fast decode layout for NTScalarArray PVs (waveforms).
    fast_array_layout: Option<FastArrayLayout>,
    /// Fast decode layout for NTEnum PVs (states, modes).
    fast_enum_layout: Option<FastEnumLayout>,
}

type MetadataBuffer = std::sync::Arc<std::sync::Mutex<Vec<(std::sync::Arc<str>, PvaValue)>>>;
type PvCache = std::sync::Arc<arc_swap::ArcSwap<HashMap<std::sync::Arc<str>, i32>>>;

/// A PVA session on one TCP connection.
///
/// Owns the TCP transport. Delegates channel/registry state to `ConnectionState` (from `client/connection.rs`).
pub struct PvaSession {
    tcp: PvaTcp,
    /// Channel and registry state - the single source of truth.
    state: ConnectionState,
    /// Active monitors by request_id (runtime-only, owns async senders).
    monitors: HashMap<i32, MonitorEntry>,
    /// Aggregated event bus - when set, events are pushed here instead of per-PV channels.
    bus_tx: Option<MonitorBusTx>,
    /// Shared buffer for metadata extraction (first full update per PV).
    metadata_buf: Option<MetadataBuffer>,
    /// Shared pv_id cache for OPT-1: resolve pv_id once per monitor, not per event.
    pv_cache: Option<PvCache>,
}

impl PvaSession {
    /// Connect and handshake with an IOC.
    pub async fn connect(
        addr: SocketAddr,
        timeout: std::time::Duration,
        buffer_size: i32,
        registry_size: i16,
    ) -> Result<Self, SessionError> {
        let mut tcp = PvaTcp::connect(addr, timeout)
            .await
            .map_err(SessionError::Transport)?;
        let hs = perform_handshake(&mut tcp, buffer_size, registry_size).await?;

        let mut state = ConnectionState::new(addr, registry_size as usize);
        state.complete_handshake(hs.byte_order, hs.server_buffer_size);

        Ok(Self {
            tcp,
            state,
            monitors: HashMap::new(),
            bus_tx: None,
            metadata_buf: None,
            pv_cache: None,
        })
    }

    /// Set the aggregated monitor bus. When set, all monitor events are
    /// pushed to the bus instead of per-PV channels.
    pub fn set_bus_tx(&mut self, bus: MonitorBusTx) {
        self.bus_tx = Some(bus);
    }

    /// Set the shared metadata buffer. First full update for each PV is pushed here.
    pub fn set_metadata_buf(&mut self, buf: MetadataBuffer) {
        self.metadata_buf = Some(buf);
    }

    pub fn set_pv_cache(
        &mut self,
        cache: std::sync::Arc<arc_swap::ArcSwap<HashMap<std::sync::Arc<str>, i32>>>,
    ) {
        self.pv_cache = Some(cache);
    }

    /// Create a channel for a PV. Returns the server channel ID.
    pub async fn create_channel(&mut self, pv_name: &str) -> Result<i32, SessionError> {
        // Use the shared ID generator from client/connection.rs.
        let client_id = self.state.add_channel(pv_name);

        // Send CMD_CREATE_CHANNEL.
        let req = CreateChannelRequest {
            channels: vec![(client_id, pv_name.to_string())],
        };
        let mut writer = PvaWriter::new(self.state.byte_order);
        req.encode(&mut writer);
        self.tcp
            .send_msg(CMD_CREATE_CHANNEL, writer.as_bytes())
            .await?;
        self.state.messages_sent += 1;

        // Wait for response by polling frames.
        loop {
            let frame = self.tcp.recv_frame().await?;
            self.state.messages_received += 1;

            if frame.header.command == CMD_CREATE_CHANNEL {
                let mut reader = PvaReader::new(&frame.payload, self.state.byte_order);
                let resp = CreateChannelResponse::decode(&mut reader)
                    .map_err(|e| SessionError::Protocol(format!("decode CREATE_CHANNEL: {e}")))?;

                if resp.client_channel_id == client_id {
                    if !resp.status.is_ok() {
                        self.state.close_channel(client_id);
                        return Err(SessionError::ChannelCreateFailed(resp.status.to_string()));
                    }
                    self.state
                        .activate_channel(client_id, resp.server_channel_id);
                    return Ok(resp.server_channel_id);
                }
            }
            self.dispatch_frame(frame).await;
        }
    }

    /// Start a monitor on a channel. Returns a `MonitorHandle` for receiving events.
    pub async fn start_monitor(
        &mut self,
        pv_name: &str,
        server_channel_id: i32,
        buffer: usize,
    ) -> Result<MonitorHandle, SessionError> {
        let request_id = next_client_id();
        let (tx, handle) = if self.bus_tx.is_some() {
            MonitorHandle::bus_handle(pv_name)
        } else {
            MonitorHandle::channel(pv_name, buffer)
        };

        // Find the client_id for this channel via ConnectionState.
        let channel_info = self
            .state
            .find_channel_by_name(pv_name)
            .ok_or_else(|| SessionError::Protocol(format!("no channel for PV {pv_name}")))?;
        let _client_id = channel_info.client_channel_id;

        // Send MONITOR INIT.
        let req = MonitorRequest::init(server_channel_id, request_id);
        let mut writer = PvaWriter::new(self.state.byte_order);
        req.encode(&mut writer);
        self.tcp.send_msg(CMD_MONITOR, writer.as_bytes()).await?;
        self.state.messages_sent += 1;

        // Create the subscription tracker (from monitor/subscription.rs).
        let subscription = MonitorSubscription::new(pv_name, server_channel_id, request_id);

        self.monitors.insert(
            request_id,
            MonitorEntry {
                request_id,
                subscription,
                tx,
                pv_index: next_pv_index(),
                bus_shard: self.bus_tx.as_ref().map_or(0, |b| {
                    crate::monitor::bus::shard_for_pv(pv_name, b.n_shards())
                }),
                pv_id: 0,
                fast_layout: None,
                fast_array_layout: None,
                fast_enum_layout: None,
            },
        );

        // Wait for INIT response (contains type description).
        loop {
            let frame = self.tcp.recv_frame().await?;
            self.state.messages_received += 1;

            if frame.header.command == CMD_MONITOR {
                let mut reader = PvaReader::new(&frame.payload, self.state.byte_order);
                let hdr = MonitorResponseHeader::decode(&mut reader)
                    .map_err(|e| SessionError::Protocol(format!("decode MONITOR: {e}")))?;

                if hdr.request_id == request_id
                    && hdr.sub_command == MonitorSubCommand::Init.to_u8()
                {
                    let status = PvaStatus::decode(&mut reader)
                        .map_err(|e| SessionError::Protocol(format!("decode status: {e}")))?;
                    if !status.is_ok() {
                        self.monitors.remove(&request_id);
                        return Err(SessionError::MonitorFailed(status.to_string()));
                    }

                    let desc = FieldDesc::decode(&mut reader, &mut self.state.registry)
                        .map_err(|e| SessionError::Protocol(format!("decode FieldDesc: {e}")))?
                        .ok_or_else(|| {
                            SessionError::Protocol("null FieldDesc in MONITOR INIT".into())
                        })?;

                    if let Some(mon) = self.monitors.get_mut(&request_id) {
                        mon.fast_layout = FastScalarLayout::detect(&desc);
                        mon.fast_array_layout = FastArrayLayout::detect(&desc);
                        mon.fast_enum_layout = FastEnumLayout::detect(&desc);
                        mon.subscription.set_type_desc(desc);
                        mon.subscription.activate();
                    }

                    // Send START with PIPELINE bit like PVXS/pvget.
                    let mut w2 = PvaWriter::new(self.state.byte_order);
                    MonitorRequest::start_pipeline(server_channel_id, request_id).encode(&mut w2);
                    self.tcp.send_msg(CMD_MONITOR, w2.as_bytes()).await?;
                    self.state.messages_sent += 1;

                    return Ok(handle);
                }
            }
            self.dispatch_frame(frame).await;
        }
    }

    /// Event loop that also accepts commands to add new monitors on the same TCP connection (session multiplexing).
    pub async fn run_event_loop_with_commands(
        mut self,
        mut cmd_rx: mpsc::Receiver<super::driver::SessionCommand>,
    ) -> Result<(), SessionError> {
        let mut echo_timer = tokio::time::interval(std::time::Duration::from_secs(15));
        echo_timer.tick().await;

        loop {
            tokio::select! {
                frame_result = self.tcp.recv_frame() => {
                    let frame = frame_result?;
                    self.state.messages_received += 1;
                    self.dispatch_frame(frame).await;
                }
                _ = echo_timer.tick() => {
                    let echo_payload: &[u8] = b"aura";
                    if let Err(e) = self.tcp.send_msg(CMD_ECHO, echo_payload).await {
                        tracing::debug!(error = %e, "echo send failed");
                        return Err(SessionError::Transport(e));
                    }
                    self.state.messages_sent += 1;
                }
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        super::driver::SessionCommand::AddMonitor { pv_name, reply } => {
                            tracing::debug!(pv = %pv_name, "adding monitor on existing session");
                            let result = self.add_monitor_inline(&pv_name).await;
                            let _ = reply.send(result);
                        }
                        super::driver::SessionCommand::AddMonitorBatch { pv_names, reply } => {
                            tracing::info!(count = pv_names.len(), "adding monitor batch (bulk mode)");
                            let results = self.add_monitors_bulk(&pv_names).await;
                            tracing::info!(
                                ok = results.iter().filter(|(_, r)| r.is_ok()).count(),
                                fail = results.iter().filter(|(_, r)| r.is_err()).count(),
                                "bulk batch complete"
                            );
                            let _ = reply.send(results);
                        }
                        super::driver::SessionCommand::RemoveMonitor { pv_name } => {
                            self.remove_monitor_inline(&pv_name).await;
                        }
                        super::driver::SessionCommand::Shutdown => {
                           let _ = self.tcp.stream.shutdown().await;
                           return Ok(());
                       }
                    }
                }
            }
        }
    }

    /// Add a new monitor on this session inline (called from the event loop when
    /// a SessionCommand::AddMonitor arrives). Creates the channel and starts the monitor on
    /// the same TCP connection.
    async fn add_monitor_inline(&mut self, pv_name: &str) -> Result<MonitorHandle, SessionError> {
        let server_cid = self.create_channel(pv_name).await?;
        tracing::debug!(pv = pv_name, server_cid, "channel created inline");
        let handle = self.start_monitor(pv_name, server_cid, 256).await?;
        tracing::info!(
            pv = pv_name,
            monitors = self.monitors.len(),
            "monitor added on existing session"
        );
        Ok(handle)
    }

    /// D4: Remove a monitor - sends CMD_MONITOR STOP + CMD_DESTROY_CHANNEL.
    async fn remove_monitor_inline(&mut self, pv_name: &str) {
        // Find the channel for this PV.
        let channel_entry = self
            .state
            .channels
            .iter()
            .find(|(_, info)| info.pv_name.as_ref() == pv_name);
        let (client_cid, server_cid) = match channel_entry {
            Some((_, info)) => match info.state.server_id() {
                Some(scid) => (info.client_channel_id, scid),
                None => {
                    tracing::debug!(pv = pv_name, "remove_monitor: channel not active");
                    return;
                }
            },
            None => {
                tracing::debug!(pv = pv_name, "remove_monitor: channel not found");
                return;
            }
        };

        // Find the monitor by PV name.
        let monitor_id = self
            .monitors
            .iter()
            .find(|(_, entry)| entry.subscription.pv_name.as_ref() == pv_name)
            .map(|(id, _)| *id);

        // Send CMD_MONITOR STOP.
        if let Some(req_id) = monitor_id {
            let mut writer = PvaWriter::new(self.state.byte_order);
            MonitorRequest::stop(server_cid, req_id).encode(&mut writer);
            let _ = self.tcp.send_msg(CMD_MONITOR, writer.as_bytes()).await;
            self.monitors.remove(&req_id);
            tracing::debug!(pv = pv_name, req_id, "monitor stopped");
        }

        // Send CMD_DESTROY_CHANNEL with correct client + server IDs.
        let mut payload = Vec::with_capacity(8);
        let order = self.state.byte_order;
        payload.extend_from_slice(&order.write_i32(client_cid));
        payload.extend_from_slice(&order.write_i32(server_cid));
        let _ = self.tcp.send_msg(CMD_DESTROY_CHANNEL, &payload).await;

        self.state
            .channels
            .retain(|_, info| info.pv_name.as_ref() != pv_name);
        tracing::info!(
            pv = pv_name,
            monitors = self.monitors.len(),
            "monitor removed"
        );
    }

    /// Bulk create channels + start monitors for many PVs at once.
    ///
    /// Three-phase pipeline - all sends are batched before reads:
    /// Phase 1: Send all CREATE_CHANNEL (pipelined) → read all responses
    /// Phase 2: Send ALL MONITOR INIT at once → read all INIT responses
    /// Phase 3: Send ALL MONITOR START at once (fire-and-forget)
    pub async fn add_monitors_bulk(
        &mut self,
        pv_names: &[String],
    ) -> Vec<(String, Result<MonitorHandle, SessionError>)> {
        let mut results: Vec<(String, Result<MonitorHandle, SessionError>)> =
            Vec::with_capacity(pv_names.len());

        if pv_names.is_empty() {
            return results;
        }

        let mut id_to_pv: HashMap<i32, std::sync::Arc<str>> =
            HashMap::with_capacity(pv_names.len());

        for pv in pv_names {
            let arc_pv: std::sync::Arc<str> = pv.as_str().into();
            let client_id = self.state.add_channel(std::sync::Arc::clone(&arc_pv));
            id_to_pv.insert(client_id, arc_pv);
        }

        let mut writer = PvaWriter::new(self.state.byte_order);

        // PHASE 1: Full-pipeline CREATE_CHANNEL (send ALL, read ALL)
        //
        // PVA is pipelined - send all requests at once into the TCP buffer,
        // flush once, then read all responses. 1 round-trip instead of N chunks.
        let mut channel_map: HashMap<i32, (std::sync::Arc<str>, i32)> =
            HashMap::with_capacity(id_to_pv.len());
        let mut channel_failures: std::collections::HashSet<i32> = std::collections::HashSet::new();

        // Send ALL CREATE_CHANNEL requests.
        self.tcp.write_buf.clear();
        let total_channels = id_to_pv.len();
        let mut sent = 0usize;
        for (&client_id, pv_name) in &id_to_pv {
            writer.clear();
            writer.write_u16(1);
            writer.write_i32(client_id);
            writer.write_string(pv_name);
            self.tcp.buffer_msg(CMD_CREATE_CHANNEL, writer.as_bytes());
            self.state.messages_sent += 1;
            sent += 1;
            // Flush every 10k to keep TCP buffer from growing unbounded.
            if sent.is_multiple_of(10_000) && self.tcp.flush_writes().await.is_err() {
                for pv in id_to_pv.values() {
                    results.push((
                        pv.to_string(),
                        Err(SessionError::Protocol("flush failed".into())),
                    ));
                }
                return results;
            }
        }
        if self.tcp.flush_writes().await.is_err() {
            for pv in id_to_pv.values() {
                results.push((
                    pv.to_string(),
                    Err(SessionError::Protocol("flush failed".into())),
                ));
            }
            return results;
        }

        // Read ALL responses (single pass, adaptive timeout).
        let mut got = 0usize;
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(30 + (total_channels as u64 / 500).max(1));

        while got < total_channels {
            let frame = match tokio::time::timeout_at(deadline, self.tcp.recv_frame()).await {
                Ok(Ok(f)) => f,
                Ok(Err(_)) => break,
                Err(_) => {
                    tracing::warn!(done = got, expected = total_channels, "Phase 1 timeout");
                    break;
                }
            };
            self.state.messages_received += 1;

            if frame.header.command == CMD_CREATE_CHANNEL {
                let mut reader = PvaReader::new(&frame.payload, self.state.byte_order);
                if let Ok(resp) = CreateChannelResponse::decode(&mut reader) {
                    got += 1;
                    if resp.status.is_ok() {
                        self.state
                            .activate_channel(resp.client_channel_id, resp.server_channel_id);
                        if let Some(pv) = id_to_pv.get(&resp.client_channel_id) {
                            channel_map.insert(
                                resp.client_channel_id,
                                (std::sync::Arc::clone(pv), resp.server_channel_id),
                            );
                        }
                    } else {
                        self.state.close_channel(resp.client_channel_id);
                        channel_failures.insert(resp.client_channel_id);
                    }
                }
            } else {
                self.dispatch_frame(frame).await;
            }
        }
        tracing::info!(
            ok = channel_map.len(),
            fail = channel_failures.len(),
            "Phase 1 complete"
        );

        // Record failures.
        for cid in &channel_failures {
            if let Some(pv) = id_to_pv.get(cid) {
                results.push((
                    pv.to_string(),
                    Err(SessionError::ChannelCreateFailed("server rejected".into())),
                ));
            }
        }

        // PHASE 2: Chunked MONITOR INIT (5k per chunk)

        struct PendingMonitor {
            pv_name: std::sync::Arc<str>,
            request_id: i32,
            server_cid: i32,
            handle: MonitorHandle,
        }

        // Build all PendingMonitors first (need handle + MonitorEntry).
        let channel_entries: Vec<(std::sync::Arc<str>, i32)> = channel_map
            .values()
            .map(|(pv, scid)| (std::sync::Arc::clone(pv), *scid))
            .collect();

        let mut all_pending: Vec<PendingMonitor> = Vec::with_capacity(channel_entries.len());
        for (pv_name, server_cid) in &channel_entries {
            let request_id = next_client_id();
            let (tx, handle) = if self.bus_tx.is_some() {
                MonitorHandle::bus_handle(&**pv_name)
            } else {
                MonitorHandle::channel(&**pv_name, 32)
            };
            let subscription = MonitorSubscription::new(&**pv_name, *server_cid, request_id);
            self.monitors.insert(
                request_id,
                MonitorEntry {
                    request_id,
                    subscription,
                    tx,
                    pv_index: next_pv_index(),
                    bus_shard: self.bus_tx.as_ref().map_or(0, |b| {
                        crate::monitor::bus::shard_for_pv(pv_name, b.n_shards())
                    }),
                    pv_id: 0,
                    fast_layout: None,
                    fast_array_layout: None,
                    fast_enum_layout: None,
                },
            );
            all_pending.push(PendingMonitor {
                pv_name: std::sync::Arc::clone(pv_name),
                request_id,
                server_cid: *server_cid,
                handle,
            });
        }

        // Send ALL MONITOR INIT at once, read ALL responses.
        let mut init_done: std::collections::HashSet<i32> = std::collections::HashSet::new();
        {
            self.tcp.write_buf.clear();
            let mut sent = 0usize;
            for pm in &all_pending {
                writer.clear();
                MonitorRequest::init(pm.server_cid, pm.request_id).encode(&mut writer);
                self.tcp.buffer_msg(CMD_MONITOR, writer.as_bytes());
                self.state.messages_sent += 1;
                sent += 1;
                if sent.is_multiple_of(10_000) {
                    let _ = self.tcp.flush_writes().await;
                }
            }
            let _ = self.tcp.flush_writes().await;
        }

        // Read ALL INIT responses (single pass, adaptive timeout).
        {
            let total_expected = all_pending.len();
            let mut got = 0usize;
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_secs(30 + (total_expected as u64 / 500).max(1));

            while got < total_expected {
                let frame = match tokio::time::timeout_at(deadline, self.tcp.recv_frame()).await {
                    Ok(Ok(f)) => f,
                    Ok(Err(_)) => break,
                    Err(_) => {
                        tracing::warn!(done = got, expected = total_expected, "Phase 2 timeout");
                        break;
                    }
                };
                self.state.messages_received += 1;

                if frame.header.command == CMD_MONITOR {
                    let mut reader = PvaReader::new(&frame.payload, self.state.byte_order);
                    if let Ok(hdr) = MonitorResponseHeader::decode(&mut reader) {
                        if hdr.sub_command == MonitorSubCommand::Init.to_u8() {
                            if let Ok(status) = PvaStatus::decode(&mut reader) {
                                if status.is_ok()
                                    && let Ok(Some(desc)) =
                                        FieldDesc::decode(&mut reader, &mut self.state.registry)
                                    && let Some(mon) = self.monitors.get_mut(&hdr.request_id)
                                {
                                    mon.fast_layout = FastScalarLayout::detect(&desc);
                                    mon.fast_array_layout = FastArrayLayout::detect(&desc);
                                    mon.fast_enum_layout = FastEnumLayout::detect(&desc);
                                    mon.subscription.set_type_desc(desc);
                                    mon.subscription.activate();
                                }
                                init_done.insert(hdr.request_id);
                                got += 1;
                            }
                        } else {
                            self.handle_monitor_update(&frame.payload);
                        }
                    }
                } else {
                    self.dispatch_frame(frame).await;
                }
            }
        }
        tracing::info!(
            init_ok = init_done.len(),
            expected = all_pending.len(),
            "Phase 2 complete"
        );

        // PHASE 2b: RETRY — re-send INIT for PVs that timed out.

        let missing_init: Vec<&PendingMonitor> = all_pending
            .iter()
            .filter(|pm| !init_done.contains(&pm.request_id))
            .collect();
        if !missing_init.is_empty() {
            tracing::warn!(
                missing = missing_init.len(),
                "Phase 2b: retrying INIT for timed-out PVs"
            );
            self.tcp.write_buf.clear();
            for pm in &missing_init {
                writer.clear();
                MonitorRequest::init(pm.server_cid, pm.request_id).encode(&mut writer);
                self.tcp.buffer_msg(CMD_MONITOR, writer.as_bytes());
                self.state.messages_sent += 1;
            }
            let _ = self.tcp.flush_writes().await;

            let retry_expected = missing_init.len();
            let mut retry_got = 0usize;
            let retry_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);

            while retry_got < retry_expected {
                let frame =
                    match tokio::time::timeout_at(retry_deadline, self.tcp.recv_frame()).await {
                        Ok(Ok(f)) => f,
                        Ok(Err(_)) => break,
                        Err(_) => break,
                    };
                self.state.messages_received += 1;
                if frame.header.command == CMD_MONITOR {
                    let mut reader = PvaReader::new(&frame.payload, self.state.byte_order);
                    if let Ok(hdr) = MonitorResponseHeader::decode(&mut reader) {
                        if hdr.sub_command == MonitorSubCommand::Init.to_u8() {
                            if let Ok(status) = PvaStatus::decode(&mut reader) {
                                if status.is_ok()
                                    && let Ok(Some(desc)) =
                                        FieldDesc::decode(&mut reader, &mut self.state.registry)
                                    && let Some(mon) = self.monitors.get_mut(&hdr.request_id)
                                {
                                    mon.fast_layout = FastScalarLayout::detect(&desc);
                                    mon.fast_array_layout = FastArrayLayout::detect(&desc);
                                    mon.fast_enum_layout = FastEnumLayout::detect(&desc);
                                    mon.subscription.set_type_desc(desc);
                                    mon.subscription.activate();
                                }
                                init_done.insert(hdr.request_id);
                                retry_got += 1;
                            }
                        } else {
                            self.handle_monitor_update(&frame.payload);
                        }
                    }
                } else {
                    self.dispatch_frame(frame).await;
                }
            }
            tracing::info!(
                recovered = retry_got,
                still_missing = retry_expected - retry_got,
                "Phase 2b complete"
            );
        }

        // PHASE 3: Bulk MONITOR START (fire-and-forget)

        self.tcp.write_buf.clear();
        let mut start_count = 0u32;
        for pm in &all_pending {
            if init_done.contains(&pm.request_id) {
                writer.clear();
                MonitorRequest::start_pipeline(pm.server_cid, pm.request_id).encode(&mut writer);
                self.tcp.buffer_msg(CMD_MONITOR, writer.as_bytes());
                self.state.messages_sent += 1;
                start_count += 1;
                if start_count.is_multiple_of(10_000) {
                    let _ = self.tcp.flush_writes().await;
                }
            }
        }
        let _ = self.tcp.flush_writes().await;
        tracing::info!(count = all_pending.len(), "Phase 3: sent all MONITOR START");

        // Build final results.
        for pm in all_pending {
            if init_done.contains(&pm.request_id) {
                results.push((pm.pv_name.to_string(), Ok(pm.handle)));
            } else {
                self.monitors.remove(&pm.request_id);
                results.push((
                    pm.pv_name.to_string(),
                    Err(SessionError::Protocol("INIT not received".into())),
                ));
            }
        }

        tracing::info!(
            total = results.len(),
            ok = results.iter().filter(|(_, r)| r.is_ok()).count(),
            "bulk subscribe complete"
        );
        results
    }

    /// Dispatch a received frame to the appropriate handler.
    async fn dispatch_frame(&mut self, frame: crate::codec::framing::PvaFrame) {
        match frame.header.command {
            CMD_MONITOR => {
                tracing::trace!(payload_len = frame.payload.len(), "monitor update received");
                self.handle_monitor_update(&frame.payload);
            }
            CMD_ECHO => {
                // Server echo request — reply with same payload.
                let _ = self.tcp.send_msg(CMD_ECHO, &frame.payload).await;
            }
            _ => {
                tracing::trace!(cmd = frame.header.command, "unhandled frame in event loop");
            }
        }
    }

    /// Handle a monitor update - decode delta using the bitset to read only changed fields.
    fn handle_monitor_update(&mut self, payload: &[u8]) {
        let mut reader = PvaReader::new(payload, self.state.byte_order);
        let hdr = match MonitorResponseHeader::decode(&mut reader) {
            Ok(h) => h,
            Err(e) => {
                tracing::debug!(error = %e, "failed to decode monitor header");
                return;
            }
        };

        let mon = match self.monitors.get_mut(&hdr.request_id) {
            Some(m) => m,
            None => {
                tracing::trace!(
                    rid = hdr.request_id,
                    "monitor update for unknown request_id"
                );
                return;
            }
        };

        let desc = match mon.subscription.type_desc.as_ref() {
            Some(d) => d,
            None => {
                tracing::debug!("monitor update before type_desc set");
                return;
            }
        };

        // Clone once — needed because we later mutate `mon` for the send.
        let desc = desc.clone();

        // Decode changed bitset.
        let changed = match PvaBitSet::decode(&mut reader) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(error = %e, "failed to decode changed bitset");
                return;
            }
        };

        // Decode only the fields whose bits are set in `changed`.
        // PVA delta encoding: bit 0 = the root structure, bit 1+ = fields.
        //
        // TWO MODES:
        // 1. Bit 0 IS set (initial full update after MONITOR START):
        //    The server sends ALL fields sequentially. We decode the entire
        //    structure without consulting the bitset for individual fields.
        // 2. Bit 0 is NOT set (delta update after pvput/scan):
        //    Only fields whose bit is set have data in the payload.
        //    We use decode_delta_structure to read selectively.

        let is_full_update = changed.is_set(0);

        // FAST PATH: zero-alloc NTScalar delta decode
        if !is_full_update && mon.subscription.updates_received > 0 {
            if let Some(ref layout) = mon.fast_layout {
                // Check no unknown fields changed (display/control/valueAlarm).
                let has_unknown = changed
                    .iter_set()
                    .any(|b| b > layout.max_known_bit as usize);
                if !has_unknown {
                    // Read fields in bit order — payload is sequential.
                    let mut value = 0.0f64;
                    let mut string_value: Option<String> = None;
                    let mut severity = 0i32;
                    let mut status = 0i32;
                    let mut seconds = 0i64;
                    let mut nanos = 0i32;
                    let is_string = layout.value_type == ScalarType::String;

                    let fast_ok = (|| -> Result<(), crate::codec::pvdata::DecodeError> {
                        for bit_usize in changed.iter_set() {
                            let bit = bit_usize as u8;
                            if bit == 0 {
                                continue;
                            } // skip root structure bit
                            if bit > layout.max_known_bit {
                                break;
                            } // unknown field
                            if bit == layout.value_bit {
                                if is_string {
                                    string_value = Some(reader.read_string()?);
                                } else {
                                    value = layout.read_value(&mut reader)?;
                                }
                            } else if bit == layout.severity_bit {
                                severity = reader.read_i32()?;
                            } else if bit == layout.status_bit {
                                status = reader.read_i32()?;
                            } else if bit == layout.message_bit {
                                let _ = reader.read_string()?; // skip alarm message
                            } else if bit == layout.seconds_bit {
                                seconds = reader.read_i64()?;
                            } else if bit == layout.nanos_bit {
                                nanos = reader.read_i32()?;
                            } else if bit == layout.user_tag_bit {
                                let _ = reader.read_i32()?; // skip userTag
                            }
                            // Structure marker bits (alarm, timeStamp) have no data — skip.
                        }
                        Ok(())
                    })();

                    if fast_ok.is_ok() {
                        mon.subscription.updates_received += 1;
                        // Lazy-resolve pv_id (once per monitor, not per event).
                        if mon.pv_id == 0
                            && let Some(ref pc) = self.pv_cache
                            && let Some(&id) = pc.load().get(&*mon.subscription.pv_name)
                        {
                            mon.pv_id = id;
                        }

                        let event = if let Some(sv) = string_value {
                            MonitorEvent::StringDelta {
                                value: sv,
                                seconds,
                                nanos,
                                severity,
                                status,
                            }
                        } else {
                            MonitorEvent::ScalarDelta {
                                value,
                                seconds,
                                nanos,
                                severity,
                                status,
                            }
                        };
                        if let Some(ref bus) = self.bus_tx {
                            let pv_name = std::sync::Arc::clone(&mon.subscription.pv_name);
                            let _ = bus.send_to_shard(pv_name, mon.pv_id, mon.bus_shard, event);
                        } else {
                            let _ = mon.tx.try_send(event);
                        }
                        return; // Fast path complete — no PvaValue allocated.
                    }
                    // Fast decode failed — fall through to generic decoder.
                }
            }
            // ARRAY FAST PATH (NTScalarArray)
            else if let Some(ref layout) = mon.fast_array_layout {
                let has_unknown = changed
                    .iter_set()
                    .any(|b| b > layout.max_known_bit as usize);
                if !has_unknown && changed.is_set(layout.value_bit as usize) {
                    let mut severity = 0i32;
                    let mut status = 0i32;
                    let mut seconds = 0i64;
                    let mut nanos = 0i32;
                    let mut values: Vec<f64> = Vec::new();

                    let fast_ok = (|| -> Result<(), crate::codec::pvdata::DecodeError> {
                        for bit_usize in changed.iter_set() {
                            let bit = bit_usize as u8;
                            if bit == 0 {
                                continue;
                            }
                            if bit > layout.max_known_bit {
                                break;
                            }
                            if bit == layout.value_bit {
                                values = layout.read_array(&mut reader)?;
                            } else if bit == layout.severity_bit {
                                severity = reader.read_i32()?;
                            } else if bit == layout.status_bit {
                                status = reader.read_i32()?;
                            } else if bit == layout.message_bit {
                                let _ = reader.read_string()?;
                            } else if bit == layout.seconds_bit {
                                seconds = reader.read_i64()?;
                            } else if bit == layout.nanos_bit {
                                nanos = reader.read_i32()?;
                            } else if bit == layout.user_tag_bit {
                                let _ = reader.read_i32()?;
                            }
                        }
                        Ok(())
                    })();

                    if fast_ok.is_ok() && !values.is_empty() {
                        mon.subscription.updates_received += 1;
                        if mon.pv_id == 0
                            && let Some(ref pc) = self.pv_cache
                            && let Some(&id) = pc.load().get(&*mon.subscription.pv_name)
                        {
                            mon.pv_id = id;
                        }
                        let event = MonitorEvent::ArrayDelta {
                            values,
                            seconds,
                            nanos,
                            severity,
                            status,
                        };
                        if let Some(ref bus) = self.bus_tx {
                            let pv_name = std::sync::Arc::clone(&mon.subscription.pv_name);
                            let _ = bus.send_to_shard(pv_name, mon.pv_id, mon.bus_shard, event);
                        } else {
                            let _ = mon.tx.try_send(event);
                        }
                        return;
                    }
                }
            }
            // ENUM FAST PATH (NTEnum)
            else if let Some(ref layout) = mon.fast_enum_layout {
                let has_unknown = changed
                    .iter_set()
                    .any(|b| b > layout.max_known_bit as usize);
                if !has_unknown && changed.is_set(layout.index_bit as usize) {
                    let mut index_value = 0i32;
                    let mut severity = 0i32;
                    let mut status = 0i32;
                    let mut seconds = 0i64;
                    let mut nanos = 0i32;

                    let fast_ok = (|| -> Result<(), crate::codec::pvdata::DecodeError> {
                        for bit_usize in changed.iter_set() {
                            let bit = bit_usize as u8;
                            if bit == 0 {
                                continue;
                            }
                            if bit > layout.max_known_bit {
                                break;
                            }
                            if bit == layout.value_struct_bit {
                                // Structure marker — no data on the wire.
                            } else if bit == layout.index_bit {
                                index_value = reader.read_i32()?;
                            } else if bit == layout.choices_bit {
                                // Skip choices string array without allocating.
                                let len = reader.read_size_non_null()?;
                                for _ in 0..len {
                                    let _ = reader.read_string()?;
                                }
                            } else if bit == layout.severity_bit {
                                severity = reader.read_i32()?;
                            } else if bit == layout.status_bit {
                                status = reader.read_i32()?;
                            } else if bit == layout.message_bit {
                                let _ = reader.read_string()?;
                            } else if bit == layout.seconds_bit {
                                seconds = reader.read_i64()?;
                            } else if bit == layout.nanos_bit {
                                nanos = reader.read_i32()?;
                            } else if bit == layout.user_tag_bit {
                                let _ = reader.read_i32()?;
                            }
                        }
                        Ok(())
                    })();

                    if fast_ok.is_ok() {
                        mon.subscription.updates_received += 1;
                        if mon.pv_id == 0
                            && let Some(ref pc) = self.pv_cache
                            && let Some(&id) = pc.load().get(&*mon.subscription.pv_name)
                        {
                            mon.pv_id = id;
                        }
                        // Emit ScalarDelta — NTEnum index stored as f64 in samples table.
                        let event = MonitorEvent::ScalarDelta {
                            value: index_value as f64,
                            seconds,
                            nanos,
                            severity,
                            status,
                        };
                        if let Some(ref bus) = self.bus_tx {
                            let pv_name = std::sync::Arc::clone(&mon.subscription.pv_name);
                            let _ = bus.send_to_shard(pv_name, mon.pv_id, mon.bus_shard, event);
                        } else {
                            let _ = mon.tx.try_send(event);
                        }
                        return;
                    }
                }
            }
        }
        // END FAST PATH

        let decode_result: Result<PvaValue, crate::codec::pvdata::DecodeError> = if is_full_update {
            PvaValue::decode_from(&mut reader, &desc)
        } else {
            // Delta update: decode only changed fields.
            let mut flat_idx = 0usize;
            flat_idx += 1; // Skip root structure bit (index 0).
            let mut fields = Vec::with_capacity(desc.fields.len());
            (|| {
                for nf in &desc.fields {
                    let child_val =
                        decode_delta_structure(&mut reader, &nf.desc, &changed, &mut flat_idx)?;
                    // Reuse the field name from the FieldDesc.
                    fields.push((nf.name.clone(), child_val));
                }
                Ok(PvaValue::Structure(fields))
            })()
        };

        let value = match decode_result {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    is_full_update,
                    reader_pos = reader.position(),
                    remaining = payload.len().saturating_sub(reader.position()),
                    "failed to decode PvaValue"
                );
                return;
            }
        };

        // HOT PATH
        //
        // For delta updates (99% of traffic): the decoded `value` already
        // contains the changed fields (value, alarm, timeStamp) with Null
        // for unchanged fields. The converter only needs those 3 fields,
        // so we send the delta directly — NO clone, NO merge into cache.
        //
        // For full updates (first sample): we send the complete structure
        // and cache it for metadata extraction (display, units, etc.)

        if is_full_update {
            mon.subscription.last_value = Some(value.clone());
        }
        // Push metadata on the FIRST update for this monitor regardless of bitset.
        if mon.subscription.updates_received == 0
            && let Some(ref buf) = self.metadata_buf
            && let Ok(mut v) = buf.lock()
        {
            let pv = std::sync::Arc::clone(&mon.subscription.pv_name);
            tracing::debug!(pv = %pv, is_full = is_full_update, buf_len = v.len(), "metadata push");
            v.push((pv, value.clone()));
        }

        mon.subscription.updates_received += 1;

        // Send the value - use aggregated bus if available, else per-PV channel.
        // Send the value - use aggregated bus if available, else per-PV channel.
        if mon.pv_id == 0
            && let Some(ref pc) = self.pv_cache
            && let Some(&id) = pc.load().get(&*mon.subscription.pv_name)
        {
            mon.pv_id = id;
        }
        let event = MonitorEvent::Value(value);
        if let Some(ref bus) = self.bus_tx {
            let pv_name = std::sync::Arc::clone(&mon.subscription.pv_name);
            match bus.send_to_shard(pv_name, mon.pv_id, mon.bus_shard, event) {
                Ok(_) => tracing::trace!(
                    pv = %mon.subscription.pv_name,
                    "monitor event sent via bus"
                ),
                Err(_) => tracing::trace!(
                    pv = %mon.subscription.pv_name,
                    "bus send failed (shard channel full)"
                ),
            }
        } else {
            match mon.tx.try_send(event) {
                Ok(_) => tracing::trace!(
                    pv = %mon.subscription.pv_name,
                    updates = mon.subscription.updates_received,
                    "monitor event sent"
                ),
                Err(e) => tracing::trace!(
                    pv = %mon.subscription.pv_name,
                    error = %e,
                    "monitor event send failed (channel full or closed)"
                ),
            }
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.state.addr
    }
    pub fn byte_order(&self) -> ByteOrder {
        self.state.byte_order
    }
    pub fn channel_count(&self) -> usize {
        self.state.channels.len()
    }
    pub fn active_channel_count(&self) -> usize {
        self.state.active_channel_count()
    }
    pub fn monitor_count(&self) -> usize {
        self.monitors.len()
    }
    pub fn messages_sent(&self) -> u64 {
        self.state.messages_sent
    }
    pub fn messages_received(&self) -> u64 {
        self.state.messages_received
    }

    /// Receive a CMD_SEARCH_RESPONSE from the TCP connection.
    /// Skips non-search messages (beacons, echos) while waiting.
    pub async fn recv_search_response(&mut self) -> Result<SearchResponse, SessionError> {
        use crate::messages::search::SearchResponse;
        loop {
            let frame = self
                .tcp
                .recv_frame()
                .await
                .map_err(SessionError::Transport)?;
            self.state.messages_received += 1;

            if frame.header.command == CMD_SEARCH_RESPONSE {
                let byte_order = self.state.byte_order;
                let mut reader = PvaReader::new(&frame.payload, byte_order);
                let resp = SearchResponse::decode(&mut reader)
                    .map_err(|e| SessionError::Protocol(format!("decode search response: {e}")))?;
                return Ok(resp);
            }
            tracing::trace!(cmd = frame.header.command, "skipping non-search message");
        }
    }

    /// Search for PVs on this IOC via TCP CMD_SEARCH.
    /// Returns the list of PV names that this IOC hosts.
    /// Sends search requests in batches of 50 PVs per packet, reads all responses with a timeout.
    pub async fn search_pvs(&mut self, pv_names: &[String]) -> Vec<String> {
        use crate::messages::search::SearchRequest;

        let mut found: Vec<String> = Vec::new();
        if pv_names.is_empty() {
            return found;
        }

        let batch_size = 500; // 10× bigger — fewer packets, fewer syscalls
        let mut id_to_pv: HashMap<i32, String> = HashMap::with_capacity(pv_names.len());
        let mut writer = PvaWriter::new(self.state.byte_order);

        // Send all search requests, flush every ~50k PVs.
        let mut batches_sent = 0u32;
        for chunk in pv_names.chunks(batch_size) {
            let channels: Vec<(i32, String)> = chunk
                .iter()
                .map(|pv| {
                    let id = next_client_id();
                    id_to_pv.insert(id, pv.clone());
                    (id, pv.clone())
                })
                .collect();

            let local_addr = self.tcp.stream.local_addr().unwrap_or_else(|_| {
                SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
            });
            let req = SearchRequest::multi(next_client_id(), channels, local_addr);
            writer.clear();
            req.encode(&mut writer);
            self.tcp.buffer_msg(CMD_SEARCH, writer.as_bytes());
            self.state.messages_sent += 1;
            batches_sent += 1;

            if batches_sent.is_multiple_of(100) && self.tcp.flush_writes().await.is_err() {
                return found;
            }
        }
        if self.tcp.flush_writes().await.is_err() {
            return found;
        }

        // Read search responses with timeout.
        // Each response contains the IDs of PVs that this IOC hosts.
        let expected_responses = pv_names.len().div_ceil(batch_size);
        let timeout_secs = 10 + (pv_names.len() as u64 / 10_000).max(1);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        let mut responses_received = 0usize;

        while responses_received < expected_responses {
            match tokio::time::timeout_at(deadline, self.recv_search_response()).await {
                Ok(Ok(resp)) => {
                    responses_received += 1;
                    for search_id in &resp.found {
                        if let Some(pv) = id_to_pv.remove(search_id) {
                            found.push(pv);
                        }
                    }
                }
                Ok(Err(_)) => break, // Connection error.
                Err(_) => {
                    tracing::debug!(
                        got = responses_received,
                        expected = expected_responses,
                        found = found.len(),
                        "search timeout"
                    );
                    break;
                }
            }
        }

        found
    }
}

impl std::fmt::Display for PvaSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Session[{} {} ch={} mon={} tx={} rx={}]",
            self.state.addr,
            self.state.byte_order,
            self.state.channels.len(),
            self.monitors.len(),
            self.state.messages_sent,
            self.state.messages_received
        )
    }
}
