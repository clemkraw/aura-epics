//! CMD_SEARCH (0x03) / CMD_SEARCH_RESPONSE (0x04) — PV discovery.
//!
//! Search is how a PVA client finds which server hosts a given PV.
//! AURA sends SEARCH over TCP (via name_servers), the server that owns
//! the PV replies with SEARCH_RESPONSE containing its TCP address.

use crate::codec::pvdata::{DecodeError, PvaReader, PvaWriter};
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

/// GUID length in bytes (PVA server identifier).
const GUID_LEN: usize = 12;

/// Encode a socket address in PVA search format.
///
/// Search uses a different format from beacon:
/// - 3 bytes reserved (0x00)
/// - 16 bytes IPv6-mapped address (network byte order)
/// - 2 bytes port (big-endian / network byte order)
fn encode_search_address(addr: &SocketAddr, writer: &mut PvaWriter) {
    // 3 bytes reserved.
    writer.write_raw(&[0u8; 3]);
    // 16 bytes IPv6-mapped address (always, even for IPv4).
    let ipv6 = match addr.ip() {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    writer.write_raw(&ipv6.octets());
    // 2 bytes port, little-endian (PVA protocol uses LE for search address port).
    writer.write_raw(&addr.port().to_le_bytes());
}

/// Decode a socket address in PVA search format.
fn decode_search_address(reader: &mut PvaReader<'_>) -> Result<SocketAddr, DecodeError> {
    // 3 bytes reserved.
    reader.read_bytes(3)?;
    decode_ipv6_address(reader)
}

/// Encode address for search response (16 bytes IPv6 + 2 bytes port BE, no reserved).
fn encode_search_response_address(addr: &SocketAddr, writer: &mut PvaWriter) {
    let ipv6 = match addr.ip() {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    writer.write_raw(&ipv6.octets());
    writer.write_raw(&addr.port().to_le_bytes());
}

/// Decode address for search response (16 bytes IPv6 + 2 bytes port BE, no reserved).
fn decode_search_response_address(reader: &mut PvaReader<'_>) -> Result<SocketAddr, DecodeError> {
    decode_ipv6_address(reader)
}

/// Shared: decode 16 bytes IPv6 + 2 bytes port BE.
fn decode_ipv6_address(reader: &mut PvaReader<'_>) -> Result<SocketAddr, DecodeError> {
    let b = reader.read_bytes(16)?;
    let mut seg = [0u8; 16];
    seg.copy_from_slice(b);
    let ipv6 = Ipv6Addr::from(seg);
    let ip: IpAddr = match ipv6.to_ipv4_mapped() {
        Some(v4) => IpAddr::V4(v4),
        None => IpAddr::V6(ipv6),
    };
    let port_bytes = reader.read_bytes(2)?;
    let port = u16::from_le_bytes([port_bytes[0], port_bytes[1]]);
    Ok(SocketAddr::new(ip, port))
}

/// Search request/response flag bits.
pub mod search_flags {
    /// Server must reply even if it doesn't host the PV.
    pub const REPLY_REQUIRED: u8 = 0x01;
    /// Request sent via unicast (not broadcast).
    pub const UNICAST: u8 = 0x80;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub search_sequence_id: i32,
    pub reply_required: bool,
    pub unicast: bool,
    pub response_address: SocketAddr,
    pub protocol: String,
    /// (client_search_id, pv_name) pairs.
    pub channels: Vec<(i32, String)>,
}

impl SearchRequest {
    pub fn single(search_id: i32, pv_name: impl Into<String>, addr: SocketAddr) -> Self {
        Self {
            search_sequence_id: search_id,
            reply_required: true,
            unicast: false,
            response_address: addr,
            protocol: "tcp".into(),
            channels: vec![(search_id, pv_name.into())],
        }
    }

    pub fn multi(seq: i32, channels: Vec<(i32, String)>, addr: SocketAddr) -> Self {
        Self {
            search_sequence_id: seq,
            reply_required: true,
            unicast: false,
            response_address: addr,
            protocol: "tcp".into(),
            channels,
        }
    }

    /// Number of channels in this search.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Whether this search has no channels.
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let search_sequence_id = reader.read_i32()?;
        let flags = reader.read_u8()?;
        let reply_required = flags & search_flags::REPLY_REQUIRED != 0;
        let unicast = flags & search_flags::UNICAST != 0;
        let response_address = decode_search_address(reader)?;
        let proto_count = reader.read_u8()?;
        let protocol = if proto_count > 0 {
            reader.read_string()?
        } else {
            "tcp".into()
        };
        for _ in 1..proto_count {
            reader.read_string()?;
        }
        let count = reader.read_u16()? as usize;
        let mut channels = Vec::with_capacity(count.min(256));
        for _ in 0..count {
            channels.push((reader.read_i32()?, reader.read_string()?));
        }
        Ok(Self {
            search_sequence_id,
            reply_required,
            unicast,
            response_address,
            protocol,
            channels,
        })
    }

    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_i32(self.search_sequence_id);
        let mut flags = 0u8;
        if self.reply_required {
            flags |= search_flags::REPLY_REQUIRED;
        }
        if self.unicast {
            flags |= search_flags::UNICAST;
        }
        writer.write_u8(flags);
        // PVA search uses fixed-size address format (different from beacon):
        // 3 bytes reserved + 16 bytes IPv6-mapped + 2 bytes port (big-endian).
        encode_search_address(&self.response_address, writer);
        writer.write_u8(1);
        writer.write_string(&self.protocol);
        writer.write_u16(self.channels.len() as u16);
        for (id, name) in &self.channels {
            writer.write_i32(*id);
            writer.write_string(name);
        }
    }
}

impl fmt::Display for SearchRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Search[seq={}] {} channels reply_to={}{}",
            self.search_sequence_id,
            self.channels.len(),
            self.response_address,
            if self.unicast { " unicast" } else { "" }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchResponse {
    pub guid: [u8; GUID_LEN],
    pub search_sequence_id: i32,
    pub server_addr: SocketAddr,
    pub protocol: String,
    pub found: Vec<i32>,
}

impl SearchResponse {
    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let mut guid = [0u8; GUID_LEN];
        guid.copy_from_slice(reader.read_bytes(GUID_LEN)?);
        let search_sequence_id = reader.read_i32()?;
        // Server address: 16 bytes IPv6 + 2 bytes port BE (no reserved, no size prefix).
        let server_addr = decode_search_response_address(reader)?;
        let protocol = reader.read_string()?;
        let _found = reader.read_u8()?; // boolean: found flag
        let count = reader.read_u16()? as usize;
        let mut found = Vec::with_capacity(count.min(256));
        for _ in 0..count {
            found.push(reader.read_i32()?);
        }
        Ok(Self {
            guid,
            search_sequence_id,
            server_addr,
            protocol,
            found,
        })
    }

    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_raw(&self.guid);
        writer.write_i32(self.search_sequence_id);
        // Server address: 16 bytes IPv6 + 2 bytes port BE.
        encode_search_response_address(&self.server_addr, writer);
        writer.write_string(&self.protocol);
        writer.write_u8(if self.found.is_empty() { 0 } else { 1 }); // found flag
        writer.write_u16(self.found.len() as u16);
        for &id in &self.found {
            writer.write_i32(id);
        }
    }

    /// Whether a specific search ID was found.
    pub fn contains(&self, search_id: i32) -> bool {
        self.found.contains(&search_id)
    }

    /// Whether no channels were found.
    pub fn is_empty(&self) -> bool {
        self.found.is_empty()
    }

    /// Number of found channels.
    pub fn found_count(&self) -> usize {
        self.found.len()
    }
}

impl fmt::Display for SearchResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SearchResponse[seq={}] {} found server={}",
            self.search_sequence_id,
            self.found.len(),
            self.server_addr
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

    fn addr() -> SocketAddr {
        "10.0.1.42:5076".parse().unwrap()
    }

    fn srv() -> SocketAddr {
        "10.0.1.100:5075".parse().unwrap()
    }

    fn resp(found: Vec<i32>) -> SearchResponse {
        SearchResponse {
            guid: [0xAA; GUID_LEN],
            search_sequence_id: 42,
            server_addr: srv(),
            protocol: "tcp".into(),
            found,
        }
    }

    #[test]
    fn test_flag_reply_required() {
        assert_eq!(search_flags::REPLY_REQUIRED, 0x01);
    }

    #[test]
    fn test_flag_unicast() {
        assert_eq!(search_flags::UNICAST, 0x80);
    }

    #[test]
    fn test_flags_no_overlap() {
        assert_eq!(search_flags::REPLY_REQUIRED & search_flags::UNICAST, 0);
    }

    #[test]
    fn test_req_single() {
        let r = SearchRequest::single(1, "PERLE:Gun:Vacuum", addr());
        assert_eq!(r.channel_count(), 1);
        assert_eq!(r.channels[0].1, "PERLE:Gun:Vacuum");
        assert!(r.reply_required);
        assert!(!r.unicast);
        assert!(!r.is_empty());
    }

    #[test]
    fn test_req_multi() {
        let r = SearchRequest::multi(
            100,
            vec![(1, "A".into()), (2, "B".into()), (3, "C".into())],
            addr(),
        );
        assert_eq!(r.channel_count(), 3);
        assert_eq!(r.search_sequence_id, 100);
    }

    #[test]
    fn test_req_empty() {
        let r = SearchRequest::multi(0, vec![], addr());
        assert!(r.is_empty());
        assert_eq!(r.channel_count(), 0);
    }

    #[test]
    fn test_req_rt_multi() {
        let o = SearchRequest::multi(42, vec![(1, "CRYO:T".into()), (2, "RF:P".into())], addr());
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(SearchRequest::decode(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_req_rt_empty() {
        let o = SearchRequest::multi(0, vec![], addr());
        let mut w = le_w();
        o.encode(&mut w);
        assert!(
            SearchRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_req_rt_single() {
        let o = SearchRequest::single(99, "PV:TEST", addr());
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(SearchRequest::decode(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_req_rt_be() {
        let o = SearchRequest::single(42, "PV:A", addr());
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(SearchRequest::decode(&mut be_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_req_rt_no_reply() {
        let mut o = SearchRequest::single(1, "PV", addr());
        o.reply_required = false;
        let mut w = le_w();
        o.encode(&mut w);
        let d = SearchRequest::decode(&mut le_r(w.as_bytes())).unwrap();
        assert!(!d.reply_required);
        assert!(!d.unicast);
    }

    #[test]
    fn test_req_rt_unicast() {
        let mut o = SearchRequest::single(1, "PV", addr());
        o.unicast = true;
        let mut w = le_w();
        o.encode(&mut w);
        let d = SearchRequest::decode(&mut le_r(w.as_bytes())).unwrap();
        assert!(d.unicast);
        assert!(d.reply_required);
    }

    #[test]
    fn test_req_rt_both_flags() {
        let mut o = SearchRequest::single(1, "PV", addr());
        o.reply_required = true;
        o.unicast = true;
        let mut w = le_w();
        o.encode(&mut w);
        let d = SearchRequest::decode(&mut le_r(w.as_bytes())).unwrap();
        assert!(d.reply_required);
        assert!(d.unicast);
    }

    #[test]
    fn test_req_rt_no_flags() {
        let mut o = SearchRequest::single(1, "PV", addr());
        o.reply_required = false;
        o.unicast = false;
        let mut w = le_w();
        o.encode(&mut w);
        let d = SearchRequest::decode(&mut le_r(w.as_bytes())).unwrap();
        assert!(!d.reply_required);
        assert!(!d.unicast);
    }

    #[test]
    fn test_req_rt_long_name() {
        let name = "PERLE:Cryo:Sector2:Cavity1:Temperature:Setpoint";
        let o = SearchRequest::single(1, name, addr());
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            SearchRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .channels[0]
                .1,
            name
        );
    }

    #[test]
    fn test_req_rt_negative_ids() {
        let o = SearchRequest::multi(-1, vec![(-99, "PV".into())], addr());
        let mut w = le_w();
        o.encode(&mut w);
        let d = SearchRequest::decode(&mut le_r(w.as_bytes())).unwrap();
        assert_eq!(d.search_sequence_id, -1);
        assert_eq!(d.channels[0].0, -99);
    }

    #[test]
    fn test_req_rt_max_id() {
        let o = SearchRequest::single(i32::MAX, "PV", addr());
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            SearchRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .search_sequence_id,
            i32::MAX
        );
    }

    #[test]
    fn test_req_rt_utf8_name() {
        let o = SearchRequest::single(1, "PV:café:日本語", addr());
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            SearchRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .channels[0]
                .1,
            "PV:café:日本語"
        );
    }

    #[test]
    fn test_req_decode_empty() {
        assert!(SearchRequest::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_req_decode_truncated() {
        let mut w = le_w();
        w.write_i32(42); // only seq id
        assert!(SearchRequest::decode(&mut le_r(w.as_bytes())).is_err());
    }

    #[test]
    fn test_req_display() {
        let s = SearchRequest::single(1, "PV:A", addr()).to_string();
        assert!(s.contains("Search"));
        assert!(s.contains("1 channels"));
    }

    #[test]
    fn test_req_display_unicast() {
        let mut r = SearchRequest::single(1, "PV", addr());
        r.unicast = true;
        assert!(r.to_string().contains("unicast"));
    }

    #[test]
    fn test_req_display_no_unicast() {
        assert!(
            !SearchRequest::single(1, "PV", addr())
                .to_string()
                .contains("unicast")
        );
    }

    #[test]
    fn test_req_clone() {
        let a = SearchRequest::single(1, "PV", addr());
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_req_debug() {
        assert!(format!("{:?}", SearchRequest::single(1, "PV", addr())).contains("SearchRequest"));
    }

    #[test]
    fn test_resp_rt() {
        let o = resp(vec![1, 3, 5]);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(SearchResponse::decode(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_resp_rt_empty() {
        let o = resp(vec![]);
        let mut w = le_w();
        o.encode(&mut w);
        assert!(
            SearchResponse::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_resp_rt_be() {
        let o = SearchResponse {
            guid: [0xBB; GUID_LEN],
            search_sequence_id: 99,
            server_addr: srv(),
            protocol: "tcp".into(),
            found: vec![7],
        };
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(SearchResponse::decode(&mut be_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_resp_rt_many_found() {
        let o = resp((0..100).collect());
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            SearchResponse::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .found_count(),
            100
        );
    }

    #[test]
    fn test_resp_rt_negative_ids() {
        let o = resp(vec![-1, -99, i32::MIN]);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            SearchResponse::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .found,
            vec![-1, -99, i32::MIN]
        );
    }

    #[test]
    fn test_resp_contains_found() {
        assert!(resp(vec![1, 3, 5]).contains(1));
        assert!(resp(vec![1, 3, 5]).contains(5));
    }

    #[test]
    fn test_resp_contains_not_found() {
        assert!(!resp(vec![1, 3, 5]).contains(2));
        assert!(!resp(vec![1, 3, 5]).contains(0));
    }

    #[test]
    fn test_resp_contains_empty() {
        assert!(!resp(vec![]).contains(1));
    }

    #[test]
    fn test_resp_is_empty_true() {
        assert!(resp(vec![]).is_empty());
    }

    #[test]
    fn test_resp_is_empty_false() {
        assert!(!resp(vec![1]).is_empty());
    }

    #[test]
    fn test_resp_found_count() {
        assert_eq!(resp(vec![1, 2, 3]).found_count(), 3);
    }

    #[test]
    fn test_resp_found_count_zero() {
        assert_eq!(resp(vec![]).found_count(), 0);
    }

    #[test]
    fn test_resp_decode_empty() {
        assert!(SearchResponse::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_resp_decode_truncated_guid() {
        assert!(SearchResponse::decode(&mut le_r(&[0; 5])).is_err());
    }

    #[test]
    fn test_resp_display() {
        let s = resp(vec![1, 2]).to_string();
        assert!(s.contains("2 found"));
        assert!(s.contains("10.0.1.100"));
    }

    #[test]
    fn test_resp_clone() {
        let a = resp(vec![1]);
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_resp_debug() {
        assert!(format!("{:?}", resp(vec![])).contains("SearchResponse"));
    }
}
