//! CMD_CREATE_CHANNEL (0x07) / CMD_DESTROY_CHANNEL (0x08).

use super::status::PvaStatus;
use crate::codec::pvdata::{DecodeError, PvaReader, PvaWriter};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateChannelRequest {
    pub channels: Vec<(i32, String)>,
}

impl CreateChannelRequest {
    pub fn single(id: i32, name: impl Into<String>) -> Self {
        Self {
            channels: vec![(id, name.into())],
        }
    }
    pub fn multi(channels: Vec<(i32, String)>) -> Self {
        Self { channels }
    }

    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let count = reader.read_u16()? as usize;
        let mut channels = Vec::with_capacity(count.min(256));
        for _ in 0..count {
            channels.push((reader.read_i32()?, reader.read_string()?));
        }
        Ok(Self { channels })
    }
    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_u16(self.channels.len() as u16);
        for (id, name) in &self.channels {
            writer.write_i32(*id);
            writer.write_string(name);
        }
    }
}

impl fmt::Display for CreateChannelRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.channels.len() == 1 {
            write!(
                f,
                "CreateChannel[cid={}, {}]",
                self.channels[0].0, self.channels[0].1
            )
        } else {
            write!(f, "CreateChannel[{} channels]", self.channels.len())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateChannelResponse {
    pub client_channel_id: i32,
    pub server_channel_id: i32,
    pub status: PvaStatus,
}

impl CreateChannelResponse {
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let client_channel_id = reader.read_i32()?;
        let server_channel_id = reader.read_i32()?;
        let status = PvaStatus::decode(reader)?;
        Ok(Self {
            client_channel_id,
            server_channel_id,
            status,
        })
    }
    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_i32(self.client_channel_id);
        writer.write_i32(self.server_channel_id);
        self.status.encode(writer);
    }
}

impl fmt::Display for CreateChannelResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChannelCreated[cid={}, sid={}, {}]",
            self.client_channel_id, self.server_channel_id, self.status
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DestroyChannel {
    pub client_channel_id: i32,
    pub server_channel_id: i32,
}

impl DestroyChannel {
    pub fn new(cid: i32, sid: i32) -> Self {
        Self {
            client_channel_id: cid,
            server_channel_id: sid,
        }
    }

    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            client_channel_id: reader.read_i32()?,
            server_channel_id: reader.read_i32()?,
        })
    }
    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_i32(self.client_channel_id);
        writer.write_i32(self.server_channel_id);
    }
}

impl fmt::Display for DestroyChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DestroyChannel[cid={}, sid={}]",
            self.client_channel_id, self.server_channel_id
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

    #[test]
    fn test_req_single() {
        assert_eq!(CreateChannelRequest::single(1, "PV:A").channels.len(), 1);
    }

    #[test]
    fn test_req_multi() {
        assert_eq!(
            CreateChannelRequest::multi(vec![(1, "A".into()), (2, "B".into())])
                .channels
                .len(),
            2
        );
    }

    #[test]
    fn test_req_roundtrip() {
        let o = CreateChannelRequest::multi(vec![
            (10, "CRYO:T".into()),
            (11, "RF:P".into()),
            (12, "MAG:I".into()),
        ]);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            CreateChannelRequest::decode(&mut le_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_req_roundtrip_empty() {
        let o = CreateChannelRequest::multi(vec![]);
        let mut w = le_w();
        o.encode(&mut w);
        assert!(
            CreateChannelRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .channels
                .is_empty()
        );
    }

    #[test]
    fn test_req_roundtrip_be() {
        let o = CreateChannelRequest::single(42, "PV:BE");
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(
            CreateChannelRequest::decode(&mut be_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_req_roundtrip_negative_id() {
        let o = CreateChannelRequest::single(-1, "PV");
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            CreateChannelRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .channels[0]
                .0,
            -1
        );
    }

    #[test]
    fn test_req_decode_empty() {
        assert!(CreateChannelRequest::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_req_display_single() {
        assert!(
            CreateChannelRequest::single(1, "PV:A")
                .to_string()
                .contains("PV:A")
        );
    }

    #[test]
    fn test_req_display_multi() {
        assert!(
            CreateChannelRequest::multi(vec![(1, "A".into()), (2, "B".into())])
                .to_string()
                .contains("2 channels")
        );
    }

    #[test]
    fn test_req_clone() {
        let a = CreateChannelRequest::single(1, "PV");
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_resp_roundtrip_ok() {
        let o = CreateChannelResponse {
            client_channel_id: 10,
            server_channel_id: 500,
            status: PvaStatus::ok(),
        };
        let mut w = le_w();
        o.encode(&mut w);
        let d = CreateChannelResponse::decode(&mut le_r(w.as_bytes())).unwrap();
        assert_eq!(d, o);
        assert!(d.is_success());
    }

    #[test]
    fn test_resp_roundtrip_error() {
        let o = CreateChannelResponse {
            client_channel_id: 10,
            server_channel_id: -1,
            status: PvaStatus::error("not found"),
        };
        let mut w = le_w();
        o.encode(&mut w);
        let d = CreateChannelResponse::decode(&mut le_r(w.as_bytes())).unwrap();
        assert!(!d.is_success());
        assert_eq!(d.status.message, "not found");
    }

    #[test]
    fn test_resp_roundtrip_be() {
        let o = CreateChannelResponse {
            client_channel_id: 1,
            server_channel_id: 2,
            status: PvaStatus::ok(),
        };
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(
            CreateChannelResponse::decode(&mut be_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_resp_decode_empty() {
        assert!(CreateChannelResponse::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_resp_display() {
        let s = CreateChannelResponse {
            client_channel_id: 10,
            server_channel_id: 500,
            status: PvaStatus::ok(),
        }
        .to_string();
        assert!(s.contains("cid=10"));
        assert!(s.contains("sid=500"));
        assert!(s.contains("OK"));
    }

    #[test]
    fn test_resp_clone() {
        let a = CreateChannelResponse {
            client_channel_id: 1,
            server_channel_id: 2,
            status: PvaStatus::ok(),
        };
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_destroy_roundtrip() {
        let o = DestroyChannel::new(10, 500);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(DestroyChannel::decode(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_destroy_roundtrip_be() {
        let o = DestroyChannel::new(10, 500);
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(DestroyChannel::decode(&mut be_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_destroy_roundtrip_negative() {
        let o = DestroyChannel::new(-1, -2);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(DestroyChannel::decode(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_destroy_copy() {
        let a = DestroyChannel::new(1, 2);
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn test_destroy_decode_empty() {
        assert!(DestroyChannel::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_destroy_display() {
        let s = DestroyChannel::new(10, 500).to_string();
        assert!(s.contains("cid=10"));
        assert!(s.contains("sid=500"));
    }

    #[test]
    fn test_destroy_debug() {
        assert!(format!("{:?}", DestroyChannel::new(1, 2)).contains("DestroyChannel"));
    }
}