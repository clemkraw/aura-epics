//! CMD_MONITOR (0x0D) — subscribe to PV value changes (hot path).

use crate::codec::commands;
use crate::codec::pvdata::{DecodeError, PvaReader, PvaWriter};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorSubCommand {
    Init,
    Start,
    StartPipeline,
    Stop,
    Destroy,
    Pipeline(i32),
}

impl MonitorSubCommand {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            commands::MONITOR_INIT => Some(Self::Init),
            commands::MONITOR_START => Some(Self::Start),
            0x44 => Some(Self::StartPipeline),
            commands::MONITOR_STOP => Some(Self::Stop),
            commands::MONITOR_DESTROY => Some(Self::Destroy),
            v if v & commands::MONITOR_PIPELINE != 0 => Some(Self::Pipeline(0)),
            _ => None,
        }
    }
    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Init => commands::MONITOR_INIT,
            Self::Start => commands::MONITOR_START,
            Self::StartPipeline => 0x44,
            Self::Stop => commands::MONITOR_STOP,
            Self::Destroy => commands::MONITOR_DESTROY,
            Self::Pipeline(_) => commands::MONITOR_PIPELINE,
        }
    }
    pub fn is_init(&self) -> bool {
        matches!(self, Self::Init)
    }
    pub fn is_pipeline(&self) -> bool {
        matches!(self, Self::Pipeline(_))
    }
}

impl fmt::Display for MonitorSubCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "INIT"),
            Self::Start => write!(f, "START"),
            Self::StartPipeline => write!(f, "START+PIPELINE"),
            Self::Stop => write!(f, "STOP"),
            Self::Destroy => write!(f, "DESTROY"),
            Self::Pipeline(n) => write!(f, "PIPELINE({n})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorRequest {
    pub server_channel_id: i32,
    pub request_id: i32,
    pub sub_command: MonitorSubCommand,
}

impl MonitorRequest {
    pub fn init(sid: i32, rid: i32) -> Self {
        Self {
            server_channel_id: sid,
            request_id: rid,
            sub_command: MonitorSubCommand::Init,
        }
    }
    pub fn start(sid: i32, rid: i32) -> Self {
        Self {
            server_channel_id: sid,
            request_id: rid,
            sub_command: MonitorSubCommand::Start,
        }
    }
    pub fn start_pipeline(sid: i32, rid: i32) -> Self {
        Self {
            server_channel_id: sid,
            request_id: rid,
            sub_command: MonitorSubCommand::StartPipeline,
        }
    }
    pub fn stop(sid: i32, rid: i32) -> Self {
        Self {
            server_channel_id: sid,
            request_id: rid,
            sub_command: MonitorSubCommand::Stop,
        }
    }
    pub fn destroy(sid: i32, rid: i32) -> Self {
        Self {
            server_channel_id: sid,
            request_id: rid,
            sub_command: MonitorSubCommand::Destroy,
        }
    }
    pub fn pipeline(sid: i32, rid: i32, credits: i32) -> Self {
        Self {
            server_channel_id: sid,
            request_id: rid,
            sub_command: MonitorSubCommand::Pipeline(credits),
        }
    }

    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let server_channel_id = reader.read_i32()?;
        let request_id = reader.read_i32()?;
        let sub_byte = reader.read_u8()?;
        let mut sub = MonitorSubCommand::from_u8(sub_byte).ok_or(DecodeError::Protocol(
            format!("unknown monitor sub: 0x{sub_byte:02X}"),
        ))?;
        if let MonitorSubCommand::Pipeline(_) = sub {
            sub = MonitorSubCommand::Pipeline(reader.read_i32()?);
        }
        Ok(Self {
            server_channel_id,
            request_id,
            sub_command: sub,
        })
    }

    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_i32(self.server_channel_id);
        writer.write_i32(self.request_id);
        writer.write_u8(self.sub_command.to_u8());
        match self.sub_command {
            MonitorSubCommand::Init => {
                // pvRequest: empty structure meaning "monitor all fields".
                // Bytes copied from pvget wire capture (PVXS reference client).
                // FD = structure tag, 02 00 = type ID with empty name,
                // 80 00 00 = structure with 0 fields (cached descriptor).
                writer.write_raw(&[0xFD, 0x02, 0x00, 0x80, 0x00, 0x00]);
            }
            MonitorSubCommand::Pipeline(credits) => {
                writer.write_i32(credits);
            }
            _ => {}
        }
    }
}

impl fmt::Display for MonitorRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MonitorReq[sid={}, rid={}, {}]",
            self.server_channel_id, self.request_id, self.sub_command
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorResponseHeader {
    pub request_id: i32,
    pub sub_command: u8,
}

impl MonitorResponseHeader {
    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            request_id: reader.read_i32()?,
            sub_command: reader.read_u8()?,
        })
    }
    pub fn encode(&self, writer: &mut PvaWriter) {
        writer.write_i32(self.request_id);
        writer.write_u8(self.sub_command);
    }
    pub fn is_init_response(&self) -> bool {
        self.sub_command & commands::MONITOR_INIT != 0
    }
}

impl fmt::Display for MonitorResponseHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MonitorResp[rid={}, sub=0x{:02X}{}]",
            self.request_id,
            self.sub_command,
            if self.is_init_response() { " INIT" } else { "" }
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
    fn test_sub_from_u8_all() {
        assert_eq!(
            MonitorSubCommand::from_u8(0x08),
            Some(MonitorSubCommand::Init)
        );
        assert_eq!(
            MonitorSubCommand::from_u8(0x04),
            Some(MonitorSubCommand::Start)
        );
        assert_eq!(
            MonitorSubCommand::from_u8(0x02),
            Some(MonitorSubCommand::Stop)
        );
        assert_eq!(
            MonitorSubCommand::from_u8(0x10),
            Some(MonitorSubCommand::Destroy)
        );
        assert!(matches!(
            MonitorSubCommand::from_u8(0x80),
            Some(MonitorSubCommand::Pipeline(_))
        ));
    }

    #[test]
    fn test_sub_from_u8_invalid() {
        assert_eq!(MonitorSubCommand::from_u8(0x00), None);
        assert_eq!(MonitorSubCommand::from_u8(0x01), None);
    }

    #[test]
    fn test_sub_to_u8_roundtrip() {
        for sub in [
            MonitorSubCommand::Init,
            MonitorSubCommand::Start,
            MonitorSubCommand::Stop,
            MonitorSubCommand::Destroy,
        ] {
            assert_eq!(MonitorSubCommand::from_u8(sub.to_u8()), Some(sub));
        }
    }

    #[test]
    fn test_sub_is_init() {
        assert!(MonitorSubCommand::Init.is_init());
        assert!(!MonitorSubCommand::Start.is_init());
    }

    #[test]
    fn test_sub_is_pipeline() {
        assert!(MonitorSubCommand::Pipeline(10).is_pipeline());
        assert!(!MonitorSubCommand::Init.is_pipeline());
    }
    #[test]
    fn test_sub_display() {
        assert_eq!(MonitorSubCommand::Init.to_string(), "INIT");
        assert_eq!(MonitorSubCommand::Pipeline(5).to_string(), "PIPELINE(5)");
        assert_eq!(MonitorSubCommand::Stop.to_string(), "STOP");
    }

    #[test]
    fn test_req_init() {
        let r = MonitorRequest::init(100, 1);
        assert_eq!(r.sub_command, MonitorSubCommand::Init);
    }

    #[test]
    fn test_req_all_constructors() {
        assert_eq!(
            MonitorRequest::start(1, 2).sub_command,
            MonitorSubCommand::Start
        );
        assert_eq!(
            MonitorRequest::stop(1, 2).sub_command,
            MonitorSubCommand::Stop
        );
        assert_eq!(
            MonitorRequest::destroy(1, 2).sub_command,
            MonitorSubCommand::Destroy
        );
        assert_eq!(
            MonitorRequest::pipeline(1, 2, 50).sub_command,
            MonitorSubCommand::Pipeline(50)
        );
    }

    #[test]
    fn test_req_roundtrip_start() {
        let o = MonitorRequest::start(500, 42);
        let mut w = le_w();
        o.encode(&mut w);
        let d = MonitorRequest::decode(&mut le_r(w.as_bytes())).unwrap();
        assert_eq!(d.server_channel_id, 500);
        assert_eq!(d.request_id, 42);
        assert_eq!(d.sub_command, MonitorSubCommand::Start);
    }

    #[test]
    fn test_req_roundtrip_stop() {
        let o = MonitorRequest::stop(100, 7);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(MonitorRequest::decode(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_req_roundtrip_destroy() {
        let o = MonitorRequest::destroy(100, 7);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(MonitorRequest::decode(&mut le_r(w.as_bytes())).unwrap(), o);
    }

    #[test]
    fn test_req_roundtrip_pipeline() {
        let o = MonitorRequest::pipeline(500, 42, 100);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            MonitorRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .sub_command,
            MonitorSubCommand::Pipeline(100)
        );
    }

    #[test]
    fn test_req_roundtrip_pipeline_zero() {
        let o = MonitorRequest::pipeline(1, 1, 0);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            MonitorRequest::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .sub_command,
            MonitorSubCommand::Pipeline(0)
        );
    }

    #[test]
    fn test_req_roundtrip_be() {
        let o = MonitorRequest::init(999, 77);
        let mut w = be_w();
        o.encode(&mut w);
        let d = MonitorRequest::decode(&mut be_r(w.as_bytes())).unwrap();
        assert_eq!(d.server_channel_id, 999);
    }

    #[test]
    fn test_req_roundtrip_negative_ids() {
        let o = MonitorRequest::start(-1, -99);
        let mut w = le_w();
        o.encode(&mut w);
        let d = MonitorRequest::decode(&mut le_r(w.as_bytes())).unwrap();
        assert_eq!(d.server_channel_id, -1);
        assert_eq!(d.request_id, -99);
    }

    #[test]
    fn test_req_decode_empty() {
        assert!(MonitorRequest::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_req_decode_truncated() {
        // Only server_channel_id, missing rest.
        let mut w = le_w();
        w.write_i32(100);
        assert!(MonitorRequest::decode(&mut le_r(w.as_bytes())).is_err());
    }

    #[test]
    fn test_req_decode_bad_sub() {
        let mut w = le_w();
        w.write_i32(100);
        w.write_i32(1);
        w.write_u8(0x00); // sub=0 invalid
        assert!(MonitorRequest::decode(&mut le_r(w.as_bytes())).is_err());
    }

    #[test]
    fn test_req_display() {
        let s = MonitorRequest::init(100, 1).to_string();
        assert!(s.contains("sid=100"));
        assert!(s.contains("rid=1"));
        assert!(s.contains("INIT"));
    }

    #[test]
    fn test_req_clone() {
        let a = MonitorRequest::init(1, 2);
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_req_debug() {
        assert!(format!("{:?}", MonitorRequest::init(1, 2)).contains("MonitorRequest"));
    }

    #[test]
    fn test_resp_roundtrip() {
        let o = MonitorResponseHeader {
            request_id: 42,
            sub_command: 0x08,
        };
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            MonitorResponseHeader::decode(&mut le_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_resp_roundtrip_be() {
        let o = MonitorResponseHeader {
            request_id: 99,
            sub_command: 0x04,
        };
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(
            MonitorResponseHeader::decode(&mut be_r(w.as_bytes())).unwrap(),
            o
        );
    }

    #[test]
    fn test_resp_is_init() {
        assert!(
            MonitorResponseHeader {
                request_id: 1,
                sub_command: 0x08
            }
            .is_init_response()
        );
        assert!(
            !MonitorResponseHeader {
                request_id: 1,
                sub_command: 0x00
            }
            .is_init_response()
        );
        assert!(
            !MonitorResponseHeader {
                request_id: 1,
                sub_command: 0x04
            }
            .is_init_response()
        );
    }

    #[test]
    fn test_resp_decode_empty() {
        assert!(MonitorResponseHeader::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_resp_display_init() {
        let s = MonitorResponseHeader {
            request_id: 42,
            sub_command: 0x08,
        }
        .to_string();
        assert!(s.contains("rid=42"));
        assert!(s.contains("INIT"));
    }

    #[test]
    fn test_resp_display_update() {
        let s = MonitorResponseHeader {
            request_id: 42,
            sub_command: 0x00,
        }
        .to_string();
        assert!(!s.contains("INIT")); // update, not init
    }

    #[test]
    fn test_resp_clone() {
        let a = MonitorResponseHeader {
            request_id: 1,
            sub_command: 0x08,
        };
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_resp_debug() {
        assert!(
            format!(
                "{:?}",
                MonitorResponseHeader {
                    request_id: 1,
                    sub_command: 0
                }
            )
            .contains("MonitorResponseHeader")
        );
    }
}
