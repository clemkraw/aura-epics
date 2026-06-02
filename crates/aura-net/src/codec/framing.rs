//! Tokio codec for PVA TCP stream framing.

use super::header::{ByteOrder, HEADER_SIZE, MAX_PAYLOAD_SIZE, PVA_MAGIC, PvaHeader, Segmentation};
use bytes::{Buf, BytesMut};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct PvaFrame {
    pub header: PvaHeader,
    pub payload: Vec<u8>,
}

impl PvaFrame {
    pub fn new(header: PvaHeader, payload: Vec<u8>) -> Self {
        Self { header, payload }
    }
    #[inline]
    pub fn is_control(&self) -> bool {
        self.header.is_control()
    }
    #[inline]
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
    /// Total wire size (header + payload).
    #[inline]
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }
}

impl fmt::Display for PvaFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}B payload)", self.header, self.payload.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeState {
    Header,
    Payload(PvaHeader),
}

pub struct PvaCodec {
    state: DecodeState,
    byte_order: ByteOrder,
    max_payload: u32,
    segment_buf: Option<SegmentBuffer>,
    total_bytes: u64,
    total_frames: u64,
}

impl PvaCodec {
    pub fn new() -> Self {
        Self {
            state: DecodeState::Header,
            byte_order: ByteOrder::LittleEndian,
            max_payload: MAX_PAYLOAD_SIZE,
            segment_buf: None,
            total_bytes: 0,
            total_frames: 0,
        }
    }
    pub fn with_byte_order(mut self, order: ByteOrder) -> Self {
        self.byte_order = order;
        self
    }
    pub fn with_max_payload(mut self, max: u32) -> Self {
        self.max_payload = max;
        self
    }
    pub fn set_byte_order(&mut self, order: ByteOrder) {
        self.byte_order = order;
    }

    #[inline]
    pub fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }
    #[inline]
    pub fn max_payload(&self) -> u32 {
        self.max_payload
    }
    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
    #[inline]
    pub fn total_frames(&self) -> u64 {
        self.total_frames
    }
    #[inline]
    pub fn is_assembling(&self) -> bool {
        self.segment_buf.is_some()
    }

    pub fn decode_frame(&mut self, src: &mut BytesMut) -> Result<Option<PvaFrame>, CodecError> {
        loop {
            match self.state {
                DecodeState::Header => {
                    if src.len() < HEADER_SIZE {
                        return Ok(None);
                    }
                    let hdr: [u8; HEADER_SIZE] = [
                        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
                    ];
                    let header = PvaHeader::decode(&hdr).ok_or(CodecError::BadMagic(hdr[0]))?;

                    if header.is_control() {
                        src.advance(HEADER_SIZE);
                        self.total_bytes += HEADER_SIZE as u64;
                        self.total_frames += 1;
                        return Ok(Some(PvaFrame::new(header, Vec::new())));
                    }
                    if header.payload_size < 0 {
                        return Err(CodecError::NegativePayload(header.payload_size));
                    }
                    if header.payload_size as u32 > self.max_payload {
                        return Err(CodecError::PayloadTooLarge(header.payload_size as u32));
                    }
                    self.state = DecodeState::Payload(header);
                }
                DecodeState::Payload(header) => {
                    let psize = header.payload_size as usize;
                    let needed = HEADER_SIZE + psize;
                    if src.len() < needed {
                        return Ok(None);
                    }

                    src.advance(HEADER_SIZE);
                    let payload = src.split_to(psize).to_vec();
                    self.total_bytes += needed as u64;
                    self.state = DecodeState::Header;

                    match header.segmentation() {
                        Segmentation::None => {
                            self.total_frames += 1;
                            return Ok(Some(PvaFrame::new(header, payload)));
                        }
                        Segmentation::First => {
                            self.segment_buf = Some(SegmentBuffer {
                                header,
                                data: payload,
                            });
                        }
                        Segmentation::Middle => {
                            let seg = self
                                .segment_buf
                                .as_mut()
                                .ok_or(CodecError::UnexpectedSegment)?;
                            if seg.data.len() + payload.len() > self.max_payload as usize * 4 {
                                return Err(CodecError::SegmentTooLarge);
                            }
                            seg.data.extend_from_slice(&payload);
                        }
                        Segmentation::Last => {
                            let mut seg = self
                                .segment_buf
                                .take()
                                .ok_or(CodecError::UnexpectedSegment)?;
                            if seg.data.len() + payload.len() > self.max_payload as usize * 4 {
                                return Err(CodecError::SegmentTooLarge);
                            }
                            seg.data.extend_from_slice(&payload);
                            let final_h = PvaHeader {
                                payload_size: seg.data.len() as i32,
                                ..seg.header
                            }
                            .with_segmentation(Segmentation::None);
                            self.total_frames += 1;
                            return Ok(Some(PvaFrame::new(final_h, seg.data)));
                        }
                    }
                }
            }
        }
    }

    pub fn encode_frame(&self, frame: &PvaFrame, dst: &mut BytesMut) {
        dst.extend_from_slice(&frame.header.encode());
        if !frame.payload.is_empty() {
            dst.extend_from_slice(&frame.payload);
        }
    }
}

impl Default for PvaCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PvaCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PvaCodec")
            .field("byte_order", &self.byte_order)
            .field("state", &self.state)
            .field("total_frames", &self.total_frames)
            .field("total_bytes", &self.total_bytes)
            .field("assembling", &self.is_assembling())
            .finish()
    }
}

#[derive(Debug, Clone)]
struct SegmentBuffer {
    header: PvaHeader,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    BadMagic(u8),
    NegativePayload(i32),
    PayloadTooLarge(u32),
    SegmentTooLarge,
    UnexpectedSegment,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic(m) => write!(f, "bad PVA magic: 0x{m:02X}"),
            Self::NegativePayload(s) => write!(f, "negative payload: {s}"),
            Self::PayloadTooLarge(s) => write!(f, "payload too large: {s} bytes"),
            Self::SegmentTooLarge => write!(f, "reassembled segment exceeds limit"),
            Self::UnexpectedSegment => write!(f, "middle/last segment without first"),
        }
    }
}

impl std::error::Error for CodecError {}

#[cfg(test)]
mod tests {
    use super::super::commands::*;
    use super::*;

    fn mk(header: PvaHeader, payload: &[u8]) -> BytesMut {
        let mut b = BytesMut::new();
        b.extend_from_slice(&header.encode());
        b.extend_from_slice(payload);
        b
    }

    #[test]
    fn test_frame_app() {
        let f = PvaFrame::new(PvaHeader::app(CMD_MONITOR, 5), vec![1, 2, 3, 4, 5]);
        assert!(!f.is_control());
        assert_eq!(f.payload_len(), 5);
    }
    #[test]
    fn test_frame_ctrl() {
        let f = PvaFrame::new(PvaHeader::ctrl(CTRL_SET_BYTE_ORDER, 0), vec![]);
        assert!(f.is_control());
        assert_eq!(f.payload_len(), 0);
    }
    #[test]
    fn test_frame_wire_size() {
        assert_eq!(
            PvaFrame::new(PvaHeader::app(CMD_GET, 10), vec![0; 10]).wire_size(),
            18
        );
    }
    #[test]
    fn test_frame_wire_size_ctrl() {
        assert_eq!(
            PvaFrame::new(PvaHeader::ctrl(CTRL_ECHO_REQUEST, 0), vec![]).wire_size(),
            8
        );
    }
    #[test]
    fn test_frame_display() {
        assert!(
            PvaFrame::new(PvaHeader::app(CMD_SEARCH, 10), vec![0; 10])
                .to_string()
                .contains("10B payload")
        );
    }
    #[test]
    fn test_frame_clone() {
        let a = PvaFrame::new(PvaHeader::app(CMD_GET, 3), vec![1, 2, 3]);
        let b = a.clone();
        assert_eq!(a.payload, b.payload);
    }
    #[test]
    fn test_frame_debug() {
        assert!(
            format!("{:?}", PvaFrame::new(PvaHeader::app(CMD_GET, 0), vec![])).contains("PvaFrame")
        );
    }

    #[test]
    fn test_decode_app() {
        let mut c = PvaCodec::new();
        let mut buf = mk(PvaHeader::server(CMD_BEACON, 3), &[1, 2, 3]);
        let f = c.decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.header.command, CMD_BEACON);
        assert_eq!(f.payload, vec![1, 2, 3]);
        assert!(buf.is_empty());
    }
    #[test]
    fn test_decode_ctrl() {
        let mut c = PvaCodec::new();
        let mut buf = mk(PvaHeader::ctrl(CTRL_SET_BYTE_ORDER, 0), &[]);
        let f = c.decode_frame(&mut buf).unwrap().unwrap();
        assert!(f.is_control());
        assert_eq!(f.header.command, CTRL_SET_BYTE_ORDER);
    }
    #[test]
    fn test_decode_zero_payload() {
        let mut c = PvaCodec::new();
        let mut buf = mk(PvaHeader::app(CMD_ECHO, 0), &[]);
        assert_eq!(c.decode_frame(&mut buf).unwrap().unwrap().payload_len(), 0);
    }
    #[test]
    fn test_decode_large_payload() {
        let mut c = PvaCodec::new();
        let data = vec![0x42; 10_000];
        let mut buf = mk(PvaHeader::app(CMD_MONITOR, data.len() as i32), &data);
        let f = c.decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.payload.len(), 10_000);
    }

    #[test]
    fn test_partial_empty() {
        assert!(
            PvaCodec::new()
                .decode_frame(&mut BytesMut::new())
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn test_partial_header() {
        let mut c = PvaCodec::new();
        let mut buf = BytesMut::from(&[PVA_MAGIC, 0x02, 0x00][..]);
        assert!(c.decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 3); // nothing consumed
    }
    #[test]
    fn test_partial_payload() {
        let mut c = PvaCodec::new();
        let h = PvaHeader::app(CMD_SEARCH, 10);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&h.encode());
        buf.extend_from_slice(&[0; 5]);
        assert!(c.decode_frame(&mut buf).unwrap().is_none());
    }
    #[test]
    fn test_incremental_byte_by_byte() {
        let mut c = PvaCodec::new();
        let payload = vec![0xAA; 4];
        let h = PvaHeader::app(CMD_GET, 4);
        let encoded = h.encode();
        let mut buf = BytesMut::new();
        for &b in &encoded {
            buf.extend_from_slice(&[b]);
            assert!(c.decode_frame(&mut buf).unwrap().is_none());
        }
        for &b in &payload[..3] {
            buf.extend_from_slice(&[b]);
            assert!(c.decode_frame(&mut buf).unwrap().is_none());
        }
        buf.extend_from_slice(&[payload[3]]);
        assert_eq!(c.decode_frame(&mut buf).unwrap().unwrap().payload, payload);
    }

    #[test]
    fn test_two_app_frames() {
        let mut c = PvaCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&mk(PvaHeader::app(CMD_SEARCH, 2), &[1, 2]));
        buf.extend_from_slice(&mk(PvaHeader::app(CMD_GET, 3), &[3, 4, 5]));
        assert_eq!(
            c.decode_frame(&mut buf).unwrap().unwrap().header.command,
            CMD_SEARCH
        );
        assert_eq!(
            c.decode_frame(&mut buf).unwrap().unwrap().header.command,
            CMD_GET
        );
        assert!(buf.is_empty());
    }
    #[test]
    fn test_ctrl_then_app() {
        let mut c = PvaCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&mk(PvaHeader::ctrl(CTRL_SET_BYTE_ORDER, 0), &[]));
        buf.extend_from_slice(&mk(PvaHeader::app(CMD_MONITOR, 1), &[0xFF]));
        assert!(c.decode_frame(&mut buf).unwrap().unwrap().is_control());
        assert!(!c.decode_frame(&mut buf).unwrap().unwrap().is_control());
    }
    #[test]
    fn test_multiple_ctrl() {
        let mut c = PvaCodec::new();
        let mut buf = BytesMut::new();
        for _ in 0..5 {
            buf.extend_from_slice(&mk(PvaHeader::ctrl(CTRL_ECHO_REQUEST, 0), &[]));
        }
        for _ in 0..5 {
            assert!(c.decode_frame(&mut buf).unwrap().unwrap().is_control());
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn test_err_bad_magic() {
        let mut buf = BytesMut::from(&[0xFF, 0x02, 0x00, 0x03, 0, 0, 0, 0][..]);
        assert_eq!(
            PvaCodec::new().decode_frame(&mut buf),
            Err(CodecError::BadMagic(0xFF))
        );
    }
    #[test]
    fn test_err_negative_payload() {
        let mut buf = BytesMut::from(&[PVA_MAGIC, 0x02, 0x00, 0x03, 0xFF, 0xFF, 0xFF, 0xFF][..]);
        assert_eq!(
            PvaCodec::new().decode_frame(&mut buf),
            Err(CodecError::NegativePayload(-1))
        );
    }
    #[test]
    fn test_err_payload_too_large() {
        let mut c = PvaCodec::new().with_max_payload(100);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&PvaHeader::app(CMD_GET, 200).encode());
        assert_eq!(
            c.decode_frame(&mut buf),
            Err(CodecError::PayloadTooLarge(200))
        );
    }
    #[test]
    fn test_err_unexpected_middle() {
        let mut buf = mk(
            PvaHeader::app(CMD_MONITOR, 5).with_segmentation(Segmentation::Middle),
            &[0; 5],
        );
        assert_eq!(
            PvaCodec::new().decode_frame(&mut buf),
            Err(CodecError::UnexpectedSegment)
        );
    }
    #[test]
    fn test_err_unexpected_last() {
        let mut buf = mk(
            PvaHeader::app(CMD_MONITOR, 5).with_segmentation(Segmentation::Last),
            &[0; 5],
        );
        assert_eq!(
            PvaCodec::new().decode_frame(&mut buf),
            Err(CodecError::UnexpectedSegment)
        );
    }

    #[test]
    fn test_seg_two_parts() {
        let mut c = PvaCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&mk(
            PvaHeader::app(CMD_MONITOR, 3).with_segmentation(Segmentation::First),
            &[1, 2, 3],
        ));
        buf.extend_from_slice(&mk(
            PvaHeader::app(CMD_MONITOR, 2).with_segmentation(Segmentation::Last),
            &[4, 5],
        ));
        let f = c.decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.payload, vec![1, 2, 3, 4, 5]);
        assert_eq!(f.header.payload_size, 5);
        assert!(!f.header.is_segmented());
    }
    #[test]
    fn test_seg_three_parts() {
        let mut c = PvaCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&mk(
            PvaHeader::app(CMD_MONITOR, 2).with_segmentation(Segmentation::First),
            &[0xAA, 0xBB],
        ));
        buf.extend_from_slice(&mk(
            PvaHeader::app(CMD_MONITOR, 2).with_segmentation(Segmentation::Middle),
            &[0xCC, 0xDD],
        ));
        buf.extend_from_slice(&mk(
            PvaHeader::app(CMD_MONITOR, 1).with_segmentation(Segmentation::Last),
            &[0xEE],
        ));
        assert_eq!(
            c.decode_frame(&mut buf).unwrap().unwrap().payload,
            vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE]
        );
    }
    #[test]
    fn test_seg_is_assembling() {
        let mut c = PvaCodec::new();
        assert!(!c.is_assembling());
        let mut buf = mk(
            PvaHeader::app(CMD_MONITOR, 2).with_segmentation(Segmentation::First),
            &[1, 2],
        );
        assert!(c.decode_frame(&mut buf).unwrap().is_none()); // buffering first
        assert!(c.is_assembling());
    }
    #[test]
    fn test_seg_preserves_command() {
        let mut c = PvaCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&mk(
            PvaHeader::app(CMD_MONITOR, 1).with_segmentation(Segmentation::First),
            &[1],
        ));
        buf.extend_from_slice(&mk(
            PvaHeader::app(CMD_MONITOR, 1).with_segmentation(Segmentation::Last),
            &[2],
        ));
        assert_eq!(
            c.decode_frame(&mut buf).unwrap().unwrap().header.command,
            CMD_MONITOR
        );
    }

    #[test]
    fn test_encode_app() {
        let f = PvaFrame::new(PvaHeader::app(CMD_SEARCH, 3), vec![1, 2, 3]);
        let mut dst = BytesMut::new();
        PvaCodec::new().encode_frame(&f, &mut dst);
        assert_eq!(dst.len(), HEADER_SIZE + 3);
        assert_eq!(dst[0], PVA_MAGIC);
        assert_eq!(dst[3], CMD_SEARCH);
    }
    #[test]
    fn test_encode_ctrl() {
        let f = PvaFrame::new(PvaHeader::ctrl(CTRL_ECHO_REQUEST, 0), vec![]);
        let mut dst = BytesMut::new();
        PvaCodec::new().encode_frame(&f, &mut dst);
        assert_eq!(dst.len(), HEADER_SIZE);
    }
    #[test]
    fn test_encode_empty_payload() {
        let f = PvaFrame::new(PvaHeader::app(CMD_ECHO, 0), vec![]);
        let mut dst = BytesMut::new();
        PvaCodec::new().encode_frame(&f, &mut dst);
        assert_eq!(dst.len(), HEADER_SIZE);
    }

    #[test]
    fn test_rt_app() {
        let orig = PvaFrame::new(PvaHeader::app(CMD_MONITOR, 100), vec![0x42; 100]);
        let mut wire = BytesMut::new();
        PvaCodec::new().encode_frame(&orig, &mut wire);
        let dec = PvaCodec::new().decode_frame(&mut wire).unwrap().unwrap();
        assert_eq!(dec.header.command, CMD_MONITOR);
        assert_eq!(dec.payload, orig.payload);
    }
    #[test]
    fn test_rt_ctrl() {
        let orig = PvaFrame::new(PvaHeader::ctrl(CTRL_ECHO_RESPONSE, 42), vec![]);
        let mut wire = BytesMut::new();
        PvaCodec::new().encode_frame(&orig, &mut wire);
        let dec = PvaCodec::new().decode_frame(&mut wire).unwrap().unwrap();
        assert!(dec.is_control());
        assert_eq!(dec.header.command, CTRL_ECHO_RESPONSE);
    }
    #[test]
    fn test_rt_be() {
        let orig = PvaFrame::new(
            PvaHeader::app(CMD_SEARCH, 4).with_byte_order(ByteOrder::BigEndian),
            vec![1, 2, 3, 4],
        );
        let mut wire = BytesMut::new();
        PvaCodec::new().encode_frame(&orig, &mut wire);
        let dec = PvaCodec::new().decode_frame(&mut wire).unwrap().unwrap();
        assert_eq!(dec.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_default() {
        let c = PvaCodec::default();
        assert_eq!(c.byte_order(), ByteOrder::LittleEndian);
        assert_eq!(c.max_payload(), MAX_PAYLOAD_SIZE);
    }
    #[test]
    fn test_with_byte_order() {
        assert_eq!(
            PvaCodec::new()
                .with_byte_order(ByteOrder::BigEndian)
                .byte_order(),
            ByteOrder::BigEndian
        );
    }
    #[test]
    fn test_with_max_payload() {
        assert_eq!(PvaCodec::new().with_max_payload(1024).max_payload(), 1024);
    }
    #[test]
    fn test_set_byte_order() {
        let mut c = PvaCodec::new();
        c.set_byte_order(ByteOrder::BigEndian);
        assert_eq!(c.byte_order(), ByteOrder::BigEndian);
    }
    #[test]
    fn test_metrics_initial() {
        let c = PvaCodec::new();
        assert_eq!(c.total_frames(), 0);
        assert_eq!(c.total_bytes(), 0);
    }
    #[test]
    fn test_metrics_one_frame() {
        let mut c = PvaCodec::new();
        let mut buf = mk(PvaHeader::app(CMD_ECHO, 4), &[0; 4]);
        c.decode_frame(&mut buf).unwrap();
        assert_eq!(c.total_frames(), 1);
        assert_eq!(c.total_bytes(), 12);
    }
    #[test]
    fn test_metrics_multi() {
        let mut c = PvaCodec::new();
        for _ in 0..10 {
            let mut buf = mk(PvaHeader::ctrl(CTRL_ECHO_REQUEST, 0), &[]);
            c.decode_frame(&mut buf).unwrap();
        }
        assert_eq!(c.total_frames(), 10);
        assert_eq!(c.total_bytes(), 80);
    }
    #[test]
    fn test_metrics_ctrl() {
        let mut c = PvaCodec::new();
        let mut buf = mk(PvaHeader::ctrl(CTRL_SET_BYTE_ORDER, 0), &[]);
        c.decode_frame(&mut buf).unwrap();
        assert_eq!(c.total_frames(), 1);
        assert_eq!(c.total_bytes(), 8);
    }

    #[test]
    fn test_err_display_bad_magic() {
        assert!(CodecError::BadMagic(0xFF).to_string().contains("0xFF"));
    }
    #[test]
    fn test_err_display_negative() {
        assert!(CodecError::NegativePayload(-1).to_string().contains("-1"));
    }
    #[test]
    fn test_err_display_too_large() {
        assert!(CodecError::PayloadTooLarge(999).to_string().contains("999"));
    }
    #[test]
    fn test_err_display_seg_large() {
        assert!(CodecError::SegmentTooLarge.to_string().contains("segment"));
    }
    #[test]
    fn test_err_display_unexpected() {
        assert!(
            CodecError::UnexpectedSegment
                .to_string()
                .contains("without first")
        );
    }
    #[test]
    fn test_err_is_error() {
        let e: Box<dyn std::error::Error> = Box::new(CodecError::BadMagic(0));
        assert!(!e.to_string().is_empty());
    }
    #[test]
    fn test_err_eq() {
        assert_eq!(CodecError::BadMagic(1), CodecError::BadMagic(1));
        assert_ne!(CodecError::BadMagic(1), CodecError::BadMagic(2));
    }
    #[test]
    fn test_err_clone() {
        let a = CodecError::SegmentTooLarge;
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn test_debug() {
        let d = format!("{:?}", PvaCodec::new());
        assert!(d.contains("PvaCodec"));
        assert!(d.contains("byte_order"));
        assert!(d.contains("assembling"));
    }
}
