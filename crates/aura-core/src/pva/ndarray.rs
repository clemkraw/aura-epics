//! NDArray sub-structures: codec, dimension, and attributes.
//!
//! These types compose the `epics:nt/NTNDArray:1.0` Normative Type
//! used by EPICS area detectors and cameras.
//!
//! - [`Codec`] — compression format descriptor (JPEG, Blosc, LZ4, etc.)
//! - [`Dimension`] — one axis of an N-dimensional image
//! - [`NdAttribute`] — key-value metadata attached to each frame
//!
//! Reference: EPICS areaDetector NDArray documentation
//! <https://areadetector.github.io/areaDetector/areaDetectorDoxygenHTML/class_n_d_array.html>

use serde::{Deserialize, Serialize};
use std::fmt;

use super::scalars::ScalarValue;

/// Compression codec descriptor for NDArray pixel data.
///
/// When `name` is empty, the data is uncompressed.
/// Common codecs: "jpeg", "blosc", "lz4", "bslz4".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Codec {
    /// Codec name. Empty string = uncompressed raw data.
    #[serde(default)]
    pub name: String,
    /// Codec-specific parameters (e.g., quality level, block size).
    #[serde(default)]
    pub parameters: serde_json::Value,
}

impl Codec {
    /// Create a codec descriptor.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            parameters: serde_json::Value::Null,
        }
    }

    /// Create a codec with parameters.
    pub fn with_params(name: impl Into<String>, parameters: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            parameters,
        }
    }

    /// Whether the data is uncompressed (no codec).
    #[inline]
    pub fn is_uncompressed(&self) -> bool {
        self.name.is_empty()
    }

    /// Whether the codec has parameters.
    pub fn has_params(&self) -> bool {
        !self.parameters.is_null()
    }
}

impl Default for Codec {
    fn default() -> Self {
        Self {
            name: String::new(),
            parameters: serde_json::Value::Null,
        }
    }
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_uncompressed() {
            f.write_str("uncompressed")
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// One axis of an N-dimensional array.
///
/// For a 1024×768 camera image:
/// ```text
/// dimensions = [
///     Dimension { size: 1024, full_size: 1024, ... },  // X (width)
///     Dimension { size: 768,  full_size: 768,  ... },  // Y (height)
/// ]
/// ```
///
/// ROI (Region of Interest) cropping is represented by `offset` and
/// `full_size`: the ROI starts at `offset` within the full detector
/// of `full_size` pixels, and `size` is the ROI width.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dimension {
    /// Number of elements along this axis (ROI size).
    pub size: i32,
    /// Offset into the full dimension (ROI start position).
    #[serde(default)]
    pub offset: i32,
    /// Full size of the detector along this axis.
    #[serde(default)]
    pub full_size: i32,
    /// Binning factor (1 = no binning, 2 = 2×2, etc.).
    #[serde(default = "default_one")]
    pub binning: i32,
    /// Whether this dimension is reversed (mirrored).
    #[serde(default)]
    pub reverse: bool,
}

fn default_one() -> i32 {
    1
}

impl Dimension {
    /// Create a simple dimension (no ROI, no binning).
    pub fn new(size: i32) -> Self {
        Self {
            size,
            offset: 0,
            full_size: size,
            binning: 1,
            reverse: false,
        }
    }

    /// Create a dimension with ROI.
    pub fn with_roi(size: i32, offset: i32, full_size: i32) -> Self {
        Self {
            size,
            offset,
            full_size,
            binning: 1,
            reverse: false,
        }
    }

    /// Whether this dimension has a ROI (subset of the full detector).
    pub fn is_roi(&self) -> bool {
        self.size != self.full_size || self.offset != 0
    }

    /// Whether binning is applied.
    pub fn is_binned(&self) -> bool {
        self.binning > 1
    }

    /// Effective pixels on the detector covered by this dimension.
    /// `size * binning` gives the detector pixels represented.
    pub fn detector_extent(&self) -> i32 {
        self.size * self.binning
    }
}

impl fmt::Display for Dimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_roi() {
            write!(f, "{}@{}(/{}", self.size, self.offset, self.full_size)?;
            if self.is_binned() {
                write!(f, " bin={}", self.binning)?;
            }
            write!(f, ")")
        } else if self.is_binned() {
            write!(f, "{} bin={}", self.size, self.binning)
        } else {
            write!(f, "{}", self.size)
        }
    }
}

/// Source type for an NDArray attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum AttributeSourceType {
    /// Value comes from the detector driver.
    #[default]
    Driver = 0,
    /// Value comes from an asyn parameter.
    Param = 1,
    /// Value comes from an EPICS PV (via CA/PVA link).
    EpicsPv = 2,
}

impl From<i32> for AttributeSourceType {
    fn from(v: i32) -> Self {
        match v {
            0 => Self::Driver,
            1 => Self::Param,
            2 => Self::EpicsPv,
            _ => Self::Driver, // default to driver
        }
    }
}

impl From<AttributeSourceType> for i32 {
    fn from(v: AttributeSourceType) -> Self {
        v as i32
    }
}

impl fmt::Display for AttributeSourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Driver => "driver",
            Self::Param => "param",
            Self::EpicsPv => "epics_pv",
        })
    }
}

/// Key-value metadata attached to an NDArray frame.
///
/// Each frame can carry arbitrary attributes — exposure time, gain,
/// temperature, beam position, etc. These are stored alongside the
/// image data in `pv_metadata` or in the `samples_image` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NdAttribute {
    /// Attribute name (e.g., "ExposureTime", "Gain", "BeamX").
    pub name: String,
    /// Attribute value.
    pub value: ScalarValue,
    /// Source name (e.g., driver name, PV name).
    #[serde(default)]
    pub source: String,
    /// Source type.
    #[serde(default)]
    pub source_type: AttributeSourceType,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}

impl NdAttribute {
    /// Create a simple attribute.
    pub fn new(name: impl Into<String>, value: ScalarValue) -> Self {
        Self {
            name: name.into(),
            value,
            source: String::new(),
            source_type: AttributeSourceType::Driver,
            description: String::new(),
        }
    }

    /// Create an attribute with source info.
    pub fn with_source(
        name: impl Into<String>,
        value: ScalarValue,
        source: impl Into<String>,
        source_type: AttributeSourceType,
    ) -> Self {
        Self {
            name: name.into(),
            value,
            source: source.into(),
            source_type,
            description: String::new(),
        }
    }

    /// Try to get the value as f64.
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        self.value.as_f64()
    }
}

impl fmt::Display for NdAttribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.name, self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_default() {
        let c = Codec::default();
        assert!(c.is_uncompressed());
        assert!(!c.has_params());
        assert_eq!(c.to_string(), "uncompressed");
    }

    #[test]
    fn test_codec_new() {
        let c = Codec::new("jpeg");
        assert!(!c.is_uncompressed());
        assert!(!c.has_params());
        assert_eq!(c.to_string(), "jpeg");
    }

    #[test]
    fn test_codec_with_params() {
        let c = Codec::with_params("blosc", serde_json::json!({"clevel": 5}));
        assert!(!c.is_uncompressed());
        assert!(c.has_params());
        assert_eq!(c.name, "blosc");
    }

    #[test]
    fn test_codec_clone_eq() {
        let a = Codec::new("lz4");
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_codec_ne() {
        assert_ne!(Codec::new("jpeg"), Codec::new("blosc"));
        assert_ne!(Codec::default(), Codec::new("lz4"));
    }

    #[test]
    fn test_codec_serde_roundtrip() {
        let c = Codec::with_params("bslz4", serde_json::json!({"block_size": 4096}));
        let json = serde_json::to_string(&c).unwrap();
        let back: Codec = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn test_codec_serde_defaults() {
        let c: Codec = serde_json::from_str("{}").unwrap();
        assert_eq!(c, Codec::default());
    }

    #[test]
    fn test_dimension_new() {
        let d = Dimension::new(1024);
        assert_eq!(d.size, 1024);
        assert_eq!(d.offset, 0);
        assert_eq!(d.full_size, 1024);
        assert_eq!(d.binning, 1);
        assert!(!d.reverse);
        assert!(!d.is_roi());
        assert!(!d.is_binned());
    }

    #[test]
    fn test_dimension_with_roi() {
        let d = Dimension::with_roi(512, 100, 1024);
        assert_eq!(d.size, 512);
        assert_eq!(d.offset, 100);
        assert_eq!(d.full_size, 1024);
        assert!(d.is_roi());
        assert!(!d.is_binned());
    }

    #[test]
    fn test_dimension_is_roi_offset_only() {
        let d = Dimension {
            size: 1024,
            offset: 10,
            full_size: 1024,
            binning: 1,
            reverse: false,
        };
        assert!(d.is_roi()); // offset != 0 → ROI
    }

    #[test]
    fn test_dimension_is_roi_size_mismatch() {
        let d = Dimension {
            size: 512,
            offset: 0,
            full_size: 1024,
            binning: 1,
            reverse: false,
        };
        assert!(d.is_roi()); // size != full_size → ROI
    }

    #[test]
    fn test_dimension_binning() {
        let d = Dimension {
            size: 512,
            offset: 0,
            full_size: 512,
            binning: 2,
            reverse: false,
        };
        assert!(d.is_binned());
        assert_eq!(d.detector_extent(), 1024); // 512 × 2
    }

    #[test]
    fn test_dimension_detector_extent_no_binning() {
        let d = Dimension::new(1024);
        assert_eq!(d.detector_extent(), 1024); // 1024 × 1
    }

    #[test]
    fn test_dimension_display_simple() {
        assert_eq!(Dimension::new(1024).to_string(), "1024");
    }

    #[test]
    fn test_dimension_display_roi() {
        let d = Dimension::with_roi(512, 100, 1024);
        assert_eq!(d.to_string(), "512@100(/1024)");
    }

    #[test]
    fn test_dimension_display_binned() {
        let d = Dimension {
            size: 512,
            offset: 0,
            full_size: 512,
            binning: 2,
            reverse: false,
        };
        assert_eq!(d.to_string(), "512 bin=2");
    }

    #[test]
    fn test_dimension_display_roi_and_binned() {
        let d = Dimension {
            size: 256,
            offset: 50,
            full_size: 1024,
            binning: 4,
            reverse: false,
        };
        assert_eq!(d.to_string(), "256@50(/1024 bin=4)");
    }

    #[test]
    fn test_dimension_clone_eq() {
        let a = Dimension::new(768);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_dimension_ne() {
        assert_ne!(Dimension::new(1024), Dimension::new(768));
    }

    #[test]
    fn test_dimension_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Dimension::new(1024));
        set.insert(Dimension::new(1024)); // duplicate
        set.insert(Dimension::new(768));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_dimension_serde_roundtrip() {
        let d = Dimension {
            size: 512,
            offset: 100,
            full_size: 1024,
            binning: 2,
            reverse: true,
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: Dimension = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_dimension_serde_defaults() {
        let json = r#"{"size":1024}"#;
        let d: Dimension = serde_json::from_str(json).unwrap();
        assert_eq!(d.offset, 0);
        assert_eq!(d.full_size, 0);
        assert_eq!(d.binning, 1); // default_one
        assert!(!d.reverse);
    }

    #[test]
    fn test_source_type_from_i32() {
        assert_eq!(AttributeSourceType::from(0), AttributeSourceType::Driver);
        assert_eq!(AttributeSourceType::from(1), AttributeSourceType::Param);
        assert_eq!(AttributeSourceType::from(2), AttributeSourceType::EpicsPv);
        assert_eq!(AttributeSourceType::from(99), AttributeSourceType::Driver); // unknown → default
    }

    #[test]
    fn test_source_type_to_i32() {
        assert_eq!(i32::from(AttributeSourceType::Driver), 0);
        assert_eq!(i32::from(AttributeSourceType::Param), 1);
        assert_eq!(i32::from(AttributeSourceType::EpicsPv), 2);
    }

    #[test]
    fn test_source_type_default() {
        assert_eq!(AttributeSourceType::default(), AttributeSourceType::Driver);
    }

    #[test]
    fn test_source_type_display() {
        assert_eq!(AttributeSourceType::Driver.to_string(), "driver");
        assert_eq!(AttributeSourceType::Param.to_string(), "param");
        assert_eq!(AttributeSourceType::EpicsPv.to_string(), "epics_pv");
    }

    #[test]
    fn test_source_type_roundtrip() {
        for v in [0, 1, 2] {
            let st = AttributeSourceType::from(v);
            assert_eq!(i32::from(st), v);
        }
    }

    #[test]
    fn test_source_type_serde() {
        for st in [
            AttributeSourceType::Driver,
            AttributeSourceType::Param,
            AttributeSourceType::EpicsPv,
        ] {
            let json = serde_json::to_string(&st).unwrap();
            let back: AttributeSourceType = serde_json::from_str(&json).unwrap();
            assert_eq!(st, back);
        }
    }

    #[test]
    fn test_attribute_new() {
        let a = NdAttribute::new("ExposureTime", ScalarValue::Double(0.001));
        assert_eq!(a.name, "ExposureTime");
        assert_eq!(a.as_f64(), Some(0.001));
        assert_eq!(a.source, "");
        assert_eq!(a.source_type, AttributeSourceType::Driver);
    }

    #[test]
    fn test_attribute_with_source() {
        let a = NdAttribute::with_source(
            "BeamX",
            ScalarValue::Double(512.3),
            "cam1:BeamX",
            AttributeSourceType::EpicsPv,
        );
        assert_eq!(a.source, "cam1:BeamX");
        assert_eq!(a.source_type, AttributeSourceType::EpicsPv);
    }

    #[test]
    fn test_attribute_as_f64_string() {
        let a = NdAttribute::new("ColorMode", ScalarValue::String("RGB".into()));
        assert_eq!(a.as_f64(), None);
    }

    #[test]
    fn test_attribute_display() {
        let a = NdAttribute::new("Gain", ScalarValue::Int(42));
        assert_eq!(a.to_string(), "Gain=42");
    }

    #[test]
    fn test_attribute_display_string_value() {
        let a = NdAttribute::new("Model", ScalarValue::String("Prosilica".into()));
        assert_eq!(a.to_string(), "Model=Prosilica");
    }

    #[test]
    fn test_attribute_clone_eq() {
        let a = NdAttribute::new("X", ScalarValue::Double(1.0));
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_attribute_ne_name() {
        let a = NdAttribute::new("X", ScalarValue::Double(1.0));
        let b = NdAttribute::new("Y", ScalarValue::Double(1.0));
        assert_ne!(a, b);
    }

    #[test]
    fn test_attribute_ne_value() {
        let a = NdAttribute::new("X", ScalarValue::Double(1.0));
        let b = NdAttribute::new("X", ScalarValue::Double(2.0));
        assert_ne!(a, b);
    }

    #[test]
    fn test_attribute_serde_roundtrip() {
        let a = NdAttribute::with_source(
            "Exposure",
            ScalarValue::Double(0.05),
            "driver",
            AttributeSourceType::Param,
        );
        let json = serde_json::to_string(&a).unwrap();
        let back: NdAttribute = serde_json::from_str(&json).unwrap();
        assert_eq!(a, back);
    }

    #[test]
    fn test_attribute_serde_defaults() {
        let json = r#"{"name":"X","value":{"type":"Int","v":0}}"#;
        let a: NdAttribute = serde_json::from_str(json).unwrap();
        assert_eq!(a.source, "");
        assert_eq!(a.source_type, AttributeSourceType::Driver);
        assert_eq!(a.description, "");
    }

    #[test]
    fn test_debug_all() {
        assert!(!format!("{:?}", Codec::default()).is_empty());
        assert!(!format!("{:?}", Dimension::new(1)).is_empty());
        assert!(!format!("{:?}", AttributeSourceType::Driver).is_empty());
        assert!(!format!("{:?}", NdAttribute::new("X", ScalarValue::Int(0))).is_empty());
    }
}
