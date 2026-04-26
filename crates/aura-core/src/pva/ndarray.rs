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
        Self { name: name.into(), parameters: serde_json::Value::Null }
    }

    /// Create a codec with parameters.
    pub fn with_params(name: impl Into<String>, parameters: serde_json::Value) -> Self {
        Self { name: name.into(), parameters }
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
        Self { name: String::new(), parameters: serde_json::Value::Null }
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

fn default_one() -> i32 { 1 }

impl Dimension {
    /// Create a simple dimension (no ROI, no binning).
    pub fn new(size: i32) -> Self {
        Self { size, offset: 0, full_size: size, binning: 1, reverse: false }
    }

    /// Create a dimension with ROI.
    pub fn with_roi(size: i32, offset: i32, full_size: i32) -> Self {
        Self { size, offset, full_size, binning: 1, reverse: false }
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
            if self.is_binned() { write!(f, " bin={}", self.binning)?; }
            write!(f, ")")
        } else if self.is_binned() {
            write!(f, "{} bin={}", self.size, self.binning)
        } else {
            write!(f, "{}", self.size)
        }
    }
}


/// Source type for an NDArray attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum AttributeSourceType {
    /// Value comes from the detector driver.
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

impl Default for AttributeSourceType {
    fn default() -> Self { Self::Driver }
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