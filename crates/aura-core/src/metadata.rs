//! PV metadata — auto-captured from the first PVA monitor response.
//!
//! When AURA first subscribes to a PV, the initial monitor delivers
//! the complete Normative Type structure including display, control,
//! valueAlarm, and enum choices. [`PvMetadata`] extracts and flattens
//! these fields into a single struct stored in the `pv_metadata` table.
//!
//! Metadata is updated if it changes (rare — e.g., operator changes
//! alarm limits via caput). The [`update_from`](PvMetadata::update_from)
//! method detects changes and returns `true` if any field was modified.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::pva::*;

/// Simplified dimension info for storage in `pv_metadata`.
///
/// Captures the essential size information from [`Dimension`] without
/// the ROI/binning details (which change per-frame, not per-PV).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DimensionInfo {
    /// Number of elements along this axis.
    pub size: i32,
    /// Full detector size along this axis.
    pub full_size: i32,
}

impl DimensionInfo {
    /// Create from an aura-net Dimension.
    #[inline]
    pub fn from_dimension(d: &Dimension) -> Self {
        Self { size: d.size, full_size: d.full_size }
    }
}

impl std::fmt::Display for DimensionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.size == self.full_size {
            write!(f, "{}", self.size)
        } else {
            write!(f, "{}/{}", self.size, self.full_size)
        }
    }
}

/// Complete metadata for a PV, extracted from PVA Normative Types.
///
/// Stored in the `pv_metadata` table. Updated rarely (only when
/// display/control/alarm fields change on the IOC side).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PvMetadata {
    /// PV name.
    pub pv_name: String,

    /// Numeric PV ID (from pv_lookup table).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pv_id: Option<i32>,

    /// High-level data type classification.
    pub data_type: PvDataType,

    /// Scalar element type (for scalars and arrays).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scalar_type: Option<ScalarType>,

    /// Array element count. None for scalars.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_size: Option<usize>,

    // ── Display ──────────────────────────────────────────────────
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub units: String,
    #[serde(default)]
    pub precision: i32,
    #[serde(default)]
    pub display_form: DisplayForm,
    #[serde(default)]
    pub display_low: f64,
    #[serde(default)]
    pub display_high: f64,

    // ── Control ──────────────────────────────────────────────────
    #[serde(default)]
    pub control_low: f64,
    #[serde(default)]
    pub control_high: f64,
    #[serde(default)]
    pub min_step: f64,

    // ── Value Alarms ─────────────────────────────────────────────
    #[serde(default)]
    pub alarm_lolo: f64,
    #[serde(default)]
    pub alarm_low: f64,
    #[serde(default)]
    pub alarm_high: f64,
    #[serde(default)]
    pub alarm_hihi: f64,
    #[serde(default)]
    pub alarm_hysteresis: f64,

    // ── Enum ─────────────────────────────────────────────────────
    #[serde(default)]
    pub enum_choices: Vec<String>,

    // ── NDArray ──────────────────────────────────────────────────
    #[serde(default)]
    pub dimensions: Vec<DimensionInfo>,

    // ── Tracking ─────────────────────────────────────────────────
    #[serde(default)]
    pub first_seen: Option<DateTime<Utc>>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

impl PvMetadata {
    /// Extract metadata from the first NormativeType received for a PV.
    ///
    /// This is called once at subscription time. It walks the NT structure
    /// and flattens display/control/alarm/enum/dimension fields into
    /// the flat `PvMetadata` struct.
    pub fn from_initial_update(pv_name: &str, nt: &NormativeType) -> Self {
        let now = Utc::now();
        let mut meta = Self::empty(pv_name, PvDataType::from_nt(nt), now);

        match nt {
            NormativeType::NTScalar(s) => {
                meta.scalar_type = Some(s.value.type_tag());
                meta.apply_optional_display_control_alarm(
                    s.display.as_ref(), s.control.as_ref(), s.value_alarm.as_ref(),
                );
            }
            NormativeType::NTEnum(e) => {
                meta.scalar_type = Some(ScalarType::Int);
                meta.enum_choices = e.value.choices.clone();
            }
            NormativeType::NTScalarArray(a) => {
                meta.scalar_type = Some(a.value.element_type());
                meta.array_size = Some(a.value.len());
                meta.apply_optional_display_control_alarm(
                    a.display.as_ref(), a.control.as_ref(), a.value_alarm.as_ref(),
                );
            }
            NormativeType::NTMatrix(m) => {
                meta.scalar_type = Some(ScalarType::Double);
                meta.array_size = Some(m.value.len());
                meta.description = m.descriptor.clone();
                if let Some(d) = &m.display {
                    meta.apply_display(d);
                }
                if m.dim.len() >= 2 {
                    meta.dimensions = m.dim.iter()
                        .map(|&s| DimensionInfo { size: s, full_size: s })
                        .collect();
                }
            }
            NormativeType::NTHistogram(h) => {
                meta.description = h.descriptor.clone();
                meta.array_size = Some(h.bin_count());
            }
            NormativeType::NTContinuum(c) => {
                meta.scalar_type = Some(ScalarType::Double);
                meta.description = c.descriptor.clone();
                meta.array_size = Some(c.point_count());
                if !c.units.is_empty() {
                    meta.units = c.units[0].clone();
                }
            }
            NormativeType::NTNameValue(nv) => {
                meta.description = nv.descriptor.clone();
                meta.array_size = Some(nv.len());
                meta.scalar_type = Some(nv.value.element_type());
            }
            NormativeType::NTTable(t) => {
                meta.array_size = Some(t.row_count());
            }
            NormativeType::NTNDArray(img) => {
                meta.scalar_type = Some(img.value.element_type());
                meta.dimensions = img.dimension.iter()
                    .map(DimensionInfo::from_dimension)
                    .collect();
            }
            NormativeType::NTMultiChannel(mc) => {
                meta.array_size = Some(mc.channel_name.len());
            }
            NormativeType::NTAggregate(_) => {
                meta.scalar_type = Some(ScalarType::Double);
            }
            NormativeType::NTUnion(u) => {
                meta.description = u.descriptor.clone();
                if let UnionValue::Scalar(s) = &u.value {
                    meta.scalar_type = Some(s.type_tag());
                }
            }
            NormativeType::Custom(_) => {}
        }

        meta
    }

    /// Update metadata if display/control/alarm/enum fields changed.
    /// Returns `true` if any field was modified.
    ///
    /// Called on every monitor update to catch rare metadata changes
    /// (e.g., operator changes alarm limits via caput). The comparison
    /// is cheap — just field-by-field equality on primitives.
    pub fn update_from(&mut self, nt: &NormativeType) -> bool {
        let snapshot = self.snapshot();

        match nt {
            NormativeType::NTScalar(s) => {
                self.apply_optional_display_control_alarm(
                    s.display.as_ref(), s.control.as_ref(), s.value_alarm.as_ref(),
                );
            }
            NormativeType::NTEnum(e) => {
                if self.enum_choices != e.value.choices {
                    self.enum_choices = e.value.choices.clone();
                }
            }
            NormativeType::NTScalarArray(a) => {
                self.array_size = Some(a.value.len());
                self.apply_optional_display_control_alarm(
                    a.display.as_ref(), a.control.as_ref(), a.value_alarm.as_ref(),
                );
            }
            NormativeType::NTMatrix(m) => {
                if let Some(d) = &m.display {
                    self.apply_display(d);
                }
            }
            NormativeType::NTNDArray(img) => {
                self.dimensions = img.dimension.iter()
                    .map(DimensionInfo::from_dimension)
                    .collect();
            }
            _ => {}
        }

        let changed = self.differs_from(&snapshot);
        if changed {
            self.updated_at = Some(Utc::now());
        }
        changed
    }

    /// Whether this PV has display limits configured.
    pub fn has_display_range(&self) -> bool {
        self.display_low < self.display_high
    }

    /// Whether this PV has control limits configured.
    pub fn has_control_range(&self) -> bool {
        self.control_low < self.control_high
    }

    /// Whether this PV has alarm thresholds configured.
    pub fn has_alarm_limits(&self) -> bool {
        self.alarm_lolo != 0.0 || self.alarm_low != 0.0
            || self.alarm_high != 0.0 || self.alarm_hihi != 0.0
    }

    /// Whether this is an enum PV.
    pub fn is_enum(&self) -> bool {
        !self.enum_choices.is_empty()
    }

    /// Whether this is an image/NDArray PV.
    pub fn is_image(&self) -> bool {
        self.data_type == PvDataType::Image
    }

    // ── Private helpers ──────────────────────────────────────────────

    fn empty(pv_name: &str, data_type: PvDataType, now: DateTime<Utc>) -> Self {
        Self {
            pv_name: pv_name.to_string(),
            pv_id: None,
            data_type,
            scalar_type: None,
            array_size: None,
            description: String::new(),
            units: String::new(),
            precision: 0,
            display_form: DisplayForm::Default,
            display_low: 0.0,
            display_high: 0.0,
            control_low: 0.0,
            control_high: 0.0,
            min_step: 0.0,
            alarm_lolo: 0.0,
            alarm_low: 0.0,
            alarm_high: 0.0,
            alarm_hihi: 0.0,
            alarm_hysteresis: 0.0,
            enum_choices: Vec::new(),
            dimensions: Vec::new(),
            first_seen: Some(now),
            updated_at: Some(now),
        }
    }

    fn apply_optional_display_control_alarm(
        &mut self,
        display: Option<&Display>,
        control: Option<&Control>,
        value_alarm: Option<&ValueAlarm>,
    ) {
        if let Some(d) = display { self.apply_display(d); }
        if let Some(c) = control { self.apply_control(c); }
        if let Some(va) = value_alarm { self.apply_value_alarm(va); }
    }

    fn apply_display(&mut self, d: &Display) {
        if !d.description.is_empty() {
            self.description = d.description.clone();
        }
        self.units = d.units.clone();
        self.precision = d.precision;
        self.display_form = d.form;
        self.display_low = d.limit_low;
        self.display_high = d.limit_high;
    }

    fn apply_control(&mut self, c: &Control) {
        self.control_low = c.limit_low;
        self.control_high = c.limit_high;
        self.min_step = c.min_step;
    }

    fn apply_value_alarm(&mut self, va: &ValueAlarm) {
        self.alarm_lolo = va.low_alarm_limit;
        self.alarm_low = va.low_warning_limit;
        self.alarm_high = va.high_warning_limit;
        self.alarm_hihi = va.high_alarm_limit;
        self.alarm_hysteresis = va.hysteresis;
    }

    /// Snapshot of the fields that can change at runtime.
    fn snapshot(&self) -> MetaSnapshot {
        MetaSnapshot {
            description: self.description.clone(),
            units: self.units.clone(),
            precision: self.precision,
            display_low: self.display_low,
            display_high: self.display_high,
            control_low: self.control_low,
            control_high: self.control_high,
            alarm_lolo: self.alarm_lolo,
            alarm_low: self.alarm_low,
            alarm_high: self.alarm_high,
            alarm_hihi: self.alarm_hihi,
            alarm_hysteresis: self.alarm_hysteresis,
            enum_choices: self.enum_choices.clone(),
            dimensions: self.dimensions.clone(),
            array_size: self.array_size,
        }
    }

    fn differs_from(&self, old: &MetaSnapshot) -> bool {
        self.description != old.description
            || self.units != old.units
            || self.precision != old.precision
            || self.display_low != old.display_low
            || self.display_high != old.display_high
            || self.control_low != old.control_low
            || self.control_high != old.control_high
            || self.alarm_lolo != old.alarm_lolo
            || self.alarm_low != old.alarm_low
            || self.alarm_high != old.alarm_high
            || self.alarm_hihi != old.alarm_hihi
            || self.alarm_hysteresis != old.alarm_hysteresis
            || self.enum_choices != old.enum_choices
            || self.dimensions != old.dimensions
            || self.array_size != old.array_size
    }
}

/// Lightweight snapshot for change detection (avoids cloning entire PvMetadata).
struct MetaSnapshot {
    description: String,
    units: String,
    precision: i32,
    display_low: f64,
    display_high: f64,
    control_low: f64,
    control_high: f64,
    alarm_lolo: f64,
    alarm_low: f64,
    alarm_high: f64,
    alarm_hihi: f64,
    alarm_hysteresis: f64,
    enum_choices: Vec<String>,
    dimensions: Vec<DimensionInfo>,
    array_size: Option<usize>,
}

impl std::fmt::Display for PvMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.pv_name, self.data_type)?;
        if !self.units.is_empty() {
            write!(f, " [{}]", self.units)?;
        }
        if !self.enum_choices.is_empty() {
            write!(f, " enum:{}", self.enum_choices.len())?;
        }
        if !self.dimensions.is_empty() {
            let dims: Vec<String> = self.dimensions.iter().map(|d| d.to_string()).collect();
            write!(f, " {}D:{}", self.dimensions.len(), dims.join("×"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> TimeStamp { TimeStamp::new(0, 0) }

    fn full_display() -> Display {
        Display {
            limit_low: 0.0, limit_high: 100.0,
            description: "Cryo temp".into(), units: "K".into(),
            precision: 3, form: DisplayForm::Default,
        }
    }

    fn full_control() -> Control {
        Control { limit_low: 0.0, limit_high: 50.0, min_step: 0.001 }
    }

    fn full_value_alarm() -> ValueAlarm {
        ValueAlarm::symmetric(10.0, 2.0, 5.0)
    }

    fn make_scalar_nt() -> NormativeType {
        NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(4.217),
            alarm: Alarm::default(), timestamp: ts(),
            display: Some(full_display()),
            control: Some(full_control()),
            value_alarm: Some(full_value_alarm()),
        })
    }

    // ── from_initial_update: all 13 NT variants ──────────────────────

    #[test]
    fn test_from_scalar_double() {
        let meta = PvMetadata::from_initial_update("CRYO:TEMP", &make_scalar_nt());
        assert_eq!(meta.pv_name, "CRYO:TEMP");
        assert_eq!(meta.data_type, PvDataType::Scalar);
        assert_eq!(meta.scalar_type, Some(ScalarType::Double));
        assert_eq!(meta.units, "K");
        assert_eq!(meta.precision, 3);
        assert_eq!(meta.description, "Cryo temp");
        assert_eq!(meta.display_high, 100.0);
        assert_eq!(meta.control_high, 50.0);
        assert_eq!(meta.min_step, 0.001);
        assert_eq!(meta.alarm_hihi, 15.0);
        assert_eq!(meta.alarm_low, 8.0);
        assert!(meta.first_seen.is_some());
    }

    #[test]
    fn test_from_scalar_string() {
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::String("hello".into()),
            alarm: Alarm::default(), timestamp: ts(),
            display: None, control: None, value_alarm: None,
        });
        let meta = PvMetadata::from_initial_update("STR:PV", &nt);
        assert_eq!(meta.data_type, PvDataType::String);
        assert_eq!(meta.scalar_type, Some(ScalarType::String));
    }

    #[test]
    fn test_from_scalar_no_metadata() {
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Int(42),
            alarm: Alarm::default(), timestamp: ts(),
            display: None, control: None, value_alarm: None,
        });
        let meta = PvMetadata::from_initial_update("BARE:PV", &nt);
        assert_eq!(meta.units, "");
        assert_eq!(meta.precision, 0);
        assert_eq!(meta.alarm_hihi, 0.0);
        assert!(!meta.has_display_range());
        assert!(!meta.has_control_range());
        assert!(!meta.has_alarm_limits());
    }

    #[test]
    fn test_from_enum() {
        let nt = NormativeType::NTEnum(NTEnum {
            value: EnumValue::from_strs(0, &["Open", "Closed", "Fault"]),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("VALVE:ST", &nt);
        assert_eq!(meta.data_type, PvDataType::Scalar);
        assert_eq!(meta.scalar_type, Some(ScalarType::Int));
        assert_eq!(meta.enum_choices, vec!["Open", "Closed", "Fault"]);
        assert!(meta.is_enum());
    }

    #[test]
    fn test_from_scalar_array() {
        let nt = NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::DoubleArray(vec![0.0; 1024]),
            alarm: Alarm::default(), timestamp: ts(),
            display: Some(Display::new("V", 2)),
            control: None, value_alarm: None,
        });
        let meta = PvMetadata::from_initial_update("SCOPE:WAVE", &nt);
        assert_eq!(meta.data_type, PvDataType::Array);
        assert_eq!(meta.scalar_type, Some(ScalarType::Double));
        assert_eq!(meta.array_size, Some(1024));
        assert_eq!(meta.units, "V");
        assert_eq!(meta.precision, 2);
    }

    #[test]
    fn test_from_matrix() {
        let nt = NormativeType::NTMatrix(NTMatrix {
            value: vec![1.0; 6], dim: vec![2, 3],
            descriptor: "response".into(),
            alarm: Alarm::default(), timestamp: ts(),
            display: Some(Display::new("mm", 1)),
        });
        let meta = PvMetadata::from_initial_update("OPT:RESP", &nt);
        assert_eq!(meta.data_type, PvDataType::Matrix);
        assert_eq!(meta.scalar_type, Some(ScalarType::Double));
        assert_eq!(meta.array_size, Some(6));
        assert_eq!(meta.description, "response");
        assert_eq!(meta.units, "mm");
        assert_eq!(meta.dimensions.len(), 2);
        assert_eq!(meta.dimensions[0].size, 2);
        assert_eq!(meta.dimensions[1].size, 3);
    }

    #[test]
    fn test_from_matrix_no_dim() {
        let nt = NormativeType::NTMatrix(NTMatrix {
            value: vec![1.0, 2.0, 3.0], dim: vec![],
            descriptor: String::new(),
            alarm: Alarm::default(), timestamp: ts(), display: None,
        });
        let meta = PvMetadata::from_initial_update("VEC:PV", &nt);
        assert!(meta.dimensions.is_empty()); // no dim → no dimensions
    }

    #[test]
    fn test_from_histogram() {
        let nt = NormativeType::NTHistogram(NTHistogram {
            ranges: vec![0.0, 1.0, 2.0, 3.0],
            value: HistogramValue::Int(vec![10, 20, 30]),
            descriptor: "beam profile".into(),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("DIAG:HIST", &nt);
        assert_eq!(meta.data_type, PvDataType::Histogram);
        assert_eq!(meta.description, "beam profile");
        assert_eq!(meta.array_size, Some(3));
    }

    #[test]
    fn test_from_continuum() {
        let nt = NormativeType::NTContinuum(NTContinuum {
            base: vec![0.0, 1.0, 2.0],
            value: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            units: vec!["s".into(), "V".into(), "A".into()],
            descriptor: "VI curve".into(),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("DIAG:VI", &nt);
        assert_eq!(meta.data_type, PvDataType::Continuum);
        assert_eq!(meta.scalar_type, Some(ScalarType::Double));
        assert_eq!(meta.description, "VI curve");
        assert_eq!(meta.array_size, Some(3));
        assert_eq!(meta.units, "s"); // base unit
    }

    #[test]
    fn test_from_continuum_no_units() {
        let nt = NormativeType::NTContinuum(NTContinuum {
            base: vec![0.0], value: vec![1.0],
            units: vec![], descriptor: String::new(),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("PV", &nt);
        assert_eq!(meta.units, "");
    }

    #[test]
    fn test_from_name_value() {
        let nt = NormativeType::NTNameValue(NTNameValue {
            name: vec!["gain".into(), "offset".into()],
            value: ArrayValue::DoubleArray(vec![1.5, -0.3]),
            descriptor: "amp settings".into(),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("AMP:CONF", &nt);
        assert_eq!(meta.data_type, PvDataType::NameValue);
        assert_eq!(meta.description, "amp settings");
        assert_eq!(meta.array_size, Some(2));
        assert_eq!(meta.scalar_type, Some(ScalarType::Double));
    }

    #[test]
    fn test_from_table() {
        let nt = NormativeType::NTTable(NTTable {
            labels: vec!["x".into(), "y".into()],
            columns: vec![
                TableColumn::new("x", ArrayValue::DoubleArray(vec![1.0, 2.0])),
                TableColumn::new("y", ArrayValue::DoubleArray(vec![3.0, 4.0])),
            ],
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("TBL:PV", &nt);
        assert_eq!(meta.data_type, PvDataType::Table);
        assert_eq!(meta.array_size, Some(2)); // 2 rows
    }

    #[test]
    fn test_from_ndarray() {
        let nt = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![0; 640 * 480]),
            codec: Codec::default(),
            compressed_size: 0, uncompressed_size: 640 * 480,
            dimension: vec![Dimension::new(640), Dimension::new(480)],
            unique_id: 1, data_timestamp: None,
            alarm: Alarm::default(), timestamp: ts(),
            attribute: vec![],
        });
        let meta = PvMetadata::from_initial_update("CAM:IMG", &nt);
        assert_eq!(meta.data_type, PvDataType::Image);
        assert_eq!(meta.scalar_type, Some(ScalarType::UByte));
        assert_eq!(meta.dimensions.len(), 2);
        assert_eq!(meta.dimensions[0].size, 640);
        assert_eq!(meta.dimensions[1].size, 480);
        assert!(meta.is_image());
    }

    #[test]
    fn test_from_multichannel() {
        let nt = NormativeType::NTMultiChannel(NTMultiChannel {
            values: vec![ScalarValue::Double(1.0), ScalarValue::Double(2.0)],
            channel_name: vec!["PV:A".into(), "PV:B".into()],
            is_connected: vec![true, true],
            severity: vec![], status: vec![], message: vec![],
            seconds_past_epoch: vec![], nanoseconds: vec![],
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("GRP:PV", &nt);
        assert_eq!(meta.data_type, PvDataType::MultiChannel);
        assert_eq!(meta.array_size, Some(2));
    }

    #[test]
    fn test_from_aggregate() {
        let nt = NormativeType::NTAggregate(NTAggregate {
            value: 4.2, n: 100, dispersion: 0.01,
            first: 4.1, last: 4.3, max: 4.5, min: 3.9,
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("AGG:PV", &nt);
        assert_eq!(meta.data_type, PvDataType::Aggregate);
        assert_eq!(meta.scalar_type, Some(ScalarType::Double));
    }

    #[test]
    fn test_from_union_scalar() {
        let nt = NormativeType::NTUnion(NTUnion {
            value: UnionValue::Scalar(ScalarValue::Double(1.0)),
            descriptor: "flex pv".into(),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("UNI:PV", &nt);
        assert_eq!(meta.data_type, PvDataType::Union);
        assert_eq!(meta.scalar_type, Some(ScalarType::Double));
        assert_eq!(meta.description, "flex pv");
    }

    #[test]
    fn test_from_union_array() {
        let nt = NormativeType::NTUnion(NTUnion {
            value: UnionValue::Array(ArrayValue::IntArray(vec![1, 2])),
            descriptor: String::new(),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("UNI:ARR", &nt);
        assert_eq!(meta.scalar_type, None); // not a scalar union
    }

    #[test]
    fn test_from_custom() {
        let nt = NormativeType::Custom(CustomStructure {
            data: serde_json::json!({"x": 1}),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("CUST:PV", &nt);
        assert_eq!(meta.data_type, PvDataType::Custom);
        assert_eq!(meta.scalar_type, None);
        assert!(meta.description.is_empty());
    }

    // ── update_from ──────────────────────────────────────────────────

    #[test]
    fn test_update_no_change() {
        let nt = make_scalar_nt();
        let mut meta = PvMetadata::from_initial_update("PV", &nt);
        assert!(!meta.update_from(&nt));
    }

    #[test]
    fn test_update_display_changed() {
        let mut meta = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        let nt2 = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(0.0),
            alarm: Alarm::default(), timestamp: ts(),
            display: Some(Display { units: "degC".into(), ..Display::default() }),
            control: None, value_alarm: None,
        });
        assert!(meta.update_from(&nt2));
        assert_eq!(meta.units, "degC");
        assert!(meta.updated_at.is_some());
    }

    #[test]
    fn test_update_alarm_changed() {
        let mut meta = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        let nt2 = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(0.0),
            alarm: Alarm::default(), timestamp: ts(),
            display: Some(full_display()),
            control: Some(full_control()),
            value_alarm: Some(ValueAlarm::symmetric(20.0, 3.0, 8.0)), // changed
        });
        assert!(meta.update_from(&nt2));
        assert_eq!(meta.alarm_hihi, 28.0); // 20 + 8
    }

    #[test]
    fn test_update_enum_choices_changed() {
        let nt = NormativeType::NTEnum(NTEnum {
            value: EnumValue::from_strs(0, &["A", "B"]),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let mut meta = PvMetadata::from_initial_update("PV", &nt);

        let nt2 = NormativeType::NTEnum(NTEnum {
            value: EnumValue::from_strs(0, &["A", "B", "C"]), // added C
            alarm: Alarm::default(), timestamp: ts(),
        });
        assert!(meta.update_from(&nt2));
        assert_eq!(meta.enum_choices, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_update_array_size_changed() {
        let nt = NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::DoubleArray(vec![0.0; 100]),
            alarm: Alarm::default(), timestamp: ts(),
            display: None, control: None, value_alarm: None,
        });
        let mut meta = PvMetadata::from_initial_update("PV", &nt);

        let nt2 = NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::DoubleArray(vec![0.0; 200]), // resized
            alarm: Alarm::default(), timestamp: ts(),
            display: None, control: None, value_alarm: None,
        });
        assert!(meta.update_from(&nt2));
        assert_eq!(meta.array_size, Some(200));
    }

    #[test]
    fn test_update_ndarray_dimensions_changed() {
        let nt = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![]),
            codec: Codec::default(),
            compressed_size: 0, uncompressed_size: 0,
            dimension: vec![Dimension::new(640), Dimension::new(480)],
            unique_id: 0, data_timestamp: None,
            alarm: Alarm::default(), timestamp: ts(), attribute: vec![],
        });
        let mut meta = PvMetadata::from_initial_update("CAM", &nt);

        let nt2 = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![]),
            codec: Codec::default(),
            compressed_size: 0, uncompressed_size: 0,
            dimension: vec![Dimension::new(1024), Dimension::new(768)], // ROI changed
            unique_id: 1, data_timestamp: None,
            alarm: Alarm::default(), timestamp: ts(), attribute: vec![],
        });
        assert!(meta.update_from(&nt2));
        assert_eq!(meta.dimensions[0].size, 1024);
    }

    #[test]
    fn test_update_matrix_display_changed() {
        let nt = NormativeType::NTMatrix(NTMatrix {
            value: vec![1.0; 4], dim: vec![2, 2],
            descriptor: String::new(),
            alarm: Alarm::default(), timestamp: ts(),
            display: Some(Display::new("mm", 1)),
        });
        let mut meta = PvMetadata::from_initial_update("MTX", &nt);

        let nt2 = NormativeType::NTMatrix(NTMatrix {
            value: vec![1.0; 4], dim: vec![2, 2],
            descriptor: String::new(),
            alarm: Alarm::default(), timestamp: ts(),
            display: Some(Display::new("µm", 3)), // changed
        });
        assert!(meta.update_from(&nt2));
        assert_eq!(meta.units, "µm");
        assert_eq!(meta.precision, 3);
    }

    #[test]
    fn test_update_unhandled_type_no_change() {
        let nt = NormativeType::NTAggregate(NTAggregate {
            value: 1.0, n: 1, dispersion: 0.0,
            first: 1.0, last: 1.0, max: 1.0, min: 1.0,
            alarm: Alarm::default(), timestamp: ts(),
        });
        let mut meta = PvMetadata::from_initial_update("AGG", &nt);
        assert!(!meta.update_from(&nt)); // aggregate has no updatable metadata
    }

    // ── Helper methods ───────────────────────────────────────────────

    #[test]
    fn test_has_display_range() {
        let meta = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        assert!(meta.has_display_range()); // 0..100
    }

    #[test]
    fn test_has_control_range() {
        let meta = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        assert!(meta.has_control_range()); // 0..50
    }

    #[test]
    fn test_has_alarm_limits() {
        let meta = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        assert!(meta.has_alarm_limits());
    }

    #[test]
    fn test_no_alarm_limits_when_zero() {
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(0.0),
            alarm: Alarm::default(), timestamp: ts(),
            display: None, control: None, value_alarm: None,
        });
        let meta = PvMetadata::from_initial_update("PV", &nt);
        assert!(!meta.has_alarm_limits());
    }

    #[test]
    fn test_is_enum() {
        let nt = NormativeType::NTEnum(NTEnum {
            value: EnumValue::from_strs(0, &["A"]),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("PV", &nt);
        assert!(meta.is_enum());

        let meta2 = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        assert!(!meta2.is_enum());
    }

    #[test]
    fn test_is_image() {
        let nt = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![]),
            codec: Codec::default(),
            compressed_size: 0, uncompressed_size: 0,
            dimension: vec![Dimension::new(1)],
            unique_id: 0, data_timestamp: None,
            alarm: Alarm::default(), timestamp: ts(), attribute: vec![],
        });
        let meta = PvMetadata::from_initial_update("CAM", &nt);
        assert!(meta.is_image());

        let meta2 = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        assert!(!meta2.is_image());
    }

    // ── Display ──────────────────────────────────────────────────────

    #[test]
    fn test_display_scalar() {
        let meta = PvMetadata::from_initial_update("CRYO:T", &make_scalar_nt());
        let s = meta.to_string();
        assert!(s.contains("CRYO:T"));
        assert!(s.contains("[K]"));
    }

    #[test]
    fn test_display_enum() {
        let nt = NormativeType::NTEnum(NTEnum {
            value: EnumValue::from_strs(0, &["A", "B", "C"]),
            alarm: Alarm::default(), timestamp: ts(),
        });
        let meta = PvMetadata::from_initial_update("VALVE", &nt);
        assert!(meta.to_string().contains("enum:3"));
    }

    #[test]
    fn test_display_image() {
        let nt = NormativeType::NTNDArray(NTNDArray {
            value: ArrayValue::UByteArray(vec![]),
            codec: Codec::default(),
            compressed_size: 0, uncompressed_size: 0,
            dimension: vec![Dimension::new(1024), Dimension::new(768)],
            unique_id: 0, data_timestamp: None,
            alarm: Alarm::default(), timestamp: ts(), attribute: vec![],
        });
        let meta = PvMetadata::from_initial_update("CAM", &nt);
        let s = meta.to_string();
        assert!(s.contains("2D"));
        assert!(s.contains("1024"));
    }

    // ── DimensionInfo ────────────────────────────────────────────────

    #[test]
    fn test_dimension_info_from_dimension() {
        let d = Dimension::with_roi(512, 100, 1024);
        let di = DimensionInfo::from_dimension(&d);
        assert_eq!(di.size, 512);
        assert_eq!(di.full_size, 1024);
    }

    #[test]
    fn test_dimension_info_display_no_roi() {
        let di = DimensionInfo { size: 1024, full_size: 1024 };
        assert_eq!(di.to_string(), "1024");
    }

    #[test]
    fn test_dimension_info_display_roi() {
        let di = DimensionInfo { size: 512, full_size: 1024 };
        assert_eq!(di.to_string(), "512/1024");
    }

    #[test]
    fn test_dimension_info_eq() {
        let a = DimensionInfo { size: 640, full_size: 640 };
        let b = a.clone();
        assert_eq!(a, b);
    }

    // ── Serde ────────────────────────────────────────────────────────

    #[test]
    fn test_serde_roundtrip() {
        let meta = PvMetadata::from_initial_update("CRYO:T", &make_scalar_nt());
        let json = serde_json::to_string(&meta).unwrap();
        let back: PvMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta.pv_name, back.pv_name);
        assert_eq!(meta.data_type, back.data_type);
        assert_eq!(meta.units, back.units);
        assert_eq!(meta.alarm_hihi, back.alarm_hihi);
    }

    #[test]
    fn test_serde_skips_none_pv_id() {
        let meta = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        let json = serde_json::to_string(&meta).unwrap();
        assert!(!json.contains("pv_id")); // skip_serializing_if
    }

    // ── Debug ────────────────────────────────────────────────────────

    #[test]
    fn test_debug() {
        let meta = PvMetadata::from_initial_update("PV", &make_scalar_nt());
        assert!(!format!("{:?}", meta).is_empty());
    }
}