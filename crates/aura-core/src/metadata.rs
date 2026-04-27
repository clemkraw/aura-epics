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
        self.description = d.description.clone();
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