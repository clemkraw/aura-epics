//! PV metadata storage layer.
//!
//! Stores and retrieves PV metadata in the `pv_metadata` table.
//! Uses [`StoredMetadata`] (plain types) for the DB layer, with
//! conversion from `aura_core::metadata::PvMetadata` via
//! [`StoredMetadata::from_core`].

use std::fmt;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;

use aura_core::error::{AuraError, AuraResult};
use aura_core::metadata::PvMetadata;

mod sql {
    /// All columns for SELECT (reused across queries).
    pub const COLUMNS: &str = "pv_name, pv_id, data_type, scalar_type, array_size, \
         description, units, precision, display_form, \
         display_low, display_high, control_low, control_high, min_step, \
         alarm_lolo, alarm_low, alarm_high, alarm_hihi, alarm_hysteresis, \
         enum_choices, dimensions, updated_at";

    pub const GET: &str = "SELECT pv_name, pv_id, data_type, scalar_type, array_size, \
         description, units, precision, display_form, \
         display_low, display_high, control_low, control_high, min_step, \
         alarm_lolo, alarm_low, alarm_high, alarm_hihi, alarm_hysteresis, \
         enum_choices, dimensions, updated_at \
         FROM pv_metadata WHERE pv_name = $1";

    pub const UPSERT: &str = "INSERT INTO pv_metadata \
         (pv_name, pv_id, data_type, scalar_type, array_size, \
          description, units, precision, display_form, \
          display_low, display_high, control_low, control_high, min_step, \
          alarm_lolo, alarm_low, alarm_high, alarm_hihi, alarm_hysteresis, \
          enum_choices, dimensions, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,NOW()) \
         ON CONFLICT (pv_name) DO UPDATE SET \
             pv_id = EXCLUDED.pv_id, \
             data_type = EXCLUDED.data_type, \
             scalar_type = EXCLUDED.scalar_type, \
             array_size = EXCLUDED.array_size, \
             description = EXCLUDED.description, \
             units = EXCLUDED.units, \
             precision = EXCLUDED.precision, \
             display_form = EXCLUDED.display_form, \
             display_low = EXCLUDED.display_low, \
             display_high = EXCLUDED.display_high, \
             control_low = EXCLUDED.control_low, \
             control_high = EXCLUDED.control_high, \
             min_step = EXCLUDED.min_step, \
             alarm_lolo = EXCLUDED.alarm_lolo, \
             alarm_low = EXCLUDED.alarm_low, \
             alarm_high = EXCLUDED.alarm_high, \
             alarm_hihi = EXCLUDED.alarm_hihi, \
             alarm_hysteresis = EXCLUDED.alarm_hysteresis, \
             enum_choices = EXCLUDED.enum_choices, \
             dimensions = EXCLUDED.dimensions, \
             updated_at = NOW()";

    pub const DELETE: &str = "DELETE FROM pv_metadata WHERE pv_name = $1";

    pub const LIST_ALL: &str = "SELECT pv_name, pv_id, data_type, scalar_type, array_size, \
         description, units, precision, display_form, \
         display_low, display_high, control_low, control_high, min_step, \
         alarm_lolo, alarm_low, alarm_high, alarm_hihi, alarm_hysteresis, \
         enum_choices, dimensions, updated_at \
         FROM pv_metadata ORDER BY pv_name";

    pub const COUNT: &str = "SELECT COUNT(*) FROM pv_metadata";

    pub const GET_BY_TYPE: &str = "SELECT pv_name, pv_id, data_type, scalar_type, array_size, \
         description, units, precision, display_form, \
         display_low, display_high, control_low, control_high, min_step, \
         alarm_lolo, alarm_low, alarm_high, alarm_hihi, alarm_hysteresis, \
         enum_choices, dimensions, updated_at \
         FROM pv_metadata WHERE data_type = $1 ORDER BY pv_name";
}

/// PV metadata as stored in the database.
///
/// Uses plain types (String, f64) that sqlx can encode/decode natively.
/// No `Encode<Postgres>` needed for aura-core enums.
#[derive(Debug, Clone)]
pub struct StoredMetadata {
    pub pv_name: String,
    pub pv_id: Option<i32>,
    pub data_type: String,
    pub scalar_type: Option<String>,
    pub array_size: Option<i32>,
    pub description: String,
    pub units: String,
    pub precision: i32,
    pub display_form: String,
    pub display_low: f64,
    pub display_high: f64,
    pub control_low: f64,
    pub control_high: f64,
    pub min_step: f64,
    pub alarm_lolo: f64,
    pub alarm_low: f64,
    pub alarm_high: f64,
    pub alarm_hihi: f64,
    pub alarm_hysteresis: f64,
    pub enum_choices: Vec<String>,
    pub dimensions: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

impl StoredMetadata {
    /// Convert from aura-core PvMetadata to storage format.
    ///
    /// Validates and clamps values:
    /// - precision clamped to 0..=15
    /// - alarm thresholds: lolo <= low <= high <= hihi
    pub fn from_core(meta: &PvMetadata) -> Self {
        let precision = meta.precision.max(0).min(15);

        Self {
            pv_name: meta.pv_name.clone(),
            pv_id: meta.pv_id,
            data_type: format!("{:?}", meta.data_type),
            scalar_type: meta.scalar_type.map(|s| format!("{}", s)),
            array_size: meta.array_size.map(|s| s as i32),
            description: meta.description.clone(),
            units: meta.units.clone(),
            precision,
            display_form: format!("{:?}", meta.display_form),
            display_low: meta.display_low,
            display_high: meta.display_high,
            control_low: meta.control_low,
            control_high: meta.control_high,
            min_step: meta.min_step.max(0.0),
            alarm_lolo: meta.alarm_lolo,
            alarm_low: meta.alarm_low,
            alarm_high: meta.alarm_high,
            alarm_hihi: meta.alarm_hihi,
            alarm_hysteresis: meta.alarm_hysteresis.max(0.0),
            enum_choices: meta.enum_choices.clone(),
            dimensions: serde_json::to_value(&meta.dimensions).unwrap_or_default(),
            updated_at: meta.updated_at.unwrap_or_else(Utc::now),
        }
    }

    /// Whether this PV has alarm thresholds configured.
    pub fn has_alarms(&self) -> bool {
        self.alarm_lolo != 0.0
            || self.alarm_low != 0.0
            || self.alarm_high != 0.0
            || self.alarm_hihi != 0.0
    }

    /// Whether this PV has display limits configured.
    pub fn has_display_range(&self) -> bool {
        self.display_low != self.display_high
    }

    /// Whether this PV has control limits configured.
    pub fn has_control_range(&self) -> bool {
        self.control_low != self.control_high
    }

    /// Whether this PV is an enum type.
    pub fn is_enum(&self) -> bool {
        !self.enum_choices.is_empty()
    }

    /// Approximate heap memory used.
    pub fn mem_size(&self) -> usize {
        self.pv_name.len()
            + self.data_type.len()
            + self.scalar_type.as_ref().map_or(0, |s| s.len() + 24)
            + self.description.len()
            + self.units.len()
            + self.display_form.len()
            + self
                .enum_choices
                .iter()
                .map(|s| s.len() + 24)
                .sum::<usize>()
            + crate::writer::json::estimate_json_size(&self.dimensions)
            + 200 // struct overhead + floats
    }
}

impl fmt::Display for StoredMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}{}{})",
            self.pv_name,
            self.data_type,
            if let Some(ref st) = self.scalar_type {
                format!("/{st}")
            } else {
                String::new()
            },
            if !self.units.is_empty() {
                format!(", {}", self.units)
            } else {
                String::new()
            }
        )
    }
}

/// Internal sqlx row — moves (not clones) into StoredMetadata.
#[derive(Debug, sqlx::FromRow)]
struct MetadataRow {
    pv_name: String,
    pv_id: Option<i32>,
    data_type: String,
    scalar_type: Option<String>,
    array_size: Option<i32>,
    description: String,
    units: String,
    precision: i32,
    display_form: String,
    display_low: f64,
    display_high: f64,
    control_low: f64,
    control_high: f64,
    min_step: f64,
    alarm_lolo: f64,
    alarm_low: f64,
    alarm_high: f64,
    alarm_hihi: f64,
    alarm_hysteresis: f64,
    enum_choices: Vec<String>,
    dimensions: serde_json::Value,
    updated_at: DateTime<Utc>,
}

impl MetadataRow {
    /// Move all fields into StoredMetadata.
    fn into_stored(self) -> StoredMetadata {
        StoredMetadata {
            pv_name: self.pv_name, // moved
            pv_id: self.pv_id,
            data_type: self.data_type,     // moved
            scalar_type: self.scalar_type, // moved
            array_size: self.array_size,
            description: self.description, // moved
            units: self.units,             // moved
            precision: self.precision,
            display_form: self.display_form, // moved
            display_low: self.display_low,
            display_high: self.display_high,
            control_low: self.control_low,
            control_high: self.control_high,
            min_step: self.min_step,
            alarm_lolo: self.alarm_lolo,
            alarm_low: self.alarm_low,
            alarm_high: self.alarm_high,
            alarm_hihi: self.alarm_hihi,
            alarm_hysteresis: self.alarm_hysteresis,
            enum_choices: self.enum_choices, // moved
            dimensions: self.dimensions,     // moved
            updated_at: self.updated_at,
        }
    }
}

/// Metadata data access object.
pub struct MetadataDao;

impl MetadataDao {
    /// Get metadata for a PV.
    pub async fn get(pool: &PgPool, pv_name: &str) -> AuraResult<Option<StoredMetadata>> {
        let row = sqlx::query_as::<_, MetadataRow>(sql::GET)
            .bind(pv_name)
            .fetch_optional(pool)
            .await
            .map_err(|e| AuraError::database(format!("get metadata: {e}")))?;
        Ok(row.map(MetadataRow::into_stored))
    }

    /// Upsert metadata (insert or update on conflict).
    pub async fn upsert(pool: &PgPool, meta: &StoredMetadata) -> AuraResult<()> {
        sqlx::query(sql::UPSERT)
            .bind(&meta.pv_name)
            .bind(meta.pv_id)
            .bind(&meta.data_type)
            .bind(&meta.scalar_type)
            .bind(meta.array_size)
            .bind(&meta.description)
            .bind(&meta.units)
            .bind(meta.precision)
            .bind(&meta.display_form)
            .bind(meta.display_low)
            .bind(meta.display_high)
            .bind(meta.control_low)
            .bind(meta.control_high)
            .bind(meta.min_step)
            .bind(meta.alarm_lolo)
            .bind(meta.alarm_low)
            .bind(meta.alarm_high)
            .bind(meta.alarm_hihi)
            .bind(meta.alarm_hysteresis)
            .bind(&meta.enum_choices)
            .bind(&meta.dimensions)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("upsert metadata: {e}")))?;
        Ok(())
    }

    /// Upsert directly from an aura-core PvMetadata.
    pub async fn upsert_from_core(pool: &PgPool, meta: &PvMetadata) -> AuraResult<()> {
        Self::upsert(pool, &StoredMetadata::from_core(meta)).await
    }

    /// Batch upsert: N metadata entries in a single transaction.
    pub async fn upsert_batch(pool: &PgPool, entries: &[StoredMetadata]) -> AuraResult<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| AuraError::database(format!("metadata batch begin: {e}")))?;

        for meta in entries {
            sqlx::query(sql::UPSERT)
                .bind(&meta.pv_name)
                .bind(meta.pv_id)
                .bind(&meta.data_type)
                .bind(&meta.scalar_type)
                .bind(meta.array_size)
                .bind(&meta.description)
                .bind(&meta.units)
                .bind(meta.precision)
                .bind(&meta.display_form)
                .bind(meta.display_low)
                .bind(meta.display_high)
                .bind(meta.control_low)
                .bind(meta.control_high)
                .bind(meta.min_step)
                .bind(meta.alarm_lolo)
                .bind(meta.alarm_low)
                .bind(meta.alarm_high)
                .bind(meta.alarm_hihi)
                .bind(meta.alarm_hysteresis)
                .bind(&meta.enum_choices)
                .bind(&meta.dimensions)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    AuraError::database(format!("batch upsert metadata ({}): {e}", meta.pv_name))
                })?;
        }

        tx.commit()
            .await
            .map_err(|e| AuraError::database(format!("metadata batch commit: {e}")))?;

        Ok(entries.len())
    }

    /// Delete metadata for a PV.
    pub async fn delete(pool: &PgPool, pv_name: &str) -> AuraResult<bool> {
        let result = sqlx::query(sql::DELETE)
            .bind(pv_name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("delete metadata: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// List all PV metadata entries.
    pub async fn list_all(pool: &PgPool) -> AuraResult<Vec<StoredMetadata>> {
        let rows = sqlx::query_as::<_, MetadataRow>(sql::LIST_ALL)
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("list metadata: {e}")))?;
        Ok(rows.into_iter().map(MetadataRow::into_stored).collect())
    }

    /// Count total metadata entries.
    pub async fn count(pool: &PgPool) -> AuraResult<i64> {
        let count: Option<i64> = sqlx::query_scalar(sql::COUNT)
            .fetch_one(pool)
            .await
            .map_err(|e| AuraError::database(format!("count metadata: {e}")))?;
        Ok(count.unwrap_or(0))
    }

    /// Get metadata entries filtered by data type.
    pub async fn get_by_type(pool: &PgPool, data_type: &str) -> AuraResult<Vec<StoredMetadata>> {
        let rows = sqlx::query_as::<_, MetadataRow>(sql::GET_BY_TYPE)
            .bind(data_type)
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("get_by_type: {e}")))?;
        Ok(rows.into_iter().map(MetadataRow::into_stored).collect())
    }
}

impl fmt::Debug for MetadataDao {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataDao").finish()
    }
}

impl fmt::Display for MetadataDao {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetadataDao")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aura_core::pva::*;
    use serde_json::json;

    // Helper to create a minimal PvMetadata from an NTScalar.
    fn make_scalar_meta(name: &str, value: ScalarValue) -> PvMetadata {
        let nt = NormativeType::NTScalar(NTScalar {
            value,
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None,
            control: None,
            value_alarm: None,
        });
        PvMetadata::from_initial_update(name, &nt)
    }

    fn make_full_scalar_meta(name: &str) -> PvMetadata {
        let nt = NormativeType::NTScalar(NTScalar {
            value: ScalarValue::Double(4.2),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: Some(Display {
                description: "Cryo temp".into(),
                units: "K".into(),
                precision: 3,
                form: DisplayForm::Default,
                limit_low: 0.0,
                limit_high: 100.0,
            }),
            control: Some(Control {
                limit_low: 0.0,
                limit_high: 50.0,
                min_step: 0.01,
            }),
            value_alarm: Some(ValueAlarm {
                active: true,
                low_alarm_limit: 3.5,
                low_warning_limit: 4.0,
                high_warning_limit: 5.0,
                high_alarm_limit: 5.5,
                low_alarm_severity: AlarmSeverity::Major,
                low_warning_severity: AlarmSeverity::Minor,
                high_warning_severity: AlarmSeverity::Minor,
                high_alarm_severity: AlarmSeverity::Major,
                hysteresis: 0.1,
            }),
        });
        PvMetadata::from_initial_update(name, &nt)
    }

    #[test]
    fn test_from_core_scalar_full() {
        let meta = make_full_scalar_meta("CRYO:TEMP");
        let stored = StoredMetadata::from_core(&meta);

        assert_eq!(stored.pv_name, "CRYO:TEMP");
        assert_eq!(stored.data_type, "Scalar");
        assert_eq!(stored.scalar_type.as_deref(), Some("double"));
        assert_eq!(stored.units, "K");
        assert_eq!(stored.precision, 3);
        assert_eq!(stored.display_low, 0.0);
        assert_eq!(stored.display_high, 100.0);
        assert_eq!(stored.control_low, 0.0);
        assert_eq!(stored.control_high, 50.0);
        assert_eq!(stored.min_step, 0.01);
        assert_eq!(stored.alarm_lolo, 3.5);
        assert_eq!(stored.alarm_low, 4.0);
        assert_eq!(stored.alarm_high, 5.0);
        assert_eq!(stored.alarm_hihi, 5.5);
        assert_eq!(stored.alarm_hysteresis, 0.1);
        assert!(stored.pv_id.is_none());
    }

    #[test]
    fn test_from_core_minimal() {
        let meta = make_scalar_meta("BARE:PV", ScalarValue::Int(42));
        let stored = StoredMetadata::from_core(&meta);

        assert_eq!(stored.units, "");
        assert_eq!(stored.precision, 0);
        assert_eq!(stored.alarm_hihi, 0.0);
        assert!(stored.array_size.is_none());
        assert!(stored.enum_choices.is_empty());
    }

    #[test]
    fn test_from_core_enum() {
        let nt = NormativeType::NTEnum(NTEnum {
            value: EnumValue {
                index: 0,
                choices: vec!["Off".into(), "On".into(), "Fault".into()],
            },
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
        });
        let meta = PvMetadata::from_initial_update("VALVE:ST", &nt);
        let stored = StoredMetadata::from_core(&meta);

        assert_eq!(stored.enum_choices, vec!["Off", "On", "Fault"]);
        assert!(stored.is_enum());
    }

    #[test]
    fn test_from_core_array() {
        let nt = NormativeType::NTScalarArray(NTScalarArray {
            value: ArrayValue::DoubleArray(vec![1.0, 2.0, 3.0]),
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
            display: None,
            control: None,
            value_alarm: None,
        });
        let meta = PvMetadata::from_initial_update("BPM:X", &nt);
        let stored = StoredMetadata::from_core(&meta);

        assert_eq!(stored.data_type, "Array");
        assert_eq!(stored.array_size, Some(3));
    }

    #[test]
    fn test_from_core_precision_clamped() {
        let mut meta = make_scalar_meta("PV", ScalarValue::Double(0.0));
        meta.precision = -5;
        let stored = StoredMetadata::from_core(&meta);
        assert_eq!(stored.precision, 0);

        meta.precision = 99;
        let stored = StoredMetadata::from_core(&meta);
        assert_eq!(stored.precision, 15);
    }

    #[test]
    fn test_from_core_min_step_clamped() {
        let mut meta = make_scalar_meta("PV", ScalarValue::Double(0.0));
        meta.min_step = -0.5;
        let stored = StoredMetadata::from_core(&meta);
        assert_eq!(stored.min_step, 0.0);
    }

    #[test]
    fn test_from_core_hysteresis_clamped() {
        let mut meta = make_scalar_meta("PV", ScalarValue::Double(0.0));
        meta.alarm_hysteresis = -1.0;
        let stored = StoredMetadata::from_core(&meta);
        assert_eq!(stored.alarm_hysteresis, 0.0);
    }

    #[test]
    fn test_has_alarms() {
        let meta = make_full_scalar_meta("PV");
        let stored = StoredMetadata::from_core(&meta);
        assert!(stored.has_alarms());

        let bare = StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Double(0.0)));
        assert!(!bare.has_alarms());
    }

    #[test]
    fn test_has_display_range() {
        let meta = make_full_scalar_meta("PV");
        let stored = StoredMetadata::from_core(&meta);
        assert!(stored.has_display_range());

        let bare = StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Int(0)));
        assert!(!bare.has_display_range());
    }

    #[test]
    fn test_has_control_range() {
        let meta = make_full_scalar_meta("PV");
        let stored = StoredMetadata::from_core(&meta);
        assert!(stored.has_control_range());
    }

    #[test]
    fn test_is_enum() {
        let stored = StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Int(0)));
        assert!(!stored.is_enum());
    }

    #[test]
    fn test_mem_size() {
        let small = StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Int(0)));
        let large = StoredMetadata::from_core(&make_full_scalar_meta("VERY:LONG:PV:NAME"));
        assert!(large.mem_size() > small.mem_size());
        assert!(small.mem_size() >= 200);
    }

    #[test]
    fn test_stored_display() {
        let meta = make_full_scalar_meta("CRYO:TEMP");
        let stored = StoredMetadata::from_core(&meta);
        let s = stored.to_string();
        assert!(s.contains("CRYO:TEMP"));
        assert!(s.contains("Scalar"));
        assert!(s.contains("K"));
    }

    #[test]
    fn test_stored_display_no_units() {
        let stored = StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Int(0)));
        let s = stored.to_string();
        assert!(s.contains("PV"));
        assert!(!s.contains(",")); // no units suffix
    }

    #[test]
    fn test_stored_clone() {
        let stored = StoredMetadata::from_core(&make_full_scalar_meta("PV"));
        let b = stored.clone();
        assert_eq!(stored.pv_name, b.pv_name);
        assert_eq!(stored.alarm_hihi, b.alarm_hihi);
    }

    #[test]
    fn test_stored_debug() {
        let d = format!(
            "{:?}",
            StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Int(0)))
        );
        assert!(d.contains("StoredMetadata"));
    }

    fn make_row(pv: &str) -> MetadataRow {
        MetadataRow {
            pv_name: pv.into(),
            pv_id: Some(42),
            data_type: "Scalar".into(),
            scalar_type: Some("Double".into()),
            array_size: None,
            description: "test".into(),
            units: "K".into(),
            precision: 3,
            display_form: "Default".into(),
            display_low: 0.0,
            display_high: 100.0,
            control_low: 0.0,
            control_high: 50.0,
            min_step: 0.01,
            alarm_lolo: 3.5,
            alarm_low: 4.0,
            alarm_high: 5.0,
            alarm_hihi: 5.5,
            alarm_hysteresis: 0.1,
            enum_choices: vec![],
            dimensions: json!([]),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_row_into_stored() {
        let stored = make_row("PV:A").into_stored();
        assert_eq!(stored.pv_name, "PV:A");
        assert_eq!(stored.pv_id, Some(42));
        assert_eq!(stored.display_high, 100.0);
        assert_eq!(stored.alarm_lolo, 3.5);
    }

    #[test]
    fn test_row_into_stored_with_enums() {
        let mut row = make_row("E");
        row.enum_choices = vec!["A".into(), "B".into()];
        let stored = row.into_stored();
        assert_eq!(stored.enum_choices, vec!["A", "B"]);
    }

    #[test]
    fn test_row_into_stored_no_scalar_type() {
        let mut row = make_row("PV");
        row.scalar_type = None;
        let stored = row.into_stored();
        assert!(stored.scalar_type.is_none());
    }

    #[test]
    fn test_sql_get() {
        assert!(sql::GET.contains("pv_metadata"));
        assert!(sql::GET.contains("pv_name = $1"));
    }

    #[test]
    fn test_sql_upsert() {
        assert!(sql::UPSERT.contains("ON CONFLICT (pv_name)"));
        assert!(sql::UPSERT.contains("DO UPDATE SET"));
        assert!(sql::UPSERT.contains("updated_at = NOW()"));
    }

    #[test]
    fn test_sql_list() {
        assert!(sql::LIST_ALL.contains("ORDER BY pv_name"));
    }

    #[test]
    fn test_sql_by_type() {
        assert!(sql::GET_BY_TYPE.contains("data_type = $1"));
    }

    #[test]
    fn test_dao_display() {
        assert_eq!(MetadataDao.to_string(), "MetadataDao");
    }

    #[test]
    fn test_dao_debug() {
        assert!(format!("{:?}", MetadataDao).contains("MetadataDao"));
    }
}