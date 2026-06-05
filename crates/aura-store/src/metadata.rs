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
    pub fn from_core(meta: &PvMetadata) -> Self {
        Self {
            pv_name: meta.pv_name.clone(),
            pv_id: meta.pv_id,
            data_type: format!("{:?}", meta.data_type),
            scalar_type: meta.scalar_type.map(|s| format!("{}", s)),
            array_size: meta.array_size.map(|s| s as i32),
            description: meta.description.clone(),
            units: meta.units.clone(),
            precision: meta.precision.max(0).min(15),
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

    pub fn has_alarms(&self) -> bool {
        self.alarm_lolo != 0.0
            || self.alarm_low != 0.0
            || self.alarm_high != 0.0
            || self.alarm_hihi != 0.0
    }

    pub fn has_display_range(&self) -> bool {
        self.display_low != self.display_high
    }
    pub fn has_control_range(&self) -> bool {
        self.control_low != self.control_high
    }
    pub fn is_enum(&self) -> bool {
        !self.enum_choices.is_empty()
    }

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
            + 200
    }
}

impl fmt::Display for StoredMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}{}{})",
            self.pv_name,
            self.data_type,
            self.scalar_type
                .as_ref()
                .map_or(String::new(), |st| format!("/{st}")),
            if !self.units.is_empty() {
                format!(", {}", self.units)
            } else {
                String::new()
            }
        )
    }
}

/// Internal sqlx row.
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
    fn into_stored(self) -> StoredMetadata {
        StoredMetadata {
            pv_name: self.pv_name,
            pv_id: self.pv_id,
            data_type: self.data_type,
            scalar_type: self.scalar_type,
            array_size: self.array_size,
            description: self.description,
            units: self.units,
            precision: self.precision,
            display_form: self.display_form,
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
            enum_choices: self.enum_choices,
            dimensions: self.dimensions,
            updated_at: self.updated_at,
        }
    }
}

/// Metadata data access object.
pub struct MetadataDao;

impl MetadataDao {
    pub async fn get(pool: &PgPool, pv_name: &str) -> AuraResult<Option<StoredMetadata>> {
        let row = sqlx::query_as::<_, MetadataRow>(sql::GET)
            .bind(pv_name)
            .fetch_optional(pool)
            .await
            .map_err(|e| AuraError::database(format!("get metadata: {e}")))?;
        Ok(row.map(MetadataRow::into_stored))
    }

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

    pub async fn upsert_batch(pool: &PgPool, entries: &[StoredMetadata]) -> AuraResult<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        if entries.len() >= 100 {
            return Self::bulk_insert(pool, entries).await;
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
                    AuraError::database(format!("upsert metadata ({}): {e}", meta.pv_name))
                })?;
        }
        tx.commit()
            .await
            .map_err(|e| AuraError::database(format!("metadata batch commit: {e}")))?;
        Ok(entries.len())
    }

    async fn bulk_insert(pool: &PgPool, entries: &[StoredMetadata]) -> AuraResult<usize> {
        let del_names: Vec<&str> = entries.iter().map(|m| m.pv_name.as_str()).collect();
        sqlx::query("DELETE FROM pv_metadata WHERE pv_name = ANY($1::text[])")
            .bind(&del_names)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("bulk metadata delete: {e}")))?;

        let pv_names: Vec<&str> = entries.iter().map(|m| m.pv_name.as_str()).collect();
        let pv_ids: Vec<Option<i32>> = entries.iter().map(|m| m.pv_id).collect();
        let data_types: Vec<&str> = entries.iter().map(|m| m.data_type.as_str()).collect();
        let scalar_types: Vec<Option<&str>> =
            entries.iter().map(|m| m.scalar_type.as_deref()).collect();
        let array_sizes: Vec<Option<i32>> = entries.iter().map(|m| m.array_size).collect();
        let descriptions: Vec<&str> = entries.iter().map(|m| m.description.as_str()).collect();
        let units: Vec<&str> = entries.iter().map(|m| m.units.as_str()).collect();
        let precisions: Vec<i32> = entries.iter().map(|m| m.precision).collect();
        let display_forms: Vec<&str> = entries.iter().map(|m| m.display_form.as_str()).collect();
        let display_lows: Vec<f64> = entries.iter().map(|m| m.display_low).collect();
        let display_highs: Vec<f64> = entries.iter().map(|m| m.display_high).collect();
        let control_lows: Vec<f64> = entries.iter().map(|m| m.control_low).collect();
        let control_highs: Vec<f64> = entries.iter().map(|m| m.control_high).collect();
        let min_steps: Vec<f64> = entries.iter().map(|m| m.min_step).collect();
        let alarm_lolos: Vec<f64> = entries.iter().map(|m| m.alarm_lolo).collect();
        let alarm_lows: Vec<f64> = entries.iter().map(|m| m.alarm_low).collect();
        let alarm_highs: Vec<f64> = entries.iter().map(|m| m.alarm_high).collect();
        let alarm_hihis: Vec<f64> = entries.iter().map(|m| m.alarm_hihi).collect();
        let alarm_hystereses: Vec<f64> = entries.iter().map(|m| m.alarm_hysteresis).collect();
        let enum_choices_pg: Vec<String> = entries
            .iter()
            .map(|m| {
                if m.enum_choices.is_empty() {
                    "{}".to_string()
                } else {
                    format!(
                        "{{{}}}",
                        m.enum_choices
                            .iter()
                            .map(|s| format!(
                                "\"{}\"",
                                s.replace('\\', "\\\\").replace('"', "\\\"")
                            ))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                }
            })
            .collect();
        let enum_refs: Vec<&str> = enum_choices_pg.iter().map(|s| s.as_str()).collect();
        let dimensions: Vec<serde_json::Value> =
            entries.iter().map(|m| m.dimensions.clone()).collect();

        sqlx::query(
            "INSERT INTO pv_metadata (\
               pv_name, pv_id, data_type, scalar_type, array_size, \
               description, units, precision, display_form, \
               display_low, display_high, control_low, control_high, min_step, \
               alarm_lolo, alarm_low, alarm_high, alarm_hihi, alarm_hysteresis, \
               enum_choices, dimensions, updated_at) \
             SELECT \
               t.pv_name, t.pv_id, t.data_type, t.scalar_type, t.array_size, \
               t.description, t.units, t.precision, t.display_form, \
               t.display_low, t.display_high, t.control_low, t.control_high, t.min_step, \
               t.alarm_lolo, t.alarm_low, t.alarm_high, t.alarm_hihi, t.alarm_hysteresis, \
               t.enum_choices::text[], t.dimensions, NOW() \
             FROM UNNEST(\
               $1::text[], $2::int[], $3::text[], $4::text[], $5::int[], \
               $6::text[], $7::text[], $8::int[], $9::text[], \
               $10::float8[], $11::float8[], $12::float8[], $13::float8[], $14::float8[], \
               $15::float8[], $16::float8[], $17::float8[], $18::float8[], $19::float8[], \
               $20::text[], $21::jsonb[] \
             ) AS t(\
               pv_name, pv_id, data_type, scalar_type, array_size, \
               description, units, precision, display_form, \
               display_low, display_high, control_low, control_high, min_step, \
               alarm_lolo, alarm_low, alarm_high, alarm_hihi, alarm_hysteresis, \
               enum_choices, dimensions)",
        )
        .bind(&pv_names)
        .bind(&pv_ids)
        .bind(&data_types)
        .bind(&scalar_types)
        .bind(&array_sizes)
        .bind(&descriptions)
        .bind(&units)
        .bind(&precisions)
        .bind(&display_forms)
        .bind(&display_lows)
        .bind(&display_highs)
        .bind(&control_lows)
        .bind(&control_highs)
        .bind(&min_steps)
        .bind(&alarm_lolos)
        .bind(&alarm_lows)
        .bind(&alarm_highs)
        .bind(&alarm_hihis)
        .bind(&alarm_hystereses)
        .bind(&enum_refs)
        .bind(&dimensions)
        .execute(pool)
        .await
        .map_err(|e| AuraError::database(format!("bulk metadata insert: {e}")))?;

        Ok(entries.len())
    }

    pub async fn delete(pool: &PgPool, pv_name: &str) -> AuraResult<bool> {
        let result = sqlx::query(sql::DELETE)
            .bind(pv_name)
            .execute(pool)
            .await
            .map_err(|e| AuraError::database(format!("delete metadata: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn list_all(pool: &PgPool) -> AuraResult<Vec<StoredMetadata>> {
        let rows = sqlx::query_as::<_, MetadataRow>(sql::LIST_ALL)
            .fetch_all(pool)
            .await
            .map_err(|e| AuraError::database(format!("list metadata: {e}")))?;
        Ok(rows.into_iter().map(MetadataRow::into_stored).collect())
    }

    pub async fn count(pool: &PgPool) -> AuraResult<i64> {
        let count: Option<i64> = sqlx::query_scalar(sql::COUNT)
            .fetch_one(pool)
            .await
            .map_err(|e| AuraError::database(format!("count metadata: {e}")))?;
        Ok(count.unwrap_or(0))
    }

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
    fn test_from_core_full() {
        let stored = StoredMetadata::from_core(&make_full_scalar_meta("CRYO:TEMP"));
        assert_eq!(stored.pv_name, "CRYO:TEMP");
        assert_eq!(stored.data_type, "Scalar");
        assert_eq!(stored.units, "K");
        assert_eq!(stored.precision, 3);
        assert_eq!(stored.alarm_lolo, 3.5);
        assert_eq!(stored.alarm_hihi, 5.5);
        assert!(stored.has_alarms());
        assert!(stored.has_display_range());
        assert!(stored.has_control_range());
    }

    #[test]
    fn test_from_core_minimal() {
        let stored = StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Int(42)));
        assert_eq!(stored.units, "");
        assert!(!stored.has_alarms());
        assert!(!stored.is_enum());
    }

    #[test]
    fn test_from_core_enum() {
        let nt = NormativeType::NTEnum(NTEnum {
            value: EnumValue {
                index: 0,
                choices: vec!["Off".into(), "On".into()],
            },
            alarm: Alarm::default(),
            timestamp: TimeStamp::new(1000, 0),
        });
        let stored = StoredMetadata::from_core(&PvMetadata::from_initial_update("V", &nt));
        assert!(stored.is_enum());
        assert_eq!(stored.enum_choices, vec!["Off", "On"]);
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
        let stored = StoredMetadata::from_core(&PvMetadata::from_initial_update("BPM", &nt));
        assert_eq!(stored.data_type, "Array");
        assert_eq!(stored.array_size, Some(3));
    }

    #[test]
    fn test_clamping() {
        let mut meta = make_scalar_meta("PV", ScalarValue::Double(0.0));
        meta.precision = -5;
        assert_eq!(StoredMetadata::from_core(&meta).precision, 0);
        meta.precision = 99;
        assert_eq!(StoredMetadata::from_core(&meta).precision, 15);
        meta.min_step = -0.5;
        assert_eq!(StoredMetadata::from_core(&meta).min_step, 0.0);
        meta.alarm_hysteresis = -1.0;
        assert_eq!(StoredMetadata::from_core(&meta).alarm_hysteresis, 0.0);
    }

    #[test]
    fn test_mem_size() {
        let small = StoredMetadata::from_core(&make_scalar_meta("PV", ScalarValue::Int(0)));
        let large = StoredMetadata::from_core(&make_full_scalar_meta("VERY:LONG:NAME"));
        assert!(large.mem_size() > small.mem_size());
        assert!(small.mem_size() >= 200);
    }

    #[test]
    fn test_display() {
        let s = StoredMetadata::from_core(&make_full_scalar_meta("CRYO:TEMP")).to_string();
        assert!(s.contains("CRYO:TEMP") && s.contains("Scalar") && s.contains("K"));
    }

    #[test]
    fn test_row_into_stored() {
        let row = MetadataRow {
            pv_name: "PV:A".into(),
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
        };
        let stored = row.into_stored();
        assert_eq!(stored.pv_name, "PV:A");
        assert_eq!(stored.pv_id, Some(42));
        assert_eq!(stored.alarm_lolo, 3.5);
    }

    #[test]
    fn test_sql() {
        assert!(sql::GET.contains("pv_name = $1"));
        assert!(sql::UPSERT.contains("ON CONFLICT (pv_name)"));
        assert!(sql::LIST_ALL.contains("ORDER BY pv_name"));
        assert!(sql::GET_BY_TYPE.contains("data_type = $1"));
    }

    #[test]
    fn test_dao_display() {
        assert_eq!(MetadataDao.to_string(), "MetadataDao");
    }
}