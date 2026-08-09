-- ═══════════════════════════════════════════════════════════════════════
-- 004_pv_metadata: PV metadata auto-captured from PVA Normative Types.
-- ═══════════════════════════════════════════════════════════════════════
--
-- Extracted from the first PVA monitor response for each PV.
-- Stores display limits (HOPR/LOPR), alarm thresholds (HIHI/HIGH/LOW/LOLO),
-- engineering units (EGU), precision (PREC), enum choices, and NDArray dimensions.
--
-- Written once per PV, updated only when IOC-side metadata changes
-- (rare — e.g., operator changes alarm limits via caput).
-- Grafana reads this for auto-configured axis labels and alarm annotations.

CREATE TABLE IF NOT EXISTS pv_metadata
(
    pv_name          TEXT PRIMARY KEY,
    pv_id            INTEGER          REFERENCES pv_lookup (pv_id) ON DELETE RESTRICT,
    data_type        TEXT             NOT NULL, -- PvDataType: "Scalar", "Array", "Image", etc.
    scalar_type      TEXT,                      -- ScalarType: "Double", "Int", etc.
    array_size       INTEGER,

    -- Display (from NTScalar.display_t)
    description      TEXT             NOT NULL DEFAULT '',
    units            TEXT             NOT NULL DEFAULT '',
    precision        INTEGER          NOT NULL DEFAULT 0,
    display_form     TEXT             NOT NULL DEFAULT 'Default',
    display_low      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    display_high     DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- Control limits (from NTScalar.control_t)
    control_low      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    control_high     DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    min_step         DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- Value alarm thresholds (from NTScalar.valueAlarm_t)
    alarm_lolo       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    alarm_low        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    alarm_high       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    alarm_hihi       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    alarm_hysteresis DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- Enum choices (from NTEnum)
    enum_choices     TEXT[]           NOT NULL DEFAULT '{}',

    -- NDArray dimensions (from NTNDArray)
    dimensions       JSONB            NOT NULL DEFAULT '[]',

    -- Tracking
    first_seen       TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pv_metadata_type
    ON pv_metadata (data_type);

CREATE INDEX IF NOT EXISTS idx_pv_metadata_pv_id
    ON pv_metadata (pv_id) WHERE pv_id IS NOT NULL;

COMMENT ON TABLE pv_metadata IS 'PV metadata auto-captured from the first PVA monitor response. Updated on change.';