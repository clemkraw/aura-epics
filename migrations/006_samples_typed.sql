-- ═══════════════════════════════════════════════════════════════════════
-- 006_samples_typed: Per-Normative-Type hypertables.
-- ═══════════════════════════════════════════════════════════════════════
--
-- Each EPICS NT type gets its own hypertable optimized for its data shape.
-- This avoids JSON bloat for structured types and allows type-specific
-- compression and retention policies.
--
-- Routing: PvDataType::table_name() in aura-core maps each NT to its table.

-- ─── Table (NTTable) ────────────────────────────────────────────────
-- Columnar table data stored as JSONB (variable schema per PV).
CREATE TABLE IF NOT EXISTS samples_table
(
    time     TIMESTAMPTZ NOT NULL,
    pv_id    INTEGER     NOT NULL,
    data     JSONB       NOT NULL,
    severity SMALLINT    NOT NULL DEFAULT 0,
    status   SMALLINT    NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_table', 'time',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE
       );



CREATE INDEX IF NOT EXISTS idx_samples_table_pv_brin
    ON samples_table USING brin (pv_id, time)
    WITH (pages_per_range = 32);

-- ─── Image / NDArray (NTNDArray) ────────────────────────────────────
-- Camera/detector frames. Largest data per row.
-- Stored as BYTEA (raw or compressed by the IOC codec).
-- Separate 1-day chunks + 30-day retention (vs 90 for scalars).
CREATE TABLE IF NOT EXISTS samples_image
(
    time              TIMESTAMPTZ NOT NULL,
    pv_id             INTEGER     NOT NULL,
    data              BYTEA       NOT NULL,
    codec             TEXT        NOT NULL DEFAULT '',   -- "", "jpeg", "blosc", "lz4"
    compressed_size   BIGINT      NOT NULL DEFAULT 0,
    uncompressed_size BIGINT      NOT NULL DEFAULT 0,
    dimensions        INTEGER[]   NOT NULL DEFAULT '{}', -- [width, height, ...]
    unique_id         INTEGER     NOT NULL DEFAULT 0,
    attributes        JSONB       NOT NULL DEFAULT '{}',
    severity          SMALLINT    NOT NULL DEFAULT 0,
    status            SMALLINT    NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_image', 'time',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE
       );

-- TOAST tuning: keep small images (< 8KB) inline to avoid TOAST table
-- round-trip. For typical images (> 8KB), behavior is unchanged.
-- No CPU/RAM impact during ingestion — only affects the threshold.
ALTER TABLE samples_image
    SET (toast_tuple_target = 8160);



CREATE INDEX IF NOT EXISTS idx_samples_image_pv_brin
    ON samples_image USING brin (pv_id, time)
    WITH (pages_per_range = 32);

-- ─── Custom / Union (NTUnion, Custom) ───────────────────────────────
-- Catch-all for non-standard structures. Stored as JSONB.
CREATE TABLE IF NOT EXISTS samples_custom
(
    time     TIMESTAMPTZ NOT NULL,
    pv_id    INTEGER     NOT NULL,
    nt_type  TEXT        NOT NULL DEFAULT 'Custom', -- "NTUnion", "Custom"
    data     JSONB       NOT NULL,
    severity SMALLINT    NOT NULL DEFAULT 0,
    status   SMALLINT    NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_custom', 'time',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE
       );



CREATE INDEX IF NOT EXISTS idx_samples_custom_pv_brin
    ON samples_custom USING brin (pv_id, time)
    WITH (pages_per_range = 32);

COMMENT ON TABLE samples_table IS 'Columnar table data stored as JSONB.';
COMMENT ON TABLE samples_image IS 'Camera/detector frames. BYTEA with codec info. 30-day retention.';
COMMENT ON TABLE samples_custom IS 'Non-standard structures stored as JSONB.';