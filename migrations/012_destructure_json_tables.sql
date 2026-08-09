-- 012_destructure_json_tables: Destructure JSON-stored array tables for gorilla compression.
--
-- Replaces PG array columns (float8[], text[], bigint[]) with element-per-row
-- tables, enabling TimescaleDB gorilla/delta compression on individual values.
--
-- Tables affected:
--   samples_namevalue  → samples_nv       (name TEXT + value FLOAT8 per row)
--   samples_histogram  → samples_hist     (range FLOAT8 + count BIGINT per row)
--   samples_continuum  → samples_cont     (base FLOAT8 + trace FLOAT8 per row)
--   samples_multi      → samples_mch      (name TEXT + value FLOAT8 + ch_sev SMALLINT per row)

-- ═══════════════════════════════════════════════════════════════════════
-- NAMEVALUE → samples_nv (destructured)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_nv
(
    time       TIMESTAMPTZ      NOT NULL,
    pv_id      INTEGER          NOT NULL,
    capture_id BIGINT           NOT NULL,
    idx        SMALLINT         NOT NULL,
    name       TEXT             NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    severity   SMALLINT         NOT NULL DEFAULT 0,
    status     SMALLINT         NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_nv', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_nv_time_brin
    ON samples_nv USING brin (time)
    WITH (pages_per_range = 32);

ALTER TABLE samples_nv
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, capture_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

SELECT add_compression_policy('samples_nv', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ═══════════════════════════════════════════════════════════════════════
-- HISTOGRAM → samples_hist (destructured)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_hist
(
    time       TIMESTAMPTZ      NOT NULL,
    pv_id      INTEGER          NOT NULL,
    capture_id BIGINT           NOT NULL,
    idx        SMALLINT         NOT NULL,
    range_val  DOUBLE PRECISION NOT NULL,
    count      BIGINT           NOT NULL,
    severity   SMALLINT         NOT NULL DEFAULT 0,
    status     SMALLINT         NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_hist', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_hist_time_brin
    ON samples_hist USING brin (time)
    WITH (pages_per_range = 32);

ALTER TABLE samples_hist
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, capture_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

SELECT add_compression_policy('samples_hist', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ═══════════════════════════════════════════════════════════════════════
-- CONTINUUM → samples_cont (destructured)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_cont
(
    time       TIMESTAMPTZ      NOT NULL,
    pv_id      INTEGER          NOT NULL,
    capture_id BIGINT           NOT NULL,
    idx        SMALLINT         NOT NULL,
    base_val   DOUBLE PRECISION NOT NULL,
    trace_val  DOUBLE PRECISION NOT NULL,
    severity   SMALLINT         NOT NULL DEFAULT 0,
    status     SMALLINT         NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_cont', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_cont_time_brin
    ON samples_cont USING brin (time)
    WITH (pages_per_range = 32);

ALTER TABLE samples_cont
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, capture_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

SELECT add_compression_policy('samples_cont', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ═══════════════════════════════════════════════════════════════════════
-- MULTI → samples_mch (destructured multi-channel)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_mch
(
    time        TIMESTAMPTZ      NOT NULL,
    pv_id       INTEGER          NOT NULL,
    capture_id  BIGINT           NOT NULL,
    idx         SMALLINT         NOT NULL,
    ch_name     TEXT             NOT NULL,
    ch_value    DOUBLE PRECISION NOT NULL,
    ch_severity SMALLINT         NOT NULL DEFAULT 0,
    severity    SMALLINT         NOT NULL DEFAULT 0,
    status      SMALLINT         NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_mch', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_mch_time_brin
    ON samples_mch USING brin (time)
    WITH (pages_per_range = 32);

ALTER TABLE samples_mch
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, capture_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

SELECT add_compression_policy('samples_mch', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ─── Retention (90 days — aligned with the global lossless window) ──
SELECT add_retention_policy('samples_nv', INTERVAL '90 days',
                            if_not_exists => TRUE);

SELECT add_retention_policy('samples_hist', INTERVAL '90 days',
                            if_not_exists => TRUE);

SELECT add_retention_policy('samples_cont', INTERVAL '90 days',
                            if_not_exists => TRUE);

SELECT add_retention_policy('samples_mch', INTERVAL '90 days',
                            if_not_exists => TRUE);