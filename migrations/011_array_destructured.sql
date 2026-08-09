-- 011_array_destructured: Destructured array storage for maximum compression.
--
-- Replaces the old samples_array (values DOUBLE PRECISION[]) with two
-- element-per-row tables optimized for TimescaleDB gorilla/delta compression.
--
-- Compression ratio: ~0.6-1.1 bytes/element vs 8 bytes in the old schema.
-- Write performance: uses the same binary COPY as scalar samples.

-- ═══════════════════════════════════════════════════════════════════════
-- NUMERIC ARRAYS (99% of waveforms: BPMs, ADCs, diagnostics)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_array_num
(
    time     TIMESTAMPTZ      NOT NULL,
    pv_id    INTEGER          NOT NULL,
    array_id BIGINT           NOT NULL, -- monotone per PV, identifies one capture
    idx      INTEGER          NOT NULL, -- element position (0-based)
    value    DOUBLE PRECISION NOT NULL,
    severity SMALLINT         NOT NULL DEFAULT 0,
    status   SMALLINT         NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_array_num', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_array_num_time_brin
    ON samples_array_num USING brin (time)
    WITH (pages_per_range = 32);

ALTER TABLE samples_array_num
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, array_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

-- 2h: destructured rows have 7× larger uncompressed footprint than array columns.
SELECT add_compression_policy('samples_array_num', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ═══════════════════════════════════════════════════════════════════════
-- STRING ARRAYS (rare: PV name lists, enum label arrays)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_array_str
(
    time     TIMESTAMPTZ NOT NULL,
    pv_id    INTEGER     NOT NULL,
    array_id BIGINT      NOT NULL,
    idx      INTEGER     NOT NULL,
    value    TEXT        NOT NULL,
    severity SMALLINT    NOT NULL DEFAULT 0,
    status   SMALLINT    NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_array_str', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_array_str_time_brin
    ON samples_array_str USING brin (time)
    WITH (pages_per_range = 32);

ALTER TABLE samples_array_str
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, array_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

SELECT add_compression_policy('samples_array_str', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ─── Retention (90 days — aligned with the global lossless window) ──
SELECT add_retention_policy('samples_array_num', INTERVAL '90 days',
                            if_not_exists => TRUE);

SELECT add_retention_policy('samples_array_str', INTERVAL '90 days',
                            if_not_exists => TRUE);

-- ═══════════════════════════════════════════════════════════════════════
-- NUMERIC ARRAYS (99% of waveforms: BPMs, ADCs, diagnostics)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_array_num
(
    time     TIMESTAMPTZ      NOT NULL,
    pv_id    INTEGER          NOT NULL,
    array_id BIGINT           NOT NULL, -- monotone per PV, identifies one capture
    idx      INTEGER          NOT NULL, -- element position (0-based)
    value    DOUBLE PRECISION NOT NULL,
    severity SMALLINT         NOT NULL DEFAULT 0,
    status   SMALLINT         NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_array_num', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_array_num_time_brin
    ON samples_array_num USING brin (time)
    WITH (pages_per_range = 32);

-- Compression: segment by pv_id, order by time+array_id+idx.
-- This groups all elements of one waveform capture together,
-- enabling gorilla to exploit element-to-element correlation.
ALTER TABLE samples_array_num
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, array_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

-- 2h: destructured rows have 7× larger uncompressed footprint than array columns.
SELECT add_compression_policy('samples_array_num', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ═══════════════════════════════════════════════════════════════════════
-- STRING ARRAYS (rare: PV name lists, enum label arrays)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS samples_array_str
(
    time     TIMESTAMPTZ NOT NULL,
    pv_id    INTEGER     NOT NULL,
    array_id BIGINT      NOT NULL,
    idx      INTEGER     NOT NULL,
    value    TEXT        NOT NULL,
    severity SMALLINT    NOT NULL DEFAULT 0,
    status   SMALLINT    NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_array_str', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_array_str_time_brin
    ON samples_array_str USING brin (time)
    WITH (pages_per_range = 32);

ALTER TABLE samples_array_str
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC, array_id DESC, idx',
        timescaledb.compress_chunk_time_interval = '24 hours'
        );

SELECT add_compression_policy('samples_array_str', INTERVAL '2 hours',
                              if_not_exists => TRUE);

-- ─── Retention (90 days — aligned with the global lossless window) ──
SELECT add_retention_policy('samples_array_num', INTERVAL '90 days',
                            if_not_exists => TRUE);

SELECT add_retention_policy('samples_array_str', INTERVAL '90 days',
                            if_not_exists => TRUE);