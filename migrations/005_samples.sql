-- ═══════════════════════════════════════════════════════════════════════
-- 005_samples: Core scalar hypertable + string hypertable.
-- ═══════════════════════════════════════════════════════════════════════
--
-- This is the highest-traffic table (~90% of all writes).
-- Stores NTScalar (numeric), NTEnum (index as f64), NTAggregate.
--
-- Storage optimization:
--   Row: time(8) + pv_id(4) + value(8) + severity(2) + status(2) + reason(2) = 26 bytes
--   With overhead: ~40 bytes per row uncompressed.
--
-- Chunk strategy: 1-hour chunks.
--   - At 100k inserts/s, each chunk holds a fraction of the day → good parallelism for compression.
--   - 1-hour chunks give fast COPY, good compression ratio, and efficient retention drops.

CREATE TABLE IF NOT EXISTS samples
(
    time     TIMESTAMPTZ      NOT NULL,
    pv_id    INTEGER          NOT NULL,
    value    DOUBLE PRECISION NOT NULL,
    severity SMALLINT         NOT NULL DEFAULT 0,
    status   SMALLINT         NOT NULL DEFAULT 0,
    reason   SMALLINT         NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples', 'time',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_samples_time_brin
    ON samples USING brin (time)
    WITH (pages_per_range = 32);

-- ─── String scalars ──────────────────────────────────────────────────
-- Separate table because TEXT values can't be compressed as efficiently
-- as DOUBLE PRECISION, and they shouldn't pollute the numeric table's
-- compression ratio.

CREATE TABLE IF NOT EXISTS samples_string
(
    time     TIMESTAMPTZ NOT NULL,
    pv_id    INTEGER     NOT NULL,
    value    TEXT        NOT NULL,
    severity SMALLINT    NOT NULL DEFAULT 0,
    status   SMALLINT    NOT NULL DEFAULT 0
);

SELECT create_hypertable('samples_string', 'time',
                         chunk_time_interval => INTERVAL '1 hour',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_samples_string_pv_time
    ON samples_string (pv_id, time DESC);

COMMENT ON TABLE samples IS 'Scalar numeric samples. ~90% of all archived data. 26 bytes/row raw payload, ~1-2 bytes/row compressed (24 h roll-up).';
COMMENT ON TABLE samples_string IS 'String scalar samples. Separate from numeric for compression efficiency.';
COMMENT ON COLUMN samples.reason IS 'StoreReason: 0=ValueChanged, 1=Heartbeat, 2=Initial, 3=Disconnected, 4=Reconnected, 5=AlarmChange';