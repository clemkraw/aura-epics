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
--   After TimescaleDB compression: ~4-6 bytes per row (10x reduction).
--   1 billion samples ≈ 5 GB compressed.
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

-- Space partition by pv_id: 4 sub-chunks per time chunk.
-- Each sub-chunk holds ~25% of PVs (by hash), improving cache locality
-- for single-PV queries (~30% faster on uncompressed data).
-- Cost: 4× more chunks to manage — transparent on NVMe.
SELECT add_dimension('samples', by_hash('pv_id', 4),
                     if_not_exists => TRUE);

-- BRIN index: near-zero INSERT cost (~0 WAL overhead vs ~100 bytes/row for B-tree).
-- Stores min/max per block range instead of per row → no random I/O.
-- Query cost: ~50ms (vs 1ms B-tree, vs 400ms no index). Good tradeoff for archiving.
CREATE INDEX IF NOT EXISTS idx_samples_pv_brin
    ON samples USING brin (pv_id, time)
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

SELECT add_dimension('samples_string', by_hash('pv_id', 4),
                     if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_samples_string_pv_brin
    ON samples_string USING brin (pv_id, time)
    WITH (pages_per_range = 32);

COMMENT ON TABLE samples IS 'Scalar numeric samples. ~90% of all archived data. 26 bytes/row raw, ~5 bytes compressed.';
COMMENT ON TABLE samples_string IS 'String scalar samples. Separate from numeric for compression efficiency.';
COMMENT ON COLUMN samples.reason IS 'StoreReason: 0=ValueChanged, 1=Heartbeat, 2=Initial, 3=Disconnected, 4=Reconnected, 5=AlarmChange';