-- ═══════════════════════════════════════════════════════════════════════
-- 007_compression: TimescaleDB compression policies.
-- ═══════════════════════════════════════════════════════════════════════
--
-- Compression reduces storage by 10-20x for numeric data:
--   - gorilla encoding for DOUBLE PRECISION (delta-of-delta + XOR)
--   - dictionary encoding for low-cardinality INTEGER (pv_id, severity)
--   - LZ4 for BYTEA and TEXT columns
--
-- Strategy:
--   - segmentby = pv_id → each PV compressed independently
--   - orderby = time DESC → optimal for "latest N samples" queries
--   - compress_after = 4 hours → avoids I/O competition with live ingestion
--
-- Expected compression ratios:
--   samples:          10-20x (repetitive float values)
--   samples_string:    3-5x  (LZ4 on text)
--   samples_image:     1-2x  (already codec-compressed by IOC)
--   samples_table:     2-3x  (LZ4 on JSONB)
--   samples_custom:    2-3x  (LZ4 on JSONB)
--
-- Note: destructured tables (samples_array_num, samples_nv, samples_hist,
-- samples_cont, samples_mch) have their own compression in 011/012.

-- ─── Scalar samples (90% of traffic) ────────────────────────────────
ALTER TABLE samples
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC'
        );
SELECT add_compression_policy('samples', INTERVAL '4 hours',
                              if_not_exists => TRUE);

-- ─── String samples ─────────────────────────────────────────────────
ALTER TABLE samples_string
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC'
        );
SELECT add_compression_policy('samples_string', INTERVAL '4 hours',
                              if_not_exists => TRUE);

-- ─── Table samples (JSONB) ─────────────────────────────────────────
ALTER TABLE samples_table
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC'
        );
SELECT add_compression_policy('samples_table', INTERVAL '4 hours',
                              if_not_exists => TRUE);

-- ─── Image samples (BYTEA — already IOC-compressed) ────────────────
ALTER TABLE samples_image
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC'
        );
SELECT add_compression_policy('samples_image', INTERVAL '8 hours',
                              if_not_exists => TRUE);

-- ─── Custom / Union samples ────────────────────────────────────────
ALTER TABLE samples_custom
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'time DESC'
        );
SELECT add_compression_policy('samples_custom', INTERVAL '4 hours',
                              if_not_exists => TRUE);