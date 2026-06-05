-- ═══════════════════════════════════════════════════════════════════════
-- 008_retention: Tiered data retention policies.
-- ═══════════════════════════════════════════════════════════════════════
--
-- Strategy:
--   - Raw scalar data:  90 days  (after that, hourly/daily aggregates remain)
--   - Array/waveform:   60 days  (larger rows, aggregates less meaningful)
--   - Image data:       14 days  (very large, keep only recent)
--   - Everything else:  90 days
--   - Aggregates:       forever  (tiny rows, infinite retention)
--
-- Retention drops entire chunks, which is instantaneous (no DELETE scan).
-- TimescaleDB handles this in background workers.
--
-- These defaults are conservative. Override per-table:
--   SELECT remove_retention_policy('samples');
--   SELECT add_retention_policy('samples', INTERVAL '1 year');
--
-- NOTE: Tables created in later migrations (011, 012, 010) have their
-- retention policies in their own migration files.

-- ─── Scalar (90 days raw, aggregates live forever) ──────────────────
SELECT add_retention_policy('samples', INTERVAL '90 days',
                            if_not_exists => TRUE);

SELECT add_retention_policy('samples_string', INTERVAL '90 days',
                            if_not_exists => TRUE);

-- ─── JSONB tables (90 days) ────────────────────────────────────────
SELECT add_retention_policy('samples_table', INTERVAL '90 days',
                            if_not_exists => TRUE);

SELECT add_retention_policy('samples_custom', INTERVAL '90 days',
                            if_not_exists => TRUE);

-- ─── Image data (14 days — largest per-row cost) ───────────────────
SELECT add_retention_policy('samples_image', INTERVAL '14 days',
                            if_not_exists => TRUE);