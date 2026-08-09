-- ═══════════════════════════════════════════════════════════════════════
-- 008_retention: Tiered data retention policies.
-- ═══════════════════════════════════════════════════════════════════════
--
-- Strategy:
--   - ALL raw data:     90 days, lossless and queryable (project requirement:
--                        same precision, same point count as ingested)
--   - Aggregates:       hourly 2 years, daily forever (see 009)
--
-- Disk budget reminder: at a sustained S ev/s, 90 days of scalars ≈
--   S × 7.78e6 rows, ~1.5-3 B/row after roll-up compression.
--   300k ev/s → ~2.3e12 rows → ~4-7 TB; 1M ev/s sustained → ~12-23 TB.
--   Images are the wildcard: budget them separately from `compressed_size`.
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

-- ─── Image data (90 days, aligned with the lossless-window requirement;
--      largest per-row cost, monitor disk usage) ───────────────────────
SELECT add_retention_policy('samples_image', INTERVAL '90 days',
                            if_not_exists => TRUE);