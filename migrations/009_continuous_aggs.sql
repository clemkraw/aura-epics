-- ═══════════════════════════════════════════════════════════════════════
-- 009_continuous_aggs: Materialized continuous aggregates.
-- ═══════════════════════════════════════════════════════════════════════

DO
$$
    BEGIN
        CREATE MATERIALIZED VIEW samples_hourly
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
        SELECT time_bucket('1 hour', time) AS bucket,
               pv_id,
               AVG(value)                  AS avg_value,
               MIN(value)                  AS min_value,
               MAX(value)                  AS max_value,
               STDDEV_SAMP(value)          AS stddev_value,
               COUNT(*)                    AS sample_count,
               FIRST(value, time)          AS first_value,
               LAST(value, time)           AS last_value,
               MAX(severity)               AS max_severity,
               -- Recomposition bricks for the hierarchical daily view.
               SUM(value)                  AS sum_value,
               SUM(value * value)          AS sum_squares
        FROM samples
        GROUP BY bucket, pv_id
        WITH NO DATA;
    EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'samples_hourly already exists, skipping';
    END
$$;

SELECT add_continuous_aggregate_policy('samples_hourly',
                                       start_offset => INTERVAL '3 hours',
                                       end_offset => INTERVAL '1 hour',
                                       schedule_interval => INTERVAL '30 minutes',
                                       if_not_exists => TRUE
       );

-- ─── Daily aggregate (HIERARCHICAL: from samples_hourly) ────────────
DO
$$
    BEGIN
        CREATE MATERIALIZED VIEW samples_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
        SELECT time_bucket('1 day', bucket)                  AS bucket,
               pv_id,
               SUM(sum_value) / NULLIF(SUM(sample_count), 0) AS avg_value,
               MIN(min_value)                                AS min_value,
               MAX(max_value)                                AS max_value,
               SQRT(GREATEST(
                            SUM(sum_squares)
                                - POWER(SUM(sum_value), 2) / NULLIF(SUM(sample_count), 0),
                            0
                    ) / NULLIF(SUM(sample_count) - 1, 0))    AS stddev_value,
               SUM(sample_count)                             AS sample_count,
               FIRST(first_value, bucket)                    AS first_value,
               LAST(last_value, bucket)                      AS last_value,
               MAX(max_severity)                             AS max_severity
        FROM samples_hourly
        GROUP BY time_bucket('1 day', bucket), pv_id
        WITH NO DATA;
    EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'samples_daily already exists, skipping';
    END
$$;

SELECT add_continuous_aggregate_policy('samples_daily',
                                       start_offset => INTERVAL '3 days',
                                       end_offset => INTERVAL '2 hours',
                                       schedule_interval => INTERVAL '1 hour',
                                       if_not_exists => TRUE
       );

-- ─── Indexes ────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_samples_hourly_pv_bucket
    ON samples_hourly (pv_id, bucket DESC);

CREATE INDEX IF NOT EXISTS idx_samples_daily_pv_bucket
    ON samples_daily (pv_id, bucket DESC);

ALTER MATERIALIZED VIEW samples_hourly
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'bucket'
        );
SELECT add_compression_policy('samples_hourly', INTERVAL '7 days',
                              if_not_exists => TRUE);

ALTER MATERIALIZED VIEW samples_daily
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_id',
        timescaledb.compress_orderby = 'bucket'
        );
SELECT add_compression_policy('samples_daily', INTERVAL '30 days',
                              if_not_exists => TRUE);

SELECT add_retention_policy('samples_hourly', INTERVAL '2 years',
                            if_not_exists => TRUE);