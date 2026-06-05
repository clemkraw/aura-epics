-- ═══════════════════════════════════════════════════════════════════════
-- 009_continuous_aggs: Materialized continuous aggregates.
-- ═══════════════════════════════════════════════════════════════════════

-- ─── Hourly aggregate ───────────────────────────────────────────────
DO
$$
    BEGIN
        CREATE MATERIALIZED VIEW samples_hourly
            WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', time) AS bucket,
               pv_id,
               AVG(value)                  AS avg_value,
               MIN(value)                  AS min_value,
               MAX(value)                  AS max_value,
               STDDEV_SAMP(value)          AS stddev_value,
               COUNT(*)                    AS sample_count,
               FIRST(value, time)          AS first_value,
               LAST(value, time)           AS last_value,
               MAX(severity)               AS max_severity
        FROM samples
        GROUP BY bucket, pv_id
        WITH NO DATA;
    EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'samples_hourly already exists, skipping';
    END
$$;

SELECT add_continuous_aggregate_policy('samples_hourly',
                                       start_offset => INTERVAL '5 hours',
                                       end_offset => INTERVAL '1 hour',
                                       schedule_interval => INTERVAL '1 hour',
                                       if_not_exists => TRUE
       );

-- ─── Daily aggregate ────────────────────────────────────────────────
DO
$$
    BEGIN
        CREATE MATERIALIZED VIEW samples_daily
            WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 day', time) AS bucket,
               pv_id,
               AVG(value)                 AS avg_value,
               MIN(value)                 AS min_value,
               MAX(value)                 AS max_value,
               STDDEV_SAMP(value)         AS stddev_value,
               COUNT(*)                   AS sample_count,
               FIRST(value, time)         AS first_value,
               LAST(value, time)          AS last_value,
               MAX(severity)              AS max_severity
        FROM samples
        GROUP BY bucket, pv_id
        WITH NO DATA;
    EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'samples_daily already exists, skipping';
    END
$$;

SELECT add_continuous_aggregate_policy('samples_daily',
                                       start_offset => INTERVAL '3 days',
                                       end_offset => INTERVAL '1 hour',
                                       schedule_interval => INTERVAL '1 hour',
                                       if_not_exists => TRUE
       );

-- ─── Indexes ────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_samples_hourly_pv_bucket
    ON samples_hourly (pv_id, bucket DESC);

CREATE INDEX IF NOT EXISTS idx_samples_daily_pv_bucket
    ON samples_daily (pv_id, bucket DESC);