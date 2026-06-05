-- ═══════════════════════════════════════════════════════════════════════
-- 014_pv_events_status: Lifecycle events + operational status tables.
-- ═══════════════════════════════════════════════════════════════════════
--
-- pv_events:  Timestamped log of every lifecycle transition per PV.
-- pv_status:  Real-time snapshot of every PV's state. Dashboard-ready.

CREATE TABLE IF NOT EXISTS pv_events
(
    event_id   BIGSERIAL,
    pv_name    TEXT        NOT NULL,
    pv_id      INTEGER,
    ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type SMALLINT    NOT NULL,
    -- 0=SUBSCRIBE 1=CONNECTED 2=FIRST_VALUE 3=DISCONNECT
    -- 4=RECONNECT 5=TIMEOUT  6=UNSUBSCRIBE 7=IOC_REMOVED
    ioc_addr   TEXT,
    detail     TEXT
);

SELECT create_hypertable('pv_events', 'ts',
                         chunk_time_interval => INTERVAL '7 days',
                         if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS idx_pv_events_pv_ts
    ON pv_events (pv_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_pv_events_type_ts
    ON pv_events (event_type, ts DESC);

-- Compression after 24h (low-traffic table).
ALTER TABLE pv_events
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'pv_name',
        timescaledb.compress_orderby = 'ts DESC'
        );
SELECT add_compression_policy('pv_events', INTERVAL '24 hours',
                              if_not_exists => TRUE);

-- Retention: 90 days of lifecycle history.
SELECT add_retention_policy('pv_events', INTERVAL '90 days',
                            if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS pv_status
(
    pv_name       TEXT PRIMARY KEY,
    pv_id         INTEGER,
    state         SMALLINT NOT NULL DEFAULT 0,
    -- 0=INIT 1=SEARCHING 2=CONNECTED 3=ARCHIVING 4=DISCONNECTED 5=TIMEOUT
    ioc_addr      TEXT,
    subscribed_at TIMESTAMPTZ,
    connected_at  TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ,
    events_total  BIGINT   NOT NULL DEFAULT 0,
    update_hz     REAL     NOT NULL DEFAULT 0,
    last_value    DOUBLE PRECISION,
    last_severity SMALLINT NOT NULL DEFAULT 0,
    health_score  REAL     NOT NULL DEFAULT 1.0
);
CREATE INDEX IF NOT EXISTS idx_pv_status_state
    ON pv_status (state);