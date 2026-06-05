-- ═══════════════════════════════════════════════════════════════════════
-- 003_pv_config: PV archiving configuration.
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pv_config
(
    pv_name      TEXT PRIMARY KEY,
    description  TEXT,
    unit         TEXT,
    heartbeat_s  DOUBLE PRECISION NOT NULL DEFAULT 0.0, -- 0 = use global default from aura.toml
    expected_ioc TEXT,                                  -- expected IOC address (for connection routing)
    enabled      BOOLEAN          NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- Auto-update updated_at on any modification.
-- aura-discover uses updated_at for incremental polling.
CREATE OR REPLACE FUNCTION fn_pv_config_set_updated_at()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1
                       FROM pg_trigger
                       WHERE tgname = 'trg_pv_config_updated_at') THEN
            CREATE TRIGGER trg_pv_config_updated_at
                BEFORE UPDATE
                ON pv_config
                FOR EACH ROW
            EXECUTE FUNCTION fn_pv_config_set_updated_at();
        END IF;
    END
$$;

-- Only active PVs (the 99% query).
CREATE INDEX IF NOT EXISTS idx_pv_config_enabled
    ON pv_config (enabled) WHERE enabled = TRUE;

-- Incremental polling: "give me everything changed since last poll".
CREATE INDEX IF NOT EXISTS idx_pv_config_updated
    ON pv_config (updated_at);

COMMENT ON TABLE pv_config IS 'PV archiving configuration. Polled by aura-discover.';
COMMENT ON COLUMN pv_config.heartbeat_s IS 'Max seconds between forced stores. 0 = use global default from aura.toml.';