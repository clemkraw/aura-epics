-- ═══════════════════════════════════════════════════════════════════════
-- 013_ioc_config: IOC server configuration (pvxs TCP).
-- ═══════════════════════════════════════════════════════════════════════
--
-- Source of truth for which IOCs AURA connects to.
-- Modified via REST API or direct SQL INSERT/DELETE.
-- Changes trigger NOTIFY → AURA reacts in <100ms.

CREATE TABLE IF NOT EXISTS ioc_config
(
    address    TEXT PRIMARY KEY,                -- "IP:PORT" (e.g. "192.168.1.53:5075")
    label      TEXT        NOT NULL DEFAULT '', -- human-readable label (e.g. "cryo-ioc-1")
    enabled    BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Notify AURA on any change (INSERT, UPDATE, DELETE).
CREATE OR REPLACE FUNCTION fn_notify_ioc_changes()
    RETURNS TRIGGER AS
$$
BEGIN
    PERFORM pg_notify('ioc_changes', COALESCE(NEW.address, OLD.address));
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1
                       FROM pg_trigger
                       WHERE tgname = 'trg_ioc_config_notify') THEN
            CREATE TRIGGER trg_ioc_config_notify
                AFTER INSERT OR UPDATE OR DELETE
                ON ioc_config
                FOR EACH ROW
            EXECUTE FUNCTION fn_notify_ioc_changes();
        END IF;
    END
$$;

COMMENT ON TABLE ioc_config IS 'IOC servers to connect to. NOTIFY on change → AURA reacts in real time.';
COMMENT ON COLUMN ioc_config.address IS 'TCP address as IP:PORT. IOC must run pvxs.';