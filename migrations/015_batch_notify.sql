-- ═══════════════════════════════════════════════════════════════════════
-- 015_batch_notify: Statement-level NOTIFY for bulk PV operations.
-- ═══════════════════════════════════════════════════════════════════════
-- Replaces row-level trigger (1 NOTIFY/row) with statement-level (1 NOTIFY/statement).
-- 50k PV INSERT → 1 NOTIFY with comma-separated payload instead of 50k NOTIFYs.
-- Payload chunked at 7500 chars (PostgreSQL NOTIFY limit is 8000).

DROP TRIGGER IF EXISTS trg_pv_config_notify ON pv_config;

-- INSERT trigger: aggregates new PV names.
CREATE OR REPLACE FUNCTION fn_notify_pv_insert()
    RETURNS TRIGGER AS
$$
DECLARE
    batch TEXT := '';
    pv    TEXT;
BEGIN
    FOR pv IN SELECT pv_name FROM new_table
        LOOP
            IF length(batch) > 0 THEN batch := batch || ','; END IF;
            batch := batch || pv;
            IF length(batch) > 7500 THEN
                PERFORM pg_notify('pv_changes', 'add:' || batch);
                batch := '';
            END IF;
        END LOOP;
    IF length(batch) > 0 THEN
        PERFORM pg_notify('pv_changes', 'add:' || batch);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- DELETE trigger: aggregates removed PV names.
CREATE OR REPLACE FUNCTION fn_notify_pv_delete()
    RETURNS TRIGGER AS
$$
DECLARE
    batch TEXT := '';
    pv    TEXT;
BEGIN
    FOR pv IN SELECT pv_name FROM old_table
        LOOP
            IF length(batch) > 0 THEN batch := batch || ','; END IF;
            batch := batch || pv;
            IF length(batch) > 7500 THEN
                PERFORM pg_notify('pv_changes', 'del:' || batch);
                batch := '';
            END IF;
        END LOOP;
    IF length(batch) > 0 THEN
        PERFORM pg_notify('pv_changes', 'del:' || batch);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Idempotent trigger creation.
DROP TRIGGER IF EXISTS trg_pv_config_insert ON pv_config;
CREATE TRIGGER trg_pv_config_insert
    AFTER INSERT
    ON pv_config
    REFERENCING NEW TABLE AS new_table
    FOR EACH STATEMENT
EXECUTE FUNCTION fn_notify_pv_insert();

DROP TRIGGER IF EXISTS trg_pv_config_delete ON pv_config;
CREATE TRIGGER trg_pv_config_delete
    AFTER DELETE
    ON pv_config
    REFERENCING OLD TABLE AS old_table
    FOR EACH STATEMENT
EXECUTE FUNCTION fn_notify_pv_delete();
