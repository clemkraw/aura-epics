-- ═══════════════════════════════════════════════════════════════════════
-- 001_extensions: Enable required PostgreSQL extensions.
-- ═══════════════════════════════════════════════════════════════════════

CREATE
    EXTENSION IF NOT EXISTS timescaledb CASCADE;

DO
$$
    BEGIN
        CREATE
            EXTENSION IF NOT EXISTS pg_stat_statements;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'pg_stat_statements not available, skipping';
    END
$$;

DO
$$
    BEGIN
        PERFORM
            set_config('timescaledb.max_background_workers', '8', false);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'could not set timescaledb.max_background_workers, using default';
    END
$$;