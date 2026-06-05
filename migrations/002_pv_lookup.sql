-- ═══════════════════════════════════════════════════════════════════════
-- 002_pv_lookup: PV name ↔ numeric ID normalization.
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pv_lookup
(
    pv_id   SERIAL PRIMARY KEY,
    pv_name TEXT NOT NULL,

    CONSTRAINT uq_pv_lookup_name UNIQUE (pv_name)
);

CREATE INDEX IF NOT EXISTS idx_pv_lookup_name
    ON pv_lookup USING hash (pv_name);


COMMENT ON TABLE pv_lookup
    IS 'PV name ↔ numeric ID normalization. Saves ~40 bytes per sample row.';

COMMENT ON COLUMN pv_lookup.pv_id
    IS 'Auto-incremented integer ID used in all sample tables.';

COMMENT ON COLUMN pv_lookup.pv_name
    IS 'Full EPICS PV name as published on the network.';