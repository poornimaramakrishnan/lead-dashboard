"""Database schema initialization for Supabase.

Run this once to create the leads and job_runs tables.
Usage:
    python -m pipeline.db_init --print    Print SQL schema
    python -m pipeline.db_init --verify   Verify tables exist
"""
import sys
import logging
from pipeline.db import get_client

logger = logging.getLogger(__name__)

# SQL statements to create tables
# These should be run via the Supabase SQL editor or psql
# This matches lead-pipeline/schema.sql v2 (2026-04-04)
SCHEMA_SQL = """
-- ═══════════════════════════════════════════════════════════════════════════════
-- Tree Permit Lead Discovery — Production Schema v2
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS moddatetime;

-- ─── Leads ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS leads (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name         TEXT        NOT NULL,
    jurisdiction        TEXT        NOT NULL DEFAULT 'Unknown',
    address             TEXT        NOT NULL DEFAULT '',
    normalized_address  TEXT        NOT NULL DEFAULT '',
    permit_type         TEXT,
    permit_description  TEXT,
    permit_number       TEXT,
    permit_status       TEXT,
    permit_date         DATE,
    owner_name          TEXT,
    contractor_name     TEXT,
    contractor_phone    TEXT,
    source_url          TEXT,
    job_run_id          UUID,
    lead_score          INTEGER     NOT NULL DEFAULT 0 CHECK (lead_score >= 0 AND lead_score <= 15),
    lead_status         TEXT        NOT NULL DEFAULT 'new'
                                    CHECK (lead_status IN ('new','approved','rejected','exported')),
    lead_type           TEXT        NOT NULL DEFAULT 'permit',
    lead_notes          TEXT,
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload_json    JSONB,
    CONSTRAINT uq_leads_source_permit UNIQUE (source_name, permit_number)
);

CREATE INDEX IF NOT EXISTS idx_leads_source         ON leads (source_name);
CREATE INDEX IF NOT EXISTS idx_leads_jurisdiction    ON leads (jurisdiction);
CREATE INDEX IF NOT EXISTS idx_leads_permit_date     ON leads (permit_date DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_leads_score           ON leads (lead_score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_discovered      ON leads (discovered_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_new             ON leads (lead_score DESC, discovered_at DESC) WHERE lead_status = 'new';
CREATE INDEX IF NOT EXISTS idx_leads_approved         ON leads (discovered_at DESC) WHERE lead_status = 'approved';
CREATE UNIQUE INDEX IF NOT EXISTS uq_leads_addr_type_date
    ON leads (normalized_address, permit_type, permit_date)
    WHERE normalized_address != '' AND permit_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_address_trgm    ON leads USING GIN (address gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_leads_raw_payload     ON leads USING GIN (raw_payload_json jsonb_path_ops);

-- ─── Job Runs ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_runs (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_name            TEXT        NOT NULL,
    source_name         TEXT        NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'running'
                                    CHECK (status IN ('running','success','failed','partial')),
    error_message       TEXT,
    records_found       INTEGER     NOT NULL DEFAULT 0 CHECK (records_found >= 0),
    records_inserted    INTEGER     NOT NULL DEFAULT 0 CHECK (records_inserted >= 0),
    records_skipped     INTEGER     NOT NULL DEFAULT 0 CHECK (records_skipped >= 0),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_runs_source  ON job_runs (source_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_runs_status  ON job_runs (status) WHERE status IN ('running','failed');
CREATE INDEX IF NOT EXISTS idx_job_runs_started ON job_runs (started_at DESC);

-- FK: leads.job_run_id → job_runs.id
ALTER TABLE leads
    ADD CONSTRAINT fk_leads_job_run
    FOREIGN KEY (job_run_id) REFERENCES job_runs(id)
    ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- ─── Auto-update triggers ────────────────────────────────────────────────────
CREATE OR REPLACE TRIGGER trg_leads_updated_at
    BEFORE UPDATE ON leads FOR EACH ROW EXECUTE FUNCTION moddatetime(updated_at);

CREATE OR REPLACE TRIGGER trg_job_runs_updated_at
    BEFORE UPDATE ON job_runs FOR EACH ROW EXECUTE FUNCTION moddatetime(updated_at);

-- ─── Row Level Security ──────────────────────────────────────────────────────
ALTER TABLE leads    ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_runs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_read_leads"           ON leads    FOR SELECT USING (true);
CREATE POLICY "anon_read_job_runs"        ON job_runs FOR SELECT USING (true);
CREATE POLICY "anon_update_lead_status"   ON leads    FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "service_all_leads"         ON leads    FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY "service_all_job_runs"      ON job_runs FOR ALL TO service_role USING (true) WITH CHECK (true);

-- ─── Helper Views ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW lead_stats AS
SELECT
    COUNT(*)                                         AS total_leads,
    COUNT(*) FILTER (WHERE lead_status = 'new')      AS new_leads,
    COUNT(*) FILTER (WHERE lead_status = 'approved') AS approved_leads,
    COUNT(*) FILTER (WHERE lead_status = 'rejected') AS rejected_leads,
    COUNT(*) FILTER (WHERE lead_status = 'exported') AS exported_leads,
    COUNT(DISTINCT source_name)                      AS active_sources,
    MIN(permit_date)                                 AS earliest_permit,
    MAX(permit_date)                                 AS latest_permit,
    MAX(discovered_at)                               AS last_discovery
FROM leads;

CREATE OR REPLACE VIEW latest_job_runs AS
SELECT DISTINCT ON (source_name)
    id, job_name, source_name, started_at, finished_at,
    status, error_message, records_found, records_inserted, records_skipped
FROM job_runs
ORDER BY source_name, started_at DESC;
"""


def print_schema():
    """Print the SQL schema for manual execution in Supabase SQL editor."""
    print("=" * 60)
    print("Copy and paste the following SQL into your Supabase SQL Editor:")
    print("=" * 60)
    print(SCHEMA_SQL)
    print("=" * 60)
    print("After running the SQL, your database is ready.")


def verify_tables():
    """Verify that the tables exist by querying them."""
    try:
        client = get_client()
        # Try to query leads table
        result = client.table("leads").select("id").limit(1).execute()
        print("✓ leads table exists")

        # Try to query job_runs table
        result = client.table("job_runs").select("id").limit(1).execute()
        print("✓ job_runs table exists")

        print("\nDatabase is ready!")
        return True
    except Exception as exc:
        print(f"✗ Database verification failed: {exc}")
        print("\nPlease run the schema SQL first. Use: python -m pipeline.db_init --print")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if "--print" in sys.argv or "--schema" in sys.argv:
        print_schema()
    elif "--verify" in sys.argv:
        verify_tables()
    else:
        print("Usage:")
        print("  python -m pipeline.db_init --print    Print SQL schema")
        print("  python -m pipeline.db_init --verify   Verify tables exist")
