# Tree Permit Lead Discovery System

Automated lead discovery pipeline that monitors public permit data sources in Miami-Dade County and Broward County for tree removal permits, stores records in Supabase PostgreSQL, and presents them in an operational dashboard.

## Architecture

```
ArcGIS API - Miami-Dade DERM Tree Permits
           ↓
ArcGIS API - Fort Lauderdale Building Permits   →   Python Workers
           ↓                                         (filter / dedupe / insert)
ArcGIS API - City of Miami Permits                       ↓
                                                    Supabase PostgreSQL
                                                         ↓
                                                    Dashboard (Tailwind + AG Grid)
                                                         ↓
                                                    Daily Email Summary (Resend)
```

## Data Sources

| Source | Endpoint | Records | Date Field |
|--------|----------|---------|------------|
| Miami-Dade DERM | DermPermits/FeatureServer/0 (WORK_GROUP='TREE') | 16,002 | ObjectId-based |
| Fort Lauderdale | BuildingPermitTracker/MapServer/0 | 595+ tree/yr | SUBMITDT |
| City of Miami Tree Permits | Tree_Permits/FeatureServer/0 | 6,011 | ReviewStatusChangedDate |
| City of Miami Building Permits | Building_Permits_Since_2014/FeatureServer/0 | 217,646 | IssuedDate |

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy and configure environment
cp .env.example .env
# Edit .env with your Supabase and Resend credentials

# 3. Initialize the database
python -m pipeline.db_init

# 4. Run a test pull
python -m pipeline.main --once

# 5. Start the scheduler
python -m pipeline.main

# 6. Serve the dashboard
cd lead-dashboard && python -m http.server 8000
```

## Project Structure

```
pipeline/
  config.py          - Configuration and environment variables
  arcgis_client.py   - ArcGIS REST API client with retry/pagination
  filters.py         - Permit classification and keyword filtering
  scoring.py         - Lead scoring logic
  dedup.py           - Deduplication and address normalization
  db.py              - Supabase database operations
  db_init.py         - Database schema initialization
  notifications.py   - Resend email daily digest
  main.py            - APScheduler entry point
  backfill.py        - Historical data backfill utility
  workers/
    derm.py          - Source 1: Miami-Dade DERM tree permits
    fort_lauderdale.py - Source 2: Fort Lauderdale building permits
    miami.py         - Source 3: City of Miami permits (tree + building)
lead-dashboard/
  index.html         - Single-page dashboard (Tailwind + AG Grid)
  app.js             - Dashboard logic (filters, actions, charts)
  styles.css         - Custom styles
tests/
  test_filters.py    - Filter logic tests
  test_scoring.py    - Lead scoring tests
  test_dedup.py      - Deduplication tests
  test_db.py         - Database operation tests (mocked)
  test_workers.py    - Worker parsing and execution tests
  test_arcgis_client.py - ArcGIS client tests
  test_notifications.py - Email notification tests
.github/workflows/
  daily_pipeline.yml - GitHub Actions daily pipeline schedule
```
