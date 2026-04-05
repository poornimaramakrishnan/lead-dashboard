# Tree Permit Lead Discovery — Dashboard

**Live:** https://poornimaramakrishnan.github.io/lead-dashboard/

Operational dashboard for the Tree Permit Lead Discovery System. Displays permit leads sourced from Miami-Dade DERM, Fort Lauderdale, and City of Miami via an automated daily pipeline.

## Features

- **Lead table** — AG Grid with sort, filter, pagination, and global search
- **Approve / Reject / Export** — single-row and bulk actions synced to Supabase
- **Overview charts** — daily timeline, source breakdown, score distribution
- **Map view** — Leaflet pins for all geocoded leads
- **System health** — pipeline job run history and source status
- **Hot Leads panel** — top 10 highest-scored leads at a glance
- **Historical Data tab** — browse leads older than 90 days
- **Dark mode** — persisted via localStorage
- **Authentication** — password-protected write actions (approve/reject/export/email)
- **Email subscriptions** — daily digest opt-in via Cloudflare Worker

## Stack

| Layer | Technology |
|---|---|
| UI | Tailwind CSS, AG Grid Community, Chart.js, Leaflet |
| Data | Supabase (PostgreSQL + PostgREST) |
| Hosting | GitHub Pages |

## Files

| File | Purpose |
|---|---|
| `index.html` | Single-page app shell |
| `app.js` | All dashboard logic — data loading, grid, charts, map, actions |
| `styles.css` | Custom styles and theme variables |

## Local Development

```bash
# Serve locally (no build step needed)
python -m http.server 8000
# Then open http://localhost:8000
```

The dashboard connects to Supabase using the public anon key embedded in `app.js`. Read-only by default; writes (approve/reject/export) are gated by Supabase Row Level Security.

## Related

Pipeline code (private): `github.com/poornimaramakrishnan/lead-pipeline`

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
# Serve the dashboard locally (no build step needed)
cd lead-dashboard && python -m http.server 8000
# Then open http://localhost:8000
```

Pipeline setup instructions are in the private pipeline repository.

## Project Structure

```
lead-dashboard/
  index.html         - Single-page dashboard (Tailwind + AG Grid)
  app.js             - Dashboard logic (filters, actions, charts, auth)
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

## Authentication

Write actions (approve, reject, export, email settings) require login.
The auth endpoint is hosted on a Cloudflare Worker. Credentials are stored
as Cloudflare Worker secrets (`DASHBOARD_USERNAME`, `DASHBOARD_PASSWORD`).
