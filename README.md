# 🌳 Tree Permit Lead Discovery — Dashboard# Tree Permit Lead Discovery — Dashboard



> **Live:** [poornimaramakrishnan.github.io/lead-dashboard](https://poornimaramakrishnan.github.io/lead-dashboard/)**Live:** https://poornimaramakrishnan.github.io/lead-dashboard/



Operational dashboard for the Tree Permit Lead Discovery System. DisplaysOperational dashboard for the Tree Permit Lead Discovery System. Displays permit leads sourced from Miami-Dade DERM, Fort Lauderdale, and City of Miami via an automated daily pipeline.

tree-related building permit leads sourced from Miami-Dade DERM, Fort Lauderdale,

and City of Miami via an automated pipeline that runs every 15 minutes.## Features



---- **Lead table** — AG Grid with sort, filter, pagination, and global search

- **Approve / Reject / Export** — single-row and bulk actions synced to Supabase

## Features- **Overview charts** — daily timeline, source breakdown, score distribution

- **Map view** — Leaflet pins for all geocoded leads

| Feature | Description |- **System health** — pipeline job run history and source status

|---------|-------------|- **Hot Leads panel** — top 10 highest-scored leads at a glance

| **Lead Table** | AG Grid with sort, filter, pagination, and global search |- **Historical Data tab** — browse leads older than 90 days

| **Approve / Reject / Export** | Single-row and bulk actions synced to Supabase in real time |- **Dark mode** — persisted via localStorage

| **Overview Charts** | Daily timeline, source breakdown, and score distribution (Chart.js) |- **Authentication** — password-protected write actions (approve/reject/export/email)

| **Map View** | Leaflet map with clustered pins for all geocoded leads |- **Email subscriptions** — daily digest opt-in via Cloudflare Worker

| **System Health** | Pipeline job run history, source status, and error tracking |

| **Hot Leads Panel** | Top 10 highest-scored leads at a glance |## Stack

| **Historical Data** | Browse leads older than 90 days in a dedicated tab |

| **Dark Mode** | One-click toggle, persisted via `localStorage` || Layer | Technology |

| **Authentication** | Password-protected write actions via Cloudflare Worker ||---|---|

| **Email Subscriptions** | Daily digest opt-in — subscribe directly from the dashboard || UI | Tailwind CSS, AG Grid Community, Chart.js, Leaflet |

| Data | Supabase (PostgreSQL + PostgREST) |

---| Hosting | GitHub Pages |



## Technology Stack## Files



| Layer | Technology || File | Purpose |

|-------|------------||---|---|

| **UI Framework** | Tailwind CSS 3.x || `index.html` | Single-page app shell |

| **Data Grid** | AG Grid Community 33.x || `app.js` | All dashboard logic — data loading, grid, charts, map, actions |

| **Charts** | Chart.js 4.x || `styles.css` | Custom styles and theme variables |

| **Maps** | Leaflet 1.9 + OpenStreetMap tiles |

| **Database** | Supabase (PostgreSQL + PostgREST) |## Local Development

| **API Proxy** | Cloudflare Worker (authentication + write operations) |

| **Hosting** | GitHub Pages (static, zero build step) |```bash

# Serve locally (no build step needed)

---python -m http.server 8000

# Then open http://localhost:8000

## Files```



| File | Lines | Purpose |The dashboard connects to Supabase using the public anon key embedded in `app.js`. Read-only by default; writes (approve/reject/export) are gated by Supabase Row Level Security.

|------|-------|---------|

| `index.html` | ~500 | Single-page application shell — Tailwind layout, modals, tab structure |## Related

| `app.js` | ~1,800 | All dashboard logic — data loading, grids, charts, map, actions, auth |

| `styles.css` | ~800 | Custom styles, dark mode variables, responsive overrides |Pipeline code (private): `github.com/poornimaramakrishnan/lead-pipeline`

| `.nojekyll` | — | Prevents GitHub Pages from processing files through Jekyll |

## Architecture

---

```

## Local DevelopmentArcGIS API - Miami-Dade DERM Tree Permits

           ↓

No build tools, bundlers, or `npm install` required — pure static HTML/JS/CSS.ArcGIS API - Fort Lauderdale Building Permits   →   Python Workers

           ↓                                         (filter / dedupe / insert)

```bashArcGIS API - City of Miami Permits                       ↓

# Option 1: Python                                                    Supabase PostgreSQL

python -m http.server 8000                                                         ↓

                                                    Dashboard (Tailwind + AG Grid)

# Option 2: Node.js                                                         ↓

npx serve .                                                    Daily Email Summary (Resend)

```

# Then open http://localhost:8000

```## Data Sources



The dashboard connects to Supabase using the public anon key (read-only by default).| Source | Endpoint | Records | Date Field |

Write operations (approve, reject, export, email subscribe) are routed through|--------|----------|---------|------------|

the Cloudflare Worker and gated by authentication + Supabase Row Level Security.| Miami-Dade DERM | DermPermits/FeatureServer/0 (WORK_GROUP='TREE') | 16,002 | ObjectId-based |

| Fort Lauderdale | BuildingPermitTracker/MapServer/0 | 595+ tree/yr | SUBMITDT |

---| City of Miami Tree Permits | Tree_Permits/FeatureServer/0 | 6,011 | ReviewStatusChangedDate |

| City of Miami Building Permits | Building_Permits_Since_2014/FeatureServer/0 | 217,646 | IssuedDate |

## Architecture

## Quick Start

```

  Browser (GitHub Pages)```bash

      │# Serve the dashboard locally (no build step needed)

      ├── Read operations (leads, job_runs, stats)cd lead-dashboard && python -m http.server 8000

      │   └── Supabase PostgREST (anon key, RLS: SELECT only)# Then open http://localhost:8000

      │```

      └── Write operations (approve, reject, subscribe, email)

          └── Cloudflare Worker (/api/*)Pipeline setup instructions are in the private pipeline repository.

              ├── /api/auth        → validate username/password

              ├── /api/review      → PATCH lead status in Supabase## Project Structure

              ├── /api/subscribe   → upsert email subscriber

              └── /api/send-report → dispatch GitHub Actions workflow```

```lead-dashboard/

  index.html         - Single-page dashboard (Tailwind + AG Grid)

---  app.js             - Dashboard logic (filters, actions, charts, auth)

  styles.css         - Custom styles

## Relatedtests/

  test_filters.py    - Filter logic tests

| Repository | Visibility | Description |  test_scoring.py    - Lead scoring tests

|------------|------------|-------------|  test_dedup.py      - Deduplication tests

| [lead-pipeline](https://github.com/poornimaramakrishnan/lead-pipeline) | Private | Python pipeline, workers, tests, GitHub Actions |  test_db.py         - Database operation tests (mocked)

| **lead-dashboard** (this repo) | Public | Static dashboard hosted on GitHub Pages |  test_workers.py    - Worker parsing and execution tests

  test_arcgis_client.py - ArcGIS client tests

---  test_notifications.py - Email notification tests

.github/workflows/

## License  daily_pipeline.yml - GitHub Actions daily pipeline schedule

```

MIT

## Authentication

Write actions (approve, reject, export, email settings) require login.
The auth endpoint is hosted on a Cloudflare Worker. Credentials are stored
as Cloudflare Worker secrets (`DASHBOARD_USERNAME`, `DASHBOARD_PASSWORD`).
