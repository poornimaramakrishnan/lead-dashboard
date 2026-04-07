# 🌳 Tree Permit Lead Discovery — Dashboard

Operational dashboard for the Tree Permit Lead Discovery System. 
Displays tree-related building permit leads sourced from Miami-Dade DERM, Fort Lauderdale, and City of Miami via an automated pipeline.

> **Live Dashboard:** [https://sawfleet2026-eng.github.io/Tree-Permit-Lead-Dashboard/](https://sawfleet2026-eng.github.io/Tree-Permit-Lead-Dashboard/)

---

## Features

- **Lead Table** — AG Grid with sort, filter, pagination, and global search
- **Approve / Reject / Export** — Single-row and bulk actions synced to Supabase in real time
- **Overview Charts** — Daily timeline, source breakdown, and score distribution (Chart.js)
- **Map View** — Leaflet map with clustered pins for all geocoded leads
- **System Health** — Pipeline job run history, source status, and error tracking
- **Hot Leads Panel** — Top 10 highest-scored leads at a glance
- **Historical Data** — Browse leads older than 90 days in a dedicated tab
- **Email Subscriptions** — Daily digest opt-in directly from the dashboard
- **Dark Mode** — One-click toggle, persisted via `localStorage`
- **Authentication** — Password-protected write actions (approve/reject/export/email)

---

## Technology Stack

| Layer | Technology |
|---|---|
| **UI Framework** | Tailwind CSS 3.x |
| **Data Grid** | AG Grid Community 33.x |
| **Charts** | Chart.js 4.x |
| **Maps** | Leaflet 1.9 + OpenStreetMap tiles |
| **Database** | Supabase (PostgreSQL + PostgREST) |
| **API Proxy** | Cloudflare Worker (authentication + write operations) |
| **Hosting** | GitHub Pages (static, zero build step) |

---

## Architecture

The dashboard connects to Supabase using the public anon key (read-only by default).
Write operations (approve, reject, export, email subscribe) are routed through the Cloudflare Worker and gated by authentication + Supabase Row Level Security.
