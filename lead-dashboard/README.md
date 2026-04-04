# 🌳 Tree Permit Lead Discovery Dashboard

**Live:** https://poornimaramakrishnan.github.io/lead-dashboard/

An internal dashboard for discovering tree-service sales leads from municipal permit portals across South Florida — Miami-Dade County, City of Fort Lauderdale, and City of Miami.

---

## What you'll see

| Feature | Details |
|---|---|
| **Lead List** | AG Grid table sorted by lead score. Double-click a row to see full permit details. |
| **Lead Score** | 1–10. High (green) = tree removal + recent + large parcel. |
| **Filters** | Date range, Jurisdiction, Status (New / Approved / Rejected / Exported). |
| **Bulk actions** | Select rows → Approve / Reject / Export CSV. |
| **Job Health** | Per-source status cards showing last run time, records found/inserted, and any errors. |
| **Demo mode** | When Supabase is not connected, 75 realistic sample leads load automatically. |

---

## Architecture

```
ArcGIS REST APIs          Private Docker Container          Supabase (PostgreSQL)
────────────────   →   ──────────────────────────────   →   ──────────────────────
DERM Permits              pipeline/workers/derm.py           leads table
Fort Lauderdale   →       pipeline/workers/fort_lauderdale.py  job_runs table
City of Miami             pipeline/workers/miami.py
                          (APScheduler — daily 6 AM)
                                    ↓
                          Resend (daily email digest)

                                                    ↓
                             GitHub Pages Dashboard (this repo)
                             Reads from Supabase via JS SDK
```

**Security model:**
- Pipeline source code is **private** (never in this public repo)
- Only the frontend HTML/CSS/JS lives here
- Supabase anon key in `app.js` is safe to expose — Row Level Security controls all access
- Docker image is built privately and run in a secure environment

---

## Connecting to live data (Supabase)

1. Create a project at [supabase.com](https://supabase.com)
2. Run `schema.sql` from the [lead-pipeline repo](https://github.com/poornimaramakrishnan/lead-pipeline) in the Supabase SQL Editor
3. In `app.js`, update `CONFIG`:
   ```js
   const CONFIG = {
       SUPABASE_URL: 'https://your-project.supabase.co',
       SUPABASE_KEY: 'your-anon-public-key',
   };
   ```
4. Commit and push — GitHub Pages redeploys in ~1 minute

---

## Sources

| Source | Jurisdiction | ArcGIS Layer |
|---|---|---|
| DERM Tree Permits | Miami-Dade County | `DermPermits/FeatureServer/0` |
| Building Permit Tracker | City of Fort Lauderdale | `BuildingPermitTracker/MapServer/0` |
| Tree Permits | City of Miami | `Tree_Permits/FeatureServer/0` |
| Building Permits Since 2014 | City of Miami | `Building_Permits_Since_2014/FeatureServer/0` |
