**Comprehensive Feature List of the Tree Permit Lead Discovery System**  
*(Fully synthesized from Developer Kickoff Brief v2, Implementation Appendix v2, Scope & Overview v2, and Source Validation Checklist – every point extracted and deduplicated. Updated per latest client-developer discussion: Dashboard changed from Lovable to custom Tailwind CSS + AG Grid for full control and easier long-term maintenance; Scheduling & deployment changed from APScheduler on a dedicated server to GitHub Actions + GitHub Pages for zero server management and free reliable operation.)*

### 1. System Overview & Purpose
- Fully automated daily lead discovery system that monitors public permit data sources in Miami-Dade County and Broward County (specifically City of Fort Lauderdale and City of Miami) for tree removal, vegetation removal, and arborist-related permits.
- Converts raw permit records into structured, deduplicated, scored leads.
- Stores leads in a relational database.
- Presents leads in a simple operational **custom dashboard** (Tailwind CSS + AG Grid) for human review, approval/rejection, and export.
- Sends daily email summaries of new leads.
- Primary goal: confirm that public permit records can reliably generate actionable tree-service opportunities with minimal manual effort (expected 5–15 minutes daily operator workflow).
- Success criteria: runs daily without intervention, discovers tree-related permits from all active sources, stores deduplicated leads, displays them in dashboard, supports approval/rejection/export, and sends email summaries.
- Human review is the final safety net; no leads are auto-exported or acted upon without operator approval.
- 4-week pilot scope: exactly the three specified sources (no additional sources, no browser automation, no CRM integration, no map view, no multi-user auth, no advanced analytics/charts, no saved filters, no mobile optimization).

### 2. Data Sources & Ingestion
- **Three public ArcGIS sources** (implemented in strict priority order; do not start next source until previous one is inserting clean records):
  1. **Miami-Dade DERM Tree Permit Dataset** (Priority 1) – Dedicated tree/vegetation permit dataset (gis-mdc.opendata.arcgis.com/datasets/derm-tree-permit). No permit-type filtering required. Public Feature Service, no auth. Date-filtered REST API (confirm exact date field: ISSUEDATE, PERMITDATE, APPDATE, etc.).
  2. **Fort Lauderdale Building Permit Tracker** (Priority 2) – General building permits (gis.fortlauderdale.gov/arcgis/rest/services/BuildingPermitTracker/BuildingPermitTracker/MapServer/0). Filter on PERMITDESC. Confirmed fields include PERMITID, PERMITTYPE, PERMITSTAT, PERMITDESC, SUBMITDT, APPROVEDT, PARCELID, FULLADDR, OWNERNAME, OWNERADDR, CONTRACTOR, CONTRACTPH, ESTCOST. Public MapServer, no auth.
  3. **City of Miami Building Permits Since 2014** (Priority 3) – General building permits (datahub-miamigis.opendata.arcgis.com/datasets/building-permits-since-2014). Filter on permit type/description/WorkItems field. Public Feature Service, no auth.
- **Query strategy**:
  - Daily incremental queries using last successful run timestamp (or 90-day lookback on first run).
  - Explicit outFields (never *), pagination via resultOffset/resultRecordCount (max 1,000 records/page for Fort Lauderdale; continue until fewer records returned).
  - SQL-style WHERE clauses on date field + permit description/keywords.
  - All queries return JSON.
- Each source has independent Python worker.
- Source kill-switch config flags (e.g., MIAMI_DADE_DERM_ACTIVE, FORT_LAUDERDALE_ACTIVE, CITY_OF_MIAMI_ACTIVE) to disable instantly without redeploy.

### 3. Processing Pipeline (Filtering, Deduplication, Scoring, Quality)
- **Classification pipeline** (applied to every record before insert):
  - Step 1 – Exact category match: accept if permit type exactly matches TREE REMOVAL, TREE REMOVAL PERMIT, ARBOR PERMIT, VEGETATION REMOVAL, TREE ALTERATION, RIGHT OF WAY TREE REMOVAL (or Landscape Tree Removal-Relocation Permit). Miami-Dade DERM passes automatically.
  - Step 2 – Description keyword match (if no exact category): accept if description contains tree removal, remove tree, dead tree, dangerous tree, tree mitigation, tree relocation, remove vegetation, vegetation removal, arbor.
  - Step 3 – Negative keyword exclusion: reject if description contains trim, trimming, pruning, landscape irrigation, grass, fertilization, mulch, tree planting (unless combined with removal), christmas tree.
  - Step 4 – Data quality check: reject if missing address/location, permit type/description, or permit date; reject any permit date older than 90 days.
- **Deduplication** (checked before every insert, in order):
  - Rule 1: same permit_number already exists → skip.
  - Rule 2: same normalized address + permit_type + permit_date → skip.
  - Address normalization: uppercase, expand abbreviations (ST→STREET, AVE→AVENUE, BLVD→BOULEVARD), strip leading zeros from street numbers, strip apartment/unit designations.
- **Lead scoring** (computed at insert time, stored in lead_score):
  - Permit type is tree removal or arbor permit → +5
  - Permit type is vegetation removal → +4
  - Permit date within last 7 days → +3
  - Parcel size > 0.5 acre (if available) → +2
  - Right-of-way permit → +1
- **Additional data quality safeguards**:
  - Store normalized address (for dedup) + original raw address (for display).
  - Coordinate validation (if present): reject or flag records outside South Florida bounding box (lat 25.1–26.4, lon –80.9––80.0).
  - Schema-change detection: warn if expected fields missing or field count drops significantly.
  - Log skipped records with reason; include skip count in job run summary. Warn if skip rate >20%.

### 4. Database (Supabase / PostgreSQL)
- **leads table** (exact fields):
  - id (uuid, PK)
  - source_name (text: "miami_dade_derm", "fort_lauderdale", "city_of_miami")
  - jurisdiction (text: County or municipality)
  - address (text: normalized street address)
  - permit_type (text: raw)
  - permit_description (text: full description)
  - permit_number (text: source permit ID)
  - permit_status (text: Issued/Approved/Pending etc.)
  - permit_date (date)
  - owner_name (text)
  - contractor_name (text)
  - contractor_phone (text)
  - source_url (text: direct link to original record)
  - lead_score (integer)
  - lead_status (text: new / approved / rejected / exported)
  - lead_type (text: permit)
  - discovered_at (timestamptz)
  - raw_payload_json (jsonb: full original API response)
- **job_runs table** (exact fields):
  - id (uuid, PK)
  - job_name (text)
  - source_name (text)
  - started_at (timestamptz)
  - finished_at (timestamptz)
  - status (text: success / failed / partial)
  - error_message (text)
  - records_found (integer)
  - records_inserted (integer)
- Full raw payload stored for every lead for reference/auditing.

### 5. Scheduling & Execution
- Daily automated runs handled via **GitHub Actions** scheduled workflows (free, reliable, zero server to manage).
- Recommended run window: starting at 6:00 AM local time (staggered execution for each source).
- Each source worker runs independently.
- GitHub Actions pipeline:
  - Triggers the Python workers
  - Writes results to Supabase
  - Sends daily email summary
- Every run (success or failure) logs full record to job_runs table.

### 6. Resilience & Operational Safeguards
- Retry logic on every API request: max 3 attempts, exponential backoff (2s → 4s → 8s). Retry on timeout/429/500/503; do not retry on 400/403.
- Request throttling: 1–2 second delay between paginated requests.
- All exceptions caught, logged, written to job_runs.error_message.
- Source independence and kill-switch support.
- Schema-change warnings logged.

### 7. Custom Dashboard (Tailwind CSS + AG Grid)
- **Custom-built dashboard** using Tailwind CSS for styling and AG Grid for advanced table functionality. Provides full control over look/feel, sorting, filtering, and exports with no third-party platform dependency. Polished, simple, and optimized for daily use by a non-technical team.
- **Lead List View** (primary/default view):
  - Columns: address, permit type, permit date, jurisdiction, source, lead score, status.
  - Default sort: lead_score DESC, then permit_date DESC.
  - New leads (status="new") visually distinct.
  - Row actions: Approve, Reject.
  - Bulk action: Export selected rows to CSV.
  - Filters (required): date range, jurisdiction, permit type, lead status.
- **Lead Detail View** (click any row):
  - Full permit description.
  - Owner name (where available).
  - Contractor name and phone (where available).
  - Direct source_url link back to original permit record.
  - Raw permit data (collapsible JSON or formatted table).
- **Job Health Panel** (always visible):
  - Per-source rows showing: source name, last run timestamp, run status (success/failed/partial), records_found, records_inserted, last error message.
- Dashboard hosted on GitHub Pages (free tier). Operator simply opens a URL each morning.

### 8. Daily Email Summary & Outputs
- Daily email sent after all workers complete.
- Content: total new leads, breakdown by source, any failed sources with error details.
- Approved leads exportable as CSV from dashboard (includes all relevant lead fields).

### 9. Operational Workflow & Playbook Features Built Into System
- Operator daily routine directly supported: review email → open dashboard → filter to status="new" and today’s date → review details/source links → approve/reject → bulk export approved leads.
- Job Health Panel provides instant visibility into source health without checking logs.
- High-skip-rate or repeated failures surface clearly in job_runs and dashboard.
- System designed for 10–40 new permits/week across sources, 20–40% actionable after review (expected 2–16 exportable leads/week initially).

### 10. Explicitly Out-of-Scope (Not Present in Final Product)
- Property turnover signals / lead_type="property_turnover".
- Broward BCS portal, Hollywood portal, or any additional sources.
- Browser automation / Playwright.
- Map view.
- User authentication or multi-user support.
- Custom reporting, analytics charts, saved filter presets.
- Mobile-optimized layout.
- CRM integration.
- Auto-export or any action beyond dashboard approval + CSV export.

This document contains **every single feature, field, rule, view, column, filter, resilience requirement, database column, scoring signal, filtering step, deduplication rule, quality check, and operational behavior** from the original documents, updated only where the client and developer explicitly agreed on the new technology choices (Tailwind + AG Grid dashboard and GitHub Actions scheduling/hosting). The final software product is exactly this operational lead discovery system.