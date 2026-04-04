"""Historical data backfill for the Tree Permit Lead Discovery pipeline.

Pulls ALL available records from all 4 ArcGIS sources (not just the last 90 days)
and inserts them into Supabase with deduplication, filtering, and scoring.

Usage:
    python -m pipeline.backfill                     # Backfill all sources
    python -m pipeline.backfill --source derm        # Single source
    python -m pipeline.backfill --source fl
    python -m pipeline.backfill --source miami_tree
    python -m pipeline.backfill --source miami_bldg
    python -m pipeline.backfill --dry-run            # Count without inserting

Expected volumes:
    DERM Tree Permits:        ~16,000 records (all are tree permits)
    Fort Lauderdale:          ~2,000 tree/vegetation permits (filtered from ~200K)
    Miami Tree Permits:       ~6,000 records (all are tree permits)
    Miami Building Permits:   ~500 tree-related permits (filtered from ~217K)
    ─────────────────────────────────────────────
    Estimated total:          ~24,500 leads
"""
import sys
import time
import logging
import argparse
from datetime import datetime, timezone

from pipeline.config import (
    DERM_PERMITS_URL,
    FORT_LAUDERDALE_URL,
    MIAMI_TREE_PERMITS_URL,
    MIAMI_BUILDING_PERMITS_URL,
)
from pipeline.arcgis_client import stream_pages, get_record_count
from pipeline.filters import classify_permit
from pipeline.scoring import compute_lead_score
from pipeline.dedup import (
    normalize_address,
    is_duplicate_by_permit_number,
    is_duplicate_by_address_date,
    make_address_dedup_key,
)
from pipeline.db import (
    insert_leads_batch,
    get_existing_permit_numbers,
    get_existing_address_keys,
    create_job_run,
    complete_job_run,
)
from pipeline.workers.derm import parse_derm_record, OUT_FIELDS as DERM_FIELDS
from pipeline.workers.fort_lauderdale import parse_fl_record, OUT_FIELDS as FL_FIELDS
from pipeline.workers.miami import (
    parse_miami_tree_record,
    parse_miami_building_record,
    TREE_OUT_FIELDS,
    BUILDING_OUT_FIELDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")

BATCH_SIZE = 50


# ═══════════════════════════════════════════════════════════════════════════════
# Backfill functions per source
# ═══════════════════════════════════════════════════════════════════════════════

def backfill_derm(dry_run: bool = False) -> dict:
    """Backfill ALL DERM tree permits (WORK_GROUP='TREE')."""
    source = "miami_dade_derm"
    where = "WORK_GROUP = 'TREE'"

    total = get_record_count(DERM_PERMITS_URL, where)
    logger.info("═══ DERM: %d total tree permits available ═══", total)

    if dry_run:
        return {"source": source, "available": total, "dry_run": True}

    run_id = create_job_run("backfill_derm", source)
    existing_permits = get_existing_permit_numbers(source)
    existing_keys = get_existing_address_keys(source)
    logger.info("  Existing: %d permits, %d address keys", len(existing_permits), len(existing_keys))

    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}
    batch = []

    for page in stream_pages(DERM_PERMITS_URL, where=where, out_fields=DERM_FIELDS, order_by="ObjectId ASC"):
        for attrs in page:
            stats["found"] += 1
            pn = attrs.get("PERMIT_NUMBER")

            if pn and is_duplicate_by_permit_number(pn, existing_permits):
                stats["skipped"] += 1
                continue

            lead = parse_derm_record(attrs)
            accepted, _ = classify_permit(lead["permit_type"], lead["permit_description"], lead["address"],
                                          datetime.now(timezone.utc), is_dedicated_tree_source=True, max_age_days=99999)
            if not accepted:
                stats["skipped"] += 1
                continue

            if lead["normalized_address"] and lead["permit_date"]:
                key = make_address_dedup_key(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"])
                if is_duplicate_by_address_date(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"], existing_keys):
                    stats["skipped"] += 1
                    continue
                existing_keys.add(key)

            lead["lead_score"] = compute_lead_score(lead["permit_type"], lead["permit_description"], datetime.now(timezone.utc))
            batch.append(lead)
            if pn:
                existing_permits.add(pn)

            if len(batch) >= BATCH_SIZE:
                inserted = insert_leads_batch(batch, job_run_id=run_id)
                stats["inserted"] += inserted
                batch = []
                _progress(stats, total)

    if batch:
        stats["inserted"] += insert_leads_batch(batch, job_run_id=run_id)

    complete_job_run(run_id, "success", stats["found"], stats["inserted"], stats["skipped"])
    logger.info("  ✅ DERM done: found=%d inserted=%d skipped=%d", stats["found"], stats["inserted"], stats["skipped"])
    return stats


def backfill_fort_lauderdale(dry_run: bool = False) -> dict:
    """Backfill ALL Fort Lauderdale tree/vegetation permits."""
    source = "fort_lauderdale"
    where = (
        "PERMITDESC LIKE '%tree%' OR PERMITDESC LIKE '%Tree%' OR "
        "PERMITDESC LIKE '%TREE%' OR PERMITDESC LIKE '%vegetation%' OR "
        "PERMITDESC LIKE '%Vegetation%' OR PERMITDESC LIKE '%arbor%' OR "
        "PERMITDESC LIKE '%Arbor%'"
    )

    total = get_record_count(FORT_LAUDERDALE_URL, where)
    logger.info("═══ Fort Lauderdale: %d tree permits available ═══", total)

    if dry_run:
        return {"source": source, "available": total, "dry_run": True}

    run_id = create_job_run("backfill_fort_lauderdale", source)
    existing_permits = get_existing_permit_numbers(source)
    existing_keys = get_existing_address_keys(source)
    logger.info("  Existing: %d permits, %d address keys", len(existing_permits), len(existing_keys))

    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}
    batch = []

    for page in stream_pages(FORT_LAUDERDALE_URL, where=where, out_fields=FL_FIELDS, order_by="SUBMITDT DESC"):
        for attrs in page:
            stats["found"] += 1
            pid = (attrs.get("PERMITID") or "").strip()

            if pid and is_duplicate_by_permit_number(pid, existing_permits):
                stats["skipped"] += 1
                continue

            lead = parse_fl_record(attrs)
            submit_dt = lead.pop("_submit_dt", None)
            accepted, _ = classify_permit(lead["permit_type"], lead["permit_description"], lead["address"],
                                          submit_dt, is_dedicated_tree_source=False, max_age_days=99999)
            if not accepted:
                stats["skipped"] += 1
                continue

            if lead["normalized_address"] and lead["permit_date"]:
                key = make_address_dedup_key(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"])
                if is_duplicate_by_address_date(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"], existing_keys):
                    stats["skipped"] += 1
                    continue
                existing_keys.add(key)

            lead["lead_score"] = compute_lead_score(lead["permit_type"], lead["permit_description"], submit_dt)
            batch.append(lead)
            if pid:
                existing_permits.add(pid)

            if len(batch) >= BATCH_SIZE:
                stats["inserted"] += insert_leads_batch(batch, job_run_id=run_id)
                batch = []
                _progress(stats, total)

    if batch:
        stats["inserted"] += insert_leads_batch(batch, job_run_id=run_id)

    complete_job_run(run_id, "success", stats["found"], stats["inserted"], stats["skipped"])
    logger.info("  ✅ Fort Lauderdale done: found=%d inserted=%d skipped=%d", stats["found"], stats["inserted"], stats["skipped"])
    return stats


def backfill_miami_tree(dry_run: bool = False) -> dict:
    """Backfill ALL City of Miami Tree Permits."""
    source = "city_of_miami_tree"
    where = "1=1"  # All records

    total = get_record_count(MIAMI_TREE_PERMITS_URL, where)
    logger.info("═══ Miami Tree Permits: %d records available ═══", total)

    if dry_run:
        return {"source": source, "available": total, "dry_run": True}

    run_id = create_job_run("backfill_miami_tree", source)
    existing_permits = get_existing_permit_numbers(source)
    existing_keys = get_existing_address_keys(source)
    logger.info("  Existing: %d permits, %d address keys", len(existing_permits), len(existing_keys))

    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}
    batch = []

    for page in stream_pages(MIAMI_TREE_PERMITS_URL, where=where, out_fields=TREE_OUT_FIELDS, order_by="ObjectId ASC"):
        for attrs in page:
            stats["found"] += 1
            pn = attrs.get("PlanNumber")

            if pn and is_duplicate_by_permit_number(str(pn), existing_permits):
                stats["skipped"] += 1
                continue

            lead = parse_miami_tree_record(attrs)
            status_date = lead.pop("_status_date", None)
            accepted, _ = classify_permit(lead["permit_type"], lead["permit_description"], lead["address"],
                                          status_date, is_dedicated_tree_source=True, max_age_days=99999)
            if not accepted:
                stats["skipped"] += 1
                continue

            if lead["normalized_address"] and lead["permit_date"]:
                key = make_address_dedup_key(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"])
                if is_duplicate_by_address_date(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"], existing_keys):
                    stats["skipped"] += 1
                    continue
                existing_keys.add(key)

            lead["lead_score"] = compute_lead_score(lead["permit_type"], lead["permit_description"], status_date)
            batch.append(lead)
            if pn:
                existing_permits.add(str(pn))

            if len(batch) >= BATCH_SIZE:
                stats["inserted"] += insert_leads_batch(batch, job_run_id=run_id)
                batch = []
                _progress(stats, total)

    if batch:
        stats["inserted"] += insert_leads_batch(batch, job_run_id=run_id)

    complete_job_run(run_id, "success", stats["found"], stats["inserted"], stats["skipped"])
    logger.info("  ✅ Miami Tree done: found=%d inserted=%d skipped=%d", stats["found"], stats["inserted"], stats["skipped"])
    return stats


def backfill_miami_building(dry_run: bool = False) -> dict:
    """Backfill City of Miami Building Permits (tree-related only)."""
    source = "city_of_miami"
    where = (
        "WorkItems LIKE '%tree%' OR WorkItems LIKE '%Tree%' OR "
        "WorkItems LIKE '%TREE%' OR WorkItems LIKE '%vegetation%' OR "
        "ScopeofWork LIKE '%tree%' OR ScopeofWork LIKE '%Tree%' OR "
        "ScopeofWork LIKE '%TREE%' OR ScopeofWork LIKE '%vegetation%'"
    )

    total = get_record_count(MIAMI_BUILDING_PERMITS_URL, where)
    logger.info("═══ Miami Building (tree-filtered): %d records available ═══", total)

    if dry_run:
        return {"source": source, "available": total, "dry_run": True}

    run_id = create_job_run("backfill_miami_building", source)
    existing_permits = get_existing_permit_numbers(source)
    existing_keys = get_existing_address_keys(source)
    logger.info("  Existing: %d permits, %d address keys", len(existing_permits), len(existing_keys))

    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}
    batch = []

    for page in stream_pages(MIAMI_BUILDING_PERMITS_URL, where=where, out_fields=BUILDING_OUT_FIELDS, order_by="IssuedDate DESC"):
        for attrs in page:
            stats["found"] += 1
            pn = attrs.get("PermitNumber") or attrs.get("ApplicationNumber")

            if pn and is_duplicate_by_permit_number(str(pn), existing_permits):
                stats["skipped"] += 1
                continue

            lead = parse_miami_building_record(attrs)
            issued_date = lead.pop("_issued_date", None)
            accepted, _ = classify_permit(lead["permit_type"], lead["permit_description"], lead["address"],
                                          issued_date, is_dedicated_tree_source=False, max_age_days=99999)
            if not accepted:
                stats["skipped"] += 1
                continue

            if lead["normalized_address"] and lead["permit_date"]:
                key = make_address_dedup_key(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"])
                if is_duplicate_by_address_date(lead["normalized_address"], lead["permit_type"] or "", lead["permit_date"], existing_keys):
                    stats["skipped"] += 1
                    continue
                existing_keys.add(key)

            lead["lead_score"] = compute_lead_score(lead["permit_type"], lead["permit_description"], issued_date)
            batch.append(lead)
            if pn:
                existing_permits.add(str(pn))

            if len(batch) >= BATCH_SIZE:
                stats["inserted"] += insert_leads_batch(batch, job_run_id=run_id)
                batch = []
                _progress(stats, total)

    if batch:
        stats["inserted"] += insert_leads_batch(batch, job_run_id=run_id)

    complete_job_run(run_id, "success", stats["found"], stats["inserted"], stats["skipped"])
    logger.info("  ✅ Miami Building done: found=%d inserted=%d skipped=%d", stats["found"], stats["inserted"], stats["skipped"])
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _progress(stats: dict, total: int):
    """Log progress every 500 records."""
    if stats["found"] % 500 == 0:
        pct = (stats["found"] / max(total, 1)) * 100
        logger.info(
            "    Progress: %d/%d (%.0f%%)  inserted=%d  skipped=%d",
            stats["found"], total, pct, stats["inserted"], stats["skipped"],
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

SOURCE_MAP = {
    "derm": ("DERM Tree Permits", backfill_derm),
    "fl": ("Fort Lauderdale", backfill_fort_lauderdale),
    "miami_tree": ("Miami Tree Permits", backfill_miami_tree),
    "miami_bldg": ("Miami Building Permits", backfill_miami_building),
}


def main():
    parser = argparse.ArgumentParser(description="Backfill historical tree permit data")
    parser.add_argument("--source", choices=list(SOURCE_MAP.keys()), help="Backfill a single source")
    parser.add_argument("--dry-run", action="store_true", help="Count records only, don't insert")
    args = parser.parse_args()

    start = time.time()
    logger.info("🌳 Tree Permit Lead Discovery — Historical Backfill")
    logger.info("=" * 60)

    if args.source:
        name, fn = SOURCE_MAP[args.source]
        logger.info("Source: %s", name)
        result = fn(dry_run=args.dry_run)
        results = {args.source: result}
    else:
        logger.info("Backfilling ALL sources")
        results = {}
        for key, (name, fn) in SOURCE_MAP.items():
            logger.info("\n─── %s ───", name)
            try:
                results[key] = fn(dry_run=args.dry_run)
            except Exception as exc:
                logger.error("  ❌ %s failed: %s", name, exc, exc_info=True)
                results[key] = {"error": str(exc)}

    elapsed = time.time() - start
    logger.info("\n" + "=" * 60)
    logger.info("Backfill complete in %.1f seconds", elapsed)
    for key, r in results.items():
        if "error" in r:
            logger.info("  ❌ %s: ERROR — %s", key, r["error"])
        elif r.get("dry_run"):
            logger.info("  📊 %s: %d records available (dry run)", key, r.get("available", 0))
        else:
            logger.info("  ✅ %s: found=%d inserted=%d skipped=%d", key, r.get("found", 0), r.get("inserted", 0), r.get("skipped", 0))


if __name__ == "__main__":
    main()
