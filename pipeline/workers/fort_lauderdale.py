"""Source 2: Fort Lauderdale Building Permit Tracker Worker.

Queries the BuildingPermitTracker MapServer for tree/vegetation permits.
This is a general building permit dataset - requires keyword filtering.

Key characteristics:
  - Date field: SUBMITDT (confirmed)
  - Filter on PERMITDESC for tree/vegetation keywords
  - Rich fields: CONTRACTOR, CONTRACTPH, OWNERNAME, ESTCOST
  - MaxRecordCount: 9,000
  - 595+ tree permits per year
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from pipeline.config import (
    FORT_LAUDERDALE_URL,
    FORT_LAUDERDALE_ACTIVE,
    INITIAL_LOOKBACK_DAYS,
    SKIP_RATE_WARNING_THRESHOLD,
    BBOX_LAT_MIN,
    BBOX_LAT_MAX,
    BBOX_LON_MIN,
    BBOX_LON_MAX,
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
    get_last_successful_run,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "fort_lauderdale"
JURISDICTION = "City of Fort Lauderdale"

OUT_FIELDS = (
    "OBJECTID,PERMITID,PERMITTYPE,PERMITSTAT,PERMITDESC,"
    "SUBMITDT,APPROVEDT,PARCELID,FULLADDR,OWNERNAME,OWNERADDR,"
    "CONTRACTOR,CONTRACTPH,ESTCOST"
)


def _epoch_to_datetime(epoch_ms) -> Optional[datetime]:
    """Convert ArcGIS epoch milliseconds to datetime."""
    if not epoch_ms:
        return None
    try:
        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def parse_fl_record(attrs: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a raw Fort Lauderdale API record into our lead schema."""
    submit_dt = _epoch_to_datetime(attrs.get("SUBMITDT"))
    permit_date = submit_dt.strftime("%Y-%m-%d") if submit_dt else None

    address = (attrs.get("FULLADDR") or "").strip()
    permit_desc = (attrs.get("PERMITDESC") or "").strip()

    return {
        "source_name": SOURCE_NAME,
        "jurisdiction": JURISDICTION,
        "address": address or None,
        "normalized_address": normalize_address(address),
        "permit_type": (attrs.get("PERMITTYPE") or "").strip() or None,
        "permit_description": permit_desc or None,
        "permit_number": (attrs.get("PERMITID") or "").strip() or None,
        "permit_status": (attrs.get("PERMITSTAT") or "").strip() or None,
        "permit_date": permit_date,
        "owner_name": (attrs.get("OWNERNAME") or "").strip() or None,
        "contractor_name": (attrs.get("CONTRACTOR") or "").strip() or None,
        "contractor_phone": (attrs.get("CONTRACTPH") or "").strip() if attrs.get("CONTRACTPH") else None,
        "source_url": (
            f"{FORT_LAUDERDALE_URL}/query?"
            f"where=PERMITID='{attrs.get('PERMITID')}'&outFields=*&f=json"
        ),
        "raw_payload": attrs,
        "_submit_dt": submit_dt,  # Keep for scoring
    }


def run_fort_lauderdale_worker() -> Dict[str, Any]:
    """
    Execute the Fort Lauderdale building permit worker.

    Returns a summary dict.
    """
    if not FORT_LAUDERDALE_ACTIVE:
        logger.info("Fort Lauderdale source is disabled via kill switch")
        return {"status": "disabled", "records_found": 0, "records_inserted": 0}

    run_id = create_job_run("fort_lauderdale_worker", SOURCE_NAME)
    summary = {
        "status": "success",
        "records_found": 0,
        "records_inserted": 0,
        "records_skipped": 0,
        "errors": [],
    }

    try:
        # Load existing records for deduplication
        existing_permits = get_existing_permit_numbers(SOURCE_NAME)
        existing_keys = get_existing_address_keys(SOURCE_NAME)

        # Determine date range
        last_run = get_last_successful_run(SOURCE_NAME)
        if last_run and last_run.get("finished_at"):
            since_date = last_run["finished_at"][:10]  # YYYY-MM-DD
        else:
            # Initial seed: look back N days
            since_date = (
                datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
            ).strftime("%Y-%m-%d")

        # Build WHERE clause: date filter + tree keyword filter
        where = (
            f"SUBMITDT >= '{since_date}' AND ("
            f"PERMITDESC LIKE '%tree%' OR PERMITDESC LIKE '%Tree%' OR "
            f"PERMITDESC LIKE '%TREE%' OR PERMITDESC LIKE '%vegetation%' OR "
            f"PERMITDESC LIKE '%Vegetation%' OR PERMITDESC LIKE '%arbor%' OR "
            f"PERMITDESC LIKE '%Arbor%'"
            f")"
        )

        total = get_record_count(FORT_LAUDERDALE_URL, where)
        logger.info(
            "Fort Lauderdale: %d tree permits since %s", total, since_date
        )

        batch_buffer = []
        BATCH_SIZE = 50

        for page in stream_pages(
            FORT_LAUDERDALE_URL,
            where=where,
            out_fields=OUT_FIELDS,
            order_by="SUBMITDT DESC",
        ):
            for attrs in page:
                summary["records_found"] += 1
                permit_id = (attrs.get("PERMITID") or "").strip()

                # Dedup Rule 1
                if permit_id and is_duplicate_by_permit_number(
                    permit_id, existing_permits
                ):
                    summary["records_skipped"] += 1
                    continue

                lead = parse_fl_record(attrs)
                submit_dt = lead.pop("_submit_dt", None)

                # 4-step classification
                accepted, reason = classify_permit(
                    lead["permit_type"],
                    lead["permit_description"],
                    lead["address"],
                    submit_dt,
                    is_dedicated_tree_source=False,
                )

                if not accepted:
                    summary["records_skipped"] += 1
                    logger.debug(
                        "Rejected FL record %s: %s", permit_id, reason
                    )
                    continue

                # Dedup Rule 2
                if lead["normalized_address"] and lead["permit_date"]:
                    key = make_address_dedup_key(
                        lead["normalized_address"],
                        lead["permit_type"] or "",
                        lead["permit_date"],
                    )
                    if is_duplicate_by_address_date(
                        lead["normalized_address"],
                        lead["permit_type"] or "",
                        lead["permit_date"],
                        existing_keys,
                    ):
                        summary["records_skipped"] += 1
                        continue
                    existing_keys.add(key)

                # Lead score
                lead["lead_score"] = compute_lead_score(
                    lead["permit_type"],
                    lead["permit_description"],
                    submit_dt,
                )

                batch_buffer.append(lead)
                if permit_id:
                    existing_permits.add(permit_id)

                if len(batch_buffer) >= BATCH_SIZE:
                    inserted = insert_leads_batch(batch_buffer)
                    summary["records_inserted"] += inserted
                    batch_buffer = []

        # Flush remaining
        if batch_buffer:
            inserted = insert_leads_batch(batch_buffer)
            summary["records_inserted"] += inserted

        # Skip rate warning
        total_found = summary["records_found"]
        total_skipped = summary["records_skipped"]
        if total_found > 10:
            skip_rate = total_skipped / total_found
            if skip_rate > SKIP_RATE_WARNING_THRESHOLD:
                logger.warning(
                    "High skip rate for Fort Lauderdale: %.1f%% (%d/%d) — possible schema change",
                    skip_rate * 100, total_skipped, total_found,
                )

        complete_job_run(
            run_id,
            status="success",
            records_found=summary["records_found"],
            records_inserted=summary["records_inserted"],
            records_skipped=summary["records_skipped"],
        )
        logger.info(
            "Fort Lauderdale worker complete: found=%d, inserted=%d, skipped=%d",
            summary["records_found"],
            summary["records_inserted"],
            summary["records_skipped"],
        )

    except Exception as exc:
        summary["status"] = "failed"
        summary["errors"].append(str(exc))
        complete_job_run(
            run_id,
            status="failed",
            records_found=summary["records_found"],
            records_inserted=summary["records_inserted"],
            records_skipped=summary["records_skipped"],
            error_message=str(exc),
        )
        logger.error("Fort Lauderdale worker failed: %s", exc, exc_info=True)

    return summary
