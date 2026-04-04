"""Source 3: City of Miami Workers.

Two sub-sources:
  3a. Tree_Permits (dedicated tree permit dataset, 6K+ records)
      - No keyword filtering needed
      - Date field: ReviewStatusChangedDate
  3b. Building_Permits_Since_2014 (general permits, 217K+ records)
      - Requires keyword filtering on WorkItems/ScopeofWork
      - Date field: IssuedDate
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from pipeline.config import (
    MIAMI_TREE_PERMITS_URL,
    MIAMI_BUILDING_PERMITS_URL,
    CITY_OF_MIAMI_ACTIVE,
    INITIAL_LOOKBACK_DAYS,
    SKIP_RATE_WARNING_THRESHOLD,
)
from pipeline.arcgis_client import stream_pages, get_record_count
from pipeline.filters import classify_permit, validate_coordinates
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

SOURCE_NAME_TREE = "city_of_miami_tree"
SOURCE_NAME_BUILDING = "city_of_miami"
JURISDICTION = "City of Miami"


def _epoch_to_datetime(epoch_ms) -> Optional[datetime]:
    """Convert ArcGIS epoch milliseconds to datetime."""
    if not epoch_ms:
        return None
    try:
        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


# ── Source 3a: City of Miami Tree Permits ─────────────────────────────────────

TREE_OUT_FIELDS = "ObjectId,ID,PlanNumber,PropertyAddress,ReviewStatus,ReviewStatusChangedDate,Latitude,Longitude"


def parse_miami_tree_record(attrs: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a City of Miami Tree Permit record."""
    status_date = _epoch_to_datetime(attrs.get("ReviewStatusChangedDate"))
    permit_date = status_date.strftime("%Y-%m-%d") if status_date else None
    address = (attrs.get("PropertyAddress") or "").strip()

    return {
        "source_name": SOURCE_NAME_TREE,
        "jurisdiction": JURISDICTION,
        "address": address or None,
        "normalized_address": normalize_address(address),
        "permit_type": "TREE PERMIT",
        "permit_description": f"Tree Permit - {attrs.get('ReviewStatus', 'Unknown status')}",
        "permit_number": attrs.get("PlanNumber"),
        "permit_status": (attrs.get("ReviewStatus") or "").strip() or None,
        "permit_date": permit_date,
        "owner_name": None,
        "contractor_name": None,
        "contractor_phone": None,
        "source_url": (
            f"{MIAMI_TREE_PERMITS_URL}/query?"
            f"where=PlanNumber='{attrs.get('PlanNumber')}'&outFields=*&f=json"
        ),
        "raw_payload": attrs,
        "_status_date": status_date,
    }


def run_miami_tree_worker() -> Dict[str, Any]:
    """Execute the City of Miami Tree Permits worker (Source 3a)."""
    if not CITY_OF_MIAMI_ACTIVE:
        logger.info("City of Miami source is disabled via kill switch")
        return {"status": "disabled", "records_found": 0, "records_inserted": 0}

    run_id = create_job_run("miami_tree_worker", SOURCE_NAME_TREE)
    summary = {
        "status": "success",
        "records_found": 0,
        "records_inserted": 0,
        "records_skipped": 0,
        "errors": [],
    }

    try:
        existing_permits = get_existing_permit_numbers(SOURCE_NAME_TREE)
        existing_keys = get_existing_address_keys(SOURCE_NAME_TREE)

        # Date filter
        last_run = get_last_successful_run(SOURCE_NAME_TREE)
        if last_run and last_run.get("finished_at"):
            since_date = last_run["finished_at"][:10]
        else:
            since_date = (
                datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
            ).strftime("%Y-%m-%d")

        where = f"ReviewStatusChangedDate >= '{since_date}'"
        total = get_record_count(MIAMI_TREE_PERMITS_URL, where)
        logger.info("Miami Tree Permits: %d records since %s", total, since_date)

        batch_buffer = []
        BATCH_SIZE = 50

        for page in stream_pages(
            MIAMI_TREE_PERMITS_URL,
            where=where,
            out_fields=TREE_OUT_FIELDS,
            order_by="ReviewStatusChangedDate DESC",
        ):
            for attrs in page:
                summary["records_found"] += 1
                plan_number = attrs.get("PlanNumber")

                if plan_number and is_duplicate_by_permit_number(
                    plan_number, existing_permits
                ):
                    summary["records_skipped"] += 1
                    continue

                lead = parse_miami_tree_record(attrs)
                status_date = lead.pop("_status_date", None)

                # Geo validation (Miami tree permits have lat/lon)
                geo_valid, geo_reason = validate_coordinates(
                    attrs.get("Latitude"), attrs.get("Longitude")
                )
                if not geo_valid:
                    summary["records_skipped"] += 1
                    logger.debug("Rejected Miami tree %s: %s", plan_number, geo_reason)
                    continue

                # Dedicated tree source - skip keyword filtering
                accepted, reason = classify_permit(
                    lead["permit_type"],
                    lead["permit_description"],
                    lead["address"],
                    status_date,
                    is_dedicated_tree_source=True,
                )

                if not accepted:
                    summary["records_skipped"] += 1
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

                lead["lead_score"] = compute_lead_score(
                    lead["permit_type"],
                    lead["permit_description"],
                    status_date,
                )

                batch_buffer.append(lead)
                if plan_number:
                    existing_permits.add(plan_number)

                if len(batch_buffer) >= BATCH_SIZE:
                    inserted = insert_leads_batch(batch_buffer)
                    summary["records_inserted"] += inserted
                    batch_buffer = []

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
                    "High skip rate for Miami Tree: %.1f%% (%d/%d) — possible schema change",
                    skip_rate * 100, total_skipped, total_found,
                )

        complete_job_run(
            run_id, "success",
            records_found=summary["records_found"],
            records_inserted=summary["records_inserted"],
            records_skipped=summary["records_skipped"],
        )
        logger.info(
            "Miami Tree worker complete: found=%d, inserted=%d, skipped=%d",
            summary["records_found"],
            summary["records_inserted"],
            summary["records_skipped"],
        )

    except Exception as exc:
        summary["status"] = "failed"
        summary["errors"].append(str(exc))
        complete_job_run(
            run_id, "failed",
            records_found=summary["records_found"],
            records_inserted=summary["records_inserted"],
            records_skipped=summary["records_skipped"],
            error_message=str(exc),
        )
        logger.error("Miami Tree worker failed: %s", exc, exc_info=True)

    return summary


# ── Source 3b: City of Miami Building Permits ─────────────────────────────────

BUILDING_OUT_FIELDS = (
    "ObjectId,PermitNumber,ApplicationNumber,WorkItems,ScopeofWork,"
    "DeliveryAddress,IssuedDate,CompanyName,CompanyAddress,CompanyCity,"
    "CompanyZip,FolioNumber,PropertyType,BuildingPermitStatusDescription,"
    "TotalCost,Latitude,Longitude"
)


def parse_miami_building_record(attrs: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a City of Miami Building Permit record."""
    issued_date = _epoch_to_datetime(attrs.get("IssuedDate"))
    permit_date = issued_date.strftime("%Y-%m-%d") if issued_date else None
    address = (attrs.get("DeliveryAddress") or "").strip()
    work_items = (attrs.get("WorkItems") or "").strip()
    scope = (attrs.get("ScopeofWork") or "").strip()
    description = work_items or scope

    return {
        "source_name": SOURCE_NAME_BUILDING,
        "jurisdiction": JURISDICTION,
        "address": address or None,
        "normalized_address": normalize_address(address),
        "permit_type": work_items[:100] if work_items else None,
        "permit_description": description,
        "permit_number": attrs.get("PermitNumber") or attrs.get("ApplicationNumber"),
        "permit_status": (attrs.get("BuildingPermitStatusDescription") or "").strip() or None,
        "permit_date": permit_date,
        "owner_name": None,
        "contractor_name": (attrs.get("CompanyName") or "").strip() or None,
        "contractor_phone": None,
        "source_url": (
            f"{MIAMI_BUILDING_PERMITS_URL}/query?"
            f"where=PermitNumber='{attrs.get('PermitNumber')}'&outFields=*&f=json"
        ),
        "raw_payload": attrs,
        "_issued_date": issued_date,
    }


def run_miami_building_worker() -> Dict[str, Any]:
    """Execute the City of Miami Building Permits worker (Source 3b)."""
    if not CITY_OF_MIAMI_ACTIVE:
        logger.info("City of Miami source is disabled via kill switch")
        return {"status": "disabled", "records_found": 0, "records_inserted": 0}

    run_id = create_job_run("miami_building_worker", SOURCE_NAME_BUILDING)
    summary = {
        "status": "success",
        "records_found": 0,
        "records_inserted": 0,
        "records_skipped": 0,
        "errors": [],
    }

    try:
        existing_permits = get_existing_permit_numbers(SOURCE_NAME_BUILDING)
        existing_keys = get_existing_address_keys(SOURCE_NAME_BUILDING)

        # Date filter
        last_run = get_last_successful_run(SOURCE_NAME_BUILDING)
        if last_run and last_run.get("finished_at"):
            since_date = last_run["finished_at"][:10]
        else:
            since_date = (
                datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
            ).strftime("%Y-%m-%d")

        # Pre-filter on the API side for tree keywords to reduce payload
        where = (
            f"IssuedDate >= '{since_date}' AND ("
            f"WorkItems LIKE '%tree%' OR WorkItems LIKE '%Tree%' OR "
            f"WorkItems LIKE '%TREE%' OR WorkItems LIKE '%vegetation%' OR "
            f"WorkItems LIKE '%arbor%' OR "
            f"ScopeofWork LIKE '%tree%' OR ScopeofWork LIKE '%Tree%' OR "
            f"ScopeofWork LIKE '%vegetation%'"
            f")"
        )

        total = get_record_count(MIAMI_BUILDING_PERMITS_URL, where)
        logger.info(
            "Miami Building Permits: %d tree-related since %s", total, since_date
        )

        batch_buffer = []
        BATCH_SIZE = 50

        for page in stream_pages(
            MIAMI_BUILDING_PERMITS_URL,
            where=where,
            out_fields=BUILDING_OUT_FIELDS,
            order_by="IssuedDate DESC",
        ):
            for attrs in page:
                summary["records_found"] += 1
                permit_number = (
                    attrs.get("PermitNumber") or attrs.get("ApplicationNumber") or ""
                ).strip()

                if permit_number and is_duplicate_by_permit_number(
                    permit_number, existing_permits
                ):
                    summary["records_skipped"] += 1
                    continue

                lead = parse_miami_building_record(attrs)
                issued_date = lead.pop("_issued_date", None)

                # Geo validation (building permits have lat/lon)
                geo_valid, geo_reason = validate_coordinates(
                    attrs.get("Latitude"), attrs.get("Longitude")
                )
                if not geo_valid:
                    summary["records_skipped"] += 1
                    logger.debug("Rejected Miami building %s: %s", permit_number, geo_reason)
                    continue

                # Full 4-step classification
                accepted, reason = classify_permit(
                    lead["permit_type"],
                    lead["permit_description"],
                    lead["address"],
                    issued_date,
                    is_dedicated_tree_source=False,
                )

                if not accepted:
                    summary["records_skipped"] += 1
                    logger.debug(
                        "Rejected Miami building record %s: %s",
                        permit_number, reason,
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

                lead["lead_score"] = compute_lead_score(
                    lead["permit_type"],
                    lead["permit_description"],
                    issued_date,
                )

                batch_buffer.append(lead)
                if permit_number:
                    existing_permits.add(permit_number)

                if len(batch_buffer) >= BATCH_SIZE:
                    inserted = insert_leads_batch(batch_buffer)
                    summary["records_inserted"] += inserted
                    batch_buffer = []

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
                    "High skip rate for Miami Building: %.1f%% (%d/%d) — possible schema change",
                    skip_rate * 100, total_skipped, total_found,
                )

        complete_job_run(
            run_id, "success",
            records_found=summary["records_found"],
            records_inserted=summary["records_inserted"],
            records_skipped=summary["records_skipped"],
        )
        logger.info(
            "Miami Building worker complete: found=%d, inserted=%d, skipped=%d",
            summary["records_found"],
            summary["records_inserted"],
            summary["records_skipped"],
        )

    except Exception as exc:
        summary["status"] = "failed"
        summary["errors"].append(str(exc))
        complete_job_run(
            run_id, "failed",
            records_found=summary["records_found"],
            records_inserted=summary["records_inserted"],
            records_skipped=summary["records_skipped"],
            error_message=str(exc),
        )
        logger.error("Miami Building worker failed: %s", exc, exc_info=True)

    return summary


def run_miami_worker() -> Dict[str, Any]:
    """Run both Miami sub-workers. Returns combined summary."""
    tree_result = run_miami_tree_worker()
    building_result = run_miami_building_worker()

    return {
        "tree": tree_result,
        "building": building_result,
        "records_found": (
            tree_result.get("records_found", 0) +
            building_result.get("records_found", 0)
        ),
        "records_inserted": (
            tree_result.get("records_inserted", 0) +
            building_result.get("records_inserted", 0)
        ),
    }
