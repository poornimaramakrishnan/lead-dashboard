"""Source 1: Miami-Dade DERM Tree Permit Worker.

Queries the DermPermits FeatureServer, filtered to WORK_GROUP='TREE'.
This is a dedicated tree permit dataset - all records are tree permits,
so no keyword filtering is needed (Steps 1-2 are skipped).

Key characteristics:
  - No date field available, so we use ObjectId-based pagination
  - For incremental runs, track the last ObjectId processed
  - Filter: WORK_GROUP = 'TREE'
  - 16,002+ tree permit records in the dataset
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from pipeline.config import (
    DERM_PERMITS_URL,
    MIAMI_DADE_DERM_ACTIVE,
    INITIAL_LOOKBACK_DAYS,
    SKIP_RATE_WARNING_THRESHOLD,
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

SOURCE_NAME = "miami_dade_derm"
JURISDICTION = "Miami-Dade County"

# Fields we need from DERM
OUT_FIELDS = (
    "ObjectId,PERMIT_NUMBER,WORK_GROUP,FACILITY_NAME,FACILITY_ADDRESS,"
    "HOUSE_NUMBER,STREET_NAME,CITY,STATE,ZIP_CODE,FOLIO,"
    "PERMIT_STATUS,PERMIT_STATUS_DESCRIPTION,"
    "TITLE_CODE,TITLE_CODE_DESCRIPTION,PERMIT_TITLE,"
    "PERMIT_CLASS,PERMIT_CLASS_DESCRIPTION"
)


def parse_derm_record(attrs: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a raw DERM API record into our lead schema."""
    address = attrs.get("FACILITY_ADDRESS") or ""
    if not address and attrs.get("HOUSE_NUMBER") and attrs.get("STREET_NAME"):
        address = f"{attrs['HOUSE_NUMBER']} {attrs['STREET_NAME']}"

    # DERM has no date field - use current time as discovered date
    # The permit_date will be set to the discovery date
    now = datetime.now(timezone.utc)

    permit_type = (attrs.get("PERMIT_TITLE") or attrs.get("TITLE_CODE_DESCRIPTION") or "TREE PERMIT")
    permit_desc = attrs.get("PERMIT_CLASS_DESCRIPTION") or permit_type

    return {
        "source_name": SOURCE_NAME,
        "jurisdiction": JURISDICTION,
        "address": address.strip() if address else None,
        "normalized_address": normalize_address(address),
        "permit_type": permit_type.strip() if permit_type else None,
        "permit_description": permit_desc,
        "permit_number": attrs.get("PERMIT_NUMBER"),
        "permit_status": (attrs.get("PERMIT_STATUS_DESCRIPTION") or "").strip(),
        "permit_date": now.strftime("%Y-%m-%d"),
        "owner_name": (attrs.get("FACILITY_NAME") or "").strip() or None,
        "contractor_name": None,
        "contractor_phone": None,
        "source_url": f"{DERM_PERMITS_URL}/query?where=PERMIT_NUMBER='{attrs.get('PERMIT_NUMBER')}'&f=json",
        "raw_payload": attrs,
    }


def run_derm_worker() -> Dict[str, Any]:
    """
    Execute the DERM tree permit worker.

    Returns a summary dict with records_found, records_inserted, etc.
    """
    if not MIAMI_DADE_DERM_ACTIVE:
        logger.info("Miami-Dade DERM source is disabled via kill switch")
        return {"status": "disabled", "records_found": 0, "records_inserted": 0}

    run_id = create_job_run("derm_tree_worker", SOURCE_NAME)
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
        logger.info(
            "Loaded %d existing permit numbers, %d address keys for dedup",
            len(existing_permits), len(existing_keys),
        )

        # Determine the ObjectId to start from
        last_run = get_last_successful_run(SOURCE_NAME)
        where_clause = "WORK_GROUP = 'TREE'"

        # If we have existing records, only fetch new ones (higher ObjectId)
        if existing_permits:
            # Fetch from where we left off - get count first
            total = get_record_count(DERM_PERMITS_URL, where_clause)
            logger.info("Total DERM tree permits available: %d", total)
        else:
            total = get_record_count(DERM_PERMITS_URL, where_clause)
            logger.info("Initial seed run - %d DERM tree permits available", total)

        batch_buffer = []
        BATCH_SIZE = 50

        for page in stream_pages(
            DERM_PERMITS_URL,
            where=where_clause,
            out_fields=OUT_FIELDS,
            order_by="ObjectId ASC",
        ):
            for attrs in page:
                summary["records_found"] += 1
                permit_number = attrs.get("PERMIT_NUMBER")

                # Dedup Rule 1: permit number
                if permit_number and is_duplicate_by_permit_number(
                    permit_number, existing_permits
                ):
                    summary["records_skipped"] += 1
                    continue

                # Parse record
                lead = parse_derm_record(attrs)

                # Filter only checks data quality (dedicated tree source)
                accepted, reason = classify_permit(
                    lead["permit_type"],
                    lead["permit_description"],
                    lead["address"],
                    datetime.now(timezone.utc),  # No date field in DERM
                    is_dedicated_tree_source=True,
                    max_age_days=99999,  # DERM has no dates, skip age check
                )

                if not accepted:
                    summary["records_skipped"] += 1
                    logger.debug("Rejected DERM record %s: %s", permit_number, reason)
                    continue

                # Dedup Rule 2: address + type + date
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

                # Compute lead score
                lead["lead_score"] = compute_lead_score(
                    lead["permit_type"],
                    lead["permit_description"],
                    datetime.now(timezone.utc),
                )

                # Add to batch
                batch_buffer.append(lead)
                if permit_number:
                    existing_permits.add(permit_number)

                # Flush batch
                if len(batch_buffer) >= BATCH_SIZE:
                    inserted = insert_leads_batch(batch_buffer)
                    summary["records_inserted"] += inserted
                    batch_buffer = []

        # Flush remaining
        if batch_buffer:
            inserted = insert_leads_batch(batch_buffer)
            summary["records_inserted"] += inserted

        # Check skip rate (high filter rejection rate may indicate schema change)
        total_found = summary["records_found"]
        total_skipped = summary["records_skipped"]
        if total_found > 10:
            skip_rate = total_skipped / total_found
            if skip_rate > SKIP_RATE_WARNING_THRESHOLD:
                logger.warning(
                    "High skip rate for DERM: %.1f%% (%d/%d) — possible schema change",
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
            "DERM worker complete: found=%d, inserted=%d, skipped=%d",
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
        logger.error("DERM worker failed: %s", exc, exc_info=True)

    return summary
