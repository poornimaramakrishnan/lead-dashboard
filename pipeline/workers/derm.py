"""Source 1: Miami-Dade DERM Tree Permit Worker.

Queries the DermPermits FeatureServer, filtered to WORK_GROUP='TREE'.
This is a dedicated tree permit dataset - all records are tree permits,
so no keyword filtering is needed (Steps 1-2 are skipped).

Key characteristics:
  - FILE_ID encodes the original filing date:
      * Old format (prefix 2000-2030): YYYYMMDDHHMMSSXX
      * New format (prefix 17xx/18xx):  epoch milliseconds (first 13 digits)
  - For incremental runs, track the last ObjectId processed
  - Filter: WORK_GROUP = 'TREE'
  - Hard rule: records older than MAX_PERMIT_AGE_DAYS (90) are rejected
"""
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from pipeline.config import (
    DERM_PERMITS_URL,
    MIAMI_DADE_DERM_ACTIVE,
    INITIAL_LOOKBACK_DAYS,
    MAX_PERMIT_AGE_DAYS,
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

# Fields we need from DERM (FILE_ID + WORK_GROUP_NUMBER for date extraction)
OUT_FIELDS = (
    "ObjectId,FILE_ID,WORK_GROUP_NUMBER,PERMIT_NUMBER,WORK_GROUP,FACILITY_NAME,FACILITY_ADDRESS,"
    "HOUSE_NUMBER,STREET_NAME,CITY,STATE,ZIP_CODE,FOLIO,"
    "PERMIT_STATUS,PERMIT_STATUS_DESCRIPTION,"
    "TITLE_CODE,TITLE_CODE_DESCRIPTION,PERMIT_TITLE,"
    "PERMIT_CLASS,PERMIT_CLASS_DESCRIPTION"
)

# DERMPermit_gdb — spatial layer populated later in the permit lifecycle.
# Has ADDRESS, LAT/LON, and non-zero FOLIO for permits that have been
# spatially assigned.  Join key: TABLE.WORK_GROUP_NUMBER == GDB.PERMITNUM.
DERM_GDB_URL = (
    "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/ArcGIS/rest/services"
    "/DERMPermit_gdb/FeatureServer/0"
)


def fetch_derm_gdb_addresses() -> Dict[str, Dict]:
    """Pre-fetch ADDRESS / LAT / LON / FOLIO from the DERMPermit_gdb spatial layer.

    Returns a dict keyed by PERMITNUM string (which equals WORK_GROUP_NUMBER
    in the DermPermits TABLE).  Only current-year TREE permits are fetched.
    Records missing an ADDRESS are excluded from the result.

    The GDB layer is populated *after* the raw permit intake table, so
    coverage is partial — typically 5-30 % of TABLE records have a GDB
    match.  Callers should fall back gracefully when the key is absent.
    """
    import json

    current_year = datetime.now(timezone.utc).year
    year_prefix = str(current_year)[2:]  # e.g. "26" for 2026
    where = f"PERMITTYPE='TREE' AND PERMITNUM LIKE '{year_prefix}%'"
    params = urllib.parse.urlencode({
        "where": where,
        "outFields": "PERMITNUM,ADDRESS,LAT,LON,FOLIO",
        "returnGeometry": "false",
        "resultRecordCount": 5000,
        "f": "json",
    })
    url = f"{DERM_GDB_URL}/query?{params}"

    gdb_map: Dict[str, Dict] = {}
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
        for feat in payload.get("features", []):
            a = feat.get("attributes", {})
            permitnum = str(a.get("PERMITNUM") or "").strip()
            address = (a.get("ADDRESS") or "").strip()
            if not permitnum or not address:
                continue
            gdb_map[permitnum] = {
                "address": address,
                "lat": a.get("LAT"),
                "lon": a.get("LON"),
                "folio": str(a.get("FOLIO") or "").strip(),
            }
        logger.info(
            "DERMPermit_gdb: fetched %d address-enriched records for year %s",
            len(gdb_map), current_year,
        )
    except Exception as exc:
        # Non-fatal: proceed without GDB enrichment rather than aborting the run
        logger.warning("Could not fetch DERMPermit_gdb addresses: %s", exc)

    return gdb_map


def decode_file_id(file_id_value) -> Optional[datetime]:
    """Decode the DERM FILE_ID into the original filing datetime.

    FILE_ID uses two formats across the dataset's history:
      Old format: 16-digit integer where first 8 digits = YYYYMMDD
                  (e.g. 2014061910375375 → 2014-06-19)
      New format: 16-digit integer where first 13 digits = epoch ms
                  (e.g. 1715277954523896 → 2024-05-09)

    Detection: if the first 4 digits form a year between 1990-2030,
    use YYYYMMDD parsing; otherwise treat as epoch milliseconds.

    NOTE: Some recently-issued permits have FILE_IDs whose epoch_ms
    decodes to a future year (e.g. 1805613473886044 → 2027).  These
    are internal sequence numbers, not timestamps.  The caller should
    fall back to WORK_GROUP_NUMBER for those records.
    """
    if file_id_value is None:
        return None
    try:
        fid_str = str(int(file_id_value))
        year_prefix = int(fid_str[:4])
        now = datetime.now(timezone.utc)

        if 1990 <= year_prefix <= 2030:
            # Old format: YYYYMMDDHHMMSSXX
            dt = datetime.strptime(fid_str[:8], "%Y%m%d").replace(
                tzinfo=timezone.utc
            )
            # Reject dates in the future (internal sequence IDs can start with 20xx too)
            if dt > now:
                return None
            return dt
        else:
            # New format: first 13 digits are epoch milliseconds
            epoch_ms = int(fid_str[:13])
            dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
            if 2000 <= dt.year <= 2030 and dt <= now:
                return dt
            return None
    except (ValueError, TypeError, OSError):
        return None


def decode_work_group_number(wgn_value) -> Optional[datetime]:
    """Derive an approximate filing date from WORK_GROUP_NUMBER.

    WORK_GROUP_NUMBER format: YYXXXXX where first two digits = year (YY).
      e.g. 2600309 → year 2026

    For a current-year WGN we return (today − 30 days) so the record
    comfortably passes the 90-day age filter regardless of when in the
    year the pipeline runs.  For a prior-year WGN we return Jul 1 of
    that year, which keeps the record inside the window for roughly the
    first 9 months after the year ends.

    This is a *fallback* used only when FILE_ID has no usable date.
    """
    if wgn_value is None:
        return None
    try:
        wgn_str = str(int(wgn_value))
        if len(wgn_str) < 2:
            return None
        year = 2000 + int(wgn_str[:2])
        if not (2015 <= year <= 2030):
            return None
        current_year = datetime.now(timezone.utc).year
        if year == current_year:
            # Use a recent date so the record always passes the 90-day ceiling
            return datetime.now(timezone.utc) - timedelta(days=30)
        else:
            # Mid-year proxy for prior years
            return datetime(year, 7, 1, tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def parse_derm_record(
    attrs: Dict[str, Any],
    gdb_map: Optional[Dict[str, Dict]] = None,
) -> Dict[str, Any]:
    """Parse a raw DERM API record into our lead schema.

    Date extraction strategy (in priority order):
      1. FILE_ID   → decode_file_id()          — exact date when available
      2. WORK_GROUP_NUMBER → decode_work_group_number() — year approximation
                                                           (YYYY-01-01) used when
                                                           FILE_ID has no usable date
                                                           (e.g. future-dated IDs)

    Address enrichment (gdb_map):
      The DermPermits TABLE has FOLIO=0 and no address for newly-filed
      2026 permits.  Pass the result of fetch_derm_gdb_addresses() as
      gdb_map and this function will look up WORK_GROUP_NUMBER to get
      the real address, lat, and lon from the DERMPermit_gdb spatial
      layer.  Falls back to FACILITY_ADDRESS / HOUSE_NUMBER+STREET_NAME
      when no GDB match exists.

    Returns a dict with an extra '_filing_date' key (datetime or None)
    for downstream age validation.
    """
    wgn = str(int(attrs["WORK_GROUP_NUMBER"])) if attrs.get("WORK_GROUP_NUMBER") else ""
    gdb = (gdb_map or {}).get(wgn, {})

    # Address: prefer GDB (real geocoded address), fall back to TABLE fields
    address = (
        gdb.get("address")
        or attrs.get("FACILITY_ADDRESS")
        or ""
    )
    if not address and attrs.get("HOUSE_NUMBER") and attrs.get("STREET_NAME"):
        address = f"{attrs['HOUSE_NUMBER']} {attrs['STREET_NAME']}"

    # Coordinates from GDB when available
    lat = gdb.get("lat")
    lon = gdb.get("lon")

    # Extract real date from FILE_ID; fall back to WORK_GROUP_NUMBER year
    filing_date = decode_file_id(attrs.get("FILE_ID"))
    if filing_date is None:
        filing_date = decode_work_group_number(attrs.get("WORK_GROUP_NUMBER"))
    permit_date_str = filing_date.strftime("%Y-%m-%d") if filing_date else None

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
        "permit_date": permit_date_str,
        "owner_name": (attrs.get("FACILITY_NAME") or "").strip() or None,
        "contractor_name": None,
        "contractor_phone": None,
        "lat": lat,
        "lon": lon,
        "source_url": f"{DERM_PERMITS_URL}/query?where=PERMIT_NUMBER='{attrs.get('PERMIT_NUMBER')}'&f=json",
        "raw_payload": attrs,
        "_filing_date": filing_date,  # datetime for age validation
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

        # Pre-fetch GDB address map once — enriches current-year records
        # that have no address in the raw DermPermits TABLE.
        gdb_map = fetch_derm_gdb_addresses()
        logger.info("GDB address map ready: %d enrichable records", len(gdb_map))

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

                # Parse record (now includes real filing date from FILE_ID)
                lead = parse_derm_record(attrs, gdb_map=gdb_map)
                filing_date = lead.pop("_filing_date", None)

                # Hard rule: reject records with no decodable date
                if not filing_date:
                    summary["records_skipped"] += 1
                    logger.debug(
                        "Rejected DERM record %s: FILE_ID date not decodable",
                        permit_number,
                    )
                    continue

                # Filter with real filing date (enforces 90-day ceiling)
                accepted, reason = classify_permit(
                    lead["permit_type"],
                    lead["permit_description"],
                    lead["address"],
                    filing_date,
                    is_dedicated_tree_source=True,
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

                # Compute lead score (use filing date for freshness scoring)
                lead["lead_score"] = compute_lead_score(
                    lead["permit_type"],
                    lead["permit_description"],
                    filing_date,
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
