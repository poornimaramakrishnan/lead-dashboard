"""Supabase database operations for leads and job_runs."""
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set, Tuple

from supabase import create_client, Client

from pipeline.config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set in environment"
            )
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def reset_client():
    """Reset the client (useful for testing)."""
    global _client
    _client = None


# ── Leads table operations ────────────────────────────────────────────────────

def insert_lead(lead: Dict[str, Any], job_run_id: Optional[str] = None) -> Optional[str]:
    """
    Insert a single lead into the leads table.
    Returns the lead ID if successful, None if failed.
    """
    client = get_client()
    lead_id = str(uuid.uuid4())
    record = {
        "id": lead_id,
        "source_name": lead.get("source_name"),
        "jurisdiction": lead.get("jurisdiction", "Unknown"),
        "address": lead.get("address") or "",
        "normalized_address": lead.get("normalized_address") or "",
        "permit_type": lead.get("permit_type"),
        "permit_description": lead.get("permit_description"),
        "permit_number": lead.get("permit_number"),
        "permit_status": lead.get("permit_status"),
        "permit_date": lead.get("permit_date"),
        "owner_name": lead.get("owner_name"),
        "contractor_name": lead.get("contractor_name"),
        "contractor_phone": lead.get("contractor_phone"),
        "source_url": lead.get("source_url"),
        "lead_score": lead.get("lead_score", 0),
        "lead_status": "new",
        "lead_type": "permit",
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "raw_payload_json": json.dumps(lead.get("raw_payload", {})),
    }
    if job_run_id:
        record["job_run_id"] = job_run_id

    try:
        result = client.table("leads").insert(record).execute()
        return lead_id
    except Exception as exc:
        logger.error("Failed to insert lead: %s", exc)
        return None


def insert_leads_batch(
    leads: List[Dict[str, Any]], job_run_id: Optional[str] = None
) -> int:
    """Insert a batch of leads. Returns count of successfully inserted records."""
    client = get_client()
    records = []
    for lead in leads:
        rec = {
            "id": str(uuid.uuid4()),
            "source_name": lead.get("source_name"),
            "jurisdiction": lead.get("jurisdiction", "Unknown"),
            "address": lead.get("address") or "",
            "normalized_address": lead.get("normalized_address") or "",
            "permit_type": lead.get("permit_type"),
            "permit_description": lead.get("permit_description"),
            "permit_number": lead.get("permit_number"),
            "permit_status": lead.get("permit_status"),
            "permit_date": lead.get("permit_date"),
            "owner_name": lead.get("owner_name"),
            "contractor_name": lead.get("contractor_name"),
            "contractor_phone": lead.get("contractor_phone"),
            "source_url": lead.get("source_url"),
            "lead_score": lead.get("lead_score", 0),
            "lead_status": "new",
            "lead_type": "permit",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
            "raw_payload_json": json.dumps(lead.get("raw_payload", {})),
        }
        if job_run_id:
            rec["job_run_id"] = job_run_id
        records.append(rec)

    if not records:
        return 0

    try:
        result = client.table("leads").insert(records).execute()
        return len(result.data) if result.data else 0
    except Exception as exc:
        logger.error("Batch insert failed: %s", exc)
        return 0


def get_existing_permit_numbers(source_name: Optional[str] = None) -> Set[str]:
    """Get all existing permit numbers for deduplication (Rule 1)."""
    client = get_client()
    query = client.table("leads").select("permit_number")
    if source_name:
        query = query.eq("source_name", source_name)

    # Paginate through all records
    all_numbers = set()
    offset = 0
    page_size = 1000
    while True:
        result = query.range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        for row in result.data:
            pn = row.get("permit_number")
            if pn:
                all_numbers.add(pn.strip())
        if len(result.data) < page_size:
            break
        offset += page_size

    return all_numbers


def get_existing_address_keys(source_name: Optional[str] = None) -> Set[str]:
    """Get address dedup keys (Rule 2): 'NORMALIZED_ADDRESS|PERMIT_TYPE|DATE'."""
    client = get_client()
    query = client.table("leads").select(
        "normalized_address, permit_type, permit_date"
    )
    if source_name:
        query = query.eq("source_name", source_name)

    all_keys = set()
    offset = 0
    page_size = 1000
    while True:
        result = query.range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        for row in result.data:
            addr = row.get("normalized_address", "")
            pt = (row.get("permit_type") or "").upper()
            pd = row.get("permit_date", "")
            if addr and pd:
                all_keys.add(f"{addr}|{pt}|{pd}")
        if len(result.data) < page_size:
            break
        offset += page_size

    return all_keys


def update_lead_status(
    lead_id: str, status: str, notes: Optional[str] = None
) -> bool:
    """Update lead_status (new/approved/rejected/exported) and optionally notes."""
    client = get_client()
    try:
        update = {"lead_status": status}
        if notes is not None:
            update["lead_notes"] = notes
        client.table("leads").update(update).eq("id", lead_id).execute()
        return True
    except Exception as exc:
        logger.error("Failed to update lead %s: %s", lead_id, exc)
        return False


# ── Job Runs table operations ─────────────────────────────────────────────────

def create_job_run(job_name: str, source_name: str) -> str:
    """Create a new job_run record and return its ID."""
    client = get_client()
    run_id = str(uuid.uuid4())
    record = {
        "id": run_id,
        "job_name": job_name,
        "source_name": source_name,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "running",
    }
    client.table("job_runs").insert(record).execute()
    return run_id


def complete_job_run(
    run_id: str,
    status: str,
    records_found: int = 0,
    records_inserted: int = 0,
    records_skipped: int = 0,
    error_message: Optional[str] = None,
):
    """Update a job_run as completed (success/failed/partial)."""
    client = get_client()
    update = {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "records_found": records_found,
        "records_inserted": records_inserted,
        "records_skipped": records_skipped,
    }
    if error_message:
        update["error_message"] = error_message[:2000]

    try:
        client.table("job_runs").update(update).eq("id", run_id).execute()
    except Exception as exc:
        logger.error("Failed to update job_run %s: %s", run_id, exc)


def get_last_successful_run(source_name: str) -> Optional[Dict[str, Any]]:
    """Get the most recent successful job run for a source."""
    client = get_client()
    try:
        result = (
            client.table("job_runs")
            .select("*")
            .eq("source_name", source_name)
            .eq("status", "success")
            .order("finished_at", desc=True)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]
    except Exception as exc:
        logger.error("Failed to get last run for %s: %s", source_name, exc)
    return None


def get_recent_job_runs(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent job runs for the health panel."""
    client = get_client()
    try:
        result = (
            client.table("job_runs")
            .select("*")
            .order("started_at", desc=True)
            .limit(limit)
            .execute()
        )
        return result.data or []
    except Exception as exc:
        logger.error("Failed to get recent runs: %s", exc)
        return []
