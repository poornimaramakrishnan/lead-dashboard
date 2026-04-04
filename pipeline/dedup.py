"""Deduplication and address normalization utilities."""
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Address abbreviation expansions ───────────────────────────────────────────
_ABBREVIATIONS = {
    r"\bST\b": "STREET",
    r"\bAVE\b": "AVENUE",
    r"\bBLVD\b": "BOULEVARD",
    r"\bDR\b": "DRIVE",
    r"\bCT\b": "COURT",
    r"\bPL\b": "PLACE",
    r"\bLN\b": "LANE",
    r"\bRD\b": "ROAD",
    r"\bCIR\b": "CIRCLE",
    r"\bTER\b": "TERRACE",
    r"\bPKWY\b": "PARKWAY",
    r"\bHWY\b": "HIGHWAY",
    r"\bSQ\b": "SQUARE",
    r"\bTRL\b": "TRAIL",
    r"\bWY\b": "WAY",
}

# Pattern to match unit/apartment designators
_UNIT_PATTERN = re.compile(
    r"\s*(?:#|APT|APARTMENT|UNIT|STE|SUITE|BLDG|BUILDING|FL|FLOOR)\s*\S*\s*$",
    re.IGNORECASE,
)

# Pattern to strip leading zeros from street numbers
_LEADING_ZEROS = re.compile(r"^0+(\d)")


def normalize_address(address: Optional[str]) -> str:
    """
    Normalize an address for deduplication matching.

    - Uppercase
    - Collapse multiple spaces
    - Expand abbreviations (ST → STREET, etc.)
    - Strip leading zeros from street numbers
    - Strip unit/apartment designations

    Returns empty string for None/empty input.
    """
    if not address:
        return ""

    # Uppercase and collapse whitespace
    addr = " ".join(address.upper().split())

    # Strip unit/apartment designators
    addr = _UNIT_PATTERN.sub("", addr)

    # Expand abbreviations
    for pattern, replacement in _ABBREVIATIONS.items():
        addr = re.sub(pattern, replacement, addr)

    # Strip leading zeros from street numbers
    addr = _LEADING_ZEROS.sub(r"\1", addr)

    # Final whitespace cleanup
    addr = " ".join(addr.split()).strip()

    return addr


def is_duplicate_by_permit_number(
    permit_number: str,
    existing_permit_numbers: set,
) -> bool:
    """
    Rule 1: Check if permit_number already exists.
    This is the most reliable deduplication signal.
    """
    if not permit_number:
        return False
    return permit_number.strip() in existing_permit_numbers


def is_duplicate_by_address_date(
    normalized_address: str,
    permit_type: str,
    permit_date: str,  # ISO date string YYYY-MM-DD
    existing_address_keys: set,
) -> bool:
    """
    Rule 2: Check if same address + permit_type + permit_date already exists.
    Catches duplicates where permit numbers differ between sources.

    The key format is: "NORMALIZED_ADDRESS|PERMIT_TYPE|YYYY-MM-DD"
    """
    if not normalized_address or not permit_date:
        return False
    key = f"{normalized_address}|{(permit_type or '').upper()}|{permit_date}"
    return key in existing_address_keys


def make_address_dedup_key(
    normalized_address: str,
    permit_type: str,
    permit_date: str,
) -> str:
    """Create a dedup key for Rule 2."""
    return f"{normalized_address}|{(permit_type or '').upper()}|{permit_date}"
