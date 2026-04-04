"""Permit classification and keyword filtering pipeline.

Implements the 4-step filtering logic from the spec:
  Step 1 - Exact category match
  Step 2 - Description keyword match
  Step 3 - Negative keyword exclusion
  Step 4 - Data quality check
"""
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple

from pipeline.config import (
    MAX_PERMIT_AGE_DAYS,
    BBOX_LAT_MIN,
    BBOX_LAT_MAX,
    BBOX_LON_MIN,
    BBOX_LON_MAX,
)

logger = logging.getLogger(__name__)

# ── Step 1: Exact permit type categories ──────────────────────────────────────
EXACT_PERMIT_TYPES = {
    "TREE REMOVAL",
    "TREE REMOVAL PERMIT",
    "ARBOR PERMIT",
    "VEGETATION REMOVAL",
    "TREE ALTERATION",
    "RIGHT OF WAY TREE REMOVAL",
    "LANDSCAPE TREE REMOVAL-RELOCATION PERMIT",
}

# ── Step 2: Description keywords (case-insensitive) ──────────────────────────
POSITIVE_KEYWORDS = [
    "tree removal",
    "remove tree",
    "dead tree",
    "dangerous tree",
    "tree mitigation",
    "tree relocation",
    "remove vegetation",
    "vegetation removal",
    "arbor",
]

# ── Step 3: Negative keywords (case-insensitive) ─────────────────────────────
NEGATIVE_KEYWORDS = [
    "trim",
    "trimming",
    "pruning",
    "landscape irrigation",
    "grass",
    "fertilization",
    "mulch",
    "christmas tree",
    "tree planting",
]

# Compile regex patterns for efficient matching
_positive_pattern = re.compile(
    "|".join(re.escape(kw) for kw in POSITIVE_KEYWORDS),
    re.IGNORECASE,
)
_negative_pattern = re.compile(
    "|".join(re.escape(kw) for kw in NEGATIVE_KEYWORDS if kw != "tree planting"),
    re.IGNORECASE,
)


def step1_exact_category_match(permit_type: Optional[str]) -> bool:
    """Check if permit type exactly matches a known tree permit category."""
    if not permit_type:
        return False
    return permit_type.strip().upper() in EXACT_PERMIT_TYPES


def step2_description_keyword_match(description: Optional[str]) -> bool:
    """Check if permit description contains positive tree-related keywords."""
    if not description:
        return False
    return bool(_positive_pattern.search(description))


def step3_negative_keyword_exclusion(description: Optional[str]) -> bool:
    """
    Check if permit description contains negative keywords.
    Returns True if the record should be REJECTED.

    Special case: 'tree planting' is only negative if NOT combined
    with a removal keyword (per spec).
    """
    if not description:
        return False

    desc_lower = description.lower()

    # Special case: "tree planting" is allowed if combined with removal
    if "tree planting" in desc_lower:
        has_removal = any(
            kw in desc_lower
            for kw in ("removal", "remove", "relocat")
        )
        if not has_removal:
            return True

    # Check all other negative keywords (pre-compiled, excludes 'tree planting')
    return bool(_negative_pattern.search(description))


def step4_data_quality_check(
    address: Optional[str],
    permit_type: Optional[str],
    permit_description: Optional[str],
    permit_date: Optional[datetime],
    max_age_days: int = MAX_PERMIT_AGE_DAYS,
) -> Tuple[bool, str]:
    """
    Validate record data quality.

    Returns (is_valid, rejection_reason).
    """
    if not address or not address.strip():
        return False, "Missing address"

    if not permit_type and not permit_description:
        return False, "Missing permit type and description"

    if not permit_date:
        return False, "Missing permit date"

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    if permit_date.tzinfo is None:
        permit_date = permit_date.replace(tzinfo=timezone.utc)
    if permit_date < cutoff:
        return False, f"Permit date {permit_date.date()} older than {max_age_days} days"

    return True, ""


def validate_coordinates(
    lat: Optional[float], lon: Optional[float]
) -> Tuple[bool, str]:
    """
    Validate coordinates are within the South Florida bounding box.
    Returns (is_valid, reason). If coords are None, returns True (not required).
    """
    if lat is None or lon is None:
        return True, ""
    try:
        lat, lon = float(lat), float(lon)
    except (ValueError, TypeError):
        return True, ""  # Can't validate, let it through

    if not (BBOX_LAT_MIN <= lat <= BBOX_LAT_MAX):
        return False, f"Latitude {lat} outside South Florida bounds ({BBOX_LAT_MIN}-{BBOX_LAT_MAX})"
    if not (BBOX_LON_MIN <= lon <= BBOX_LON_MAX):
        return False, f"Longitude {lon} outside South Florida bounds ({BBOX_LON_MIN}-{BBOX_LON_MAX})"
    return True, ""


def classify_permit(
    permit_type: Optional[str],
    permit_description: Optional[str],
    address: Optional[str],
    permit_date: Optional[datetime],
    is_dedicated_tree_source: bool = False,
    max_age_days: int = MAX_PERMIT_AGE_DAYS,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Run the full 4-step classification pipeline on a single record.

    Args:
        permit_type: Raw permit type from source
        permit_description: Full description text
        address: Property address
        permit_date: Permit date as datetime
        is_dedicated_tree_source: If True, skip Steps 1-2 (e.g. DERM tree dataset)
        max_age_days: Maximum permit age in days
        latitude: Optional lat for geo validation
        longitude: Optional lon for geo validation

    Returns:
        (accepted, reason) - whether the record passed, and why it was
        accepted or rejected.
    """
    # Step 4 first - data quality (always applies)
    valid, reason = step4_data_quality_check(
        address, permit_type, permit_description, permit_date, max_age_days
    )
    if not valid:
        return False, f"Quality: {reason}"

    # Coordinate validation (if available)
    geo_valid, geo_reason = validate_coordinates(latitude, longitude)
    if not geo_valid:
        return False, f"Geo: {geo_reason}"

    # For dedicated tree sources (DERM, Miami Tree Permits), all records pass
    if is_dedicated_tree_source:
        return True, "Dedicated tree source"

    # Step 1 - exact category match
    if step1_exact_category_match(permit_type):
        # Still check Step 3 negative keywords
        if step3_negative_keyword_exclusion(permit_description):
            return False, f"Negative keyword in description"
        return True, "Exact category match"

    # Step 2 - description keyword match
    if step2_description_keyword_match(permit_description):
        # Step 3 - negative keyword exclusion
        if step3_negative_keyword_exclusion(permit_description):
            return False, f"Negative keyword in description"
        return True, "Description keyword match"

    return False, "No category or keyword match"
