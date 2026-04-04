"""Lead scoring module.

Computes an integer lead_score for every record at insert time.
Higher scores surface first in the dashboard default sort.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Scoring rules from spec ───────────────────────────────────────────────────
# Signal                                          | Points
# Permit type is tree removal or arbor permit     | +5
# Permit type is vegetation removal               | +4
# Permit date is within last 7 days               | +3
# Parcel size > 0.5 acre (if available)           | +2
# Right-of-way permit                             | +1

_TREE_REMOVAL_TYPES = {
    "TREE REMOVAL",
    "TREE REMOVAL PERMIT",
    "ARBOR PERMIT",
    "TREE ALTERATION",
}

_VEGETATION_TYPES = {
    "VEGETATION REMOVAL",
}

_ROW_KEYWORDS = [
    "right of way",
    "right-of-way",
    "row ",
    "r.o.w.",
]


def compute_lead_score(
    permit_type: Optional[str],
    permit_description: Optional[str],
    permit_date: Optional[datetime],
    parcel_size_acres: Optional[float] = None,
) -> int:
    """
    Compute lead score for a permit record.

    Returns an integer score >= 0.
    """
    score = 0
    pt_upper = (permit_type or "").strip().upper()
    desc_lower = (permit_description or "").lower()

    # +5 for tree removal / arbor permit type
    if pt_upper in _TREE_REMOVAL_TYPES:
        score += 5

    # +4 for vegetation removal
    if pt_upper in _VEGETATION_TYPES:
        score += 4

    # Also check description for tree removal keywords if type didn't match
    if score == 0:
        if any(kw in desc_lower for kw in ["tree removal", "remove tree", "arbor"]):
            score += 5
        elif any(kw in desc_lower for kw in ["vegetation removal", "remove vegetation"]):
            score += 4
        elif any(kw in desc_lower for kw in ["dead tree", "dangerous tree"]):
            score += 5

    # +3 for permits within last 7 days
    if permit_date:
        if permit_date.tzinfo is None:
            permit_date = permit_date.replace(tzinfo=timezone.utc)
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        if permit_date >= week_ago:
            score += 3

    # +2 for large parcel
    if parcel_size_acres is not None and parcel_size_acres > 0.5:
        score += 2

    # +1 for right-of-way
    if any(kw in desc_lower for kw in _ROW_KEYWORDS):
        score += 1
    if "RIGHT OF WAY" in pt_upper:
        score += 1

    return score
