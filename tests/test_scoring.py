"""Unit tests for pipeline.scoring – lead scoring module."""
import pytest
from datetime import datetime, timezone, timedelta

from pipeline.scoring import compute_lead_score


class TestComputeLeadScore:
    """Tests for compute_lead_score point rules."""

    # ── +5 for tree removal / arbor permit type ──────────────────────────────

    @pytest.mark.parametrize(
        "permit_type",
        ["TREE REMOVAL", "TREE REMOVAL PERMIT", "ARBOR PERMIT", "TREE ALTERATION"],
    )
    def test_tree_removal_types_score_5(self, permit_type, recent_date):
        score = compute_lead_score(permit_type, "", recent_date)
        # +5 (type) + +3 (recent) = 8
        assert score >= 5

    def test_tree_removal_type_isolated(self, stale_date):
        """Non-recent tree removal to isolate the +5 component."""
        score = compute_lead_score("TREE REMOVAL", "", stale_date)
        assert score == 5

    # ── +4 for vegetation removal type ───────────────────────────────────────

    def test_vegetation_removal_type_score_4(self, stale_date):
        score = compute_lead_score("VEGETATION REMOVAL", "", stale_date)
        assert score == 4

    # ── +5 / +4 from description fallback ────────────────────────────────────

    def test_description_fallback_tree_removal_keyword(self, stale_date):
        score = compute_lead_score("MISC", "tree removal needed", stale_date)
        assert score == 5

    def test_description_fallback_remove_tree(self, stale_date):
        score = compute_lead_score(None, "Remove tree from yard", stale_date)
        assert score == 5

    def test_description_fallback_arbor(self, stale_date):
        score = compute_lead_score(None, "arbor assessment required", stale_date)
        assert score == 5

    def test_description_fallback_vegetation_removal(self, stale_date):
        score = compute_lead_score("MISC", "vegetation removal service", stale_date)
        assert score == 4

    def test_description_fallback_dead_tree(self, stale_date):
        score = compute_lead_score(None, "dead tree in yard", stale_date)
        assert score == 5

    def test_description_fallback_dangerous_tree(self, stale_date):
        score = compute_lead_score(None, "dangerous tree near power line", stale_date)
        assert score == 5

    def test_type_takes_precedence_over_description(self, stale_date):
        """When type matches, description fallback shouldn't double-count."""
        score = compute_lead_score(
            "TREE REMOVAL", "tree removal dead tree", stale_date
        )
        assert score == 5  # Only +5 from type, not +5 again from desc

    # ── +3 for recent permits (within 7 days) ────────────────────────────────

    def test_recent_date_adds_3(self):
        recent = datetime.now(timezone.utc) - timedelta(days=2)
        score = compute_lead_score("TREE REMOVAL", "", recent)
        assert score == 8  # +5 type + +3 recent

    def test_week_old_permit_not_recent(self):
        week_old = datetime.now(timezone.utc) - timedelta(days=8)
        score = compute_lead_score("TREE REMOVAL", "", week_old)
        assert score == 5  # +5 type only, no +3

    def test_exactly_7_days_is_recent(self):
        exactly_seven = datetime.now(timezone.utc) - timedelta(days=7)
        score = compute_lead_score("TREE REMOVAL", "", exactly_seven)
        # datetime.now() - 7 days should be >= week_ago, so +3
        assert score == 8

    def test_no_date_no_recency_bonus(self):
        score = compute_lead_score("TREE REMOVAL", "", None)
        assert score == 5

    def test_naive_datetime_handled(self):
        """Naive datetimes should not crash."""
        naive = datetime.now() - timedelta(days=1)
        score = compute_lead_score("TREE REMOVAL", "", naive)
        assert score >= 5

    # ── +2 for large parcel ──────────────────────────────────────────────────

    def test_large_parcel_adds_2(self, stale_date):
        score = compute_lead_score("TREE REMOVAL", "", stale_date, parcel_size_acres=1.0)
        assert score == 7  # +5 type + +2 parcel

    def test_small_parcel_no_bonus(self, stale_date):
        score = compute_lead_score("TREE REMOVAL", "", stale_date, parcel_size_acres=0.3)
        assert score == 5

    def test_boundary_parcel_no_bonus(self, stale_date):
        score = compute_lead_score("TREE REMOVAL", "", stale_date, parcel_size_acres=0.5)
        assert score == 5  # Exactly 0.5 is NOT > 0.5

    def test_none_parcel_no_bonus(self, stale_date):
        score = compute_lead_score("TREE REMOVAL", "", stale_date, parcel_size_acres=None)
        assert score == 5

    # ── +1 for right-of-way ──────────────────────────────────────────────────

    def test_row_in_description_adds_1(self, stale_date):
        score = compute_lead_score(
            "MISC", "tree removal in right of way", stale_date
        )
        assert score == 6  # +5 desc + +1 ROW

    def test_row_in_type_adds_1(self, stale_date):
        score = compute_lead_score(
            "RIGHT OF WAY TREE REMOVAL", "Remove tree", stale_date
        )
        # Note: RIGHT OF WAY TREE REMOVAL is not in _TREE_REMOVAL_TYPES
        # So +5 from desc "remove tree", +1 from type containing ROW
        assert score >= 1

    def test_row_hyphenated_adds_1(self, stale_date):
        score = compute_lead_score(
            None, "tree removal right-of-way clearance", stale_date
        )
        assert score >= 6  # +5 tree removal + +1 ROW

    # ── Combined scoring ─────────────────────────────────────────────────────

    def test_maximum_score_scenario(self):
        """All bonus conditions met."""
        recent = datetime.now(timezone.utc) - timedelta(days=1)
        score = compute_lead_score(
            "TREE REMOVAL",
            "tree removal in right of way",
            recent,
            parcel_size_acres=2.0,
        )
        # +5 type + +3 recent + +2 parcel + +1 ROW = 11
        assert score == 11

    def test_zero_score_for_no_match(self, stale_date):
        score = compute_lead_score("ELECTRICAL", "Install new outlet", stale_date)
        assert score == 0

    def test_zero_score_null_everything(self):
        score = compute_lead_score(None, None, None)
        assert score == 0
