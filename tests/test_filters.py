"""Unit tests for pipeline.filters – the 4-step classification pipeline."""
import pytest
from datetime import datetime, timezone, timedelta

from pipeline.filters import (
    step1_exact_category_match,
    step2_description_keyword_match,
    step3_negative_keyword_exclusion,
    step4_data_quality_check,
    classify_permit,
    EXACT_PERMIT_TYPES,
    POSITIVE_KEYWORDS,
    NEGATIVE_KEYWORDS,
)


# ══════════════════════════════════════════════════════════════════════════════
#  Step 1: Exact category match
# ══════════════════════════════════════════════════════════════════════════════

class TestStep1ExactCategoryMatch:
    """Tests for step1_exact_category_match."""

    @pytest.mark.parametrize("permit_type", list(EXACT_PERMIT_TYPES))
    def test_all_exact_categories_accepted(self, permit_type):
        assert step1_exact_category_match(permit_type) is True

    @pytest.mark.parametrize(
        "permit_type",
        ["tree removal", "  TREE REMOVAL  ", "Tree Removal", "TREE removal"],
    )
    def test_case_insensitive_and_whitespace(self, permit_type):
        assert step1_exact_category_match(permit_type) is True

    def test_non_tree_type_rejected(self):
        assert step1_exact_category_match("BUILDING PERMIT") is False
        assert step1_exact_category_match("PLUMBING") is False
        assert step1_exact_category_match("ELECTRICAL") is False

    def test_partial_match_rejected(self):
        assert step1_exact_category_match("TREE") is False
        assert step1_exact_category_match("REMOVAL") is False

    def test_none_rejected(self):
        assert step1_exact_category_match(None) is False

    def test_empty_string_rejected(self):
        assert step1_exact_category_match("") is False


# ══════════════════════════════════════════════════════════════════════════════
#  Step 2: Description keyword match
# ══════════════════════════════════════════════════════════════════════════════

class TestStep2DescriptionKeywordMatch:
    """Tests for step2_description_keyword_match."""

    @pytest.mark.parametrize("keyword", POSITIVE_KEYWORDS)
    def test_all_positive_keywords_match(self, keyword):
        desc = f"This permit is for {keyword} in the backyard"
        assert step2_description_keyword_match(desc) is True

    def test_case_insensitive(self):
        assert step2_description_keyword_match("TREE REMOVAL needed") is True
        assert step2_description_keyword_match("Tree Removal needed") is True
        assert step2_description_keyword_match("tReE rEmOvAl") is True

    def test_keyword_in_longer_text(self):
        desc = "The homeowner requests to remove tree from backyard due to storm damage"
        assert step2_description_keyword_match(desc) is True

    def test_no_match(self):
        assert step2_description_keyword_match("Install new fence") is False
        assert step2_description_keyword_match("Roof repair") is False

    def test_none_input(self):
        assert step2_description_keyword_match(None) is False

    def test_empty_string(self):
        assert step2_description_keyword_match("") is False


# ══════════════════════════════════════════════════════════════════════════════
#  Step 3: Negative keyword exclusion
# ══════════════════════════════════════════════════════════════════════════════

class TestStep3NegativeKeywordExclusion:
    """Tests for step3_negative_keyword_exclusion."""

    @pytest.mark.parametrize("keyword", NEGATIVE_KEYWORDS)
    def test_all_negative_keywords_trigger_rejection(self, keyword):
        desc = f"Tree {keyword} service needed"
        assert step3_negative_keyword_exclusion(desc) is True

    def test_case_insensitive(self):
        assert step3_negative_keyword_exclusion("TREE TRIMMING permit") is True
        assert step3_negative_keyword_exclusion("tree pruning") is True

    def test_no_negative_keywords(self):
        assert step3_negative_keyword_exclusion("Tree removal needed") is False
        assert step3_negative_keyword_exclusion("Dead tree hazard") is False

    def test_none_input(self):
        assert step3_negative_keyword_exclusion(None) is False

    def test_empty_string(self):
        assert step3_negative_keyword_exclusion("") is False

    def test_mixed_keywords_negative_present(self):
        """If both positive and negative keywords are present, negative should still be detected."""
        desc = "Tree removal and trimming service"
        assert step3_negative_keyword_exclusion(desc) is True


# ══════════════════════════════════════════════════════════════════════════════
#  Step 4: Data quality check
# ══════════════════════════════════════════════════════════════════════════════

class TestStep4DataQualityCheck:
    """Tests for step4_data_quality_check."""

    def test_valid_record(self, recent_date):
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", "Remove tree", recent_date
        )
        assert valid is True
        assert reason == ""

    def test_missing_address(self, recent_date):
        valid, reason = step4_data_quality_check(
            None, "TREE REMOVAL", "Remove tree", recent_date
        )
        assert valid is False
        assert "Missing address" in reason

    def test_empty_address(self, recent_date):
        valid, reason = step4_data_quality_check(
            "   ", "TREE REMOVAL", "Remove tree", recent_date
        )
        assert valid is False
        assert "Missing address" in reason

    def test_missing_both_type_and_description(self, recent_date):
        valid, reason = step4_data_quality_check(
            "123 Main St", None, None, recent_date
        )
        assert valid is False
        assert "Missing permit type and description" in reason

    def test_type_only_is_valid(self, recent_date):
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", None, recent_date
        )
        assert valid is True

    def test_description_only_is_valid(self, recent_date):
        valid, reason = step4_data_quality_check(
            "123 Main St", None, "Remove dead tree", recent_date
        )
        assert valid is True

    def test_missing_date(self):
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", "Remove tree", None
        )
        assert valid is False
        assert "Missing permit date" in reason

    def test_old_date_rejected(self, old_date):
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", "Remove tree", old_date
        )
        assert valid is False
        assert "older than" in reason

    def test_date_right_at_boundary(self):
        """Permit exactly 90 days old should be accepted."""
        boundary_date = datetime.now(timezone.utc) - timedelta(days=89)
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", "Remove tree", boundary_date
        )
        assert valid is True

    def test_date_just_past_boundary(self):
        """Permit 91 days old should be rejected."""
        boundary_date = datetime.now(timezone.utc) - timedelta(days=91)
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", "Remove tree", boundary_date
        )
        assert valid is False

    def test_naive_datetime_handled(self):
        """Naive datetime (no tzinfo) should be handled gracefully."""
        naive_date = datetime.now() - timedelta(days=5)
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", "Remove tree", naive_date
        )
        assert valid is True

    def test_custom_max_age(self, old_date):
        """Custom max_age_days should be respected."""
        valid, reason = step4_data_quality_check(
            "123 Main St", "TREE REMOVAL", "Remove tree", old_date, max_age_days=365
        )
        assert valid is True


# ══════════════════════════════════════════════════════════════════════════════
#  classify_permit – full pipeline integration
# ══════════════════════════════════════════════════════════════════════════════

class TestClassifyPermit:
    """Tests for the full classify_permit pipeline."""

    def test_exact_match_accepted(self, recent_date):
        accepted, reason = classify_permit(
            "TREE REMOVAL", "Remove dead oak tree", "123 Main St", recent_date
        )
        assert accepted is True
        assert "Exact category match" in reason

    def test_keyword_match_accepted(self, recent_date):
        accepted, reason = classify_permit(
            "BUILDING", "remove tree from front yard", "123 Main St", recent_date
        )
        assert accepted is True
        assert "Description keyword match" in reason

    def test_negative_keyword_rejects_even_with_exact_type(self, recent_date):
        accepted, reason = classify_permit(
            "TREE REMOVAL", "Tree trimming and shaping", "123 Main St", recent_date
        )
        assert accepted is False
        assert "Negative keyword" in reason

    def test_negative_keyword_rejects_with_keyword_match(self, recent_date):
        accepted, reason = classify_permit(
            "BUILDING", "tree removal and pruning service", "123 Main St", recent_date
        )
        assert accepted is False
        assert "Negative keyword" in reason

    def test_no_match_rejected(self, recent_date):
        accepted, reason = classify_permit(
            "ELECTRICAL", "Install outlet in garage", "123 Main St", recent_date
        )
        assert accepted is False
        assert "No category or keyword match" in reason

    def test_dedicated_tree_source_skips_keyword_check(self, recent_date):
        accepted, reason = classify_permit(
            "UNKNOWN", "something random", "123 Main St", recent_date,
            is_dedicated_tree_source=True,
        )
        assert accepted is True
        assert "Dedicated tree source" in reason

    def test_dedicated_tree_source_still_checks_quality(self):
        """Even dedicated sources must have valid data quality."""
        accepted, reason = classify_permit(
            "TREE PERMIT", "Tree permit", None, datetime.now(timezone.utc),
            is_dedicated_tree_source=True,
        )
        assert accepted is False
        assert "Quality" in reason

    def test_quality_failure_blocks_before_keyword_check(self):
        """Step 4 (quality) runs first; if it fails, steps 1-3 never run."""
        accepted, reason = classify_permit(
            "TREE REMOVAL", "Tree removal", "", None
        )
        assert accepted is False
        assert "Quality" in reason

    def test_arbor_permit_accepted(self, recent_date):
        accepted, reason = classify_permit(
            "ARBOR PERMIT", "Arbor assessment", "123 Main St", recent_date
        )
        assert accepted is True

    def test_vegetation_removal_accepted(self, recent_date):
        accepted, reason = classify_permit(
            "VEGETATION REMOVAL", "Clear vegetation", "123 Main St", recent_date
        )
        assert accepted is True

    def test_right_of_way_tree_removal_accepted(self, recent_date):
        accepted, reason = classify_permit(
            "RIGHT OF WAY TREE REMOVAL", "ROW tree removal", "123 Main St", recent_date
        )
        assert accepted is True

    def test_dead_tree_keyword_accepted(self, recent_date):
        accepted, reason = classify_permit(
            "MISC", "dead tree removal hazard", "123 Main St", recent_date
        )
        assert accepted is True

    def test_dangerous_tree_keyword_accepted(self, recent_date):
        accepted, reason = classify_permit(
            "MISC", "dangerous tree needs removal", "123 Main St", recent_date
        )
        assert accepted is True

    def test_landscape_irrigation_rejected(self, recent_date):
        """Landscape irrigation is a negative keyword."""
        accepted, reason = classify_permit(
            "MISC", "tree removal and landscape irrigation", "123 Main St", recent_date
        )
        assert accepted is False

    def test_grass_only_no_tree_match(self, recent_date):
        """Grass with no tree keywords should fail on Step 2."""
        accepted, reason = classify_permit(
            "MISC", "grass installation and sod", "123 Main St", recent_date
        )
        assert accepted is False
        assert "No category or keyword match" in reason
