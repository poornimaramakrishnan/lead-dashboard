"""Unit tests for pipeline.dedup – address normalization and deduplication."""
import pytest

from pipeline.dedup import (
    normalize_address,
    is_duplicate_by_permit_number,
    is_duplicate_by_address_date,
    make_address_dedup_key,
)


# ══════════════════════════════════════════════════════════════════════════════
#  normalize_address
# ══════════════════════════════════════════════════════════════════════════════

class TestNormalizeAddress:
    """Tests for address normalization."""

    def test_uppercase(self):
        assert normalize_address("123 main street") == "123 MAIN STREET"

    def test_collapse_whitespace(self):
        result = normalize_address("123   MAIN    STREET")
        assert "  " not in result
        assert result == "123 MAIN STREET"

    def test_expand_st_to_street(self):
        assert normalize_address("123 MAIN ST") == "123 MAIN STREET"

    def test_expand_ave_to_avenue(self):
        assert normalize_address("456 BISCAYNE AVE") == "456 BISCAYNE AVENUE"

    def test_expand_blvd_to_boulevard(self):
        assert normalize_address("789 OCEAN BLVD") == "789 OCEAN BOULEVARD"

    def test_expand_dr_to_drive(self):
        assert normalize_address("100 PALM DR") == "100 PALM DRIVE"

    def test_expand_ct_to_court(self):
        assert normalize_address("200 OAK CT") == "200 OAK COURT"

    def test_expand_ln_to_lane(self):
        assert normalize_address("300 PINE LN") == "300 PINE LANE"

    def test_expand_rd_to_road(self):
        assert normalize_address("400 BIRCH RD") == "400 BIRCH ROAD"

    def test_expand_cir_to_circle(self):
        assert normalize_address("500 ELM CIR") == "500 ELM CIRCLE"

    def test_expand_ter_to_terrace(self):
        assert normalize_address("600 MAPLE TER") == "600 MAPLE TERRACE"

    def test_expand_pkwy_to_parkway(self):
        assert normalize_address("700 CORAL PKWY") == "700 CORAL PARKWAY"

    def test_expand_hwy_to_highway(self):
        assert normalize_address("800 US HWY") == "800 US HIGHWAY"

    def test_strip_leading_zeros(self):
        assert normalize_address("0123 MAIN STREET") == "123 MAIN STREET"

    def test_strip_multiple_leading_zeros(self):
        assert normalize_address("00045 OAK AVE") == "45 OAK AVENUE"

    def test_single_digit_preserved(self):
        """Street number '0' should not be stripped to empty string (edge case)."""
        # '01' -> '1'
        assert normalize_address("01 MAIN ST") == "1 MAIN STREET"

    def test_strip_unit_designator(self):
        assert normalize_address("123 MAIN ST APT 4B") == "123 MAIN STREET"

    def test_strip_unit_hash(self):
        assert normalize_address("123 MAIN ST #201") == "123 MAIN STREET"

    def test_strip_suite(self):
        assert normalize_address("123 MAIN ST STE 100") == "123 MAIN STREET"

    def test_strip_building(self):
        assert normalize_address("123 MAIN ST BLDG A") == "123 MAIN STREET"

    def test_strip_floor(self):
        assert normalize_address("123 MAIN ST FL 3") == "123 MAIN STREET"

    def test_none_returns_empty(self):
        assert normalize_address(None) == ""

    def test_empty_returns_empty(self):
        assert normalize_address("") == ""

    def test_whitespace_only_returns_empty(self):
        assert normalize_address("   ") == ""

    def test_full_normalization_pipeline(self):
        """Test multiple normalizations applied together."""
        raw = "  0123  nw 5th  st  apt 4B  "
        result = normalize_address(raw)
        assert result == "123 NW 5TH STREET"

    def test_mixed_case_with_abbreviations(self):
        raw = "456 e Las Olas Blvd"
        result = normalize_address(raw)
        assert result == "456 E LAS OLAS BOULEVARD"

    def test_word_boundary_prevents_false_expansion(self):
        """ST in 'STREET' should NOT be expanded again."""
        result = normalize_address("123 MAIN STREET")
        assert result == "123 MAIN STREET"  # Not "123 MAIN STREETREET"

    def test_abbreviation_not_in_middle_of_word(self):
        """DR in 'DREW' should NOT be expanded."""
        result = normalize_address("123 DREW AVE")
        # \bDR\b should not match DREW because of word boundary
        assert "DRIVE" not in result or "DREW" in result


# ══════════════════════════════════════════════════════════════════════════════
#  is_duplicate_by_permit_number
# ══════════════════════════════════════════════════════════════════════════════

class TestIsDuplicateByPermitNumber:
    """Tests for Rule 1: permit number dedup."""

    def test_known_permit_is_duplicate(self):
        existing = {"FL-2025-001", "FL-2025-002", "FL-2025-003"}
        assert is_duplicate_by_permit_number("FL-2025-001", existing) is True

    def test_unknown_permit_is_not_duplicate(self):
        existing = {"FL-2025-001", "FL-2025-002"}
        assert is_duplicate_by_permit_number("FL-2025-999", existing) is False

    def test_empty_permit_is_not_duplicate(self):
        existing = {"FL-2025-001"}
        assert is_duplicate_by_permit_number("", existing) is False

    def test_none_permit_is_not_duplicate(self):
        existing = {"FL-2025-001"}
        assert is_duplicate_by_permit_number(None, existing) is False

    def test_whitespace_stripped_matches(self):
        """Function strips whitespace before lookup, so trailing/leading space should match."""
        existing = {"FL-2025-001"}
        assert is_duplicate_by_permit_number("FL-2025-001 ", existing) is True
        assert is_duplicate_by_permit_number(" FL-2025-001", existing) is True

    def test_empty_existing_set(self):
        assert is_duplicate_by_permit_number("FL-2025-001", set()) is False


# ══════════════════════════════════════════════════════════════════════════════
#  is_duplicate_by_address_date
# ══════════════════════════════════════════════════════════════════════════════

class TestIsDuplicateByAddressDate:
    """Tests for Rule 2: address + type + date dedup."""

    def test_exact_match_is_duplicate(self):
        existing = {"123 MAIN STREET|TREE REMOVAL|2025-03-15"}
        assert is_duplicate_by_address_date(
            "123 MAIN STREET", "TREE REMOVAL", "2025-03-15", existing
        ) is True

    def test_different_address_not_duplicate(self):
        existing = {"123 MAIN STREET|TREE REMOVAL|2025-03-15"}
        assert is_duplicate_by_address_date(
            "456 OAK AVENUE", "TREE REMOVAL", "2025-03-15", existing
        ) is False

    def test_different_type_not_duplicate(self):
        existing = {"123 MAIN STREET|TREE REMOVAL|2025-03-15"}
        assert is_duplicate_by_address_date(
            "123 MAIN STREET", "VEGETATION REMOVAL", "2025-03-15", existing
        ) is False

    def test_different_date_not_duplicate(self):
        existing = {"123 MAIN STREET|TREE REMOVAL|2025-03-15"}
        assert is_duplicate_by_address_date(
            "123 MAIN STREET", "TREE REMOVAL", "2025-03-16", existing
        ) is False

    def test_empty_address_not_duplicate(self):
        existing = {"123 MAIN STREET|TREE REMOVAL|2025-03-15"}
        assert is_duplicate_by_address_date(
            "", "TREE REMOVAL", "2025-03-15", existing
        ) is False

    def test_empty_date_not_duplicate(self):
        existing = {"123 MAIN STREET|TREE REMOVAL|2025-03-15"}
        assert is_duplicate_by_address_date(
            "123 MAIN STREET", "TREE REMOVAL", "", existing
        ) is False

    def test_case_insensitive_type(self):
        existing = {"123 MAIN STREET|TREE REMOVAL|2025-03-15"}
        assert is_duplicate_by_address_date(
            "123 MAIN STREET", "tree removal", "2025-03-15", existing
        ) is True


# ══════════════════════════════════════════════════════════════════════════════
#  make_address_dedup_key
# ══════════════════════════════════════════════════════════════════════════════

class TestMakeAddressDedupKey:
    """Tests for key generation."""

    def test_normal_key(self):
        key = make_address_dedup_key("123 MAIN STREET", "TREE REMOVAL", "2025-03-15")
        assert key == "123 MAIN STREET|TREE REMOVAL|2025-03-15"

    def test_type_uppercased(self):
        key = make_address_dedup_key("123 MAIN STREET", "tree removal", "2025-03-15")
        assert key == "123 MAIN STREET|TREE REMOVAL|2025-03-15"

    def test_none_type(self):
        key = make_address_dedup_key("123 MAIN STREET", None, "2025-03-15")
        assert key == "123 MAIN STREET||2025-03-15"

    def test_key_can_be_looked_up_in_set(self):
        key = make_address_dedup_key("123 MAIN STREET", "TREE REMOVAL", "2025-03-15")
        existing = {key}
        assert is_duplicate_by_address_date(
            "123 MAIN STREET", "TREE REMOVAL", "2025-03-15", existing
        ) is True
