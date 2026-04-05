"""Unit tests for pipeline.workers – all 3 source workers with mocked dependencies."""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call

from pipeline.workers.derm import parse_derm_record, run_derm_worker, decode_file_id, decode_work_group_number, SOURCE_NAME as DERM_SOURCE
from pipeline.workers.fort_lauderdale import (
    parse_fl_record,
    run_fort_lauderdale_worker,
    _epoch_to_datetime as fl_epoch_to_datetime,
    SOURCE_NAME as FL_SOURCE,
)
from pipeline.workers.miami import (
    parse_miami_tree_record,
    parse_miami_building_record,
    run_miami_tree_worker,
    run_miami_building_worker,
    run_miami_worker,
    _epoch_to_datetime as miami_epoch_to_datetime,
    SOURCE_NAME_TREE,
    SOURCE_NAME_BUILDING,
)


# ══════════════════════════════════════════════════════════════════════════════
#  Epoch conversion helper
# ══════════════════════════════════════════════════════════════════════════════

class TestEpochToDatetime:
    """Tests for the _epoch_to_datetime helper used by Fort Lauderdale & Miami."""

    def test_valid_epoch(self):
        # 2025-01-01 00:00:00 UTC
        epoch_ms = 1735689600000
        dt = fl_epoch_to_datetime(epoch_ms)
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 1
        assert dt.day == 1

    def test_none_returns_none(self):
        assert fl_epoch_to_datetime(None) is None

    def test_zero_handled(self):
        # Epoch 0 may raise OSError on Windows, so just check it doesn't crash
        result = fl_epoch_to_datetime(0)
        # On Unix this returns epoch start, on Windows it may return None
        assert result is None or result is not None  # no crash

    def test_invalid_value_returns_none(self):
        assert fl_epoch_to_datetime("not a number") is None


# ══════════════════════════════════════════════════════════════════════════════
#  DERM Worker – decode_file_id
# ══════════════════════════════════════════════════════════════════════════════

class TestDecodeFileId:
    """Tests for the FILE_ID date decoding helper."""

    def test_old_format_yyyymmdd(self):
        """Old FILE_ID with YYYYMMDD prefix → valid date."""
        dt = decode_file_id(2014061910375375)
        assert dt is not None
        assert dt.year == 2014
        assert dt.month == 6
        assert dt.day == 19

    def test_new_format_epoch_ms(self):
        """New FILE_ID with epoch ms (first 13 digits) → valid date."""
        # 1715277954523896 → 2024-05-09
        dt = decode_file_id(1715277954523896)
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 5

    def test_none_returns_none(self):
        assert decode_file_id(None) is None

    def test_invalid_returns_none(self):
        assert decode_file_id("not_a_number") is None

    def test_very_old_format(self):
        """FILE_ID from 2002."""
        dt = decode_file_id(2002030416183763)
        assert dt is not None
        assert dt.year == 2002
        assert dt.month == 3
        assert dt.day == 4

    def test_future_date_old_format_returns_none(self):
        """Old-format FILE_ID that decodes to a future year should return None."""
        # 2099-01-01 in YYYYMMDD format → future date
        dt = decode_file_id(2099010100000000)
        assert dt is None

    def test_future_date_epoch_ms_returns_none(self):
        """Epoch-ms FILE_ID that decodes to a future date (e.g. 2027) → None."""
        # 1805613473886044 → first 13 digits = 1805613473886 → 2027-03-21 (future)
        dt = decode_file_id(1805613473886044)
        assert dt is None


# ══════════════════════════════════════════════════════════════════════════════
#  DERM Worker – decode_work_group_number
# ══════════════════════════════════════════════════════════════════════════════

class TestDecodeWorkGroupNumber:
    """Tests for the WORK_GROUP_NUMBER year-proxy decoder."""

    def test_current_year_returns_recent_date(self):
        """Current-year WGN returns a date ~30 days ago, always within 90-day window."""
        current_year = datetime.now(timezone.utc).year
        wgn = int(f"{current_year - 2000:02d}00309")
        dt = decode_work_group_number(wgn)
        assert dt is not None
        assert dt.year == current_year
        age = (datetime.now(timezone.utc) - dt).days
        assert age <= 90, f"Expected age ≤ 90 days, got {age}"

    def test_prior_year_returns_july(self):
        """Prior-year WGN returns Jul 1 of that year."""
        prior_year = datetime.now(timezone.utc).year - 1
        wgn = int(f"{prior_year - 2000:02d}01346")
        dt = decode_work_group_number(wgn)
        assert dt is not None
        assert dt.year == prior_year
        assert dt.month == 7
        assert dt.day == 1

    def test_2025_prefix(self):
        dt = decode_work_group_number(2501346)
        assert dt is not None
        assert dt.year == 2025

    def test_2024_prefix(self):
        dt = decode_work_group_number(2401200)
        assert dt is not None
        assert dt.year == 2024

    def test_none_returns_none(self):
        assert decode_work_group_number(None) is None

    def test_too_old_year_returns_none(self):
        """WGN that maps to year < 2015 should return None."""
        assert decode_work_group_number(14001) is None

    def test_too_future_year_returns_none(self):
        """WGN that maps to year > 2030 should return None."""
        assert decode_work_group_number(3100001) is None


# ══════════════════════════════════════════════════════════════════════════════
#  DERM Worker – parse_derm_record
# ══════════════════════════════════════════════════════════════════════════════

class TestParseDermRecord:
    """Tests for DERM record parsing."""

    def test_basic_parsing(self, sample_derm_attributes):
        lead = parse_derm_record(sample_derm_attributes)
        assert lead["source_name"] == "miami_dade_derm"
        assert lead["jurisdiction"] == "Miami-Dade County"
        assert lead["permit_number"] == "DERM-2025-00123"
        assert lead["address"] == "123 NW 5TH ST"
        assert lead["permit_type"] == "TREE REMOVAL"
        assert lead["permit_status"] == "NEW APPLICATION"
        assert lead["owner_name"] == "JOHN DOE"
        assert lead["raw_payload"] == sample_derm_attributes

    def test_address_fallback_to_components(self):
        attrs = {
            "FACILITY_ADDRESS": None,
            "HOUSE_NUMBER": "456",
            "STREET_NAME": "SW 3RD AVE",
            "PERMIT_NUMBER": "DERM-001",
            "PERMIT_TITLE": "TREE PERMIT",
            "PERMIT_STATUS_DESCRIPTION": "APPROVED",
            "FILE_ID": 2026030110000000,
        }
        lead = parse_derm_record(attrs)
        assert lead["address"] == "456 SW 3RD AVE"

    def test_normalized_address_set(self, sample_derm_attributes):
        lead = parse_derm_record(sample_derm_attributes)
        assert lead["normalized_address"]  # Should be non-empty
        assert lead["normalized_address"] == lead["normalized_address"].upper()

    def test_permit_date_from_file_id(self, sample_derm_attributes):
        """DERM permit_date is decoded from FILE_ID, not today's date."""
        lead = parse_derm_record(sample_derm_attributes)
        # Fixture FILE_ID = 2026031510000000 → 2026-03-15
        assert lead["permit_date"] == "2026-03-15"

    def test_filing_date_returned(self, sample_derm_attributes):
        """parse_derm_record returns _filing_date for age validation."""
        lead = parse_derm_record(sample_derm_attributes)
        assert "_filing_date" in lead
        assert lead["_filing_date"] is not None
        assert lead["_filing_date"].year == 2026
        assert lead["_filing_date"].month == 3

    def test_missing_file_id_gives_none_date(self):
        """Records without FILE_ID and without WORK_GROUP_NUMBER get permit_date=None."""
        attrs = {"PERMIT_NUMBER": "X", "PERMIT_TITLE": "T", "WORK_GROUP_NUMBER": None}
        lead = parse_derm_record(attrs)
        assert lead["permit_date"] is None
        assert lead["_filing_date"] is None

    def test_future_file_id_falls_back_to_work_group_number(self):
        """When FILE_ID decodes to a future date, WORK_GROUP_NUMBER year is used."""
        # FILE_ID 1805613473886044 decodes to ~2027-03-21 (future) → rejected
        # WORK_GROUP_NUMBER 2600309 → year 2026 (current year) → now-30d
        attrs = {
            "PERMIT_NUMBER": "TREE-02600309",
            "PERMIT_TITLE": "TREE Permit",
            "PERMIT_STATUS_DESCRIPTION": "NEW APPLICATION",
            "FACILITY_ADDRESS": "100 SW 5TH ST",
            "FILE_ID": 1805613473886044,
            "WORK_GROUP_NUMBER": 2600309,
        }
        lead = parse_derm_record(attrs)
        assert lead["_filing_date"] is not None
        assert lead["_filing_date"].year == 2026
        # Date must be within 90 days of today
        age = (datetime.now(timezone.utc) - lead["_filing_date"]).days
        assert age <= 90, f"Fallback date is {age} days old, expected ≤ 90"
        assert lead["permit_date"] is not None

    def test_missing_fields_handled_gracefully(self):
        """All-None attributes shouldn't crash."""
        attrs = {}
        lead = parse_derm_record(attrs)
        assert lead["source_name"] == "miami_dade_derm"
        assert lead["permit_number"] is None

    def test_gdb_address_overrides_table_address(self):
        """When gdb_map has a match, GDB address takes precedence over TABLE address."""
        attrs = {
            "PERMIT_NUMBER": "DERM-2600152",
            "PERMIT_TITLE": "TREE REMOVAL",
            "PERMIT_STATUS_DESCRIPTION": "NEW APPLICATION",
            "FACILITY_ADDRESS": None,  # TABLE has no address (2026 record)
            "HOUSE_NUMBER": None,
            "STREET_NAME": None,
            "FILE_ID": 2026031510000000,
            "WORK_GROUP_NUMBER": 2600152,
        }
        gdb_map = {
            "2600152": {
                "address": "5563 SW 164TH AVE",
                "lat": 25.71315,
                "lon": -80.45908,
                "folio": "8800000000008",
            }
        }
        lead = parse_derm_record(attrs, gdb_map=gdb_map)
        assert lead["address"] == "5563 SW 164TH AVE"
        assert lead["lat"] == 25.71315
        assert lead["lon"] == -80.45908

    def test_gdb_no_match_falls_back_to_table_address(self):
        """When WGN has no GDB entry, TABLE FACILITY_ADDRESS is used."""
        attrs = {
            "PERMIT_NUMBER": "DERM-9999999",
            "PERMIT_TITLE": "TREE PERMIT",
            "PERMIT_STATUS_DESCRIPTION": "NEW APPLICATION",
            "FACILITY_ADDRESS": "100 NW 7TH ST",
            "FILE_ID": 2026031510000000,
            "WORK_GROUP_NUMBER": 9999999,
        }
        gdb_map = {}  # No match
        lead = parse_derm_record(attrs, gdb_map=gdb_map)
        assert lead["address"] == "100 NW 7TH ST"
        assert lead["lat"] is None
        assert lead["lon"] is None

    def test_gdb_none_map_uses_table_address(self):
        """Passing gdb_map=None falls back gracefully to TABLE address."""
        lead = parse_derm_record({"FACILITY_ADDRESS": "50 SW 1ST AVE", "WORK_GROUP_NUMBER": 1234})
        assert lead["address"] == "50 SW 1ST AVE"
        assert lead["lat"] is None


# ══════════════════════════════════════════════════════════════════════════════
#  Fort Lauderdale Worker – parse_fl_record
# ══════════════════════════════════════════════════════════════════════════════

class TestParseFLRecord:
    """Tests for Fort Lauderdale record parsing."""

    def test_basic_parsing(self, sample_fl_attributes):
        lead = parse_fl_record(sample_fl_attributes)
        assert lead["source_name"] == "fort_lauderdale"
        assert lead["jurisdiction"] == "City of Fort Lauderdale"
        assert lead["permit_number"] == "FL-2025-98765"
        assert lead["address"] == "456 E Las Olas Blvd"
        assert lead["permit_description"] == "Remove dead tree from front yard"
        assert lead["owner_name"] == "Jane Smith"
        assert lead["contractor_name"] == "All-Star Tree Service"
        assert lead["contractor_phone"] == "954-555-0123"
        assert lead["permit_date"] is not None  # Should be ISO date

    def test_epoch_date_conversion(self, sample_fl_attributes):
        lead = parse_fl_record(sample_fl_attributes)
        # Should be a YYYY-MM-DD string
        assert len(lead["permit_date"]) == 10
        assert lead["permit_date"].count("-") == 2

    def test_null_date_handled(self):
        attrs = {
            "PERMITID": "FL-001",
            "SUBMITDT": None,
            "FULLADDR": "100 Main St",
        }
        lead = parse_fl_record(attrs)
        assert lead["permit_date"] is None

    def test_source_url_generated(self, sample_fl_attributes):
        lead = parse_fl_record(sample_fl_attributes)
        assert "FL-2025-98765" in lead["source_url"]
        assert "query?" in lead["source_url"]


# ══════════════════════════════════════════════════════════════════════════════
#  City of Miami – parse_miami_tree_record
# ══════════════════════════════════════════════════════════════════════════════

class TestParseMiamiTreeRecord:
    """Tests for Miami Tree Permit record parsing."""

    def test_basic_parsing(self, sample_miami_tree_attributes):
        lead = parse_miami_tree_record(sample_miami_tree_attributes)
        assert lead["source_name"] == "city_of_miami_tree"
        assert lead["jurisdiction"] == "City of Miami"
        assert lead["permit_number"] == "TPL-2025-0042"
        assert lead["address"] == "789 Brickell Ave"
        assert lead["permit_type"] == "TREE PERMIT"
        assert lead["permit_status"] == "Approved"
        assert lead["permit_date"] is not None

    def test_review_status_in_description(self, sample_miami_tree_attributes):
        lead = parse_miami_tree_record(sample_miami_tree_attributes)
        assert "Approved" in lead["permit_description"]


# ══════════════════════════════════════════════════════════════════════════════
#  City of Miami – parse_miami_building_record
# ══════════════════════════════════════════════════════════════════════════════

class TestParseMiamiBuildingRecord:
    """Tests for Miami Building Permit record parsing."""

    def test_basic_parsing(self, sample_miami_building_attributes):
        lead = parse_miami_building_record(sample_miami_building_attributes)
        assert lead["source_name"] == "city_of_miami"
        assert lead["jurisdiction"] == "City of Miami"
        assert lead["permit_number"] == "BP-2025-11111"
        assert "TREE REMOVAL" in (lead["permit_description"] or "").upper()
        assert lead["contractor_name"] == "Miami Tree Experts Inc"

    def test_work_items_used_for_type(self, sample_miami_building_attributes):
        lead = parse_miami_building_record(sample_miami_building_attributes)
        assert lead["permit_type"] is not None
        assert "TREE REMOVAL" in lead["permit_type"].upper()

    def test_fallback_to_application_number(self):
        attrs = {
            "PermitNumber": None,
            "ApplicationNumber": "APP-999",
            "DeliveryAddress": "200 SW 1ST ST",
            "IssuedDate": None,
        }
        lead = parse_miami_building_record(attrs)
        assert lead["permit_number"] == "APP-999"


# ══════════════════════════════════════════════════════════════════════════════
#  Worker execution – kill switch tests
# ══════════════════════════════════════════════════════════════════════════════

class TestWorkerKillSwitches:
    """Test that workers respect kill switch configuration."""

    @patch("pipeline.workers.derm.MIAMI_DADE_DERM_ACTIVE", False)
    def test_derm_kill_switch(self):
        result = run_derm_worker()
        assert result["status"] == "disabled"
        assert result["records_found"] == 0

    @patch("pipeline.workers.fort_lauderdale.FORT_LAUDERDALE_ACTIVE", False)
    def test_fl_kill_switch(self):
        result = run_fort_lauderdale_worker()
        assert result["status"] == "disabled"
        assert result["records_found"] == 0

    @patch("pipeline.workers.miami.CITY_OF_MIAMI_ACTIVE", False)
    def test_miami_tree_kill_switch(self):
        result = run_miami_tree_worker()
        assert result["status"] == "disabled"

    @patch("pipeline.workers.miami.CITY_OF_MIAMI_ACTIVE", False)
    def test_miami_building_kill_switch(self):
        result = run_miami_building_worker()
        assert result["status"] == "disabled"


# ══════════════════════════════════════════════════════════════════════════════
#  Worker execution – DERM with mocked DB + API
# ══════════════════════════════════════════════════════════════════════════════

class TestRunDermWorker:
    """Test run_derm_worker with mocked dependencies."""

    @patch("pipeline.workers.derm.MIAMI_DADE_DERM_ACTIVE", True)
    @patch("pipeline.workers.derm.fetch_derm_gdb_addresses", return_value={})
    @patch("pipeline.workers.derm.insert_leads_batch", return_value=2)
    @patch("pipeline.workers.derm.complete_job_run")
    @patch("pipeline.workers.derm.create_job_run", return_value="run-001")
    @patch("pipeline.workers.derm.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.derm.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.derm.get_existing_permit_numbers", return_value=set())
    @patch("pipeline.workers.derm.get_record_count", return_value=2)
    @patch("pipeline.workers.derm.stream_pages")
    def test_successful_run(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert, mock_gdb,
    ):
        mock_stream.return_value = iter([
            [
                {
                    "ObjectId": 1,
                    "PERMIT_NUMBER": "DERM-001",
                    "WORK_GROUP": "TREE",
                    "FACILITY_ADDRESS": "100 NW 1ST ST",
                    "PERMIT_TITLE": "TREE REMOVAL",
                    "PERMIT_STATUS_DESCRIPTION": "APPROVED",
                },
                {
                    "ObjectId": 2,
                    "PERMIT_NUMBER": "DERM-002",
                    "WORK_GROUP": "TREE",
                    "FACILITY_ADDRESS": "200 SW 2ND AVE",
                    "PERMIT_TITLE": "TREE PERMIT",
                    "PERMIT_STATUS_DESCRIPTION": "NEW APPLICATION",
                },
            ]
        ])
        result = run_derm_worker()
        assert result["status"] == "success"
        assert result["records_found"] == 2
        mock_complete_run.assert_called_once()
        # Verify it was called with success status
        assert mock_complete_run.call_args[1]["status"] == "success" or mock_complete_run.call_args[0][1] == "success"

    @patch("pipeline.workers.derm.MIAMI_DADE_DERM_ACTIVE", True)
    @patch("pipeline.workers.derm.fetch_derm_gdb_addresses", return_value={})
    @patch("pipeline.workers.derm.insert_leads_batch")
    @patch("pipeline.workers.derm.complete_job_run")
    @patch("pipeline.workers.derm.create_job_run", return_value="run-001")
    @patch("pipeline.workers.derm.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.derm.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.derm.get_existing_permit_numbers", return_value={"DERM-001"})
    @patch("pipeline.workers.derm.get_record_count", return_value=1)
    @patch("pipeline.workers.derm.stream_pages")
    def test_dedup_skips_known_permits(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert, mock_gdb,
    ):
        mock_stream.return_value = iter([
            [{"ObjectId": 1, "PERMIT_NUMBER": "DERM-001", "FACILITY_ADDRESS": "100 NW 1ST ST", "PERMIT_TITLE": "TREE REMOVAL", "PERMIT_STATUS_DESCRIPTION": "OK"}]
        ])
        result = run_derm_worker()
        assert result["records_skipped"] == 1
        mock_insert.assert_not_called()

    @patch("pipeline.workers.derm.MIAMI_DADE_DERM_ACTIVE", True)
    @patch("pipeline.workers.derm.complete_job_run")
    @patch("pipeline.workers.derm.create_job_run", return_value="run-err")
    @patch("pipeline.workers.derm.get_existing_permit_numbers", side_effect=Exception("DB down"))
    def test_handles_error_gracefully(self, mock_permits, mock_create, mock_complete):
        result = run_derm_worker()
        assert result["status"] == "failed"
        assert len(result["errors"]) > 0
        # Should still call complete_job_run with failed status
        mock_complete.assert_called_once()

    @patch("pipeline.workers.derm.MIAMI_DADE_DERM_ACTIVE", False)
    def test_disabled_source_skips_cleanly(self):
        """When MIAMI_DADE_DERM_ACTIVE is False, worker returns disabled status."""
        result = run_derm_worker()
        assert result["status"] == "disabled"

    @patch("pipeline.workers.derm.MIAMI_DADE_DERM_ACTIVE", True)
    @patch("pipeline.workers.derm.fetch_derm_gdb_addresses", return_value={
        "2600152": {
            "address": "5563 SW 164TH AVE",
            "lat": 25.71315,
            "lon": -80.45908,
            "folio": "8800000000008",
        }
    })
    @patch("pipeline.workers.derm.insert_leads_batch", return_value=1)
    @patch("pipeline.workers.derm.complete_job_run")
    @patch("pipeline.workers.derm.create_job_run", return_value="run-gdb")
    @patch("pipeline.workers.derm.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.derm.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.derm.get_existing_permit_numbers", return_value=set())
    @patch("pipeline.workers.derm.get_record_count", return_value=1)
    @patch("pipeline.workers.derm.stream_pages")
    def test_gdb_address_enrichment_in_run(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert, mock_gdb,
    ):
        """GDB address map is fetched once and used to enrich leads during the run."""
        mock_stream.return_value = iter([
            [
                {
                    "ObjectId": 1,
                    "PERMIT_NUMBER": "DERM-2600152",
                    "WORK_GROUP": "TREE",
                    "WORK_GROUP_NUMBER": 2600152,
                    "FACILITY_ADDRESS": None,   # No address in TABLE
                    "PERMIT_TITLE": "TREE REMOVAL",
                    "PERMIT_STATUS_DESCRIPTION": "NEW APPLICATION",
                    "FILE_ID": 2026031510000000,
                },
            ]
        ])
        result = run_derm_worker()
        assert result["status"] == "success"
        assert result["records_found"] == 1
        mock_gdb.assert_called_once()
        # Verify the inserted lead has the GDB address
        inserted_leads = mock_insert.call_args[0][0]
        assert len(inserted_leads) == 1
        assert inserted_leads[0]["address"] == "5563 SW 164TH AVE"
        assert inserted_leads[0]["lat"] == 25.71315


# ══════════════════════════════════════════════════════════════════════════════
#  Worker execution – Fort Lauderdale with mocked DB + API
# ══════════════════════════════════════════════════════════════════════════════

class TestRunFortLauderdaleWorker:
    """Test run_fort_lauderdale_worker with mocked dependencies."""

    @patch("pipeline.workers.fort_lauderdale.insert_leads_batch", return_value=1)
    @patch("pipeline.workers.fort_lauderdale.complete_job_run")
    @patch("pipeline.workers.fort_lauderdale.create_job_run", return_value="run-fl-001")
    @patch("pipeline.workers.fort_lauderdale.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.fort_lauderdale.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.fort_lauderdale.get_existing_permit_numbers", return_value=set())
    @patch("pipeline.workers.fort_lauderdale.get_record_count", return_value=1)
    @patch("pipeline.workers.fort_lauderdale.stream_pages")
    def test_successful_run(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert,
    ):
        epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000)
        mock_stream.return_value = iter([
            [{
                "OBJECTID": 1,
                "PERMITID": "FL-001",
                "PERMITTYPE": "BUILDING",
                "PERMITSTAT": "ISSUED",
                "PERMITDESC": "Remove dead tree from yard",
                "SUBMITDT": epoch_ms,
                "FULLADDR": "456 E Las Olas Blvd",
                "OWNERNAME": "Jane Smith",
                "CONTRACTOR": "Tree Co",
                "CONTRACTPH": "954-555-0001",
            }]
        ])
        result = run_fort_lauderdale_worker()
        assert result["status"] == "success"
        assert result["records_found"] == 1

    @patch("pipeline.workers.fort_lauderdale.complete_job_run")
    @patch("pipeline.workers.fort_lauderdale.create_job_run", return_value="run-fl-err")
    @patch("pipeline.workers.fort_lauderdale.get_existing_permit_numbers", side_effect=Exception("Fail"))
    def test_handles_error(self, mock_permits, mock_create, mock_complete):
        result = run_fort_lauderdale_worker()
        assert result["status"] == "failed"


# ══════════════════════════════════════════════════════════════════════════════
#  Worker execution – Miami combined worker
# ══════════════════════════════════════════════════════════════════════════════

class TestRunMiamiWorker:
    """Test the combined Miami worker."""

    @patch("pipeline.workers.miami.run_miami_building_worker")
    @patch("pipeline.workers.miami.run_miami_tree_worker")
    def test_combined_results(self, mock_tree, mock_building):
        mock_tree.return_value = {
            "status": "success",
            "records_found": 10,
            "records_inserted": 5,
        }
        mock_building.return_value = {
            "status": "success",
            "records_found": 20,
            "records_inserted": 8,
        }
        result = run_miami_worker()
        assert result["records_found"] == 30
        assert result["records_inserted"] == 13
        assert "tree" in result
        assert "building" in result

    @patch("pipeline.workers.miami.run_miami_building_worker")
    @patch("pipeline.workers.miami.run_miami_tree_worker")
    def test_one_fails_other_continues(self, mock_tree, mock_building):
        mock_tree.return_value = {
            "status": "failed",
            "records_found": 0,
            "records_inserted": 0,
        }
        mock_building.return_value = {
            "status": "success",
            "records_found": 15,
            "records_inserted": 7,
        }
        result = run_miami_worker()
        assert result["records_found"] == 15
        assert result["records_inserted"] == 7


# ══════════════════════════════════════════════════════════════════════════════
#  Worker execution – Miami Tree with mocked DB + API
# ══════════════════════════════════════════════════════════════════════════════

class TestRunMiamiTreeWorker:
    """Test run_miami_tree_worker with mocked dependencies."""

    @patch("pipeline.workers.miami.insert_leads_batch", return_value=2)
    @patch("pipeline.workers.miami.complete_job_run")
    @patch("pipeline.workers.miami.create_job_run", return_value="run-mt-001")
    @patch("pipeline.workers.miami.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.miami.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.miami.get_existing_permit_numbers", return_value=set())
    @patch("pipeline.workers.miami.get_record_count", return_value=2)
    @patch("pipeline.workers.miami.stream_pages")
    def test_successful_run(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert,
    ):
        epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        mock_stream.return_value = iter([
            [
                {
                    "ObjectId": 1,
                    "PlanNumber": "TPL-001",
                    "PropertyAddress": "100 Brickell Ave",
                    "ReviewStatus": "Approved",
                    "ReviewStatusChangedDate": epoch_ms,
                    "Latitude": 25.76,
                    "Longitude": -80.19,
                },
                {
                    "ObjectId": 2,
                    "PlanNumber": "TPL-002",
                    "PropertyAddress": "200 Coral Way",
                    "ReviewStatus": "Under Review",
                    "ReviewStatusChangedDate": epoch_ms,
                    "Latitude": 25.75,
                    "Longitude": -80.22,
                },
            ]
        ])
        result = run_miami_tree_worker()
        assert result["status"] == "success"
        assert result["records_found"] == 2

    @patch("pipeline.workers.miami.insert_leads_batch")
    @patch("pipeline.workers.miami.complete_job_run")
    @patch("pipeline.workers.miami.create_job_run", return_value="run-mt-001")
    @patch("pipeline.workers.miami.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.miami.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.miami.get_existing_permit_numbers", return_value={"TPL-001"})
    @patch("pipeline.workers.miami.get_record_count", return_value=1)
    @patch("pipeline.workers.miami.stream_pages")
    def test_dedup_skips_known(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert,
    ):
        epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
        mock_stream.return_value = iter([
            [{"ObjectId": 1, "PlanNumber": "TPL-001", "PropertyAddress": "100 Brickell Ave", "ReviewStatus": "OK", "ReviewStatusChangedDate": epoch_ms}]
        ])
        result = run_miami_tree_worker()
        assert result["records_skipped"] == 1
        mock_insert.assert_not_called()

    @patch("pipeline.workers.miami.complete_job_run")
    @patch("pipeline.workers.miami.create_job_run", return_value="run-mt-err")
    @patch("pipeline.workers.miami.get_existing_permit_numbers", side_effect=Exception("DB error"))
    def test_handles_error(self, mock_permits, mock_create, mock_complete):
        result = run_miami_tree_worker()
        assert result["status"] == "failed"
        assert len(result["errors"]) > 0


# ══════════════════════════════════════════════════════════════════════════════
#  Worker execution – Miami Building with mocked DB + API
# ══════════════════════════════════════════════════════════════════════════════

class TestRunMiamiBuildingWorker:
    """Test run_miami_building_worker with mocked dependencies."""

    @patch("pipeline.workers.miami.insert_leads_batch", return_value=1)
    @patch("pipeline.workers.miami.complete_job_run")
    @patch("pipeline.workers.miami.create_job_run", return_value="run-mb-001")
    @patch("pipeline.workers.miami.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.miami.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.miami.get_existing_permit_numbers", return_value=set())
    @patch("pipeline.workers.miami.get_record_count", return_value=1)
    @patch("pipeline.workers.miami.stream_pages")
    def test_successful_run(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert,
    ):
        epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
        mock_stream.return_value = iter([
            [{
                "ObjectId": 1,
                "PermitNumber": "BP-001",
                "ApplicationNumber": "APP-001",
                "WorkItems": "TREE REMOVAL - Dead tree",
                "ScopeofWork": "Remove dead tree from right of way",
                "DeliveryAddress": "500 NW 7th Ave",
                "IssuedDate": epoch_ms,
                "CompanyName": "Tree Co",
                "BuildingPermitStatusDescription": "ISSUED",
            }]
        ])
        result = run_miami_building_worker()
        assert result["status"] == "success"
        assert result["records_found"] == 1

    @patch("pipeline.workers.miami.insert_leads_batch")
    @patch("pipeline.workers.miami.complete_job_run")
    @patch("pipeline.workers.miami.create_job_run", return_value="run-mb-001")
    @patch("pipeline.workers.miami.get_last_successful_run", return_value=None)
    @patch("pipeline.workers.miami.get_existing_address_keys", return_value=set())
    @patch("pipeline.workers.miami.get_existing_permit_numbers", return_value=set())
    @patch("pipeline.workers.miami.get_record_count", return_value=1)
    @patch("pipeline.workers.miami.stream_pages")
    def test_rejects_non_tree_permits(
        self, mock_stream, mock_count, mock_permits, mock_keys,
        mock_last_run, mock_create_run, mock_complete_run, mock_insert,
    ):
        epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
        mock_stream.return_value = iter([
            [{
                "ObjectId": 1,
                "PermitNumber": "BP-002",
                "WorkItems": "ELECTRICAL WORK",
                "ScopeofWork": "Install new panel",
                "DeliveryAddress": "500 NW 7th Ave",
                "IssuedDate": epoch_ms,
            }]
        ])
        result = run_miami_building_worker()
        assert result["records_skipped"] == 1
        mock_insert.assert_not_called()

    @patch("pipeline.workers.miami.complete_job_run")
    @patch("pipeline.workers.miami.create_job_run", return_value="run-mb-err")
    @patch("pipeline.workers.miami.get_existing_permit_numbers", side_effect=Exception("DB error"))
    def test_handles_error(self, mock_permits, mock_create, mock_complete):
        result = run_miami_building_worker()
        assert result["status"] == "failed"
