"""Unit tests for pipeline.db – Supabase database operations (mocked)."""
import pytest
import json
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, PropertyMock

from pipeline.db import (
    get_client,
    reset_client,
    insert_lead,
    insert_leads_batch,
    get_existing_permit_numbers,
    get_existing_address_keys,
    update_lead_status,
    create_job_run,
    complete_job_run,
    get_last_successful_run,
    get_recent_job_runs,
)


@pytest.fixture(autouse=True)
def reset_db_client():
    """Reset the DB client singleton between tests."""
    reset_client()
    yield
    reset_client()


def _mock_execute(data=None):
    """Create a mock execute() result."""
    result = MagicMock()
    result.data = data or []
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  get_client
# ══════════════════════════════════════════════════════════════════════════════

class TestGetClient:
    """Tests for Supabase client initialization."""

    @patch("pipeline.db.SUPABASE_URL", "")
    @patch("pipeline.db.SUPABASE_KEY", "test-key")
    def test_raises_without_url(self):
        with pytest.raises(ValueError, match="SUPABASE_URL"):
            get_client()

    @patch("pipeline.db.SUPABASE_URL", "https://test.supabase.co")
    @patch("pipeline.db.SUPABASE_KEY", "")
    def test_raises_without_key(self):
        with pytest.raises(ValueError, match="SUPABASE_KEY"):
            get_client()

    @patch("pipeline.db.create_client")
    @patch("pipeline.db.SUPABASE_URL", "https://test.supabase.co")
    @patch("pipeline.db.SUPABASE_KEY", "test-key-123")
    def test_creates_client_once(self, mock_create):
        mock_create.return_value = MagicMock()
        client1 = get_client()
        client2 = get_client()
        assert client1 is client2
        mock_create.assert_called_once_with("https://test.supabase.co", "test-key-123")


# ══════════════════════════════════════════════════════════════════════════════
#  insert_lead
# ══════════════════════════════════════════════════════════════════════════════

class TestInsertLead:
    """Tests for inserting individual leads."""

    @patch("pipeline.db.get_client")
    def test_successful_insert(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value = _mock_execute([{"id": "abc"}])
        mock_get_client.return_value = mock_client

        lead = {
            "source_name": "fort_lauderdale",
            "jurisdiction": "City of Fort Lauderdale",
            "address": "123 Main St",
            "normalized_address": "123 MAIN STREET",
            "permit_type": "TREE REMOVAL",
            "permit_description": "Remove dead tree",
            "permit_number": "FL-001",
            "permit_date": "2025-03-15",
            "lead_score": 8,
            "raw_payload": {"OBJECTID": 1},
        }
        lead_id = insert_lead(lead)
        assert lead_id is not None
        mock_client.table.assert_called_with("leads")

    @patch("pipeline.db.get_client")
    def test_failed_insert_returns_none(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.side_effect = Exception("DB error")
        mock_get_client.return_value = mock_client

        result = insert_lead({"source_name": "test"})
        assert result is None

    @patch("pipeline.db.get_client")
    def test_raw_payload_serialized_to_json(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value = _mock_execute()
        mock_get_client.return_value = mock_client

        payload = {"key": "value", "nested": {"a": 1}}
        insert_lead({"raw_payload": payload})

        # Get the record that was inserted
        insert_call = mock_client.table.return_value.insert.call_args[0][0]
        assert insert_call["raw_payload_json"] == json.dumps(payload)


# ══════════════════════════════════════════════════════════════════════════════
#  insert_leads_batch
# ══════════════════════════════════════════════════════════════════════════════

class TestInsertLeadsBatch:
    """Tests for batch lead insertion."""

    @patch("pipeline.db.get_client")
    def test_batch_insert_returns_count(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value = _mock_execute([
            {"id": "a"}, {"id": "b"}, {"id": "c"},
        ])
        mock_get_client.return_value = mock_client

        leads = [
            {"source_name": "test", "raw_payload": {}},
            {"source_name": "test", "raw_payload": {}},
            {"source_name": "test", "raw_payload": {}},
        ]
        count = insert_leads_batch(leads)
        assert count == 3

    @patch("pipeline.db.get_client")
    def test_empty_batch_returns_zero(self, mock_get_client):
        count = insert_leads_batch([])
        assert count == 0

    @patch("pipeline.db.get_client")
    def test_batch_error_returns_zero(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.side_effect = Exception("Batch failed")
        mock_get_client.return_value = mock_client

        count = insert_leads_batch([{"source_name": "test", "raw_payload": {}}])
        assert count == 0


# ══════════════════════════════════════════════════════════════════════════════
#  get_existing_permit_numbers
# ══════════════════════════════════════════════════════════════════════════════

class TestGetExistingPermitNumbers:
    """Tests for permit number dedup lookups."""

    @patch("pipeline.db.get_client")
    def test_returns_set_of_numbers(self, mock_get_client):
        mock_client = MagicMock()
        mock_query = mock_client.table.return_value.select.return_value
        # First page has data, second page is empty (pagination end)
        mock_query.range.return_value.execute.side_effect = [
            _mock_execute([
                {"permit_number": "FL-001"},
                {"permit_number": "FL-002"},
                {"permit_number": "FL-003"},
            ]),
            _mock_execute([]),
        ]
        mock_get_client.return_value = mock_client

        numbers = get_existing_permit_numbers()
        assert numbers == {"FL-001", "FL-002", "FL-003"}

    @patch("pipeline.db.get_client")
    def test_filters_by_source(self, mock_get_client):
        mock_client = MagicMock()
        mock_query = mock_client.table.return_value.select.return_value
        mock_eq = mock_query.eq.return_value
        mock_eq.range.return_value.execute.return_value = _mock_execute([])
        mock_get_client.return_value = mock_client

        get_existing_permit_numbers("fort_lauderdale")
        mock_query.eq.assert_called_with("source_name", "fort_lauderdale")

    @patch("pipeline.db.get_client")
    def test_skips_null_permit_numbers(self, mock_get_client):
        mock_client = MagicMock()
        mock_query = mock_client.table.return_value.select.return_value
        mock_query.range.return_value.execute.side_effect = [
            _mock_execute([
                {"permit_number": "FL-001"},
                {"permit_number": None},
                {"permit_number": "FL-003"},
            ]),
            _mock_execute([]),
        ]
        mock_get_client.return_value = mock_client

        numbers = get_existing_permit_numbers()
        assert numbers == {"FL-001", "FL-003"}


# ══════════════════════════════════════════════════════════════════════════════
#  update_lead_status
# ══════════════════════════════════════════════════════════════════════════════

class TestUpdateLeadStatus:
    """Tests for updating lead status."""

    @patch("pipeline.db.get_client")
    def test_successful_update(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        result = update_lead_status("lead-123", "approved")
        assert result is True
        mock_client.table.return_value.update.assert_called_with({"lead_status": "approved"})

    @patch("pipeline.db.get_client")
    def test_failed_update_returns_false(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.update.return_value.eq.return_value.execute.side_effect = Exception("Error")
        mock_get_client.return_value = mock_client
        result = update_lead_status("lead-123", "rejected")
        assert result is False


# ══════════════════════════════════════════════════════════════════════════════
#  Job Runs
# ══════════════════════════════════════════════════════════════════════════════

class TestJobRuns:
    """Tests for job_runs table operations."""

    @patch("pipeline.db.get_client")
    def test_create_job_run_returns_id(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        run_id = create_job_run("derm_worker", "miami_dade_derm")
        assert run_id  # Should be a UUID string
        assert len(run_id) == 36  # UUID format

    @patch("pipeline.db.get_client")
    def test_complete_job_run(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        complete_job_run("run-123", "success", records_found=50, records_inserted=10)
        update_call = mock_client.table.return_value.update.call_args[0][0]
        assert update_call["status"] == "success"
        assert update_call["records_found"] == 50
        assert update_call["records_inserted"] == 10

    @patch("pipeline.db.get_client")
    def test_complete_job_run_with_error(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        complete_job_run("run-123", "failed", error_message="Connection refused")
        update_call = mock_client.table.return_value.update.call_args[0][0]
        assert update_call["status"] == "failed"
        assert update_call["error_message"] == "Connection refused"

    @patch("pipeline.db.get_client")
    def test_error_message_truncated_to_2000(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        long_error = "x" * 5000
        complete_job_run("run-123", "failed", error_message=long_error)
        update_call = mock_client.table.return_value.update.call_args[0][0]
        assert len(update_call["error_message"]) == 2000

    @patch("pipeline.db.get_client")
    def test_get_last_successful_run(self, mock_get_client):
        mock_client = MagicMock()
        mock_result = _mock_execute([{"id": "run-1", "finished_at": "2025-03-15T06:05:00Z"}])
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = mock_result
        mock_get_client.return_value = mock_client

        result = get_last_successful_run("miami_dade_derm")
        assert result is not None
        assert result["id"] == "run-1"

    @patch("pipeline.db.get_client")
    def test_get_last_successful_run_none(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = _mock_execute([])
        mock_get_client.return_value = mock_client

        result = get_last_successful_run("nonexistent")
        assert result is None

    @patch("pipeline.db.get_client")
    def test_get_recent_job_runs(self, mock_get_client):
        mock_client = MagicMock()
        runs = [{"id": "r1"}, {"id": "r2"}]
        mock_client.table.return_value.select.return_value.order.return_value.limit.return_value.execute.return_value = _mock_execute(runs)
        mock_get_client.return_value = mock_client

        result = get_recent_job_runs(limit=5)
        assert len(result) == 2

    @patch("pipeline.db.get_client")
    def test_get_recent_job_runs_error(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.order.return_value.limit.return_value.execute.side_effect = Exception("DB error")
        mock_get_client.return_value = mock_client

        result = get_recent_job_runs()
        assert result == []
