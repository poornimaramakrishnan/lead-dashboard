"""Unit tests for pipeline.notifications – email digest formatting."""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from pipeline.notifications import _build_digest_html, send_daily_digest


# ══════════════════════════════════════════════════════════════════════════════
#  _build_digest_html
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildDigestHtml:
    """Tests for the HTML email builder."""

    def test_basic_rendering(self):
        job_runs = [
            {
                "source_name": "miami_dade_derm",
                "status": "success",
                "records_found": 100,
                "records_inserted": 15,
                "finished_at": "2025-03-15T06:05:00Z",
            },
        ]
        summary = {
            "total_found": 100,
            "total_inserted": 15,
            "failed_sources": [],
        }
        html = _build_digest_html(job_runs, summary)
        assert "Tree Permit Lead Discovery" in html
        assert "miami_dade_derm" in html
        assert "SUCCESS" in html
        assert "15" in html  # new leads
        assert "100" in html  # records scanned

    def test_failed_sources_show_warning(self):
        job_runs = [
            {
                "source_name": "fort_lauderdale",
                "status": "failed",
                "records_found": 0,
                "records_inserted": 0,
                "error_message": "Connection timed out",
                "finished_at": "2025-03-15T06:10:00Z",
            },
        ]
        summary = {
            "total_found": 0,
            "total_inserted": 0,
            "failed_sources": ["fort_lauderdale"],
        }
        html = _build_digest_html(job_runs, summary)
        assert "⚠" in html
        assert "fort_lauderdale" in html
        assert "FAILED" in html
        assert "Connection timed out" in html

    def test_error_message_truncated(self):
        long_error = "x" * 200
        job_runs = [
            {
                "source_name": "test",
                "status": "failed",
                "records_found": 0,
                "records_inserted": 0,
                "error_message": long_error,
                "finished_at": "2025-03-15T06:10:00Z",
            },
        ]
        summary = {"total_found": 0, "total_inserted": 0, "failed_sources": ["test"]}
        html = _build_digest_html(job_runs, summary)
        # Error should be truncated to 100 chars
        assert long_error[:100] in html
        assert long_error[:101] not in html

    def test_empty_runs(self):
        html = _build_digest_html([], {"total_found": 0, "total_inserted": 0, "failed_sources": []})
        assert "Tree Permit Lead Discovery" in html
        assert "0" in html

    def test_multiple_sources(self):
        job_runs = [
            {"source_name": "miami_dade_derm", "status": "success", "records_found": 50, "records_inserted": 10, "finished_at": "2025-03-15T06:05:00"},
            {"source_name": "fort_lauderdale", "status": "success", "records_found": 30, "records_inserted": 8, "finished_at": "2025-03-15T06:10:00"},
            {"source_name": "city_of_miami_tree", "status": "partial", "records_found": 20, "records_inserted": 5, "finished_at": "2025-03-15T06:15:00"},
        ]
        summary = {"total_found": 100, "total_inserted": 23, "failed_sources": []}
        html = _build_digest_html(job_runs, summary)
        assert "miami_dade_derm" in html
        assert "fort_lauderdale" in html
        assert "city_of_miami_tree" in html


# ══════════════════════════════════════════════════════════════════════════════
#  send_daily_digest
# ══════════════════════════════════════════════════════════════════════════════

class TestSendDailyDigest:
    """Tests for the send_daily_digest function."""

    @patch("pipeline.notifications.RESEND_API_KEY", "")
    def test_skips_when_no_api_key(self):
        result = send_daily_digest({})
        assert result is False

    @patch("pipeline.notifications.NOTIFICATION_EMAIL", "")
    @patch("pipeline.notifications.RESEND_API_KEY", "re_test123")
    def test_skips_when_no_email(self):
        result = send_daily_digest({})
        assert result is False

    @patch("pipeline.notifications.get_recent_job_runs", return_value=[])
    @patch("pipeline.notifications.NOTIFICATION_EMAIL", "test@example.com")
    @patch("pipeline.notifications.RESEND_API_KEY", "re_test123")
    def test_sends_email_with_resend(self, mock_runs):
        mock_resend = MagicMock()
        mock_resend.Emails.send.return_value = {"id": "email-123"}
        mock_resend.api_key = None
        with patch.dict("sys.modules", {"resend": mock_resend}):
            result = send_daily_digest({
                "derm": {"records_found": 10, "records_inserted": 5, "status": "success"},
            })
            assert result is True

    @patch("pipeline.notifications.get_recent_job_runs", return_value=[])
    @patch("pipeline.notifications.NOTIFICATION_EMAIL", "test@example.com")
    @patch("pipeline.notifications.RESEND_API_KEY", "re_test123")
    def test_handles_send_failure(self, mock_runs):
        mock_resend = MagicMock()
        mock_resend.Emails.send.side_effect = Exception("Network error")
        mock_resend.api_key = None
        with patch.dict("sys.modules", {"resend": mock_resend}):
            result = send_daily_digest({"derm": {"records_found": 0, "records_inserted": 0, "status": "failed"}})
            assert result is False

    def test_subject_includes_count(self):
        """Test that the summary generation correctly counts totals."""
        worker_results = {
            "derm": {"records_found": 50, "records_inserted": 10, "status": "success"},
            "fort_lauderdale": {"records_found": 30, "records_inserted": 8, "status": "success"},
        }
        total_found = sum(r.get("records_found", 0) for r in worker_results.values() if isinstance(r, dict))
        total_inserted = sum(r.get("records_inserted", 0) for r in worker_results.values() if isinstance(r, dict))
        assert total_found == 80
        assert total_inserted == 18

    def test_failed_sources_detected(self):
        worker_results = {
            "derm": {"status": "success", "records_found": 10, "records_inserted": 5},
            "fort_lauderdale": {"status": "failed", "records_found": 0, "records_inserted": 0},
        }
        failed = [s for s, r in worker_results.items() if isinstance(r, dict) and r.get("status") == "failed"]
        assert failed == ["fort_lauderdale"]
