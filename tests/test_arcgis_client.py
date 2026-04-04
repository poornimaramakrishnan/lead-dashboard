"""Unit tests for pipeline.arcgis_client – ArcGIS REST API client."""
import pytest
import json
import time
from unittest.mock import patch, MagicMock
import requests

from pipeline.arcgis_client import (
    query_arcgis,
    get_record_count,
    stream_pages,
    validate_schema,
    ArcGISError,
    ArcGISAccessDenied,
    ArcGISBadRequest,
)

BASE_URL = "https://test.arcgis.com/services/TestLayer/FeatureServer/0"


def _make_response(status_code=200, json_data=None, text=""):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = json_data or {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"{status_code} Error"
        )
    return resp


# ══════════════════════════════════════════════════════════════════════════════
#  query_arcgis
# ══════════════════════════════════════════════════════════════════════════════

class TestQueryArcgis:
    """Tests for the query_arcgis function."""

    @patch("pipeline.arcgis_client.requests.get")
    def test_successful_query(self, mock_get):
        mock_get.return_value = _make_response(200, {
            "features": [
                {"attributes": {"OBJECTID": 1, "NAME": "Test"}},
            ]
        })
        result = query_arcgis(BASE_URL, where="1=1")
        assert "features" in result
        assert len(result["features"]) == 1
        mock_get.assert_called_once()

    @patch("pipeline.arcgis_client.requests.get")
    def test_query_params_passed_correctly(self, mock_get):
        mock_get.return_value = _make_response(200, {"features": []})
        query_arcgis(
            BASE_URL,
            where="NAME='test'",
            out_fields="NAME,ID",
            order_by="ID ASC",
            result_offset=100,
            result_record_count=50,
        )
        call_kwargs = mock_get.call_args
        params = call_kwargs[1]["params"] if "params" in call_kwargs[1] else call_kwargs[0][1] if len(call_kwargs[0]) > 1 else call_kwargs[1].get("params", {})
        assert params["where"] == "NAME='test'"
        assert params["outFields"] == "NAME,ID"
        assert params["orderByFields"] == "ID ASC"
        assert params["resultOffset"] == "100"
        assert params["resultRecordCount"] == "50"

    @patch("pipeline.arcgis_client.requests.get")
    def test_400_raises_bad_request(self, mock_get):
        mock_get.return_value = _make_response(400, text="Bad query")
        with pytest.raises(ArcGISBadRequest):
            query_arcgis(BASE_URL)

    @patch("pipeline.arcgis_client.requests.get")
    def test_403_raises_access_denied(self, mock_get):
        mock_get.return_value = _make_response(403)
        with pytest.raises(ArcGISAccessDenied):
            query_arcgis(BASE_URL)

    @patch("pipeline.arcgis_client.time.sleep")  # Don't actually sleep in tests
    @patch("pipeline.arcgis_client.requests.get")
    def test_500_retries_then_fails(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(500)
        with pytest.raises(ArcGISError, match="All .* attempts failed"):
            query_arcgis(BASE_URL)
        # Should retry MAX_RETRY_ATTEMPTS times
        assert mock_get.call_count == 3

    @patch("pipeline.arcgis_client.time.sleep")
    @patch("pipeline.arcgis_client.requests.get")
    def test_429_retries_then_succeeds(self, mock_get, mock_sleep):
        """Rate limit (429) retries and eventually succeeds."""
        mock_get.side_effect = [
            _make_response(429),
            _make_response(429),
            _make_response(200, {"features": []}),
        ]
        result = query_arcgis(BASE_URL)
        assert mock_get.call_count == 3
        assert result == {"features": []}

    @patch("pipeline.arcgis_client.time.sleep")
    @patch("pipeline.arcgis_client.requests.get")
    def test_timeout_retries(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            requests.exceptions.Timeout("Timeout"),
            requests.exceptions.Timeout("Timeout"),
            _make_response(200, {"features": []}),
        ]
        result = query_arcgis(BASE_URL)
        assert mock_get.call_count == 3

    @patch("pipeline.arcgis_client.time.sleep")
    @patch("pipeline.arcgis_client.requests.get")
    def test_connection_error_retries(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("DNS fail"),
            _make_response(200, {"features": []}),
        ]
        result = query_arcgis(BASE_URL)
        assert mock_get.call_count == 2

    @patch("pipeline.arcgis_client.requests.get")
    def test_arcgis_error_in_200_response(self, mock_get):
        """ArcGIS sometimes returns 200 with an error object."""
        mock_get.return_value = _make_response(200, {
            "error": {"code": 500, "message": "Internal error"}
        })
        with pytest.raises(ArcGISError, match="Internal error"):
            query_arcgis(BASE_URL)

    @patch("pipeline.arcgis_client.requests.get")
    def test_arcgis_error_400_in_body(self, mock_get):
        mock_get.return_value = _make_response(200, {
            "error": {"code": 400, "message": "Invalid query"}
        })
        with pytest.raises(ArcGISBadRequest):
            query_arcgis(BASE_URL)

    @patch("pipeline.arcgis_client.requests.get")
    def test_arcgis_error_403_in_body(self, mock_get):
        mock_get.return_value = _make_response(200, {
            "error": {"code": 403, "message": "Access denied"}
        })
        with pytest.raises(ArcGISAccessDenied):
            query_arcgis(BASE_URL)

    @patch("pipeline.arcgis_client.requests.get")
    def test_return_count_only(self, mock_get):
        mock_get.return_value = _make_response(200, {"count": 42})
        result = query_arcgis(BASE_URL, return_count_only=True)
        assert result["count"] == 42
        params = mock_get.call_args[1]["params"]
        assert params["returnCountOnly"] == "true"

    @patch("pipeline.arcgis_client.requests.get")
    def test_extra_params_merged(self, mock_get):
        mock_get.return_value = _make_response(200, {"features": []})
        query_arcgis(BASE_URL, extra_params={"token": "abc123"})
        params = mock_get.call_args[1]["params"]
        assert params["token"] == "abc123"


# ══════════════════════════════════════════════════════════════════════════════
#  get_record_count
# ══════════════════════════════════════════════════════════════════════════════

class TestGetRecordCount:
    """Tests for get_record_count."""

    @patch("pipeline.arcgis_client.requests.get")
    def test_returns_count(self, mock_get):
        mock_get.return_value = _make_response(200, {"count": 16002})
        count = get_record_count(BASE_URL, "WORK_GROUP='TREE'")
        assert count == 16002

    @patch("pipeline.arcgis_client.requests.get")
    def test_missing_count_returns_zero(self, mock_get):
        mock_get.return_value = _make_response(200, {})
        count = get_record_count(BASE_URL)
        assert count == 0


# ══════════════════════════════════════════════════════════════════════════════
#  stream_pages
# ══════════════════════════════════════════════════════════════════════════════

class TestStreamPages:
    """Tests for the stream_pages generator."""

    @patch("pipeline.arcgis_client.REQUEST_DELAY_SECONDS", 0)  # No delay in tests
    @patch("pipeline.arcgis_client.requests.get")
    def test_single_page(self, mock_get):
        mock_get.return_value = _make_response(200, {
            "features": [
                {"attributes": {"ID": 1}},
                {"attributes": {"ID": 2}},
            ]
        })
        pages = list(stream_pages(BASE_URL, page_size=1000))
        assert len(pages) == 1
        assert pages[0] == [{"ID": 1}, {"ID": 2}]

    @patch("pipeline.arcgis_client.REQUEST_DELAY_SECONDS", 0)
    @patch("pipeline.arcgis_client.requests.get")
    def test_multiple_pages(self, mock_get):
        # Page 1: full page (2 records = page_size), Page 2: partial (1 record)
        mock_get.side_effect = [
            _make_response(200, {
                "features": [
                    {"attributes": {"ID": 1}},
                    {"attributes": {"ID": 2}},
                ]
            }),
            _make_response(200, {
                "features": [
                    {"attributes": {"ID": 3}},
                ]
            }),
        ]
        pages = list(stream_pages(BASE_URL, page_size=2))
        assert len(pages) == 2
        assert pages[0] == [{"ID": 1}, {"ID": 2}]
        assert pages[1] == [{"ID": 3}]

    @patch("pipeline.arcgis_client.REQUEST_DELAY_SECONDS", 0)
    @patch("pipeline.arcgis_client.requests.get")
    def test_empty_response_stops(self, mock_get):
        mock_get.return_value = _make_response(200, {"features": []})
        pages = list(stream_pages(BASE_URL))
        assert len(pages) == 0

    @patch("pipeline.arcgis_client.REQUEST_DELAY_SECONDS", 0)
    @patch("pipeline.arcgis_client.requests.get")
    def test_missing_features_key_stops(self, mock_get):
        mock_get.return_value = _make_response(200, {})
        pages = list(stream_pages(BASE_URL))
        assert len(pages) == 0

    @patch("pipeline.arcgis_client.REQUEST_DELAY_SECONDS", 0)
    @patch("pipeline.arcgis_client.requests.get")
    def test_extracts_attributes_from_features(self, mock_get):
        """Features have nested 'attributes' – stream_pages should extract them."""
        mock_get.return_value = _make_response(200, {
            "features": [
                {"attributes": {"NAME": "Test"}, "geometry": {"x": 1, "y": 2}},
            ]
        })
        pages = list(stream_pages(BASE_URL))
        assert pages[0][0] == {"NAME": "Test"}  # Geometry stripped

    @patch("pipeline.arcgis_client.REQUEST_DELAY_SECONDS", 0)
    @patch("pipeline.arcgis_client.requests.get")
    def test_pagination_offsets(self, mock_get):
        """Verify offsets are incremented correctly."""
        mock_get.side_effect = [
            _make_response(200, {"features": [{"attributes": {"ID": i}} for i in range(5)]}),
            _make_response(200, {"features": [{"attributes": {"ID": i}} for i in range(5, 8)]}),
        ]
        pages = list(stream_pages(BASE_URL, page_size=5))
        assert len(pages) == 2
        # Check the offset used in second call
        second_call_params = mock_get.call_args_list[1][1]["params"]
        assert second_call_params["resultOffset"] == "5"


# ══════════════════════════════════════════════════════════════════════════════
#  validate_schema
# ══════════════════════════════════════════════════════════════════════════════

class TestValidateSchema:
    """Tests for validate_schema."""

    @patch("pipeline.arcgis_client.requests.get")
    def test_all_fields_present(self, mock_get):
        mock_get.return_value = _make_response(200, {
            "fields": [
                {"name": "ID"},
                {"name": "NAME"},
                {"name": "ADDRESS"},
            ],
            "maxRecordCount": 2000,
        })
        result = validate_schema(BASE_URL, ["ID", "NAME", "ADDRESS"])
        assert result["valid"] is True
        assert result["missing"] == []
        assert result["max_record_count"] == 2000

    @patch("pipeline.arcgis_client.requests.get")
    def test_missing_fields_detected(self, mock_get):
        mock_get.return_value = _make_response(200, {
            "fields": [{"name": "ID"}, {"name": "NAME"}],
        })
        result = validate_schema(BASE_URL, ["ID", "NAME", "PHONE"])
        assert result["valid"] is False
        assert "PHONE" in result["missing"]

    @patch("pipeline.arcgis_client.requests.get")
    def test_case_insensitive_comparison(self, mock_get):
        mock_get.return_value = _make_response(200, {
            "fields": [{"name": "objectid"}, {"name": "Name"}],
        })
        result = validate_schema(BASE_URL, ["ObjectId", "NAME"])
        assert result["valid"] is True
