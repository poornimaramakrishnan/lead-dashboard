"""Shared pytest fixtures for the test suite."""
import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch


# ── Prevent real Supabase / Resend calls ──────────────────────────────────────
# Ensure config uses safe defaults during tests
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key-not-real")
os.environ.setdefault("RESEND_API_KEY", "")
os.environ.setdefault("NOTIFICATION_EMAIL", "test@example.com")


@pytest.fixture
def recent_date():
    """A datetime within the last 7 days (UTC)."""
    return datetime.now(timezone.utc) - timedelta(days=2)


@pytest.fixture
def old_date():
    """A datetime older than 90 days (UTC)."""
    return datetime.now(timezone.utc) - timedelta(days=120)


@pytest.fixture
def stale_date():
    """A datetime between 7 and 90 days old (UTC)."""
    return datetime.now(timezone.utc) - timedelta(days=45)


@pytest.fixture
def sample_derm_attributes():
    """A typical DERM API record (attributes dict)."""
    # FILE_ID encodes a recent date: 2026-03-15 (YYYYMMDDHHMMSSXX format)
    return {
        "ObjectId": 12345,
        "FILE_ID": 2026031510000000,
        "WORK_GROUP_NUMBER": 2600123,
        "PERMIT_NUMBER": "DERM-2025-00123",
        "WORK_GROUP": "TREE",
        "FACILITY_NAME": "JOHN DOE",
        "FACILITY_ADDRESS": "123 NW 5TH ST",
        "HOUSE_NUMBER": "123",
        "STREET_NAME": "NW 5TH ST",
        "CITY": "MIAMI",
        "STATE": "FL",
        "ZIP_CODE": "33130",
        "FOLIO": "01-2345-678-9012",
        "PERMIT_STATUS": "A",
        "PERMIT_STATUS_DESCRIPTION": "NEW APPLICATION",
        "TITLE_CODE": "T",
        "TITLE_CODE_DESCRIPTION": "Tree Permit",
        "PERMIT_TITLE": "TREE REMOVAL",
        "PERMIT_CLASS": "C",
        "PERMIT_CLASS_DESCRIPTION": "Commercial",
    }


@pytest.fixture
def sample_fl_attributes():
    """A typical Fort Lauderdale API record."""
    epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
    return {
        "OBJECTID": 54321,
        "PERMITID": "FL-2025-98765",
        "PERMITTYPE": "BUILDING",
        "PERMITSTAT": "ISSUED",
        "PERMITDESC": "Remove dead tree from front yard",
        "SUBMITDT": epoch_ms,
        "APPROVEDT": epoch_ms + 86400000,
        "PARCELID": "504201010010",
        "FULLADDR": "456 E Las Olas Blvd",
        "OWNERNAME": "Jane Smith",
        "OWNERADDR": "456 E Las Olas Blvd, Fort Lauderdale FL",
        "CONTRACTOR": "All-Star Tree Service",
        "CONTRACTPH": "954-555-0123",
        "ESTCOST": 2500.00,
    }


@pytest.fixture
def sample_miami_tree_attributes():
    """A typical City of Miami Tree Permit record."""
    epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=5)).timestamp() * 1000)
    return {
        "ObjectId": 6001,
        "ID": 7891,
        "PlanNumber": "TPL-2025-0042",
        "PropertyAddress": "789 Brickell Ave",
        "ReviewStatus": "Approved",
        "ReviewStatusChangedDate": epoch_ms,
        "Latitude": 25.7617,
        "Longitude": -80.1918,
    }


@pytest.fixture
def sample_miami_building_attributes():
    """A typical City of Miami Building Permit record."""
    epoch_ms = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
    return {
        "ObjectId": 200001,
        "PermitNumber": "BP-2025-11111",
        "ApplicationNumber": "APP-2025-22222",
        "WorkItems": "TREE REMOVAL - DEAD TREE IN RIGHT OF WAY",
        "ScopeofWork": "Remove dead tree from right-of-way per county order",
        "DeliveryAddress": "100 SW 1ST STREET APT 4B",
        "IssuedDate": epoch_ms,
        "CompanyName": "Miami Tree Experts Inc",
        "CompanyAddress": "500 NW 7th Ave",
        "CompanyCity": "Miami",
        "CompanyZip": "33136",
        "FolioNumber": "01-3456-789-0123",
        "PropertyType": "Residential",
        "BuildingPermitStatusDescription": "ISSUED",
        "TotalCost": 3500.00,
        "Latitude": 25.7743,
        "Longitude": -80.1937,
    }


@pytest.fixture
def mock_supabase_client():
    """Create a mock Supabase client with chained method support."""
    mock = MagicMock()
    # Make table().select().eq() etc. all return mock objects with .execute()
    table_mock = MagicMock()
    mock.table.return_value = table_mock
    return mock
