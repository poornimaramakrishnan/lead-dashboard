"""Configuration module - loads all settings from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()


# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# ── Email ─────────────────────────────────────────────────────────────────────
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL", "")

# ── Source Kill Switches ──────────────────────────────────────────────────────
# DERM disabled: 2026 records have no address data (FOLIO=0, all address fields
# null). The FOLIO→address cross-reference only works for older records that
# have already been processed. Re-enable when DERM populates FOLIO on new
# applications, then use PaParcelView_gdb to enrich addresses.
MIAMI_DADE_DERM_ACTIVE = os.getenv("MIAMI_DADE_DERM_ACTIVE", "false").lower() == "true"
FORT_LAUDERDALE_ACTIVE = os.getenv("FORT_LAUDERDALE_ACTIVE", "true").lower() == "true"
CITY_OF_MIAMI_ACTIVE = os.getenv("CITY_OF_MIAMI_ACTIVE", "true").lower() == "true"

# ── Schedule ──────────────────────────────────────────────────────────────────
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "6"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))
INITIAL_LOOKBACK_DAYS = int(os.getenv("INITIAL_LOOKBACK_DAYS", "90"))

# ── ArcGIS Endpoints ─────────────────────────────────────────────────────────
DERM_PERMITS_URL = (
    "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/ArcGIS/rest/services"
    "/DermPermits/FeatureServer/0"
)
FORT_LAUDERDALE_URL = (
    "https://gis.fortlauderdale.gov/arcgis/rest/services"
    "/BuildingPermitTracker/BuildingPermitTracker/MapServer/0"
)
MIAMI_TREE_PERMITS_URL = (
    "https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services"
    "/Tree_Permits/FeatureServer/0"
)
MIAMI_BUILDING_PERMITS_URL = (
    "https://services1.arcgis.com/CvuPhqcTQpZPT9qY/arcgis/rest/services"
    "/Building_Permits_Since_2014/FeatureServer/0"
)

# ── ArcGIS Query Settings ────────────────────────────────────────────────────
PAGE_SIZE = 1000
REQUEST_DELAY_SECONDS = 1.5  # Throttle between paginated requests
MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 2  # Exponential: 2s, 4s, 8s

# ── Geo Bounding Box (South Florida) ─────────────────────────────────────────
BBOX_LAT_MIN = 25.1
BBOX_LAT_MAX = 26.4
BBOX_LON_MIN = -80.9
BBOX_LON_MAX = -80.0

# ── Data Quality ──────────────────────────────────────────────────────────────
MAX_PERMIT_AGE_DAYS = 90  # Hard ceiling: no lead older than this is ever stored
SKIP_RATE_WARNING_THRESHOLD = 0.20  # 20%
