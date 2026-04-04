"""ArcGIS REST API client with retry logic, pagination, and request throttling."""
import time
import logging
import requests
from typing import Generator, Dict, Any, List, Optional, Set

from pipeline.config import (
    PAGE_SIZE,
    REQUEST_DELAY_SECONDS,
    MAX_RETRY_ATTEMPTS,
    RETRY_BACKOFF_BASE,
)

logger = logging.getLogger(__name__)


class ArcGISError(Exception):
    """Raised when ArcGIS API returns an error."""
    pass


class ArcGISAccessDenied(ArcGISError):
    """Raised on 403 - do not retry."""
    pass


class ArcGISBadRequest(ArcGISError):
    """Raised on 400 - do not retry, fix the query."""
    pass


def _should_retry(status_code: int) -> bool:
    """Determine if a request should be retried based on HTTP status."""
    return status_code in (429, 500, 502, 503, 504)


def query_arcgis(
    base_url: str,
    where: str = "1=1",
    out_fields: str = "*",
    order_by: Optional[str] = None,
    result_offset: int = 0,
    result_record_count: int = PAGE_SIZE,
    return_geometry: bool = False,
    return_count_only: bool = False,
    extra_params: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Execute a single ArcGIS REST query with retry and exponential backoff.

    Raises ArcGISAccessDenied on 403, ArcGISBadRequest on 400.
    Retries on 429/5xx up to MAX_RETRY_ATTEMPTS times.
    """
    url = f"{base_url}/query"
    params = {
        "where": where,
        "outFields": out_fields,
        "resultOffset": str(result_offset),
        "resultRecordCount": str(result_record_count),
        "returnGeometry": str(return_geometry).lower(),
        "f": "json",
    }
    if return_count_only:
        params["returnCountOnly"] = "true"
    if order_by:
        params["orderByFields"] = order_by
    if extra_params:
        params.update(extra_params)

    last_exception = None
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=30)

            # Non-retryable errors
            if response.status_code == 400:
                raise ArcGISBadRequest(
                    f"Bad request (400) querying {base_url}: {response.text[:200]}"
                )
            if response.status_code == 403:
                raise ArcGISAccessDenied(
                    f"Access denied (403) querying {base_url}"
                )

            # Retryable errors
            if _should_retry(response.status_code):
                delay = RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    "Attempt %d/%d failed with %d for %s – retrying in %ds",
                    attempt, MAX_RETRY_ATTEMPTS, response.status_code, base_url, delay,
                )
                time.sleep(delay)
                continue

            response.raise_for_status()
            data = response.json()

            # ArcGIS can return 200 but with an error object
            if "error" in data:
                err = data["error"]
                code = err.get("code", 0)
                msg = err.get("message", str(err))
                if code == 400:
                    raise ArcGISBadRequest(f"ArcGIS error 400: {msg}")
                if code == 403:
                    raise ArcGISAccessDenied(f"ArcGIS error 403: {msg}")
                raise ArcGISError(f"ArcGIS error {code}: {msg}")

            return data

        except (ArcGISBadRequest, ArcGISAccessDenied):
            raise
        except ArcGISError:
            raise
        except requests.exceptions.Timeout:
            delay = RETRY_BACKOFF_BASE ** attempt
            logger.warning(
                "Attempt %d/%d timed out for %s – retrying in %ds",
                attempt, MAX_RETRY_ATTEMPTS, base_url, delay,
            )
            last_exception = requests.exceptions.Timeout(f"Timeout querying {base_url}")
            time.sleep(delay)
        except requests.exceptions.ConnectionError as exc:
            delay = RETRY_BACKOFF_BASE ** attempt
            logger.warning(
                "Attempt %d/%d connection error for %s – retrying in %ds",
                attempt, MAX_RETRY_ATTEMPTS, base_url, delay,
            )
            last_exception = exc
            time.sleep(delay)
        except Exception as exc:
            last_exception = exc
            delay = RETRY_BACKOFF_BASE ** attempt
            logger.warning(
                "Attempt %d/%d unexpected error for %s: %s – retrying in %ds",
                attempt, MAX_RETRY_ATTEMPTS, base_url, exc, delay,
            )
            time.sleep(delay)

    raise ArcGISError(
        f"All {MAX_RETRY_ATTEMPTS} attempts failed for {base_url}: {last_exception}"
    )


def get_record_count(base_url: str, where: str = "1=1") -> int:
    """Get the total count of records matching a query."""
    data = query_arcgis(base_url, where=where, return_count_only=True)
    return data.get("count", 0)


def stream_pages(
    base_url: str,
    where: str = "1=1",
    out_fields: str = "*",
    order_by: Optional[str] = None,
    page_size: int = PAGE_SIZE,
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Generator that yields pages of features from an ArcGIS endpoint.

    Paginates using resultOffset/resultRecordCount. Adds a throttle delay
    between pages to avoid rate limiting. Stops when fewer records than
    page_size are returned.
    """
    offset = 0
    page_num = 0

    while True:
        page_num += 1
        logger.info(
            "Fetching page %d (offset=%d, pageSize=%d) from %s",
            page_num, offset, page_size, base_url,
        )

        data = query_arcgis(
            base_url,
            where=where,
            out_fields=out_fields,
            order_by=order_by,
            result_offset=offset,
            result_record_count=page_size,
        )

        features = data.get("features", [])
        if not features:
            logger.info("No more features returned at offset %d", offset)
            break

        yield [f.get("attributes", {}) for f in features]

        if len(features) < page_size:
            logger.info("Last page reached (%d < %d)", len(features), page_size)
            break

        offset += page_size

        # Throttle to avoid rate limiting
        if REQUEST_DELAY_SECONDS > 0:
            time.sleep(REQUEST_DELAY_SECONDS)


def validate_schema(
    base_url: str,
    expected_fields: List[str],
) -> Dict[str, Any]:
    """
    Validate that an ArcGIS endpoint has the expected fields.

    Returns a dict with 'valid' bool, 'missing' fields, 'extra' fields,
    and the full field list.
    """
    url = f"{base_url}?f=json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    meta = response.json()

    actual_fields = [f.get("name", "") for f in meta.get("fields", [])]
    expected_set = set(f.upper() for f in expected_fields)
    actual_set = set(f.upper() for f in actual_fields)

    missing = expected_set - actual_set
    extra = actual_set - expected_set

    result = {
        "valid": len(missing) == 0,
        "missing": list(missing),
        "extra": list(extra),
        "actual_fields": actual_fields,
        "max_record_count": meta.get("maxRecordCount"),
    }

    if missing:
        logger.warning(
            "Schema validation warning for %s: missing fields %s",
            base_url, missing,
        )

    return result
