"""Email notifications - daily digest via Resend."""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from pipeline.config import RESEND_API_KEY, NOTIFICATION_EMAIL
from pipeline.db import get_recent_job_runs

logger = logging.getLogger(__name__)


def _build_digest_html(
    job_runs: List[Dict[str, Any]],
    summary: Dict[str, Any],
) -> str:
    """Build the daily digest email HTML."""
    now = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # Source status rows
    source_rows = ""
    for run in job_runs:
        status = run.get("status", "unknown")
        status_color = {
            "success": "#22c55e",
            "failed": "#ef4444",
            "partial": "#f59e0b",
            "running": "#3b82f6",
        }.get(status, "#6b7280")
        error_msg = run.get("error_message", "")
        error_html = f'<br><small style="color:#ef4444">{error_msg[:100]}</small>' if error_msg else ""

        source_rows += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb">{run.get('source_name','?')}</td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb">
                <span style="color:{status_color};font-weight:600">{status.upper()}</span>
                {error_html}
            </td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;text-align:center">{run.get('records_found',0)}</td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;text-align:center">{run.get('records_inserted',0)}</td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;font-size:12px">{run.get('finished_at','—')[:19]}</td>
        </tr>
        """

    total_found = summary.get("total_found", 0)
    total_inserted = summary.get("total_inserted", 0)
    failed_sources = summary.get("failed_sources", [])
    failed_html = ""
    if failed_sources:
        failed_list = ", ".join(failed_sources)
        failed_html = f"""
        <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:12px;margin:16px 0">
            <strong style="color:#dc2626">⚠ Sources with errors:</strong> {failed_list}
        </div>
        """

    return f"""
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:640px;margin:0 auto;padding:20px">
        <h2 style="color:#1f2937;margin-bottom:4px">🌳 Tree Permit Lead Discovery</h2>
        <p style="color:#6b7280;margin-top:0">Daily Summary — {now}</p>

        {failed_html}

        <div style="display:flex;gap:16px;margin:20px 0">
            <div style="flex:1;background:#f0fdf4;border-radius:8px;padding:16px;text-align:center">
                <div style="font-size:28px;font-weight:700;color:#166534">{total_inserted}</div>
                <div style="color:#15803d;font-size:14px">New Leads</div>
            </div>
            <div style="flex:1;background:#eff6ff;border-radius:8px;padding:16px;text-align:center">
                <div style="font-size:28px;font-weight:700;color:#1e40af">{total_found}</div>
                <div style="color:#2563eb;font-size:14px">Records Scanned</div>
            </div>
        </div>

        <table style="width:100%;border-collapse:collapse;font-size:14px">
            <thead>
                <tr style="background:#f9fafb">
                    <th style="padding:8px;text-align:left;border-bottom:2px solid #e5e7eb">Source</th>
                    <th style="padding:8px;text-align:left;border-bottom:2px solid #e5e7eb">Status</th>
                    <th style="padding:8px;text-align:center;border-bottom:2px solid #e5e7eb">Found</th>
                    <th style="padding:8px;text-align:center;border-bottom:2px solid #e5e7eb">Inserted</th>
                    <th style="padding:8px;text-align:left;border-bottom:2px solid #e5e7eb">Completed</th>
                </tr>
            </thead>
            <tbody>
                {source_rows}
            </tbody>
        </table>

        <p style="color:#9ca3af;font-size:12px;margin-top:24px;text-align:center">
            Tree Permit Lead Discovery System • Automated Daily Report
        </p>
    </div>
    """


def send_daily_digest(worker_results: Dict[str, Any]) -> bool:
    """
    Send the daily email digest summarizing all worker runs.

    Args:
        worker_results: Dict with keys for each source, containing
                       records_found, records_inserted, status, errors.
    """
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set - skipping email")
        return False

    if not NOTIFICATION_EMAIL:
        logger.warning("NOTIFICATION_EMAIL not set - skipping email")
        return False

    try:
        import resend
        resend.api_key = RESEND_API_KEY

        # Get recent job runs for the digest
        recent_runs = get_recent_job_runs(limit=10)

        # Build summary
        total_found = 0
        total_inserted = 0
        failed_sources = []
        for source, result in worker_results.items():
            if isinstance(result, dict):
                total_found += result.get("records_found", 0)
                total_inserted += result.get("records_inserted", 0)
                if result.get("status") == "failed":
                    failed_sources.append(source)

        summary = {
            "total_found": total_found,
            "total_inserted": total_inserted,
            "failed_sources": failed_sources,
        }

        html = _build_digest_html(recent_runs, summary)

        subject = f"🌳 Lead Digest: {total_inserted} new leads"
        if failed_sources:
            subject = f"⚠ Lead Digest: {total_inserted} new leads ({len(failed_sources)} source errors)"

        params = {
            "from": "Tree Permits <onboarding@resend.dev>",
            "to": [NOTIFICATION_EMAIL],
            "subject": subject,
            "html": html,
        }

        result = resend.Emails.send(params)
        logger.info("Daily digest sent: %s", result)
        return True

    except Exception as exc:
        logger.error("Failed to send daily digest: %s", exc)
        return False
