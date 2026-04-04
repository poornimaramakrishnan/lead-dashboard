"""Main entry point - APScheduler-based daily pipeline runner.

Usage:
  python -m pipeline.main          # Start scheduler (runs daily at configured time)
  python -m pipeline.main --once   # Run all workers once immediately
"""
import sys
import logging
from datetime import datetime, timezone

from pipeline.config import SCHEDULE_HOUR, SCHEDULE_MINUTE
from pipeline.workers.derm import run_derm_worker
from pipeline.workers.fort_lauderdale import run_fort_lauderdale_worker
from pipeline.workers.miami import run_miami_tree_worker, run_miami_building_worker
from pipeline.notifications import send_daily_digest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_all_workers():
    """Run all source workers in sequence and send daily digest."""
    logger.info("=" * 60)
    logger.info("Starting daily pipeline run at %s", datetime.now(timezone.utc))
    logger.info("=" * 60)

    results = {}

    # Source 1: Miami-Dade DERM (priority: first)
    logger.info("Running Source 1: Miami-Dade DERM Tree Permits...")
    try:
        results["miami_dade_derm"] = run_derm_worker()
    except Exception as exc:
        logger.error("DERM worker crashed: %s", exc, exc_info=True)
        results["miami_dade_derm"] = {
            "status": "failed",
            "records_found": 0,
            "records_inserted": 0,
            "errors": [str(exc)],
        }

    # Source 2: Fort Lauderdale (priority: second)
    logger.info("Running Source 2: Fort Lauderdale Building Permits...")
    try:
        results["fort_lauderdale"] = run_fort_lauderdale_worker()
    except Exception as exc:
        logger.error("Fort Lauderdale worker crashed: %s", exc, exc_info=True)
        results["fort_lauderdale"] = {
            "status": "failed",
            "records_found": 0,
            "records_inserted": 0,
            "errors": [str(exc)],
        }

    # Source 3a: City of Miami Tree Permits (priority: third)
    logger.info("Running Source 3a: City of Miami Tree Permits...")
    try:
        results["city_of_miami_tree"] = run_miami_tree_worker()
    except Exception as exc:
        logger.error("Miami Tree worker crashed: %s", exc, exc_info=True)
        results["city_of_miami_tree"] = {
            "status": "failed",
            "records_found": 0,
            "records_inserted": 0,
            "errors": [str(exc)],
        }

    # Source 3b: City of Miami Building Permits
    logger.info("Running Source 3b: City of Miami Building Permits...")
    try:
        results["city_of_miami"] = run_miami_building_worker()
    except Exception as exc:
        logger.error("Miami Building worker crashed: %s", exc, exc_info=True)
        results["city_of_miami"] = {
            "status": "failed",
            "records_found": 0,
            "records_inserted": 0,
            "errors": [str(exc)],
        }

    # Summary
    total_found = sum(
        r.get("records_found", 0) for r in results.values() if isinstance(r, dict)
    )
    total_inserted = sum(
        r.get("records_inserted", 0) for r in results.values() if isinstance(r, dict)
    )
    failed = [
        name for name, r in results.items()
        if isinstance(r, dict) and r.get("status") == "failed"
    ]

    logger.info("=" * 60)
    logger.info(
        "Pipeline complete: found=%d, inserted=%d, failed=%s",
        total_found, total_inserted, failed or "none",
    )
    logger.info("=" * 60)

    # Send daily digest
    logger.info("Sending daily email digest...")
    send_daily_digest(results)

    return results


def start_scheduler():
    """Start the APScheduler blocking scheduler for daily runs."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()

    # Schedule each worker independently (10-minute gaps)
    scheduler.add_job(
        run_derm_worker,
        "cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        id="derm_worker",
        name="Miami-Dade DERM Tree Permits",
    )
    scheduler.add_job(
        run_fort_lauderdale_worker,
        "cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE + 10,
        id="fort_lauderdale_worker",
        name="Fort Lauderdale Building Permits",
    )
    scheduler.add_job(
        run_miami_tree_worker,
        "cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE + 20,
        id="miami_tree_worker",
        name="City of Miami Tree Permits",
    )
    scheduler.add_job(
        run_miami_building_worker,
        "cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE + 30,
        id="miami_building_worker",
        name="City of Miami Building Permits",
    )

    # Daily digest at +40 minutes
    scheduler.add_job(
        lambda: send_daily_digest({}),
        "cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE + 40,
        id="daily_digest",
        name="Daily Email Digest",
    )

    logger.info(
        "Scheduler started. Workers will run daily at %02d:%02d",
        SCHEDULE_HOUR, SCHEDULE_MINUTE,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    if "--once" in sys.argv:
        run_all_workers()
    else:
        start_scheduler()
