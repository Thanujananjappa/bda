"""Background scheduler for automated weekly dropout risk scoring."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import (  # noqa: E402
    SCORING_CRON_DAY,
    SCORING_CRON_HOUR,
    SCORING_INTERVAL_MINUTES,
    WEEKLY_SCHEDULER_ENABLED,
)
from ml.weekly_scoring import run_weekly_scoring  # noqa: E402

logger = logging.getLogger("weekly_scheduler")
_scheduler: BackgroundScheduler | None = None


def _score_job() -> None:
    try:
        run_weekly_scoring()
        logger.info("Scheduled weekly scoring completed.")
    except Exception:
        logger.exception("Scheduled weekly scoring failed.")


def start_scheduler() -> BackgroundScheduler | None:
    global _scheduler
    if not WEEKLY_SCHEDULER_ENABLED:
        logger.info("Weekly scheduler disabled (WEEKLY_SCHEDULER_ENABLED=false).")
        return None
    if _scheduler is not None:
        return _scheduler

    _scheduler = BackgroundScheduler(daemon=True)
    if SCORING_INTERVAL_MINUTES > 0:
        _scheduler.add_job(
            _score_job,
            IntervalTrigger(minutes=SCORING_INTERVAL_MINUTES),
            id="weekly_scoring_interval",
            replace_existing=True,
        )
        logger.info(
            "Scoring job scheduled every %s minute(s).", SCORING_INTERVAL_MINUTES
        )
    else:
        _scheduler.add_job(
            _score_job,
            CronTrigger(day_of_week=SCORING_CRON_DAY, hour=SCORING_CRON_HOUR, minute=0),
            id="weekly_scoring_cron",
            replace_existing=True,
        )
        logger.info(
            "Scoring job scheduled weekly on %s at %s:00 UTC.",
            SCORING_CRON_DAY,
            SCORING_CRON_HOUR,
        )

    _scheduler.start()
    return _scheduler


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
