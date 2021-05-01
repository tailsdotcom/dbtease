"""CRON routines."""

# import logging
import datetime
from crontab import CronTab


def refresh_due(schedule: str, last_refresh: datetime.datetime) -> bool:
    """Work out whether a refresh is due based on cron and last refresh."""
    if not last_refresh:
        return True
    cron_expr = CronTab(schedule)
    last_schedule_due = datetime.datetime.utcnow() - datetime.timedelta(
        seconds=-cron_expr.previous(default_utc=True)
    )
    # logging.warning(f"SCHEDULE: {schedule}, LAST REFRESH: {last_refresh}, LAST DUE: {last_schedule_due}")
    return last_refresh < last_schedule_due
