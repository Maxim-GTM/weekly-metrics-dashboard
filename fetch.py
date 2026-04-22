"""Daily cron: fetch latest GSC + GA4 data and upsert to DB."""

import logging
import sys
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

_ALL_TABLES = ["gsc", "gsc_country", "gsc_page_daily", "gsc_site_daily",
               "ga4", "ga4_traffic", "ga4_events"]

# GSC has a ~2-day lag; using 2 days back is safe for both GSC and GA4.
_LAG_DAYS = 2


def _cron_date_range() -> tuple[str, str]:
    """Compute the incremental fetch range.

    Start: MIN(latest date across all tables), inclusive, to re-fetch the most
    recent date and heal any partial data from a previous run.
    Falls back to 28 days ago if the DB is empty.

    End: today − LAG_DAYS (safe for both GA4 and GSC).
    """
    from db import latest_data_date

    known_dates = [d for t in _ALL_TABLES if (d := latest_data_date(t)) is not None]
    end = date.today() - timedelta(days=_LAG_DAYS)
    start = min(known_dates) if known_dates else end - timedelta(days=28)
    return start.isoformat(), end.isoformat()


def main() -> None:
    from config import is_ga4_configured, is_gsc_configured
    from google_api import fetch_gsc_data, fetch_ga4_data

    start_date, end_date = _cron_date_range()
    log.info("Fetching %s → %s", start_date, end_date)

    errors = []

    if is_gsc_configured():
        try:
            count = fetch_gsc_data(start_date, end_date)
            log.info("GSC: upserted %d rows", count)
        except Exception as exc:
            log.error("GSC fetch failed: %s", exc)
            errors.append(exc)
    else:
        log.warning("GSC not configured, skipping")

    if is_ga4_configured():
        try:
            count = fetch_ga4_data(start_date, end_date)
            log.info("GA4: upserted %d rows", count)
        except Exception as exc:
            log.error("GA4 fetch failed: %s", exc)
            errors.append(exc)
    else:
        log.warning("GA4 not configured, skipping")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
