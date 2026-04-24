"""Daily cron: fetch latest GSC + GA4 data and upsert to DB."""

import logging
import sys
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# GSC has a ~2-day lag; using 2 days back is safe for both GSC and GA4.
_LAG_DAYS = 2


def _cron_date_range() -> tuple[str, str]:
    """Always fetch the last 7 days of available data.

    End: today − LAG_DAYS (the most recent day GA4/GSC has processed).
    Start: end − 6 days (7-day window, re-fetched with ON CONFLICT DO UPDATE).
    """
    end = date.today() - timedelta(days=_LAG_DAYS)
    start = end - timedelta(days=6)
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
