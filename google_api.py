"""Google API integration for fetching GSC and GA4 data."""

from datetime import date, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)

def _not_jobs_filter() -> FilterExpression:
    """Exclude pages whose path contains /jobs (careers traffic, not marketing)."""
    return FilterExpression(
        not_expression=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                    value="/jobs",
                    case_sensitive=False,
                ),
            )
        )
    )


TRACKED_EVENT_NAMES = [
    "bifrost_homepage_enterprise_form_submit",
    "bifrost_demo_form_submit",
    "bifrost_enterprise_page_form_submit",
]

from config import (
    get_google_credentials,
    get_gsc_property,
    get_ga4_property_id,
)

GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def _get_credentials(scopes: list[str]) -> service_account.Credentials:
    """Build service account credentials from JSON key file or string."""
    creds = get_google_credentials()
    if creds is None:
        raise RuntimeError(
            "Google service account key not configured. "
            "Set GOOGLE_SERVICE_ACCOUNT_JSON in .env"
        )
    if isinstance(creds, dict):
        return service_account.Credentials.from_service_account_info(
            creds, scopes=scopes
        )
    return service_account.Credentials.from_service_account_file(
        creds, scopes=scopes
    )


def button_date_range(num_weeks: int = 4) -> tuple[str, str]:
    """Return (start_date, end_date) aligned to Sun-Sat week boundaries.

    The analysis week runs Sun → Sat. GSC data has a ~2-day lag, so the
    window ends at the last fully available Saturday. Covers *num_weeks*
    complete Sun–Sat weeks.
    """
    ref = date.today() - timedelta(days=2)
    # isoweekday: Mon=1..Sun=7. Saturday = 6. Days back to last Saturday ≤ ref.
    days_since_saturday = (ref.isoweekday() - 6) % 7
    end = ref - timedelta(days=days_since_saturday)  # last Saturday ≤ ref
    start = end - timedelta(weeks=num_weeks) + timedelta(days=1)  # Sunday
    return start.isoformat(), end.isoformat()


def latest_complete_week() -> tuple[date, date]:
    """Return (sunday, saturday) of the most recent complete Sun–Sat week
    with GSC data available (respects the ~2-day lag)."""
    start_str, end_str = button_date_range(num_weeks=1)
    return date.fromisoformat(start_str), date.fromisoformat(end_str)


def fetch_gsc_data(start_date: str, end_date: str) -> int:
    """Fetch GSC Search Analytics data for the given range and upsert to DB.

    Returns row count for the main (date × page × query) table.
    """
    site_url = get_gsc_property()
    if not site_url:
        raise RuntimeError("GSC_PROPERTY not set in .env")

    from db import (
        upsert_gsc,
        upsert_gsc_country,
        upsert_gsc_page_daily,
        upsert_gsc_site_daily,
        sync_gsc_keyword_rankings,
    )

    credentials = _get_credentials(GSC_SCOPES)
    service = build("searchconsole", "v1", credentials=credentials)

    # --- Site-level daily totals (date only, no page/query dim — matches GSC UI exactly) ---
    site_daily_rows = _fetch_gsc_dimensioned(
        service, site_url, start_date, end_date,
        dimensions=["date"],
        mapper=lambda keys, row: {
            "date": keys[0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
        },
    )
    if site_daily_rows:
        upsert_gsc_site_daily(site_daily_rows)

    # --- Country fetch (date × country aggregates) ---
    country_rows = _fetch_gsc_dimensioned(
        service, site_url, start_date, end_date,
        dimensions=["date", "country"],
        mapper=lambda keys, row: {
            "date": keys[0],
            "country": keys[1],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
        },
    )
    if country_rows:
        upsert_gsc_country(country_rows)

    # --- Page-level daily aggregates (no query dimension → accurate totals).
    #     GSC drops anonymized-query rows when `query` is a dimension, so the
    #     (date, page, query) sum under-counts vs the GSC UI. This separate
    #     fetch without the query dimension restores the true totals. ---
    page_daily_rows = _fetch_gsc_dimensioned(
        service, site_url, start_date, end_date,
        dimensions=["date", "page"],
        mapper=lambda keys, row: {
            "date": keys[0],
            "page": keys[1],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
        },
    )
    if page_daily_rows:
        upsert_gsc_page_daily(page_daily_rows)

    # --- Main fetch (date × page × query) ---
    all_rows = _fetch_gsc_dimensioned(
        service, site_url, start_date, end_date,
        dimensions=["date", "page", "query"],
        mapper=lambda keys, row: {
            "date": keys[0],
            "page": keys[1],
            "query": keys[2],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"],
        },
    )

    if not all_rows:
        raise RuntimeError(
            f"No GSC data returned for {start_date} to {end_date}. "
            "Check that the service account has access to the property."
        )

    upsert_gsc(all_rows)
    sync_gsc_keyword_rankings()
    return len(all_rows)


def _fetch_gsc_dimensioned(service, site_url, start_date, end_date, dimensions, mapper):
    """Run a paginated GSC search-analytics query for the given dimensions."""
    out = []
    start_row = 0
    page_size = 25000
    while True:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "type": "web",
            "rowLimit": page_size,
            "startRow": start_row,
        }
        response = (
            service.searchanalytics()
            .query(siteUrl=site_url, body=body)
            .execute()
        )
        rows = response.get("rows", [])
        if not rows:
            break
        for row in rows:
            out.append(mapper(row["keys"], row))
        if len(rows) < page_size:
            break
        start_row += page_size
    return out


def fetch_ga4_data(start_date: str, end_date: str) -> int:
    """Fetch GA4 report data for the given range and upsert to DB.

    Returns row count for the main (date × page × source × medium) table.
    """
    property_id = get_ga4_property_id()
    if not property_id:
        raise RuntimeError("GA4_PROPERTY_ID not set in .env")

    from db import (
        upsert_ga4,
        upsert_ga4_traffic,
        upsert_ga4_events,
    )

    credentials = _get_credentials(GA4_SCOPES)
    client = BetaAnalyticsDataClient(credentials=credentials)

    not_jobs = _not_jobs_filter()

    # --- Source+medium-level traffic with user counts ---
    traffic_rows = _fetch_ga4_report(
        client, property_id, start_date, end_date,
        dimensions=["date", "sessionSource", "sessionMedium"],
        metrics=["sessions", "totalUsers", "activeUsers"],
        mapper=lambda dims, metrics: {
            "date": _ga4_date(dims[0]),
            "session_source": dims[1],
            "session_medium": dims[2],
            "sessions": int(metrics[0]),
            "total_users": int(metrics[1]),
            "active_users": int(metrics[2]),
        },
    )
    if traffic_rows:
        upsert_ga4_traffic(traffic_rows)

    # --- Tracked events by channel group ---
    event_filter = FilterExpression(
        and_group=FilterExpressionList(expressions=[
            FilterExpression(
                or_group=FilterExpressionList(expressions=[
                    FilterExpression(filter=Filter(
                        field_name="eventName",
                        string_filter=Filter.StringFilter(value=name),
                    ))
                    for name in TRACKED_EVENT_NAMES
                ])
            ),
            not_jobs,
        ])
    )
    event_rows = _fetch_ga4_report(
        client, property_id, start_date, end_date,
        dimensions=["date", "eventName", "sessionPrimaryChannelGroup"],
        metrics=["eventCount"],
        dimension_filter=event_filter,
        mapper=lambda dims, metrics: {
            "date": _ga4_date(dims[0]),
            "event_name": dims[1],
            "session_primary_channel_group": dims[2],
            "event_count": int(metrics[0]),
        },
    )
    if event_rows:
        upsert_ga4_events(event_rows)

    # --- Main page-level sessions fetch ---
    all_rows = _fetch_ga4_report(
        client, property_id, start_date, end_date,
        dimensions=["date", "pagePath", "sessionSource", "sessionMedium"],
        metrics=["sessions"],
        dimension_filter=not_jobs,
        mapper=lambda dims, metrics: {
            "date": _ga4_date(dims[0]),
            "page_path": dims[1],
            "session_source": dims[2],
            "session_medium": dims[3],
            "sessions": int(metrics[0]),
        },
    )

    if not all_rows:
        raise RuntimeError(
            f"No GA4 data returned for {start_date} to {end_date}. "
            "Check that the service account has access to the property."
        )

    upsert_ga4(all_rows)
    return len(all_rows)


def _ga4_date(raw: str) -> str:
    """Convert GA4's YYYYMMDD date dim to YYYY-MM-DD."""
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _fetch_ga4_report(
    client,
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: list[str],
    metrics: list[str],
    mapper,
    dimension_filter=None,
    page_size: int = 100000,
) -> list[dict]:
    """Run a paginated GA4 runReport and return mapped rows."""
    out = []
    offset = 0
    while True:
        kwargs = dict(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            limit=page_size,
            offset=offset,
        )
        if dimension_filter is not None:
            kwargs["dimension_filter"] = dimension_filter
        request = RunReportRequest(**kwargs)
        response = client.run_report(request)
        if not response.rows:
            break
        for row in response.rows:
            dims = [d.value for d in row.dimension_values]
            mvals = [m.value for m in row.metric_values]
            out.append(mapper(dims, mvals))
        if len(response.rows) < page_size:
            break
        offset += page_size
    return out
