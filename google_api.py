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

def _not_jobs_filter(field: str = "pagePath") -> FilterExpression:
    """Exclude sessions where the given field contains /jobs."""
    return FilterExpression(
        not_expression=FilterExpression(
            filter=Filter(
                field_name=field,
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                    value="/jobs",
                    case_sensitive=False,
                ),
            )
        )
    )


# Per-category GA4 filter specs: list of (field, match_type, value).
# Multiple conditions within a category are OR-combined.
# match_type: "EXACT" | "BEGINS_WITH" | "CONTAINS"
CATEGORY_FILTERS: dict[str, list[tuple[str, str, str]]] = {
    "Articles":           [("pagePath", "BEGINS_WITH", "/articles")],
    "Blog":               [("pagePath", "BEGINS_WITH", "/blog")],
    "Maxim Docs":         [("pagePath", "BEGINS_WITH", "/docs")],
    "Features":           [("pagePath", "BEGINS_WITH", "/features")],
    "Pricing":            [("pagePath", "BEGINS_WITH", "/pricing"),
                           ("pagePath", "EXACT",       "/bifrost/pricing")],
    "LLM Cost Calculator":[("pagePath", "BEGINS_WITH", "/bifrost/llm-cost-calculator"),
                           ("pagePath", "BEGINS_WITH", "/llm-cost-calculator")],
    "Comparison Pages":   [("pagePath", "BEGINS_WITH", "/compare")],
    "Products":           [("pagePath", "BEGINS_WITH", "/products")],
    "Resources":          [("pagePath", "BEGINS_WITH", "/resources")],
    "Alternatives":       [("pagePath", "BEGINS_WITH", "/alternatives")],
    "Industry Pages":     [("pagePath", "BEGINS_WITH", "/industry")],
    "Provider Status":    [("pagePath", "BEGINS_WITH", "/provider-status")],
    "Model Library":      [("pagePath", "BEGINS_WITH", "/model-library")],
    "Bifrost Docs":       [("hostName", "EXACT",       "docs.getbifrost.ai")],
    "Maxim Homepage":     [("pagePath", "EXACT",       "/")],
    "Bifrost Enterprise": [("pagePath", "EXACT",       "/enterprise")],
    "Bifrost":            [("pagePath", "EXACT",       "/bifrost"),
                           ("pagePath", "EXACT",       "/bifrost/enterprise"),
                           ("pagePath", "EXACT",       "/bifrost/book-a-demo")],
}


def _build_category_filter(conditions: list[tuple[str, str, str]]) -> FilterExpression:
    """Build an OR-combined FilterExpression from a list of (field, match_type, value)."""
    _mt = {
        "EXACT":       Filter.StringFilter.MatchType.EXACT,
        "BEGINS_WITH": Filter.StringFilter.MatchType.BEGINS_WITH,
        "CONTAINS":    Filter.StringFilter.MatchType.CONTAINS,
    }
    exprs = [
        FilterExpression(filter=Filter(
            field_name=field,
            string_filter=Filter.StringFilter(match_type=_mt[mt], value=value),
        ))
        for field, mt, value in conditions
    ]
    return exprs[0] if len(exprs) == 1 else FilterExpression(
        or_group=FilterExpressionList(expressions=exprs)
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
        upsert_ga4_landing_pages,
        upsert_ga4_page_events,
    )

    credentials = _get_credentials(GA4_SCOPES)
    client = BetaAnalyticsDataClient(credentials=credentials)

    not_jobs = _not_jobs_filter("pagePath")

    # --- Source+medium-level traffic (daily) — accurate sessions, inflated user counts ---
    traffic_rows = _fetch_ga4_report(
        client, property_id, start_date, end_date,
        dimensions=["date", "sessionSource", "sessionMedium"],
        metrics=["sessions", "totalUsers", "activeUsers",
                 "engagedSessions", "newUsers", "userEngagementDuration"],
        mapper=lambda dims, metrics: {
            "date": _ga4_date(dims[0]),
            "session_source": dims[1],
            "session_medium": dims[2],
            "sessions": int(metrics[0]),
            "total_users": int(metrics[1]),
            "active_users": int(metrics[2]),
            "engaged_sessions": int(metrics[3]),
            "new_users": int(metrics[4]),
            "engagement_duration_s": int(float(metrics[5])),
        },
    )
    if traffic_rows:
        from db import upsert_ga4_traffic
        upsert_ga4_traffic(traffic_rows)

    # --- Weekly traffic aggregates (no date dim) — accurate user counts for periods ---
    _fetch_ga4_traffic_weekly(client, property_id, start_date, end_date)

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

    # --- Main page-level sessions fetch with engagement metrics ---
    all_rows = _fetch_ga4_report(
        client, property_id, start_date, end_date,
        dimensions=["date", "pagePath", "sessionSource", "sessionMedium"],
        metrics=["sessions", "engagedSessions", "userEngagementDuration", "newUsers",
                 "conversions"],
        dimension_filter=not_jobs,
        mapper=lambda dims, metrics: {
            "date": _ga4_date(dims[0]),
            "page_path": dims[1],
            "session_source": dims[2],
            "session_medium": dims[3],
            "sessions": int(metrics[0]),
            "engaged_sessions": int(metrics[1]),
            "engagement_duration_s": int(float(metrics[2])),
            "new_users": int(metrics[3]),
            "exits": 0,
            "conversions": int(float(metrics[4])),
        },
    )

    if not all_rows:
        raise RuntimeError(
            f"No GA4 data returned for {start_date} to {end_date}. "
            "Check that the service account has access to the property."
        )

    upsert_ga4(all_rows)

    # --- Landing page entry-point sessions ---
    landing_rows = _fetch_ga4_report(
        client, property_id, start_date, end_date,
        dimensions=["date", "landingPage", "sessionSource", "sessionMedium"],
        metrics=["sessions", "engagedSessions", "newUsers", "userEngagementDuration",
                 "conversions"],
        dimension_filter=None,
        mapper=lambda dims, metrics: {
            "date": _ga4_date(dims[0]),
            "landing_page": dims[1],
            "session_source": dims[2],
            "session_medium": dims[3],
            "sessions": int(metrics[0]),
            "engaged_sessions": int(metrics[1]),
            "new_users": int(metrics[2]),
            "engagement_duration_s": int(float(metrics[3])),
            "conversions": int(float(metrics[4])),
        },
    )
    if landing_rows:
        upsert_ga4_landing_pages(landing_rows)

    # --- Page-level events: page_view + scroll (for scroll depth analysis) ---
    page_event_filter = FilterExpression(
        and_group=FilterExpressionList(expressions=[
            FilterExpression(
                or_group=FilterExpressionList(expressions=[
                    FilterExpression(filter=Filter(
                        field_name="eventName",
                        string_filter=Filter.StringFilter(value="scroll"),
                    )),
                    FilterExpression(filter=Filter(
                        field_name="eventName",
                        string_filter=Filter.StringFilter(value="page_view"),
                    )),
                ])
            ),
            not_jobs,
        ])
    )
    page_event_rows = _fetch_ga4_report(
        client, property_id, start_date, end_date,
        dimensions=["date", "pagePath", "eventName"],
        metrics=["eventCount"],
        dimension_filter=page_event_filter,
        mapper=lambda dims, metrics: {
            "date": _ga4_date(dims[0]),
            "page_path": dims[1],
            "event_name": dims[2],
            "event_count": int(metrics[0]),
        },
    )
    if page_event_rows:
        upsert_ga4_page_events(page_event_rows)

    # --- Per-category session counts (one filtered query per category) ---
    fetch_ga4_category_sessions(start_date, end_date, client=client, property_id=property_id)

    return len(all_rows)


def _fetch_ga4_traffic_weekly(client, property_id: str, start_date: str, end_date: str):
    """Fetch weekly user acquisition aggregates using firstUserSource dimension.

    Uses firstUserSource/firstUserMedium (User Acquisition report) without the
    date dimension so GA4 deduplicates users across the week server-side.
    Sessions stay in ga4_traffic (Traffic Acquisition / sessionSource).
    Week-start date stored as the row's date.
    """
    from datetime import date as _date, timedelta
    from db import upsert_ga4_traffic_weekly, get_pool

    start = _date.fromisoformat(start_date)
    end = _date.fromisoformat(end_date)
    weeks: list[tuple[_date, _date]] = []
    wk = start
    while wk <= end:
        weeks.append((wk, min(wk + timedelta(days=6), end)))
        wk += timedelta(days=7)

    all_rows = []
    for week_start, week_end in weeks:
        rows = _fetch_ga4_report(
            client, property_id,
            week_start.isoformat(), week_end.isoformat(),
            dimensions=["firstUserSource", "firstUserMedium"],
            metrics=["totalUsers", "newUsers"],
            mapper=lambda dims, metrics, d=week_start: {
                "date": d.isoformat(),
                "first_user_source": dims[0],
                "first_user_medium": dims[1],
                "total_users": int(metrics[0]),
                "new_users": int(metrics[1]),
            },
        )
        all_rows.extend(rows)

    if all_rows:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ga4_traffic_weekly WHERE date >= %s AND date <= %s",
                    (start_date, end_date),
                )
            conn.commit()
        upsert_ga4_traffic_weekly(all_rows)


def fetch_ga4_category_sessions(
    start_date: str,
    end_date: str,
    client=None,
    property_id: str | None = None,
) -> int:
    """Fetch GA4 sessions per page category using per-category page filters.

    Fetches each 7-day week separately without the date dimension to avoid
    GA4 privacy thresholds that silently drop small (date, source, medium)
    buckets. The week-start date is stored as the row's date.
    'Other' is derived at display time as total_sessions − sum(categories).
    """
    from datetime import date as _date, timedelta
    from db import upsert_ga4_category_sessions

    if client is None:
        credentials = _get_credentials(GA4_SCOPES)
        client = BetaAnalyticsDataClient(credentials=credentials)
    if property_id is None:
        property_id = get_ga4_property_id()
    if not property_id:
        raise RuntimeError("GA4_PROPERTY_ID not set in .env")

    # Split range into 7-day weeks; store week-start as the date.
    start = _date.fromisoformat(start_date)
    end = _date.fromisoformat(end_date)
    weeks: list[tuple[_date, _date]] = []
    wk = start
    while wk <= end:
        weeks.append((wk, min(wk + timedelta(days=6), end)))
        wk += timedelta(days=7)

    all_rows = []
    for week_start, week_end in weeks:
        for category, conditions in CATEGORY_FILTERS.items():
            dim_filter = _build_category_filter(conditions)
            rows = _fetch_ga4_report(
                client, property_id,
                week_start.isoformat(), week_end.isoformat(),
                dimensions=["sessionSource", "sessionMedium"],
                metrics=["sessions"],
                dimension_filter=dim_filter,
                mapper=lambda dims, metrics, cat=category, d=week_start: {
                    "date": d.isoformat(),
                    "page_category": cat,
                    "session_source": dims[0],
                    "session_medium": dims[1],
                    "sessions": int(metrics[0]),
                },
            )
            all_rows.extend(rows)

    if all_rows:
        # Delete existing rows for this range (handles transition from per-day
        # to per-week storage — old daily rows would otherwise linger).
        from db import get_pool
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ga4_category_sessions WHERE date >= %s AND date <= %s",
                    (start_date, end_date),
                )
            conn.commit()
        upsert_ga4_category_sessions(all_rows)
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
