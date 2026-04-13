"""Google API integration for fetching GSC and GA4 data."""

import os
from datetime import date, timedelta

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

from config import (
    DATA_DIR,
    GSC_CSV_FILENAME,
    GA4_CSV_FILENAME,
    get_google_credentials_path,
    get_gsc_property,
    get_ga4_property_id,
)

GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def _get_credentials(scopes: list[str]) -> service_account.Credentials:
    """Build service account credentials from the JSON key file."""
    key_path = get_google_credentials_path()
    if key_path is None:
        raise RuntimeError(
            "Google service account key not configured. "
            "Set GOOGLE_SERVICE_ACCOUNT_JSON in .env"
        )
    return service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )


def _date_range_last_7() -> tuple[str, str]:
    """Return (start_date, end_date) for the last 7 days.

    GSC data has a ~2-day lag, so the window ends 2 days ago.
    """
    end = date.today() - timedelta(days=2)
    start = end - timedelta(days=6)  # 7 days inclusive
    return start.isoformat(), end.isoformat()


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def fetch_gsc_data() -> str:
    """Fetch GSC Search Analytics data for the past week and save as CSV.

    Returns the path to the saved CSV file.
    """
    site_url = get_gsc_property()
    if not site_url:
        raise RuntimeError("GSC_PROPERTY not set in .env")

    credentials = _get_credentials(GSC_SCOPES)
    service = build("searchconsole", "v1", credentials=credentials)

    start_date, end_date = _date_range_last_7()

    all_rows = []
    start_row = 0
    page_size = 25000

    while True:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["date", "page", "query"],
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
            keys = row["keys"]
            all_rows.append(
                {
                    "date": keys[0],
                    "page": keys[1],
                    "query": keys[2],
                    "clicks": row["clicks"],
                    "impressions": row["impressions"],
                    "ctr": row["ctr"],
                    "position": row["position"],
                }
            )

        if len(rows) < page_size:
            break
        start_row += page_size

    if not all_rows:
        raise RuntimeError(
            f"No GSC data returned for {start_date} to {end_date}. "
            "Check that the service account has access to the property."
        )

    df = pd.DataFrame(all_rows)
    _ensure_data_dir()
    csv_path = os.path.join(DATA_DIR, GSC_CSV_FILENAME)
    df.to_csv(csv_path, index=False)
    return csv_path


def fetch_ga4_data() -> str:
    """Fetch GA4 report data for the past week and save as CSV.

    Returns the path to the saved CSV file.
    """
    property_id = get_ga4_property_id()
    if not property_id:
        raise RuntimeError("GA4_PROPERTY_ID not set in .env")

    credentials = _get_credentials(GA4_SCOPES)
    client = BetaAnalyticsDataClient(credentials=credentials)

    start_date, end_date = _date_range_last_7()

    all_rows = []
    offset = 0
    page_size = 100000

    while True:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
            ],
            metrics=[
                Metric(name="sessions"),
            ],
            limit=page_size,
            offset=offset,
        )
        response = client.run_report(request)

        if not response.rows:
            break

        for row in response.rows:
            raw_date = row.dimension_values[0].value
            formatted_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            all_rows.append(
                {
                    "date": formatted_date,
                    "page_path": row.dimension_values[1].value,
                    "session_source": row.dimension_values[2].value,
                    "session_medium": row.dimension_values[3].value,
                    "sessions": int(row.metric_values[0].value),
                }
            )

        if len(response.rows) < page_size:
            break
        offset += page_size

    if not all_rows:
        raise RuntimeError(
            f"No GA4 data returned for {start_date} to {end_date}. "
            "Check that the service account has access to the property."
        )

    df = pd.DataFrame(all_rows)
    _ensure_data_dir()
    csv_path = os.path.join(DATA_DIR, GA4_CSV_FILENAME)
    df.to_csv(csv_path, index=False)
    return csv_path
