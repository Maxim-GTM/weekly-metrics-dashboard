"""Weekly Metrics Dashboard — Streamlit entry point."""

import atexit
from datetime import date

import streamlit as st

from config import is_ga4_configured, is_gsc_configured
from db import get_pool, close_pool, latest_data_date

st.set_page_config(
    page_title="Weekly Metrics Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

# Initialize DB connection pool
get_pool()
atexit.register(close_pool)

st.title("Weekly Metrics Dashboard")

# --- Stale data warning ---
# Data is "fresh" if it covers up through the latest complete Sun–Sat Saturday.
# Warning fires only when we're missing days relative to that expected end date.
from google_api import latest_complete_week

_, expected_latest = latest_complete_week()

def _days_behind(table: str) -> int | None:
    """Days the table is behind the expected latest Saturday. None if empty."""
    latest = latest_data_date(table)
    if latest is None:
        return None
    gap = (expected_latest - latest).days
    return gap if gap > 0 else 0

gsc_gap = _days_behind("gsc")
ga4_gap = _days_behind("ga4")

stale = []
if gsc_gap is not None and gsc_gap > 0:
    stale.append(f"GSC is **{gsc_gap} day(s) behind** (latest expected: {expected_latest:%b %d})")
elif gsc_gap is None and is_gsc_configured():
    stale.append("No GSC data yet")
if ga4_gap is not None and ga4_gap > 0:
    stale.append(f"GA4 is **{ga4_gap} day(s) behind** (latest expected: {expected_latest:%b %d})")
elif ga4_gap is None and is_ga4_configured():
    stale.append("No GA4 data yet")

if stale:
    st.warning(f"{' | '.join(stale)} — consider re-fetching before the call.")

# --- Section navigation ---
SECTIONS = {
    "Search Impressions (GSC)": "search_impressions",
    "GSC Coverage (Index Status)": "gsc_coverage",
    "Traffic Analytics (GA4)": "traffic_analytics",
    "Content Analytics (GA4)": "content_analytics",
    # "Growth Diagnostics": "diagnostics",  # hidden until ready
    "Keyword Performance": "keyword_performance",
    "GEO Performance (Profound)": "geo_profound",
}

selected = st.sidebar.radio("Section", list(SECTIONS.keys()))

if selected == "Search Impressions (GSC)":
    from sections.search_impressions import render
elif selected == "GSC Coverage (Index Status)":
    from sections.gsc_coverage import render
elif selected == "Traffic Analytics (GA4)":
    from sections.traffic_analytics import render
elif selected == "Content Analytics (GA4)":
    from sections.content_analytics import render
# elif selected == "Growth Diagnostics":
#     from sections.diagnostics import render
elif selected == "Keyword Performance":
    from sections.keyword_performance import render
elif selected == "GEO Performance (Profound)":
    from sections.geo_profound import render

render()
