"""Weekly Metrics Dashboard — Streamlit entry point."""

import atexit
import os
from datetime import date

import streamlit as st

from config import is_ga4_configured, is_gsc_configured, get_google_credentials
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

# Debug: show env var status in sidebar (remove after fixing)
with st.sidebar:
    with st.expander("Debug: Env Vars"):
        sa_val = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        st.write(f"GOOGLE_SERVICE_ACCOUNT_JSON: {'SET (' + str(len(sa_val)) + ' chars)' if sa_val else 'NOT SET'}")
        if sa_val:
            st.write(f"  starts with: {repr(sa_val[:20])}")
        st.write(f"GSC_PROPERTY: {'SET' if os.getenv('GSC_PROPERTY') else 'NOT SET'}")
        st.write(f"GA4_PROPERTY_ID: {'SET' if os.getenv('GA4_PROPERTY_ID') else 'NOT SET'}")
        creds = get_google_credentials()
        st.write(f"get_google_credentials(): {type(creds).__name__ if creds else 'None'}")

# --- Stale data warning ---
def _data_age_days(table: str) -> int | None:
    """Return age in days of the newest row in the table, or None if empty."""
    latest = latest_data_date(table)
    if latest is None:
        return None
    return (date.today() - latest).days

gsc_age = _data_age_days("gsc")
ga4_age = _data_age_days("ga4")

stale = []
if gsc_age is not None and gsc_age > 7:
    stale.append(f"GSC data is **{gsc_age} days old**")
elif gsc_age is None and is_gsc_configured():
    stale.append("No GSC data yet")
if ga4_age is not None and ga4_age > 7:
    stale.append(f"GA4 data is **{ga4_age} days old**")
elif ga4_age is None and is_ga4_configured():
    stale.append("No GA4 data yet")

if stale:
    st.warning(f"{' | '.join(stale)} — consider re-fetching before the call.")

# --- Generate Call Summary ---
def _build_section_summaries() -> dict[str, str]:
    """Gather data summaries from all available sources."""
    summaries = {}

    # GSC
    from sections.search_impressions import _load_all_gsc_data
    gsc_df = _load_all_gsc_data()
    if gsc_df is not None and not gsc_df.empty:
        gsc_df["period"] = gsc_df["date"].dt.to_period("W").apply(lambda r: r.start_time)
        periods = sorted(gsc_df["period"].unique())
        latest = gsc_df[gsc_df["period"] == periods[-1]]
        prev = gsc_df[gsc_df["period"] == periods[-2]] if len(periods) >= 2 else None

        imp = latest["impressions"].sum()
        clicks = latest["clicks"].sum()
        ctr = clicks / imp * 100 if imp else 0
        imp_change = ""
        if prev is not None:
            prev_imp = prev["impressions"].sum()
            if prev_imp:
                imp_change = f" ({(imp - prev_imp) / prev_imp * 100:+.0f}% vs prev week)"

        cat = latest.groupby("page_category").agg(
            impressions=("impressions", "sum"), clicks=("clicks", "sum")
        ).reset_index().sort_values("impressions", ascending=False)
        cat_lines = "\n".join(
            f"  {r['page_category']}: {r['impressions']:,.0f}"
            for _, r in cat.head(8).iterrows()
        )
        pages = latest["page"].nunique()
        summaries["Search Impressions"] = (
            f"Weekly impressions: {imp:,.0f}{imp_change}, Clicks: {clicks:,.0f}, CTR: {ctr:.1f}%\n"
            f"Pages in search: {pages}\n"
            f"By page category:\n{cat_lines}"
        )

    # GA4
    from sections.traffic_analytics import _load_all_ga4_data, GEO_SOURCES
    ga4_df = _load_all_ga4_data()
    if ga4_df is not None and not ga4_df.empty:
        ga4_df["period"] = ga4_df["date"].dt.to_period("W").apply(lambda r: r.start_time)
        periods = sorted(ga4_df["period"].unique())
        latest = ga4_df[ga4_df["period"] == periods[-1]]
        prev = ga4_df[ga4_df["period"] == periods[-2]] if len(periods) >= 2 else None

        sessions = latest["sessions"].sum()
        sess_change = ""
        if prev is not None:
            prev_sess = prev["sessions"].sum()
            if prev_sess:
                sess_change = f" ({(sessions - prev_sess) / prev_sess * 100:+.0f}% vs prev week)"

        # By source
        src = latest.groupby("session_source")["sessions"].sum().reset_index().sort_values("sessions", ascending=False)
        src_lines = ", ".join(f"{r['session_source']}({int(r['sessions']):,})" for _, r in src.head(8).iterrows())

        # By medium
        med_lines = ""
        if "session_medium" in latest.columns:
            med = latest.groupby("session_medium")["sessions"].sum().reset_index().sort_values("sessions", ascending=False)
            med_lines = "\nBy medium: " + ", ".join(f"{r['session_medium']}({int(r['sessions']):,})" for _, r in med.head(5).iterrows())

        # By category
        cat = latest.groupby("page_category")["sessions"].sum().reset_index().sort_values("sessions", ascending=False)
        cat_lines = ", ".join(f"{r['page_category']}({int(r['sessions']):,})" for _, r in cat.head(8).iterrows())

        # GEO
        geo = latest[latest["source_normalized"].isin(GEO_SOURCES)]
        geo_lines = ""
        if not geo.empty:
            geo_src = geo.groupby("session_source")["sessions"].sum().reset_index().sort_values("sessions", ascending=False)
            geo_lines = "\nAI source traffic: " + ", ".join(f"{r['session_source']}({int(r['sessions']):,})" for _, r in geo_src.iterrows())

        summaries["Traffic Analytics"] = (
            f"Weekly sessions: {sessions:,.0f}{sess_change}\n"
            f"Top sources: {src_lines}{med_lines}\n"
            f"By page category: {cat_lines}{geo_lines}"
        )

    return summaries

col_summary_btn, col_summary_out = st.columns([1, 4])
with col_summary_btn:
    gen_summary = st.button("Generate Call Summary")

if gen_summary:
    from llm import _get_client, generate_call_summary
    if _get_client() is None:
        st.info("Set `OPENAI_API_KEY` in `.env` to generate call summaries.")
    else:
        with st.spinner("Gathering data and generating summary..."):
            summaries = _build_section_summaries()
            if not summaries:
                st.warning("No data available. Fetch or upload data first.")
            else:
                result = generate_call_summary(summaries)
                if result:
                    st.markdown("---")
                    st.markdown(result)
                    st.download_button(
                        "Download as Markdown",
                        result,
                        file_name=f"call_notes_{date.today().isoformat()}.md",
                        mime="text/markdown",
                    )
                else:
                    st.error("Failed to generate summary.")

# --- Section navigation ---
SECTIONS = {
    "Search Impressions (GSC)": "search_impressions",
    "Traffic Analytics (GA4)": "traffic_analytics",
    "Keyword Performance": "keyword_performance",
    "GEO Performance (Profound)": "geo_profound",
}

selected = st.sidebar.radio("Section", list(SECTIONS.keys()))

# Lazy-import the selected section to avoid loading all at once
if selected == "Search Impressions (GSC)":
    from sections.search_impressions import render
elif selected == "Traffic Analytics (GA4)":
    from sections.traffic_analytics import render
elif selected == "Keyword Performance":
    from sections.keyword_performance import render
elif selected == "GEO Performance (Profound)":
    from sections.geo_profound import render

render()
