"""Traffic Analytics — Google Analytics (GA4) data via API.

CSV columns: date, page_path, session_source, session_medium, sessions
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from config import DATA_DIR, GA4_CSV_FILENAME, categorize_page, is_ga4_configured
from google_api import fetch_ga4_data
from llm import render_llm_insights

EXPECTED_COLUMNS = [
    "date",
    "page_path",
    "session_source",
    "sessions",
]

# Sources of particular interest for GEO traffic
GEO_SOURCES = ["chatgpt.com", "claude.ai", "perplexity.ai", "gemini.google.com"]

# Key session sources to always show
KEY_SOURCES = ["google", "github", "(direct)", "youtube", "reddit", "linkedin"]


def _load_ga4_data(source) -> pd.DataFrame | None:
    if source is not None:
        df = pd.read_csv(source)
    else:
        path = os.path.join(DATA_DIR, GA4_CSV_FILENAME)
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        st.error(
            f"CSV is missing expected columns: {missing}. "
            f"Expected at minimum: {EXPECTED_COLUMNS}"
        )
        return None

    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page_path"].apply(categorize_page)
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0)

    # Normalize source names for GEO detection
    df["source_normalized"] = df["session_source"].str.lower().str.strip()

    return df


def render():
    st.header("Traffic Analytics (GA4)")

    csv_path = os.path.join(DATA_DIR, GA4_CSV_FILENAME)

    # --- Fetch button ---
    if is_ga4_configured():
        col_btn, col_status = st.columns([1, 3])
        with col_btn:
            fetch_clicked = st.button("Fetch & Analyse", key="ga4_fetch", type="primary")
        if fetch_clicked:
            with col_status:
                with st.spinner("Fetching GA4 data for the past week..."):
                    try:
                        fetch_ga4_data()
                        st.success("GA4 data fetched successfully.")
                    except Exception as e:
                        st.error(f"Failed to fetch GA4 data: {e}")
    else:
        st.info(
            "GA4 not configured. Set `GOOGLE_SERVICE_ACCOUNT_JSON` and "
            "`GA4_PROPERTY_ID` in `.env` to enable automatic fetching."
        )

    # --- Load data: CSV on disk or file upload fallback ---
    has_csv = os.path.exists(csv_path)

    if has_csv:
        df = _load_ga4_data(None)
    else:
        uploaded = st.file_uploader(
            "Or upload a GA4 CSV",
            type=["csv"],
            key="ga4_upload",
            help=f"Expected columns: {', '.join(EXPECTED_COLUMNS)}",
        )
        if uploaded is None:
            return
        df = _load_ga4_data(uploaded)

    if df is None:
        return

    # --- Date range filter ---
    dates = sorted(df["date"].dt.date.unique())
    date_range = st.date_input(
        "Date range",
        value=(min(dates), max(dates)),
        min_value=min(dates),
        max_value=max(dates),
        key="ga4_dates",
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        df = df[(df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])]

    view = st.radio("View", ["Weekly", "Monthly"], horizontal=True, key="ga4_view")
    if view == "Weekly":
        df["period"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    else:
        df["period"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)

    # --- Overview ---
    st.subheader("Overview")
    total_sessions = df["sessions"].sum()
    unique_sources = df["session_source"].nunique()

    m1, m2 = st.columns(2)
    m1.metric("Total Sessions", f"{total_sessions:,.0f}")
    m2.metric("Unique Sources", unique_sources)

    # --- Sessions by source ---
    st.subheader("Sessions by Source")

    source_breakdown = (
        df.groupby("session_source")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    fig_sources = px.bar(
        source_breakdown.head(15),
        x="sessions",
        y="session_source",
        orientation="h",
        title="Top 15 Session Sources",
        text="sessions",
    )
    fig_sources.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_sources.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_sources, use_container_width=True)

    # --- Source trend over time ---
    st.subheader(f"Source Trend ({view})")

    top_sources = source_breakdown.head(8)["session_source"].tolist()
    source_trend = (
        df[df["session_source"].isin(top_sources)]
        .groupby(["period", "session_source"])["sessions"]
        .sum()
        .reset_index()
    )
    fig_source_trend = px.line(
        source_trend,
        x="period",
        y="sessions",
        color="session_source",
        title=f"Top Sources Over Time ({view})",
        markers=True,
    )
    st.plotly_chart(fig_source_trend, use_container_width=True)

    # --- Page category breakdown ---
    st.subheader("Sessions by Page Category")

    cat_breakdown = (
        df.groupby("page_category")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    fig_cats = px.bar(
        cat_breakdown,
        x="page_category",
        y="sessions",
        text="sessions",
        title="Sessions by Page Category",
        color="page_category",
    )
    fig_cats.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_cats.update_layout(showlegend=False)
    st.plotly_chart(fig_cats, use_container_width=True)

    # --- Per-source landing page drill-down ---
    st.subheader("Landing Page Breakdown by Source")

    available_sources = source_breakdown["session_source"].tolist()
    drill_source = st.selectbox(
        "Select source to drill down",
        available_sources[:20],
        key="drill_source",
    )

    drill_df = (
        df[df["session_source"] == drill_source]
        .groupby("page_category")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    fig_drill = px.bar(
        drill_df,
        x="page_category",
        y="sessions",
        text="sessions",
        title=f"Page Category Breakdown — {drill_source}",
        color="page_category",
    )
    fig_drill.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_drill.update_layout(showlegend=False)
    st.plotly_chart(fig_drill, use_container_width=True)

    # --- GEO Traffic (AI sources) ---
    st.subheader("GEO Traffic (AI Sources)")

    geo_df = df[df["source_normalized"].isin(GEO_SOURCES)]
    if geo_df.empty:
        st.info("No traffic from AI sources (chatgpt.com, claude.ai, perplexity.ai, gemini.google.com) detected.")
    else:
        geo_by_source = (
            geo_df.groupby("session_source")["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )

        fig_geo = px.bar(
            geo_by_source,
            x="session_source",
            y="sessions",
            text="sessions",
            title="Sessions from AI Sources",
            color="session_source",
        )
        fig_geo.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig_geo.update_layout(showlegend=False)
        st.plotly_chart(fig_geo, use_container_width=True)

        # GEO per-source page category breakdown
        geo_cat = (
            geo_df.groupby(["session_source", "page_category"])["sessions"]
            .sum()
            .reset_index()
        )
        fig_geo_cat = px.bar(
            geo_cat,
            x="session_source",
            y="sessions",
            color="page_category",
            title="AI Source Traffic by Page Category",
            barmode="stack",
            text="sessions",
        )
        st.plotly_chart(fig_geo_cat, use_container_width=True)

        # GEO trend
        geo_trend = (
            geo_df.groupby(["period", "session_source"])["sessions"]
            .sum()
            .reset_index()
        )
        fig_geo_trend = px.line(
            geo_trend,
            x="period",
            y="sessions",
            color="session_source",
            title=f"AI Source Traffic Trend ({view})",
            markers=True,
        )
        st.plotly_chart(fig_geo_trend, use_container_width=True)

    # --- LLM Insights ---
    top_5_sources = source_breakdown.head(5)
    geo_summary = ""
    if not geo_df.empty:
        geo_summary = f"\nAI source traffic: {geo_by_source.to_string(index=False)}"

    data_summary = f"""Total sessions: {total_sessions:,.0f}
Top sources: {', '.join(f"{r['session_source']}({r['sessions']:,.0f})" for _, r in top_5_sources.iterrows())}
Page categories: {', '.join(f"{r['page_category']}({r['sessions']:,.0f})" for _, r in cat_breakdown.head(5).iterrows())}
{geo_summary}"""

    render_llm_insights("Traffic Analytics", data_summary)
