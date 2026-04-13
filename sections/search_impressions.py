"""Search Impressions — Google Search Console data via API.

CSV columns: date, page, query, clicks, impressions, ctr, position
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from config import DATA_DIR, GSC_CSV_FILENAME, categorize_page, is_gsc_configured
from google_api import fetch_gsc_data
from llm import render_llm_insights

EXPECTED_COLUMNS = ["date", "page", "query", "clicks", "impressions", "ctr", "position"]


def _load_gsc_data(source) -> pd.DataFrame | None:
    """Load GSC data from file upload or CSV on disk."""
    if source is not None:
        df = pd.read_csv(source)
    else:
        path = os.path.join(DATA_DIR, GSC_CSV_FILENAME)
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path)

    df.columns = [c.strip().lower() for c in df.columns]

    # Validate expected columns
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        st.error(
            f"CSV is missing expected columns: {missing}. "
            f"Expected: {EXPECTED_COLUMNS}"
        )
        return None

    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page"].apply(
        lambda p: categorize_page(pd.Series([p]).str.extract(r"https?://[^/]+(/.*)")[0].iloc[0])
    )
    return df


def render():
    st.header("Search Impressions (GSC)")

    csv_path = os.path.join(DATA_DIR, GSC_CSV_FILENAME)

    # --- Fetch button ---
    if is_gsc_configured():
        col_btn, col_status = st.columns([1, 3])
        with col_btn:
            fetch_clicked = st.button("Fetch & Analyse", key="gsc_fetch", type="primary")
        if fetch_clicked:
            with col_status:
                with st.spinner("Fetching GSC data for the past week..."):
                    try:
                        fetch_gsc_data()
                        st.success("GSC data fetched successfully.")
                    except Exception as e:
                        st.error(f"Failed to fetch GSC data: {e}")
    else:
        st.info(
            "GSC not configured. Set `GOOGLE_SERVICE_ACCOUNT_JSON` and "
            "`GSC_PROPERTY` in `.env` to enable automatic fetching."
        )

    # --- Load data: CSV on disk or file upload fallback ---
    has_csv = os.path.exists(csv_path)

    if has_csv:
        df = _load_gsc_data(None)
    else:
        uploaded = st.file_uploader(
            "Or upload a GSC CSV",
            type=["csv"],
            key="gsc_upload",
            help=f"Expected columns: {', '.join(EXPECTED_COLUMNS)}",
        )
        if uploaded is None:
            return
        df = _load_gsc_data(uploaded)

    if df is None:
        return

    # --- Date range filter ---
    dates = sorted(df["date"].dt.date.unique())
    date_range = st.date_input(
        "Date range",
        value=(min(dates), max(dates)),
        min_value=min(dates),
        max_value=max(dates),
        key="gsc_dates",
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        df = df[(df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])]

    # --- View toggle ---
    view = st.radio("View", ["Weekly", "Monthly"], horizontal=True, key="gsc_view")

    if view == "Weekly":
        df["period"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    else:
        df["period"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)

    # --- Top-level metrics ---
    st.subheader("Overview")
    total_impressions = df["impressions"].sum()
    total_clicks = df["clicks"].sum()
    avg_ctr = total_clicks / total_impressions * 100 if total_impressions else 0

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Impressions", f"{total_impressions:,.0f}")
    m2.metric("Total Clicks", f"{total_clicks:,.0f}")
    m3.metric("Avg CTR", f"{avg_ctr:.1f}%")

    # --- Impressions over time ---
    st.subheader(f"Impressions Trend ({view})")

    trend = df.groupby("period").agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()

    fig_trend = px.bar(
        trend,
        x="period",
        y="impressions",
        title=f"{view} Impressions",
        labels={"period": "Period", "impressions": "Impressions"},
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    # --- Page category breakdown ---
    st.subheader("Impressions by Page Category")

    cat_breakdown = (
        df.groupby("page_category")
        .agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
        )
        .reset_index()
    )
    cat_breakdown["ctr"] = (cat_breakdown["clicks"] / cat_breakdown["impressions"] * 100).round(1)
    cat_breakdown["% of total"] = (
        cat_breakdown["impressions"] / total_impressions * 100
    ).round(1)
    cat_breakdown = cat_breakdown.sort_values("impressions", ascending=False)

    col_chart, col_table = st.columns([2, 1])

    with col_chart:
        fig_cat = px.bar(
            cat_breakdown,
            x="page_category",
            y="impressions",
            text="impressions",
            title="Impressions by Page Category",
            color="page_category",
        )
        fig_cat.update_traces(textposition="outside", texttemplate="%{text:,.0f}")
        fig_cat.update_layout(showlegend=False)
        st.plotly_chart(fig_cat, use_container_width=True)

    with col_table:
        st.dataframe(
            cat_breakdown.rename(
                columns={
                    "page_category": "Page Category",
                    "impressions": "Impressions",
                    "clicks": "Clicks",
                    "ctr": "CTR %",
                    "% of total": "% of Total",
                }
            ),
            hide_index=True,
            use_container_width=True,
        )

    # --- Page category trend over time ---
    st.subheader(f"Page Category Trend ({view})")

    cat_trend = (
        df.groupby(["period", "page_category"])["impressions"]
        .sum()
        .reset_index()
    )
    fig_cat_trend = px.area(
        cat_trend,
        x="period",
        y="impressions",
        color="page_category",
        title=f"Impressions by Page Category ({view})",
    )
    st.plotly_chart(fig_cat_trend, use_container_width=True)

    # --- LLM Insights ---
    top_cats = cat_breakdown.head(5)
    data_summary = f"""Period: {date_range[0]} to {date_range[1] if isinstance(date_range, tuple) and len(date_range) == 2 else 'now'}
Total impressions: {total_impressions:,.0f}, Total clicks: {total_clicks:,.0f}, Avg CTR: {avg_ctr:.1f}%
Top page categories by impressions:
{chr(10).join(f"  {r['page_category']}: {r['impressions']:,.0f} ({r['% of total']}% of total, CTR: {r['ctr']}%)" for _, r in top_cats.iterrows())}"""

    render_llm_insights("Search Impressions", data_summary)
