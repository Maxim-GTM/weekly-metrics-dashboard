"""Search Impressions — Google Search Console data via API."""

import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, is_ga4_configured, is_gsc_configured
from db import query_df, upsert_gsc
from llm import render_chart_insight
from sections.fetch_button import render_fetch_button as _render_fetch_button

EXPECTED_COLUMNS = ["date", "page", "query", "clicks", "impressions", "ctr", "position"]


def _enrich_gsc_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns to a GSC dataframe."""
    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page"].apply(
        lambda p: categorize_page(pd.Series([p]).str.extract(r"https?://[^/]+(/.*)")[0].iloc[0])
    ).astype("category")
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_all_gsc_data() -> pd.DataFrame | None:
    """Load GSC data from the database (last 4 weeks).

    We intentionally drop `query`, `ctr`, `position` from the SELECT — they
    aren't used by this section and `query` is the heaviest string column in
    the gsc table.
    """
    df = query_df(
        "SELECT date, page, clicks, impressions "
        "FROM gsc WHERE date >= CURRENT_DATE - INTERVAL '28 days' ORDER BY date"
    )
    if df.empty:
        return None
    return _enrich_gsc_df(df)


def render():
    st.header("Search Impressions (GSC)")

    # Surface the analysis window we're fetching against.
    try:
        from google_api import latest_complete_week
        sun, sat = latest_complete_week()
        st.caption(
            f"Analysis week (Sun–Sat): **{sun.strftime('%b %d')} – {sat.strftime('%b %d, %Y')}**. "
            f"GSC + GA4 fetches pull the latest 4 complete Sun–Sat weeks ending on this Saturday."
        )
    except Exception:
        pass

    # --- Fetch button (shared: fetches both GSC + GA4) ---
    if is_gsc_configured() or is_ga4_configured():
        _render_fetch_button()
    else:
        st.info(
            "Google APIs not configured. Set `GOOGLE_SERVICE_ACCOUNT_JSON`, "
            "`GSC_PROPERTY`, and/or `GA4_PROPERTY_ID` in `.env`."
        )

    # --- Load data from DB or CSV upload fallback ---
    df = _load_all_gsc_data()

    if df is None:
        uploaded = st.file_uploader(
            "Or upload a GSC CSV",
            type=["csv"],
            key="gsc_upload",
            help=f"Expected columns: {', '.join(EXPECTED_COLUMNS)}",
        )
        if uploaded is not None:
            raw = pd.read_csv(uploaded)
            raw.columns = [c.strip().lower() for c in raw.columns]
            missing = [c for c in EXPECTED_COLUMNS if c not in raw.columns]
            if missing:
                st.error(f"Missing columns: {missing}")
                return
            upsert_gsc(raw[EXPECTED_COLUMNS].to_dict("records"))
            st.success(f"Inserted {len(raw):,} rows.")
            st.rerun()
        return

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
        # "W-SAT" = week ending Saturday → start_time is the Sunday.
        df["period"] = df["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
    else:
        df["period"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)

    # --- Period comparison setup ---
    periods = sorted(df["period"].unique())
    latest_period = periods[-1]
    prev_period = periods[-2] if len(periods) >= 2 else None

    df_latest = df[df["period"] == latest_period]
    df_prev = df[df["period"] == prev_period] if prev_period is not None else None

    # --- Top-level metrics (latest period with delta) ---
    period_label = "Week" if view == "Weekly" else "Month"
    if view == "Weekly":
        period_range_label = (
            f"{latest_period.strftime('%b %d')} – "
            f"{(latest_period + pd.Timedelta(days=6)).strftime('%b %d, %Y')}"
        )
    else:
        period_range_label = latest_period.strftime("%b %Y")
    st.subheader(f"Period Summary ({period_range_label})")

    latest_impressions = df_latest["impressions"].sum()
    latest_clicks = df_latest["clicks"].sum()
    latest_ctr = latest_clicks / latest_impressions * 100 if latest_impressions else 0

    imp_delta = click_delta = ctr_delta = None
    if df_prev is not None:
        prev_impressions = df_prev["impressions"].sum()
        prev_clicks = df_prev["clicks"].sum()
        prev_ctr = prev_clicks / prev_impressions * 100 if prev_impressions else 0
        if prev_impressions:
            imp_delta = f"{(latest_impressions - prev_impressions) / prev_impressions * 100:+.0f}%"
        if prev_clicks:
            click_delta = f"{(latest_clicks - prev_clicks) / prev_clicks * 100:+.0f}%"
        ctr_delta = f"{latest_ctr - prev_ctr:+.1f}pp"

    # Pages appearing in search (proxy for indexed pages)
    pages_in_search = df_latest["page"].nunique()
    pages_delta = None
    if df_prev is not None:
        prev_pages = df_prev["page"].nunique()
        if prev_pages:
            pages_delta = f"{pages_in_search - prev_pages:+d}"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Impressions", f"{latest_impressions:,.0f}", delta=imp_delta)
    m2.metric("Clicks", f"{latest_clicks:,.0f}", delta=click_delta)
    m3.metric("CTR", f"{latest_ctr:.1f}%", delta=ctr_delta)
    m4.metric("Pages in Search", f"{pages_in_search:,}", delta=pages_delta)

    # --- Impressions over time ---
    st.subheader(f"Impressions Trend ({view})")

    trend = df.groupby("period").agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()
    if view == "Weekly":
        trend["period_label"] = trend["period"].apply(
            lambda d: f"{d.strftime('%b %d')} – {(d + pd.Timedelta(days=6)).strftime('%b %d')}"
        )
    else:
        trend["period_label"] = trend["period"].dt.strftime("%b %Y")

    fig_trend = px.bar(
        trend,
        x="period_label",
        y="impressions",
        title=f"{view} Impressions",
        labels={"period_label": "Period", "impressions": "Impressions"},
    )
    fig_trend.update_xaxes(type="category")
    st.plotly_chart(fig_trend, width="stretch")

    trend_summary = "\n".join(f"  {r['period_label']}: {r['impressions']:,.0f}" for _, r in trend.iterrows())
    render_chart_insight("gsc_trend", trend_summary, "What's driving the week-over-week impressions trend?")

    # --- Page category breakdown (latest period with change vs previous) ---
    st.subheader(f"Impressions by Page Category ({period_range_label})")

    cat_latest = (
        df_latest.groupby("page_category")
        .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"))
        .reset_index()
    )
    cat_latest["ctr"] = (cat_latest["clicks"] / cat_latest["impressions"] * 100).round(1)
    cat_latest["% of total"] = (
        cat_latest["impressions"] / latest_impressions * 100
    ).round(1)

    if df_prev is not None:
        cat_prev = (
            df_prev.groupby("page_category")
            .agg(impressions_prev=("impressions", "sum"), clicks_prev=("clicks", "sum"))
            .reset_index()
        )
        cat_latest = cat_latest.merge(cat_prev, on="page_category", how="left").fillna(0)
        cat_latest["imp_change"] = cat_latest.apply(
            lambda r: f"{(r['impressions'] - r['impressions_prev']) / r['impressions_prev'] * 100:+.0f}%"
            if r["impressions_prev"] > 0 else "new",
            axis=1,
        )
        cat_latest["ctr_prev"] = (
            cat_latest["clicks_prev"] / cat_latest["impressions_prev"] * 100
        ).round(1).fillna(0)

    cat_latest = cat_latest.sort_values("impressions", ascending=False)

    display_cols = {
        "page_category": "Page Category",
        "impressions": "Impressions",
        "clicks": "Clicks",
        "ctr": "CTR %",
        "% of total": "% of Total",
    }
    if df_prev is not None:
        display_cols["imp_change"] = "Change"
    st.dataframe(
        cat_latest[list(display_cols.keys())].rename(columns=display_cols),
        hide_index=True,
        width="stretch",
    )

    # --- Page category trend over time ---
    st.subheader(f"Page Category Trend ({view})")

    cat_trend = (
        df.groupby(["period", "page_category"])["impressions"]
        .sum()
        .reset_index()
    )
    if view == "Weekly":
        cat_trend["period_label"] = cat_trend["period"].apply(
            lambda d: f"{d.strftime('%b %d')} – {(d + pd.Timedelta(days=6)).strftime('%b %d')}"
        )
    else:
        cat_trend["period_label"] = cat_trend["period"].dt.strftime("%b %Y")

    fig_cat_trend = px.bar(
        cat_trend,
        x="period_label",
        y="impressions",
        color="page_category",
        barmode="group",
        title=f"Impressions by Page Category ({view})",
        labels={"period_label": "Period", "impressions": "Impressions"},
    )
    fig_cat_trend.update_xaxes(type="category")
    st.plotly_chart(fig_cat_trend, width="stretch")

    # --- Top 10 countries ---
    st.subheader(f"Top Countries ({period_range_label})")

    country_df = query_df(
        "SELECT date, country, clicks, impressions, ctr, position "
        "FROM gsc_country WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )

    if country_df.empty:
        st.info(
            "No country-level data yet. Click **Fetch GSC + GA4 Data** to pull it — "
            "country aggregates are fetched alongside the main GSC data."
        )
    else:
        country_df["date"] = pd.to_datetime(country_df["date"])
        if isinstance(date_range, tuple) and len(date_range) == 2:
            country_df = country_df[
                (country_df["date"].dt.date >= date_range[0])
                & (country_df["date"].dt.date <= date_range[1])
            ]
        if view == "Weekly":
            country_df["period"] = country_df["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
        else:
            country_df["period"] = country_df["date"].dt.to_period("M").apply(lambda r: r.start_time)

        c_latest = country_df[country_df["period"] == latest_period]
        c_prev = country_df[country_df["period"] == prev_period] if prev_period is not None else None

        country_stats = (
            c_latest.groupby("country")
            .agg(
                impressions=("impressions", "sum"),
                clicks=("clicks", "sum"),
                position=("position", "mean"),
            )
            .reset_index()
        )
        country_stats["ctr"] = (country_stats["clicks"] / country_stats["impressions"] * 100).round(2)
        country_stats["position"] = country_stats["position"].round(1)

        if c_prev is not None and not c_prev.empty:
            prev_stats = (
                c_prev.groupby("country")
                .agg(impressions_prev=("impressions", "sum"))
                .reset_index()
            )
            country_stats = country_stats.merge(prev_stats, on="country", how="left").fillna(0)
            country_stats["imp_change"] = country_stats.apply(
                lambda r: f"{(r['impressions'] - r['impressions_prev']) / r['impressions_prev'] * 100:+.0f}%"
                if r["impressions_prev"] > 0 else "new",
                axis=1,
            )

        country_stats = country_stats.sort_values("impressions", ascending=False).head(10)

        display_cols = {
            "country": "Country",
            "impressions": "Impressions",
            "clicks": "Clicks",
            "ctr": "CTR %",
            "position": "Avg Position",
        }
        if "imp_change" in country_stats.columns:
            display_cols["imp_change"] = "Change"
        st.dataframe(
            country_stats[list(display_cols.keys())].rename(columns=display_cols),
            hide_index=True,
            width="stretch",
        )
