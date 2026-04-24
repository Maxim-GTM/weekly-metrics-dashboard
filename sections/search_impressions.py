"""Search Impressions — Google Search Console data via API."""

from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, is_ga4_configured, is_gsc_configured
from db import query_df, upsert_gsc, upsert_gsc_page_daily
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


def _bucket(frame: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """Return a copy with `bucket` (timestamp) and `bucket_label` columns.

    Used only to change trend-chart x-axis granularity; summary metrics and
    tables remain driven by the date range selection.
    """
    frame = frame.copy()
    if granularity == "Weekly":
        frame["bucket"] = frame["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
        frame["bucket_label"] = frame["bucket"].apply(
            lambda d: f"{d.strftime('%b %d')} – {(d + pd.Timedelta(days=6)).strftime('%b %d')}"
        )
    elif granularity == "Monthly":
        frame["bucket"] = frame["date"].dt.to_period("M").apply(lambda r: r.start_time)
        frame["bucket_label"] = frame["bucket"].dt.strftime("%b %Y")
    else:  # Daily
        frame["bucket"] = frame["date"].dt.normalize()
        frame["bucket_label"] = frame["bucket"].dt.strftime("%b %d")
    return frame


@st.cache_data(ttl=300, max_entries=1)
def _load_gsc_site_daily() -> pd.DataFrame | None:
    """Load site-level GSC totals (date only, no page dimension).

    Matches GSC UI headline numbers exactly — avoids multi-URL-per-query
    impression inflation that occurs when summing page-level rows.
    """
    df = query_df(
        "SELECT date, clicks, impressions "
        "FROM gsc_site_daily WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_all_gsc_data() -> pd.DataFrame | None:
    """Load page-level GSC aggregates (last 8 weeks) for breakdown charts/tables."""
    df = query_df(
        "SELECT date, page, clicks, impressions "
        "FROM gsc_page_daily WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
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
    # site_df: date-level totals for headline metrics (matches GSC UI)
    # df: page-level data for breakdowns and trend charts
    site_df = _load_gsc_site_daily()
    df = _load_all_gsc_data()

    if site_df is None and df is None:
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
            # CSV exports with a `query` dimension still under-count anonymized
            # queries — but this keeps the upload path working until the user
            # re-fetches via the API. Aggregate best-effort to (date, page).
            page_agg = (
                raw.groupby(["date", "page"], as_index=False)
                .agg(clicks=("clicks", "sum"), impressions=("impressions", "sum"))
            )
            page_agg["ctr"] = page_agg["clicks"] / page_agg["impressions"].replace(0, 1)
            page_agg["position"] = 0.0
            upsert_gsc_page_daily(page_agg.to_dict("records"))
            st.success(f"Inserted {len(raw):,} rows.")
            st.rerun()
        return

    # --- Date range filter ---
    # Default: the most recent complete Sun–Sat week that exists in the data.
    # Prior period (for % deltas) = a same-length window immediately before.
    ref_df = site_df if site_df is not None else df
    dates = sorted(ref_df["date"].dt.date.unique())
    try:
        from google_api import latest_complete_week
        default_start, default_end = latest_complete_week()
    except Exception:
        default_end = max(dates)
        default_start = default_end - timedelta(days=6)
    default_start = max(default_start, min(dates))
    default_end = min(default_end, max(dates))
    date_range = st.date_input(
        "Date range",
        value=(default_start, default_end),
        min_value=min(dates),
        max_value=max(dates),
        key="gsc_dates",
    )

    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.info("Pick a start and end date.")
        return

    curr_start, curr_end = date_range
    period_days = (curr_end - curr_start).days + 1
    prev_end = curr_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=period_days - 1)

    period_range_label = f"{curr_start.strftime('%b %d')} – {curr_end.strftime('%b %d, %Y')}"
    st.subheader(f"Period Summary ({period_range_label})")
    st.caption("Total impressions, clicks, CTR, and unique pages Google served our site for in search results during this period, compared to the previous same-length window.")

    # Headline metrics from site-level totals (matches GSC UI)
    if site_df is not None:
        site_latest = site_df[(site_df["date"].dt.date >= curr_start) & (site_df["date"].dt.date <= curr_end)]
        site_prev_raw = site_df[(site_df["date"].dt.date >= prev_start) & (site_df["date"].dt.date <= prev_end)]
        site_prev = site_prev_raw if not site_prev_raw.empty else None
        latest_impressions = site_latest["impressions"].sum()
        latest_clicks = site_latest["clicks"].sum()
    elif df is not None:
        df_latest_tmp = df[(df["date"].dt.date >= curr_start) & (df["date"].dt.date <= curr_end)]
        site_prev = None
        latest_impressions = df_latest_tmp["impressions"].sum()
        latest_clicks = df_latest_tmp["clicks"].sum()
    else:
        latest_impressions = latest_clicks = 0
        site_prev = None

    latest_ctr = latest_clicks / latest_impressions * 100 if latest_impressions else 0

    imp_delta = click_delta = ctr_delta = None
    if site_prev is not None:
        prev_impressions = site_prev["impressions"].sum()
        prev_clicks = site_prev["clicks"].sum()
        prev_ctr = prev_clicks / prev_impressions * 100 if prev_impressions else 0
        if prev_impressions:
            imp_delta = f"{(latest_impressions - prev_impressions) / prev_impressions * 100:+.0f}%"
        if prev_clicks:
            click_delta = f"{(latest_clicks - prev_clicks) / prev_clicks * 100:+.0f}%"
        ctr_delta = f"{latest_ctr - prev_ctr:+.1f} pts"

    # Pages in search from page-level data (site_daily has no page column)
    pages_in_search = 0
    pages_delta = None
    if df is not None:
        df_latest = df[(df["date"].dt.date >= curr_start) & (df["date"].dt.date <= curr_end)]
        df_prev_raw = df[(df["date"].dt.date >= prev_start) & (df["date"].dt.date <= prev_end)]
        df_prev = df_prev_raw if not df_prev_raw.empty else None
        pages_in_search = df_latest["page"].nunique()
        if df_prev is not None:
            prev_pages = df_prev["page"].nunique()
            if prev_pages:
                pages_delta = f"{pages_in_search - prev_pages:+d}"
    else:
        df_latest = pd.DataFrame()
        df_prev = None

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Impressions", f"{latest_impressions:,.0f}", delta=imp_delta)
    m2.metric("Clicks", f"{latest_clicks:,.0f}", delta=click_delta)
    m3.metric("CTR", f"{latest_ctr:.1f}%", delta=ctr_delta)
    m4.metric("Pages Receiving Impressions", f"{pages_in_search:,}", delta=pages_delta)

    if df_latest.empty:
        return

    # --- Impressions over time ---
    st.subheader("Impressions Trend")
    st.caption("How total search impressions have moved across the selected window — choose a granularity to smooth daily noise or zoom into specific spikes.")

    granularity = st.radio(
        "Trend granularity",
        ["Daily", "Weekly", "Monthly"],
        horizontal=True,
        key="gsc_granularity",
        help="Changes trend-chart x-axis only. Summary metrics and tables above are driven by the date range.",
    )
    df_latest_b = _bucket(df_latest, granularity)

    trend = (
        df_latest_b.groupby(["bucket", "bucket_label"], observed=True)
        .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"))
        .reset_index()
        .sort_values("bucket")
    )

    fig_trend = px.bar(
        trend,
        x="bucket_label",
        y="impressions",
        title=f"{granularity} Impressions",
        labels={"bucket_label": granularity.rstrip("ly") or "Date", "impressions": "Impressions"},
    )
    fig_trend.update_xaxes(type="category")
    st.plotly_chart(fig_trend, width="stretch")

    trend_summary = "\n".join(f"  {r['bucket_label']}: {r['impressions']:,.0f}" for _, r in trend.iterrows())
    render_chart_insight("gsc_trend", trend_summary, "What's driving the impressions trend over this period?")

    # --- Page category breakdown (latest period with change vs previous) ---
    st.subheader(f"Impressions by Page Category ({period_range_label})")
    st.caption("Search impressions and clicks for each section of the site, with each category's share of total impressions and its change vs the prior period.")

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
    st.subheader("Impressions by Page Category Over Time")
    st.caption("How impressions for each content section have shifted across the period — use this to spot which areas are gaining or losing search visibility relative to each other.")

    cat_trend = (
        df_latest_b.groupby(["bucket", "bucket_label", "page_category"], observed=True)["impressions"]
        .sum()
        .reset_index()
        .sort_values("bucket")
    )

    fig_cat_trend = px.bar(
        cat_trend,
        x="bucket_label",
        y="impressions",
        color="page_category",
        barmode="group",
        title=f"Impressions by Page Category ({granularity})",
        labels={"bucket_label": granularity.rstrip("ly") or "Date", "impressions": "Impressions"},
    )
    fig_cat_trend.update_xaxes(type="category")
    st.plotly_chart(fig_cat_trend, width="stretch")

    # --- Top 10 countries ---
    st.subheader(f"Top Countries ({period_range_label})")
    st.caption("The 10 countries generating the most impressions, with average ranking position and period-over-period impression change — useful for spotting geographic reach shifts.")

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
        c_latest = country_df[
            (country_df["date"].dt.date >= curr_start)
            & (country_df["date"].dt.date <= curr_end)
        ]
        c_prev_raw = country_df[
            (country_df["date"].dt.date >= prev_start)
            & (country_df["date"].dt.date <= prev_end)
        ]
        c_prev = c_prev_raw if not c_prev_raw.empty else None

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
            "position": "Avg Position (1=top)",
        }
        if "imp_change" in country_stats.columns:
            display_cols["imp_change"] = "Change"
        st.dataframe(
            country_stats[list(display_cols.keys())].rename(columns=display_cols),
            hide_index=True,
            width="stretch",
        )

