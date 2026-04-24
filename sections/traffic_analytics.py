"""Traffic Analytics — Google Analytics (GA4) data via API."""

from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, is_ga4_configured, is_gsc_configured
from db import query_df, upsert_ga4
from llm import render_chart_insight
from sections.fetch_button import render_fetch_button as _render_fetch_button

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


def _enrich_ga4_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns to a GA4 dataframe."""
    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page_path"].apply(categorize_page).astype("category")
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0)
    df["source_normalized"] = df["session_source"].str.lower().str.strip().astype("category")
    df["session_source"] = df["session_source"].astype("category")
    df["session_medium"] = df["session_medium"].astype("category")
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_all_ga4_data() -> pd.DataFrame | None:
    """Load GA4 data from the database (last 4 weeks)."""
    df = query_df(
        "SELECT date, page_path, session_source, session_medium, sessions "
        "FROM ga4 WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )
    if df.empty:
        return None
    return _enrich_ga4_df(df)


@st.cache_data(ttl=300, max_entries=1)
def _load_ga4_traffic() -> pd.DataFrame | None:
    """Source+medium-level traffic with user counts."""
    df = query_df(
        "SELECT date, session_source, session_medium, sessions, total_users, active_users "
        "FROM ga4_traffic WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["session_source"] = df["session_source"].astype("category")
    df["session_medium"] = df["session_medium"].astype("category")
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_ga4_landing_pages() -> pd.DataFrame | None:
    """Entry-point sessions by landing page + source."""
    df = query_df(
        "SELECT date, landing_page, session_source, session_medium, sessions "
        "FROM ga4_landing_pages WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0).astype(int)
    df["page_category"] = df["landing_page"].apply(categorize_page).astype("category")
    df["session_source"] = df["session_source"].astype("category")
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_ga4_category_sessions() -> pd.DataFrame | None:
    """Per-category session counts with source breakdown (GA4 server-side dedup per category)."""
    df = query_df(
        "SELECT date, page_category, session_source, session_medium, sessions "
        "FROM ga4_category_sessions "
        "WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0).astype(int)
    df["session_source"] = df["session_source"].astype("category")
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_ga4_traffic_weekly() -> pd.DataFrame | None:
    """Weekly user acquisition aggregates by firstUserSource (matches GA4 User Acquisition report)."""
    df = query_df(
        "SELECT date, first_user_source, first_user_medium, total_users, new_users "
        "FROM ga4_traffic_weekly WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["first_user_source"] = df["first_user_source"].astype("category")
    df["first_user_medium"] = df["first_user_medium"].astype("category")
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_ga4_events() -> pd.DataFrame | None:
    """Tracked event counts by channel group."""
    df = query_df(
        "SELECT date, event_name, session_primary_channel_group, event_count "
        "FROM ga4_events WHERE date >= CURRENT_DATE - INTERVAL '56 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["event_name"] = df["event_name"].astype("category")
    df["session_primary_channel_group"] = df["session_primary_channel_group"].astype("category")
    return df


def render():
    st.header("Traffic Analytics (GA4)")

    # --- Fetch button (shared: fetches both GSC + GA4) ---
    if is_gsc_configured() or is_ga4_configured():
        _render_fetch_button()
    else:
        st.info(
            "Google APIs not configured. Set `GOOGLE_SERVICE_ACCOUNT_JSON`, "
            "`GSC_PROPERTY`, and/or `GA4_PROPERTY_ID` in `.env`."
        )

    # --- Load data from DB or CSV upload fallback ---
    # `df`      — page-level (date × pagePath × source × medium). Sessions here
    #             are inflated N× when grouped at source level because GA4
    #             attributes one session to every page it visits. Use only for
    #             page-level analysis.
    # `df_src`  — source-level (date × source × medium) from `ga4_traffic`.
    #             Accurate session counts that match the GA4 UI.
    df = _load_all_ga4_data()
    df_src = _load_ga4_traffic()
    df_lp = _load_ga4_landing_pages()
    df_cat = _load_ga4_category_sessions()
    lp_cat_latest = None  # set below once date range is known

    if df is None:
        uploaded = st.file_uploader(
            "Or upload a GA4 CSV",
            type=["csv"],
            key="ga4_upload",
            help=f"Expected columns: {', '.join(EXPECTED_COLUMNS)}",
        )
        if uploaded is not None:
            raw = pd.read_csv(uploaded)
            raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]
            missing = [c for c in EXPECTED_COLUMNS if c not in raw.columns]
            if missing:
                st.error(f"Missing columns: {missing}")
                return
            if "session_medium" not in raw.columns:
                raw["session_medium"] = ""
            rows = raw[["date", "page_path", "session_source", "session_medium", "sessions"]].copy()
            rows["engaged_sessions"] = 0
            rows["engagement_duration_s"] = 0
            rows["new_users"] = 0
            rows["exits"] = 0
            rows["conversions"] = 0
            upsert_ga4(rows.to_dict("records"))
            st.success(f"Inserted {len(raw):,} rows.")
            st.rerun()
        return

    if df is None:
        return

    # --- Date range filter ---
    # Default: the most recent complete Sun–Sat week. Previous period for
    # % deltas = a same-length window immediately before the selected range.
    dates = sorted(df["date"].dt.date.unique())
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
        key="ga4_dates",
    )

    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.info("Pick a start and end date.")
        return

    curr_start, curr_end = date_range
    period_days = (curr_end - curr_start).days + 1
    prev_end = curr_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=period_days - 1)

    if df_src is not None:
        df_src = df_src.copy()
        df_src["source_normalized"] = (
            df_src["session_source"].astype(str).str.lower().str.strip()
        )

    def _slice(frame, start, end):
        return frame[(frame["date"].dt.date >= start) & (frame["date"].dt.date <= end)]

    df_latest = _slice(df, curr_start, curr_end)
    df_prev_raw = _slice(df, prev_start, prev_end)
    df_prev = df_prev_raw if not df_prev_raw.empty else None

    # Source-level frames (accurate session counts; no pagePath inflation).
    if df_src is not None:
        df_src_latest = _slice(df_src, curr_start, curr_end)
        df_src_prev_raw = _slice(df_src, prev_start, prev_end)
        df_src_prev = df_src_prev_raw if not df_src_prev_raw.empty else None
    else:
        df_src_latest = df_latest
        df_src_prev = df_prev

    df_src_display = df_src_latest
    df_src_prev_display = df_src_prev

    period_range_label = f"{curr_start.strftime('%b %d')} – {curr_end.strftime('%b %d, %Y')}"

    # --- Overview (latest period with delta) ---
    st.subheader(f"Period Summary ({period_range_label})")
    st.caption("Total GA4 sessions and distinct traffic sources for the selected period vs the previous same-length window.")
    latest_sessions = df_src_latest["sessions"].sum()  # unfiltered — matches GA4 UI

    session_delta = None
    if df_src_prev is not None:
        prev_sessions = df_src_prev["sessions"].sum()
        if prev_sessions:
            session_delta = f"{(latest_sessions - prev_sessions) / prev_sessions * 100:+.0f}%"

    m1, m2 = st.columns(2)
    m1.metric("Total Sessions", f"{latest_sessions:,.0f}", delta=session_delta)
    m2.metric("Unique Sources", df_src_display["session_source"].nunique())

    # --- Traffic by Medium ---
    st.subheader(f"Traffic by Medium ({period_range_label})")
    st.caption("Sessions grouped by channel type (organic, direct, referral, paid, etc.) — shows which acquisition mediums are growing or shrinking vs the prior period.")

    medium_col = "session_medium"
    if medium_col in df_src_display.columns:
        med_latest = (
            df_src_display.groupby(medium_col)["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )

        if df_src_prev_display is not None:
            med_prev = (
                df_src_prev_display.groupby(medium_col)["sessions"]
                .sum()
                .reset_index()
                .rename(columns={"sessions": "sessions_prev"})
            )
            med_latest = med_latest.merge(med_prev, on=medium_col, how="left").fillna(0)
            med_latest["change"] = med_latest.apply(
                lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
                if r["sessions_prev"] > 0 else "new",
                axis=1,
            )

        med_display = {medium_col: "Medium", "sessions": "Sessions"}
        if df_src_prev_display is not None:
            med_display["change"] = "Change"
        st.dataframe(
            med_latest[list(med_display.keys())].rename(columns=med_display),
            hide_index=True,
            width="stretch",
        )

    # --- Sessions by source (latest period with change) ---
    st.subheader(f"Sessions by Source ({period_range_label})")
    st.caption("Top 30 individual traffic sources ranked by session volume, with period-over-period change. Matches GA4's Traffic Acquisition report numbers.")

    source_latest = (
        df_src_display.groupby("session_source", observed=True)["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    if df_src_prev_display is not None:
        source_prev = (
            df_src_prev_display.groupby("session_source", observed=True)["sessions"]
            .sum()
            .reset_index()
            .rename(columns={"sessions": "sessions_prev"})
        )
        source_latest = source_latest.merge(source_prev, on="session_source", how="left").fillna(0)
        source_latest["change"] = source_latest.apply(
            lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
            if r["sessions_prev"] > 0 else "new",
            axis=1,
        )

    src_display = {"session_source": "Source", "sessions": "Sessions"}
    if df_src_prev_display is not None:
        src_display["change"] = "Change"
    st.dataframe(
        source_latest.head(30)[list(src_display.keys())].rename(columns=src_display),
        hide_index=True,
        width="stretch",
    )

    # --- Source trend over time ---
    st.subheader("Sessions Over Time — Top Sources")
    st.caption("Session volume for the top 8 sources plotted over time — shows which sources are trending up, down, or plateauing across the selected window.")

    granularity = st.radio(
        "Trend granularity",
        ["Daily", "Weekly", "Monthly"],
        horizontal=True,
        key="ga4_granularity",
        help="Changes trend-chart x-axis only. Summary metrics and tables above are driven by the date range.",
    )

    top_sources = source_latest.head(8)["session_source"].tolist()
    trend_df = _bucket(df_src_display, granularity)
    source_trend = (
        trend_df[trend_df["session_source"].isin(top_sources)]
        .groupby(["bucket", "bucket_label", "session_source"], observed=True)["sessions"]
        .sum()
        .reset_index()
        .sort_values("bucket")
    )
    fig_source_trend = px.line(
        source_trend,
        x="bucket_label",
        y="sessions",
        color="session_source",
        title=f"Top Sources Over Time ({granularity})",
        markers=True,
    )
    fig_source_trend.update_xaxes(type="category")
    st.plotly_chart(fig_source_trend, width="stretch")

    totals = (
        source_trend.groupby("session_source", observed=True)["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )
    st.caption(f"Totals across {period_range_label}:")
    st.dataframe(
        totals.rename(columns={"session_source": "Source", "sessions": "Sessions"}),
        hide_index=True,
        width="stretch",
    )

    src_trend_summary = source_trend.groupby("session_source", observed=True)["sessions"].agg(["first", "last"]).reset_index()
    src_trend_text = "\n".join(f"  {r['session_source']}: {int(r['first']):,} → {int(r['last']):,}" for _, r in src_trend_summary.iterrows())
    render_chart_insight("source_trend", src_trend_text, "What's driving changes in traffic sources?")

    # --- Page category breakdown (latest period with change) ---
    # Entry Sessions  = sessions that started on a category page (ga4_landing_pages).
    # Total Sessions  = sessions that visited any page in the category, GA4-accurate
    #                   (ga4_category_sessions, fetched with per-category server-side filters).
    # Other (Total)   = ga4_traffic total − sum of all named-category totals.
    st.subheader(f"Sessions by Page Category ({period_range_label})")

    if df_lp is not None:
        lp_cat_latest = _slice(df_lp, curr_start, curr_end)
        lp_cat_prev_raw = _slice(df_lp, prev_start, prev_end)
        lp_cat_prev = lp_cat_prev_raw if not lp_cat_prev_raw.empty else None
    else:
        lp_cat_latest = None
        lp_cat_prev = None

    # Entry sessions per category
    if lp_cat_latest is not None:
        entry_latest = (
            lp_cat_latest.groupby("page_category", observed=True)["sessions"]
            .sum().reset_index()
            .rename(columns={"sessions": "entry_sessions"})
        )
        entry_prev = (
            lp_cat_prev.groupby("page_category", observed=True)["sessions"]
            .sum().reset_index()
            .rename(columns={"sessions": "entry_prev"})
        ) if lp_cat_prev is not None else None
    else:
        entry_latest = None
        entry_prev = None

    # Total sessions per category (GA4-accurate)
    if df_cat is not None:
        cat_latest_raw = _slice(df_cat, curr_start, curr_end)
        cat_prev_raw = _slice(df_cat, prev_start, prev_end)

        total_latest = (
            cat_latest_raw.groupby("page_category")["sessions"]
            .sum().reset_index()
            .rename(columns={"sessions": "total_sessions"})
        )
        total_prev = (
            cat_prev_raw.groupby("page_category")["sessions"]
            .sum().reset_index()
            .rename(columns={"sessions": "total_prev"})
        ) if not cat_prev_raw.empty else None

        # Compute Other = ga4_traffic total − named category sum
        traffic_total = int(df_src_latest["sessions"].sum()) if df_src is not None else 0
        named_total = int(total_latest["total_sessions"].sum())
        other_total = max(0, traffic_total - named_total)
        other_row = pd.DataFrame([{"page_category": "Other", "total_sessions": other_total}])
        total_latest = pd.concat([total_latest, other_row], ignore_index=True)

        if total_prev is not None:
            traffic_total_prev = int(_slice(df_src, prev_start, prev_end)["sessions"].sum()) if df_src is not None else 0
            named_total_prev = int(total_prev["total_prev"].sum())
            other_prev = max(0, traffic_total_prev - named_total_prev)
            total_prev = pd.concat([
                total_prev,
                pd.DataFrame([{"page_category": "Other", "total_prev": other_prev}])
            ], ignore_index=True)
    else:
        total_latest = None
        total_prev = None

    # Merge entry + total, sort by total sessions
    if total_latest is not None and entry_latest is not None:
        cat_table = total_latest.merge(entry_latest, on="page_category", how="outer").fillna(0)
    elif total_latest is not None:
        cat_table = total_latest.copy()
        cat_table["entry_sessions"] = 0
    elif entry_latest is not None:
        cat_table = entry_latest.rename(columns={"entry_sessions": "entry_sessions"})
        cat_table["total_sessions"] = 0
    else:
        cat_table = None

    if cat_table is not None:
        cat_table = cat_table.sort_values("total_sessions", ascending=False)

        # Period-over-period change columns
        if total_prev is not None:
            cat_table = cat_table.merge(total_prev, on="page_category", how="left").fillna(0)
            cat_table["total_change"] = cat_table.apply(
                lambda r: f"{(r['total_sessions'] - r['total_prev']) / r['total_prev'] * 100:+.0f}%"
                if r["total_prev"] > 0 else "new", axis=1,
            )
        if entry_prev is not None:
            cat_table = cat_table.merge(entry_prev, on="page_category", how="left").fillna(0)
            cat_table["entry_change"] = cat_table.apply(
                lambda r: f"{(r['entry_sessions'] - r['entry_prev']) / r['entry_prev'] * 100:+.0f}%"
                if r["entry_prev"] > 0 else "new", axis=1,
            )

        col_map = {"page_category": "Page Category"}
        if "total_sessions" in cat_table.columns:
            col_map["total_sessions"] = "Total Sessions"
        if "total_change" in cat_table.columns:
            col_map["total_change"] = "Total Δ"
        if "entry_sessions" in cat_table.columns:
            col_map["entry_sessions"] = "Entry Sessions"
        if "entry_change" in cat_table.columns:
            col_map["entry_change"] = "Entry Δ"

        st.dataframe(
            cat_table[list(col_map.keys())].rename(columns=col_map),
            hide_index=True,
            width="stretch",
        )
        st.caption(
            "**Total Sessions** — sessions that visited any page in the category (matches GA4 Traffic Acquisition with page filter). "
            "**Entry Sessions** — sessions that started on a category page."
        )
    else:
        st.info("Re-fetch data to populate page category session counts.")

    # --- Per-source landing page drill-down ---
    st.subheader("Entry Sessions by Page Category — Source Drill-down")
    st.caption("Select a source to see which sections of the site its users first arrive at — entry sessions per page category, not total sessions.")

    available_sources = source_latest["session_source"].tolist()
    drill_source = st.selectbox(
        "Select source to drill down",
        available_sources[:20],
        key="drill_source",
    )

    if df_lp is not None:
        lp_latest = _slice(df_lp, curr_start, curr_end)
        drill_source_df = lp_latest[lp_latest["session_source"].astype(str) == drill_source]
        lp_col = "landing_page"
    else:
        drill_source_df = df_latest[df_latest["session_source"].astype(str) == drill_source]
        lp_col = "page_path"

    drill_df = (
        drill_source_df
        .groupby("page_category", observed=True)["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    # Show total vs Sessions by Source so any remaining mismatch is visible
    lp_total = int(drill_df["sessions"].sum())
    src_total = int(
        source_latest[source_latest["session_source"].astype(str) == drill_source]["sessions"].sum()
    )
    if lp_total != src_total:
        st.caption(
            f"Landing page total: {lp_total:,} | Sessions by Source: {src_total:,} — "
            "re-fetch data to sync (landing page table needs refresh)."
        )

    # Cap chart at top 15 categories; roll the tail into "Other"
    TOP_CHART = 15
    if len(drill_df) > TOP_CHART:
        tail_sessions = int(drill_df.iloc[TOP_CHART:]["sessions"].sum())
        drill_df_chart = drill_df.head(TOP_CHART).copy()
        if tail_sessions:
            other_mask = drill_df_chart["page_category"].astype(str) == "Other"
            if other_mask.any():
                drill_df_chart.loc[other_mask, "sessions"] += tail_sessions
            else:
                drill_df_chart = pd.concat([
                    drill_df_chart,
                    pd.DataFrame([{"page_category": "Other", "sessions": tail_sessions}])
                ], ignore_index=True)
    else:
        drill_df_chart = drill_df

    fig_drill = px.bar(
        drill_df_chart,
        x="page_category",
        y="sessions",
        text="sessions",
        title=f"Page Category Breakdown — {drill_source}",
        color="page_category",
    )
    fig_drill.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_drill.update_layout(showlegend=False)
    st.plotly_chart(fig_drill, width="stretch")

    # "Other" breakdown table
    if "Other" in drill_df["page_category"].astype(str).values:
        other_df = (
            drill_source_df[drill_source_df["page_category"].astype(str) == "Other"]
            .groupby(lp_col)["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )
        with st.expander(f"'Other' breakdown ({other_df['sessions'].sum():,} sessions)", expanded=True):
            st.dataframe(
                other_df.rename(columns={lp_col: "Landing Page", "sessions": "Sessions"}),
                hide_index=True,
                width="stretch",
            )

    # --- Inverted drill-down: source breakdown by page category ---
    st.subheader("Source Breakdown by Page Category")
    st.caption("The inverse drill-down: select a content section to see which sources its visitors come from — useful for understanding each page category's acquisition mix.")

    # Use ga4_category_sessions (GA4-accurate) when available, fall back to landing pages
    if df_cat is not None:
        _cat_src_base = _slice(df_cat, curr_start, curr_end)
        _cat_src_col = "session_source"
    elif lp_cat_latest is not None:
        _cat_src_base = lp_cat_latest
        _cat_src_col = "session_source"
    else:
        _cat_src_base = df_latest
        _cat_src_col = "session_source"

    available_cats = (
        _cat_src_base.groupby("page_category", observed=True)["sessions"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )
    drill_cat = st.selectbox(
        "Select page category to drill down",
        available_cats,
        key="drill_page_category",
    )

    cat_source_df = (
        _cat_src_base[_cat_src_base["page_category"].astype(str) == str(drill_cat)]
        .groupby(_cat_src_col, observed=True)["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
        .head(15)
    )

    fig_cat_src = px.bar(
        cat_source_df,
        x=_cat_src_col,
        y="sessions",
        text="sessions",
        title=f"Top Sources — {drill_cat}",
        color=_cat_src_col,
    )
    fig_cat_src.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_cat_src.update_layout(showlegend=False, xaxis_tickangle=-30)
    st.plotly_chart(fig_cat_src, width="stretch")

    # --- GEO Traffic (AI sources) ---
    st.subheader(f"GEO Traffic — AI Sources ({period_range_label})")
    st.caption("Sessions originating from AI platforms (ChatGPT, Claude, Perplexity, Gemini) — a direct signal of how often AI assistants are driving users to our site.")

    geo_latest = df_src_latest[df_src_latest["source_normalized"].isin(GEO_SOURCES)]
    geo_prev = (
        df_src_prev[df_src_prev["source_normalized"].isin(GEO_SOURCES)]
        if df_src_prev is not None else None
    )

    if geo_latest.empty:
        st.info("No traffic from AI sources (chatgpt.com, claude.ai, perplexity.ai, gemini.google.com) detected.")
    else:
        geo_by_source = (
            geo_latest.groupby("session_source", observed=True)["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )

        if geo_prev is not None and not geo_prev.empty:
            geo_prev_agg = (
                geo_prev.groupby("session_source", observed=True)["sessions"]
                .sum()
                .reset_index()
                .rename(columns={"sessions": "sessions_prev"})
            )
            geo_by_source = geo_by_source.merge(geo_prev_agg, on="session_source", how="left").fillna(0)
            geo_by_source["change"] = geo_by_source.apply(
                lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
                if r["sessions_prev"] > 0 else "new",
                axis=1,
            )

        # Per-source metrics with delta
        geo_cols = st.columns(len(geo_by_source))
        for col, (_, row) in zip(geo_cols, geo_by_source.iterrows()):
            delta = row.get("change") if "change" in geo_by_source.columns else None
            col.metric(row["session_source"], f"{int(row['sessions']):,}", delta=delta)

        # GEO trend — granularity from the chart toggle, within the selected range.
        geo_source_latest = df_src_latest if df_src is not None else df_latest
        geo_all = _bucket(
            geo_source_latest[geo_source_latest["source_normalized"].isin(GEO_SOURCES)],
            granularity,
        )
        geo_trend = (
            geo_all.groupby(["bucket", "bucket_label", "session_source"], observed=True)["sessions"]
            .sum()
            .reset_index()
            .sort_values("bucket")
        )
        fig_geo_trend = px.line(
            geo_trend,
            x="bucket_label",
            y="sessions",
            color="session_source",
            title=f"AI Source Traffic Trend ({granularity})",
            markers=True,
        )
        fig_geo_trend.update_xaxes(type="category")
        st.plotly_chart(fig_geo_trend, width="stretch")

        geo_totals = (
            geo_trend.groupby("session_source", observed=True)["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )
        st.caption(f"Totals across {period_range_label}:")
        st.dataframe(
            geo_totals.rename(columns={"session_source": "AI Source", "sessions": "Sessions"}),
            hide_index=True,
            width="stretch",
        )

        geo_trend_text = "\n".join(
            f"  {r['session_source']}: {int(r['sessions']):,}"
            for _, r in geo_by_source.iterrows()
        )
        render_chart_insight("geo_trend", geo_trend_text, "What's the AI traffic trajectory and what does it mean?")

    # ------------------------------------------------------------------
    # Users & Visits by source/medium
    # Sessions from ga4_traffic (daily, accurate).
    # User counts from ga4_traffic_weekly (weekly aggregate, accurate —
    # daily user counts inflate multi-day visitors when summed).
    # ------------------------------------------------------------------
    traffic = _load_ga4_traffic()
    traffic_weekly = _load_ga4_traffic_weekly()

    if traffic is None:
        st.info(
            "No source+medium-level user metrics yet. Click **Fetch GSC + GA4 Data** to populate."
        )
    else:
        t_latest = _slice(traffic, curr_start, curr_end)
        t_prev_raw = _slice(traffic, prev_start, prev_end)
        t_prev = t_prev_raw if not t_prev_raw.empty else None

        # Weekly slices for user acquisition (week-start date falls within the range).
        tw_latest = _slice(traffic_weekly, curr_start, curr_end) if traffic_weekly is not None else None
        tw_prev_raw = _slice(traffic_weekly, prev_start, prev_end) if traffic_weekly is not None else None
        tw_prev = tw_prev_raw if (tw_prev_raw is not None and not tw_prev_raw.empty) else None

        def _users_by_source(df_weekly, src_col="first_user_source"):
            if df_weekly is None:
                return None
            return (
                df_weekly.groupby(src_col, observed=True)[["total_users", "new_users"]]
                .sum()
                .reset_index()
                .rename(columns={src_col: "source"})
            )

        def _build_src_table(df_sess, df_sess_prev, df_users, df_users_prev):
            """Merge Traffic Acquisition sessions with User Acquisition user counts."""
            sess = (
                df_sess.groupby("session_source", observed=True)[["sessions"]]
                .sum().reset_index()
                .rename(columns={"session_source": "source"})
                .sort_values("sessions", ascending=False)
            )
            if df_users is not None:
                sess = sess.merge(df_users, on="source", how="left").fillna(0)
            else:
                sess["total_users"] = 0
                sess["new_users"] = 0

            if df_sess_prev is not None:
                prev_s = (
                    df_sess_prev.groupby("session_source", observed=True)[["sessions"]]
                    .sum().reset_index()
                    .rename(columns={"session_source": "source", "sessions": "sessions_prev"})
                )
                sess = sess.merge(prev_s, on="source", how="left").fillna(0)
                sess["session_change"] = sess.apply(
                    lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
                    if r["sessions_prev"] > 0 else "new", axis=1,
                )
            if df_users_prev is not None:
                prev_u = df_users_prev.rename(columns={"total_users": "total_users_prev"})
                sess = sess.merge(prev_u[["source", "total_users_prev"]], on="source", how="left").fillna(0)
                sess["user_change"] = sess.apply(
                    lambda r: f"{(r['total_users'] - r['total_users_prev']) / r['total_users_prev'] * 100:+.0f}%"
                    if r["total_users_prev"] > 0 else "new", axis=1,
                )
            return sess

        users_latest = _users_by_source(tw_latest)
        users_prev = _users_by_source(tw_prev)

        st.subheader(f"Users & Sessions by Source ({period_range_label})")
        st.caption(
            "Sessions = Traffic Acquisition (session source). "
            "Users = User Acquisition (first user source). Matches GA4 report numbers."
        )
        by_src = _build_src_table(t_latest, t_prev, users_latest, users_prev).head(30)
        cols_src = {"source": "Source", "sessions": "Sessions", "total_users": "Total Users", "new_users": "New Users"}
        if "session_change" in by_src.columns:
            cols_src["session_change"] = "Session Δ"
        if "user_change" in by_src.columns:
            cols_src["user_change"] = "Users Δ"
        st.dataframe(
            by_src[list(cols_src.keys())].rename(columns=cols_src),
            hide_index=True,
            width="stretch",
        )

        st.subheader(f"Users & Sessions by Medium ({period_range_label})")
        st.caption("Sessions and unique users rolled up by medium — parallels the source table above but at channel-type granularity (organic, direct, referral, etc.).")

        def _users_by_medium(df_weekly):
            if df_weekly is None:
                return None
            agg = (
                df_weekly.groupby("first_user_medium", observed=True)[["total_users", "new_users"]]
                .sum().reset_index()
            )
            agg["first_user_medium"] = agg["first_user_medium"].astype(str)
            return agg.rename(columns={"first_user_medium": "medium"})

        users_by_med_latest = _users_by_medium(tw_latest)
        users_by_med_prev = _users_by_medium(tw_prev)

        med_sess = (
            t_latest.groupby("session_medium", observed=True)[["sessions"]]
            .sum().reset_index().sort_values("sessions", ascending=False)
        )
        med_sess["session_medium"] = med_sess["session_medium"].astype(str)
        med_sess = med_sess.rename(columns={"session_medium": "medium"})
        if users_by_med_latest is not None:
            med_sess = med_sess.merge(users_by_med_latest, on="medium", how="left").fillna(0)
        else:
            med_sess["total_users"] = 0
            med_sess["new_users"] = 0
        if t_prev is not None:
            prev_med = (
                t_prev.groupby("session_medium", observed=True)[["sessions"]]
                .sum().reset_index()
                .rename(columns={"session_medium": "medium", "sessions": "sessions_prev"})
            )
            prev_med["medium"] = prev_med["medium"].astype(str)
            med_sess = med_sess.merge(prev_med, on="medium", how="left").fillna(0)
            med_sess["session_change"] = med_sess.apply(
                lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
                if r["sessions_prev"] > 0 else "new", axis=1,
            )
        cols_med = {"medium": "Medium", "sessions": "Sessions", "total_users": "Total Users", "new_users": "New Users"}
        if "session_change" in med_sess.columns:
            cols_med["session_change"] = "Session Δ"
        st.dataframe(
            med_sess[list(cols_med.keys())].rename(columns=cols_med),
            hide_index=True,
            width="stretch",
        )

        # Users trend — weekly user acquisition data; top 8 first-user sources.
        st.subheader(f"User Acquisition Trend — Top Sources ({period_range_label})")
        st.caption("Total users over time for the top 8 first-user sources — sourced from GA4's User Acquisition report. Log scale available to make smaller sources visible alongside Google.")
        top_users = by_src.head(8)["source"].tolist()
        trend_base = tw_latest.rename(columns={"first_user_source": "session_source"}) if tw_latest is not None else t_latest
        t_latest_b = _bucket(trend_base, granularity)
        trend_users = (
            t_latest_b[t_latest_b["session_source"].isin(top_users)]
            .groupby(["bucket", "bucket_label", "session_source"], observed=True)["total_users"]
            .sum()
            .reset_index()
            .sort_values("bucket")
        )
        user_scale = st.radio(
            "Y-axis scale",
            ["Linear", "Log"],
            horizontal=True,
            index=1,
            key="user_trend_scale",
            help="Log scale makes small sources visible alongside Google.",
        )
        fig_users = px.line(
            trend_users[trend_users["total_users"] > 0],  # drop zeros so log scale works
            x="bucket_label",
            y="total_users",
            color="session_source",
            title=f"Total Users Over Time — Top Sources ({granularity})",
            markers=True,
            log_y=(user_scale == "Log"),
        )
        fig_users.update_xaxes(type="category")
        st.plotly_chart(fig_users, width="stretch")

        user_totals = (
            trend_users.groupby("session_source", observed=True)["total_users"]
            .sum()
            .reset_index()
            .sort_values("total_users", ascending=False)
        )
        st.caption(f"Totals across {period_range_label}:")
        st.dataframe(
            user_totals.rename(columns={"session_source": "Source", "total_users": "Total Users"}),
            hide_index=True,
            width="stretch",
        )

    # ------------------------------------------------------------------
    # Conversion events by channel group
    # ------------------------------------------------------------------
    events = _load_ga4_events()
    if events is None:
        st.info(
            "No event data yet. Click **Fetch GSC + GA4 Data** — tracked events are fetched "
            "alongside the main GA4 data."
        )
    else:
        st.subheader(f"Tracked Events ({period_range_label})")
        st.caption(
            "Three conversion events, split by GA4's **Session primary channel group** "
            "(Organic Search / Paid Search / Direct / Referral / Organic Social / Paid Social / etc.)."
        )

        e_latest = _slice(events, curr_start, curr_end)
        e_prev_raw = _slice(events, prev_start, prev_end)
        e_prev = e_prev_raw if not e_prev_raw.empty else None

        # Per-event totals with delta
        totals = e_latest.groupby("event_name")["event_count"].sum().reset_index()
        if e_prev is not None:
            prev_totals = (
                e_prev.groupby("event_name")["event_count"]
                .sum()
                .reset_index()
                .rename(columns={"event_count": "prev"})
            )
            totals = totals.merge(prev_totals, on="event_name", how="left").fillna(0)

        ev_cols = st.columns(len(totals)) if len(totals) > 0 else None
        for col, (_, row) in zip(ev_cols or [], totals.iterrows()):
            delta = None
            if "prev" in totals.columns and row["prev"] > 0:
                delta = f"{(row['event_count'] - row['prev']) / row['prev'] * 100:+.0f}%"
            col.metric(row["event_name"], f"{int(row['event_count']):,}", delta=delta)

        # Channel-group breakdown (latest period)
        breakdown = (
            e_latest.groupby(["event_name", "session_primary_channel_group"])["event_count"]
            .sum()
            .reset_index()
        )
        if not breakdown.empty:
            fig_ev = px.bar(
                breakdown,
                x="event_name",
                y="event_count",
                color="session_primary_channel_group",
                barmode="stack",
                text="event_count",
                title=f"Events by Channel Group ({period_range_label})",
                labels={"event_name": "Event", "event_count": "Count", "session_primary_channel_group": "Channel Group"},
            )
            fig_ev.update_traces(texttemplate="%{text}", textposition="inside")
            st.plotly_chart(fig_ev, width="stretch")

            st.dataframe(
                breakdown.pivot(
                    index="event_name",
                    columns="session_primary_channel_group",
                    values="event_count",
                ).fillna(0).astype(int).reset_index().rename(columns={"event_name": "Event"}),
                hide_index=True,
                width="stretch",
            )

            # Weekly conversion trend
            e_all = _slice(events, prev_start, curr_end)
            e_all = e_all.copy()
            e_all["week"] = e_all["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
            weekly_conv = (
                e_all.groupby(["week", "event_name"])["event_count"].sum().reset_index()
            )
            weekly_conv["event_name"] = weekly_conv["event_name"].str.replace("bifrost_", "").str.replace("_", " ")
            fig_conv_trend = px.line(
                weekly_conv, x="week", y="event_count", color="event_name",
                markers=True,
                title="Weekly Conversion Trend",
                labels={"week": "Week", "event_count": "Conversions", "event_name": "Event"},
            )
            st.plotly_chart(fig_conv_trend, width="stretch")
