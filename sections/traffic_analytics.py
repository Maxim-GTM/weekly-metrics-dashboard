"""Traffic Analytics — Google Analytics (GA4) data via API."""

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
        "FROM ga4 WHERE date >= CURRENT_DATE - INTERVAL '28 days' ORDER BY date"
    )
    if df.empty:
        return None
    return _enrich_ga4_df(df)


@st.cache_data(ttl=300, max_entries=1)
def _load_ga4_traffic() -> pd.DataFrame | None:
    """Source+medium-level traffic with user counts."""
    df = query_df(
        "SELECT date, session_source, session_medium, sessions, total_users, active_users "
        "FROM ga4_traffic WHERE date >= CURRENT_DATE - INTERVAL '28 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["session_source"] = df["session_source"].astype("category")
    df["session_medium"] = df["session_medium"].astype("category")
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_ga4_events() -> pd.DataFrame | None:
    """Tracked event counts by channel group."""
    df = query_df(
        "SELECT date, event_name, session_primary_channel_group, event_count "
        "FROM ga4_events WHERE date >= CURRENT_DATE - INTERVAL '28 days' ORDER BY date"
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
    df = _load_all_ga4_data()

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
            upsert_ga4(raw[["date", "page_path", "session_source", "session_medium", "sessions"]].to_dict("records"))
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
        key="ga4_dates",
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        df = df[(df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])]

    view = st.radio("View", ["Weekly", "Monthly"], horizontal=True, key="ga4_view")
    if view == "Weekly":
        # "W-SAT" = week ending Saturday → start_time is the Sunday.
        df["period"] = df["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
    else:
        df["period"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)

    # --- Period comparison setup ---
    periods = sorted(df["period"].unique())
    latest_period = periods[-1]
    prev_period = periods[-2] if len(periods) >= 2 else None
    period_label = "Week" if view == "Weekly" else "Month"
    if view == "Weekly":
        period_range_label = (
            f"{latest_period.strftime('%b %d')} – "
            f"{(latest_period + pd.Timedelta(days=6)).strftime('%b %d, %Y')}"
        )
    else:
        period_range_label = latest_period.strftime("%b %Y")

    df_latest = df[df["period"] == latest_period]
    df_prev = df[df["period"] == prev_period] if prev_period is not None else None

    # --- Overview (latest period with delta) ---
    st.subheader(f"Period Summary ({period_range_label})")
    latest_sessions = df_latest["sessions"].sum()

    session_delta = None
    if df_prev is not None:
        prev_sessions = df_prev["sessions"].sum()
        if prev_sessions:
            session_delta = f"{(latest_sessions - prev_sessions) / prev_sessions * 100:+.0f}%"

    m1, m2 = st.columns(2)
    m1.metric("Total Sessions", f"{latest_sessions:,.0f}", delta=session_delta)
    m2.metric("Unique Sources", df_latest["session_source"].nunique())

    # --- Traffic by Medium ---
    st.subheader(f"Traffic by Medium ({period_range_label})")

    medium_col = "session_medium"
    if medium_col in df.columns:
        med_latest = (
            df_latest.groupby(medium_col)["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )

        if df_prev is not None:
            med_prev = (
                df_prev.groupby(medium_col)["sessions"]
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
        if df_prev is not None:
            med_display["change"] = "Change"
        st.dataframe(
            med_latest[list(med_display.keys())].rename(columns=med_display),
            hide_index=True,
            width="stretch",
        )

    # --- Sessions by source (latest period with change) ---
    st.subheader(f"Sessions by Source ({period_range_label})")

    source_latest = (
        df_latest.groupby("session_source")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    if df_prev is not None:
        source_prev = (
            df_prev.groupby("session_source")["sessions"]
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
    if df_prev is not None:
        src_display["change"] = "Change"
    st.dataframe(
        source_latest.head(30)[list(src_display.keys())].rename(columns=src_display),
        hide_index=True,
        width="stretch",
    )

    # --- Source trend over time ---
    st.subheader(f"Source Trend ({view})")

    top_sources = source_latest.head(8)["session_source"].tolist()
    trend_x = "period" if df["period"].nunique() > 1 else "date"
    source_trend = (
        df[df["session_source"].isin(top_sources)]
        .groupby([trend_x, "session_source"])["sessions"]
        .sum()
        .reset_index()
    )
    fig_source_trend = px.line(
        source_trend,
        x=trend_x,
        y="sessions",
        color="session_source",
        title=f"Top Sources Over Time ({view if trend_x == 'period' else 'Daily'})",
        markers=True,
    )
    st.plotly_chart(fig_source_trend, width="stretch")

    if trend_x == "date":
        totals = (
            source_trend.groupby("session_source")["sessions"]
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

    src_trend_summary = source_trend.groupby("session_source")["sessions"].agg(["first", "last"]).reset_index()
    src_trend_text = "\n".join(f"  {r['session_source']}: {int(r['first']):,} → {int(r['last']):,}" for _, r in src_trend_summary.iterrows())
    render_chart_insight("source_trend", src_trend_text, "What's driving changes in traffic sources?")

    # --- Page category breakdown (latest period with change) ---
    st.subheader(f"Sessions by Page Category ({period_range_label})")

    cat_latest = (
        df_latest.groupby("page_category")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
    )

    if df_prev is not None:
        cat_prev = (
            df_prev.groupby("page_category")["sessions"]
            .sum()
            .reset_index()
            .rename(columns={"sessions": "sessions_prev"})
        )
        cat_latest = cat_latest.merge(cat_prev, on="page_category", how="left").fillna(0)
        cat_latest["change"] = cat_latest.apply(
            lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
            if r["sessions_prev"] > 0 else "new",
            axis=1,
        )

    cat_display = {"page_category": "Page Category", "sessions": "Sessions"}
    if df_prev is not None:
        cat_display["change"] = "Change"
    st.dataframe(
        cat_latest[list(cat_display.keys())].rename(columns=cat_display),
        hide_index=True,
        width="stretch",
    )

    # --- Per-source landing page drill-down ---
    st.subheader("Landing Page Breakdown by Source")

    available_sources = source_latest["session_source"].tolist()
    drill_source = st.selectbox(
        "Select source to drill down",
        available_sources[:20],
        key="drill_source",
    )

    drill_df = (
        df_latest[df_latest["session_source"] == drill_source]
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
    st.plotly_chart(fig_drill, width="stretch")

    # --- Inverted drill-down: source breakdown by page category ---
    st.subheader("Source Breakdown by Page Category")

    available_cats = (
        df_latest.groupby("page_category")["sessions"]
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
        df_latest[df_latest["page_category"] == drill_cat]
        .groupby("session_source")["sessions"]
        .sum()
        .reset_index()
        .sort_values("sessions", ascending=False)
        .head(15)
    )

    fig_cat_src = px.bar(
        cat_source_df,
        x="session_source",
        y="sessions",
        text="sessions",
        title=f"Top Sources — {drill_cat}",
        color="session_source",
    )
    fig_cat_src.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_cat_src.update_layout(showlegend=False, xaxis_tickangle=-30)
    st.plotly_chart(fig_cat_src, width="stretch")

    # --- GEO Traffic (AI sources) ---
    st.subheader(f"GEO Traffic — AI Sources ({period_range_label})")

    geo_latest = df_latest[df_latest["source_normalized"].isin(GEO_SOURCES)]
    geo_prev = df_prev[df_prev["source_normalized"].isin(GEO_SOURCES)] if df_prev is not None else None

    if geo_latest.empty:
        st.info("No traffic from AI sources (chatgpt.com, claude.ai, perplexity.ai, gemini.google.com) detected.")
    else:
        geo_by_source = (
            geo_latest.groupby("session_source")["sessions"]
            .sum()
            .reset_index()
            .sort_values("sessions", ascending=False)
        )

        if geo_prev is not None and not geo_prev.empty:
            geo_prev_agg = (
                geo_prev.groupby("session_source")["sessions"]
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

        # GEO trend (full data). Fall back to daily if only one period bucket.
        geo_all = df[df["source_normalized"].isin(GEO_SOURCES)]
        geo_x = "period" if geo_all["period"].nunique() > 1 else "date"
        geo_trend = (
            geo_all.groupby([geo_x, "session_source"])["sessions"]
            .sum()
            .reset_index()
        )
        fig_geo_trend = px.line(
            geo_trend,
            x=geo_x,
            y="sessions",
            color="session_source",
            title=f"AI Source Traffic Trend ({view if geo_x == 'period' else 'Daily'})",
            markers=True,
        )
        st.plotly_chart(fig_geo_trend, width="stretch")

        if geo_x == "date":
            geo_totals = (
                geo_trend.groupby("session_source")["sessions"]
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
    # Users & Visits by source/medium (from ga4_traffic)
    # ------------------------------------------------------------------
    traffic = _load_ga4_traffic()
    if traffic is None:
        st.info(
            "No source+medium-level user metrics yet. Click **Fetch GSC + GA4 Data** to populate."
        )
    else:
        if isinstance(date_range, tuple) and len(date_range) == 2:
            traffic = traffic[
                (traffic["date"].dt.date >= date_range[0])
                & (traffic["date"].dt.date <= date_range[1])
            ]
        if view == "Weekly":
            traffic["period"] = traffic["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
        else:
            traffic["period"] = traffic["date"].dt.to_period("M").apply(lambda r: r.start_time)
        t_latest = traffic[traffic["period"] == latest_period]
        t_prev = traffic[traffic["period"] == prev_period] if prev_period is not None else None

        def _with_change(df_curr, df_prev_agg, key):
            agg = (
                df_curr.groupby(key)[["sessions", "total_users", "active_users"]]
                .sum()
                .reset_index()
                .sort_values("sessions", ascending=False)
            )
            if df_prev_agg is None:
                return agg
            prev = (
                df_prev_agg.groupby(key)[["sessions", "total_users"]]
                .sum()
                .reset_index()
                .rename(columns={"sessions": "sessions_prev", "total_users": "total_users_prev"})
            )
            agg = agg.merge(prev, on=key, how="left").fillna(0)
            agg["session_change"] = agg.apply(
                lambda r: f"{(r['sessions'] - r['sessions_prev']) / r['sessions_prev'] * 100:+.0f}%"
                if r["sessions_prev"] > 0 else "new", axis=1,
            )
            agg["user_change"] = agg.apply(
                lambda r: f"{(r['total_users'] - r['total_users_prev']) / r['total_users_prev'] * 100:+.0f}%"
                if r["total_users_prev"] > 0 else "new", axis=1,
            )
            return agg

        st.subheader(f"Users & Sessions by Source ({period_range_label})")
        by_src = _with_change(t_latest, t_prev, "session_source").head(30)
        cols_src = {"session_source": "Source", "sessions": "Sessions", "total_users": "Total Users", "active_users": "Active Users"}
        if "session_change" in by_src.columns:
            cols_src["session_change"] = "Session Δ"
            cols_src["user_change"] = "Users Δ"
        st.dataframe(
            by_src[list(cols_src.keys())].rename(columns=cols_src),
            hide_index=True,
            width="stretch",
        )

        st.subheader(f"Users & Sessions by Medium ({period_range_label})")
        by_med = _with_change(t_latest, t_prev, "session_medium")
        cols_med = {"session_medium": "Medium", "sessions": "Sessions", "total_users": "Total Users", "active_users": "Active Users"}
        if "session_change" in by_med.columns:
            cols_med["session_change"] = "Session Δ"
            cols_med["user_change"] = "Users Δ"
        st.dataframe(
            by_med[list(cols_med.keys())].rename(columns=cols_med),
            hide_index=True,
            width="stretch",
        )

        # Users trend — top 8 sources. Fall back to daily if only one period bucket.
        top_users = by_src.head(8)["session_source"].tolist()
        users_x = "period" if traffic["period"].nunique() > 1 else "date"
        trend_users = (
            traffic[traffic["session_source"].isin(top_users)]
            .groupby([users_x, "session_source"])["total_users"]
            .sum()
            .reset_index()
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
            x=users_x,
            y="total_users",
            color="session_source",
            title=f"Total Users Over Time — Top Sources ({view if users_x == 'period' else 'Daily'})",
            markers=True,
            log_y=(user_scale == "Log"),
        )
        st.plotly_chart(fig_users, width="stretch")

        if users_x == "date":
            user_totals = (
                trend_users.groupby("session_source")["total_users"]
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

        if isinstance(date_range, tuple) and len(date_range) == 2:
            events = events[
                (events["date"].dt.date >= date_range[0])
                & (events["date"].dt.date <= date_range[1])
            ]
        if view == "Weekly":
            events["period"] = events["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
        else:
            events["period"] = events["date"].dt.to_period("M").apply(lambda r: r.start_time)
        e_latest = events[events["period"] == latest_period]
        e_prev = events[events["period"] == prev_period] if prev_period is not None else None

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
