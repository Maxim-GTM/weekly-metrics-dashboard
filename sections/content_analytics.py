"""Content Analytics — engagement quality, channel comparison, landing pages,
conversions, and dropoff analysis."""

from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from config import categorize_page, is_ga4_configured
from db import query_df
from llm import analyse_content_drill_down, analyse_dropoff_pages
from sections.fetch_button import render_fetch_button as _render_fetch_button

GEO_SOURCES = ["chatgpt.com", "claude.ai", "perplexity.ai", "gemini.google.com"]

# Navigation/archive paths that are not real content — excluded from all analyses
_NAV_PATTERNS = r"/author/|/tag/|/page/\d"


def _drop_nav_pages(df: pd.DataFrame, path_col: str = "page_path") -> pd.DataFrame:
    return df[~df[path_col].str.contains(_NAV_PATTERNS, na=False, regex=True)]

DRILL_DIMENSIONS = {
    "Sessions": "sessions",
    "Engagement Rate": "engagement_rate",
    "Bounce Rate": "bounce_rate",
    "Avg Engagement Time": "avg_engagement_s",
    "New User Rate": "new_user_rate",
    "Conversions": "conversions",
}


def _fmt_duration(seconds: float) -> str:
    s = max(0, int(seconds))
    return f"{s // 60}:{s % 60:02d}"


def _pct(v: float) -> str:
    return f"{v:.1%}"


@st.cache_data(ttl=300)
def _load_ga4() -> pd.DataFrame | None:
    df = query_df(
        "SELECT date, page_path, session_source, session_medium, "
        "sessions, engaged_sessions, engagement_duration_s, new_users, "
        "exits, conversions "
        "FROM ga4 WHERE date >= CURRENT_DATE - INTERVAL '90 days' ORDER BY date"
    )
    if df.empty:
        return None
    df = _drop_nav_pages(df)
    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["page_path"].apply(categorize_page).astype("category")
    for col in ["sessions", "engaged_sessions", "engagement_duration_s",
                "new_users", "exits", "conversions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def _load_traffic() -> pd.DataFrame | None:
    df = query_df(
        "SELECT date, session_source, session_medium, sessions, "
        "engaged_sessions, new_users, engagement_duration_s "
        "FROM ga4_traffic WHERE date >= CURRENT_DATE - INTERVAL '90 days' ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    for col in ["sessions", "engaged_sessions", "new_users", "engagement_duration_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def _load_landing_pages() -> pd.DataFrame | None:
    df = query_df(
        "SELECT date, landing_page, session_source, session_medium, "
        "sessions, engaged_sessions, new_users, engagement_duration_s, conversions "
        "FROM ga4_landing_pages WHERE date >= CURRENT_DATE - INTERVAL '90 days' ORDER BY date"
    )
    if df.empty:
        return None
    df = _drop_nav_pages(df, path_col="landing_page")
    df["date"] = pd.to_datetime(df["date"])
    df["page_category"] = df["landing_page"].apply(categorize_page).astype("category")
    for col in ["sessions", "engaged_sessions", "new_users", "engagement_duration_s", "conversions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df





def _derive_metrics(agg: pd.DataFrame) -> pd.DataFrame:
    """Add rate columns to an aggregated DataFrame that has the raw count columns."""
    agg = agg.copy()
    safe_sessions = agg["sessions"].replace(0, pd.NA)
    agg["engagement_rate"] = (agg["engaged_sessions"] / safe_sessions).fillna(0)
    agg["bounce_rate"] = 1 - agg["engagement_rate"]
    agg["avg_engagement_s"] = (agg["engagement_duration_s"] / safe_sessions).fillna(0)
    if "new_users" in agg.columns:
        agg["new_user_rate"] = (agg["new_users"] / safe_sessions).fillna(0)
    return agg


def _page_table_text(df: pd.DataFrame) -> str:
    """Format a page-level DataFrame as plain text for LLM prompts."""
    lines = []
    cols = ["page_path", "sessions", "engagement_rate", "bounce_rate", "avg_engagement_s"]
    if "new_user_rate" in df.columns:
        cols.append("new_user_rate")
    if "conversions" in df.columns:
        cols.append("conversions")
    for _, row in df[cols].iterrows():
        parts = [
            f"page={row['page_path']}",
            f"sessions={int(row['sessions'])}",
            f"eng={row['engagement_rate']:.0%}",
            f"bounce={row['bounce_rate']:.0%}",
            f"avg_time={_fmt_duration(row['avg_engagement_s'])}",
        ]
        if "new_user_rate" in row.index:
            parts.append(f"new_user_rate={row['new_user_rate']:.0%}")
        if "conversions" in row.index:
            parts.append(f"conversions={int(row['conversions'])}")
        lines.append("  " + " | ".join(parts))
    return "\n".join(lines) if lines else "(no data)"


def render():
    st.header("Content Analytics (GA4)")

    if is_ga4_configured():
        _render_fetch_button()

    df = _load_ga4()
    df_src = _load_traffic()
    df_lp = _load_landing_pages()

    if df is None:
        st.info("No GA4 data yet. Fetch data or check your GA4 configuration.")
        return

    # --- Date range selector ---
    dates = sorted(df["date"].dt.date.unique())
    default_end = max(dates)
    default_start = max(default_end - timedelta(days=27), min(dates))

    date_range = st.date_input(
        "Date range",
        value=(default_start, default_end),
        min_value=min(dates),
        max_value=max(dates),
        key="content_analytics_dates",
    )
    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.info("Pick a start and end date.")
        return

    curr_start, curr_end = date_range
    date_label = f"{curr_start:%b %d} – {curr_end:%b %d, %Y}"
    mask = (df["date"].dt.date >= curr_start) & (df["date"].dt.date <= curr_end)
    df_f = df[mask]

    tabs = st.tabs([
        "Content Quality",
        "Channel Quality",
        "Landing Pages",
    ])

    # =========================================================================
    # TAB 1: Content Quality + Category Drill-Down
    # =========================================================================
    with tabs[0]:
        st.subheader("Content Quality by Category")

        cat = (
            df_f.groupby("page_category", observed=True)
            .agg(
                sessions=("sessions", "sum"),
                engaged_sessions=("engaged_sessions", "sum"),
                engagement_duration_s=("engagement_duration_s", "sum"),
                new_users=("new_users", "sum"),
                exits=("exits", "sum"),
                conversions=("conversions", "sum"),
            )
            .reset_index()
        )
        cat = _derive_metrics(cat[cat["sessions"] > 0])
        cat = cat.sort_values("sessions", ascending=False)

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                cat.sort_values("engagement_rate", ascending=True),
                x="engagement_rate", y="page_category", orientation="h",
                labels={"engagement_rate": "Engagement Rate", "page_category": "Category"},
                title="Engagement Rate by Category",
            )
            fig.update_xaxes(tickformat=".0%")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig2 = px.bar(
                cat.sort_values("avg_engagement_s", ascending=True),
                x="avg_engagement_s", y="page_category", orientation="h",
                labels={"avg_engagement_s": "Avg Time (s)", "page_category": "Category"},
                title="Avg Engagement Time by Category",
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Summary table
        tbl = cat[[
            "page_category", "sessions", "engagement_rate", "bounce_rate",
            "avg_engagement_s", "new_user_rate", "conversions",
        ]].rename(columns={
            "page_category": "Category", "sessions": "Sessions",
            "engagement_rate": "Eng. Rate", "bounce_rate": "Bounce Rate",
            "avg_engagement_s": "Avg Time", "new_user_rate": "New User Rate",
            "conversions": "Conversions",
        }).copy()
        tbl["Eng. Rate"] = tbl["Eng. Rate"].map(_pct)
        tbl["Bounce Rate"] = tbl["Bounce Rate"].map(_pct)
        tbl["Avg Time"] = tbl["Avg Time"].apply(_fmt_duration)
        tbl["New User Rate"] = tbl["New User Rate"].map(_pct)
        st.dataframe(tbl, use_container_width=True, hide_index=True)

        st.divider()

        # --- Category Drill-Down ---
        st.subheader("Category Drill-Down")

        categories = sorted(cat["page_category"].astype(str).tolist())
        dim_options = [k for k in DRILL_DIMENSIONS if k != "Conversions" or df_f["conversions"].sum() > 0]

        drill_col1, drill_col2 = st.columns([2, 2])
        with drill_col1:
            selected_cat = st.selectbox("Category", categories, key="drill_cat")
        with drill_col2:
            rank_by_label = st.selectbox("Rank by", dim_options, key="drill_dim")

        rank_by_col = DRILL_DIMENSIONS[rank_by_label]

        pages_in_cat = df_f[df_f["page_category"] == selected_cat]
        page_agg = (
            pages_in_cat.groupby("page_path", observed=True)
            .agg(
                sessions=("sessions", "sum"),
                engaged_sessions=("engaged_sessions", "sum"),
                engagement_duration_s=("engagement_duration_s", "sum"),
                new_users=("new_users", "sum"),
                exits=("exits", "sum"),
                conversions=("conversions", "sum"),
            )
            .reset_index()
        )
        page_agg = _derive_metrics(page_agg[page_agg["sessions"] > 0])

        # Only include rank column if it exists and has data
        if rank_by_col not in page_agg.columns or page_agg[rank_by_col].sum() == 0:
            if rank_by_col == "conversions":
                st.caption("No conversions data yet — fetch new GA4 data to populate.")
                rank_by_col = "sessions"
                rank_by_label = "Sessions"
                ascending = False

        # Top 20 = highest values, Bottom 20 = lowest values, regardless of metric type
        page_agg_sorted = page_agg.sort_values(rank_by_col, ascending=False)
        top_20 = page_agg_sorted.head(20)
        bottom_20 = page_agg_sorted.tail(20)

        # Bar chart: top 20 (ascending=True so longest bar is at the top visually)
        top_sorted = top_20.sort_values(rank_by_col, ascending=True)
        chart_h = max(400, len(top_sorted) * 38)
        fig_top = px.bar(
            top_sorted,
            x=rank_by_col, y="page_path", orientation="h",
            labels={rank_by_col: rank_by_label, "page_path": ""},
            title=f"Top 20 — {selected_cat} by {rank_by_label}",
            height=chart_h,
        )
        if rank_by_label in ("Engagement Rate", "Bounce Rate", "New User Rate"):
            fig_top.update_xaxes(tickformat=".0%")
        if rank_by_label == "Avg Engagement Time":
            fig_top.update_xaxes(title="Avg Time (s)")
        fig_top.update_yaxes(tickmode="linear", automargin=True)
        fig_top.update_layout(margin=dict(l=300, r=20, t=40, b=40))
        st.plotly_chart(fig_top, use_container_width=True)

        # Engagement rate distribution pie
        if len(page_agg) > 0:
            buckets = pd.cut(
                page_agg["engagement_rate"],
                bins=[0, 0.2, 0.4, 0.6, 0.8, 1.01],
                labels=["0–20%", "20–40%", "40–60%", "60–80%", "80–100%"],
                right=False,
            ).value_counts().reset_index()
            buckets.columns = ["Engagement Rate Bucket", "Pages"]
            fig_pie = px.pie(
                buckets, names="Engagement Rate Bucket", values="Pages",
                title=f"Engagement Rate Distribution — {selected_cat}",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        # Top 20 / Bottom 20 tables
        def _format_page_table(frame: pd.DataFrame) -> pd.DataFrame:
            cols = ["page_path", "sessions", "engagement_rate", "bounce_rate",
                    "avg_engagement_s", "new_user_rate"]
            if frame["conversions"].sum() > 0:
                cols.append("conversions")
            out = frame[cols].sort_values(rank_by_col, ascending=False).rename(columns={
                "page_path": "Page", "sessions": "Sessions",
                "engagement_rate": "Eng. Rate", "bounce_rate": "Bounce Rate",
                "avg_engagement_s": "Avg Time", "new_user_rate": "New User Rate",
                "conversions": "Conversions",
            }).copy()
            out["Eng. Rate"] = out["Eng. Rate"].map(_pct)
            out["Bounce Rate"] = out["Bounce Rate"].map(_pct)
            out["Avg Time"] = out["Avg Time"].apply(_fmt_duration)
            out["New User Rate"] = out["New User Rate"].map(_pct)
            if "Exit Rate" in out.columns:
                out["Exit Rate"] = out["Exit Rate"].map(_pct)
            return out

        t1, t2 = st.columns(2)
        with t1:
            st.markdown(f"**Top 20 — {rank_by_label}**")
            st.dataframe(_format_page_table(top_20), use_container_width=True, hide_index=True)
        with t2:
            st.markdown(f"**Bottom 20 — {rank_by_label}**")
            st.dataframe(_format_page_table(bottom_20), use_container_width=True, hide_index=True)

        # --- Analyse button ---
        analyse_key = f"drill_analysis_{selected_cat}_{rank_by_label}_{curr_start}_{curr_end}"
        if st.button("Analyse", key=f"btn_{analyse_key}", type="primary"):
            # Build first-touch conversion pages in this category from landing pages data
            converting_text = "(no landing page conversion data yet — fetch new data)"
            if df_lp is not None:
                mask_lp = (df_lp["date"].dt.date >= curr_start) & (df_lp["date"].dt.date <= curr_end)
                lp_cat = df_lp[mask_lp & (df_lp["page_category"] == selected_cat)]
                lp_conv = (
                    lp_cat.groupby("landing_page")
                    .agg(sessions=("sessions", "sum"), conversions=("conversions", "sum"))
                    .reset_index()
                )
                lp_conv = lp_conv[lp_conv["conversions"] > 0].sort_values("conversions", ascending=False)
                if not lp_conv.empty:
                    converting_text = "\n".join(
                        f"  {row['landing_page']} — {row['conversions']} conversions from {row['sessions']} entry sessions"
                        for _, row in lp_conv.iterrows()
                    )
                elif df_f["conversions"].sum() == 0:
                    converting_text = "(conversions not yet fetched — re-fetch GA4 data)"
                else:
                    converting_text = "(none in this category for the selected period)"

            with st.spinner("Analysing..."):
                result = analyse_content_drill_down(
                    category=selected_cat,
                    date_range=date_label,
                    rank_by=rank_by_label,
                    top_pages_text=_page_table_text(top_20),
                    bottom_pages_text=_page_table_text(bottom_20),
                    converting_landing_pages_text=converting_text,
                )
                st.session_state[analyse_key] = result

        if analyse_key in st.session_state and st.session_state[analyse_key]:
            st.info(st.session_state[analyse_key])

        st.divider()

        # --- Dropoff pages ---
        st.subheader("Top Dropoff Pages")
        st.caption(
            "High-traffic pages with high bounce rate (low engagement) and zero conversions. "
            "Pages that appear in converting sessions are excluded — these are pure dead ends."
        )

        page_bounce = (
            df_f.groupby("page_path")
            .agg(
                sessions=("sessions", "sum"),
                engaged_sessions=("engaged_sessions", "sum"),
                engagement_duration_s=("engagement_duration_s", "sum"),
                conversions=("conversions", "sum"),
            )
            .reset_index()
        )
        page_bounce = _derive_metrics(page_bounce[page_bounce["sessions"] >= 20])
        page_bounce["page_category"] = page_bounce["page_path"].apply(categorize_page)

        converting_pages = set(page_bounce[page_bounce["conversions"] > 0]["page_path"].tolist())
        dropoff_pages = page_bounce[
            (~page_bounce["page_path"].isin(converting_pages)) &
            (page_bounce["bounce_rate"] >= 0.5)
        ].sort_values("sessions", ascending=False).head(30)

        top_conv_for_ref = page_bounce[
            page_bounce["page_path"].isin(converting_pages)
        ].sort_values("conversions", ascending=False).head(10)

        if dropoff_pages.empty:
            st.info("No dropoff pages found for the selected period.")
        else:
            fig_drop = px.bar(
                dropoff_pages.head(20).sort_values("sessions", ascending=True),
                x="sessions", y="page_path", orientation="h",
                labels={"sessions": "Sessions", "page_path": "Page"},
                title="Top 20 Dropoff Pages — high traffic, high bounce, zero conversions",
            )
            st.plotly_chart(fig_drop, use_container_width=True)

            display_drop = dropoff_pages[[
                "page_path", "page_category", "sessions",
                "bounce_rate", "engagement_rate", "avg_engagement_s",
            ]].rename(columns={
                "page_path": "Page", "page_category": "Category",
                "sessions": "Sessions", "bounce_rate": "Bounce Rate",
                "engagement_rate": "Eng. Rate", "avg_engagement_s": "Avg Time",
            }).copy()
            display_drop["Bounce Rate"] = display_drop["Bounce Rate"].map(_pct)
            display_drop["Eng. Rate"] = display_drop["Eng. Rate"].map(_pct)
            display_drop["Avg Time"] = display_drop["Avg Time"].apply(_fmt_duration)
            st.dataframe(display_drop, use_container_width=True, hide_index=True)

            drop_key = f"dropoff_analysis_{curr_start}_{curr_end}"
            if st.button("Analyse Dropoffs", key=f"btn_{drop_key}", type="primary"):
                drop_text = _page_table_text(dropoff_pages.head(20))
                conv_ref_text = _page_table_text(top_conv_for_ref) if not top_conv_for_ref.empty else "(no conversion data)"
                with st.spinner("Analysing..."):
                    result = analyse_dropoff_pages(
                        date_range=date_label,
                        dropoff_pages_text=drop_text,
                        converting_pages_text=conv_ref_text,
                    )
                    st.session_state[drop_key] = result

            if drop_key in st.session_state and st.session_state[drop_key]:
                st.info(st.session_state[drop_key])

    # =========================================================================
    # TAB 2: Channel Quality
    # =========================================================================
    with tabs[1]:
        st.subheader("Channel Quality by Source")

        if df_src is None:
            st.info("No traffic source data available.")
        else:
            mask_src = (df_src["date"].dt.date >= curr_start) & (df_src["date"].dt.date <= curr_end)
            src = (
                df_src[mask_src]
                .groupby("session_source", observed=True)
                .agg(
                    sessions=("sessions", "sum"),
                    engaged_sessions=("engaged_sessions", "sum"),
                    engagement_duration_s=("engagement_duration_s", "sum"),
                    new_users=("new_users", "sum"),
                )
                .reset_index()
            )
            src = src[src["sessions"] >= 20].copy()

            if src.empty:
                st.info("Not enough data for the selected range (min 20 sessions per source).")
            else:
                src = _derive_metrics(src)
                src = src.sort_values("sessions", ascending=False).head(15)

                has_eng_data = src["engagement_rate"].sum() > 0

                if not has_eng_data:
                    st.caption("Engagement data is 0 — re-fetch GA4 data to populate engagement metrics.")

                col1, col2 = st.columns(2)
                with col1:
                    sorted_eng = src.sort_values("engagement_rate", ascending=True)
                    fig = px.bar(
                        sorted_eng, x="engagement_rate", y="session_source", orientation="h",
                        labels={"engagement_rate": "Engagement Rate", "session_source": "Source"},
                        title="Engagement Rate by Source",
                    )
                    fig.update_xaxes(tickformat=".0%")
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    sorted_time = src.sort_values("avg_engagement_s", ascending=True)
                    fig2 = px.bar(
                        sorted_time, x="avg_engagement_s", y="session_source", orientation="h",
                        labels={"avg_engagement_s": "Avg Time (s)", "session_source": "Source"},
                        title="Avg Engagement Time by Source",
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                # Weekly trend: engagement rate for top 5 sources
                top5_sources = src.head(5)["session_source"].tolist()
                mask_trend = (
                    (df_src["date"].dt.date >= curr_start) &
                    (df_src["date"].dt.date <= curr_end) &
                    (df_src["session_source"].isin(top5_sources))
                )
                trend = df_src[mask_trend].copy()
                trend["week"] = trend["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
                weekly_trend = (
                    trend.groupby(["week", "session_source"], observed=True)
                    .agg(sessions=("sessions", "sum"), engaged_sessions=("engaged_sessions", "sum"))
                    .reset_index()
                )
                weekly_trend = weekly_trend[weekly_trend["sessions"] > 0].copy()
                weekly_trend["engagement_rate"] = weekly_trend["engaged_sessions"] / weekly_trend["sessions"]

                if weekly_trend["engagement_rate"].sum() > 0:
                    fig3 = px.line(
                        weekly_trend, x="week", y="engagement_rate", color="session_source",
                        labels={
                            "week": "Week", "engagement_rate": "Engagement Rate",
                            "session_source": "Source",
                        },
                        title="Weekly Engagement Rate — Top 5 Sources",
                        markers=True,
                    )
                    fig3.update_yaxes(tickformat=".0%")
                    st.plotly_chart(fig3, use_container_width=True)

                display_src = src.sort_values("engagement_rate", ascending=False)[[
                    "session_source", "sessions", "new_users", "engagement_rate", "avg_engagement_s",
                ]].rename(columns={
                    "session_source": "Source", "sessions": "Sessions", "new_users": "New Users",
                    "engagement_rate": "Eng. Rate", "avg_engagement_s": "Avg Time",
                }).copy()
                display_src["Eng. Rate"] = display_src["Eng. Rate"].map(_pct)
                display_src["Avg Time"] = display_src["Avg Time"].apply(_fmt_duration)
                st.dataframe(display_src, use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB 3: Landing Pages
    # =========================================================================
    with tabs[2]:
        st.subheader("Top Landing Pages")
        st.caption("Entry-point sessions only — first page of the session.")

        if df_lp is None:
            st.info("No landing page data yet. Fetch GA4 data to populate.")
        else:
            mask_lp = (df_lp["date"].dt.date >= curr_start) & (df_lp["date"].dt.date <= curr_end)
            df_lp_f = df_lp[mask_lp]

            source_options = ["All"] + sorted(df_lp_f["session_source"].astype(str).unique().tolist())
            selected_source = st.selectbox("Filter by source", source_options, key="lp_source")
            if selected_source != "All":
                df_lp_f = df_lp_f[df_lp_f["session_source"] == selected_source]

            lp = (
                df_lp_f.groupby(["landing_page", "page_category"], observed=True)
                .agg(
                    sessions=("sessions", "sum"),
                    engaged_sessions=("engaged_sessions", "sum"),
                    engagement_duration_s=("engagement_duration_s", "sum"),
                    new_users=("new_users", "sum"),
                    conversions=("conversions", "sum"),
                )
                .reset_index()
            )
            lp = _derive_metrics(lp[lp["sessions"] > 0])
            lp = lp.sort_values("sessions", ascending=False).head(30)

            col1, col2 = st.columns(2)
            with col1:
                top_sessions = lp.head(15).sort_values("sessions", ascending=True)
                fig = px.bar(
                    top_sessions, x="sessions", y="landing_page", orientation="h",
                    labels={"sessions": "Sessions", "landing_page": "Page"},
                    title="Top 15 Entry Pages by Sessions",
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                top_new = lp.sort_values("new_users", ascending=False).head(15).sort_values("new_users", ascending=True)
                fig2 = px.bar(
                    top_new, x="new_users", y="landing_page", orientation="h",
                    labels={"new_users": "New Users", "landing_page": "Page"},
                    title="Top 15 Entry Pages by New Users",
                )
                st.plotly_chart(fig2, use_container_width=True)

            if lp["conversions"].sum() > 0:
                top_conv = lp.sort_values("conversions", ascending=False).head(15).sort_values("conversions", ascending=True)
                fig3 = px.bar(
                    top_conv, x="conversions", y="landing_page", orientation="h",
                    labels={"conversions": "Conversions", "landing_page": "Page"},
                    title="Top 15 Entry Pages by Conversions",
                )
                st.plotly_chart(fig3, use_container_width=True)

            display_lp = lp[[
                "landing_page", "page_category", "sessions", "new_users",
                "engagement_rate", "avg_engagement_s", "conversions",
            ]].rename(columns={
                "landing_page": "Landing Page", "page_category": "Category",
                "sessions": "Sessions", "new_users": "New Users",
                "engagement_rate": "Eng. Rate", "avg_engagement_s": "Avg Time",
                "conversions": "Conversions",
            }).copy()
            display_lp["Eng. Rate"] = display_lp["Eng. Rate"].map(_pct)
            display_lp["Avg Time"] = display_lp["Avg Time"].apply(_fmt_duration)
            st.dataframe(display_lp, use_container_width=True, hide_index=True)


