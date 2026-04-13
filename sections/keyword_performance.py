"""Keyword Performance — SEMrush Position Tracking data.

Expected input: SEMrush Position Tracking "Rankings Overview" CSV export.

The CSV has a 5-line metadata header, then columns:
    Keyword, Tags, Intents,
    <domain>_<YYYYMMDD>,  <domain>_<YYYYMMDD>_type,  <domain>_<YYYYMMDD>_landing,
    ... (repeated per day),
    <domain>_difference, Search Volume, CPC, Keyword Difficulty
"""

import re
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import categorize_page
from llm import render_llm_insights

# Maps raw intent codes to human-readable labels.
INTENT_LABELS = {
    "i": "Informational",
    "c": "Commercial",
    "n": "Navigational",
    "t": "Transactional",
}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _skip_metadata(uploaded_file) -> int:
    """Return the number of header rows to skip before the real CSV header."""
    uploaded_file.seek(0)
    lines = uploaded_file.read().decode("utf-8", errors="replace").splitlines()
    uploaded_file.seek(0)
    for i, line in enumerate(lines):
        if line.startswith("Keyword,"):
            return i
    return 0


def _parse_dates_from_columns(columns: list[str]) -> list[str]:
    """Extract sorted unique YYYYMMDD date strings from column names."""
    dates = set()
    for col in columns:
        m = re.search(r"_(\d{8})$", col)
        if m:
            dates.add(m.group(1))
    return sorted(dates)


def _parse_position_tracking_csv(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the SEMrush Position Tracking CSV.

    Returns:
        keywords_df: One row per keyword with static metadata.
        daily_df: Long-format dataframe with one row per keyword × date,
                  columns: keyword, date, rank, result_type, landing_page.
    """
    skip = _skip_metadata(uploaded_file)
    df = pd.read_csv(uploaded_file, skiprows=skip)
    df.columns = [c.strip() for c in df.columns]

    # --- Extract dates from column names ---
    dates = _parse_dates_from_columns(df.columns)

    # --- Find the domain prefix (everything before _YYYYMMDD) ---
    sample_col = [c for c in df.columns if re.search(r"_\d{8}$", c)]
    if not sample_col:
        st.error("Could not find date-stamped position columns in the CSV.")
        return pd.DataFrame(), pd.DataFrame()
    domain_prefix = re.sub(r"_\d{8}$", "", sample_col[0])

    # --- Build long-format daily data ---
    daily_rows = []
    for _, row in df.iterrows():
        kw = row["Keyword"]
        for d in dates:
            rank_col = f"{domain_prefix}_{d}"
            type_col = f"{domain_prefix}_{d}_type"
            land_col = f"{domain_prefix}_{d}_landing"

            rank_raw = row.get(rank_col, "-")
            rank = pd.to_numeric(rank_raw, errors="coerce")

            result_type = str(row.get(type_col, "")).strip()
            landing = str(row.get(land_col, "")).strip()

            daily_rows.append({
                "keyword": kw,
                "date": pd.to_datetime(d, format="%Y%m%d"),
                "rank": rank,
                "result_type": result_type if pd.notna(rank) else "",
                "landing_page": landing if pd.notna(rank) else "",
            })

    daily_df = pd.DataFrame(daily_rows)

    # --- Build keyword metadata table ---
    diff_col = f"{domain_prefix}_difference"
    keywords_df = df[["Keyword"]].copy()
    keywords_df = keywords_df.rename(columns={"Keyword": "keyword"})

    if "Tags" in df.columns:
        keywords_df["tags"] = df["Tags"].fillna("")
    if "Intents" in df.columns:
        keywords_df["intents"] = df["Intents"].fillna("")
    if "Search Volume" in df.columns:
        keywords_df["search_volume"] = pd.to_numeric(df["Search Volume"], errors="coerce")
    if "CPC" in df.columns:
        keywords_df["cpc"] = pd.to_numeric(df["CPC"], errors="coerce")
    if "Keyword Difficulty" in df.columns:
        keywords_df["difficulty"] = pd.to_numeric(df["Keyword Difficulty"], errors="coerce")
    if diff_col in df.columns:
        keywords_df["wow_change"] = pd.to_numeric(df[diff_col], errors="coerce")

    return keywords_df, daily_df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _latest_rank(daily_df: pd.DataFrame) -> pd.DataFrame:
    """For each keyword return the most recent day's rank, type, and landing page."""
    latest_date = daily_df["date"].max()
    return daily_df[daily_df["date"] == latest_date].copy()


def _page_path_from_url(url: str) -> str:
    """Extract the path from a full URL for page categorization."""
    if not isinstance(url, str) or not url.startswith("http"):
        return ""
    try:
        return urlparse(url).path
    except Exception:
        return ""


def _expand_intents(intent_str: str) -> list[str]:
    """Split '|'-separated intent codes into labels."""
    if not isinstance(intent_str, str) or not intent_str.strip():
        return []
    return [INTENT_LABELS.get(code.strip(), code.strip()) for code in intent_str.split("|")]


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render():
    st.header("Keyword Performance")

    uploaded = st.file_uploader(
        "Upload SEMrush Position Tracking CSV",
        type=["csv"],
        key="keyword_upload",
        help="SEMrush → Position Tracking → Rankings Overview → Export CSV",
    )

    if uploaded is None:
        st.info("Upload a SEMrush Position Tracking CSV to see analysis.")
        return

    keywords_df, daily_df = _parse_position_tracking_csv(uploaded)
    if daily_df.empty:
        return

    latest = _latest_rank(daily_df)
    dates = sorted(daily_df["date"].unique())
    n_days = len(dates)

    # ---------------------------------------------------------------
    # Overview metrics
    # ---------------------------------------------------------------
    st.subheader("Overview")

    total_keywords = keywords_df.shape[0]
    ranked_latest = latest[latest["rank"].notna()]
    n_ranked = len(ranked_latest)
    n_top10 = (ranked_latest["rank"] <= 10).sum()
    n_top3 = (ranked_latest["rank"] <= 3).sum()
    n_unranked = total_keywords - n_ranked

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Tracked", total_keywords)
    m2.metric("Ranked", n_ranked)
    m3.metric("Top 3", int(n_top3))
    m4.metric("Top 10", int(n_top10))
    m5.metric("Unranked", n_unranked)

    # ---------------------------------------------------------------
    # Latest rankings snapshot
    # ---------------------------------------------------------------
    st.subheader("Latest Rankings")

    snapshot = latest.merge(keywords_df, on="keyword", how="left")
    snapshot = snapshot[snapshot["rank"].notna()].copy()
    snapshot["landing_category"] = snapshot["landing_page"].apply(
        lambda u: categorize_page(_page_path_from_url(u))
    )

    display_cols = {
        "keyword": "Keyword",
        "rank": "Rank",
        "result_type": "SERP Type",
        "landing_category": "Page Category",
        "wow_change": "WoW Change",
        "search_volume": "Volume",
        "difficulty": "Difficulty",
    }
    snapshot_display = (
        snapshot[list(display_cols.keys())]
        .rename(columns=display_cols)
        .sort_values("Rank")
    )

    st.dataframe(
        snapshot_display,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Rank": st.column_config.NumberColumn(format="%d"),
            "WoW Change": st.column_config.NumberColumn(
                help="Position change vs prior week (negative = improved)",
            ),
        },
    )

    # ---------------------------------------------------------------
    # AI Overview vs Organic breakdown
    # ---------------------------------------------------------------
    st.subheader("SERP Feature Breakdown")

    type_counts = (
        latest[latest["rank"].notna()]
        .groupby("result_type")
        .size()
        .reset_index(name="keywords")
        .sort_values("keywords", ascending=False)
    )

    col_pie, col_bar = st.columns(2)

    with col_pie:
        fig_pie = px.pie(
            type_counts,
            values="keywords",
            names="result_type",
            title="Rankings by SERP Feature",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_bar:
        # SERP type by rank bucket
        ranked_all = latest[latest["rank"].notna()].copy()
        ranked_all["rank_bucket"] = pd.cut(
            ranked_all["rank"],
            bins=[0, 3, 10, 20, 50, 200],
            labels=["#1-3", "#4-10", "#11-20", "#21-50", "#50+"],
        )
        bucket_type = (
            ranked_all.groupby(["rank_bucket", "result_type"], observed=True)
            .size()
            .reset_index(name="count")
        )
        fig_bucket = px.bar(
            bucket_type,
            x="rank_bucket",
            y="count",
            color="result_type",
            title="SERP Feature by Rank Bucket",
            barmode="stack",
            labels={"rank_bucket": "Position Range", "count": "Keywords"},
        )
        st.plotly_chart(fig_bucket, use_container_width=True)

    # ---------------------------------------------------------------
    # Daily rank trends
    # ---------------------------------------------------------------
    st.subheader("Daily Rank Trends")

    # Let user select keywords to chart
    ranked_keywords = sorted(
        ranked_latest["keyword"].tolist(),
        key=lambda k: ranked_latest.loc[ranked_latest["keyword"] == k, "rank"].iloc[0],
    )
    default_kws = ranked_keywords[:8]

    selected_kws = st.multiselect(
        "Select keywords to chart",
        ranked_keywords,
        default=default_kws,
        key="kw_trend_select",
    )

    if selected_kws:
        trend_df = daily_df[daily_df["keyword"].isin(selected_kws)].copy()
        trend_df = trend_df[trend_df["rank"].notna()]

        fig_trend = px.line(
            trend_df,
            x="date",
            y="rank",
            color="keyword",
            title="Position Over Time (lower is better)",
            markers=True,
        )
        fig_trend.update_yaxes(autorange="reversed")
        fig_trend.update_layout(xaxis_tickformat="%b %d")
        st.plotly_chart(fig_trend, use_container_width=True)

    # ---------------------------------------------------------------
    # Landing page performance
    # ---------------------------------------------------------------
    st.subheader("Landing Page Performance")

    page_perf = latest[latest["rank"].notna()].copy()
    page_perf["page_category"] = page_perf["landing_page"].apply(
        lambda u: categorize_page(_page_path_from_url(u))
    )

    cat_agg = (
        page_perf.groupby("page_category")
        .agg(
            keywords=("keyword", "count"),
            avg_rank=("rank", "mean"),
            best_rank=("rank", "min"),
        )
        .reset_index()
        .sort_values("keywords", ascending=False)
    )
    cat_agg["avg_rank"] = cat_agg["avg_rank"].round(1)

    col_chart, col_table = st.columns([2, 1])

    with col_chart:
        fig_cat = px.bar(
            cat_agg,
            x="page_category",
            y="keywords",
            text="keywords",
            title="Keywords Ranking by Page Category",
            color="page_category",
        )
        fig_cat.update_traces(textposition="outside")
        fig_cat.update_layout(showlegend=False)
        st.plotly_chart(fig_cat, use_container_width=True)

    with col_table:
        st.dataframe(
            cat_agg.rename(columns={
                "page_category": "Page Category",
                "keywords": "Keywords",
                "avg_rank": "Avg Rank",
                "best_rank": "Best Rank",
            }),
            hide_index=True,
            use_container_width=True,
        )

    # ---------------------------------------------------------------
    # Search intent analysis
    # ---------------------------------------------------------------
    st.subheader("Performance by Search Intent")

    intent_rows = []
    for _, row in keywords_df.iterrows():
        labels = _expand_intents(row.get("intents", ""))
        for label in labels:
            intent_rows.append({"keyword": row["keyword"], "intent": label})

    if intent_rows:
        intent_df = pd.DataFrame(intent_rows).merge(
            latest[["keyword", "rank"]], on="keyword", how="left"
        )

        intent_summary = (
            intent_df.groupby("intent")
            .agg(
                total=("keyword", "count"),
                ranked=("rank", lambda x: x.notna().sum()),
                avg_rank=("rank", "mean"),
            )
            .reset_index()
        )
        intent_summary["avg_rank"] = intent_summary["avg_rank"].round(1)
        intent_summary["rank_rate"] = (
            (intent_summary["ranked"] / intent_summary["total"] * 100).round(0).astype(int).astype(str) + "%"
        )
        intent_summary = intent_summary.sort_values("total", ascending=False)

        fig_intent = px.bar(
            intent_summary,
            x="intent",
            y=["ranked", "total"],
            title="Ranked vs Total Keywords by Intent",
            barmode="group",
            labels={"value": "Keywords", "intent": "Intent"},
        )
        st.plotly_chart(fig_intent, use_container_width=True)

        st.dataframe(
            intent_summary.rename(columns={
                "intent": "Intent",
                "total": "Total",
                "ranked": "Ranked",
                "avg_rank": "Avg Rank",
                "rank_rate": "Rank Rate",
            }),
            hide_index=True,
            use_container_width=True,
        )

    # ---------------------------------------------------------------
    # Rank stability
    # ---------------------------------------------------------------
    if n_days >= 3:
        st.subheader("Rank Stability")
        st.caption("Keywords with the most position volatility during the week.")

        stability = (
            daily_df[daily_df["rank"].notna()]
            .groupby("keyword")
            .agg(
                min_rank=("rank", "min"),
                max_rank=("rank", "max"),
                std=("rank", "std"),
                days_ranked=("rank", "count"),
            )
            .reset_index()
        )
        stability["swing"] = stability["max_rank"] - stability["min_rank"]
        stability = stability[stability["days_ranked"] >= 3]
        stability = stability.sort_values("swing", ascending=False)

        volatile = stability[stability["swing"] > 0].head(15)

        if not volatile.empty:
            fig_vol = px.bar(
                volatile,
                x="swing",
                y="keyword",
                orientation="h",
                title="Biggest Position Swings This Week",
                labels={"swing": "Position Swing (max − min)", "keyword": ""},
                text="swing",
            )
            fig_vol.update_layout(yaxis={"categoryorder": "total ascending"})
            fig_vol.update_traces(textposition="outside")
            st.plotly_chart(fig_vol, use_container_width=True)
        else:
            st.success("All positions were stable this week.")

    # ---------------------------------------------------------------
    # Keyword difficulty vs rank
    # ---------------------------------------------------------------
    st.subheader("Keyword Difficulty vs Rank")

    diff_rank = snapshot[["keyword", "rank", "difficulty", "search_volume"]].dropna(
        subset=["rank", "difficulty"]
    )

    if not diff_rank.empty:
        diff_rank["volume_label"] = diff_rank["search_volume"].apply(
            lambda v: f"{int(v):,}" if pd.notna(v) else "n/a"
        )
        diff_rank["search_volume"] = diff_rank["search_volume"].fillna(0)

        fig_scatter = px.scatter(
            diff_rank,
            x="difficulty",
            y="rank",
            text="keyword",
            size="search_volume",
            size_max=30,
            title="Difficulty vs Position (bubble size = search volume)",
            labels={"difficulty": "Keyword Difficulty", "rank": "Position"},
            hover_data={"volume_label": True, "keyword": False},
        )
        fig_scatter.update_traces(textposition="top center", textfont_size=8)
        fig_scatter.update_yaxes(autorange="reversed")
        st.plotly_chart(fig_scatter, use_container_width=True)

    # ---------------------------------------------------------------
    # LLM Insights
    # ---------------------------------------------------------------
    type_summary = type_counts.to_string(index=False) if not type_counts.empty else "N/A"

    improved = keywords_df[keywords_df["wow_change"].notna() & (keywords_df["wow_change"] < 0)]
    declined = keywords_df[keywords_df["wow_change"].notna() & (keywords_df["wow_change"] > 0)]

    top5 = snapshot.nsmallest(5, "rank")
    top5_str = ", ".join(
        f"{r['keyword']}(#{int(r['rank'])}, {r['result_type']})"
        for _, r in top5.iterrows()
    )

    data_summary = f"""Tracking {total_keywords} keywords, {n_ranked} ranked, {n_unranked} unranked.
Top 3: {int(n_top3)}, Top 10: {int(n_top10)}
SERP features: {type_summary}
Top 5 keywords: {top5_str}
Improved WoW: {len(improved)}, Declined WoW: {len(declined)}
Date range: {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')} ({n_days} days)"""

    render_llm_insights("Keyword Performance", data_summary)
