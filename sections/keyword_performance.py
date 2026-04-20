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
from db import (
    query_df,
    upsert_keywords,
    update_keyword_products,
    sync_gsc_keyword_rankings,
    replace_keyword_tiers,
    has_keyword_tiers,
)
from llm import render_chart_insight

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
                "source": "semrush",
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


def _parse_tier_sheet(uploaded_file) -> list[dict] | None:
    """Parse the Keyword Performance sheet → list of {keyword, tier} dicts.

    Only the first two columns are used (Primary Keywords, Primary/Secondary).
    All other columns (historical ranks etc) are ignored.
    """
    uploaded_file.seek(0)
    df = pd.read_csv(uploaded_file)
    df.columns = [c.strip() for c in df.columns]

    kw_col = next(
        (c for c in ["Primary Keywords", "Keywords", "Keyword"] if c in df.columns),
        None,
    )
    if kw_col is None or "Primary/Secondary" not in df.columns:
        st.error(
            "Expected columns 'Primary Keywords' and 'Primary/Secondary' in the sheet."
        )
        return None

    rows = []
    for _, row in df.iterrows():
        kw = str(row[kw_col]).strip().lower()
        tier = str(row["Primary/Secondary"]).strip().lower()
        if not kw or tier not in ("primary", "secondary"):
            continue
        rows.append({"keyword": kw, "tier": tier})
    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _latest_rank(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate metrics over the latest Sun–Sat week per (keyword, source).

    Rank = mean across the week. Clicks / Impressions = sum. CTR recomputed.
    WoW and tier are invariant per (keyword, source) and passed through.
    Landing page and SERP type are taken from the most recent date in the week
    (they're categorical, so averaging doesn't apply).
    """
    if daily_df.empty:
        return daily_df.copy()
    df = daily_df.copy()
    df["week"] = df["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
    latest_week_per_source = df.groupby("source", observed=True)["week"].transform("max")
    df = df[df["week"] == latest_week_per_source]

    # Numeric aggregation
    numeric = (
        df.groupby(["keyword", "source"], as_index=False, observed=True)
        .agg(
            rank=("rank", "mean"),
            best_rank=("rank", "min"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
            wow_change=("wow_change", "max"),
            tier=("tier", "first"),
        )
    )
    numeric["ctr"] = (
        numeric["clicks"] / numeric["impressions"].where(numeric["impressions"] > 0)
    ) * 100

    # Most recent day's categorical fields per (keyword, source)
    latest_row = (
        df.sort_values("date")
        .groupby(["keyword", "source"], as_index=False, observed=True)
        .agg(
            date=("date", "last"),
            landing_page=("landing_page", "last"),
            result_type=("result_type", "last"),
        )
    )
    return latest_row.merge(numeric, on=["keyword", "source"])


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

@st.cache_data(ttl=300, max_entries=1)
def _load_keyword_data() -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Load keyword data from database.

    Tier is computed at query time: LEFT JOIN keyword_tiers, default 'tertiary'.
    """
    daily_df = query_df("""
        SELECT kr.keyword, kr.date, kr.rank, kr.source, kr.result_type, kr.landing_page,
               kr.clicks, kr.impressions,
               COALESCE(kt.tier, 'tertiary') AS tier
        FROM keyword_rankings kr
        LEFT JOIN keyword_tiers kt
          ON LOWER(TRIM(kt.keyword)) = LOWER(TRIM(kr.keyword))
        ORDER BY kr.date
    """)
    if daily_df.empty:
        return None
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df["rank"] = pd.to_numeric(daily_df["rank"], errors="coerce")
    daily_df["clicks"] = pd.to_numeric(daily_df["clicks"], errors="coerce")
    daily_df["impressions"] = pd.to_numeric(daily_df["impressions"], errors="coerce")
    daily_df["ctr"] = (daily_df["clicks"] / daily_df["impressions"].where(daily_df["impressions"] > 0)) * 100
    daily_df["source"] = daily_df["source"].astype("category")
    daily_df["tier"] = daily_df["tier"].astype("category")

    keywords_df = query_df("""
        SELECT DISTINCT ON (kr.keyword)
            kr.keyword, kr.search_volume, kr.cpc, kr.difficulty,
            kr.tags, kr.intents, kr.product,
            COALESCE(kt.tier, 'tertiary') AS tier
        FROM keyword_rankings kr
        LEFT JOIN keyword_tiers kt
          ON LOWER(TRIM(kt.keyword)) = LOWER(TRIM(kr.keyword))
        ORDER BY kr.keyword, kr.date DESC
    """)

    # Compute wow_change per (keyword, source) as the difference between
    # each keyword's *average rank* in the latest Sun–Sat week and its
    # average in the previous Sun–Sat week. Smooths over GSC's daily
    # impression-weighted averages and tolerates days with missing data.
    wow_parts = []
    for src, src_df in daily_df.groupby("source"):
        src_df = src_df.copy()
        src_df["week"] = (
            src_df["date"].dt.to_period("W-SAT").apply(lambda r: r.start_time)
        )
        weeks = sorted(src_df["week"].unique())
        if len(weeks) < 2:
            continue
        latest_week, prev_week = weeks[-1], weeks[-2]

        latest_avg = (
            src_df[src_df["week"] == latest_week]
            .groupby("keyword")["rank"].mean()
            .reset_index()
            .rename(columns={"rank": "rank_latest"})
        )
        prev_avg = (
            src_df[src_df["week"] == prev_week]
            .groupby("keyword")["rank"].mean()
            .reset_index()
            .rename(columns={"rank": "rank_prev"})
        )
        wow = latest_avg.merge(prev_avg, on="keyword", how="inner")
        wow["wow_change"] = wow["rank_latest"] - wow["rank_prev"]
        wow["source"] = src
        wow_parts.append(wow[["keyword", "source", "wow_change"]])

    if wow_parts:
        wow_all = pd.concat(wow_parts, ignore_index=True)
        daily_df = daily_df.merge(wow_all, on=["keyword", "source"], how="left")
    else:
        daily_df["wow_change"] = np.nan

    return keywords_df, daily_df


def _insert_keyword_upload(uploaded_file):
    """Parse a SEMrush CSV and insert every keyword into the database.

    Unranked keywords are kept — they're valid tracked keywords (tertiary
    by default, unless the performance sheet tiers them primary/secondary).
    """
    keywords_df, daily_df = _parse_position_tracking_csv(uploaded_file)
    if daily_df.empty:
        return 0

    # Merge metadata into daily rows for upsert
    merged = daily_df.merge(
        keywords_df[["keyword", "search_volume", "cpc", "difficulty", "tags", "intents"]].drop_duplicates("keyword"),
        on="keyword",
        how="left",
    )
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    if "source" not in merged.columns:
        merged["source"] = "semrush"
    merged = merged.fillna({"tags": "", "intents": "", "result_type": "", "landing_page": "", "source": "semrush"})

    # Convert to records and replace NaN with None (psycopg needs None, not nan)
    rows = merged.to_dict("records")
    for row in rows:
        for key in ("rank", "search_volume", "cpc", "difficulty"):
            v = row.get(key)
            if v is not None and (isinstance(v, float) and np.isnan(v)):
                row[key] = None

    upsert_keywords(rows)
    sync_gsc_keyword_rankings()
    return len(rows)


def render():
    st.header("Keyword Performance")

    # --- Tier sheet uploader (source of truth for primary/secondary) ---
    tiers_loaded = has_keyword_tiers()
    tier_file = st.file_uploader(
        "Upload Keyword Performance Sheet (Primary/Secondary classification)",
        type=["csv"],
        key="tier_sheet_upload",
        help=(
            "Required once. Re-upload any time to refresh tier assignments "
            "— old tiers are wiped and replaced."
        ),
    )
    if tier_file is not None:
        rows = _parse_tier_sheet(tier_file)
        if rows is not None:
            replace_keyword_tiers(rows)
            _load_keyword_data.clear()
            st.success(f"Saved {len(rows):,} keyword tiers.")
            tiers_loaded = True

    if not tiers_loaded:
        st.warning(
            "Upload the Keyword Performance Sheet above before uploading SEMrush data. "
            "Tiers are required to classify keywords."
        )
        return

    # --- SEMrush CSV uploader ---
    uploaded = st.file_uploader(
        "Upload SEMrush Position Tracking CSV",
        type=["csv"],
        key="keyword_upload",
        help="SEMrush → Position Tracking → Rankings Overview → Export CSV",
    )

    if uploaded is not None:
        count = _insert_keyword_upload(uploaded)
        if count:
            st.success(f"Inserted {count:,} keyword ranking rows.")
            _load_keyword_data.clear()

    result = _load_keyword_data()
    if result is None:
        if uploaded is None:
            st.info("Upload a SEMrush Position Tracking CSV to see analysis.")
        return

    keywords_df, daily_df = result

    if daily_df.empty:
        return

    # --- Filters ---
    col_date, col_tier, col_source, col_product = st.columns(4)

    with col_date:
        all_dates = sorted(daily_df["date"].dt.date.unique())
        date_range = st.date_input(
            "Date range",
            value=(min(all_dates), max(all_dates)),
            min_value=min(all_dates),
            max_value=max(all_dates),
            key="kw_dates",
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            daily_df = daily_df[
                (daily_df["date"].dt.date >= date_range[0]) & (daily_df["date"].dt.date <= date_range[1])
            ]

    with col_tier:
        tier_options = ["primary", "secondary", "tertiary"]
        selected_tiers = st.multiselect(
            "Tier", tier_options, default=tier_options, key="kw_tier"
        )

    with col_source:
        sources = sorted(daily_df["source"].unique())
        default_source = "semrush" if "semrush" in sources else sources[0]
        selected_source = st.selectbox(
            "Source",
            sources,
            index=sources.index(default_source),
            key="kw_source",
            help="SEMrush and GSC are analyzed independently.",
        )

    with col_product:
        products = sorted(keywords_df[keywords_df["product"] != ""]["product"].unique())
        product_options = ["All"] + products
        selected_product = st.selectbox("Product", product_options, key="kw_product")

    if selected_tiers:
        daily_df = daily_df[daily_df["tier"].isin(selected_tiers)]
        keywords_df = keywords_df[keywords_df["tier"].isin(selected_tiers)]
    else:
        daily_df = daily_df.iloc[0:0]
        keywords_df = keywords_df.iloc[0:0]

    daily_df = daily_df[daily_df["source"] == selected_source]

    # For GSC, restrict to the latest complete Sun–Sat week and show it in the UI.
    if selected_source == "gsc" and not daily_df.empty:
        try:
            from google_api import latest_complete_week
            sun, sat = latest_complete_week()
            daily_df = daily_df[
                (daily_df["date"].dt.date >= sun)
                & (daily_df["date"].dt.date <= sat)
            ]
            st.caption(
                f"GSC analysis window: **{sun.strftime('%b %d')} – {sat.strftime('%b %d, %Y')}** "
                f"(latest complete Sun–Sat week, respecting GSC's ~2-day lag)."
            )
        except Exception:
            pass

    if selected_product != "All":
        product_kws = keywords_df[keywords_df["product"] == selected_product]["keyword"].tolist()
        daily_df = daily_df[daily_df["keyword"].isin(product_kws)]
        keywords_df = keywords_df[keywords_df["product"] == selected_product]

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

    snapshot["tier"] = snapshot["keyword"].map(
        keywords_df.set_index("keyword")["tier"].to_dict()
    ).fillna("tertiary")

    # Base columns shown for every source
    display_cols = {
        "keyword": "Keyword",
        "tier": "Tier",
        "product": "Product",
        "source": "Source",
        "best_rank": "Best Rank",
        "rank": "Avg Rank",
        "wow_change": "Avg WoW Change",
    }
    # Source-specific columns: GSC rows have traffic data; SEMrush rows have market data + SERP features.
    if selected_source == "gsc":
        display_cols["impressions"] = "Impressions"
        display_cols["clicks"] = "Clicks"
        display_cols["ctr"] = "CTR"
    else:  # semrush
        display_cols["result_type"] = "SERP Type"
        display_cols["search_volume"] = "Volume"
        display_cols["difficulty"] = "Difficulty"

    snapshot_display = (
        snapshot[list(display_cols.keys())]
        .rename(columns=display_cols)
        .sort_values("Avg Rank")
    )

    st.dataframe(
        snapshot_display,
        hide_index=True,
        width="stretch",
        column_config={
            "Best Rank": st.column_config.NumberColumn(
                format="%.2f",
                help="Best (lowest) rank observed during the latest Sun–Sat week",
            ),
            "Avg Rank": st.column_config.NumberColumn(
                format="%.2f",
                help="Average rank across the latest Sun–Sat week",
            ),
            "Avg WoW Change": st.column_config.NumberColumn(
                help="Avg rank this Sun–Sat week − avg rank previous Sun–Sat week (negative = improved)",
            ),
            "Impressions": st.column_config.NumberColumn(
                format="%d",
                help="GSC impressions summed across the latest Sun–Sat week",
            ),
            "Clicks": st.column_config.NumberColumn(
                format="%d",
                help="GSC clicks summed across the latest Sun–Sat week",
            ),
            "CTR": st.column_config.NumberColumn(
                format="%.2f%%",
                help="Weekly CTR = sum(clicks) ÷ sum(impressions)",
            ),
        },
    )

    # Keep type_counts for LLM summary
    type_counts = (
        latest[latest["rank"].notna()]
        .groupby("result_type")
        .size()
        .reset_index(name="keywords")
        .sort_values("keywords", ascending=False)
    )

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
            title=f"Position Over Time ({selected_source.upper()}) — lower is better",
            markers=True,
        )
        fig_trend.update_yaxes(autorange="reversed")
        unique_dates = sorted(trend_df["date"].unique())
        fig_trend.update_xaxes(
            tickmode="array",
            tickvals=unique_dates,
            tickformat="%b %d",
        )
        st.plotly_chart(fig_trend, width="stretch")

        kw_trend_text = "\n".join(
            f"  {kw}: rank {int(trend_df[trend_df['keyword']==kw]['rank'].iloc[-1])}"
            for kw in selected_kws
            if not trend_df[trend_df['keyword']==kw].empty
        )
        render_chart_insight("kw_trends", kw_trend_text, "Which keywords improved or declined and what does it suggest?")

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
            st.plotly_chart(fig_vol, width="stretch")
        else:
            st.success("All positions were stable this week.")

    # ---------------------------------------------------------------
    # Product tagging
    # ---------------------------------------------------------------
    st.subheader("Tag Keywords by Product")
    st.caption("Assign keywords to Maxim or Bifrost to enable product-level filtering.")

    all_keywords = sorted(keywords_df["keyword"].unique())
    product_map = keywords_df.set_index("keyword")["product"].to_dict()

    # Build editable dataframe
    tag_df = pd.DataFrame({
        "Keyword": all_keywords,
        "Product": [product_map.get(kw, "") for kw in all_keywords],
    })

    with st.form("product_tag_form", clear_on_submit=False, border=False):
        edited = st.data_editor(
            tag_df,
            column_config={
                "Product": st.column_config.SelectboxColumn(
                    options=["", "Maxim", "Bifrost"],
                    required=False,
                ),
            },
            hide_index=True,
            width="stretch",
            key="product_tagger",
        )
        submitted = st.form_submit_button("Save product tags")

    if submitted:
        changes = {}
        for _, row in edited.iterrows():
            kw = row["Keyword"]
            new_product = row["Product"] or ""
            if new_product != product_map.get(kw, ""):
                changes[kw] = new_product
        if changes:
            update_keyword_products(changes)
            st.success(f"Updated {len(changes)} keyword(s).")
            st.cache_data.clear()
            st.rerun()
        else:
            st.info("No changes to save.")

