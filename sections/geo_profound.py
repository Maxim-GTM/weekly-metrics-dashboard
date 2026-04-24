"""GEO Performance — Profound data analysis."""

import json
import re
from urllib.parse import urlparse

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import (
    COMPETITOR_DOMAINS,
    COMPETITOR_NAMES,
    OWNED_DOMAINS,
    TOPIC_COMPETITORS,
    categorize_page,
    categorize_page_deep,
)
from db import query_df, upsert_profound
from llm import render_chart_insight

PLATFORMS = ["ChatGPT", "Google AI Overviews", "Perplexity"]


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300, max_entries=1)
def _load_profound_data_from_db() -> pd.DataFrame | None:
    """Load all Profound data from database."""
    df = query_df("""
        SELECT date, topic, prompt, platform, position, mentioned, mentions,
               citations, run_id, platform_id, tags, region,
               persona, type, search_queries, normalized_mentions
        FROM profound ORDER BY date
    """)
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["platform"] = df["platform"].astype("category")
    df["topic"] = df["topic"].astype("category")
    return df


def _parse_profound_csv_for_db(uploaded_file) -> list[dict]:
    """Parse a Profound CSV and return rows ready for DB upsert."""
    df = pd.read_csv(uploaded_file)
    cit_cols = [c for c in df.columns if re.match(r"citation_\d+", c)]

    rows = []
    for _, row in df.iterrows():
        citations = []
        for c in cit_cols:
            v = row.get(c)
            if isinstance(v, str) and v.startswith("http"):
                citations.append(v)

        mentioned_raw = str(row.get("mentioned?", "")).lower()
        mentioned = mentioned_raw in ("yes", "true", "1")

        rows.append({
            "date": str(row["date"]),
            "topic": str(row.get("topic", "")),
            "prompt": str(row.get("prompt", "")),
            "platform": str(row.get("platform", "")),
            "position": str(row.get("position", "")),
            "mentioned": mentioned,
            "mentions": str(row.get("mentions", "") if pd.notna(row.get("mentions")) else ""),
            "normalized_mentions": str(row.get("normalized_mentions", "") if pd.notna(row.get("normalized_mentions")) else ""),
            "citations": json.dumps(citations),
            "response": str(row.get("response", "") if pd.notna(row.get("response")) else ""),
            "run_id": str(row.get("run_id", "") if pd.notna(row.get("run_id")) else ""),
            "platform_id": str(row.get("platformId", "") if pd.notna(row.get("platformId")) else ""),
            "tags": str(row.get("tags", "") if pd.notna(row.get("tags")) else ""),
            "region": str(row.get("region", "") if pd.notna(row.get("region")) else ""),
            "persona": str(row.get("persona", "") if pd.notna(row.get("persona")) else ""),
            "type": str(row.get("type", "") if pd.notna(row.get("type")) else ""),
            "search_queries": str(row.get("search_queries", "") if pd.notna(row.get("search_queries")) else ""),
        })
    return rows


def _is_mentioned(df: pd.DataFrame) -> pd.Series:
    """Boolean series: was our brand mentioned by name?"""
    return df["mentioned"].astype(bool)


def _has_domain(row, domain_set: set) -> bool:
    """Check if any citation URL belongs to one of the given domains."""
    citations = row.get("citations", [])
    if not isinstance(citations, list):
        return False
    for v in citations:
        if not isinstance(v, str) or not v.startswith("http"):
            continue
        try:
            h = urlparse(v).netloc.lower().removeprefix("www.")
            if any(h == d or h.endswith("." + d) for d in domain_set):
                return True
        except Exception:
            pass
    return False


def _extract_owned_urls(df: pd.DataFrame) -> pd.Series:
    """Extract all owned-domain URLs from the citations JSONB column."""
    urls = []
    for citations in df["citations"]:
        if not isinstance(citations, list):
            continue
        for v in citations:
            if not isinstance(v, str) or not v.startswith("http"):
                continue
            try:
                h = urlparse(v).netloc.lower().removeprefix("www.")
                if any(h == d or h.endswith("." + d) for d in OWNED_DOMAINS):
                    parsed = urlparse(v)
                    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
                    urls.append(clean)
            except Exception:
                pass
    return pd.Series(urls) if urls else pd.Series(dtype=str)


def _comp_mentioned(mentions_str: str) -> bool:
    """Check if any competitor name appears in the mentions column."""
    m = str(mentions_str).lower()
    return any(name in m for name in COMPETITOR_NAMES)


def _extract_comp_names(mentions_series: pd.Series) -> pd.Series:
    """From a series of comma-separated mention strings, extract competitor names."""
    names = (
        mentions_series.dropna()
        .str.lower()
        .str.split(",")
        .explode()
        .str.strip()
    )
    return names[names.isin(COMPETITOR_NAMES)]


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def render():
    st.header("GEO Performance — Profound Data")

    uploaded = st.file_uploader(
        "Upload Profound CSV (raw data with citations)",
        type=["csv"],
        key="profound_upload",
    )

    if uploaded is not None:
        rows = _parse_profound_csv_for_db(uploaded)
        upsert_profound(rows)
        _load_profound_data_from_db.clear()
        st.success(f"Inserted {len(rows):,} rows.")

    df = _load_profound_data_from_db()
    if df is None:
        if uploaded is None:
            st.info("Upload a Profound CSV export to see GEO analysis.")
        return

    # Pre-compute derived columns once
    df["is_mentioned"] = _is_mentioned(df)
    df["has_fp_citation"] = df.apply(
        lambda row: _has_domain(row, OWNED_DOMAINS), axis=1
    )
    df["comp_mentioned"] = df["mentions"].apply(_comp_mentioned)

    # --- Filters ---
    _platform_opts = ["All"] + PLATFORMS
    selected_platform = st.sidebar.selectbox(
        "Platform",
        _platform_opts,
        index=_platform_opts.index(st.session_state.get("geo_platform", "All")),
        key="geo_platform",
    )

    col1, col2 = st.columns(2)
    with col1:
        topics = sorted(df["topic"].unique())
        selected_topic = st.selectbox("Topic", ["All"] + topics)
    with col2:
        dates = sorted(df["date"].dt.date.unique())
        date_range = st.date_input(
            "Date range",
            value=(min(dates), max(dates)),
            min_value=min(dates),
            max_value=max(dates),
        )

    # Apply filters
    mask = pd.Series(True, index=df.index)
    if selected_topic != "All":
        mask &= df["topic"] == selected_topic
    if isinstance(date_range, tuple) and len(date_range) == 2:
        mask &= (df["date"].dt.date >= date_range[0]) & (
            df["date"].dt.date <= date_range[1]
        )
    if selected_platform != "All":
        mask &= df["platform"] == selected_platform
    filtered = df[mask]

    if filtered.empty:
        st.warning("No data matches the selected filters.")
        return

    # --- Overview metrics ---
    st.subheader("Overview")
    st.caption("High-level GEO visibility: how many prompts we tracked, how many we appeared in at least once across any platform, and how many of our pages were cited in responses.")

    total_prompts = filtered["prompt"].nunique()
    mentioned = filtered[filtered["is_mentioned"]]
    prompts_appeared = mentioned["prompt"].nunique()

    total_rows = len(filtered)
    mentioned_rows = int(filtered["is_mentioned"].sum())

    owned_urls = _extract_owned_urls(filtered)
    unique_articles_cited = owned_urls.nunique() if not owned_urls.empty else 0

    m1, m2, m3 = st.columns(3)
    m1.metric("Unique Prompts", total_prompts)
    m2.metric(
        "Prompts We Appeared In",
        f"{prompts_appeared}/{total_prompts}",
        help="Prompts where our brand was mentioned at least once, across any platform within the date range.",
    )
    m3.metric("Our Articles Cited", unique_articles_cited)

    # --- Per-prompt mention rate distribution, by platform ---
    st.subheader("Per-Prompt Mention Rate (by Platform)")
    st.caption(
        "For each (prompt × platform), mention rate = rows mentioned ÷ total rows "
        "for that prompt on that platform within the selected date range. "
        "Then aggregate across prompts to see the distribution and threshold buckets."
    )

    per_pp = (
        filtered.groupby(["platform", "prompt"])
        .agg(
            rows=("is_mentioned", "size"),
            mentioned_rows=("is_mentioned", "sum"),
        )
        .reset_index()
    )
    per_pp["rate"] = per_pp["mentioned_rows"] / per_pp["rows"] * 100

    per_platform_rows = []
    threshold_rows = []
    for platform in PLATFORMS:
        pdf = per_pp[per_pp["platform"] == platform]
        if pdf.empty:
            continue
        rates = pdf["rate"]
        per_platform_rows.append({
            "Platform": platform,
            "Prompts": len(pdf),
            "Mean mention rate": f"{rates.mean():.1f}%",
            "≥ 25%": int((rates >= 25).sum()),
            "≥ 50%": int((rates >= 50).sum()),
            "≥ 75%": int((rates >= 75).sum()),
            "= 100%": int((rates >= 100).sum()),
        })
        for label, cutoff in [("≥ 25%", 25), ("≥ 50%", 50), ("≥ 75%", 75), ("= 100%", 100)]:
            threshold_rows.append({
                "Platform": platform,
                "Threshold": label,
                "Prompts": int((rates >= cutoff).sum()),
            })

    if per_platform_rows:
        st.dataframe(pd.DataFrame(per_platform_rows), hide_index=True, width="stretch")
        st.caption(
            "Mean = average per-prompt mention rate on that platform. "
            "Threshold columns = prompts whose mention rate meets that floor. "
            "**These nest — ≥50% is a subset of ≥25%, not additional prompts.** "
            "E.g. if ≥25% = 8 and ≥50% = 6, that means 6 of the same 8 prompts also clear 50%."
        )

        thr_df = pd.DataFrame(threshold_rows)
        fig_thr = px.bar(
            thr_df,
            x="Threshold",
            y="Prompts",
            color="Platform",
            barmode="group",
            text="Prompts",
            title="Prompts reaching each mention-rate threshold, by platform",
            category_orders={"Threshold": ["≥ 25%", "≥ 50%", "≥ 75%", "= 100%"]},
        )
        fig_thr.update_traces(textposition="outside")
        st.plotly_chart(fig_thr, width="stretch")

        # --- Rank buckets ---
        def _rank_bucket(pos_str: str) -> str | None:
            s = str(pos_str).strip().lstrip("#")
            try:
                n = int(s)
                if n == 1:
                    return "Rank 1"
                if n == 2:
                    return "Rank 2"
                if n == 3:
                    return "Rank 3"
                return "Others (4+)"
            except ValueError:
                return None

        rank_data = filtered[filtered["is_mentioned"]].copy()
        rank_data["rank_bucket"] = rank_data["position"].apply(_rank_bucket)
        rank_data = rank_data[rank_data["rank_bucket"].notna()]

        if not rank_data.empty:
            st.subheader("Rank Distribution (When Mentioned)")
            st.caption(
                "How often our brand appears first, second, third, or lower within an AI response — "
                "counted across individual response instances (not unique prompts). "
                "Rank 1 = first entity mentioned in that response."
            )
            rank_by_platform = (
                rank_data.groupby(["platform", "rank_bucket"])
                .size()
                .reset_index(name="Responses")
            )
            fig_rank = px.bar(
                rank_by_platform,
                x="platform",
                y="Responses",
                color="rank_bucket",
                barmode="group",
                text="Responses",
                title="Mention rank distribution by platform (response instances)",
                labels={"platform": "Platform", "rank_bucket": "Rank"},
                category_orders={
                    "rank_bucket": ["Rank 1", "Rank 2", "Rank 3", "Others (4+)"]
                },
            )
            fig_rank.update_traces(textposition="outside")
            st.plotly_chart(fig_rank, width="stretch")

            rank_summary = (
                rank_data.groupby("rank_bucket")
                .size()
                .reindex(["Rank 1", "Rank 2", "Rank 3", "Others (4+)"], fill_value=0)
                .reset_index(name="Responses")
            )
            rank_summary["Share (%)"] = (
                rank_summary["Responses"] / rank_summary["Responses"].sum() * 100
            ).round(1)
            st.dataframe(rank_summary, hide_index=True, width="stretch")

            # --- Per-prompt rank table ---
            st.markdown("**Rank by Prompt**")
            st.caption(
                "How many individual AI response instances placed us at each rank position, per prompt. "
                "E.g. 'Rank 1 = 3' means we were the first entity named in 3 separate responses for that prompt — not that we consistently rank first overall."
            )
            BUCKET_ORDER = ["Rank 1", "Rank 2", "Rank 3", "Others (4+)"]
            rank_scope = rank_data
            prompt_rank = (
                rank_scope.groupby(["prompt", "rank_bucket"])
                .size()
                .unstack(fill_value=0)
                .reindex(columns=BUCKET_ORDER, fill_value=0)
                .reset_index()
            )
            prompt_rank["Total Mentions"] = prompt_rank[BUCKET_ORDER].sum(axis=1)
            prompt_rank["Best Rank"] = (
                rank_scope.groupby("prompt")["position"]
                .apply(lambda s: s.str.lstrip("#").apply(pd.to_numeric, errors="coerce").min())
                .apply(lambda n: f"#{int(n)}" if pd.notna(n) else "")
                .values
            )
            prompt_rank = prompt_rank.sort_values("Rank 1", ascending=False)
            st.dataframe(prompt_rank, hide_index=True, width="stretch")

    # --- Cross-platform overlap ---
    st.subheader("Cross-Platform Overlap")
    st.caption("Of the prompts where we were mentioned, how many appeared simultaneously across ChatGPT, AI Overviews, and Perplexity — a measure of breadth vs platform-specific visibility.")

    prompt_platforms = (
        mentioned.groupby("prompt")["platform"].apply(set).reset_index()
    )
    prompt_platforms["num_platforms"] = prompt_platforms["platform"].apply(len)

    exactly_three = int((prompt_platforms["num_platforms"] == 3).sum())
    exactly_two = int((prompt_platforms["num_platforms"] == 2).sum())
    exactly_one = int((prompt_platforms["num_platforms"] == 1).sum())

    o1, o2, o3 = st.columns(3)
    o1.metric("All 3 Platforms", exactly_three)
    o2.metric("Exactly 2 Platforms", exactly_two)
    o3.metric("Exactly 1 Platform", exactly_one)
    st.caption(
        f"These three buckets are mutually exclusive and sum to {exactly_three + exactly_two + exactly_one} prompts where we were mentioned at least once. "
        "A prompt in 'All 3 Platforms' means our brand appeared on ChatGPT, AI Overviews, and Perplexity for that prompt."
    )

    overlap_data = []
    for p1 in PLATFORMS:
        row = {}
        prompts_p1 = set(mentioned[mentioned["platform"] == p1]["prompt"].unique())
        for p2 in PLATFORMS:
            prompts_p2 = set(mentioned[mentioned["platform"] == p2]["prompt"].unique())
            row[p2] = len(prompts_p1 & prompts_p2)
        overlap_data.append(row)

    overlap_df = pd.DataFrame(overlap_data, index=PLATFORMS)

    # --- Our Articles Cited ---
    st.subheader("Our Articles Cited")
    st.caption(
        "AI responses can reference our content in two ways: by **naming our brand** ('Maxim / Bifrost does X') "
        "and by **linking to our URLs** (a citation). These don't always happen together — an AI can link to our blog post "
        "without saying our name, or mention us by name without citing a specific page. "
        "The two sections below split citations by whether our brand was also named in that same response."
    )

    def _owned_pages_df(scope_df: pd.DataFrame) -> pd.DataFrame:
        urls = _extract_owned_urls(scope_df)
        if urls.empty:
            return pd.DataFrame(columns=["URL", "Times Cited", "Page", "Page Category"])
        uc = urls.value_counts().reset_index()
        uc.columns = ["URL", "Times Cited"]
        uc["Page"] = uc["URL"].apply(lambda u: urlparse(u).path.rstrip("/") or "/")
        uc["Page Category"] = uc["Page"].apply(categorize_page_deep)
        return uc

    def _render_owned_citations(uc: pd.DataFrame, label: str, insight_id: str):
        if uc.empty:
            st.info(f"No owned-domain citations in {label.lower()}.")
            return
        fig = px.bar(
            uc.head(15),
            x="Times Cited",
            y="Page",
            orientation="h",
            title=f"{label} — Top 15 pages",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, width="stretch")

        st.dataframe(
            uc[["Page", "Page Category", "Times Cited"]],
            hide_index=True,
            width="stretch",
        )

        cat = uc.groupby("Page Category").agg(
            unique_pages=("Page", "nunique"),
            total_citations=("Times Cited", "sum"),
        ).reset_index().sort_values("total_citations", ascending=False)
        cat.columns = ["Page Category", "Unique Pages", "Total Citations"]
        st.write(f"**{label} — by page category:**")
        st.dataframe(cat, hide_index=True, width="stretch")

        # Themed analysis scoped to just this bucket.
        pages_text = "\n".join(
            f"  {r['Page']} [{r['Page Category']}]  cited={r['Times Cited']}x"
            for _, r in uc.head(40).iterrows()
        )
        question = (
            f"These are pages from the '{label}' bucket. "
            "Group them into 4–6 concrete THEMES that cut across URL categories "
            "(e.g. 'top-N listicles on AI gateways', 'product comparison pages', "
            "'Bifrost docs sections'). For each theme, list: "
            "(1) representative page slugs, "
            "(2) total citations in the theme. "
            "Stop there — do not recommend actions, do not explain causes, just surface the themes."
        )
        render_chart_insight(insight_id, pages_text, question)

    mentioned_pages = _owned_pages_df(filtered[filtered["is_mentioned"]])
    not_mentioned_pages = _owned_pages_df(filtered[~filtered["is_mentioned"]])

    st.markdown("##### Our URL cited AND our brand named in the same response")
    _render_owned_citations(mentioned_pages, "Cited + Mentioned", "cited_mentioned_themes")

    st.markdown("##### Our URL cited but our brand NOT named (AI used our content silently)")
    _render_owned_citations(not_mentioned_pages, "Cited but Not Mentioned", "cited_not_mentioned_themes")

    # ------------------------------------------------------------------
    # CITATIONS VS MENTIONS — diverging bar, per-prompt gap
    # ------------------------------------------------------------------
    st.subheader("Citations vs Mentions Gap (per prompt)")
    st.caption(
        "Gap = citation rate − mention rate (percentage points). "
        "Positive = cited but not mentioned by name. "
        "Negative = mentioned without a citation (attribution-only)."
    )

    cite_mention = (
        filtered.groupby("prompt")
        .agg(
            mention_rate=("is_mentioned", "mean"),
            cite_rate=("has_fp_citation", "mean"),
            topic=("topic", "first"),
        )
        .reset_index()
    )
    cite_mention["mention_rate"] *= 100
    cite_mention["cite_rate"] *= 100
    cite_mention["gap"] = cite_mention["cite_rate"] - cite_mention["mention_rate"]
    cite_mention["abs_gap"] = cite_mention["gap"].abs()

    top_gap = cite_mention[cite_mention["gap"] > 0].sort_values("gap", ascending=False).head(20)

    if not top_gap.empty:
        top_gap = top_gap.sort_values("gap")
        top_gap["label"] = top_gap["prompt"].str[:60]
        fig_gap = px.bar(
            top_gap,
            x="gap",
            y="label",
            orientation="h",
            title="Top 20 prompts by citation-vs-mention gap (percentage points)",
            color_discrete_sequence=["#14b8a6"],
            labels={"gap": "Citation rate − Mention rate (pp)", "label": ""},
        )
        fig_gap.add_vline(x=0, line_color="#888", line_dash="dash")
        st.plotly_chart(fig_gap, width="stretch")

    # ------------------------------------------------------------------
    # COMPETITOR MENTIONS — replaces old "Most Cited Pages" section
    # ------------------------------------------------------------------
    st.subheader("Competitor Mentions")
    st.caption("How often each competitor brand appears in AI responses across tracked prompts, compared to our own mention rate — including a raw frequency count and per-platform breakdown.")

    comp_names = _extract_comp_names(filtered["mentions"])

    if comp_names.empty:
        st.info("No competitor mentions found in the filtered data.")
    else:
        # --- Mention rate comparison: us vs each competitor (prompt-level) ---
        total_prompts_for_rate = filtered["prompt"].nunique()
        our_rate = prompts_appeared / total_prompts_for_rate * 100 if total_prompts_for_rate else 0

        # For each competitor, count unique prompts where their name appears in any mention row
        comp_rates = []
        mentions_lower = filtered["mentions"].fillna("").str.lower()
        for name in COMPETITOR_NAMES:
            hit_rows = filtered[mentions_lower.str.contains(rf"\b{re.escape(name)}\b", regex=True)]
            if hit_rows.empty:
                continue
            n_prompts = hit_rows["prompt"].nunique()
            comp_rates.append({
                "Entity": name.title(),
                "Mention Rate (%)": n_prompts / total_prompts_for_rate * 100,
                "Kind": "Competitor",
            })

        if comp_rates:
            comp_rates_df = pd.DataFrame(comp_rates).sort_values("Mention Rate (%)", ascending=False).head(10)
            us_row = pd.DataFrame([{"Entity": "Us", "Mention Rate (%)": our_rate, "Kind": "Us"}])
            rate_df = pd.concat([us_row, comp_rates_df], ignore_index=True)
            fig_rate = px.bar(
                rate_df,
                x="Mention Rate (%)",
                y="Entity",
                orientation="h",
                color="Kind",
                color_discrete_map={"Us": "#3b82f6", "Competitor": "#94a3b8"},
                title="Mention Rate — Us vs Top Competitors",
                text="Mention Rate (%)",
            )
            fig_rate.update_traces(texttemplate="%{text:.0f}%", textposition="outside")
            fig_rate.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_rate, width="stretch")

        # --- Raw mention frequency chart (original) ---
        st.caption(
            "**Raw frequency below** — unlike the chart above (which counts unique prompts), "
            "this counts every individual AI response instance where a competitor was named. "
            "A competitor that's tracked on more prompts will naturally have a higher raw count."
        )
        comp_counts = comp_names.value_counts().reset_index()
        comp_counts.columns = ["Competitor", "Mentions"]

        fig_comp = px.bar(
            comp_counts.head(15),
            x="Mentions",
            y="Competitor",
            orientation="h",
            title="Most Mentioned Competitors — raw response count (not % of prompts)",
        )
        fig_comp.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_comp, width="stretch")

        # Per-platform competitor breakdown
        comp_by_platform = []
        for platform in PLATFORMS:
            pdf = filtered[filtered["platform"] == platform]
            pcomp = _extract_comp_names(pdf["mentions"])
            if pcomp.empty:
                continue
            for name, count in pcomp.value_counts().head(5).items():
                total_rows = len(pdf)
                comp_by_platform.append(
                    {
                        "Platform": platform,
                        "Competitor": name,
                        "Mention Count": count,
                        "Rate": f"{count / total_rows * 100:.1f}%",
                    }
                )

        if comp_by_platform:
            st.write("**Top competitors by platform:**")
            st.dataframe(
                pd.DataFrame(comp_by_platform), hide_index=True, width="stretch"
            )

    # Keep gap_prompts for LLM summary
    prompt_stats = (
        filtered.groupby(["topic", "prompt"])
        .agg(
            total=("is_mentioned", "size"),
            mentioned_count=("is_mentioned", "sum"),
            cited_count=("has_fp_citation", "sum"),
        )
        .reset_index()
    )
    prompt_stats["mention_rate"] = prompt_stats["mentioned_count"] / prompt_stats["total"]
    prompt_stats["cite_rate"] = prompt_stats["cited_count"] / prompt_stats["total"]
    prompt_stats["gap"] = prompt_stats["cite_rate"] - prompt_stats["mention_rate"]
    gap_prompts = prompt_stats[
        (prompt_stats["cited_count"] > 0) & (prompt_stats["gap"] > 0)
    ].sort_values("gap", ascending=False)

    # ------------------------------------------------------------------
    # HEAD-TO-HEAD COMPETITOR COMPARISON
    # ------------------------------------------------------------------
    st.subheader("Head-to-Head vs Competitors")
    st.caption("Per-platform mention rates: us vs configured rivals on the same tracked prompts. Contested prompts are those where a competitor's mention rate meets or exceeds ours.")

    # Determine which topic we're looking at for competitor selection
    active_topic = selected_topic if selected_topic != "All" else None
    active_topics = [active_topic] if active_topic else list(TOPIC_COMPETITORS.keys())

    for topic in active_topics:
        rivals = TOPIC_COMPETITORS.get(topic)
        if not rivals:
            continue

        topic_data = filtered[filtered["topic"] == topic] if selected_topic == "All" else filtered
        if topic_data.empty:
            continue

        st.write(f"**{topic}** vs {', '.join(r.title() for r in rivals)}")

        # Compute mention flags for each rival
        rival_cols = {}
        for rival in rivals:
            col_name = f"{rival}_mentioned"
            topic_data = topic_data.copy()
            topic_data[col_name] = (
                topic_data["mentions"].fillna("").str.lower().str.contains(rival)
            )
            rival_cols[rival] = col_name

        h2h = (
            topic_data.groupby(["platform", "prompt"])
            .agg(
                total=("is_mentioned", "size"),
                our_mentions=("is_mentioned", "sum"),
                **{
                    f"{rival}_mentions": (col_name, "sum")
                    for rival, col_name in rival_cols.items()
                },
            )
            .reset_index()
        )
        h2h["our_rate"] = h2h["our_mentions"] / h2h["total"]
        for rival in rivals:
            h2h[f"{rival}_rate"] = h2h[f"{rival}_mentions"] / h2h["total"]

        # Show per-platform summary
        platform_h2h = (
            h2h.groupby("platform")
            .agg(
                total=("total", "sum"),
                our_mentions=("our_mentions", "sum"),
                **{
                    f"{rival}_mentions": (f"{rival}_mentions", "sum")
                    for rival in rivals
                },
            )
            .reset_index()
        )

        rows = []
        for _, row in platform_h2h.iterrows():
            entry = {"Platform": row["platform"]}
            entry[f"{topic} Mention Rate"] = f"{row['our_mentions'] / row['total']:.1%}"
            for rival in rivals:
                entry[f"{rival.title()} Rate"] = (
                    f"{row[f'{rival}_mentions'] / row['total']:.1%}"
                )
            rows.append(entry)

        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

        # Chart: contested prompts — just us vs the single top rival per prompt
        contested = h2h.copy()
        rival_rate_cols = [f"{r}_rate" for r in rivals]
        contested["max_rival_rate"] = contested[rival_rate_cols].max(axis=1)
        contested["top_rival"] = (
            contested[rival_rate_cols].idxmax(axis=1).str.replace("_rate", "", regex=False).str.title()
        )
        contested = contested[contested["max_rival_rate"] >= contested["our_rate"]]
        contested = contested[contested["max_rival_rate"] > 0]

        if not contested.empty:
            contested = contested.sort_values("max_rival_rate", ascending=False).head(15)
            contested["label"] = (
                contested["prompt"].str[:55] + "  (vs " + contested["top_rival"] + ")"
            )

            long_df = pd.concat([
                pd.DataFrame({
                    "label": contested["label"],
                    "Entity": topic,
                    "rate": contested["our_rate"] * 100,
                }),
                pd.DataFrame({
                    "label": contested["label"],
                    "Entity": "Top rival",
                    "rate": contested["max_rival_rate"] * 100,
                }),
            ])

            fig_h2h = px.bar(
                long_df,
                x="rate",
                y="label",
                color="Entity",
                orientation="h",
                barmode="group",
                title=f"Contested Prompts — {topic} vs the leading competitor for each prompt",
                color_discrete_map={topic: "#3b82f6", "Top rival": "#ef4444"},
                labels={"rate": "Mention Rate (%)", "label": ""},
            )
            fig_h2h.update_layout(
                yaxis={
                    "categoryorder": "array",
                    "categoryarray": contested["label"][::-1].tolist(),
                },
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_h2h, width="stretch")
        else:
            st.success(f"No contested prompts — {topic} leads on all tracked prompts!")

    # ------------------------------------------------------------------
    # COMPETITIVE COVERAGE GAPS — competitors mentioned, we're not, in specific rows
    # ------------------------------------------------------------------
    st.subheader("Competitive Coverage Gaps")
    st.caption(
        "Each row below is a prompt where competitors appeared in at least one AI response "
        "that did not mention us. **A prompt can appear here even if we're mentioned some of "
        "the time** — check 'Our Rate' to see our overall mention rate for that prompt. "
        "'Zero-Coverage Platforms' lists platforms where our rate is truly 0% for that prompt."
    )

    # Find individual response rows where we're NOT mentioned but a competitor IS
    missed = filtered[~filtered["is_mentioned"] & filtered["comp_mentioned"]].copy()

    if missed.empty:
        st.success("No competitive gaps — we appear in every response where competitors do!")
    else:
        # Our overall mention rate per prompt (across ALL rows, not just gap rows)
        prompt_rates = (
            filtered.groupby("prompt")
            .agg(
                total_rows=("is_mentioned", "size"),
                our_mentioned=("is_mentioned", "sum"),
            )
            .reset_index()
        )
        prompt_rates["our_rate_pct"] = (
            prompt_rates["our_mentioned"] / prompt_rates["total_rows"] * 100
        ).round(0).astype(int)

        # Platforms where our mention rate is truly 0 for each prompt
        per_prompt_platform = (
            filtered.groupby(["prompt", "platform"])["is_mentioned"].sum().reset_index()
        )
        zero_platforms = (
            per_prompt_platform[per_prompt_platform["is_mentioned"] == 0]
            .groupby("prompt")["platform"]
            .apply(lambda x: ", ".join(sorted(x.unique())))
            .reset_index()
            .rename(columns={"platform": "zero_platforms"})
        )

        # Gap summary: group gap rows by prompt
        missed_summary = (
            missed.groupby("prompt")
            .agg(
                competitors=("mentions", lambda x: ", ".join(
                    sorted({
                        name for mentions_str in x.dropna()
                        for name in str(mentions_str).lower().split(",")
                        if name.strip() in COMPETITOR_NAMES
                    })
                )),
                gap_instances=("prompt", "size"),
            )
            .reset_index()
        )

        # Add topic, our rate, and zero-coverage platforms
        prompt_topics = filtered.groupby("prompt")["topic"].first()
        missed_summary["topic"] = missed_summary["prompt"].map(prompt_topics)
        missed_summary = missed_summary.merge(
            prompt_rates[["prompt", "our_rate_pct", "total_rows"]], on="prompt", how="left"
        )
        missed_summary = missed_summary.merge(zero_platforms, on="prompt", how="left")
        missed_summary["zero_platforms"] = missed_summary["zero_platforms"].fillna("—")
        missed_summary["our_rate"] = missed_summary["our_rate_pct"].astype(str) + "%"

        # Sort by our mention rate ascending (worst coverage first)
        missed_summary = missed_summary.sort_values("our_rate_pct")

        st.dataframe(
            missed_summary[[
                "topic", "prompt", "our_rate", "zero_platforms", "competitors", "gap_instances"
            ]].rename(columns={
                "topic": "Topic",
                "prompt": "Prompt",
                "our_rate": "Our Rate",
                "zero_platforms": "Zero-Coverage Platforms",
                "competitors": "Competitors Mentioned",
                "gap_instances": "Gap Instances",
            }),
            hide_index=True,
            width="stretch",
        )

        # Metric: how many prompts have any gap vs total
        n_missed = missed_summary["prompt"].nunique()
        st.metric(
            "Prompts With At Least One Competitive Gap",
            f"{n_missed} / {total_prompts}",
        )

    # Insight for competitive landscape
    comp_summary = ""
    if not comp_names.empty:
        top3 = comp_names.value_counts().head(3)
        comp_summary = "\n".join(f"  {n}: {c}x" for n, c in top3.items())
    missed_text = f"Missed prompts: {missed['prompt'].nunique() if not missed.empty else 0}" if not missed.empty else ""
    per_platform_summary = ", ".join(
        f"{r['Platform']}: mean {r['Mean mention rate']}, ≥50% on {r['≥ 50%']}/{r['Prompts']} prompts"
        for r in per_platform_rows
    )
    h2h_text = f"""Prompt reach: {prompts_appeared}/{total_prompts}
Per-platform per-prompt rate: {per_platform_summary}
Top competitors:\n{comp_summary}
{missed_text}"""
    render_chart_insight("h2h_competitors", h2h_text, "Where are we losing to competitors and what should we prioritize?")

    # ------------------------------------------------------------------
    # PER-PROMPT TREND OVER TIME
    # ------------------------------------------------------------------
    st.subheader("Prompt Trends Over Time")
    st.caption(
        "Daily mention rate per prompt, averaged across platforms "
        "(= rows mentioned on that date ÷ rows for that prompt on that date)."
    )

    daily_prompt = (
        filtered.groupby(["date", "prompt"])
        .agg(rate=("is_mentioned", "mean"))
        .reset_index()
    )
    daily_prompt["rate"] *= 100

    available_prompts = (
        filtered.groupby("prompt")["is_mentioned"].sum()
        .sort_values(ascending=False)
        .index.tolist()
    )
    default_trend = available_prompts[:5]
    selected_trend = st.multiselect(
        "Prompts to chart",
        options=available_prompts,
        default=default_trend,
        key="prompt_trend_select",
    )

    if selected_trend:
        trend_df = daily_prompt[daily_prompt["prompt"].isin(selected_trend)]
        fig_trend = px.line(
            trend_df,
            x="date",
            y="rate",
            color="prompt",
            markers=True,
            title="Daily Mention Rate per Prompt (%)",
            labels={"rate": "Mention rate (%)", "date": "Date"},
        )
        unique_dates = sorted(trend_df["date"].unique())
        fig_trend.update_xaxes(
            tickmode="array", tickvals=unique_dates, tickformat="%b %d"
        )
        fig_trend.update_layout(yaxis_range=[-5, 105])
        st.plotly_chart(fig_trend, width="stretch")

    # --- Prompt-level detail table ---
    st.subheader("Prompt Detail")
    st.caption("Whether our brand was mentioned at least once on each platform for every tracked prompt — use this to identify platforms or individual prompts where we have zero presence.")

    prompt_detail = (
        filtered.groupby(["prompt", "platform"])
        .agg(
            mentioned=("is_mentioned", lambda x: x.any()),
            best_position=("position", "first"),
        )
        .reset_index()
    )
    prompt_pivot = prompt_detail.pivot(
        index="prompt", columns="platform", values="mentioned"
    ).fillna(False)
    prompt_pivot = prompt_pivot.replace({True: "Yes", False: "No"})
    prompt_pivot["Mentioned On"] = prompt_pivot.apply(
        lambda row: sum(1 for v in row if v == "Yes"), axis=1
    )
    prompt_pivot = prompt_pivot.sort_values("Mentioned On", ascending=False)

    st.dataframe(prompt_pivot, width="stretch")
