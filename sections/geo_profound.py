"""GEO Performance — Profound data analysis."""

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
)
from llm import render_llm_insights

PLATFORMS = ["ChatGPT", "Google AI Overviews", "Perplexity"]


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _load_profound_data(uploaded_file) -> pd.DataFrame:
    df = pd.read_csv(uploaded_file)
    df["date"] = pd.to_datetime(df["date"])
    return df


def _citation_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if re.match(r"citation_\d+", c)]


def _is_mentioned(df: pd.DataFrame) -> pd.Series:
    """Boolean series: was our brand mentioned by name?"""
    return df["mentioned?"].astype(str).str.lower().isin(["yes", "true", "1"])


def _has_domain(row, domain_set: set, cit_cols: list[str]) -> bool:
    """Check if any citation URL belongs to one of the given domains."""
    for c in cit_cols:
        v = row[c]
        if not isinstance(v, str) or not v.startswith("http"):
            continue
        try:
            h = urlparse(v).netloc.lower().removeprefix("www.")
            if any(h == d or h.endswith("." + d) for d in domain_set):
                return True
        except Exception:
            pass
    return False


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

    if uploaded is None:
        st.info("Upload a Profound CSV export to see GEO analysis.")
        return

    df = _load_profound_data(uploaded)

    # Pre-compute derived columns once
    cit_cols = _citation_columns(df)
    df["is_mentioned"] = _is_mentioned(df)
    df["has_fp_citation"] = df.apply(
        lambda row: _has_domain(row, OWNED_DOMAINS, cit_cols), axis=1
    )
    df["comp_mentioned"] = df["mentions"].apply(_comp_mentioned)

    # --- Filters ---
    col1, col2, col3 = st.columns(3)
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
    with col3:
        selected_platform = st.selectbox("Platform", ["All"] + PLATFORMS)

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

    total_prompts = filtered["prompt"].nunique()
    mentioned = filtered[filtered["is_mentioned"]]
    prompts_appeared = mentioned["prompt"].nunique()

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Unique Prompts", total_prompts)
    m2.metric("Prompts We're Mentioned In", prompts_appeared)
    m3.metric("Mention Rate", f"{prompts_appeared / total_prompts * 100:.0f}%")

    # --- Per-platform appearance ---
    st.subheader("Mentions by Platform")

    platform_stats = []
    for platform in PLATFORMS:
        pdf = filtered[filtered["platform"] == platform]
        total = pdf["prompt"].nunique()
        appeared = pdf[pdf["is_mentioned"]]["prompt"].nunique()
        platform_stats.append(
            {
                "Platform": platform,
                "Total Prompts": total,
                "Mentioned": appeared,
                "Mention Rate": f"{appeared / total * 100:.0f}%" if total else "N/A",
            }
        )

    stats_df = pd.DataFrame(platform_stats)
    col_chart, col_table = st.columns([2, 1])

    with col_chart:
        fig = px.bar(
            stats_df,
            x="Platform",
            y="Mentioned",
            color="Platform",
            text="Mentioned",
            title="Prompts Mentioned by Platform",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.dataframe(stats_df, hide_index=True, use_container_width=True)

    # --- Cross-platform overlap ---
    st.subheader("Cross-Platform Overlap")

    prompt_platforms = (
        mentioned.groupby("prompt")["platform"].apply(set).reset_index()
    )
    prompt_platforms["num_platforms"] = prompt_platforms["platform"].apply(len)

    all_three = (prompt_platforms["num_platforms"] == 3).sum()
    at_least_two = (prompt_platforms["num_platforms"] >= 2).sum()
    exactly_one = (prompt_platforms["num_platforms"] == 1).sum()

    o1, o2, o3 = st.columns(3)
    o1.metric("All 3 Platforms", all_three)
    o2.metric("At Least 2 Platforms", at_least_two)
    o3.metric("Exactly 1 Platform", exactly_one)

    overlap_data = []
    for p1 in PLATFORMS:
        row = {}
        prompts_p1 = set(mentioned[mentioned["platform"] == p1]["prompt"].unique())
        for p2 in PLATFORMS:
            prompts_p2 = set(mentioned[mentioned["platform"] == p2]["prompt"].unique())
            row[p2] = len(prompts_p1 & prompts_p2)
        overlap_data.append(row)

    overlap_df = pd.DataFrame(overlap_data, index=PLATFORMS)
    fig_heatmap = px.imshow(
        overlap_df,
        text_auto=True,
        title="Platform Overlap (shared prompt mentions)",
        color_continuous_scale="Blues",
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

    # --- Position distribution ---
    st.subheader("Position Distribution")

    positioned = mentioned[mentioned["position"].astype(str).str.strip() != ""].copy()
    if not positioned.empty:
        positioned["position_num"] = (
            positioned["position"].astype(str).str.replace("#", "").astype(int)
        )
        fig_pos = px.histogram(
            positioned,
            x="position_num",
            color="platform",
            barmode="group",
            nbins=15,
            title="Distribution of Mention Positions by Platform",
            labels={"position_num": "Position", "count": "Count"},
        )
        st.plotly_chart(fig_pos, use_container_width=True)

        avg_pos = positioned.groupby("platform")["position_num"].mean()
        st.write("**Average position by platform:**")
        for platform, pos in avg_pos.items():
            st.write(f"- {platform}: #{pos:.1f}")

    # ------------------------------------------------------------------
    # COMPETITOR MENTIONS — replaces old "Most Cited Pages" section
    # ------------------------------------------------------------------
    st.subheader("Competitor Mentions")

    comp_names = _extract_comp_names(filtered["mentions"])

    if comp_names.empty:
        st.info("No competitor mentions found in the filtered data.")
    else:
        comp_counts = comp_names.value_counts().reset_index()
        comp_counts.columns = ["Competitor", "Mentions"]

        fig_comp = px.bar(
            comp_counts.head(15),
            x="Mentions",
            y="Competitor",
            orientation="h",
            title="Most Mentioned Competitors",
        )
        fig_comp.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_comp, use_container_width=True)

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
                pd.DataFrame(comp_by_platform), hide_index=True, use_container_width=True
            )

    # ------------------------------------------------------------------
    # CITE vs MENTION GAP
    # ------------------------------------------------------------------
    st.subheader("Cited but Not Mentioned")
    st.caption(
        "Prompts where our URL appears in citations but our brand isn't mentioned "
        "by name — the best targets for improving brand visibility."
    )

    # Per-prompt stats
    prompt_stats = (
        filtered.groupby(["topic", "prompt"])
        .agg(
            total=("is_mentioned", "size"),
            mentioned_count=("is_mentioned", "sum"),
            cited_count=("has_fp_citation", "sum"),
            comp_mention_count=("comp_mentioned", "sum"),
        )
        .reset_index()
    )
    prompt_stats["mention_rate"] = prompt_stats["mentioned_count"] / prompt_stats["total"]
    prompt_stats["cite_rate"] = prompt_stats["cited_count"] / prompt_stats["total"]
    prompt_stats["gap"] = prompt_stats["cite_rate"] - prompt_stats["mention_rate"]

    # Only show prompts where we ARE cited but gap > 0
    gap_prompts = prompt_stats[
        (prompt_stats["cited_count"] > 0) & (prompt_stats["gap"] > 0)
    ].sort_values("gap", ascending=False)

    if gap_prompts.empty:
        st.success("No cite-but-not-mentioned gaps found — great brand visibility!")
    else:
        top_gaps = gap_prompts.head(15).copy()
        top_gaps["label"] = top_gaps["prompt"].str[:60]

        fig_gap = go.Figure()
        fig_gap.add_trace(
            go.Bar(
                y=top_gaps["label"],
                x=top_gaps["cite_rate"],
                name="Citation Rate",
                orientation="h",
                marker_color="#93c5fd",
            )
        )
        fig_gap.add_trace(
            go.Bar(
                y=top_gaps["label"],
                x=top_gaps["mention_rate"],
                name="Mention Rate",
                orientation="h",
                marker_color="#3b82f6",
            )
        )
        fig_gap.update_layout(
            title="Biggest Citation → Mention Gaps (Top 15)",
            barmode="overlay",
            yaxis={"categoryorder": "array", "categoryarray": top_gaps["label"][::-1].tolist()},
            xaxis_title="Rate",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_gap, use_container_width=True)

        # Platform breakdown of the gap
        platform_gap = (
            filtered.groupby("platform")
            .agg(
                total=("is_mentioned", "size"),
                mentioned=("is_mentioned", "sum"),
                cited=("has_fp_citation", "sum"),
            )
            .reset_index()
        )
        platform_gap["mention_rate"] = platform_gap["mentioned"] / platform_gap["total"]
        platform_gap["cite_rate"] = platform_gap["cited"] / platform_gap["total"]
        platform_gap["gap"] = platform_gap["cite_rate"] - platform_gap["mention_rate"]

        st.write("**Citation → Mention gap by platform:**")
        display_gap = platform_gap[["platform", "cite_rate", "mention_rate", "gap"]].copy()
        display_gap.columns = ["Platform", "Citation Rate", "Mention Rate", "Gap"]
        for col in ["Citation Rate", "Mention Rate", "Gap"]:
            display_gap[col] = display_gap[col].map(lambda x: f"{x:.1%}")
        st.dataframe(display_gap, hide_index=True, use_container_width=True)

    # ------------------------------------------------------------------
    # HEAD-TO-HEAD COMPETITOR COMPARISON
    # ------------------------------------------------------------------
    st.subheader("Head-to-Head vs Competitors")

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

        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

        # Chart: contested prompts where any competitor mentions >= ours
        contested = h2h.copy()
        contested["max_rival"] = contested[[f"{r}_rate" for r in rivals]].max(axis=1)
        contested = contested[contested["max_rival"] >= contested["our_rate"]]
        contested = contested[contested["max_rival"] > 0]

        if not contested.empty:
            contested = contested.sort_values("max_rival", ascending=False).head(15)
            contested["label"] = contested["prompt"].str[:50]

            fig_h2h = go.Figure()
            fig_h2h.add_trace(
                go.Bar(
                    y=contested["label"],
                    x=contested["our_rate"],
                    name=topic,
                    orientation="h",
                    marker_color="#3b82f6",
                )
            )
            colors = ["#ef4444", "#f97316", "#eab308", "#84cc16"]
            for i, rival in enumerate(rivals):
                fig_h2h.add_trace(
                    go.Bar(
                        y=contested["label"],
                        x=contested[f"{rival}_rate"],
                        name=rival.title(),
                        orientation="h",
                        marker_color=colors[i % len(colors)],
                    )
                )

            fig_h2h.update_layout(
                title=f"Contested Prompts — {topic} vs Competitors",
                barmode="group",
                yaxis={
                    "categoryorder": "array",
                    "categoryarray": contested["label"][::-1].tolist(),
                },
                xaxis_title="Mention Rate",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_h2h, use_container_width=True)
        else:
            st.success(f"No contested prompts — {topic} leads on all tracked prompts!")

    # --- Prompt-level detail table ---
    st.subheader("Prompt Detail")

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

    st.dataframe(prompt_pivot, use_container_width=True)

    # --- LLM Insights ---
    # Build summary including new mention-focused data
    comp_summary = ""
    if not comp_names.empty:
        top3 = comp_names.value_counts().head(3)
        comp_summary = f"\nTop competitors mentioned: {', '.join(f'{n} ({c}x)' for n, c in top3.items())}"

    gap_summary = ""
    if not gap_prompts.empty:
        gap_summary = (
            f"\nCite-but-not-mentioned prompts: {len(gap_prompts)}"
            f", biggest gaps: {', '.join(gap_prompts['prompt'].head(3).tolist())}"
        )

    data_summary = f"""Topic: {selected_topic}
Total prompts: {total_prompts}, Mentioned in: {prompts_appeared} ({prompts_appeared/total_prompts*100:.0f}%)
Per platform: {', '.join(f"{s['Platform']}: {s['Mentioned']}/{s['Total Prompts']}" for s in platform_stats)}
Cross-platform: All 3: {all_three}, At least 2: {at_least_two}, Only 1: {exactly_one}{comp_summary}{gap_summary}"""

    render_llm_insights("GEO Performance", data_summary)
