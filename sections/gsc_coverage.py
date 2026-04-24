"""GSC Coverage — index status from uploaded GSC Coverage and Drilldown exports."""

import io
import zipfile
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from db import query_df


# ---------------------------------------------------------------------------
# Zip parsing
# ---------------------------------------------------------------------------

def _csv_from_zip(zip_bytes: bytes, filename: str) -> pd.DataFrame | None:
    """Extract and parse a named CSV from a zip, handling nested directories."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        match = next(
            (n for n in zf.namelist() if n == filename or n.endswith("/" + filename)),
            None,
        )
        if match is None:
            return None
        with zf.open(match) as f:
            return pd.read_csv(f)


def _parse_coverage_zip(zip_bytes: bytes) -> tuple[list[dict], list[dict], str | None]:
    """Parse a GSC Coverage export zip. Returns (daily_rows, reason_rows, as_of_date)."""
    chart = _csv_from_zip(zip_bytes, "Chart.csv")
    if chart is None or "Not indexed" not in chart.columns:
        return [], [], None

    as_of_date = str(chart["Date"].max())

    daily_rows = [
        {
            "date": str(row["Date"]),
            "indexed": int(row["Indexed"]),
            "not_indexed": int(row["Not indexed"]),
            "impressions": int(row["Impressions"]),
        }
        for _, row in chart.iterrows()
    ]

    reason_rows = []
    for fname in ("Critical issues.csv", "Non-critical issues.csv"):
        df = _csv_from_zip(zip_bytes, fname)
        if df is None:
            continue
        for _, row in df.iterrows():
            reason_rows.append({
                "as_of_date": as_of_date,
                "reason": str(row.get("Reason", "")),
                "source": str(row.get("Source", "")),
                "validation": str(row.get("Validation", "")),
                "pages": int(row.get("Pages", 0)),
            })

    return daily_rows, reason_rows, as_of_date


def _parse_drilldown_zip(zip_bytes: bytes) -> tuple[str | None, list[dict], str | None]:
    """Parse a GSC Coverage Drilldown zip. Returns (reason, url_rows, as_of_date)."""
    metadata = _csv_from_zip(zip_bytes, "Metadata.csv")
    chart = _csv_from_zip(zip_bytes, "Chart.csv")
    table = _csv_from_zip(zip_bytes, "Table.csv")

    if metadata is None or table is None or "URL" not in table.columns:
        return None, [], None

    meta_dict = dict(zip(metadata.iloc[:, 0].astype(str), metadata.iloc[:, 1].astype(str)))
    reason = meta_dict.get("Issue")
    if not reason:
        return None, [], None

    as_of_date = str(chart["Date"].max()) if chart is not None else date.today().isoformat()

    url_rows = []
    for _, row in table.iterrows():
        lc = row.get("Last crawled")
        url_rows.append({
            "as_of_date": as_of_date,
            "reason": reason,
            "url": str(row["URL"]),
            "last_crawled": str(lc) if pd.notna(lc) else None,
        })

    return reason, url_rows, as_of_date


# ---------------------------------------------------------------------------
# DB loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, max_entries=1)
def _load_coverage_daily() -> pd.DataFrame | None:
    df = query_df(
        "SELECT date, indexed, not_indexed, impressions "
        "FROM gsc_coverage_daily ORDER BY date"
    )
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_latest_reasons() -> pd.DataFrame | None:
    df = query_df("""
        SELECT reason, source, validation, pages
        FROM gsc_coverage_reasons
        WHERE as_of_date = (SELECT MAX(as_of_date) FROM gsc_coverage_reasons)
        ORDER BY pages DESC
    """)
    return df if not df.empty else None


@st.cache_data(ttl=300, max_entries=1)
def _load_latest_urls() -> pd.DataFrame | None:
    """Load the most recent URL snapshot per reason."""
    df = query_df("""
        WITH latest_per_reason AS (
            SELECT reason, MAX(as_of_date) AS max_date
            FROM gsc_coverage_urls
            GROUP BY reason
        )
        SELECT u.as_of_date, u.reason, u.url, u.last_crawled
        FROM gsc_coverage_urls u
        JOIN latest_per_reason l ON u.reason = l.reason AND u.as_of_date = l.max_date
        ORDER BY u.reason, u.url
    """)
    if df.empty:
        return None
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


@st.cache_data(ttl=300, max_entries=1)
def _load_prev_urls() -> dict[str, set[str]]:
    """Previous URL snapshot per reason for NEW detection. Returns {reason: {url, ...}}."""
    df = query_df("""
        WITH latest_per_reason AS (
            SELECT reason, MAX(as_of_date) AS max_date
            FROM gsc_coverage_urls
            GROUP BY reason
        ),
        prev_per_reason AS (
            SELECT u.reason, MAX(u.as_of_date) AS prev_date
            FROM gsc_coverage_urls u
            JOIN latest_per_reason l ON u.reason = l.reason
            WHERE u.as_of_date < l.max_date
            GROUP BY u.reason
        )
        SELECT u.reason, u.url
        FROM gsc_coverage_urls u
        JOIN prev_per_reason p ON u.reason = p.reason AND u.as_of_date = p.prev_date
    """)
    if df.empty:
        return {}
    result: dict[str, set[str]] = {}
    for reason, group in df.groupby("reason"):
        result[reason] = set(group["url"].tolist())
    return result


# ---------------------------------------------------------------------------
# Upload handlers
# ---------------------------------------------------------------------------

def _handle_coverage_upload(zip_bytes: bytes):
    from db import upsert_gsc_coverage_daily, upsert_gsc_coverage_reasons

    daily_rows, reason_rows, as_of_date = _parse_coverage_zip(zip_bytes)
    if not daily_rows:
        st.error(
            "Could not parse this ZIP as a Coverage export. "
            "Upload the top-level Coverage export (Chart.csv must contain Indexed and Not indexed columns)."
        )
        return

    upsert_gsc_coverage_daily(daily_rows)
    if reason_rows:
        upsert_gsc_coverage_reasons(reason_rows)

    st.success(f"Imported {len(daily_rows):,} days of coverage data (latest: {as_of_date}).")
    _load_coverage_daily.clear()
    _load_latest_reasons.clear()
    st.rerun()


def _handle_drilldown_upload(zip_bytes: bytes):
    from db import get_pool, upsert_gsc_coverage_urls

    reason, url_rows, as_of_date = _parse_drilldown_zip(zip_bytes)
    if not url_rows:
        st.error(
            "Could not parse this ZIP as a Drilldown export. "
            "Upload a per-reason drilldown export (needs Table.csv with URL column and Metadata.csv with Issue entry)."
        )
        return

    # Replace any existing rows for this (as_of_date, reason) snapshot
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM gsc_coverage_urls WHERE as_of_date = %s AND reason = %s",
                (as_of_date, reason),
            )
        conn.commit()

    upsert_gsc_coverage_urls(url_rows)
    st.success(f"Imported {len(url_rows):,} URLs for '{reason}' (as of {as_of_date}).")
    _load_latest_urls.clear()
    _load_prev_urls.clear()
    st.rerun()


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render():
    st.header("GSC Coverage (Index Status)")

    coverage_df = _load_coverage_daily()
    reasons_df = _load_latest_reasons()
    urls_df = _load_latest_urls()

    # --- Data freshness ---
    freshness = []
    if coverage_df is not None:
        latest_cov = coverage_df["date"].max().date()
        freshness.append(f"Coverage data as of **{latest_cov:%b %d, %Y}**")
    if urls_df is not None:
        n_reasons = urls_df["reason"].nunique()
        freshness.append(f"**{n_reasons}** drilldown reason{'s' if n_reasons != 1 else ''} uploaded")
    if freshness:
        st.caption(" · ".join(freshness))

    # --- Upload ---
    st.subheader("Upload GSC Exports")
    st.markdown(
        "In GSC → **Indexing → Pages**: export the top-level report as the Coverage ZIP, "
        "then click into a specific issue reason and export that as the Drilldown ZIP."
    )
    col_cov, col_dd = st.columns(2)

    with col_cov:
        cov_file = st.file_uploader(
            "Coverage ZIP",
            type=["zip"],
            key="gsc_coverage_zip",
            help="Top-level export. Contains Chart.csv (Indexed/Not indexed) + issues CSVs.",
        )
        if cov_file is not None:
            _handle_coverage_upload(cov_file.read())

    with col_dd:
        dd_file = st.file_uploader(
            "Drilldown ZIP",
            type=["zip"],
            key="gsc_drilldown_zip",
            help="Per-reason export. Contains Table.csv with individual URLs and Last crawled dates.",
        )
        if dd_file is not None:
            _handle_drilldown_upload(dd_file.read())

    if coverage_df is None:
        st.info("Upload a Coverage ZIP to see indexed / not-indexed trends.")
        return

    # --- Metrics ---
    latest_date = coverage_df["date"].max()
    latest = coverage_df[coverage_df["date"] == latest_date].iloc[0]
    prev_row = coverage_df[coverage_df["date"] == latest_date - timedelta(days=7)]
    prev = prev_row.iloc[0] if not prev_row.empty else None

    def _fmt_delta(curr: int, prev_val: int | None, inverse: bool = False) -> str | None:
        if prev_val is None:
            return None
        return f"{curr - prev_val:+,}"

    curr_indexed = int(latest["indexed"])
    curr_not = int(latest["not_indexed"])
    curr_total = curr_indexed + curr_not
    prev_indexed = int(prev["indexed"]) if prev is not None else None
    prev_not = int(prev["not_indexed"]) if prev is not None else None
    prev_total = (prev_indexed + prev_not) if prev is not None else None

    st.subheader(f"Index Status — {latest_date.date():%b %d, %Y}")
    st.caption("Snapshot of how many pages Google has indexed vs. excluded as of the latest Coverage upload — delta is vs. the same date 7 days prior.")
    m1, m2, m3 = st.columns(3)
    m1.metric("Indexed Pages", f"{curr_indexed:,}", delta=_fmt_delta(curr_indexed, prev_indexed))
    m2.metric(
        "Not Indexed",
        f"{curr_not:,}",
        delta=_fmt_delta(curr_not, prev_not),
        delta_color="inverse",
    )
    m3.metric("Total Known Pages", f"{curr_total:,}", delta=_fmt_delta(curr_total, prev_total))

    # --- Coverage trend ---
    st.subheader("Coverage Trend")
    st.caption("How the count of indexed and non-indexed pages has changed over time — a rising 'Not Indexed' or falling 'Indexed' line signals crawl or indexability problems that need investigation.")

    trend_long = pd.melt(
        coverage_df,
        id_vars=["date"],
        value_vars=["indexed", "not_indexed"],
        var_name="status",
        value_name="pages",
    )
    trend_long["status"] = trend_long["status"].map(
        {"indexed": "Indexed", "not_indexed": "Not Indexed"}
    )

    fig = px.line(
        trend_long,
        x="date",
        y="pages",
        color="status",
        color_discrete_map={"Indexed": "#00c853", "Not Indexed": "#f44336"},
        title="Indexed vs Not Indexed Pages Over Time",
        labels={"date": "Date", "pages": "Pages", "status": ""},
    )
    st.plotly_chart(fig, width="stretch")

    # --- Reason breakdown ---
    if reasons_df is not None:
        st.subheader("Not-Indexed by Reason")
        st.caption("Why Google excluded pages from the index, with page count and validation status for each reason as of the latest Coverage upload.")
        total_not = reasons_df["pages"].sum()
        display = reasons_df.copy()
        display["% of not-indexed"] = (display["pages"] / total_not * 100).round(1)
        st.dataframe(
            display.rename(columns={
                "reason": "Reason",
                "pages": "Pages",
                "source": "Detected by",
                "validation": "Validation",
                "% of not-indexed": "% of Not-Indexed",
            })[["Reason", "Pages", "% of Not-Indexed", "Detected by", "Validation"]],
            hide_index=True,
            width="stretch",
        )

    # --- Drilldown URL lists (one expander per reason) ---
    if urls_df is not None:
        prev_urls_by_reason = _load_prev_urls()
        # Build lookup: reason → actual total from Coverage reasons table
        reason_totals: dict[str, int] = {}
        if reasons_df is not None:
            reason_totals = dict(zip(reasons_df["reason"], reasons_df["pages"]))

        st.subheader("Drilldown URLs by Reason")
        st.caption("Individual URLs excluded from the index for each reason, with last-crawled dates and NEW markers for URLs that appeared since the prior upload. GSC exports up to 1,000 URLs per reason — upload additional drilldown ZIPs to cover more reasons.")

        for reason_name, group in urls_df.groupby("reason", sort=False):
            as_of = group["as_of_date"].max().date()
            curr_set = set(group["url"])
            prev_set = prev_urls_by_reason.get(reason_name, set())
            newly_flagged = curr_set - prev_set

            total = reason_totals.get(reason_name)
            url_count_label = (
                f"{len(curr_set):,} of {total:,} URLs" if total else f"{len(curr_set):,} URLs"
            )
            new_label = f" · **{len(newly_flagged)} NEW**" if newly_flagged else ""
            header = (
                f"{reason_name} — {url_count_label} as of {as_of:%b %d, %Y}{new_label}"
            )
            with st.expander(header):
                display_rows = group.copy()
                display_rows["_new"] = display_rows["url"].isin(newly_flagged)
                display_rows = display_rows.sort_values(["_new", "url"], ascending=[False, True])

                for _, row in display_rows.iterrows():
                    badge = " **`NEW`**" if row["_new"] else ""
                    crawled = (
                        f" — last crawled {row['last_crawled']}"
                        if pd.notna(row.get("last_crawled"))
                        else ""
                    )
                    st.markdown(f"- {row['url']}{badge}{crawled}")
