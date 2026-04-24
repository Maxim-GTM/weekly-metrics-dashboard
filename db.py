"""Database connection pool and query helpers for Neon Postgres."""

import json
import os
from datetime import date

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()

_pool: ConnectionPool | None = None

BATCH_SIZE = 10_000


def get_pool() -> ConnectionPool:
    """Return the module-level connection pool, creating it on first call."""
    global _pool
    if _pool is None:
        dsn = (os.environ.get("DATABASE_URL") or "").strip()
        if not dsn:
            raise RuntimeError("DATABASE_URL not set in .env")
        _pool = ConnectionPool(
            dsn,
            min_size=1,
            max_size=5,
            check=ConnectionPool.check_connection,
            kwargs={"row_factory": dict_row, "autocommit": False},
        )
    return _pool


def close_pool():
    """Close the connection pool."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def query_df(sql: str, params: dict | tuple = ()) -> pd.DataFrame:
    """Execute a read query and return a pandas DataFrame."""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _batch_execute(sql: str, rows: list[dict]):
    """Execute an INSERT/UPSERT in batches."""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                cur.executemany(sql, rows[i : i + BATCH_SIZE])
        conn.commit()


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------


def upsert_gsc(rows: list[dict]):
    """Upsert GSC rows into the gsc table."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc (date, page, query, clicks, impressions, ctr, position)
        VALUES (%(date)s, %(page)s, %(query)s, %(clicks)s, %(impressions)s, %(ctr)s, %(position)s)
        ON CONFLICT (date, md5(page), md5(query)) DO UPDATE SET
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            ctr = EXCLUDED.ctr,
            position = EXCLUDED.position
        """,
        rows,
    )


def upsert_gsc_page_daily(rows: list[dict]):
    """Upsert page-level GSC aggregates (no query dimension)."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc_page_daily (date, page, clicks, impressions, ctr, position)
        VALUES (%(date)s, %(page)s, %(clicks)s, %(impressions)s, %(ctr)s, %(position)s)
        ON CONFLICT (date, md5(page)) DO UPDATE SET
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            ctr = EXCLUDED.ctr,
            position = EXCLUDED.position
        """,
        rows,
    )


def upsert_gsc_site_daily(rows: list[dict]):
    """Upsert date-level GSC site totals (no page/query dimension — matches GSC UI)."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc_site_daily (date, clicks, impressions, ctr, position)
        VALUES (%(date)s, %(clicks)s, %(impressions)s, %(ctr)s, %(position)s)
        ON CONFLICT (date) DO UPDATE SET
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            ctr = EXCLUDED.ctr,
            position = EXCLUDED.position
        """,
        rows,
    )


def upsert_gsc_country(rows: list[dict]):
    """Upsert GSC country-level aggregates."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc_country (date, country, clicks, impressions, ctr, position)
        VALUES (%(date)s, %(country)s, %(clicks)s, %(impressions)s, %(ctr)s, %(position)s)
        ON CONFLICT (date, country) DO UPDATE SET
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            ctr = EXCLUDED.ctr,
            position = EXCLUDED.position
        """,
        rows,
    )


def upsert_ga4(rows: list[dict]):
    """Upsert GA4 rows into the ga4 table."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4 (date, page_path, session_source, session_medium,
                         sessions, engaged_sessions, engagement_duration_s, new_users,
                         exits, conversions)
        VALUES (%(date)s, %(page_path)s, %(session_source)s, %(session_medium)s,
                %(sessions)s, %(engaged_sessions)s, %(engagement_duration_s)s, %(new_users)s,
                %(exits)s, %(conversions)s)
        ON CONFLICT (date, page_path, session_source, session_medium) DO UPDATE SET
            sessions              = EXCLUDED.sessions,
            engaged_sessions      = EXCLUDED.engaged_sessions,
            engagement_duration_s = EXCLUDED.engagement_duration_s,
            new_users             = EXCLUDED.new_users,
            exits                 = EXCLUDED.exits,
            conversions           = EXCLUDED.conversions
        """,
        rows,
    )


def upsert_ga4_traffic(rows: list[dict]):
    """Upsert source+medium-level GA4 traffic metrics."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4_traffic
            (date, session_source, session_medium, sessions, total_users, active_users,
             engaged_sessions, new_users, engagement_duration_s)
        VALUES
            (%(date)s, %(session_source)s, %(session_medium)s,
             %(sessions)s, %(total_users)s, %(active_users)s,
             %(engaged_sessions)s, %(new_users)s, %(engagement_duration_s)s)
        ON CONFLICT (date, session_source, session_medium) DO UPDATE SET
            sessions              = EXCLUDED.sessions,
            total_users           = EXCLUDED.total_users,
            active_users          = EXCLUDED.active_users,
            engaged_sessions      = EXCLUDED.engaged_sessions,
            new_users             = EXCLUDED.new_users,
            engagement_duration_s = EXCLUDED.engagement_duration_s
        """,
        rows,
    )


def upsert_ga4_traffic_weekly(rows: list[dict]):
    """Upsert weekly first-user-source GA4 user acquisition aggregates."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4_traffic_weekly
            (date, first_user_source, first_user_medium, total_users, new_users)
        VALUES
            (%(date)s, %(first_user_source)s, %(first_user_medium)s,
             %(total_users)s, %(new_users)s)
        ON CONFLICT (date, first_user_source, first_user_medium) DO UPDATE SET
            total_users = EXCLUDED.total_users,
            new_users   = EXCLUDED.new_users
        """,
        rows,
    )


def upsert_ga4_events(rows: list[dict]):
    """Upsert GA4 event counts by channel group."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4_events
            (date, event_name, session_primary_channel_group, event_count)
        VALUES
            (%(date)s, %(event_name)s, %(session_primary_channel_group)s, %(event_count)s)
        ON CONFLICT (date, event_name, session_primary_channel_group) DO UPDATE SET
            event_count = EXCLUDED.event_count
        """,
        rows,
    )


def upsert_ga4_landing_pages(rows: list[dict]):
    """Upsert GA4 landing page rows."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4_landing_pages
            (date, landing_page, session_source, session_medium,
             sessions, engaged_sessions, new_users, engagement_duration_s, conversions)
        VALUES
            (%(date)s, %(landing_page)s, %(session_source)s, %(session_medium)s,
             %(sessions)s, %(engaged_sessions)s, %(new_users)s, %(engagement_duration_s)s,
             %(conversions)s)
        ON CONFLICT (date, landing_page, session_source, session_medium) DO UPDATE SET
            sessions              = EXCLUDED.sessions,
            engaged_sessions      = EXCLUDED.engaged_sessions,
            new_users             = EXCLUDED.new_users,
            engagement_duration_s = EXCLUDED.engagement_duration_s,
            conversions           = EXCLUDED.conversions
        """,
        rows,
    )


def upsert_ga4_category_sessions(rows: list[dict]):
    """Upsert GA4 category-level session counts (date × category × source × medium)."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4_category_sessions
            (date, page_category, session_source, session_medium, sessions)
        VALUES (%(date)s, %(page_category)s, %(session_source)s, %(session_medium)s, %(sessions)s)
        ON CONFLICT (date, page_category, session_source, session_medium) DO UPDATE SET
            sessions = EXCLUDED.sessions
        """,
        rows,
    )


def upsert_ga4_page_events(rows: list[dict]):
    """Upsert GA4 page-level event counts (scroll depth, etc.)."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4_page_events (date, page_path, event_name, event_count)
        VALUES (%(date)s, %(page_path)s, %(event_name)s, %(event_count)s)
        ON CONFLICT (date, page_path, event_name) DO UPDATE SET
            event_count = EXCLUDED.event_count
        """,
        rows,
    )


def upsert_ga4_page_before_conversion(rows: list[dict]):
    """Upsert previous-page-path counts for conversion events."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4_page_before_conversion (date, previous_page_path, event_name, event_count)
        VALUES (%(date)s, %(previous_page_path)s, %(event_name)s, %(event_count)s)
        ON CONFLICT (date, previous_page_path, event_name) DO UPDATE SET
            event_count = EXCLUDED.event_count
        """,
        rows,
    )


def upsert_gsc_coverage_daily(rows: list[dict]):
    """Upsert daily indexed/not-indexed counts from GSC Coverage Chart.csv."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc_coverage_daily (date, indexed, not_indexed, impressions)
        VALUES (%(date)s, %(indexed)s, %(not_indexed)s, %(impressions)s)
        ON CONFLICT (date) DO UPDATE SET
            indexed     = EXCLUDED.indexed,
            not_indexed = EXCLUDED.not_indexed,
            impressions = EXCLUDED.impressions
        """,
        rows,
    )


def upsert_gsc_coverage_reasons(rows: list[dict]):
    """Upsert not-indexed reason snapshot from GSC Coverage issues CSVs."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc_coverage_reasons (as_of_date, reason, source, validation, pages)
        VALUES (%(as_of_date)s, %(reason)s, %(source)s, %(validation)s, %(pages)s)
        ON CONFLICT (as_of_date, reason) DO UPDATE SET
            source     = EXCLUDED.source,
            validation = EXCLUDED.validation,
            pages      = EXCLUDED.pages
        """,
        rows,
    )


def upsert_gsc_coverage_urls(rows: list[dict]):
    """Upsert individual URLs from a GSC Coverage Drilldown Table.csv."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc_coverage_urls (as_of_date, reason, url, last_crawled)
        VALUES (%(as_of_date)s, %(reason)s, %(url)s, %(last_crawled)s)
        ON CONFLICT (as_of_date, reason, md5(url)) DO NOTHING
        """,
        rows,
    )


def upsert_gsc_non_indexed(rows: list[dict]):
    """Upsert non-indexed page snapshots (week_start × page_url)."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO gsc_non_indexed (week_start, page_url)
        VALUES (%(week_start)s, %(page_url)s)
        ON CONFLICT (week_start, md5(page_url)) DO NOTHING
        """,
        rows,
    )


def upsert_keywords(rows: list[dict]):
    """Upsert keyword ranking rows."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO keyword_rankings
            (keyword, date, source, rank, result_type, landing_page,
             search_volume, cpc, difficulty, tags, intents)
        VALUES
            (%(keyword)s, %(date)s, %(source)s, %(rank)s, %(result_type)s, %(landing_page)s,
             %(search_volume)s, %(cpc)s, %(difficulty)s, %(tags)s, %(intents)s)
        ON CONFLICT (keyword, date, source) DO UPDATE SET
            rank = EXCLUDED.rank,
            result_type = EXCLUDED.result_type,
            landing_page = EXCLUDED.landing_page,
            search_volume = EXCLUDED.search_volume,
            cpc = EXCLUDED.cpc,
            difficulty = EXCLUDED.difficulty,
            tags = EXCLUDED.tags,
            intents = EXCLUDED.intents
        """,
        rows,
    )


def upsert_profound(rows: list[dict]):
    """Upsert Profound rows. citations must be a JSON string."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO profound
            (date, topic, prompt, platform, position, mentioned, mentions,
             citations, response, run_id, platform_id, tags, region,
             persona, type, search_queries, normalized_mentions)
        VALUES
            (%(date)s, %(topic)s, %(prompt)s, %(platform)s, %(position)s,
             %(mentioned)s, %(mentions)s, %(citations)s::jsonb, %(response)s,
             %(run_id)s, %(platform_id)s, %(tags)s, %(region)s,
             %(persona)s, %(type)s, %(search_queries)s, %(normalized_mentions)s)
        ON CONFLICT (date, topic, prompt, platform) DO UPDATE SET
            position = EXCLUDED.position,
            mentioned = EXCLUDED.mentioned,
            mentions = EXCLUDED.mentions,
            citations = EXCLUDED.citations,
            response = EXCLUDED.response,
            run_id = EXCLUDED.run_id,
            platform_id = EXCLUDED.platform_id,
            tags = EXCLUDED.tags,
            region = EXCLUDED.region,
            persona = EXCLUDED.persona,
            type = EXCLUDED.type,
            search_queries = EXCLUDED.search_queries,
            normalized_mentions = EXCLUDED.normalized_mentions
        """,
        rows,
    )


def replace_keyword_tiers(rows: list[dict]):
    """Truncate keyword_tiers and insert fresh rows. rows: [{keyword, tier}]."""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE keyword_tiers")
            if rows:
                cur.executemany(
                    "INSERT INTO keyword_tiers (keyword, tier) "
                    "VALUES (%(keyword)s, %(tier)s)",
                    rows,
                )
        conn.commit()


def has_keyword_tiers() -> bool:
    """Return True if keyword_tiers has any rows."""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM keyword_tiers)")
            return cur.fetchone()["exists"]


def sync_gsc_keyword_rankings() -> int:
    """Populate keyword_rankings (source='gsc') from the gsc table.

    For each tracked keyword (anything already in keyword_rankings with
    source='semrush'), pulls the best (min) position per date from gsc and
    upserts a matching source='gsc' row. Returns number of rows affected.
    """
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH tracked AS (
                    SELECT DISTINCT keyword AS canonical,
                           LOWER(TRIM(keyword)) AS normalized
                    FROM keyword_rankings
                    WHERE source = 'semrush'
                ),
                agg AS (
                    SELECT t.canonical           AS keyword,
                           g.date                AS date,
                           SUM(g.clicks)         AS clicks,
                           SUM(g.impressions)    AS impressions,
                           MIN(g.position)       AS rank
                    FROM gsc g
                    JOIN tracked t
                      ON LOWER(TRIM(g.query)) = t.normalized
                    GROUP BY t.canonical, g.date
                ),
                best_landing AS (
                    SELECT DISTINCT ON (t.canonical, g.date)
                           t.canonical AS keyword,
                           g.date      AS date,
                           g.page      AS landing_page
                    FROM gsc g
                    JOIN tracked t
                      ON LOWER(TRIM(g.query)) = t.normalized
                    ORDER BY t.canonical, g.date, g.position ASC
                )
                INSERT INTO keyword_rankings
                    (keyword, date, source, rank, result_type, landing_page,
                     search_volume, cpc, difficulty, tags, intents,
                     clicks, impressions)
                SELECT a.keyword, a.date, 'gsc', a.rank, '', b.landing_page,
                       NULL, NULL, NULL, '', '',
                       a.clicks, a.impressions
                FROM agg a
                JOIN best_landing b USING (keyword, date)
                ON CONFLICT (keyword, date, source) DO UPDATE SET
                    rank = EXCLUDED.rank,
                    landing_page = EXCLUDED.landing_page,
                    clicks = EXCLUDED.clicks,
                    impressions = EXCLUDED.impressions
            """)
            affected = cur.rowcount
        conn.commit()
    return affected


def update_keyword_products(keyword_products: dict[str, str]):
    """Update the product tag for keywords. keyword_products: {keyword: product}."""
    if not keyword_products:
        return
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            for keyword, product in keyword_products.items():
                cur.execute(
                    "UPDATE keyword_rankings SET product = %s WHERE keyword = %s",
                    (product, keyword),
                )
        conn.commit()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def has_data_for_range(table: str, start_date: str, end_date: str) -> bool:
    """Check if any rows exist in table for the given date range."""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT EXISTS(SELECT 1 FROM {table} WHERE date >= %s AND date <= %s)",
                (start_date, end_date),
            )
            return cur.fetchone()["exists"]


def latest_data_date(table: str) -> date | None:
    """Return the most recent date value in the given table, or None if empty."""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(date) AS max_date FROM {table}")
            row = cur.fetchone()
            return row["max_date"] if row else None
