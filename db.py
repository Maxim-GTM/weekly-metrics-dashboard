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


def upsert_ga4(rows: list[dict]):
    """Upsert GA4 rows into the ga4 table."""
    if not rows:
        return
    _batch_execute(
        """
        INSERT INTO ga4 (date, page_path, session_source, session_medium, sessions)
        VALUES (%(date)s, %(page_path)s, %(session_source)s, %(session_medium)s, %(sessions)s)
        ON CONFLICT (date, page_path, session_source, session_medium) DO UPDATE SET
            sessions = EXCLUDED.sessions
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
            (keyword, date, rank, result_type, landing_page,
             search_volume, cpc, difficulty, tags, intents)
        VALUES
            (%(keyword)s, %(date)s, %(rank)s, %(result_type)s, %(landing_page)s,
             %(search_volume)s, %(cpc)s, %(difficulty)s, %(tags)s, %(intents)s)
        ON CONFLICT (keyword, date) DO UPDATE SET
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
