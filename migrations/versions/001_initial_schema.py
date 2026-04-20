"""Initial schema: all 8 tables for the weekly metrics dashboard.

This is a squashed migration. Prior incremental migrations (001–008) have been
consolidated into this single file for clarity.

For existing databases already at the old revision 008, run:
    uv run alembic stamp 001
to tell Alembic the schema is current without re-running CREATE TABLE.

Revision ID: 001
Revises:
Create Date: 2026-04-20
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # --- GSC: page × query level with surrogate PK (md5 index works around the
    #     2704-byte btree limit for long TEXT natural keys) ---
    op.execute("""
        CREATE TABLE gsc (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            date        DATE             NOT NULL,
            page        TEXT             NOT NULL,
            query       TEXT             NOT NULL,
            clicks      INTEGER          NOT NULL DEFAULT 0,
            impressions INTEGER          NOT NULL DEFAULT 0,
            ctr         DOUBLE PRECISION NOT NULL DEFAULT 0,
            position    DOUBLE PRECISION NOT NULL DEFAULT 0
        );
        CREATE UNIQUE INDEX idx_gsc_natural_key ON gsc (date, md5(page), md5(query));
        CREATE INDEX idx_gsc_date ON gsc (date);
    """)

    # --- GSC country-level aggregate ---
    op.execute("""
        CREATE TABLE gsc_country (
            date        DATE             NOT NULL,
            country     TEXT             NOT NULL,
            clicks      INTEGER          NOT NULL DEFAULT 0,
            impressions INTEGER          NOT NULL DEFAULT 0,
            ctr         DOUBLE PRECISION NOT NULL DEFAULT 0,
            position    DOUBLE PRECISION NOT NULL DEFAULT 0,
            PRIMARY KEY (date, country)
        );
        CREATE INDEX idx_gsc_country_date ON gsc_country (date);
    """)

    # --- GA4 page-level sessions ---
    op.execute("""
        CREATE TABLE ga4 (
            date            DATE    NOT NULL,
            page_path       TEXT    NOT NULL,
            session_source  TEXT    NOT NULL,
            session_medium  TEXT    NOT NULL DEFAULT '',
            sessions        INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, page_path, session_source, session_medium)
        );
        CREATE INDEX idx_ga4_date ON ga4 (date);
    """)

    # --- GA4 source+medium aggregate with user counts ---
    op.execute("""
        CREATE TABLE ga4_traffic (
            date            DATE    NOT NULL,
            session_source  TEXT    NOT NULL DEFAULT '',
            session_medium  TEXT    NOT NULL DEFAULT '',
            sessions        INTEGER NOT NULL DEFAULT 0,
            total_users     INTEGER NOT NULL DEFAULT 0,
            active_users    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, session_source, session_medium)
        );
        CREATE INDEX idx_ga4_traffic_date ON ga4_traffic (date);
    """)

    # --- GA4 tracked conversion events by channel group ---
    op.execute("""
        CREATE TABLE ga4_events (
            date                          DATE    NOT NULL,
            event_name                    TEXT    NOT NULL,
            session_primary_channel_group TEXT    NOT NULL DEFAULT '',
            event_count                   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, event_name, session_primary_channel_group)
        );
        CREATE INDEX idx_ga4_events_date ON ga4_events (date);
        CREATE INDEX idx_ga4_events_name ON ga4_events (event_name);
    """)

    # --- Keyword rankings (SEMrush + synced GSC) ---
    op.execute("""
        CREATE TABLE keyword_rankings (
            keyword         TEXT             NOT NULL,
            date            DATE             NOT NULL,
            source          TEXT             NOT NULL DEFAULT 'semrush',
            rank            DOUBLE PRECISION,
            result_type     TEXT             NOT NULL DEFAULT '',
            landing_page    TEXT             NOT NULL DEFAULT '',
            search_volume   INTEGER,
            cpc             DOUBLE PRECISION,
            difficulty      DOUBLE PRECISION,
            tags            TEXT             NOT NULL DEFAULT '',
            intents         TEXT             NOT NULL DEFAULT '',
            product         TEXT             NOT NULL DEFAULT '',
            clicks          INTEGER,
            impressions     INTEGER,
            PRIMARY KEY (keyword, date, source)
        );
        CREATE INDEX idx_keywords_date ON keyword_rankings (date);
    """)

    # --- Keyword tier classification (populated from the performance sheet) ---
    op.execute("""
        CREATE TABLE keyword_tiers (
            keyword TEXT PRIMARY KEY,
            tier    TEXT NOT NULL CHECK (tier IN ('primary', 'secondary'))
        );
    """)

    # --- Profound GEO prompts with citations ---
    op.execute("""
        CREATE TABLE profound (
            date                DATE    NOT NULL,
            topic               TEXT    NOT NULL,
            prompt              TEXT    NOT NULL,
            platform            TEXT    NOT NULL,
            position            TEXT    NOT NULL DEFAULT '',
            mentioned           BOOLEAN NOT NULL DEFAULT FALSE,
            mentions            TEXT    NOT NULL DEFAULT '',
            normalized_mentions TEXT    NOT NULL DEFAULT '',
            citations           JSONB   NOT NULL DEFAULT '[]'::jsonb,
            response            TEXT    NOT NULL DEFAULT '',
            run_id              TEXT    NOT NULL DEFAULT '',
            platform_id         TEXT    NOT NULL DEFAULT '',
            tags                TEXT    NOT NULL DEFAULT '',
            region              TEXT    NOT NULL DEFAULT '',
            persona             TEXT    NOT NULL DEFAULT '',
            type                TEXT    NOT NULL DEFAULT '',
            search_queries      TEXT    NOT NULL DEFAULT '',
            PRIMARY KEY (date, topic, prompt, platform)
        );
        CREATE INDEX idx_profound_date ON profound (date);
        CREATE INDEX idx_profound_topic ON profound (topic);
        CREATE INDEX idx_profound_platform ON profound (platform);
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS profound")
    op.execute("DROP TABLE IF EXISTS keyword_tiers")
    op.execute("DROP TABLE IF EXISTS keyword_rankings")
    op.execute("DROP TABLE IF EXISTS ga4_events")
    op.execute("DROP TABLE IF EXISTS ga4_traffic")
    op.execute("DROP TABLE IF EXISTS ga4")
    op.execute("DROP TABLE IF EXISTS gsc_country")
    op.execute("DROP TABLE IF EXISTS gsc")
