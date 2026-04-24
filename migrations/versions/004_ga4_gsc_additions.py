"""GA4 and GSC schema additions.

New tables and columns added on top of the initial GSC schema (003):
  - ga4: engagement columns (engaged_sessions, engagement_duration_s, new_users, exits, conversions)
  - ga4_traffic: engagement columns (engaged_sessions, new_users, engagement_duration_s)
  - ga4_landing_pages: sessions by landing page × source × medium
  - ga4_page_events: event counts by page and event name
  - ga4_category_sessions: weekly session counts by page category × source × medium
  - ga4_traffic_weekly: weekly user acquisition by first-user-source (avoids privacy-threshold drops)
  - ga4_page_before_conversion: page visited immediately before a conversion event
  - gsc_non_indexed: weekly snapshots of sitemap pages absent from GSC results
  - gsc_coverage_daily: daily indexed/not-indexed counts from GSC Coverage Chart
  - gsc_coverage_reasons: not-indexed reason breakdown snapshots
  - gsc_coverage_urls: individual URLs per not-indexed reason

Revision ID: 004
Revises: 003
"""

from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE ga4
            ADD COLUMN IF NOT EXISTS engaged_sessions       INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS engagement_duration_s  INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS new_users              INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS exits                  INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS conversions            INTEGER NOT NULL DEFAULT 0
    """)

    op.execute("""
        ALTER TABLE ga4_traffic
            ADD COLUMN IF NOT EXISTS engaged_sessions       INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS new_users              INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS engagement_duration_s  INTEGER NOT NULL DEFAULT 0
    """)

    op.execute("""
        CREATE TABLE ga4_landing_pages (
            date                    DATE    NOT NULL,
            landing_page            TEXT    NOT NULL,
            session_source          TEXT    NOT NULL DEFAULT '',
            session_medium          TEXT    NOT NULL DEFAULT '',
            sessions                INTEGER NOT NULL DEFAULT 0,
            engaged_sessions        INTEGER NOT NULL DEFAULT 0,
            new_users               INTEGER NOT NULL DEFAULT 0,
            engagement_duration_s   INTEGER NOT NULL DEFAULT 0,
            conversions             INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, landing_page, session_source, session_medium)
        );
        CREATE INDEX idx_ga4_landing_pages_date ON ga4_landing_pages (date);
    """)

    op.execute("""
        CREATE TABLE ga4_page_events (
            date        DATE    NOT NULL,
            page_path   TEXT    NOT NULL,
            event_name  TEXT    NOT NULL,
            event_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, page_path, event_name)
        );
        CREATE INDEX idx_ga4_page_events_date  ON ga4_page_events (date);
        CREATE INDEX idx_ga4_page_events_event ON ga4_page_events (event_name);
    """)

    op.execute("""
        CREATE TABLE ga4_category_sessions (
            date            DATE    NOT NULL,
            page_category   TEXT    NOT NULL,
            session_source  TEXT    NOT NULL DEFAULT '',
            session_medium  TEXT    NOT NULL DEFAULT '',
            sessions        INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, page_category, session_source, session_medium)
        );
        CREATE INDEX idx_ga4_category_sessions_date ON ga4_category_sessions (date);
    """)

    op.execute("""
        CREATE TABLE ga4_traffic_weekly (
            date               DATE    NOT NULL,
            first_user_source  TEXT    NOT NULL DEFAULT '',
            first_user_medium  TEXT    NOT NULL DEFAULT '',
            total_users        INTEGER NOT NULL DEFAULT 0,
            new_users          INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, first_user_source, first_user_medium)
        );
        CREATE INDEX idx_ga4_traffic_weekly_date ON ga4_traffic_weekly (date);
    """)

    op.execute("""
        CREATE TABLE ga4_page_before_conversion (
            date                DATE    NOT NULL,
            previous_page_path  TEXT    NOT NULL,
            event_name          TEXT    NOT NULL,
            event_count         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, previous_page_path, event_name)
        );
        CREATE INDEX idx_ga4_pbc_date ON ga4_page_before_conversion (date);
    """)

    op.execute("""
        CREATE TABLE gsc_non_indexed (
            id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            week_start DATE NOT NULL,
            page_url   TEXT NOT NULL
        );
        CREATE UNIQUE INDEX idx_gsc_non_indexed_natural ON gsc_non_indexed (week_start, md5(page_url));
        CREATE INDEX idx_gsc_non_indexed_week ON gsc_non_indexed (week_start);
    """)

    op.execute("""
        CREATE TABLE gsc_coverage_daily (
            date        DATE    PRIMARY KEY,
            indexed     INTEGER NOT NULL DEFAULT 0,
            not_indexed INTEGER NOT NULL DEFAULT 0,
            impressions INTEGER NOT NULL DEFAULT 0
        );
    """)

    op.execute("""
        CREATE TABLE gsc_coverage_reasons (
            as_of_date DATE NOT NULL,
            reason     TEXT NOT NULL,
            source     TEXT NOT NULL DEFAULT '',
            validation TEXT NOT NULL DEFAULT '',
            pages      INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (as_of_date, reason)
        );
        CREATE INDEX idx_gsc_coverage_reasons_date ON gsc_coverage_reasons (as_of_date);
    """)

    op.execute("""
        CREATE TABLE gsc_coverage_urls (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            as_of_date  DATE NOT NULL,
            reason      TEXT NOT NULL,
            url         TEXT NOT NULL,
            last_crawled DATE
        );
        CREATE UNIQUE INDEX idx_gsc_coverage_urls_natural
            ON gsc_coverage_urls (as_of_date, reason, md5(url));
        CREATE INDEX idx_gsc_coverage_urls_date   ON gsc_coverage_urls (as_of_date);
        CREATE INDEX idx_gsc_coverage_urls_reason ON gsc_coverage_urls (reason);
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS gsc_coverage_urls;")
    op.execute("DROP TABLE IF EXISTS gsc_coverage_reasons;")
    op.execute("DROP TABLE IF EXISTS gsc_coverage_daily;")
    op.execute("DROP TABLE IF EXISTS gsc_non_indexed;")
    op.execute("DROP TABLE IF EXISTS ga4_page_before_conversion;")
    op.execute("DROP TABLE IF EXISTS ga4_traffic_weekly;")
    op.execute("DROP TABLE IF EXISTS ga4_category_sessions;")
    op.execute("DROP TABLE IF EXISTS ga4_page_events;")
    op.execute("DROP TABLE IF EXISTS ga4_landing_pages;")
    op.execute("""
        ALTER TABLE ga4_traffic
            DROP COLUMN IF EXISTS engaged_sessions,
            DROP COLUMN IF EXISTS new_users,
            DROP COLUMN IF EXISTS engagement_duration_s
    """)
    op.execute("""
        ALTER TABLE ga4
            DROP COLUMN IF EXISTS engaged_sessions,
            DROP COLUMN IF EXISTS engagement_duration_s,
            DROP COLUMN IF EXISTS new_users,
            DROP COLUMN IF EXISTS exits,
            DROP COLUMN IF EXISTS conversions
    """)
