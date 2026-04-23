"""GA4 tables: engagement columns, landing pages, page events, exits, conversions.

Revision ID: 004
Revises: 003
Create Date: 2026-04-23
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
        CREATE INDEX idx_ga4_page_events_date ON ga4_page_events (date);
        CREATE INDEX idx_ga4_page_events_event ON ga4_page_events (event_name);
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS ga4_page_events")
    op.execute("DROP TABLE IF EXISTS ga4_landing_pages")
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
