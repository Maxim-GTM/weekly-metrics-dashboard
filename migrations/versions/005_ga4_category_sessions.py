"""GA4 category-level session counts with source and medium dimensions.

Revision ID: 005
Revises: 004
Create Date: 2026-04-23
"""

from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade():
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


def downgrade():
    op.execute("DROP TABLE IF EXISTS ga4_category_sessions")
