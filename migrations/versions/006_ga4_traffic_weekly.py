"""Weekly user acquisition aggregates by first-user-source for accurate user counts.

Revision ID: 006
Revises: 005
Create Date: 2026-04-23
"""

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade():
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


def downgrade():
    op.execute("DROP TABLE IF EXISTS ga4_traffic_weekly")
