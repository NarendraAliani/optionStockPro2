"""Merge heads after min_signal_volume and spot_price changes.

Revision ID: 8a0f2c1d7b2e
Revises: 5f1c8a3d2b11, 4d7a1c9e2f10
Create Date: 2026-03-06
"""
from alembic import op  # noqa: F401
import sqlalchemy as sa  # noqa: F401


# revision identifiers, used by Alembic.
revision = '8a0f2c1d7b2e'
down_revision = ('5f1c8a3d2b11', '4d7a1c9e2f10')
branch_labels = None
depends_on = None


def upgrade():
    # Merge migration: no schema changes.
    pass


def downgrade():
    # Merge migration: no schema changes.
    pass
