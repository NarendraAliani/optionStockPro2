"""Add live candle comparison logic to scanner configs

Revision ID: c3b1f6d4d9a1
Revises: 081a3891dfe8
Create Date: 2026-03-18 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c3b1f6d4d9a1'
down_revision = '081a3891dfe8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'scanner_configs',
        sa.Column('live_candle_logic', sa.String(length=20), nullable=False, server_default='closed')
    )


def downgrade():
    op.drop_column('scanner_configs', 'live_candle_logic')
