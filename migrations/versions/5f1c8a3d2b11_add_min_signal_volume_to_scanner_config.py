"""Add min_signal_volume to scanner_configs.

Revision ID: 5f1c8a3d2b11
Revises: 3c9e1b2c4d5e
Create Date: 2026-03-06
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5f1c8a3d2b11'
down_revision = '3c9e1b2c4d5e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'scanner_configs',
        sa.Column('min_signal_volume', sa.Integer(), nullable=False, server_default='10')
    )
    op.alter_column('scanner_configs', 'min_signal_volume', server_default=None)


def downgrade():
    op.drop_column('scanner_configs', 'min_signal_volume')
