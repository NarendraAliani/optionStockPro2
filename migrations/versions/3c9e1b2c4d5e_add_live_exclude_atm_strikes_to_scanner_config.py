"""Add live_exclude_atm_strikes to scanner_configs.

Revision ID: 3c9e1b2c4d5e
Revises: 2b8c4f0e1a2b
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3c9e1b2c4d5e'
down_revision = '2b8c4f0e1a2b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'scanner_configs',
        sa.Column('live_exclude_atm_strikes', sa.Integer(), nullable=False, server_default='0')
    )
    op.alter_column('scanner_configs', 'live_exclude_atm_strikes', server_default=None)


def downgrade():
    op.drop_column('scanner_configs', 'live_exclude_atm_strikes')
