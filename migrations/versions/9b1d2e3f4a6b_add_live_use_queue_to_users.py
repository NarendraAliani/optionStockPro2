"""Add live_use_queue to users.

Revision ID: 9b1d2e3f4a6b
Revises: 8a0f2c1d7b2e
Create Date: 2026-03-06
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9b1d2e3f4a6b'
down_revision = '8a0f2c1d7b2e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'users',
        sa.Column('live_use_queue', sa.Boolean(), nullable=False, server_default=sa.text('0'))
    )
    op.alter_column('users', 'live_use_queue', server_default=None)


def downgrade():
    op.drop_column('users', 'live_use_queue')
