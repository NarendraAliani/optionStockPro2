"""Add spot_price to signals.

Revision ID: 4d7a1c9e2f10
Revises: 3c9e1b2c4d5e
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4d7a1c9e2f10'
down_revision = '3c9e1b2c4d5e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'signals',
        sa.Column('spot_price', sa.Numeric(10, 2), nullable=True)
    )


def downgrade():
    op.drop_column('signals', 'spot_price')
