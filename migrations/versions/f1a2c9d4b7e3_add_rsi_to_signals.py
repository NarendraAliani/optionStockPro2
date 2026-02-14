"""add rsi to signals

Revision ID: f1a2c9d4b7e3
Revises: 0c7b2f2f4521
Create Date: 2026-02-09 12:05:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f1a2c9d4b7e3'
down_revision = '0c7b2f2f4521'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('signals', schema=None) as batch_op:
        batch_op.add_column(sa.Column('rsi', sa.Numeric(precision=10, scale=2), nullable=True))


def downgrade():
    with op.batch_alter_table('signals', schema=None) as batch_op:
        batch_op.drop_column('rsi')

