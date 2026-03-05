"""add telegram credentials to users

Revision ID: 2b8c4f0e1a2b
Revises: f3c2b1a9d8e7
Create Date: 2026-03-02 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2b8c4f0e1a2b'
down_revision = 'f3c2b1a9d8e7'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.add_column(sa.Column('telegram_bot_token', sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column('telegram_channel_id', sa.String(length=64), nullable=True))


def downgrade():
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.drop_column('telegram_channel_id')
        batch_op.drop_column('telegram_bot_token')
