"""add notification sound preferences to users

Revision ID: 1b2d4e6f8a10
Revises: 9a7c4b21d5ef
Create Date: 2026-02-13 22:15:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1b2d4e6f8a10'
down_revision = '9a7c4b21d5ef'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.add_column(sa.Column('notification_sound_enabled', sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column('notification_sound_source', sa.String(length=20), nullable=True))
        batch_op.add_column(sa.Column('notification_sound_data', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('notification_sound_filename', sa.String(length=255), nullable=True))

    op.execute("UPDATE users SET notification_sound_enabled = 1 WHERE notification_sound_enabled IS NULL")
    op.execute("UPDATE users SET notification_sound_source = 'beep' WHERE notification_sound_source IS NULL")


def downgrade():
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.drop_column('notification_sound_filename')
        batch_op.drop_column('notification_sound_data')
        batch_op.drop_column('notification_sound_source')
        batch_op.drop_column('notification_sound_enabled')
