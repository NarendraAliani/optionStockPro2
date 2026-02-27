"""add notification services toggle to users

Revision ID: f3c2b1a9d8e7
Revises: e4b1c2d9f3a7
Create Date: 2026-02-27 13:55:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f3c2b1a9d8e7'
down_revision = 'e4b1c2d9f3a7'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.add_column(sa.Column('notification_services_enabled', sa.Boolean(), nullable=True))

    op.execute("UPDATE users SET notification_services_enabled = 1 WHERE notification_services_enabled IS NULL")


def downgrade():
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.drop_column('notification_services_enabled')
