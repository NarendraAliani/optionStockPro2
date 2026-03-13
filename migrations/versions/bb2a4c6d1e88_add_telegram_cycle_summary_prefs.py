"""add telegram cycle summary preferences

Revision ID: bb2a4c6d1e88
Revises: aa1f3c2d9b77
Create Date: 2026-03-13 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bb2a4c6d1e88'
down_revision = 'aa1f3c2d9b77'
branch_labels = None
depends_on = None


def _add_column_if_missing(table_name, column):
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns(table_name)}
    if column.name in columns:
        return
    with op.batch_alter_table(table_name, schema=None) as batch_op:
        batch_op.add_column(column)


def upgrade():
    _add_column_if_missing('users', sa.Column('telegram_cycle_summary_enabled', sa.Boolean(), nullable=True))
    _add_column_if_missing('users', sa.Column('telegram_cycle_summary_mode', sa.String(length=20), nullable=True))
    _add_column_if_missing('users', sa.Column('telegram_cycle_summary_channel_id', sa.String(length=64), nullable=True))

    op.execute("UPDATE users SET telegram_cycle_summary_enabled = 1 WHERE telegram_cycle_summary_enabled IS NULL")
    op.execute("UPDATE users SET telegram_cycle_summary_mode = 'short' WHERE telegram_cycle_summary_mode IS NULL")

    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.alter_column('telegram_cycle_summary_enabled', existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column('telegram_cycle_summary_mode', existing_type=sa.String(length=20), nullable=False)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}
    with op.batch_alter_table('users', schema=None) as batch_op:
        for name in (
            'telegram_cycle_summary_channel_id',
            'telegram_cycle_summary_mode',
            'telegram_cycle_summary_enabled'
        ):
            if name in columns:
                batch_op.drop_column(name)
