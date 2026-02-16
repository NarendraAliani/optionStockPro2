"""add runtime tuning preferences to users

Revision ID: c3f9a8b2d1e4
Revises: a4c8f2d19e6b
Create Date: 2026-02-16 13:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c3f9a8b2d1e4'
down_revision = 'a4c8f2d19e6b'
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
    _add_column_if_missing('users', sa.Column('live_workers_cap', sa.Integer(), nullable=True))
    _add_column_if_missing('users', sa.Column('backtest_workers_cap', sa.Integer(), nullable=True))
    _add_column_if_missing('users', sa.Column('scan_profile', sa.String(length=20), nullable=True))
    _add_column_if_missing('users', sa.Column('auto_tune_default', sa.Boolean(), nullable=True))
    _add_column_if_missing('users', sa.Column('live_prefilter_default', sa.Boolean(), nullable=True))
    _add_column_if_missing('users', sa.Column('live_prefilter_top_movers', sa.Integer(), nullable=True))
    _add_column_if_missing('users', sa.Column('live_prefilter_top_volume', sa.Integer(), nullable=True))
    _add_column_if_missing('users', sa.Column('live_prefilter_max_stocks', sa.Integer(), nullable=True))
    _add_column_if_missing('users', sa.Column('guardrail_enabled', sa.Boolean(), nullable=True))
    _add_column_if_missing('users', sa.Column('guardrail_load_threshold', sa.Integer(), nullable=True))

    op.execute("UPDATE users SET live_workers_cap = 8 WHERE live_workers_cap IS NULL")
    op.execute("UPDATE users SET backtest_workers_cap = 8 WHERE backtest_workers_cap IS NULL")
    op.execute("UPDATE users SET scan_profile = 'balanced' WHERE scan_profile IS NULL")
    op.execute("UPDATE users SET auto_tune_default = 1 WHERE auto_tune_default IS NULL")
    op.execute("UPDATE users SET live_prefilter_default = 1 WHERE live_prefilter_default IS NULL")
    op.execute("UPDATE users SET live_prefilter_top_movers = 30 WHERE live_prefilter_top_movers IS NULL")
    op.execute("UPDATE users SET live_prefilter_top_volume = 30 WHERE live_prefilter_top_volume IS NULL")
    op.execute("UPDATE users SET live_prefilter_max_stocks = 50 WHERE live_prefilter_max_stocks IS NULL")
    op.execute("UPDATE users SET guardrail_enabled = 1 WHERE guardrail_enabled IS NULL")
    op.execute("UPDATE users SET guardrail_load_threshold = 1200 WHERE guardrail_load_threshold IS NULL")

    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.alter_column('live_workers_cap', existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column('backtest_workers_cap', existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column('scan_profile', existing_type=sa.String(length=20), nullable=False)
        batch_op.alter_column('auto_tune_default', existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column('live_prefilter_default', existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column('live_prefilter_top_movers', existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column('live_prefilter_top_volume', existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column('live_prefilter_max_stocks', existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column('guardrail_enabled', existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column('guardrail_load_threshold', existing_type=sa.Integer(), nullable=False)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}
    with op.batch_alter_table('users', schema=None) as batch_op:
        for name in (
            'guardrail_load_threshold',
            'guardrail_enabled',
            'live_prefilter_max_stocks',
            'live_prefilter_top_volume',
            'live_prefilter_top_movers',
            'live_prefilter_default',
            'auto_tune_default',
            'scan_profile',
            'backtest_workers_cap',
            'live_workers_cap'
        ):
            if name in columns:
                batch_op.drop_column(name)
