"""add backtest rebalance and liquidity preferences

Revision ID: e4b1c2d9f3a7
Revises: f7e1b5c29a44
Create Date: 2026-02-16 16:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e4b1c2d9f3a7'
down_revision = 'f7e1b5c29a44'
branch_labels = None
depends_on = None


def _add_if_missing(table_name, column):
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns(table_name)}
    if column.name in columns:
        return
    with op.batch_alter_table(table_name, schema=None) as batch_op:
        batch_op.add_column(column)


def upgrade():
    _add_if_missing('users', sa.Column('backtest_rebalance_frequency', sa.String(length=20), nullable=True))
    _add_if_missing('users', sa.Column('backtest_strict_first_candle', sa.Boolean(), nullable=True))
    _add_if_missing('users', sa.Column('backtest_liquidity_filter_enabled', sa.Boolean(), nullable=True))
    _add_if_missing('users', sa.Column('backtest_min_candles_per_strike', sa.Integer(), nullable=True))
    _add_if_missing('users', sa.Column('backtest_min_avg_volume', sa.Integer(), nullable=True))

    op.execute("UPDATE users SET backtest_rebalance_frequency = 'weekly' WHERE backtest_rebalance_frequency IS NULL")
    op.execute("UPDATE users SET backtest_strict_first_candle = 1 WHERE backtest_strict_first_candle IS NULL")
    op.execute("UPDATE users SET backtest_liquidity_filter_enabled = 1 WHERE backtest_liquidity_filter_enabled IS NULL")
    op.execute("UPDATE users SET backtest_min_candles_per_strike = 5 WHERE backtest_min_candles_per_strike IS NULL")
    op.execute("UPDATE users SET backtest_min_avg_volume = 1 WHERE backtest_min_avg_volume IS NULL")

    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.alter_column('backtest_rebalance_frequency', existing_type=sa.String(length=20), nullable=False)
        batch_op.alter_column('backtest_strict_first_candle', existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column('backtest_liquidity_filter_enabled', existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column('backtest_min_candles_per_strike', existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column('backtest_min_avg_volume', existing_type=sa.Integer(), nullable=False)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}
    with op.batch_alter_table('users', schema=None) as batch_op:
        for name in (
            'backtest_min_avg_volume',
            'backtest_min_candles_per_strike',
            'backtest_liquidity_filter_enabled',
            'backtest_strict_first_candle',
            'backtest_rebalance_frequency'
        ):
            if name in columns:
                batch_op.drop_column(name)
