"""
Signal Detail Model
"""
from app import db


class SignalDetail(db.Model):
    """Extended per-signal candle/detail data."""
    __tablename__ = 'signal_details'

    id = db.Column(db.Integer, primary_key=True)
    signal_id = db.Column(
        db.Integer,
        db.ForeignKey('signals.id', ondelete='CASCADE'),
        nullable=False,
        unique=True,
        index=True
    )
    candle_open = db.Column(db.Numeric(10, 2))
    candle_high = db.Column(db.Numeric(10, 2))
    candle_low = db.Column(db.Numeric(10, 2))
    candle_close = db.Column(db.Numeric(10, 2))
    displayed_at = db.Column(db.DateTime)
