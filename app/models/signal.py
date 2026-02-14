"""
Signal Model
"""
from app import db
from datetime import datetime, timezone


class Signal(db.Model):
    """Detected option signals"""
    __tablename__ = 'signals'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    scanner_config_id = db.Column(db.Integer, db.ForeignKey('scanner_configs.id'))
    mode = db.Column(db.Enum('live', 'backtest'), nullable=False, index=True)
    symbol = db.Column(db.String(50), nullable=False, index=True)
    option_type = db.Column(db.Enum('CE', 'PE'), nullable=False)
    strike_price = db.Column(db.Numeric(10, 2), nullable=False)
    expiry_date = db.Column(db.Date, nullable=False)
    entry_price = db.Column(db.Numeric(10, 2), nullable=False)
    current_price = db.Column(db.Numeric(10, 2), nullable=False)
    price_change_percent = db.Column(db.Numeric(10, 2), nullable=False)
    volume = db.Column(db.BigInteger)
    open_interest = db.Column(db.BigInteger)
    rsi = db.Column(db.Numeric(10, 2))
    timeframe = db.Column(db.String(10))
    detected_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    
    def to_dict(self):
        """Convert signal to dictionary for JSON serialization"""
        detected_at = self.detected_at
        # Stored DB timestamps are UTC-naive; expose them as explicit UTC.
        if detected_at and detected_at.tzinfo is None:
            detected_at = detected_at.replace(tzinfo=timezone.utc)
        displayed_at = getattr(self, 'displayed_at', None)
        if displayed_at and displayed_at.tzinfo is None:
            displayed_at = displayed_at.replace(tzinfo=timezone.utc)
        return {
            'id': self.id,
            'mode': self.mode,
            'symbol': self.symbol,
            'option_type': self.option_type,
            'strike_price': float(self.strike_price),
            'expiry_date': self.expiry_date.isoformat(),
            'entry_price': float(self.entry_price),
            'current_price': float(self.current_price),
            'price_change_percent': float(self.price_change_percent),
            'candle_open': float(getattr(self, 'candle_open', None)) if getattr(self, 'candle_open', None) is not None else None,
            'candle_high': float(getattr(self, 'candle_high', None)) if getattr(self, 'candle_high', None) is not None else None,
            'candle_low': float(getattr(self, 'candle_low', None)) if getattr(self, 'candle_low', None) is not None else None,
            'candle_close': float(getattr(self, 'candle_close', None)) if getattr(self, 'candle_close', None) is not None else None,
            'volume': self.volume,
            'open_interest': self.open_interest,
            'rsi': float(self.rsi) if self.rsi is not None else None,
            'timeframe': self.timeframe,
            'detected_at': detected_at.isoformat().replace('+00:00', 'Z') if detected_at else None,
            'displayed_at': displayed_at.isoformat().replace('+00:00', 'Z') if displayed_at else None
        }
    
    def __repr__(self):
        return f'<Signal {self.symbol} {self.option_type} {self.strike_price}>'
