"""
Scanner Configuration Model
"""
from app import db
from datetime import datetime
import json


class ScannerConfig(db.Model):
    """Scanner configuration presets"""
    __tablename__ = 'scanner_configs'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    config_name = db.Column(db.String(100), nullable=False)
    stock_selection = db.Column(db.JSON)  # ["NIFTY", "BANKNIFTY"] or "ALL"
    strike_range = db.Column(db.Integer, default=5)
    price_multiplier = db.Column(db.Numeric(5, 2), default=2.00)
    timeframe = db.Column(db.String(10), default='5min')
    refresh_interval = db.Column(db.Integer, default=300)  # seconds
    is_default = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    signals = db.relationship('Signal', backref='config', lazy='dynamic')
    
    def get_stock_selection(self):
        """Get stock selection as Python object"""
        if isinstance(self.stock_selection, str):
            # JSON column may return plain strings such as "all".
            # Try JSON decode first; if not JSON, treat as a literal selector.
            try:
                return json.loads(self.stock_selection)
            except (TypeError, ValueError):
                return self.stock_selection
        return self.stock_selection
    
    def set_stock_selection(self, stocks):
        """Set stock selection from Python object"""
        if isinstance(stocks, (list, dict)):
            self.stock_selection = stocks
        else:
            # Allow plain selectors like "all", "nifty50", "banknifty".
            self.stock_selection = stocks
    
    def __repr__(self):
        return f'<ScannerConfig {self.config_name}>'
