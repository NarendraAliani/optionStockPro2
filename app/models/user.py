"""
User Model
"""
from app import db
from flask_login import UserMixin
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash


class User(UserMixin, db.Model):
    """User account model"""
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False, index=True)
    email = db.Column(db.String(100), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    theme_preference = db.Column(db.Enum('light', 'dark'), default='light')
    scan_workers = db.Column(db.Integer, nullable=False, default=8)
    live_workers_cap = db.Column(db.Integer, nullable=False, default=8)
    backtest_workers_cap = db.Column(db.Integer, nullable=False, default=8)
    scan_profile = db.Column(db.String(20), nullable=False, default='balanced')
    auto_tune_default = db.Column(db.Boolean, nullable=False, default=True)
    live_prefilter_default = db.Column(db.Boolean, nullable=False, default=True)
    live_prefilter_top_movers = db.Column(db.Integer, nullable=False, default=30)
    live_prefilter_top_volume = db.Column(db.Integer, nullable=False, default=30)
    live_prefilter_max_stocks = db.Column(db.Integer, nullable=False, default=50)
    backtest_rebalance_frequency = db.Column(db.String(20), nullable=False, default='weekly')
    backtest_strict_first_candle = db.Column(db.Boolean, nullable=False, default=True)
    backtest_liquidity_filter_enabled = db.Column(db.Boolean, nullable=False, default=True)
    backtest_min_candles_per_strike = db.Column(db.Integer, nullable=False, default=5)
    backtest_min_avg_volume = db.Column(db.Integer, nullable=False, default=1)
    guardrail_enabled = db.Column(db.Boolean, nullable=False, default=True)
    guardrail_load_threshold = db.Column(db.Integer, nullable=False, default=1200)
    backtest_scan_log_enabled = db.Column(db.Boolean, nullable=False, default=False)
    api_error_log_enabled = db.Column(db.Boolean, nullable=False, default=False)
    notification_services_enabled = db.Column(db.Boolean, nullable=False, default=True)
    notification_sound_enabled = db.Column(db.Boolean, default=True)
    notification_sound_source = db.Column(db.String(20), default='beep')
    notification_sound_data = db.Column(db.Text)
    notification_sound_filename = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    api_credential = db.relationship('APICredential', backref='user', uselist=False, cascade='all, delete-orphan')
    scanner_configs = db.relationship('ScannerConfig', backref='user', lazy='dynamic', cascade='all, delete-orphan')
    signals = db.relationship('Signal', backref='user', lazy='dynamic', cascade='all, delete-orphan')
    sessions = db.relationship('UserSession', backref='user', lazy='dynamic', cascade='all, delete-orphan')
    
    def set_password(self, password):
        """Hash and set password"""
        self.password_hash = generate_password_hash(password, method='pbkdf2:sha256')
    
    def check_password(self, password):
        """Verify password"""
        return check_password_hash(self.password_hash, password)
    
    def __repr__(self):
        return f'<User {self.username}>'
