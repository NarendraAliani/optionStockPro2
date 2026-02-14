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
