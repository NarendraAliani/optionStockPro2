"""
API Credential Model
"""
from app import db
from datetime import datetime


class APICredential(db.Model):
    """Angel One API credentials (encrypted)"""
    __tablename__ = 'api_credentials'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, unique=True)
    api_key_encrypted = db.Column(db.Text, nullable=True)
    api_key_market_encrypted = db.Column(db.Text, nullable=True)
    api_key_historical_encrypted = db.Column(db.Text, nullable=True)
    use_mpin = db.Column(db.Boolean, default=False)
    auth_token_encrypted = db.Column(db.Text, nullable=True)
    refresh_token_encrypted = db.Column(db.Text, nullable=True)
    feed_token_encrypted = db.Column(db.Text, nullable=True)
    token_expires_at = db.Column(db.DateTime, nullable=True)
    client_id_encrypted = db.Column(db.Text, nullable=False)
    client_secret_encrypted = db.Column(db.Text, nullable=False)
    totp_secret_encrypted = db.Column(db.Text)
    is_validated = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<APICredential user_id={self.user_id}>'
