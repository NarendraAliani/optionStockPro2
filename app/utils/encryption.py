"""
Encryption Utilities for API Credentials
"""
from cryptography.fernet import Fernet
from flask import current_app
import base64
import hashlib


class EncryptionHelper:
    """Handle encryption/decryption of sensitive data"""
    
    @staticmethod
    def _get_cipher():
        """Get Fernet cipher from encryption key"""
        key = current_app.config['ENCRYPTION_KEY']
        # Ensure key is 32 bytes for Fernet
        key_bytes = hashlib.sha256(key.encode()).digest()
        key_b64 = base64.urlsafe_b64encode(key_bytes)
        return Fernet(key_b64)
    
    @staticmethod
    def encrypt(plaintext):
        """Encrypt plaintext string"""
        if not plaintext:
            return None
        cipher = EncryptionHelper._get_cipher()
        encrypted = cipher.encrypt(plaintext.encode())
        return encrypted.decode()
    
    @staticmethod
    def decrypt(ciphertext):
        """Decrypt ciphertext string"""
        if not ciphertext:
            return None
        cipher = EncryptionHelper._get_cipher()
        decrypted = cipher.decrypt(ciphertext.encode())
        return decrypted.decode()
