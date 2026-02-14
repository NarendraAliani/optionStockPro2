"""
Form Validators
"""
from wtforms.validators import ValidationError
import re


class PasswordComplexity:
    """
    Validate password complexity requirements
    - Minimum 8 characters
    - At least one uppercase letter
    - At least one lowercase letter
    - At least one digit
    """
    
    def __init__(self, message=None):
        if not message:
            message = (
                'Password must be at least 8 characters and contain '
                'uppercase, lowercase, and numbers.'
            )
        self.message = message
    
    def __call__(self, form, field):
        password = field.data
        
        if len(password) < 8:
            raise ValidationError('Password must be at least 8 characters long.')
        
        if not re.search(r'[A-Z]', password):
            raise ValidationError('Password must contain at least one uppercase letter.')
        
        if not re.search(r'[a-z]', password):
            raise ValidationError('Password must contain at least one lowercase letter.')
        
        if not re.search(r'\d', password):
            raise ValidationError('Password must contain at least one number.')


class UsernameValidator:
    """
    Validate username format
    - Alphanumeric and underscores only
    - 3-50 characters
    """
    
    def __init__(self, message=None):
        if not message:
            message = 'Username must be 3-50 characters (letters, numbers, underscores only).'
        self.message = message
    
    def __call__(self, form, field):
        username = field.data
        
        if not re.match(r'^[a-zA-Z0-9_]{3,50}$', username):
            raise ValidationError(self.message)
