"""
Authentication Helper Functions
"""
from functools import wraps
from flask import redirect, url_for, flash
from flask_login import current_user


def login_required_with_api(f):
    """
    Decorator to require login AND API credentials
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login'))
        
        if not current_user.api_credential or not current_user.api_credential.is_validated:
            flash('Please configure your API credentials first.', 'warning')
            return redirect(url_for('settings.api_credentials'))
        
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """
    Decorator to require admin privileges (future use)
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login'))
        
        # Add admin check logic here when implementing roles
        # if not current_user.is_admin:
        #     flash('Admin access required.', 'danger')
        #     return redirect(url_for('dashboard.index'))
        
        return f(*args, **kwargs)
    return decorated_function
