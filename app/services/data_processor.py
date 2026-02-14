"""
Data Processing Utilities
"""
import pandas as pd
from datetime import datetime, timedelta


class DataProcessor:
    """Utility functions for data transformation"""
    
    @staticmethod
    def get_next_expiry(symbol, expiry_type='weekly', reference_date=None):
        """
        Calculate next expiry date
        
        Args:
            symbol: Stock/Index symbol
            expiry_type: 'weekly' or 'monthly'
        
        Returns:
            datetime: Next expiry date (Thursday)
        """
        today = reference_date if reference_date else datetime.now()
        
        if expiry_type == 'weekly':
            # Find next Thursday
            days_ahead = 3 - today.weekday()  # Thursday is 3
            if days_ahead <= 0:
                days_ahead += 7
            return today + timedelta(days=days_ahead)
        
        else:  # monthly
            # Last Thursday of current month
            # Find last day of month
            if today.month == 12:
                last_day = datetime(today.year + 1, 1, 1) - timedelta(days=1)
            else:
                last_day = datetime(today.year, today.month + 1, 1) - timedelta(days=1)
            
            # Find last Thursday
            while last_day.weekday() != 3:  # Thursday is 3
                last_day -= timedelta(days=1)
            
            return last_day
    
    @staticmethod
    def is_index(symbol):
        """
        Check if symbol is an index
        
        Args:
            symbol: Symbol name
        
        Returns:
            bool: True if index
        """
        indices = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']
        return symbol.upper() in indices
    
    @staticmethod
    def get_expiry_type(symbol):
        """
        Get expiry type for symbol
        
        Args:
            symbol: Symbol name
        
        Returns:
            str: 'weekly' or 'monthly'
        """
        return 'weekly' if DataProcessor.is_index(symbol) else 'monthly'
    
    @staticmethod
    def format_option_symbol(underlying, expiry, strike, option_type):
        """
        Format option symbol for Angel One
        
        Args:
            underlying: Underlying symbol
            expiry: Expiry date
            strike: Strike price
            option_type: 'CE' or 'PE'
        
        Returns:
            str: Formatted option symbol
        """
        expiry_str = expiry.strftime('%d%b%y').upper()
        return f'{underlying}{expiry_str}{int(strike)}{option_type}'
