"""
Signal Detection Algorithm
"""
import logging

logger = logging.getLogger(__name__)


class SignalDetector:
    """Signal detection logic"""
    
    @staticmethod
    def detect_signal(option_data, config):
        """
        Detect if option price movement meets signal criteria
        
        Args:
            option_data: Dictionary with option OHLCV data
                {
                    'symbol': str,
                    'option_type': 'CE' or 'PE',
                    'strike_price': float,
                    'ltp': float (current price),
                    'previous_candle_close': float,
                    'volume': int,
                    'open_interest': int
                }
            config: ScannerConfig object with detection parameters
        
        Returns:
            dict: Signal data if detected, None otherwise
        """
        try:
            current_price = option_data['ltp']
            previous_close = option_data['previous_candle_close']

            if previous_close is None or current_price is None:
                return None
            if previous_close <= 0.20 or current_price <= 0.20:
                return None

            min_volume = 10
            try:
                min_volume = int(getattr(config, 'min_signal_volume', 10) or 10)
            except Exception:
                min_volume = 10
            volume = 0
            try:
                volume = int(option_data.get('volume', 0) or 0)
            except Exception:
                volume = 0
            if volume < max(0, min_volume):
                return None

            # Calculate price change percentage
            price_change = (current_price - previous_close) / previous_close
            price_change_percent = price_change * 100

            threshold = float(config.price_multiplier)
            if threshold <= 0:
                return None

            upper = previous_close * threshold

            # Only bullish multiplier breakout:
            # next candle close must be strictly greater than previous close * multiplier.
            if current_price > upper:
                return {
                    'symbol': option_data['symbol'],
                    'option_type': option_data['option_type'],
                    'strike_price': option_data['strike_price'],
                    'entry_price': previous_close,
                    'current_price': current_price,
                    'price_change_percent': price_change_percent,
                    'candle_open': option_data.get('candle_open'),
                    'candle_high': option_data.get('candle_high'),
                    'candle_low': option_data.get('candle_low'),
                    'candle_close': option_data.get('candle_close', current_price),
                    'volume': option_data.get('volume', 0),
                    'open_interest': option_data.get('open_interest', 0),
                    'rsi': option_data.get('rsi'),
                    'timeframe': config.timeframe
                }
            
            return None
        
        except Exception as e:
            logger.error(f'Error detecting signal: {str(e)}')
            return None
    
    @staticmethod
    def calculate_strike_range(spot_price, strike_range, strike_step=50):
        """
        Calculate strike prices in range
        
        Args:
            spot_price: Current spot price
            strike_range: Number of strikes above/below ATM
            strike_step: Strike price interval (default: 50)
        
        Returns:
            list: Strike prices
        """
        strike_count = int(strike_range or 0)
        if strike_count <= 0:
            return []

        # Round to nearest strike
        atm_strike = round(spot_price / strike_step) * strike_step

        candidates = []
        for i in range(-strike_count, strike_count + 1):
            candidates.append(atm_strike + (i * strike_step))

        candidates = sorted(
            set(candidates),
            key=lambda strike: (abs(strike - spot_price), strike)
        )
        # strike_range means strikes above/below ATM, so total = (2 * strike_count + 1).
        return candidates[: (strike_count * 2 + 1)]
