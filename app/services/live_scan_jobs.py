"""
RQ job helpers for live scan symbol processing.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple

from app import create_app
from app.models.api_credential import APICredential
from app.models.user import User
from app.services.angel_api import AngelOneAPI
from app.services.signal_detector import SignalDetector
from app.services.queue_manager import is_live_stop_requested
from app.utils.encryption import EncryptionHelper


class _DummyConfig:
    def __init__(self, price_multiplier: float, timeframe: int) -> None:
        self.price_multiplier = price_multiplier
        self.timeframe = timeframe


def _build_live_cycle_log_row(symbol: str, strike: Any, option_type: str, option_snapshot: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = option_snapshot or {}
    candle_time = snapshot.get('candle_time')
    if isinstance(candle_time, datetime):
        candle_time = candle_time.isoformat()
    return {
        'Current Candle Time': candle_time or '',
        'Stock': str(snapshot.get('symbol') or symbol or ''),
        'Strike': snapshot.get('strike_price', strike),
        'Type': str(snapshot.get('option_type') or option_type or ''),
        'Prev Close': snapshot.get('previous_candle_close', ''),
        'Current Close': snapshot.get('current_candle_close', ''),
        'Open': snapshot.get('current_candle_open', ''),
        'High': snapshot.get('current_candle_high', ''),
        'Low': snapshot.get('current_candle_low', ''),
        'Close': snapshot.get('current_candle_close', '')
    }


def scan_live_symbol_job(payload: Dict[str, Any]) -> Tuple[list, int, int, list]:
    """
    Job payload keys:
      user_id, symbol, expiry, strikes_by_type, timeframe, price_multiplier, spot_price
    """
    app = create_app()
    with app.app_context():
        user_id = int(payload.get('user_id') or 0)
        if is_live_stop_requested(user_id):
            return [], 0, 0

        user = User.query.get(user_id)
        if not user:
            return [], 0, 0

        creds = APICredential.query.filter_by(user_id=user_id).first()
        if not creds:
            return [], 0, 0

        api_key_market = EncryptionHelper.decrypt(creds.api_key_market_encrypted) if getattr(creds, 'api_key_market_encrypted', None) else None
        api_key_legacy = EncryptionHelper.decrypt(creds.api_key_encrypted) if creds.api_key_encrypted else None
        api_key = api_key_market or api_key_legacy
        client_id = EncryptionHelper.decrypt(creds.client_id_encrypted)
        client_secret = EncryptionHelper.decrypt(creds.client_secret_encrypted)
        totp_secret = EncryptionHelper.decrypt(creds.totp_secret_encrypted) if creds.totp_secret_encrypted else None

        angel_api = AngelOneAPI(
            api_key=api_key,
            client_id=client_id,
            password=client_secret,
            totp_secret=totp_secret,
            totp_code=None,
            use_mpin=bool(creds.use_mpin)
        )

        if getattr(creds, 'auth_token_encrypted', None):
            auth_token = EncryptionHelper.decrypt(creds.auth_token_encrypted) if creds.auth_token_encrypted else None
            refresh_token = EncryptionHelper.decrypt(creds.refresh_token_encrypted) if creds.refresh_token_encrypted else None
            feed_token = EncryptionHelper.decrypt(creds.feed_token_encrypted) if creds.feed_token_encrypted else None
            if auth_token:
                angel_api.attach_tokens(auth_token, refresh_token, feed_token)

        if not angel_api.load_scrip_master():
            return [], 0, 0

        symbol = str(payload.get('symbol') or '').strip().upper()
        strikes_by_type = payload.get('strikes_by_type') or {}
        expiry_raw = payload.get('expiry')
        expiry = expiry_raw
        if isinstance(expiry_raw, str):
            try:
                expiry = datetime.fromisoformat(expiry_raw).date()
            except Exception:
                expiry = expiry_raw
        timeframe = int(payload.get('timeframe') or 5)
        price_multiplier = float(payload.get('price_multiplier') or 2.0)
        spot_price = payload.get('spot_price')

        config = _DummyConfig(price_multiplier=price_multiplier, timeframe=timeframe)
        strikes_processed = 0
        skipped = 0
        detected = []
        cycle_rows = []

        for option_type in ('CE', 'PE'):
            for strike in strikes_by_type.get(option_type, []):
                if is_live_stop_requested(user_id):
                    return detected, strikes_processed, skipped, cycle_rows
                strikes_processed += 1
                option_snapshot = angel_api.get_recent_option_candle_pair(
                    underlying=symbol,
                    expiry=expiry,
                    strike=strike,
                    option_type=option_type,
                    timeframe=timeframe,
                    exchange='NFO'
                )
                if not option_snapshot:
                    skipped += 1
                    cycle_rows.append(_build_live_cycle_log_row(symbol, strike, option_type, None))
                    continue

                signal_payload = {
                    'symbol': option_snapshot['symbol'],
                    'option_type': option_snapshot['option_type'],
                    'strike_price': option_snapshot['strike_price'],
                    'ltp': option_snapshot['current_candle_close'],
                    'previous_candle_close': option_snapshot['previous_candle_close'],
                    'candle_open': option_snapshot.get('current_candle_open'),
                    'candle_high': option_snapshot.get('current_candle_high'),
                    'candle_low': option_snapshot.get('current_candle_low'),
                    'candle_close': option_snapshot.get('current_candle_close'),
                    'volume': option_snapshot.get('volume', 0),
                    'open_interest': option_snapshot.get('open_interest', 0),
                    'rsi': option_snapshot.get('rsi')
                }
                cycle_rows.append(_build_live_cycle_log_row(symbol, strike, option_type, option_snapshot))
                signal = SignalDetector.detect_signal(signal_payload, config)
                if not signal:
                    continue
                signal['expiry_date'] = expiry
                signal['detected_at'] = option_snapshot.get('candle_time') or datetime.utcnow()
                signal['displayed_at'] = datetime.utcnow()
                if spot_price is not None:
                    signal['spot_price'] = float(spot_price)
                detected.append(signal)

        return detected, strikes_processed, skipped, cycle_rows
