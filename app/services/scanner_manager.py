"""
Scanner Manager for live and backtest execution
"""
from datetime import datetime, timedelta
from threading import Lock
from threading import Thread
from app import socketio, db
from app.models.api_credential import APICredential
from app.models.scanner_config import ScannerConfig
from app.models.signal import Signal
from app.services.angel_api import AngelOneAPI
from app.services.scanner_engine import ScannerEngine
from app.utils.encryption import EncryptionHelper
import logging

logger = logging.getLogger(__name__)

_lock = Lock()
_live_scanners = {}
_scanner_status = {}
_backtest_status = {}
_backtest_jobs = {}


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _get_user_default_config_payload(user_id):
    cfg = ScannerConfig.query.filter_by(user_id=user_id, is_default=True).first()
    if not cfg:
        return {}
    timeframe_text = str(cfg.timeframe or '5')
    return {
        'stockSelection': cfg.get_stock_selection() or 'all',
        'strikeRange': int(cfg.strike_range or 5),
        'priceMultiplier': float(cfg.price_multiplier or 2.0),
        'timeframe': int(timeframe_text.replace('min', '') or 5),
        'refreshInterval': max(1, int((cfg.refresh_interval or 300) / 60))
    }


def _pick_config_value(config_data, defaults, key, fallback):
    source = config_data if isinstance(config_data, dict) else {}
    value = source.get(key)
    if value is None or value == '':
        value = defaults.get(key, fallback)
    return value


def _build_scanner_config(user_id, config_data, mode):
    user_defaults = _get_user_default_config_payload(user_id)
    stock_selection = _pick_config_value(config_data, user_defaults, 'stockSelection', 'all')
    default_strike_range = 2 if str(mode or '').lower() == 'live' else 5
    strike_range = int(_pick_config_value(config_data, user_defaults, 'strikeRange', default_strike_range))
    price_multiplier = float(_pick_config_value(config_data, user_defaults, 'priceMultiplier', 2.0))
    timeframe = int(_pick_config_value(config_data, user_defaults, 'timeframe', 5))
    allowed_timeframes = {1, 3, 5, 15, 30, 60}
    if timeframe not in allowed_timeframes:
        timeframe = 5
    refresh_interval = int(_pick_config_value(config_data, user_defaults, 'refreshInterval', 5)) * 60

    config_name = f'{mode.capitalize()} {datetime.utcnow().strftime("%Y%m%d-%H%M%S")}'

    config = ScannerConfig(
        user_id=user_id,
        config_name=config_name,
        stock_selection=stock_selection,
        strike_range=strike_range,
        price_multiplier=price_multiplier,
        timeframe=f'{timeframe}min',
        refresh_interval=refresh_interval,
        is_default=False
    )
    db.session.add(config)
    db.session.commit()
    return config


def _next_token_expiry(now=None):
    current = now or datetime.now()
    expiry = datetime(current.year, current.month, current.day, 5, 0, 0)
    if current >= expiry:
        expiry = expiry + timedelta(days=1)
    return expiry


def _tokens_valid(creds, margin_seconds=120):
    if not creds.token_expires_at:
        return False
    return datetime.now() < (creds.token_expires_at - timedelta(seconds=margin_seconds))


def _store_tokens(creds, angel_api):
    try:
        if angel_api.auth_token:
            creds.auth_token_encrypted = EncryptionHelper.encrypt(angel_api.auth_token)
        if angel_api.refresh_token:
            creds.refresh_token_encrypted = EncryptionHelper.encrypt(angel_api.refresh_token)
        if angel_api.feed_token:
            creds.feed_token_encrypted = EncryptionHelper.encrypt(angel_api.feed_token)
        creds.token_expires_at = _next_token_expiry()
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f'Failed to store tokens: {str(e)}')


def _build_angel_api(creds, mode='live', totp_code=None):
    api_key_market = EncryptionHelper.decrypt(creds.api_key_market_encrypted) if getattr(creds, 'api_key_market_encrypted', None) else None
    api_key_historical = EncryptionHelper.decrypt(creds.api_key_historical_encrypted) if getattr(creds, 'api_key_historical_encrypted', None) else None
    api_key_legacy = EncryptionHelper.decrypt(creds.api_key_encrypted) if creds.api_key_encrypted else None

    if mode == 'backtest':
        api_key = api_key_historical or api_key_market or api_key_legacy
    else:
        api_key = api_key_market or api_key_legacy or api_key_historical

    client_id = EncryptionHelper.decrypt(creds.client_id_encrypted)
    client_secret = EncryptionHelper.decrypt(creds.client_secret_encrypted)
    totp_secret = EncryptionHelper.decrypt(creds.totp_secret_encrypted) if creds.totp_secret_encrypted else None

    angel_api = AngelOneAPI(
        api_key=api_key,
        client_id=client_id,
        password=client_secret,
        totp_secret=totp_secret,
        totp_code=totp_code,
        use_mpin=bool(creds.use_mpin)
    )

    if not api_key:
        return None, 'API key is missing for the selected mode.'

    if _tokens_valid(creds) and not totp_code:
        auth_token = EncryptionHelper.decrypt(creds.auth_token_encrypted) if creds.auth_token_encrypted else None
        refresh_token = EncryptionHelper.decrypt(creds.refresh_token_encrypted) if creds.refresh_token_encrypted else None
        feed_token = EncryptionHelper.decrypt(creds.feed_token_encrypted) if creds.feed_token_encrypted else None
        if auth_token:
            angel_api.attach_tokens(auth_token, refresh_token, feed_token)
            return angel_api, None

    if not angel_api.authenticate():
        return None, angel_api.last_error or 'Failed to authenticate with Angel One.'

    return angel_api, None


def connect_api_session(app, user_id, mode='backtest', totp_code=None):
    """
    Authenticate and cache Angel One session tokens without starting scan/backtest.

    This supports a two-step flow:
      1) Connect with TOTP
      2) Start fetch/run using cached token
    """
    with app.app_context():
        creds = APICredential.query.filter_by(user_id=user_id).first()
        if not creds:
            return False, 'API credentials not configured.'

        target_mode = (mode or 'backtest').strip().lower()
        if target_mode not in ('live', 'backtest'):
            target_mode = 'backtest'

        angel_api, error = _build_angel_api(creds, mode=target_mode, totp_code=totp_code)
        if not angel_api:
            return False, f'Failed to authenticate with Angel One. {error}'

        _store_tokens(creds, angel_api)
        return True, f'Connected successfully for {target_mode} mode.'


def start_live_scan(app, user_id, config_data):
    with app.app_context():
        with _lock:
            existing = _live_scanners.get(user_id)
            if existing and existing.is_running:
                return False, 'Scanner already running.'

        creds = APICredential.query.filter_by(user_id=user_id).first()
        if not creds:
            return False, 'API credentials not configured.'

        totp_code = (config_data or {}).get('totpCode')
        angel_api, error = _build_angel_api(creds, mode='live', totp_code=totp_code)
        if not angel_api:
            return False, f'Failed to authenticate with Angel One. {error}'
        _store_tokens(creds, angel_api)

        if not angel_api.load_scrip_master():
            return False, 'Scrip master not available. Configure ANGEL_SCRIP_MASTER_PATH or URL.'

        config = _build_scanner_config(user_id, config_data, mode='live')
        engine = ScannerEngine(user_id=user_id, config=config, angel_api=angel_api)
        engine.is_running = True
        selected_expiries = engine.get_selected_expiries(reference_date=datetime.now())
        scripts_total = int(len(selected_expiries or {}))
        engine.last_cycle_stocks_total = scripts_total

        with _lock:
            _live_scanners[user_id] = engine
            _scanner_status[user_id] = {
                'running': True,
                'config_id': config.id,
                'started_at': datetime.utcnow().isoformat() + 'Z',
                'stopped_at': None,
                'last_run_at': None,
                'last_error': None,
                'last_signals': 0,
                'last_stocks_scanned': 0,
                'scripts_total': scripts_total,
                'last_strikes_scanned': 0,
                'strikes_total': 0,
                'timeframe': config.timeframe,
                'price_multiplier': float(config.price_multiplier),
                'selected_expiries': selected_expiries
            }

        Thread(
            target=_run_live_loop,
            args=(app, user_id),
            daemon=True,
            name=f'live-scan-{user_id}'
        ).start()

    return True, 'Live scanner started successfully.'


def _run_live_loop(app, user_id):
    with app.app_context():
        engine = _live_scanners.get(user_id)
        if not engine:
            return
        try:
            engine.run_live_loop(_update_live_status)
        except Exception as e:
            _update_live_status(user_id, error=str(e))


def stop_live_scan(user_id):
    with _lock:
        engine = _live_scanners.get(user_id)
        if not engine:
            return False, 'No running scanner found.'
        engine.stop_live_scan()
        _live_scanners.pop(user_id, None)
        status = _scanner_status.get(user_id)
        if status:
            status['running'] = False
            status['stopped_at'] = datetime.utcnow().isoformat() + 'Z'
    return True, 'Scanner stopped.'


def start_backtest(app, user_id, config_data):
    config_data = config_data or {}
    from_date = config_data.get('startDate')
    to_date = config_data.get('endDate')
    if not from_date or not to_date:
        return False, 'Start date and end date are required.'

    try:
        from_dt = datetime.strptime(from_date, '%Y-%m-%d')
        # Include the full end date window for historical candles.
        to_dt = datetime.strptime(to_date, '%Y-%m-%d') + timedelta(hours=23, minutes=59)
    except Exception:
        return False, 'Invalid date format. Use YYYY-MM-DD.'

    with app.app_context():
        with _lock:
            existing_status = _backtest_status.get(user_id)
            existing_engine = _backtest_jobs.get(user_id)
            if existing_status and existing_status.get('running') and existing_engine:
                return False, 'Backtest already running.'

        clear_previous_raw = config_data.get('clearPrevious', True)
        if isinstance(clear_previous_raw, str):
            clear_previous = clear_previous_raw.lower() in ('1', 'true', 'yes')
        else:
            clear_previous = bool(clear_previous_raw)
        if clear_previous:
            Signal.query.filter_by(user_id=user_id, mode='backtest').delete(synchronize_session=False)
            db.session.commit()

        creds = APICredential.query.filter_by(user_id=user_id).first()
        if not creds:
            return False, 'API credentials not configured.'

        totp_code = config_data.get('totpCode')
        angel_api, error = _build_angel_api(creds, mode='backtest', totp_code=totp_code)
        if not angel_api:
            return False, f'Failed to authenticate with Angel One. {error}'
        _store_tokens(creds, angel_api)

        if not angel_api.load_scrip_master():
            return False, 'Scrip master not available. Configure ANGEL_SCRIP_MASTER_PATH or URL.'

        config = _build_scanner_config(user_id, config_data, mode='backtest')
        engine = ScannerEngine(user_id=user_id, config=config, angel_api=angel_api)
        engine.is_running = True
        try:
            planned_scripts = len(engine._resolve_stock_selection())
        except Exception:
            planned_scripts = 0
        selected_expiries = engine.get_selected_expiries(reference_date=from_dt)
        scripts_total = int(planned_scripts or len(selected_expiries or {}))
        engine.last_backtest_scripts_total = scripts_total

        with _lock:
            _backtest_jobs[user_id] = engine
            _backtest_status[user_id] = {
                'running': True,
                'config_id': config.id,
                'started_at': datetime.utcnow().isoformat() + 'Z',
                'finished_at': None,
                'last_error': None,
                'stopped_by_user': False,
                'signals': 0,
                'scripts_scanned': 0,
                'scripts_total': scripts_total,
                'from_date': from_dt.strftime('%Y-%m-%d'),
                'to_date': to_dt.strftime('%Y-%m-%d'),
                'timeframe': config.timeframe,
                'price_multiplier': float(config.price_multiplier),
                'selected_expiries': selected_expiries
            }

        Thread(
            target=_run_backtest,
            args=(app, engine, from_dt, to_dt),
            daemon=True,
            name=f'backtest-{user_id}'
        ).start()

    return True, 'Backtest started successfully.'


def _run_backtest(app, engine, from_dt, to_dt):
    with app.app_context():
        try:
            planned_scripts = len(engine._resolve_stock_selection())
        except Exception:
            planned_scripts = 0
        engine.last_backtest_scripts_total = _safe_int(getattr(engine, 'last_backtest_scripts_total', 0) or planned_scripts)
        with _lock:
            status = _backtest_status.get(engine.user_id)
            if status:
                status['scripts_total'] = _safe_int(status.get('scripts_total', 0) or planned_scripts)

        def _progress(user_id, scripts_scanned=None, signals=None):
            with _lock:
                status = _backtest_status.get(user_id)
                if not status:
                    return
                if scripts_scanned is not None:
                    status['scripts_scanned'] = max(
                        _safe_int(status.get('scripts_scanned', 0)),
                        _safe_int(scripts_scanned)
                    )
                if signals is not None:
                    status['signals'] = max(
                        _safe_int(status.get('signals', 0)),
                        _safe_int(signals)
                    )
                status['scripts_total'] = max(
                    _safe_int(status.get('scripts_total', 0)),
                    _safe_int(getattr(engine, 'last_backtest_scripts_total', 0) or planned_scripts)
                )

        try:
            engine.run_backtest(from_dt, to_dt, progress_callback=_progress)
            persisted_count = Signal.query.filter_by(
                user_id=engine.user_id,
                mode='backtest',
                scanner_config_id=engine.config_id
            ).count()
            with _lock:
                status = _backtest_status.get(engine.user_id)
                if status:
                    status['running'] = False
                    status['finished_at'] = datetime.utcnow().isoformat() + 'Z'
                    status['signals'] = persisted_count
                    status['scripts_scanned'] = _safe_int(getattr(engine, 'last_backtest_scripts_scanned', 0) or 0)
                    status['scripts_total'] = max(
                        _safe_int(status.get('scripts_total', 0)),
                        _safe_int(getattr(engine, 'last_backtest_scripts_total', 0) or planned_scripts)
                    )
                    status['stopped_by_user'] = (engine.is_running is False)
                    if persisted_count == 0:
                        status['last_error'] = None
        except Exception as e:
            logger.error(f'Backtest failed: {str(e)}')
            with _lock:
                status = _backtest_status.get(engine.user_id)
                if status:
                    status['running'] = False
                    status['finished_at'] = datetime.utcnow().isoformat() + 'Z'
                    status['scripts_scanned'] = _safe_int(getattr(engine, 'last_backtest_scripts_scanned', 0) or 0)
                    status['scripts_total'] = max(
                        _safe_int(status.get('scripts_total', 0)),
                        _safe_int(getattr(engine, 'last_backtest_scripts_total', 0) or planned_scripts)
                    )
                    status['last_error'] = str(e)
        finally:
            with _lock:
                _backtest_jobs.pop(engine.user_id, None)


def stop_backtest(user_id):
    with _lock:
        engine = _backtest_jobs.get(user_id)
        status = _backtest_status.get(user_id)
        if not engine or not status or not status.get('running'):
            return False, 'No running backtest found.'
        engine.stop_backtest()
        status['running'] = False
        status['stopped_by_user'] = True
        status['finished_at'] = datetime.utcnow().isoformat() + 'Z'
    return True, 'Backtest stop requested.'


def _update_live_status(
    user_id,
    last_run_at=None,
    signals=None,
    error=None,
    stocks_scanned=None,
    stocks_total=None,
    strikes_scanned=None,
    strikes_total=None
):
    emit_progress = False
    progress_payload = None
    with _lock:
        status = _scanner_status.get(user_id)
        if not status:
            return
        if last_run_at:
            status['last_run_at'] = last_run_at
        if signals is not None:
            status['last_signals'] = signals
        if stocks_scanned is not None:
            status['last_stocks_scanned'] = stocks_scanned
        if stocks_total is not None:
            status['scripts_total'] = stocks_total
        if strikes_scanned is not None:
            status['last_strikes_scanned'] = strikes_scanned
        if strikes_total is not None:
            status['strikes_total'] = strikes_total
        if error:
            status['last_error'] = error
        elif last_run_at or signals is not None:
            # Clear previous error after a successful cycle update.
            status['last_error'] = None
        if (
            stocks_scanned is not None
            or stocks_total is not None
            or strikes_scanned is not None
            or strikes_total is not None
        ):
            emit_progress = True
            progress_payload = {
                'mode': 'live',
                'stocks_scanned': _safe_int(status.get('last_stocks_scanned', 0)),
                'stocks_total': _safe_int(status.get('scripts_total', 0)),
                'strikes_scanned': _safe_int(status.get('last_strikes_scanned', 0)),
                'strikes_total': _safe_int(status.get('strikes_total', 0)),
                'running': bool(status.get('running', False)),
                'updated_at': datetime.utcnow().isoformat() + 'Z'
            }
    if emit_progress and progress_payload:
        socketio.emit('live_progress', progress_payload, to=f'user_{user_id}')


def get_scan_status(user_id):
    with _lock:
        return _scanner_status.get(user_id, {'running': False})


def get_backtest_status(user_id):
    with _lock:
        status = dict(_backtest_status.get(user_id, {'running': False}))
        engine = _backtest_jobs.get(user_id)
        if engine:
            status['scripts_scanned'] = max(
                _safe_int(status.get('scripts_scanned', 0)),
                _safe_int(getattr(engine, 'last_backtest_scripts_scanned', 0))
            )
            status['signals'] = max(
                _safe_int(status.get('signals', 0)),
                _safe_int(getattr(engine, 'last_backtest_signals', 0))
            )
            status['scripts_total'] = max(
                _safe_int(status.get('scripts_total', 0)),
                _safe_int(getattr(engine, 'last_backtest_scripts_total', 0))
            )
        return status
