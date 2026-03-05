"""
Scanner Manager for live and backtest execution
"""
from datetime import datetime, timedelta
from threading import Lock
from threading import Thread
import os
import uuid
from app import socketio, db
from app.models.api_credential import APICredential
from app.models.scanner_config import ScannerConfig
from app.models.signal import Signal
from app.models.user import User
from app.services.angel_api import AngelOneAPI
from app.services.scanner_engine import ScannerEngine
from app.services.telegram_notifier import send_signal_notification
from app.services.streaming_manager import start_live_stream, stop_live_stream
from app.services.queue_manager import get_queue, clear_live_stop_flag, set_live_stop_flag
from app.services.live_scan_jobs import scan_live_symbol_job
from app.utils.encryption import EncryptionHelper
import logging

logger = logging.getLogger(__name__)

_lock = Lock()
_live_scanners = {}
_scanner_status = {}
_backtest_status = {}
_backtest_jobs = {}
DEFAULT_SCAN_WORKERS = 8
MAX_SCAN_WORKERS = 16


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def _optional_bool(value):
    if value in (None, ''):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'on'):
        return True
    if text in ('0', 'false', 'no', 'off'):
        return False
    return None


def _parse_iso_utc(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith('Z'):
            text = text[:-1] + '+00:00'
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _timeframe_minutes(value, default=15):
    text = str(value or '').strip().lower()
    if text.endswith('min'):
        text = text[:-3]
    minutes = _safe_int(text, default)
    return minutes if minutes > 0 else default


def _clamp_int(value, minimum, maximum, default):
    num = _safe_int(value, default)
    return max(minimum, min(maximum, num))


def _estimate_symbol_count(stock_selection, mode='live'):
    if isinstance(stock_selection, list):
        return max(1, len([s for s in stock_selection if str(s).strip()]))
    token = str(stock_selection or 'all').strip().lower()
    if token == 'all':
        # NFO OPTSTK universe is typically large; live prefilter narrows workload.
        return 120 if mode == 'live' else 180
    if token == 'nifty50':
        return 50
    if token == 'banknifty':
        return 1
    return 1


def _scan_profile(config_data, user):
    cfg_data = config_data if isinstance(config_data, dict) else {}
    token = str(cfg_data.get('scanProfile') or getattr(user, 'scan_profile', 'balanced') or 'balanced').strip().lower()
    return token if token in ('conservative', 'balanced', 'aggressive') else 'balanced'


def _recent_rate_limit_hint(user_id, mode='live'):
    status_map = _scanner_status if str(mode).lower() == 'live' else _backtest_status
    status = status_map.get(user_id) or {}
    last_error = str(status.get('last_error') or '').lower()
    return any(token in last_error for token in ('429', 'rate limit', 'too many requests', 'throttle'))


def _auto_tune_workers(user_id, config_data, fallback_cap, mode='live'):
    cfg_data = config_data if isinstance(config_data, dict) else {}
    mode_name = 'backtest' if str(mode).lower() == 'backtest' else 'live'
    user = User.query.get(user_id)
    cpu = max(1, _safe_int(os.cpu_count(), 4))
    cap = max(1, min(MAX_SCAN_WORKERS, _safe_int(fallback_cap, DEFAULT_SCAN_WORKERS)))
    profile = _scan_profile(cfg_data, user)
    timeframe = _timeframe_minutes(cfg_data.get('timeframe'), 15)
    strike_range = max(0, min(50, _safe_int(cfg_data.get('strikeRange'), 0)))
    symbol_count = _estimate_symbol_count(cfg_data.get('stockSelection', 'all'), mode_name)

    date_span_days = 1
    if mode_name == 'backtest':
        start_raw = cfg_data.get('startDate')
        end_raw = cfg_data.get('endDate')
        try:
            start_dt = datetime.strptime(str(start_raw), '%Y-%m-%d')
            end_dt = datetime.strptime(str(end_raw), '%Y-%m-%d')
            date_span_days = max(1, (end_dt - start_dt).days + 1)
        except Exception:
            date_span_days = 7

    # When scanning all strikes (0), model load using a conservative wide-range proxy.
    strike_factor = 20 if strike_range <= 0 else strike_range
    load_units = float(symbol_count * max(2, strike_factor * 2))
    if mode_name == 'backtest':
        load_units *= float(date_span_days)
    if timeframe <= 3:
        load_units *= 1.35
    elif timeframe >= 15:
        load_units *= 0.8
    if mode_name == 'live' and str(cfg_data.get('stockSelection', 'all')).strip().lower() == 'all':
        load_units *= 0.75

    if profile == 'conservative':
        load_units *= 0.85
    elif profile == 'aggressive':
        load_units *= 1.20

    if load_units < 120:
        suggested = 4
    elif load_units < 300:
        suggested = 6
    elif load_units < 700:
        suggested = 8
    elif load_units < 1200:
        suggested = 10
    elif load_units < 2200:
        suggested = 12
    else:
        suggested = 14

    cpu_ceiling = max(2, min(MAX_SCAN_WORKERS, cpu * 2))
    workers = min(suggested, cap, cpu_ceiling)
    if profile == 'conservative':
        workers = max(2, workers - 1)
    elif profile == 'aggressive':
        workers = min(cap, cpu_ceiling, workers + 1)
    rate_limited_recently = _recent_rate_limit_hint(user_id, mode_name)
    if rate_limited_recently:
        workers = max(2, workers - 2)
    workers = max(1, min(MAX_SCAN_WORKERS, int(workers)))

    detail = {
        'enabled': True,
        'mode': mode_name,
        'workers': workers,
        'cap': cap,
        'cpu': cpu,
        'symbols_estimate': int(symbol_count),
        'strike_range': int(strike_range),
        'timeframe_min': int(timeframe),
        'date_span_days': int(date_span_days),
        'profile': profile,
        'load_units': int(round(load_units)),
        'rate_limit_backoff': bool(rate_limited_recently),
        'summary': (
            f'Auto ({mode_name}): workers={workers}, cap={cap}, cpu={cpu}, '
            f'symbols~{int(symbol_count)}, strikeRange={int(strike_range)}, '
            f'timeframe={int(timeframe)}m'
            + (f', span={int(date_span_days)}d' if mode_name == 'backtest' else '')
            + (', backoff=rate-limit' if rate_limited_recently else '')
        )
    }
    return workers, detail


def _get_user_default_config_payload(user_id):
    cfg = ScannerConfig.query.filter_by(user_id=user_id, is_default=True).first()
    if not cfg:
        return {}
    timeframe_text = str(cfg.timeframe or '5')
    return {
        'stockSelection': cfg.get_stock_selection() or 'all',
        'strikeRange': int(cfg.strike_range) if cfg.strike_range is not None else 0,
        'liveExcludeAtmStrikes': int(cfg.live_exclude_atm_strikes or 0),
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
    default_strike_range = 0
    strike_range = int(_pick_config_value(config_data, user_defaults, 'strikeRange', default_strike_range))
    exclude_atm = _safe_int(_pick_config_value(config_data, user_defaults, 'liveExcludeAtmStrikes', 0), 0)
    price_multiplier = float(_pick_config_value(config_data, user_defaults, 'priceMultiplier', 2.0))
    timeframe = int(_pick_config_value(config_data, user_defaults, 'timeframe', 15))
    allowed_timeframes = {1, 3, 5, 15, 30, 60}
    if timeframe not in allowed_timeframes:
        timeframe = 15
    refresh_interval = int(_pick_config_value(config_data, user_defaults, 'refreshInterval', 15)) * 60

    config_name = f'{mode.capitalize()} {datetime.utcnow().strftime("%Y%m%d-%H%M%S")}'

    config = ScannerConfig(
        user_id=user_id,
        config_name=config_name,
        stock_selection=stock_selection,
        strike_range=strike_range,
        live_exclude_atm_strikes=max(0, min(10, exclude_atm)),
        price_multiplier=price_multiplier,
        timeframe=f'{timeframe}min',
        refresh_interval=refresh_interval,
        is_default=False
    )
    db.session.add(config)
    db.session.commit()
    return config


def _effective_worker_count(user_id, config_data=None, mode='live'):
    cfg_data = config_data if isinstance(config_data, dict) else {}
    requested = cfg_data.get('workers')
    user = User.query.get(user_id)
    default_cap = int(getattr(user, 'scan_workers', DEFAULT_SCAN_WORKERS) or DEFAULT_SCAN_WORKERS)
    mode_name = 'backtest' if str(mode).lower() == 'backtest' else 'live'
    mode_cap_attr = 'backtest_workers_cap' if mode_name == 'backtest' else 'live_workers_cap'
    mode_cap = int(getattr(user, mode_cap_attr, default_cap) or default_cap)
    explicit_mode_cap = cfg_data.get('backtestWorkersCap') if mode_name == 'backtest' else cfg_data.get('liveWorkersCap')
    requested_cap = _safe_int(explicit_mode_cap, mode_cap) if explicit_mode_cap not in (None, '') else mode_cap
    fallback = max(1, min(MAX_SCAN_WORKERS, requested_cap))
    auto_tune_enabled = _safe_bool(
        cfg_data.get('autoTune'),
        default=bool(getattr(user, 'auto_tune_default', True))
    )
    try:
        manual_workers = int(requested) if requested not in (None, '') else default_cap
    except Exception:
        manual_workers = default_cap
    manual_workers = max(1, min(MAX_SCAN_WORKERS, manual_workers, fallback))

    if auto_tune_enabled:
        return _auto_tune_workers(user_id, cfg_data, fallback_cap=manual_workers, mode=mode)

    return manual_workers, {
        'enabled': False,
        'mode': 'backtest' if str(mode).lower() == 'backtest' else 'live',
        'workers': manual_workers,
        'cap': manual_workers,
        'summary': f'Manual: workers={manual_workers}'
    }


def _resolve_live_prefilter_settings(user_id, config_data=None):
    cfg_data = config_data if isinstance(config_data, dict) else {}
    user = User.query.get(user_id)
    enabled_raw = cfg_data.get('livePrefilterEnabled')
    enabled = _safe_bool(enabled_raw, default=bool(getattr(user, 'live_prefilter_default', True))) if enabled_raw is not None else bool(getattr(user, 'live_prefilter_default', True))
    top_movers = _clamp_int(
        cfg_data.get('livePrefilterTopMovers', getattr(user, 'live_prefilter_top_movers', 30)),
        0,
        500,
        30
    )
    top_volume = _clamp_int(
        cfg_data.get('livePrefilterTopVolume', getattr(user, 'live_prefilter_top_volume', 30)),
        0,
        500,
        30
    )
    max_stocks = _clamp_int(
        cfg_data.get('livePrefilterMaxStocks', getattr(user, 'live_prefilter_max_stocks', 50)),
        1,
        500,
        50
    )
    return {
        'enabled': bool(enabled),
        'top_movers': int(top_movers),
        'top_volume': int(top_volume),
        'max_stocks': int(max_stocks)
    }


def _resolve_backtest_runtime_settings(user_id, config_data=None):
    cfg_data = config_data if isinstance(config_data, dict) else {}
    user = User.query.get(user_id)

    frequency = str(
        cfg_data.get(
            'backtestRebalanceFrequency',
            getattr(user, 'backtest_rebalance_frequency', 'weekly')
        )
        or 'weekly'
    ).strip().lower()
    if frequency not in ('daily', 'weekly', 'monthly'):
        frequency = 'weekly'

    strict_first_candle_raw = cfg_data.get('backtestStrictFirstCandle')
    strict_first_candle_default = bool(getattr(user, 'backtest_strict_first_candle', True))
    strict_first_candle = (
        _safe_bool(strict_first_candle_raw, default=strict_first_candle_default)
        if strict_first_candle_raw is not None
        else strict_first_candle_default
    )

    liquidity_enabled_raw = cfg_data.get('backtestLiquidityFilterEnabled')
    liquidity_enabled_default = bool(getattr(user, 'backtest_liquidity_filter_enabled', True))
    liquidity_enabled = (
        _safe_bool(liquidity_enabled_raw, default=liquidity_enabled_default)
        if liquidity_enabled_raw is not None
        else liquidity_enabled_default
    )

    min_candles = _clamp_int(
        cfg_data.get('backtestMinCandlesPerStrike', getattr(user, 'backtest_min_candles_per_strike', 5)),
        2,
        500,
        5
    )
    min_avg_volume = _clamp_int(
        cfg_data.get('backtestMinAvgVolume', getattr(user, 'backtest_min_avg_volume', 1)),
        0,
        1_000_000,
        1
    )
    return {
        'rebalance_frequency': frequency,
        'strict_first_candle': bool(strict_first_candle),
        'liquidity_filter_enabled': bool(liquidity_enabled),
        'min_candles_per_strike': int(min_candles),
        'min_avg_volume': int(min_avg_volume)
    }


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

        user = User.query.get(user_id)
        if not user:
            return False, 'User not found.'

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
        worker_count, tune_detail = _effective_worker_count(user_id, config_data, mode='live')
        prefilter = _resolve_live_prefilter_settings(user_id, config_data)
        engine = ScannerEngine(
            user_id=user_id,
            config=config,
            angel_api=angel_api,
            workers=worker_count,
            live_prefilter_enabled=prefilter['enabled'],
            live_prefilter_top_movers=prefilter['top_movers'],
            live_prefilter_top_volume=prefilter['top_volume'],
            live_prefilter_max_stocks=prefilter['max_stocks'],
            api_error_log_enabled=bool(getattr(user, 'api_error_log_enabled', False))
        )
        engine.is_running = True
        selected_expiries = engine.get_selected_expiries(reference_date=datetime.now())
        scripts_total = int(len(selected_expiries or {}))
        engine.last_cycle_stocks_total = scripts_total

        clear_live_stop_flag(user_id)
        _start_streaming_for_live(user_id, engine, angel_api)

        with _lock:
            previous_status = dict(_scanner_status.get(user_id) or {})
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
                'skipped_candles': 0,
                'timeframe': config.timeframe,
                'price_multiplier': float(config.price_multiplier),
                'selected_expiries': selected_expiries,
                'workers': worker_count,
                'live_prefilter_enabled': bool(prefilter['enabled']),
                'live_prefilter_top_movers': int(prefilter['top_movers']),
                'live_prefilter_top_volume': int(prefilter['top_volume']),
                'live_prefilter_max_stocks': int(prefilter['max_stocks']),
                'auto_tune': bool(tune_detail.get('enabled')),
                'auto_tune_summary': tune_detail.get('summary'),
                'auto_tune_details': tune_detail,
                'recent_cycles': list(previous_status.get('recent_cycles') or [])[:10],
                'cycle_started_at': None
            }

        Thread(
            target=_run_live_loop,
            args=(app, user_id),
            daemon=True,
            name=f'live-scan-{user_id}'
        ).start()

    return True, f'Live scanner started successfully. Workers: {worker_count}. {tune_detail.get("summary", "")}'.strip()


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
        status = _scanner_status.get(user_id)
        if engine:
            engine.stop_live_scan()
            _live_scanners.pop(user_id, None)
            set_live_stop_flag(user_id)
            stop_live_stream(user_id)
        elif not status or not status.get('running'):
            return False, 'No running scanner found.'
        if status:
            status['running'] = False
            status['stopped_at'] = datetime.utcnow().isoformat() + 'Z'
    return True, 'Scanner stopped.'


def _start_streaming_for_live(user_id, engine, angel_api):
    try:
        enabled = str(os.getenv('LIVE_STREAMING_ENABLED', 'false')).strip().lower() in ('1', 'true', 'yes', 'on')
        if not enabled:
            return
        exchange_type = int(os.getenv('LIVE_STREAMING_EXCHANGE_TYPE', '2') or 2)
        tokens = []
        symbols = engine._resolve_stock_selection()
        ref = datetime.now()
        for symbol in symbols:
            expiry = engine._resolve_expiry(symbol, reference_date=ref)
            if not expiry:
                continue
            tokens.extend(angel_api.get_option_tokens_for_expiry(symbol, expiry, exchange='NFO'))
        tokens = list(dict.fromkeys(tokens))
        if not tokens:
            return
        start_live_stream(user_id, angel_api, {exchange_type: tokens})
    except Exception as exc:
        logger.warning('Live streaming start failed: %s', str(exc))


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
        # Do not request future candles when end date is today.
        now_dt = datetime.now()
        if to_dt > now_dt:
            to_dt = now_dt
    except Exception:
        return False, 'Invalid date format. Use YYYY-MM-DD.'

    with app.app_context():
        with _lock:
            existing_status = _backtest_status.get(user_id)
            existing_engine = _backtest_jobs.get(user_id)
            if existing_status and existing_status.get('running') and existing_engine:
                return False, 'Backtest already running.'

        user = User.query.get(user_id)
        if not user:
            return False, 'User not found.'

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
        worker_count, tune_detail = _effective_worker_count(user_id, config_data, mode='backtest')
        backtest_runtime = _resolve_backtest_runtime_settings(user_id, config_data)
        engine = ScannerEngine(
            user_id=user_id,
            config=config,
            angel_api=angel_api,
            workers=worker_count,
            backtest_rebalance_frequency=backtest_runtime['rebalance_frequency'],
            backtest_strict_first_candle=backtest_runtime['strict_first_candle'],
            backtest_liquidity_filter_enabled=backtest_runtime['liquidity_filter_enabled'],
            backtest_min_candles_per_strike=backtest_runtime['min_candles_per_strike'],
            backtest_min_avg_volume=backtest_runtime['min_avg_volume'],
            backtest_scan_log_enabled=bool(getattr(user, 'backtest_scan_log_enabled', False)),
            api_error_log_enabled=bool(getattr(user, 'api_error_log_enabled', False))
        )
        engine.is_running = True
        try:
            planned_scripts = len(engine._resolve_stock_selection())
        except Exception:
            planned_scripts = 0
        selected_expiries = engine.get_selected_expiries(reference_date=from_dt)
        scripts_total = int(planned_scripts or len(selected_expiries or {}))
        engine.last_backtest_scripts_total = scripts_total

        with _lock:
            previous_status = dict(_backtest_status.get(user_id) or {})
            _backtest_jobs[user_id] = engine
            _backtest_status[user_id] = {
                'running': True,
                'config_id': config.id,
                'started_at': datetime.utcnow().isoformat() + 'Z',
                'finished_at': None,
                'last_error': None,
                'stopped_by_user': False,
                'run_outcome': 'running',
                'completion_note': 'Backtest is running.',
                'signals': 0,
                'scripts_scanned': 0,
                'scripts_total': scripts_total,
                'strikes_scanned': 0,
                'strikes_total': 0,
                'candles_missing': 0,
                'instruments_missing': 0,
                'from_date': from_dt.strftime('%Y-%m-%d'),
                'to_date': to_dt.strftime('%Y-%m-%d'),
                'timeframe': config.timeframe,
                'price_multiplier': float(config.price_multiplier),
                'selected_expiries': selected_expiries,
                'workers': worker_count,
                'backtest_log_enabled': bool(getattr(user, 'backtest_scan_log_enabled', False)),
                'latest_backtest_log': None,
                'latest_backtest_csv': None,
                'rebalance_frequency': backtest_runtime['rebalance_frequency'],
                'strict_first_candle': bool(backtest_runtime['strict_first_candle']),
                'liquidity_filter_enabled': bool(backtest_runtime['liquidity_filter_enabled']),
                'min_candles_per_strike': int(backtest_runtime['min_candles_per_strike']),
                'min_avg_volume': int(backtest_runtime['min_avg_volume']),
                'auto_tune': bool(tune_detail.get('enabled')),
                'auto_tune_summary': tune_detail.get('summary'),
                'auto_tune_details': tune_detail,
                'recent_runs': list(previous_status.get('recent_runs') or [])[:10],
                'run_started_at': datetime.utcnow().isoformat() + 'Z'
            }

        Thread(
            target=_run_backtest,
            args=(app, engine, from_dt, to_dt),
            daemon=True,
            name=f'backtest-{user_id}'
        ).start()

    return True, (
        f'Backtest started successfully. Workers: {worker_count}. '
        f'Rebalance: {backtest_runtime["rebalance_frequency"]}. '
        f'{tune_detail.get("summary", "")}'
    ).strip()


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

        def _progress(user_id, scripts_scanned=None, strikes_scanned=None, strikes_total=None, signals=None):
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
                if strikes_scanned is not None:
                    status['strikes_scanned'] = max(
                        _safe_int(status.get('strikes_scanned', 0)),
                        _safe_int(strikes_scanned)
                    )
                if strikes_total is not None:
                    status['strikes_total'] = max(
                        _safe_int(status.get('strikes_total', 0)),
                        _safe_int(strikes_total)
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
                    started = _parse_iso_utc(status.get('run_started_at')) or _parse_iso_utc(status.get('started_at'))
                    finished = _parse_iso_utc(datetime.utcnow().isoformat() + 'Z')
                    duration_ms = 0
                    if started and finished:
                        duration_ms = max(0, int((finished - started).total_seconds() * 1000))
                    status['running'] = False
                    status['finished_at'] = datetime.utcnow().isoformat() + 'Z'
                    status['signals'] = persisted_count
                    status['scripts_scanned'] = _safe_int(getattr(engine, 'last_backtest_scripts_scanned', 0) or 0)
                    status['scripts_total'] = max(
                        _safe_int(status.get('scripts_total', 0)),
                        _safe_int(getattr(engine, 'last_backtest_scripts_total', 0) or planned_scripts)
                    )
                    status['strikes_scanned'] = _safe_int(getattr(engine, 'last_backtest_strikes_scanned', 0) or 0)
                    status['strikes_total'] = _safe_int(getattr(engine, 'last_backtest_strikes_total', 0) or 0)
                    status['candles_missing'] = _safe_int(getattr(engine, 'last_backtest_candles_missing', 0) or 0)
                    status['instruments_missing'] = _safe_int(getattr(engine, 'last_backtest_instruments_missing', 0) or 0)
                    # Preserve explicit stop requests set by stop_backtest();
                    # normal completion should remain "not stopped by user".
                    status['stopped_by_user'] = bool(status.get('stopped_by_user', False))
                    status['run_outcome'] = 'stopped' if status['stopped_by_user'] else 'completed'
                    status['latest_backtest_log'] = getattr(engine, '_backtest_log_path', None)
                    status['latest_backtest_csv'] = getattr(engine, '_backtest_csv_path', None)
                    log_generated = bool(status.get('latest_backtest_log')) and os.path.exists(str(status.get('latest_backtest_log')))
                    csv_generated = bool(status.get('latest_backtest_csv')) and os.path.exists(str(status.get('latest_backtest_csv')))
                    if status['stopped_by_user']:
                        status['completion_note'] = 'Backtest stopped by user.'
                    elif bool(status.get('backtest_log_enabled')):
                        status['completion_note'] = (
                            'Backtest completed. Diagnostic logs generated.'
                            if (log_generated or csv_generated)
                            else 'Backtest completed. Diagnostic log files were not generated.'
                        )
                    else:
                        status['completion_note'] = 'Backtest completed.'
                    if _safe_int(status.get('signals', 0), 0) == 0:
                        missing_info = (
                            f' Missing candles: {_safe_int(status.get("candles_missing", 0), 0)}'
                            f', missing instruments: {_safe_int(status.get("instruments_missing", 0), 0)}.'
                        )
                        status['completion_note'] = f'{status["completion_note"]}{missing_info}'
                    recent = list(status.get('recent_runs') or [])
                    recent.insert(0, {
                        'finished_at': status['finished_at'],
                        'duration_ms': duration_ms,
                        'scripts_scanned': _safe_int(status.get('scripts_scanned', 0)),
                        'scripts_total': _safe_int(status.get('scripts_total', 0)),
                        'strikes_scanned': _safe_int(status.get('strikes_scanned', 0)),
                        'strikes_total': _safe_int(status.get('strikes_total', 0)),
                        'candles_missing': _safe_int(status.get('candles_missing', 0)),
                        'instruments_missing': _safe_int(status.get('instruments_missing', 0)),
                        'signals': _safe_int(status.get('signals', 0)),
                        'workers': _safe_int(status.get('workers', 0)),
                        'stopped_by_user': bool(status.get('stopped_by_user', False)),
                        'outcome': status.get('run_outcome'),
                        'log_generated': bool(log_generated or csv_generated),
                        'error': None
                    })
                    status['recent_runs'] = recent[:10]
                    status['run_started_at'] = None
                    if persisted_count == 0:
                        status['last_error'] = None
        except Exception as e:
            logger.error(f'Backtest failed: {str(e)}')
            with _lock:
                status = _backtest_status.get(engine.user_id)
                if status:
                    started = _parse_iso_utc(status.get('run_started_at')) or _parse_iso_utc(status.get('started_at'))
                    finished = _parse_iso_utc(datetime.utcnow().isoformat() + 'Z')
                    duration_ms = 0
                    if started and finished:
                        duration_ms = max(0, int((finished - started).total_seconds() * 1000))
                    status['running'] = False
                    status['finished_at'] = datetime.utcnow().isoformat() + 'Z'
                    status['scripts_scanned'] = _safe_int(getattr(engine, 'last_backtest_scripts_scanned', 0) or 0)
                    status['scripts_total'] = max(
                        _safe_int(status.get('scripts_total', 0)),
                        _safe_int(getattr(engine, 'last_backtest_scripts_total', 0) or planned_scripts)
                    )
                    status['strikes_scanned'] = _safe_int(getattr(engine, 'last_backtest_strikes_scanned', 0) or 0)
                    status['strikes_total'] = _safe_int(getattr(engine, 'last_backtest_strikes_total', 0) or 0)
                    status['candles_missing'] = _safe_int(getattr(engine, 'last_backtest_candles_missing', 0) or 0)
                    status['instruments_missing'] = _safe_int(getattr(engine, 'last_backtest_instruments_missing', 0) or 0)
                    status['last_error'] = str(e)
                    status['run_outcome'] = 'failed'
                    status['latest_backtest_log'] = getattr(engine, '_backtest_log_path', None)
                    status['latest_backtest_csv'] = getattr(engine, '_backtest_csv_path', None)
                    status['completion_note'] = 'Backtest failed.'
                    recent = list(status.get('recent_runs') or [])
                    recent.insert(0, {
                        'finished_at': status['finished_at'],
                        'duration_ms': duration_ms,
                        'scripts_scanned': _safe_int(status.get('scripts_scanned', 0)),
                        'scripts_total': _safe_int(status.get('scripts_total', 0)),
                        'strikes_scanned': _safe_int(status.get('strikes_scanned', 0)),
                        'strikes_total': _safe_int(status.get('strikes_total', 0)),
                        'candles_missing': _safe_int(status.get('candles_missing', 0)),
                        'instruments_missing': _safe_int(status.get('instruments_missing', 0)),
                        'signals': _safe_int(status.get('signals', 0)),
                        'workers': _safe_int(status.get('workers', 0)),
                        'stopped_by_user': bool(status.get('stopped_by_user', False)),
                        'outcome': status.get('run_outcome'),
                        'log_generated': bool(status.get('latest_backtest_log') or status.get('latest_backtest_csv')),
                        'error': str(e)
                    })
                    status['recent_runs'] = recent[:10]
                    status['run_started_at'] = None
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
        status['run_outcome'] = 'stopped'
        status['completion_note'] = 'Backtest stopped by user.'
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
    strikes_total=None,
    skipped_candles=None
):
    emit_progress = False
    progress_payload = None
    with _lock:
        status = _scanner_status.get(user_id)
        if not status:
            return
        if (
            bool(status.get('running'))
            and stocks_scanned is not None
            and strikes_scanned is not None
            and _safe_int(stocks_scanned, 0) == 0
            and _safe_int(strikes_scanned, 0) == 0
        ):
            status['cycle_started_at'] = datetime.utcnow().isoformat() + 'Z'
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
        if skipped_candles is not None:
            status['skipped_candles'] = skipped_candles
        if error:
            status['last_error'] = error
        elif last_run_at or signals is not None:
            # Clear previous error after a successful cycle update.
            status['last_error'] = None
        if last_run_at:
            started = _parse_iso_utc(status.get('cycle_started_at')) or _parse_iso_utc(status.get('started_at'))
            finished = _parse_iso_utc(last_run_at) or datetime.utcnow()
            duration_ms = 0
            if started and finished:
                duration_ms = max(0, int((finished - started).total_seconds() * 1000))
            recent = list(status.get('recent_cycles') or [])
            recent.insert(0, {
                'completed_at': last_run_at,
                'duration_ms': duration_ms,
                'stocks_scanned': _safe_int(status.get('last_stocks_scanned', 0)),
                'stocks_total': _safe_int(status.get('scripts_total', 0)),
                'strikes_scanned': _safe_int(status.get('last_strikes_scanned', 0)),
                'strikes_total': _safe_int(status.get('strikes_total', 0)),
                'skipped_candles': _safe_int(status.get('skipped_candles', 0)),
                'signals': _safe_int(status.get('last_signals', 0)),
                'workers': _safe_int(status.get('workers', 0)),
                'auto_tune': bool(status.get('auto_tune', False)),
                'auto_tune_summary': status.get('auto_tune_summary')
            })
            status['recent_cycles'] = recent[:10]
            status['cycle_started_at'] = None
        if (
            stocks_scanned is not None
            or stocks_total is not None
            or strikes_scanned is not None
            or strikes_total is not None
            or skipped_candles is not None
        ):
            emit_progress = True
            progress_payload = {
                'mode': 'live',
                'stocks_scanned': _safe_int(status.get('last_stocks_scanned', 0)),
                'stocks_total': _safe_int(status.get('scripts_total', 0)),
                'strikes_scanned': _safe_int(status.get('last_strikes_scanned', 0)),
                'strikes_total': _safe_int(status.get('strikes_total', 0)),
                'skipped_candles': _safe_int(status.get('skipped_candles', 0)),
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
            status['strikes_scanned'] = max(
                _safe_int(status.get('strikes_scanned', 0)),
                _safe_int(getattr(engine, 'last_backtest_strikes_scanned', 0))
            )
            status['strikes_total'] = max(
                _safe_int(status.get('strikes_total', 0)),
                _safe_int(getattr(engine, 'last_backtest_strikes_total', 0))
            )
            status['candles_missing'] = max(
                _safe_int(status.get('candles_missing', 0)),
                _safe_int(getattr(engine, 'last_backtest_candles_missing', 0))
            )
            status['instruments_missing'] = max(
                _safe_int(status.get('instruments_missing', 0)),
                _safe_int(getattr(engine, 'last_backtest_instruments_missing', 0))
            )
        return status


def trigger_webhook_scan(app, user_id, symbol, timeframe=None, candle_time=None, strike_range=None):
    job_id = uuid.uuid4().hex[:12]

    def _run():
        with app.app_context():
            user = User.query.get(user_id)
            if not user:
                logger.warning('Webhook scan user not found: %s', user_id)
                return

            creds = APICredential.query.filter_by(user_id=user_id).first()
            if not creds:
                logger.warning('Webhook scan missing API credentials for user %s', user_id)
                return

            angel_api, error = _build_angel_api(creds, mode='live', totp_code=None)
            if not angel_api:
                logger.warning('Webhook scan auth failed for user %s: %s', user_id, error)
                return
            _store_tokens(creds, angel_api)

            if not angel_api.load_scrip_master():
                logger.warning('Webhook scan missing scrip master for user %s', user_id)
                return

            cfg_payload = _get_user_default_config_payload(user_id)
            cfg_payload['stockSelection'] = str(symbol or '').strip().upper()
            if timeframe:
                cfg_payload['timeframe'] = _timeframe_minutes(timeframe, cfg_payload.get('timeframe', 15))
            if strike_range is not None and str(strike_range).strip() != '':
                cfg_payload['strikeRange'] = max(0, min(50, _safe_int(strike_range, 0)))

            config = _build_scanner_config(user_id, cfg_payload, mode='live')
            engine = ScannerEngine(
                user_id=user_id,
                config=config,
                angel_api=angel_api,
                workers=1,
                live_prefilter_enabled=False,
                live_prefilter_top_movers=0,
                live_prefilter_top_volume=0,
                live_prefilter_max_stocks=1,
                api_error_log_enabled=bool(getattr(user, 'api_error_log_enabled', False))
            )
            engine.is_running = True
            try:
                signals = engine.scan_once(reference_time=_parse_iso_utc(candle_time))
            except Exception as e:
                logger.error('Webhook scan failed for user %s: %s', user_id, str(e))
                return
            finally:
                engine.is_running = False

            if signals:
                for signal in signals:
                    payload = engine._serialize_realtime_signal(signal)
                    socketio.emit('new_signal', payload, to=f'user_{user_id}')
                    send_signal_notification(payload, mode='live', user_id=user_id)

            logger.info(
                'Webhook scan complete user=%s symbol=%s signals=%s',
                user_id, str(symbol or '').upper(), len(signals or [])
            )

    Thread(target=_run, daemon=True, name=f'webhook-scan-{user_id}-{job_id}').start()
    return True, 'Webhook scan queued.', job_id
