"""
Scanner Routes
"""
from flask import Blueprint, render_template, jsonify, request, send_file
from flask_login import login_required, current_user
from app import db, csrf
from app.models.scanner_config import ScannerConfig
from app.services.scanner_manager import (
    start_live_scan as start_live_scan_mgr,
    stop_live_scan as stop_live_scan_mgr,
    start_backtest as start_backtest_mgr,
    stop_backtest as stop_backtest_mgr,
    connect_api_session as connect_api_session_mgr
)
from app.services.angel_api import AngelOneAPI
from app.services.scanner_engine import ScannerEngine
from flask import current_app
import logging
import os
from datetime import datetime
from pathlib import Path

scanner_bp = Blueprint('scanner', __name__)
logger = logging.getLogger(__name__)

ALLOWED_TIMEFRAMES = {1, 3, 5, 15, 30, 60}
LIVE_TRACE_FILE = os.path.join('logs', 'live_trace.log')
DEFAULT_SCAN_WORKERS = 8
VALID_SCAN_PROFILES = {'conservative', 'balanced', 'aggressive'}
VALID_REBALANCE_FREQUENCIES = {'daily', 'weekly', 'monthly'}


def _trace_live(message):
    """Write live flow traces even when app logger has no file handler."""
    try:
        os.makedirs('logs', exist_ok=True)
        stamp = datetime.utcnow().isoformat() + 'Z'
        line = f'{stamp} pid={os.getpid()} {message}\n'
        with open(LIVE_TRACE_FILE, 'a', encoding='utf-8') as handle:
            handle.write(line)
    except Exception:
        pass


def _default_config_payload():
    return {
        'stockSelection': 'all',
        'strikeRange': 0,
        'liveExcludeAtmStrikes': 0,
        'minSignalVolume': 10,
        'priceMultiplier': 2.0,
        'timeframe': 15,
        'refreshInterval': 15,
        'workers': DEFAULT_SCAN_WORKERS,
        'autoTune': True,
        'scanProfile': 'balanced',
        'livePrefilterEnabled': True,
        'livePrefilterTopMovers': 30,
        'livePrefilterTopVolume': 30,
        'livePrefilterMaxStocks': 50,
        'liveWorkersCap': DEFAULT_SCAN_WORKERS,
        'backtestWorkersCap': DEFAULT_SCAN_WORKERS,
        'backtestRebalanceFrequency': 'weekly',
        'backtestStrictFirstCandle': True,
        'backtestLiquidityFilterEnabled': True,
        'backtestMinCandlesPerStrike': 5,
        'backtestMinAvgVolume': 1,
        'guardrailEnabled': True,
        'guardrailLoadThreshold': 1200,
        'notificationServicesEnabled': True
    }


def _coerce_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _clamp_int(value, minimum, maximum, default):
    num = _safe_int(value, default)
    return max(minimum, min(maximum, num))


def _normalize_profile(value, default='balanced'):
    token = str(value or default).strip().lower()
    return token if token in VALID_SCAN_PROFILES else default


def _normalize_rebalance_frequency(value, default='weekly'):
    token = str(value or default).strip().lower()
    return token if token in VALID_REBALANCE_FREQUENCIES else default


def _user_runtime_defaults():
    default_cfg = _default_config_payload()
    scan_workers = max(1, min(16, int(getattr(current_user, 'scan_workers', DEFAULT_SCAN_WORKERS) or DEFAULT_SCAN_WORKERS)))
    live_cap = max(1, min(16, int(getattr(current_user, 'live_workers_cap', scan_workers) or scan_workers)))
    backtest_cap = max(1, min(16, int(getattr(current_user, 'backtest_workers_cap', scan_workers) or scan_workers)))
    return {
        'workers': scan_workers,
        'autoTune': bool(getattr(current_user, 'auto_tune_default', True)),
        'scanProfile': _normalize_profile(getattr(current_user, 'scan_profile', default_cfg['scanProfile'])),
        'livePrefilterEnabled': bool(getattr(current_user, 'live_prefilter_default', True)),
        'livePrefilterTopMovers': _clamp_int(getattr(current_user, 'live_prefilter_top_movers', 30), 0, 500, 30),
        'livePrefilterTopVolume': _clamp_int(getattr(current_user, 'live_prefilter_top_volume', 30), 0, 500, 30),
        'livePrefilterMaxStocks': _clamp_int(getattr(current_user, 'live_prefilter_max_stocks', 50), 1, 500, 50),
        'liveWorkersCap': live_cap,
        'backtestWorkersCap': backtest_cap,
        'backtestRebalanceFrequency': _normalize_rebalance_frequency(
            getattr(current_user, 'backtest_rebalance_frequency', 'weekly')
        ),
        'backtestStrictFirstCandle': bool(getattr(current_user, 'backtest_strict_first_candle', True)),
        'backtestLiquidityFilterEnabled': bool(getattr(current_user, 'backtest_liquidity_filter_enabled', True)),
        'backtestMinCandlesPerStrike': _clamp_int(getattr(current_user, 'backtest_min_candles_per_strike', 5), 2, 500, 5),
        'backtestMinAvgVolume': _clamp_int(getattr(current_user, 'backtest_min_avg_volume', 1), 0, 1_000_000, 1),
        'guardrailEnabled': bool(getattr(current_user, 'guardrail_enabled', True)),
        'guardrailLoadThreshold': _clamp_int(getattr(current_user, 'guardrail_load_threshold', 1200), 100, 25000, 1200),
        'notificationServicesEnabled': bool(getattr(current_user, 'notification_services_enabled', True))
    }


def _normalize_config_payload(data):
    payload = _default_config_payload()
    if not isinstance(data, dict):
        return payload

    stock_selection = data.get('stockSelection', payload['stockSelection'])
    if isinstance(stock_selection, list):
        symbols = [
            str(s).strip().upper()
            for s in stock_selection
            if str(s).strip()
        ]
        payload['stockSelection'] = symbols if symbols else payload['stockSelection']
    elif isinstance(stock_selection, str):
        stock_selection = stock_selection.strip()
        if stock_selection.lower() in ('all', 'nifty50', 'banknifty'):
            payload['stockSelection'] = stock_selection.lower()
        elif stock_selection:
            payload['stockSelection'] = stock_selection.upper()
    try:
        # 0 means scan all available strikes; 1..50 keeps bounded custom ranges.
        payload['strikeRange'] = max(0, min(50, int(data.get('strikeRange', payload['strikeRange']))))
    except Exception:
        pass
    try:
        payload['liveExcludeAtmStrikes'] = _clamp_int(
            data.get('liveExcludeAtmStrikes', payload.get('liveExcludeAtmStrikes', 0)),
            0,
            10,
            0
        )
    except Exception:
        pass
    try:
        payload['minSignalVolume'] = _clamp_int(
            data.get('minSignalVolume', payload.get('minSignalVolume', 10)),
            0,
            1_000_000,
            10
        )
    except Exception:
        pass
    try:
        payload['priceMultiplier'] = max(1.0, float(data.get('priceMultiplier', payload['priceMultiplier'])))
    except Exception:
        pass
    try:
        timeframe = int(data.get('timeframe', payload['timeframe']))
        payload['timeframe'] = timeframe if timeframe in ALLOWED_TIMEFRAMES else 15
    except Exception:
        pass
    try:
        payload['refreshInterval'] = max(1, min(60, int(data.get('refreshInterval', payload['refreshInterval']))))
    except Exception:
        pass
    auto_tune_raw = data.get('autoTune', payload.get('autoTune', True))
    payload['autoTune'] = str(auto_tune_raw).strip().lower() in ('1', 'true', 'yes', 'on')
    payload['scanProfile'] = _normalize_profile(data.get('scanProfile', payload.get('scanProfile', 'balanced')))
    payload['livePrefilterEnabled'] = _coerce_bool(
        data.get('livePrefilterEnabled', payload.get('livePrefilterEnabled', True)),
        default=True
    )
    payload['livePrefilterTopMovers'] = _clamp_int(
        data.get('livePrefilterTopMovers', payload.get('livePrefilterTopMovers', 30)),
        0,
        500,
        30
    )
    payload['livePrefilterTopVolume'] = _clamp_int(
        data.get('livePrefilterTopVolume', payload.get('livePrefilterTopVolume', 30)),
        0,
        500,
        30
    )
    payload['livePrefilterMaxStocks'] = _clamp_int(
        data.get('livePrefilterMaxStocks', payload.get('livePrefilterMaxStocks', 50)),
        1,
        500,
        50
    )
    payload['liveWorkersCap'] = _clamp_int(
        data.get('liveWorkersCap', payload.get('liveWorkersCap', DEFAULT_SCAN_WORKERS)),
        1,
        16,
        DEFAULT_SCAN_WORKERS
    )
    payload['backtestWorkersCap'] = _clamp_int(
        data.get('backtestWorkersCap', payload.get('backtestWorkersCap', DEFAULT_SCAN_WORKERS)),
        1,
        16,
        DEFAULT_SCAN_WORKERS
    )
    payload['backtestRebalanceFrequency'] = _normalize_rebalance_frequency(
        data.get('backtestRebalanceFrequency', payload.get('backtestRebalanceFrequency', 'weekly'))
    )
    payload['backtestStrictFirstCandle'] = _coerce_bool(
        data.get('backtestStrictFirstCandle', payload.get('backtestStrictFirstCandle', True)),
        default=True
    )
    payload['backtestLiquidityFilterEnabled'] = _coerce_bool(
        data.get('backtestLiquidityFilterEnabled', payload.get('backtestLiquidityFilterEnabled', True)),
        default=True
    )
    payload['backtestMinCandlesPerStrike'] = _clamp_int(
        data.get('backtestMinCandlesPerStrike', payload.get('backtestMinCandlesPerStrike', 5)),
        2,
        500,
        5
    )
    payload['backtestMinAvgVolume'] = _clamp_int(
        data.get('backtestMinAvgVolume', payload.get('backtestMinAvgVolume', 1)),
        0,
        1_000_000,
        1
    )
    payload['guardrailEnabled'] = _coerce_bool(
        data.get('guardrailEnabled', payload.get('guardrailEnabled', True)),
        default=True
    )
    payload['guardrailLoadThreshold'] = _clamp_int(
        data.get('guardrailLoadThreshold', payload.get('guardrailLoadThreshold', 1200)),
        100,
        25000,
        1200
    )
    payload['notificationServicesEnabled'] = _coerce_bool(
        data.get('notificationServicesEnabled', payload.get('notificationServicesEnabled', True)),
        default=True
    )
    return payload


def _get_user_default_config(user_id):
    cfg = ScannerConfig.query.filter_by(user_id=user_id, is_default=True).first()
    if not cfg:
        return None
    stock_selection = cfg.get_stock_selection()
    return {
        'stockSelection': stock_selection if stock_selection else 'all',
        'strikeRange': int(cfg.strike_range) if cfg.strike_range is not None else 0,
        'liveExcludeAtmStrikes': int(cfg.live_exclude_atm_strikes or 0),
        'minSignalVolume': int(cfg.min_signal_volume or 10),
        'priceMultiplier': float(cfg.price_multiplier or 2.0),
        'timeframe': int(str(cfg.timeframe or '5').replace('min', '')),
        'refreshInterval': max(1, int((cfg.refresh_interval or 300) / 60)),
        **_user_runtime_defaults()
    }


@scanner_bp.route('/configuration', methods=['GET'])
@login_required
def configuration():
    """Scanner configuration page."""
    return render_template('scanner/configuration.html')


@scanner_bp.route('/api/config', methods=['GET'])
@login_required
def get_scanner_config():
    """Get saved default scanner config for current user."""
    cfg = _get_user_default_config(current_user.id)
    default_cfg = _default_config_payload()
    default_cfg.update(_user_runtime_defaults())
    config = cfg or default_cfg
    try:
        from app.services.telegram_notifier import get_telegram_notifier
        notifier = get_telegram_notifier()
        token_effective, channel_effective = notifier.resolve_credentials(current_user.id)
        user_token = str(getattr(current_user, 'telegram_bot_token', '') or '')
        user_channel = str(getattr(current_user, 'telegram_channel_id', '') or '')
        config.update({
            'telegramBotToken': '',
            'telegramChannelId': '',
            'telegramBotTokenPlaceholder': user_token or token_effective or '',
            'telegramChannelIdPlaceholder': user_channel or channel_effective or ''
        })
    except Exception:
        config.update({
            'telegramBotToken': '',
            'telegramChannelId': '',
            'telegramBotTokenPlaceholder': '',
            'telegramChannelIdPlaceholder': ''
        })
    return jsonify({
        'success': True,
        'config': config
    })


@scanner_bp.route('/api/stocks', methods=['GET'])
@login_required
def get_stock_options():
    """Get supported stock symbols for scanner selection."""
    source = 'fallback_nifty50'
    stocks = []
    try:
        # Scrip master fetch does not require authenticated trading session.
        api = AngelOneAPI(api_key='', client_id='', password='')
        stocks = api.get_available_option_underlyings(
            exchange='NFO',
            instrument_type='OPTSTK'
        ) or []
        if stocks:
            source = 'scrip_master_nfo_optstk'
    except Exception as e:
        logger.warning(f'Failed to load NFO OPTSTK stocks from scrip master: {str(e)}')

    if not stocks:
        stocks = ScannerEngine.NIFTY_50_SYMBOLS

    return jsonify({
        'success': True,
        'stocks': stocks,
        'count': len(stocks),
        'source': source,
        'filters': {
            'exch_seg': 'NFO',
            'instrumenttype': 'OPTSTK'
        }
    })


@scanner_bp.route('/api/config', methods=['POST'])
@login_required
def save_scanner_config():
    """Save default scanner config for current user."""
    raw_payload = request.get_json() or {}
    payload = _normalize_config_payload(raw_payload)
    cfg = ScannerConfig.query.filter_by(user_id=current_user.id, is_default=True).first()
    if not cfg:
        cfg = ScannerConfig(
            user_id=current_user.id,
            config_name='Default Scanner Config',
            is_default=True
        )
        db.session.add(cfg)

    cfg.stock_selection = payload['stockSelection']
    cfg.strike_range = payload['strikeRange']
    cfg.live_exclude_atm_strikes = _clamp_int(payload.get('liveExcludeAtmStrikes', 0), 0, 10, 0)
    cfg.min_signal_volume = _clamp_int(payload.get('minSignalVolume', 10), 0, 1_000_000, 10)
    cfg.price_multiplier = payload['priceMultiplier']
    cfg.timeframe = f"{payload['timeframe']}min"
    cfg.refresh_interval = payload['refreshInterval'] * 60

    # Persist runtime defaults on user profile as well.
    current_user.scan_workers = _clamp_int(payload.get('workers', DEFAULT_SCAN_WORKERS), 1, 16, DEFAULT_SCAN_WORKERS)
    current_user.live_workers_cap = _clamp_int(payload.get('liveWorkersCap', current_user.scan_workers), 1, 16, current_user.scan_workers)
    current_user.backtest_workers_cap = _clamp_int(payload.get('backtestWorkersCap', current_user.scan_workers), 1, 16, current_user.scan_workers)
    current_user.scan_profile = _normalize_profile(payload.get('scanProfile', 'balanced'))
    current_user.auto_tune_default = _coerce_bool(payload.get('autoTune'), default=True)
    current_user.live_prefilter_default = _coerce_bool(payload.get('livePrefilterEnabled'), default=True)
    current_user.live_prefilter_top_movers = _clamp_int(payload.get('livePrefilterTopMovers', 30), 0, 500, 30)
    current_user.live_prefilter_top_volume = _clamp_int(payload.get('livePrefilterTopVolume', 30), 0, 500, 30)
    current_user.live_prefilter_max_stocks = _clamp_int(payload.get('livePrefilterMaxStocks', 50), 1, 500, 50)
    current_user.backtest_rebalance_frequency = _normalize_rebalance_frequency(payload.get('backtestRebalanceFrequency', 'weekly'))
    current_user.backtest_strict_first_candle = _coerce_bool(payload.get('backtestStrictFirstCandle'), default=True)
    current_user.backtest_liquidity_filter_enabled = _coerce_bool(payload.get('backtestLiquidityFilterEnabled'), default=True)
    current_user.backtest_min_candles_per_strike = _clamp_int(payload.get('backtestMinCandlesPerStrike', 5), 2, 500, 5)
    current_user.backtest_min_avg_volume = _clamp_int(payload.get('backtestMinAvgVolume', 1), 0, 1_000_000, 1)
    current_user.guardrail_enabled = _coerce_bool(payload.get('guardrailEnabled'), default=True)
    current_user.guardrail_load_threshold = _clamp_int(payload.get('guardrailLoadThreshold', 1200), 100, 25000, 1200)
    current_user.notification_services_enabled = _coerce_bool(payload.get('notificationServicesEnabled'), default=True)
    token = str(raw_payload.get('telegramBotToken') or '').strip()
    channel_id = str(raw_payload.get('telegramChannelId') or '').strip()
    current_user.telegram_bot_token = token or None
    current_user.telegram_channel_id = channel_id or None

    db.session.commit()
    try:
        from app.services.telegram_notifier import invalidate_notification_pref_cache
        invalidate_notification_pref_cache(current_user.id)
    except Exception:
        pass
    return jsonify({
        'success': True,
        'message': 'Scanner configuration saved.',
        'config': payload
    })


@scanner_bp.route('/api/config/export', methods=['GET'])
@login_required
def export_scanner_config():
    cfg = _get_user_default_config(current_user.id) or _default_config_payload()
    payload = {
        'version': 1,
        'exported_at': datetime.utcnow().isoformat() + 'Z',
        'config': cfg
    }
    return jsonify({'success': True, 'payload': payload})


@scanner_bp.route('/api/config/import', methods=['POST'])
@login_required
def import_scanner_config():
    incoming = request.get_json(silent=True) or {}
    raw = incoming.get('payload') if isinstance(incoming.get('payload'), dict) else incoming
    if not isinstance(raw, dict):
        return jsonify({'success': False, 'error': 'Invalid import payload.'}), 400
    cfg_payload = raw.get('config') if isinstance(raw.get('config'), dict) else raw
    payload = _normalize_config_payload(cfg_payload)

    cfg = ScannerConfig.query.filter_by(user_id=current_user.id, is_default=True).first()
    if not cfg:
        cfg = ScannerConfig(
            user_id=current_user.id,
            config_name='Default Scanner Config',
            is_default=True
        )
        db.session.add(cfg)

    cfg.stock_selection = payload['stockSelection']
    cfg.strike_range = payload['strikeRange']
    cfg.live_exclude_atm_strikes = _clamp_int(payload.get('liveExcludeAtmStrikes', 0), 0, 10, 0)
    cfg.min_signal_volume = _clamp_int(payload.get('minSignalVolume', 10), 0, 1_000_000, 10)
    cfg.price_multiplier = payload['priceMultiplier']
    cfg.timeframe = f"{payload['timeframe']}min"
    cfg.refresh_interval = payload['refreshInterval'] * 60

    current_user.scan_workers = _clamp_int(payload.get('workers', DEFAULT_SCAN_WORKERS), 1, 16, DEFAULT_SCAN_WORKERS)
    current_user.live_workers_cap = _clamp_int(payload.get('liveWorkersCap', current_user.scan_workers), 1, 16, current_user.scan_workers)
    current_user.backtest_workers_cap = _clamp_int(payload.get('backtestWorkersCap', current_user.scan_workers), 1, 16, current_user.scan_workers)
    current_user.scan_profile = _normalize_profile(payload.get('scanProfile', 'balanced'))
    current_user.auto_tune_default = _coerce_bool(payload.get('autoTune'), default=True)
    current_user.live_prefilter_default = _coerce_bool(payload.get('livePrefilterEnabled'), default=True)
    current_user.live_prefilter_top_movers = _clamp_int(payload.get('livePrefilterTopMovers', 30), 0, 500, 30)
    current_user.live_prefilter_top_volume = _clamp_int(payload.get('livePrefilterTopVolume', 30), 0, 500, 30)
    current_user.live_prefilter_max_stocks = _clamp_int(payload.get('livePrefilterMaxStocks', 50), 1, 500, 50)
    current_user.backtest_rebalance_frequency = _normalize_rebalance_frequency(payload.get('backtestRebalanceFrequency', 'weekly'))
    current_user.backtest_strict_first_candle = _coerce_bool(payload.get('backtestStrictFirstCandle'), default=True)
    current_user.backtest_liquidity_filter_enabled = _coerce_bool(payload.get('backtestLiquidityFilterEnabled'), default=True)
    current_user.backtest_min_candles_per_strike = _clamp_int(payload.get('backtestMinCandlesPerStrike', 5), 2, 500, 5)
    current_user.backtest_min_avg_volume = _clamp_int(payload.get('backtestMinAvgVolume', 1), 0, 1_000_000, 1)
    current_user.guardrail_enabled = _coerce_bool(payload.get('guardrailEnabled'), default=True)
    current_user.guardrail_load_threshold = _clamp_int(payload.get('guardrailLoadThreshold', 1200), 100, 25000, 1200)
    current_user.notification_services_enabled = _coerce_bool(payload.get('notificationServicesEnabled'), default=True)
    if isinstance(cfg_payload, dict):
        token = str(cfg_payload.get('telegramBotToken') or '').strip()
        channel_id = str(cfg_payload.get('telegramChannelId') or '').strip()
        if token or channel_id:
            current_user.telegram_bot_token = token or None
            current_user.telegram_channel_id = channel_id or None

    db.session.commit()
    try:
        from app.services.telegram_notifier import invalidate_notification_pref_cache
        invalidate_notification_pref_cache(current_user.id)
    except Exception:
        pass
    return jsonify({'success': True, 'message': 'Configuration imported successfully.', 'config': payload})


@scanner_bp.route('/api/start-live', methods=['POST'])
@login_required
def start_live_scan():
    """Start live scanner"""
    try:
        _trace_live(f'start-live request user_id={current_user.id}')
        logger.info(f'Live start requested by user_id={current_user.id}')
        # Get scanner configuration from request
        config_data = request.get_json()
        success, message = start_live_scan_mgr(
            current_app._get_current_object(),
            current_user.id,
            config_data or {}
        )

        if success:
            _trace_live(f'start-live success user_id={current_user.id} message="{message}"')
            logger.info(f'Live start accepted for user_id={current_user.id}: {message}')
            return jsonify({
                'success': True,
                'message': message,
                'status': 'running'
            })

        _trace_live(f'start-live reject user_id={current_user.id} message="{message}"')
        logger.warning(f'Live start rejected for user_id={current_user.id}: {message}')
        return jsonify({
            'success': False,
            'error': message
        }), 400
        
    except Exception as e:
        logger.error(f"Error starting live scan: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to start scanner: {str(e)}'
        }), 500


@scanner_bp.route('/api/start-backtest', methods=['POST'])
@login_required
def start_backtest():
    """Start backtest"""
    try:
        # Get backtest configuration from request
        config_data = request.get_json()
        success, message = start_backtest_mgr(
            current_app._get_current_object(),
            current_user.id,
            config_data or {}
        )

        if success:
            return jsonify({
                'success': True,
                'message': message,
                'status': 'running'
            })

        return jsonify({
            'success': False,
            'error': message
        }), 400
        
    except Exception as e:
        logger.error(f"Error starting backtest: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to start backtest: {str(e)}'
        }), 500


@scanner_bp.route('/api/connect', methods=['POST'])
@login_required
def connect_session():
    """Connect to Angel One with optional TOTP and cache tokens."""
    try:
        payload = request.get_json() or {}
        mode = (payload.get('mode') or 'backtest').strip().lower()
        totp_code = (payload.get('totpCode') or '').strip() or None
        _trace_live(f'connect request user_id={current_user.id} mode={mode} has_totp={"yes" if totp_code else "no"}')
        logger.info(f'Connect session requested by user_id={current_user.id}, mode={mode}')

        success, message = connect_api_session_mgr(
            current_app._get_current_object(),
            current_user.id,
            mode=mode,
            totp_code=totp_code
        )

        if success:
            _trace_live(f'connect success user_id={current_user.id} mode={mode} message="{message}"')
            logger.info(f'Connect session success for user_id={current_user.id}, mode={mode}')
            return jsonify({
                'success': True,
                'message': message
            })

        _trace_live(f'connect reject user_id={current_user.id} mode={mode} message="{message}"')
        logger.warning(f'Connect session rejected for user_id={current_user.id}, mode={mode}: {message}')
        return jsonify({
            'success': False,
            'error': message
        }), 400
    except Exception as e:
        logger.error(f"Error connecting API session: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to connect API session: {str(e)}'
        }), 500


@scanner_bp.route('/api/stop', methods=['POST'])
@login_required
def stop_scanner():
    """Stop scanner"""
    try:
        success, message = stop_live_scan_mgr(current_user.id)

        if success:
            return jsonify({
                'success': True,
                'message': message
            })

        return jsonify({
            'success': False,
            'error': message
        }), 400
        
    except Exception as e:
        logger.error(f"Error stopping scanner: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to stop scanner: {str(e)}'
        }), 500


@scanner_bp.route('/api/stop-backtest', methods=['POST'])
@login_required
def stop_backtest():
    """Stop running backtest"""
    try:
        success, message = stop_backtest_mgr(current_user.id)
        if success:
            return jsonify({
                'success': True,
                'message': message
            })
        return jsonify({
            'success': False,
            'error': message
        }), 400
    except Exception as e:
        logger.error(f"Error stopping backtest: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to stop backtest: {str(e)}'
        }), 500


@scanner_bp.route('/api/test-telegram', methods=['POST'])
@login_required
def test_telegram():
    payload = request.get_json(silent=True) or {}
    text = (payload.get('text') or '').strip()
    if not text:
        stamp = datetime.utcnow().isoformat() + 'Z'
        text = f'Telegram test message @ {stamp}'
    try:
        from app.services.telegram_notifier import send_test_notification, get_telegram_notifier
        notifier = get_telegram_notifier()
        ok, reason, detail = send_test_notification(text, user_id=current_user.id)
        status = 200 if ok else 400
        token, channel = notifier.resolve_credentials(current_user.id)
        diag = {
            'enabled': bool(getattr(notifier, 'enabled', False)),
            'active': bool(notifier.is_active_for_user(current_user.id)),
            'live_only': bool(getattr(notifier, 'live_only', False)),
            'channel_id': channel or None,
            'token_suffix': token[-4:] if len(token) >= 4 else None,
            'notification_services_enabled': bool(getattr(current_user, 'notification_services_enabled', True))
        }
        return jsonify({
            'success': bool(ok),
            'reason': reason,
            'detail': detail,
            'diag': diag
        }), status
    except Exception as e:
        return jsonify({
            'success': False,
            'reason': f'exception:{str(e)}',
            'detail': None
        }), 500


@scanner_bp.route('/api/telegram-diagnostics', methods=['POST'])
@login_required
def telegram_diagnostics():
    try:
        from app.services.telegram_notifier import get_telegram_notifier
        notifier = get_telegram_notifier()
        token, channel = notifier.resolve_credentials(current_user.id)
        reason = 'ok' if notifier.is_active_for_user(current_user.id) else 'telegram_disabled_or_unconfigured'
        diag = {
            'enabled': bool(getattr(notifier, 'enabled', False)),
            'active': bool(notifier.is_active_for_user(current_user.id)),
            'live_only': bool(getattr(notifier, 'live_only', False)),
            'channel_id': channel or None,
            'token_suffix': token[-4:] if len(token) >= 4 else None,
            'notification_services_enabled': bool(getattr(current_user, 'notification_services_enabled', True))
        }
        status = 200 if diag['active'] else 400
        return jsonify({
            'success': bool(diag['active']),
            'reason': reason,
            'detail': None,
            'diag': diag
        }), status
    except Exception as e:
        return jsonify({
            'success': False,
            'reason': f'exception:{str(e)}',
            'detail': None
        }), 500


@scanner_bp.route('/api/test-telegram-signal', methods=['POST'])
@login_required
def test_telegram_signal():
    payload = request.get_json(silent=True) or {}
    symbol = (payload.get('symbol') or 'RELIANCE').strip().upper()
    option_type = (payload.get('option_type') or 'CE').strip().upper()
    timeframe = int(payload.get('timeframe') or 15)
    sample = {
        'symbol': symbol,
        'strike_price': float(payload.get('strike_price') or 2850),
        'option_type': option_type if option_type in ('CE', 'PE') else 'CE',
        'timeframe': timeframe,
        'entry_price': float(payload.get('entry_price') or 12.45),
        'current_price': float(payload.get('current_price') or 14.2),
        'price_change_percent': float(payload.get('price_change_percent') or 14.06),
        'volume': int(payload.get('volume') or 182340),
        'rsi': float(payload.get('rsi') or 63.8),
        'expiry_date': payload.get('expiry_date') or (datetime.utcnow().date().isoformat()),
        'detected_at': datetime.utcnow().isoformat() + 'Z'
    }
    try:
        from app.services.telegram_notifier import get_telegram_notifier
        notifier = get_telegram_notifier()
        ok, reason = notifier.notify_signal(sample, mode='live', user_id=current_user.id)
        token, channel = notifier.resolve_credentials(current_user.id)
        diag = {
            'active': bool(notifier.is_active_for_user(current_user.id)),
            'live_only': bool(getattr(notifier, 'live_only', False)),
            'channel_id': channel or None,
            'token_suffix': token[-4:] if len(token) >= 4 else None,
            'notification_services_enabled': bool(getattr(current_user, 'notification_services_enabled', True))
        }
        status = 200 if ok else 400
        return jsonify({
            'success': bool(ok),
            'reason': reason,
            'detail': notifier.get_last_send_detail(),
            'sample': sample,
            'diag': diag
        }), status
    except Exception as e:
        return jsonify({
            'success': False,
            'reason': f'exception:{str(e)}'
        }), 500


@scanner_bp.route('/api/strike-preview', methods=['POST'])
@login_required
def strike_preview():
    payload = request.get_json(silent=True) or {}
    symbol = str(payload.get('symbol') or '').strip().upper()
    if not symbol:
        return jsonify({'success': False, 'error': 'Symbol is required.'}), 400
    try:
        cmp_value = float(payload.get('cmp'))
    except Exception:
        return jsonify({'success': False, 'error': 'CMP (spot price) is required.'}), 400

    try:
        strike_range = int(payload.get('strikeRange', 0))
    except Exception:
        strike_range = 0
    strike_range = max(0, min(50, strike_range))

    try:
        max_preview = int(payload.get('maxPreview', 20))
    except Exception:
        max_preview = 20
    max_preview = max(0, min(200, max_preview))

    class _DummyConfig:
        def __init__(self, strike_range_value):
            self.id = None
            self.strike_range = strike_range_value
            self.timeframe = '15min'
            self.refresh_interval = 900

    try:
        api = AngelOneAPI(api_key='', client_id='', password='')
        if not api.load_scrip_master():
            return jsonify({'success': False, 'error': 'Scrip master not available.'}), 500
        engine = ScannerEngine(
            user_id=current_user.id,
            config=_DummyConfig(strike_range),
            angel_api=api,
            workers=1
        )
        expiry = engine._resolve_expiry(symbol, reference_date=datetime.utcnow())
        if not expiry:
            return jsonify({'success': False, 'error': 'Unable to resolve expiry for symbol.'}), 400
        strikes_by_type = engine._resolve_strikes_by_type(symbol, expiry, cmp_value)

        def _preview(strikes):
            if max_preview <= 0:
                return sorted(strikes)
            ordered = sorted(strikes, key=lambda s: (abs(float(s) - cmp_value), float(s)))
            trimmed = ordered[:max_preview]
            return sorted(set(float(s) for s in trimmed))

        ce_all = [float(s) for s in strikes_by_type.get('CE', [])]
        pe_all = [float(s) for s in strikes_by_type.get('PE', [])]
        response = {
            'success': True,
            'symbol': symbol,
            'cmp': float(cmp_value),
            'strikeRange': int(strike_range),
            'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
            'strikeStep': float(engine._strike_step(symbol)),
            'totals': {
                'ce': len(ce_all),
                'pe': len(pe_all)
            },
            'preview': {
                'ce': _preview(ce_all),
                'pe': _preview(pe_all)
            },
            'truncated': {
                'ce': len(ce_all) > max_preview if max_preview > 0 else False,
                'pe': len(pe_all) > max_preview if max_preview > 0 else False
            }
        }
        return jsonify(response)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@scanner_bp.route('/api/backtest/logs/latest', methods=['GET'])
@login_required
def download_latest_backtest_log_scanner():
    """Compatibility route for downloading latest backtest log."""
    logs_dir = Path('logs')
    if not logs_dir.exists() or not logs_dir.is_dir():
        return jsonify({'success': False, 'error': 'Log directory not found.'}), 404

    prefix = f'backtest_scan_user_{current_user.id}_'
    candidates = []
    for path in logs_dir.iterdir():
        if not path.is_file():
            continue
        if not path.name.startswith(prefix) or not path.name.endswith('.log'):
            continue
        candidates.append(path)

    if not candidates:
        return jsonify({'success': False, 'error': 'No backtest log file found for this user yet.'}), 404

    latest = max(candidates, key=lambda p: p.stat().st_mtime).resolve()
    if not latest.exists():
        return jsonify({'success': False, 'error': 'Latest backtest log file was not found on disk.'}), 404
    return send_file(
        latest,
        as_attachment=True,
        download_name=latest.name,
        mimetype='text/plain'
    )


@scanner_bp.route('/api/error-logs/latest', methods=['GET'])
@login_required
def download_latest_error_log_scanner():
    """Compatibility route for downloading latest API error log."""
    logs_dir = Path('logs')
    if not logs_dir.exists() or not logs_dir.is_dir():
        return jsonify({'success': False, 'error': 'Log directory not found.'}), 404

    prefix = f'api_error_user_{current_user.id}_'
    candidates = []
    for path in logs_dir.iterdir():
        if not path.is_file():
            continue
        if not path.name.startswith(prefix) or not path.name.endswith('.log'):
            continue
        candidates.append(path)

    if not candidates:
        return jsonify({'success': False, 'error': 'No API error log file found for this user yet.'}), 404

    latest = max(candidates, key=lambda p: p.stat().st_mtime).resolve()
    if not latest.exists():
        return jsonify({'success': False, 'error': 'Latest error log file was not found on disk.'}), 404
    return send_file(
        latest,
        as_attachment=True,
        download_name=latest.name,
        mimetype='text/plain'
    )


@scanner_bp.route('/api/backtest/logs/latest-csv', methods=['GET'])
@login_required
def download_latest_backtest_log_csv_scanner():
    """Compatibility route for downloading latest backtest CSV diagnostic log."""
    logs_dir = Path('logs')
    if not logs_dir.exists() or not logs_dir.is_dir():
        return jsonify({'success': False, 'error': 'Log directory not found.'}), 404

    prefix = f'backtest_scan_user_{current_user.id}_'
    candidates = []
    for path in logs_dir.iterdir():
        if not path.is_file():
            continue
        if not path.name.startswith(prefix) or not path.name.endswith('.csv'):
            continue
        candidates.append(path)

    if not candidates:
        return jsonify({'success': False, 'error': 'No backtest CSV log file found for this user yet.'}), 404

    latest = max(candidates, key=lambda p: p.stat().st_mtime).resolve()
    if not latest.exists():
        return jsonify({'success': False, 'error': 'Latest backtest CSV log file was not found on disk.'}), 404
    return send_file(
        latest,
        as_attachment=True,
        download_name=latest.name,
        mimetype='text/csv'
    )


@scanner_bp.route('/api/debug/start-live/<int:user_id>', methods=['POST'])
@csrf.exempt
def debug_start_live(user_id):
    """Debug-only: start live scan for a specific user without login/session."""
    if not current_app.debug:
        return jsonify({'success': False, 'error': 'Not found.'}), 404
    try:
        payload = request.get_json(silent=True) or {}
        _trace_live(f'debug start-live request user_id={user_id}')
        success, message = start_live_scan_mgr(
            current_app._get_current_object(),
            user_id,
            payload
        )
        _trace_live(
            f'debug start-live {"success" if success else "reject"} '
            f'user_id={user_id} message="{message}"'
        )
        status_code = 200 if success else 400
        return jsonify({'success': success, 'message': message}), status_code
    except Exception as e:
        _trace_live(f'debug start-live error user_id={user_id} err="{str(e)}"')
        return jsonify({'success': False, 'error': str(e)}), 500

