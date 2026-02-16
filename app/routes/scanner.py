"""
Scanner Routes
"""
from flask import Blueprint, render_template, jsonify, request
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

scanner_bp = Blueprint('scanner', __name__)
logger = logging.getLogger(__name__)

ALLOWED_TIMEFRAMES = {1, 3, 5, 15, 30, 60}
LIVE_TRACE_FILE = os.path.join('logs', 'live_trace.log')
DEFAULT_SCAN_WORKERS = 8
VALID_SCAN_PROFILES = {'conservative', 'balanced', 'aggressive'}


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
        'strikeRange': 2,
        'priceMultiplier': 2.0,
        'timeframe': 5,
        'refreshInterval': 5,
        'workers': DEFAULT_SCAN_WORKERS,
        'autoTune': True,
        'scanProfile': 'balanced',
        'livePrefilterEnabled': True,
        'livePrefilterTopMovers': 30,
        'livePrefilterTopVolume': 30,
        'livePrefilterMaxStocks': 50,
        'liveWorkersCap': DEFAULT_SCAN_WORKERS,
        'backtestWorkersCap': DEFAULT_SCAN_WORKERS,
        'guardrailEnabled': True,
        'guardrailLoadThreshold': 1200
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
        'guardrailEnabled': bool(getattr(current_user, 'guardrail_enabled', True)),
        'guardrailLoadThreshold': _clamp_int(getattr(current_user, 'guardrail_load_threshold', 1200), 100, 25000, 1200)
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
        payload['strikeRange'] = max(1, min(20, int(data.get('strikeRange', payload['strikeRange']))))
    except Exception:
        pass
    try:
        payload['priceMultiplier'] = max(1.0, float(data.get('priceMultiplier', payload['priceMultiplier'])))
    except Exception:
        pass
    try:
        timeframe = int(data.get('timeframe', payload['timeframe']))
        payload['timeframe'] = timeframe if timeframe in ALLOWED_TIMEFRAMES else 5
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
    return payload


def _get_user_default_config(user_id):
    cfg = ScannerConfig.query.filter_by(user_id=user_id, is_default=True).first()
    if not cfg:
        return None
    stock_selection = cfg.get_stock_selection()
    return {
        'stockSelection': stock_selection if stock_selection else 'all',
        'strikeRange': int(cfg.strike_range or 5),
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
    return jsonify({
        'success': True,
        'config': cfg or default_cfg
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
    payload = _normalize_config_payload(request.get_json() or {})
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
    current_user.guardrail_enabled = _coerce_bool(payload.get('guardrailEnabled'), default=True)
    current_user.guardrail_load_threshold = _clamp_int(payload.get('guardrailLoadThreshold', 1200), 100, 25000, 1200)

    db.session.commit()
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
    current_user.guardrail_enabled = _coerce_bool(payload.get('guardrailEnabled'), default=True)
    current_user.guardrail_load_threshold = _clamp_int(payload.get('guardrailLoadThreshold', 1200), 100, 25000, 1200)

    db.session.commit()
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

