"""
Settings Routes
"""
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.api_credential import APICredential
from app.models.user import User
from app.utils.encryption import EncryptionHelper
from app.services.angel_api import (
    download_scrip_master,
    get_scrip_master_status,
    get_scrip_master_url,
    get_scrip_master_path,
    AngelOneAPI
)
import requests
import os
import time
import json
import re
from urllib.parse import urljoin
from smartapi import SmartConnect
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms.validators import DataRequired
from flask_wtf import FlaskForm
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

settings_bp = Blueprint('settings', __name__)
DEFAULT_SCAN_WORKERS = 8
MAX_SCAN_WORKERS = 16
DEFAULT_PROFILE = 'balanced'
VALID_SCAN_PROFILES = ('conservative', 'balanced', 'aggressive')
VALID_REBALANCE_FREQUENCIES = ('daily', 'weekly', 'monthly')


def _clamp_scan_workers(value, default=DEFAULT_SCAN_WORKERS):
    try:
        workers = int(value)
    except Exception:
        workers = default
    return max(1, min(MAX_SCAN_WORKERS, workers))


def _coerce_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def _form_bool(form, name, default=False):
    values = form.getlist(name)
    if not values:
        return default
    return _coerce_bool(values[-1], default=default)


def _clamp_int(value, minimum, maximum, default):
    try:
        num = int(value)
    except Exception:
        num = default
    return max(minimum, min(maximum, num))


def _normalized_profile(value):
    token = str(value or DEFAULT_PROFILE).strip().lower()
    if token not in VALID_SCAN_PROFILES:
        return DEFAULT_PROFILE
    return token


def _normalized_rebalance_frequency(value):
    token = str(value or 'weekly').strip().lower()
    if token not in VALID_REBALANCE_FREQUENCIES:
        return 'weekly'
    return token


def _suggested_scan_workers():
    cpu = os.cpu_count() or DEFAULT_SCAN_WORKERS
    # API-bound workload: keep recommendation moderate to avoid rate-limit pressure.
    return max(DEFAULT_SCAN_WORKERS, min(MAX_SCAN_WORKERS, cpu))


class APICredentialForm(FlaskForm):
    """API credentials form"""
    api_key_market = StringField('Market API Key', validators=[DataRequired()])
    api_key_historical = StringField('Historical API Key (Optional)')
    client_id = StringField('Client ID', validators=[DataRequired()])
    client_secret = PasswordField('Client Secret', validators=[DataRequired()])
    use_mpin = BooleanField('Use MPIN login')
    submit = SubmitField('Save Credentials')


@settings_bp.route('/api-credentials', methods=['GET', 'POST'])
@login_required
def api_credentials():
    """API credentials management"""
    form = APICredentialForm()
    
    # Get existing credentials if any
    existing_creds = APICredential.query.filter_by(user_id=current_user.id).first()
    masked = None

    def mask_secret(value, keep=4):
        if not value:
            return None
        value = str(value)
        if len(value) <= keep * 2:
            return '*' * len(value)
        return f"{value[:keep]}{'*' * (len(value) - keep * 2)}{value[-keep:]}"
    
    if form.validate_on_submit():
        # Encrypt credentials
        market_key = form.api_key_market.data
        historical_key = form.api_key_historical.data or market_key
        api_key_enc = EncryptionHelper.encrypt(market_key)
        api_key_market_enc = EncryptionHelper.encrypt(market_key)
        api_key_historical_enc = EncryptionHelper.encrypt(historical_key)
        client_id_enc = EncryptionHelper.encrypt(form.client_id.data)
        client_secret_enc = EncryptionHelper.encrypt(form.client_secret.data)
        totp_secret_enc = None
        use_mpin = bool(form.use_mpin.data)
        
        if existing_creds:
            # Update existing
            existing_creds.api_key_encrypted = api_key_enc
            existing_creds.api_key_market_encrypted = api_key_market_enc
            existing_creds.api_key_historical_encrypted = api_key_historical_enc
            existing_creds.client_id_encrypted = client_id_enc
            existing_creds.client_secret_encrypted = client_secret_enc
            existing_creds.totp_secret_encrypted = totp_secret_enc
            existing_creds.use_mpin = use_mpin
            existing_creds.is_validated = False  # Re-validate after update
        else:
            # Create new
            creds = APICredential(
                user_id=current_user.id,
                api_key_encrypted=api_key_enc,
                api_key_market_encrypted=api_key_market_enc,
                api_key_historical_encrypted=api_key_historical_enc,
                client_id_encrypted=client_id_enc,
                client_secret_encrypted=client_secret_enc,
                totp_secret_encrypted=totp_secret_enc,
                use_mpin=use_mpin
            )
            db.session.add(creds)
        
        db.session.commit()
        flash('API credentials saved successfully!', 'success')
        return redirect(url_for('settings.preferences'))

    api_status = {
        'market': False,
        'historical': False
    }

    if existing_creds:
        api_key_market = None
        api_key_historical = None
        client_id = None

        if existing_creds.api_key_market_encrypted:
            api_key_market = EncryptionHelper.decrypt(existing_creds.api_key_market_encrypted)
        if existing_creds.api_key_historical_encrypted:
            api_key_historical = EncryptionHelper.decrypt(existing_creds.api_key_historical_encrypted)
        if existing_creds.api_key_encrypted and not api_key_market:
            api_key_market = EncryptionHelper.decrypt(existing_creds.api_key_encrypted)
        if existing_creds.api_key_encrypted and not api_key_historical:
            api_key_historical = EncryptionHelper.decrypt(existing_creds.api_key_encrypted)
        if existing_creds.client_id_encrypted:
            client_id = EncryptionHelper.decrypt(existing_creds.client_id_encrypted)

        masked = {
            'market_key': mask_secret(api_key_market),
            'historical_key': mask_secret(api_key_historical),
            'client_id': mask_secret(client_id),
            'updated_at': existing_creds.updated_at
        }
        api_status['market'] = bool(api_key_market)
        api_status['historical'] = bool(api_key_historical)

        form.use_mpin.data = bool(existing_creds.use_mpin)

    scrip_status = get_scrip_master_status()
    scrip_url = get_scrip_master_url()
    scrip_path = get_scrip_master_path()

    return render_template(
        'settings/api_credentials.html',
        form=form,
        has_credentials=existing_creds is not None,
        masked=masked,
        api_status=api_status,
        scrip_status=scrip_status,
        scrip_url=scrip_url,
        scrip_path=scrip_path
    )


@settings_bp.route('/api-credentials/reveal', methods=['POST'])
@login_required
def reveal_credentials():
    """Reveal stored credentials (use carefully)."""
    creds = APICredential.query.filter_by(user_id=current_user.id).first()
    if not creds:
        return jsonify({'success': False, 'error': 'No credentials found.'}), 404

    api_key_market = None
    api_key_historical = None

    if creds.api_key_market_encrypted:
        api_key_market = EncryptionHelper.decrypt(creds.api_key_market_encrypted)
    if creds.api_key_historical_encrypted:
        api_key_historical = EncryptionHelper.decrypt(creds.api_key_historical_encrypted)
    if creds.api_key_encrypted and not api_key_market:
        api_key_market = EncryptionHelper.decrypt(creds.api_key_encrypted)
    if creds.api_key_encrypted and not api_key_historical:
        api_key_historical = EncryptionHelper.decrypt(creds.api_key_encrypted)

    client_id = EncryptionHelper.decrypt(creds.client_id_encrypted) if creds.client_id_encrypted else ''
    client_secret = EncryptionHelper.decrypt(creds.client_secret_encrypted) if creds.client_secret_encrypted else ''
    totp_secret = EncryptionHelper.decrypt(creds.totp_secret_encrypted) if creds.totp_secret_encrypted else ''

    return jsonify({
        'success': True,
        'credentials': {
            'market_key': api_key_market or '',
            'historical_key': api_key_historical or '',
            'client_id': client_id,
            'client_secret': client_secret,
            'use_mpin': bool(creds.use_mpin)
        }
    })


@settings_bp.route('/download-scrip-master', methods=['POST'])
@login_required
def download_scrip_master_route():
    """Download or refresh the scrip master file."""
    success, message, _ = download_scrip_master(force=True)
    flash(message, 'success' if success else 'danger')
    return redirect(url_for('settings.api_credentials'))


@settings_bp.route('/api-credentials/test', methods=['POST'])
@login_required
def test_api_credentials():
    """Test API credentials with optional TOTP code."""
    data = request.get_json() or {}
    mode = (data.get('mode') or 'market').lower()
    totp_code = data.get('totpCode')

    creds = APICredential.query.filter_by(user_id=current_user.id).first()
    if not creds:
        return jsonify({'success': False, 'error': 'No credentials found.'}), 404

    api_key_market = EncryptionHelper.decrypt(creds.api_key_market_encrypted) if creds.api_key_market_encrypted else None
    api_key_historical = EncryptionHelper.decrypt(creds.api_key_historical_encrypted) if creds.api_key_historical_encrypted else None
    api_key_legacy = EncryptionHelper.decrypt(creds.api_key_encrypted) if creds.api_key_encrypted else None

    if mode == 'historical':
        api_key = api_key_historical or api_key_market or api_key_legacy
    else:
        api_key = api_key_market or api_key_legacy or api_key_historical

    if not api_key:
        return jsonify({'success': False, 'error': 'API key is missing for the selected mode.'}), 400

    client_id = EncryptionHelper.decrypt(creds.client_id_encrypted) if creds.client_id_encrypted else ''
    client_secret = EncryptionHelper.decrypt(creds.client_secret_encrypted) if creds.client_secret_encrypted else ''

    api = AngelOneAPI(
        api_key=api_key,
        client_id=client_id,
        password=client_secret,
        totp_code=totp_code,
        use_mpin=bool(creds.use_mpin)
    )

    ok = api.authenticate()
    if not ok:
        return jsonify({
            'success': False,
            'error': api.last_error or 'Authentication failed.'
        }), 400

    return jsonify({'success': True, 'message': 'Authentication successful.'})


@settings_bp.route('/api-credentials/ping', methods=['POST'])
@login_required
def ping_api_roots():
    """Ping SmartAPI roots to validate connectivity."""
    configured_root = os.getenv('SMARTAPI_ROOT_URL')
    roots = []
    if configured_root:
        roots.append(configured_root)
    roots.extend([
        'https://apiconnect.angelone.in',
        'https://apiconnect.angelbroking.com'
    ])
    # Deduplicate while preserving order
    seen = set()
    roots = [root for root in roots if not (root in seen or seen.add(root))]
    path = '/rest/auth/angelbroking/user/v1/loginByPassword'
    results = []

    for root in roots:
        url = f'{root}{path}'
        try:
            started = time.time()
            method = 'HEAD'
            resp = requests.head(url, timeout=8)
            if resp.status_code in (400, 404, 405):
                method = 'GET'
                resp = requests.get(url, timeout=8, stream=True)
            elapsed_ms = int((time.time() - started) * 1000)
            results.append({
                'root': root,
                'url': url,
                'method': method,
                'status_code': resp.status_code,
                'content_length': resp.headers.get('Content-Length', 'unknown'),
                'content_type': resp.headers.get('Content-Type', 'unknown'),
                'server': resp.headers.get('Server', 'unknown'),
                'elapsed_ms': elapsed_ms
            })
            try:
                resp.close()
            except Exception:
                pass
        except Exception as e:
            results.append({
                'root': root,
                'url': url,
                'error': str(e)
            })

    return jsonify({
        'success': True,
        'configured_root': configured_root,
        'results': results
    })


@settings_bp.route('/api-credentials/time-check', methods=['GET'])
@login_required
def api_time_check():
    """Return server time and optional external UTC reference for drift checks."""
    server_utc = datetime.now(timezone.utc)
    server_epoch = int(server_utc.timestamp())
    external_epoch = None
    external_source = None
    external_error = None

    # Prefer a lightweight HEAD request to get a trusted Date header.
    for url in ['https://apiconnect.angelone.in', 'https://www.google.com']:
        try:
            resp = requests.head(url, timeout=5)
            date_header = resp.headers.get('Date')
            if date_header:
                dt = parsedate_to_datetime(date_header)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                external_epoch = int(dt.timestamp())
                external_source = url
                break
        except Exception as e:
            external_error = str(e)

    payload = {
        'success': True,
        'server_epoch': server_epoch,
        'server_utc_iso': server_utc.isoformat()
    }
    if external_epoch is not None:
        payload['external_epoch'] = external_epoch
        payload['external_source'] = external_source
        payload['server_vs_external_seconds'] = server_epoch - external_epoch
    elif external_error:
        payload['external_error'] = external_error

    return jsonify(payload)


def _sanitize_probe_response(text, content_type):
    if not text:
        return '<empty>'
    snippet = text.strip()
    if not snippet:
        return '<empty>'

    if content_type and 'json' in content_type.lower():
        try:
            payload = json.loads(snippet)
            def scrub(obj):
                if isinstance(obj, dict):
                    cleaned = {}
                    for key, value in obj.items():
                        key_lower = str(key).lower()
                        if key_lower in {
                            'jwttoken', 'refreshtoken', 'feedtoken',
                            'token', 'access_token', 'refresh_token'
                        }:
                            cleaned[key] = '***'
                        else:
                            cleaned[key] = scrub(value)
                    return cleaned
                if isinstance(obj, list):
                    return [scrub(item) for item in obj]
                return obj
            snippet = json.dumps(scrub(payload))
        except Exception:
            pass

    # redact long tokens/secrets
    snippet = re.sub(r'[A-Za-z0-9_\-]{24,}', '***', snippet)
    return snippet[:200]


@settings_bp.route('/api-credentials/probe', methods=['POST'])
@login_required
def probe_api_login():
    """Probe login endpoint with raw POST and report response metadata."""
    data = request.get_json() or {}
    mode = (data.get('mode') or 'market').lower()
    totp_code = (data.get('totpCode') or '').strip() or None

    creds = APICredential.query.filter_by(user_id=current_user.id).first()
    if not creds:
        return jsonify({'success': False, 'error': 'No credentials found.'}), 404

    api_key_market = EncryptionHelper.decrypt(creds.api_key_market_encrypted) if creds.api_key_market_encrypted else None
    api_key_historical = EncryptionHelper.decrypt(creds.api_key_historical_encrypted) if creds.api_key_historical_encrypted else None
    api_key_legacy = EncryptionHelper.decrypt(creds.api_key_encrypted) if creds.api_key_encrypted else None

    if mode == 'historical':
        api_key = api_key_historical or api_key_market or api_key_legacy
    else:
        api_key = api_key_market or api_key_legacy or api_key_historical

    if not api_key:
        return jsonify({'success': False, 'error': 'API key is missing for the selected mode.'}), 400

    client_id = EncryptionHelper.decrypt(creds.client_id_encrypted) if creds.client_id_encrypted else ''
    client_secret = EncryptionHelper.decrypt(creds.client_secret_encrypted) if creds.client_secret_encrypted else ''

    use_mpin = bool(creds.use_mpin)
    # Use loginByPassword for both password and PIN (MPIN) login.
    route = '/rest/auth/angelbroking/user/v1/loginByPassword'

    payload = {
        'clientcode': client_id,
        'password': client_secret
    }
    if totp_code:
        payload['totp'] = totp_code

    configured_root = os.getenv('SMARTAPI_ROOT_URL')
    roots = []
    if configured_root:
        roots.append(configured_root)
    roots.extend([
        'https://apiconnect.angelone.in',
        'https://apiconnect.angelbroking.com'
    ])
    seen = set()
    roots = [root for root in roots if not (root in seen or seen.add(root))]

    results = []
    client_headers = None
    for root in roots:
        url = urljoin(root, route)
        try:
            api = AngelOneAPI(
                api_key=api_key,
                client_id=client_id,
                password=client_secret,
                totp_code=totp_code,
                use_mpin=use_mpin
            )
            api.smart_api = SmartConnect(api_key=api_key, root=root)
            api._apply_client_headers()
            headers = api.smart_api.requestHeaders()
            headers['Content-type'] = 'application/json'
            headers['Accept'] = 'application/json'
            headers['User-Agent'] = 'OptionSignalPro/1.0'
            if client_headers is None:
                client_headers = {
                    'client_public_ip': headers.get('X-ClientPublicIP', ''),
                    'client_local_ip': headers.get('X-ClientLocalIP', ''),
                    'client_mac': headers.get('X-MACAddress', ''),
                    'user_type': headers.get('X-UserType', ''),
                    'source_id': headers.get('X-SourceID', '')
                }

            started = time.time()
            resp = requests.post(url, data=json.dumps(payload), headers=headers, timeout=12)
            elapsed_ms = int((time.time() - started) * 1000)
            content_type = resp.headers.get('Content-Type', 'unknown')
            text = resp.text or ''
            results.append({
                'root': root,
                'url': url,
                'status_code': resp.status_code,
                'content_length': resp.headers.get('Content-Length', str(len(resp.content or b''))),
                'content_type': content_type,
                'server': resp.headers.get('Server', 'unknown'),
                'elapsed_ms': elapsed_ms,
                'snippet': _sanitize_probe_response(text, content_type)
            })
        except Exception as e:
            results.append({
                'root': root,
                'url': url,
                'error': str(e)
            })

    return jsonify({
        'success': True,
        'configured_root': configured_root,
        'route': route,
        'login_mode': 'pin-via-password' if use_mpin else 'password',
        'client_headers': client_headers,
        'results': results
    })


@settings_bp.route('/preferences', methods=['GET', 'POST'])
@login_required
def preferences():
    """User preferences"""
    if request.method == 'POST':
        theme = request.form.get('theme', 'light')
        scan_workers = _clamp_scan_workers(request.form.get('scan_workers', DEFAULT_SCAN_WORKERS))
        live_workers_cap = _clamp_scan_workers(request.form.get('live_workers_cap', scan_workers), default=scan_workers)
        backtest_workers_cap = _clamp_scan_workers(request.form.get('backtest_workers_cap', scan_workers), default=scan_workers)
        scan_profile = _normalized_profile(request.form.get('scan_profile', DEFAULT_PROFILE))
        auto_tune_default = _form_bool(request.form, 'auto_tune_default', default=True)
        live_prefilter_default = _form_bool(request.form, 'live_prefilter_default', default=True)
        live_prefilter_top_movers = _clamp_int(request.form.get('live_prefilter_top_movers'), 0, 500, 30)
        live_prefilter_top_volume = _clamp_int(request.form.get('live_prefilter_top_volume'), 0, 500, 30)
        live_prefilter_max_stocks = _clamp_int(request.form.get('live_prefilter_max_stocks'), 1, 500, 50)
        live_fast_mode_enabled = _form_bool(request.form, 'live_fast_mode_enabled', default=True)
        live_fast_max_stocks = _clamp_int(request.form.get('live_fast_max_stocks'), 1, 1000, 60)
        live_fast_max_strikes_per_stock = _clamp_int(request.form.get('live_fast_max_strikes_per_stock'), 2, 500, 24)
        backtest_rebalance_frequency = _normalized_rebalance_frequency(request.form.get('backtest_rebalance_frequency', 'weekly'))
        backtest_strict_first_candle = _form_bool(request.form, 'backtest_strict_first_candle', default=True)
        backtest_liquidity_filter_enabled = _form_bool(request.form, 'backtest_liquidity_filter_enabled', default=True)
        backtest_min_candles_per_strike = _clamp_int(request.form.get('backtest_min_candles_per_strike'), 2, 500, 5)
        backtest_min_avg_volume = _clamp_int(request.form.get('backtest_min_avg_volume'), 0, 1_000_000, 1)
        guardrail_enabled = _form_bool(request.form, 'guardrail_enabled', default=True)
        guardrail_load_threshold = _clamp_int(request.form.get('guardrail_load_threshold'), 100, 25000, 1200)
        backtest_scan_log_enabled = _form_bool(request.form, 'backtest_scan_log_enabled', default=False)
        api_error_log_enabled = _form_bool(request.form, 'api_error_log_enabled', default=False)
        current_user.theme_preference = theme
        current_user.scan_workers = scan_workers
        current_user.live_workers_cap = live_workers_cap
        current_user.backtest_workers_cap = backtest_workers_cap
        current_user.scan_profile = scan_profile
        current_user.auto_tune_default = auto_tune_default
        current_user.live_prefilter_default = live_prefilter_default
        current_user.live_prefilter_top_movers = live_prefilter_top_movers
        current_user.live_prefilter_top_volume = live_prefilter_top_volume
        current_user.live_prefilter_max_stocks = live_prefilter_max_stocks
        current_user.live_fast_mode_enabled = live_fast_mode_enabled
        current_user.live_fast_max_stocks = live_fast_max_stocks
        current_user.live_fast_max_strikes_per_stock = live_fast_max_strikes_per_stock
        current_user.backtest_rebalance_frequency = backtest_rebalance_frequency
        current_user.backtest_strict_first_candle = backtest_strict_first_candle
        current_user.backtest_liquidity_filter_enabled = backtest_liquidity_filter_enabled
        current_user.backtest_min_candles_per_strike = backtest_min_candles_per_strike
        current_user.backtest_min_avg_volume = backtest_min_avg_volume
        current_user.guardrail_enabled = guardrail_enabled
        current_user.guardrail_load_threshold = guardrail_load_threshold
        current_user.backtest_scan_log_enabled = backtest_scan_log_enabled
        current_user.api_error_log_enabled = api_error_log_enabled
        db.session.commit()
        flash('Preferences updated!', 'success')
        return redirect(url_for('dashboard.index'))
    
    return render_template(
        'settings/preferences.html',
        user=current_user,
        default_scan_workers=DEFAULT_SCAN_WORKERS,
        max_scan_workers=MAX_SCAN_WORKERS,
        suggested_scan_workers=_suggested_scan_workers(),
        default_profile=DEFAULT_PROFILE,
        scan_profiles=VALID_SCAN_PROFILES,
        rebalance_frequencies=VALID_REBALANCE_FREQUENCIES
    )


@settings_bp.route('/preferences/sound', methods=['GET'])
@login_required
def get_sound_preferences():
    """Return notification sound preferences for current user."""
    return jsonify({
        'success': True,
        'preferences': {
            'enabled': bool(getattr(current_user, 'notification_sound_enabled', True)),
            'source': getattr(current_user, 'notification_sound_source', 'beep') or 'beep',
            'data_url': getattr(current_user, 'notification_sound_data', None),
            'filename': getattr(current_user, 'notification_sound_filename', None)
        }
    })


@settings_bp.route('/preferences/sound', methods=['POST'])
@login_required
def save_sound_preferences():
    """Persist notification sound preferences for current user."""
    payload = request.get_json(silent=True) or {}
    source = str(payload.get('source') or 'beep').strip().lower()
    enabled = payload.get('enabled', True)
    data_url = payload.get('data_url')
    filename = payload.get('filename')
    clear_custom = bool(payload.get('clear_custom', False))

    if source not in ('beep', 'custom'):
        return jsonify({'success': False, 'error': 'Invalid sound source.'}), 400

    current_user.notification_sound_enabled = bool(enabled)

    if clear_custom:
        current_user.notification_sound_source = 'beep'
        current_user.notification_sound_data = None
        current_user.notification_sound_filename = None
        db.session.commit()
        return jsonify({
            'success': True,
            'preferences': {
                'enabled': bool(current_user.notification_sound_enabled),
                'source': 'beep',
                'data_url': None,
                'filename': None
            }
        })

    if source == 'custom':
        existing_data = getattr(current_user, 'notification_sound_data', None)
        if data_url:
            if not str(data_url).startswith('data:audio/'):
                return jsonify({'success': False, 'error': 'Invalid custom sound payload.'}), 400
            if len(str(data_url)) > 2_000_000:
                return jsonify({'success': False, 'error': 'Custom sound file is too large. Use a shorter clip.'}), 400
            current_user.notification_sound_data = str(data_url)
            current_user.notification_sound_filename = (str(filename or '')[:255] or 'custom-sound')
        elif not existing_data:
            return jsonify({'success': False, 'error': 'Please upload a custom sound file first.'}), 400
        current_user.notification_sound_source = 'custom'
    else:
        current_user.notification_sound_source = 'beep'
        # Keep existing custom sound in DB for later reuse unless explicitly cleared.

    db.session.commit()
    return jsonify({
        'success': True,
        'preferences': {
            'enabled': bool(current_user.notification_sound_enabled),
            'source': current_user.notification_sound_source or 'beep',
            'data_url': current_user.notification_sound_data,
            'filename': current_user.notification_sound_filename
        }
    })
