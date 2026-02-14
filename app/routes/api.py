"""
API Routes for AJAX and WebSocket
"""
from flask import Blueprint, jsonify, request, Response
from flask_login import login_required, current_user
from app import socketio, db
from app.services.scanner_manager import (
    start_live_scan,
    stop_live_scan,
    get_scan_status,
    get_backtest_status,
    _build_angel_api,
    _store_tokens
)
from flask import current_app
from flask_socketio import emit, join_room, leave_room
from app.models.signal import Signal
from app.models.signal_detail import SignalDetail
from app.models.api_credential import APICredential
from app.models.scanner_config import ScannerConfig
from datetime import datetime, timedelta, timezone
import csv
import io
import os
import re
from sqlalchemy import inspect as sa_inspect

api_bp = Blueprint('api', __name__)
LIVE_TRACE_FILE = os.path.join('logs', 'live_trace.log')
_signal_details_table_cached = None
IST_TZ = timezone(timedelta(hours=5, minutes=30))


def _trace_live(message):
    try:
        os.makedirs('logs', exist_ok=True)
        stamp = datetime.utcnow().isoformat() + 'Z'
        line = f'{stamp} pid={os.getpid()} {message}\n'
        with open(LIVE_TRACE_FILE, 'a', encoding='utf-8') as handle:
            handle.write(line)
    except Exception:
        pass


def _latest_scanner_config(mode):
    return ScannerConfig.query.filter(
        ScannerConfig.user_id == current_user.id,
        ScannerConfig.config_name.like(f'{mode.capitalize()} %')
    ).order_by(ScannerConfig.id.desc()).first()


def _signals_query(mode, scanner_config_id=None, latest_config=False):
    query = Signal.query.filter_by(user_id=current_user.id, mode=mode)
    if scanner_config_id:
        return query.filter_by(scanner_config_id=scanner_config_id)
    if latest_config:
        latest_cfg = _latest_scanner_config(mode)
        if latest_cfg:
            return query.filter_by(scanner_config_id=latest_cfg.id)
    return query


def _signal_details_table_exists():
    global _signal_details_table_cached
    if _signal_details_table_cached is not None:
        return _signal_details_table_cached
    try:
        inspector = sa_inspect(db.engine)
        exists = inspector.has_table('signal_details')
        if not exists:
            SignalDetail.__table__.create(bind=db.engine, checkfirst=True)
            exists = sa_inspect(db.engine).has_table('signal_details')
        _signal_details_table_cached = bool(exists)
    except Exception:
        _signal_details_table_cached = False
    return _signal_details_table_cached


def _load_signal_details(signals):
    if not signals or not _signal_details_table_exists():
        return {}
    signal_ids = [s.id for s in signals if getattr(s, 'id', None)]
    if not signal_ids:
        return {}
    try:
        rows = SignalDetail.query.filter(SignalDetail.signal_id.in_(signal_ids)).all()
    except Exception:
        return {}
    return {row.signal_id: row for row in rows}


def _extract_strike_from_symbol(symbol):
    if not symbol:
        return None
    text = str(symbol).strip().upper()
    # Preferred Angel format: UNDERLYING + DD + MMM + YY + STRIKE + CE/PE
    match = re.match(r'^(.*?)(\d{2})([A-Z]{3})(\d{2})(\d+(?:\.\d+)?)(CE|PE)$', text)
    if match:
        try:
            return float(match.group(5))
        except Exception:
            return None
    # Fallback for non-standard symbols
    tail = re.search(r'(\d+(?:\.\d+)?)(CE|PE)$', text)
    if not tail:
        return None
    try:
        return float(tail.group(1))
    except Exception:
        return None


def _format_strike_display(raw_strike, symbol=None):
    if raw_strike is None:
        return None
    try:
        strike = float(raw_strike)
    except Exception:
        return raw_strike

    sym_strike = _extract_strike_from_symbol(symbol)
    if sym_strike is not None:
        candidates = [strike, strike / 100.0, strike / 10.0, strike * 10.0, strike * 100.0]
        strike = min(candidates, key=lambda val: abs(val - sym_strike))
    elif strike >= 10000 and abs(strike % 100) < 0.001:
        # Fallback normalization when symbol parse fails but strike looks 100x scaled.
        strike = strike / 100.0

    return round(float(strike), 2)


def _timeframe_to_minutes(value, default=5):
    if value is None:
        return default
    text = str(value).strip().lower()
    if text.endswith('min'):
        text = text[:-3]
    text = text.replace('minutes', '').strip()
    try:
        minutes = int(text)
        return minutes if minutes > 0 else default
    except Exception:
        return default


def _to_utc_naive(value):
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _format_ist_datetime(value):
    if not value:
        return ''
    dt = value
    if isinstance(dt, str):
        try:
            text = dt.strip()
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            dt = datetime.fromisoformat(text)
        except Exception:
            return str(value)
    if not isinstance(dt, datetime):
        return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ist_dt = dt.astimezone(IST_TZ)
    return ist_dt.strftime('%d/%m/%Y, %I:%M:%S %p IST')


def _format_contract_symbol(symbol):
    if not symbol:
        return ''
    text = str(symbol).strip().upper()
    match = re.match(r'^(.*?)(\d{2})([A-Z]{3})(\d{2})(\d+(?:\.\d+)?)(CE|PE)$', text)
    if not match:
        return text
    underlying = (match.group(1) or '').strip()
    day = match.group(2)
    month = match.group(3)
    year = match.group(4)
    strike = match.group(5)
    option_type = match.group(6)
    return f'{underlying} {day} {month} {year} {strike} {option_type}'


def _format_number_2dp(value):
    if value is None or value == '':
        return ''
    try:
        return f'{float(value):.2f}'
    except Exception:
        return str(value)


def _format_volume(value):
    if value is None or value == '':
        return ''
    try:
        return str(int(float(value)))
    except Exception:
        return str(value)


def _format_strike_csv(value):
    if value is None or value == '':
        return ''
    try:
        num = float(value)
        if abs(num - int(num)) < 0.0001:
            return str(int(num))
        return f'{num:.2f}'.rstrip('0').rstrip('.')
    except Exception:
        return str(value)


def _serialize_signal_row(signal, detail=None):
    row = signal.to_dict()
    row['strike_price'] = _format_strike_display(row.get('strike_price'), row.get('symbol'))
    if detail:
        row['candle_open'] = float(detail.candle_open) if detail.candle_open is not None else None
        row['candle_high'] = float(detail.candle_high) if detail.candle_high is not None else None
        row['candle_low'] = float(detail.candle_low) if detail.candle_low is not None else None
        row['candle_close'] = float(detail.candle_close) if detail.candle_close is not None else None
        row['displayed_at'] = detail.displayed_at.isoformat() + 'Z' if detail.displayed_at else None
    else:
        row['displayed_at'] = None
    return row


@api_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    db_status = 'ok'
    try:
        db.session.execute(db.text('SELECT 1'))
    except Exception as e:
        db_status = f'error: {str(e)}'

    status = 'ok' if db_status == 'ok' else 'degraded'
    code = 200 if status == 'ok' else 503

    return jsonify({
        'status': status,
        'db': db_status,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), code


@api_bp.route('/signals/recent', methods=['GET'])
@login_required
def get_recent_signals():
    """Get recent signals for current user"""
    limit = request.args.get('limit', 50, type=int)
    mode = (request.args.get('mode', 'live') or 'live').lower()
    if mode not in ('live', 'backtest'):
        return jsonify({'success': False, 'error': 'Invalid mode.'}), 400
    scanner_config_id = request.args.get('scanner_config_id', type=int)
    latest_config = request.args.get('latest_config', '1').lower() in ('1', 'true', 'yes')

    query = _signals_query(mode, scanner_config_id=scanner_config_id, latest_config=latest_config)

    signals = query.order_by(Signal.detected_at.desc()).limit(limit).all()
    detail_map = _load_signal_details(signals)
    payload = []
    for s in signals:
        payload.append(_serialize_signal_row(s, detail_map.get(s.id)))
    
    return jsonify({
        'success': True,
        'signals': payload
    })


@api_bp.route('/signals/counts', methods=['GET'])
@login_required
def get_signal_counts():
    """Get signal counts for current user."""
    latest_config = request.args.get('latest_config', '0').lower() in ('1', 'true', 'yes')

    if latest_config:
        live_cfg = ScannerConfig.query.filter(
            ScannerConfig.user_id == current_user.id,
            ScannerConfig.config_name.like('Live %')
        ).order_by(ScannerConfig.id.desc()).first()
        backtest_cfg = ScannerConfig.query.filter(
            ScannerConfig.user_id == current_user.id,
            ScannerConfig.config_name.like('Backtest %')
        ).order_by(ScannerConfig.id.desc()).first()

        live_count = Signal.query.filter_by(
            user_id=current_user.id,
            mode='live',
            scanner_config_id=live_cfg.id if live_cfg else -1
        ).count()
        backtest_count = Signal.query.filter_by(
            user_id=current_user.id,
            mode='backtest',
            scanner_config_id=backtest_cfg.id if backtest_cfg else -1
        ).count()
    else:
        live_count = Signal.query.filter_by(user_id=current_user.id, mode='live').count()
        backtest_count = Signal.query.filter_by(user_id=current_user.id, mode='backtest').count()

    return jsonify({
        'success': True,
        'scope': 'latest_config' if latest_config else 'all_time',
        'counts': {
            'live': live_count,
            'backtest': backtest_count,
            'total': live_count + backtest_count
        }
    })


@api_bp.route('/scanner/configs', methods=['GET'])
@login_required
def get_scanner_configs():
    """Get user's scanner configurations"""
    configs = ScannerConfig.query.filter_by(user_id=current_user.id).all()
    
    return jsonify({
        'success': True,
        'configs': [{
            'id': c.id,
            'name': c.config_name,
            'stock_selection': c.get_stock_selection(),
            'strike_range': c.strike_range,
            'price_multiplier': float(c.price_multiplier),
            'timeframe': c.timeframe,
            'refresh_interval': c.refresh_interval,
            'is_default': c.is_default
        } for c in configs]
    })


@api_bp.route('/scanner/status', methods=['GET'])
@login_required
def scanner_status():
    """Get scanner and backtest status for current user"""
    live = get_scan_status(current_user.id)
    backtest = get_backtest_status(current_user.id)
    _trace_live(
        'status poll '
        f'user_id={current_user.id} '
        f'live_running={bool((live or {}).get("running"))} '
        f'backtest_running={bool((backtest or {}).get("running"))}'
    )
    return jsonify({
        'success': True,
        'live': live,
        'backtest': backtest
    })


@api_bp.route('/debug/scanner/status/<int:user_id>', methods=['GET'])
def debug_scanner_status(user_id):
    """Debug-only: inspect scanner status for a user without auth session."""
    if not current_app.debug:
        return jsonify({'success': False, 'error': 'Not found.'}), 404
    live = get_scan_status(user_id)
    backtest = get_backtest_status(user_id)
    _trace_live(
        f'debug status read user_id={user_id} '
        f'live_running={bool((live or {}).get("running"))} '
        f'backtest_running={bool((backtest or {}).get("running"))}'
    )
    return jsonify({
        'success': True,
        'user_id': user_id,
        'server_pid': os.getpid(),
        'live': live,
        'backtest': backtest
    })


@api_bp.route('/signals/export.csv', methods=['GET'])
@login_required
def export_signals_csv():
    """Download signals as CSV for current user."""
    mode = (request.args.get('mode', 'live') or 'live').lower()
    if mode not in ('live', 'backtest'):
        return jsonify({'success': False, 'error': 'Invalid mode.'}), 400

    scanner_config_id = request.args.get('scanner_config_id', type=int)
    latest_config = request.args.get('latest_config', '1').lower() in ('1', 'true', 'yes')

    query = _signals_query(mode, scanner_config_id=scanner_config_id, latest_config=latest_config)
    signals = query.order_by(Signal.detected_at.desc()).all()
    detail_map = _load_signal_details(signals)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'Time', 'Stock', 'Strike', 'Type',
        'Prev Close (Entry Basis)', 'Signal Close (Current)',
        'Open', 'High', 'Low', 'Close',
        'Vol', 'RSI', '% Change', 'Status', 'Displayed At'
    ])
    for s in signals:
        row = _serialize_signal_row(s, detail_map.get(s.id))
        writer.writerow([
            _format_ist_datetime(row.get('detected_at', '')),
            _format_contract_symbol(row.get('symbol', '')),
            _format_strike_csv(row.get('strike_price', '')),
            row.get('option_type', ''),
            _format_number_2dp(row.get('entry_price', '')),
            _format_number_2dp(row.get('current_price', '')),
            _format_number_2dp(row.get('candle_open', '')),
            _format_number_2dp(row.get('candle_high', '')),
            _format_number_2dp(row.get('candle_low', '')),
            _format_number_2dp(row.get('candle_close', '')),
            _format_volume(row.get('volume', '')),
            _format_number_2dp(row.get('rsi', '')),
            _format_number_2dp(row.get('price_change_percent', '')),
            'Backtest' if row.get('mode') == 'backtest' else 'Live',
            _format_ist_datetime(row.get('displayed_at') or (datetime.utcnow().isoformat() + 'Z'))
        ])

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f'signals_{mode}_{timestamp}.csv'
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


@api_bp.route('/signals/backfill-ohlc', methods=['POST'])
@login_required
def backfill_signals_ohlc():
    """Backfill OHLC/displayed_at for existing signals missing signal_details."""
    if not _signal_details_table_exists():
        return jsonify({'success': False, 'error': 'signal_details table is unavailable.'}), 500

    payload = request.get_json(silent=True) or {}
    mode = str(payload.get('mode') or 'all').strip().lower()
    limit = min(max(int(payload.get('limit') or 300), 1), 2000)
    if mode not in ('all', 'live', 'backtest'):
        return jsonify({'success': False, 'error': 'Invalid mode. Use all/live/backtest.'}), 400

    creds = APICredential.query.filter_by(user_id=current_user.id).first()
    if not creds:
        return jsonify({'success': False, 'error': 'API credentials not configured.'}), 400

    angel_api, error = _build_angel_api(creds, mode='backtest', totp_code=None)
    if not angel_api:
        return jsonify({'success': False, 'error': f'Failed to authenticate. {error}'}), 400
    _store_tokens(creds, angel_api)

    query = (
        db.session.query(Signal)
        .outerjoin(SignalDetail, SignalDetail.signal_id == Signal.id)
        .filter(Signal.user_id == current_user.id, SignalDetail.id.is_(None))
    )
    if mode in ('live', 'backtest'):
        query = query.filter(Signal.mode == mode)
    signals = query.order_by(Signal.detected_at.desc()).limit(limit).all()

    updated = 0
    skipped = 0
    failures = 0

    for signal in signals:
        try:
            instrument = angel_api.lookup_instrument(signal.symbol, exchange='NFO')
            if not instrument:
                skipped += 1
                continue

            minutes = _timeframe_to_minutes(signal.timeframe, default=5)
            detected_at = signal.detected_at or datetime.utcnow()
            from_date = detected_at - timedelta(minutes=minutes * 4)
            to_date = detected_at + timedelta(minutes=minutes * 2)
            candles = angel_api.get_historical_data(
                symbol=signal.symbol,
                timeframe=minutes,
                from_date=from_date,
                to_date=to_date,
                exchange='NFO',
                symbol_token=instrument['token']
            )
            if not candles:
                skipped += 1
                continue

            detected_utc = _to_utc_naive(detected_at) or detected_at
            best_candle = None
            best_score = None
            for candle in candles:
                if len(candle) < 5:
                    continue
                ts = angel_api._parse_candle_time(candle[0])
                if ts is None:
                    continue
                ts_utc = _to_utc_naive(ts) or ts
                score = abs((ts_utc - detected_utc).total_seconds())
                if best_score is None or score < best_score:
                    best_candle = candle
                    best_score = score

            if not best_candle:
                skipped += 1
                continue

            detail = SignalDetail(
                signal_id=signal.id,
                candle_open=float(best_candle[1]) if len(best_candle) > 1 else None,
                candle_high=float(best_candle[2]) if len(best_candle) > 2 else None,
                candle_low=float(best_candle[3]) if len(best_candle) > 3 else None,
                candle_close=float(best_candle[4]) if len(best_candle) > 4 else None,
                displayed_at=signal.detected_at or datetime.utcnow()
            )
            db.session.add(detail)
            updated += 1
        except Exception:
            failures += 1

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Backfill failed: {str(e)}'}), 500

    return jsonify({
        'success': True,
        'processed': len(signals),
        'updated': updated,
        'skipped': skipped,
        'failed': failures
    })


# WebSocket Events
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    if current_user.is_authenticated:
        join_room(f'user_{current_user.id}')
        emit('connected', {'status': 'connected'})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    if current_user.is_authenticated:
        leave_room(f'user_{current_user.id}')


@socketio.on('start_live_scan')
def handle_start_live_scan(data):
    """Start live scanning"""
    if not current_user.is_authenticated:
        emit('error', {'message': 'Not authenticated'})
        return
    
    success, message = start_live_scan(
        current_app._get_current_object(),
        current_user.id,
        data or {}
    )

    if success:
        emit('scan_started', {
            'status': 'started',
            'message': message
        })
    else:
        emit('error', {'message': message})


@socketio.on('stop_live_scan')
def handle_stop_live_scan():
    """Stop live scanning"""
    if not current_user.is_authenticated:
        emit('error', {'message': 'Not authenticated'})
        return
    
    success, message = stop_live_scan(current_user.id)
    if success:
        emit('scan_stopped', {'status': 'stopped'})
    else:
        emit('error', {'message': message})
