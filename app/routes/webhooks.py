"""
Webhook routes (scheduler-driven triggers).
"""
import os
from flask import Blueprint, request, jsonify, current_app
from app import csrf
from app.models.user import User
from app.services.scanner_manager import trigger_webhook_scan

webhooks_bp = Blueprint('webhooks', __name__)


def _resolve_user(payload):
    user_id = payload.get('user_id')
    username = payload.get('username') or payload.get('user')
    if user_id:
        try:
            return User.query.get(int(user_id))
        except Exception:
            return None
    if username:
        token = str(username).strip()
        return User.query.filter(
            (User.username == token) | (User.email == token)
        ).first()
    default_user = os.getenv('WEBHOOK_DEFAULT_USER', '').strip()
    if default_user:
        return User.query.filter(
            (User.username == default_user) | (User.email == default_user)
        ).first()
    return None


def _auth_failed():
    return jsonify({'success': False, 'error': 'Unauthorized webhook token.'}), 401


@webhooks_bp.route('/candle-close', methods=['POST'])
@csrf.exempt
def webhook_candle_close():
    token_required = os.getenv('WEBHOOK_TOKEN', '').strip()
    if token_required:
        provided = request.headers.get('X-Webhook-Token', '').strip()
        if not provided or provided != token_required:
            return _auth_failed()

    payload = request.get_json(silent=True) or {}
    symbol = str(payload.get('symbol') or '').strip().upper()
    if not symbol:
        return jsonify({'success': False, 'error': 'symbol is required.'}), 400

    timeframe = payload.get('timeframe') or payload.get('timeframe_min')
    candle_time = payload.get('candle_time') or payload.get('candleTime')
    strike_range = payload.get('strike_range') or payload.get('strikeRange')

    user = _resolve_user(payload)
    if not user:
        return jsonify({'success': False, 'error': 'User not found for webhook.'}), 404

    ok, message, job_id = trigger_webhook_scan(
        app=current_app._get_current_object(),
        user_id=user.id,
        symbol=symbol,
        timeframe=timeframe,
        candle_time=candle_time,
        strike_range=strike_range
    )
    status = 202 if ok else 400
    return jsonify({'success': ok, 'message': message, 'job_id': job_id}), status
