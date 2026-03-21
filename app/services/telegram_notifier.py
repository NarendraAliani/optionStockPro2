"""
Telegram notification service for scanner signals.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from html import escape as html_escape
from threading import Lock
from time import sleep, time
from typing import Any, Dict, Optional, Tuple
import logging
import os

import requests

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - fallback for uncommon runtime issues
    ZoneInfo = None

try:
    from flask import current_app, has_app_context
except Exception:  # pragma: no cover - defensive import guard
    current_app = None

    def has_app_context() -> bool:
        return False


logger = logging.getLogger(__name__)

_TRUE_VALUES = {'1', 'true', 'yes', 'on'}


def _cfg(name: str, default: Any = None) -> Any:
    if has_app_context():
        try:
            return current_app.config.get(name, os.getenv(name, default))
        except Exception:
            pass
    return os.getenv(name, default)


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in _TRUE_VALUES


def _as_int(value: Any, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def _as_float(value: Any, default: float, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


class TelegramNotifier:
    def __init__(self) -> None:
        self.enabled = _as_bool(_cfg('TELEGRAM_ENABLED', False), default=False)
        self.bot_token = str(_cfg('TELEGRAM_BOT_TOKEN', '') or '').strip()
        self.channel_id = str(_cfg('TELEGRAM_CHANNEL_ID', '') or '').strip()
        self.cycle_summary_enabled_default = _as_bool(_cfg('TELEGRAM_CYCLE_SUMMARY_ENABLED', True), default=True)
        self.cycle_summary_mode_default = str(_cfg('TELEGRAM_CYCLE_SUMMARY_MODE', 'SHORT') or 'SHORT').strip().upper()
        self.cycle_summary_channel_default = str(_cfg('TELEGRAM_CYCLE_SUMMARY_CHANNEL_ID', '') or '').strip()
        self.live_only = _as_bool(_cfg('TELEGRAM_LIVE_ONLY', True), default=True)
        self.message_mode = str(_cfg('TELEGRAM_MESSAGE_MODE', 'SHORT') or 'SHORT').strip().upper()
        self.parse_mode = str(_cfg('TELEGRAM_PARSE_MODE', 'HTML') or 'HTML').strip().upper()
        self.max_messages_per_min = _as_int(_cfg('TELEGRAM_MAX_MESSAGES_PER_MIN', 20), 20, minimum=1, maximum=500)
        self.retry_count = _as_int(_cfg('TELEGRAM_RETRY_COUNT', 2), 2, minimum=0, maximum=10)
        self.retry_delay_seconds = _as_float(_cfg('TELEGRAM_RETRY_DELAY_SECONDS', 3), 3.0, minimum=0.0, maximum=60.0)
        self.cooldown_minutes = _as_int(_cfg('TELEGRAM_COOLDOWN_MINUTES', 15), 15, minimum=0, maximum=24 * 60)
        self.app_env = str(_cfg('APP_ENV', 'LIVE') or 'LIVE').strip().upper()
        self.timezone_name = str(_cfg('TIMEZONE', 'Asia/Kolkata') or 'Asia/Kolkata').strip()
        self.user_pref_cache_seconds = _as_int(
            _cfg('TELEGRAM_USER_PREF_CACHE_SECONDS', 30),
            30,
            minimum=0,
            maximum=3600
        )
        self._send_times = deque()
        self._last_sent_by_key: Dict[str, float] = {}
        self._user_pref_cache: Dict[int, Tuple[bool, float]] = {}
        self._lock = Lock()
        self._session = requests.Session()
        self._last_send_detail: Optional[Dict[str, Any]] = None

    def is_active(self) -> bool:
        return bool(self.enabled and self.bot_token and self.channel_id)

    def resolve_credentials(self, user_id: Optional[int] = None) -> Tuple[str, str]:
        bot_token = self.bot_token
        channel_id = self.channel_id

        if has_app_context() and isinstance(user_id, int):
            try:
                from app.models.user import User
                user = User.query.get(int(user_id))
                if user is not None:
                    token_override = str(getattr(user, 'telegram_bot_token', '') or '').strip()
                    channel_override = str(getattr(user, 'telegram_channel_id', '') or '').strip()
                    if token_override:
                        bot_token = token_override
                    if channel_override:
                        channel_id = channel_override
            except Exception:
                pass

        return bot_token, channel_id

    def is_active_for_user(self, user_id: Optional[int] = None) -> bool:
        if not self.enabled:
            return False
        bot_token, channel_id = self.resolve_credentials(user_id)
        return bool(bot_token and channel_id)

    def resolve_cycle_summary_prefs(self, user_id: Optional[int] = None) -> Tuple[bool, str, str]:
        enabled = self.cycle_summary_enabled_default
        mode = self.cycle_summary_mode_default
        channel_override = self.cycle_summary_channel_default
        if has_app_context() and isinstance(user_id, int):
            try:
                from app.models.user import User
                user = User.query.get(int(user_id))
                if user is not None:
                    enabled = bool(getattr(user, 'telegram_cycle_summary_enabled', enabled))
                    mode = str(getattr(user, 'telegram_cycle_summary_mode', mode) or mode).strip().upper()
                    override = str(getattr(user, 'telegram_cycle_summary_channel_id', '') or '').strip()
                    if override:
                        channel_override = override
            except Exception:
                pass
        if mode not in ('SHORT', 'DETAILED'):
            mode = 'SHORT'
        return bool(enabled), mode, channel_override

    def notify_signal(self, signal: Dict[str, Any], mode: str = 'live', user_id: Optional[int] = None) -> Tuple[bool, str]:
        signal_mode = 'backtest' if str(mode or '').strip().lower() == 'backtest' else 'live'
        if not self.is_active_for_user(user_id):
            return False, 'telegram_disabled_or_unconfigured'
        if self.live_only and signal_mode != 'live':
            return False, 'telegram_live_only'
        if not self._user_notifications_enabled(user_id):
            return False, 'notification_services_disabled'

        try:
            detected_at = signal.get('detected_at')
        except Exception:
            detected_at = None
        message = self._build_message(signal or {}, signal_mode, user_id=user_id)
        key = self._cooldown_key(signal or {}, signal_mode, user_id=user_id)
        allowed, reason = self._allow_send(key)
        if not allowed:
            return False, reason

        bot_token, channel_id = self.resolve_credentials(user_id)
        endpoint = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        payload = {
            'chat_id': channel_id,
            'text': message,
            'disable_web_page_preview': True
        }
        if self.parse_mode in ('HTML', 'MARKDOWN', 'MARKDOWNV2'):
            payload['parse_mode'] = self.parse_mode

        attempts = max(1, self.retry_count + 1)
        for attempt in range(1, attempts + 1):
            try:
                response = self._session.post(endpoint, json=payload, timeout=10)
                if response.status_code == 200:
                    body = response.json() if response.content else {}
                    if bool(body.get('ok', False)):
                        self._mark_sent(key)
                        try:
                            stamp = datetime.utcnow().isoformat() + 'Z'
                            logger.info(
                                'Telegram sent at %s mode=%s user=%s symbol=%s strike=%s detected_at=%s',
                                stamp,
                                signal_mode,
                                str(user_id),
                                str(signal.get('symbol') or ''),
                                str(signal.get('strike_price') or ''),
                                str(detected_at or '')
                            )
                        except Exception:
                            pass
                        return True, 'sent'
                    self._last_send_detail = {
                        'status_code': response.status_code,
                        'body': body,
                        'text': response.text[:500]
                    }
                    logger.warning('Telegram API returned non-ok payload: %s', body)
                else:
                    self._last_send_detail = {
                        'status_code': response.status_code,
                        'body': None,
                        'text': response.text[:500]
                    }
                    logger.warning('Telegram API HTTP %s: %s', response.status_code, response.text[:300])
            except Exception as exc:
                self._last_send_detail = {'exception': str(exc)}
                logger.warning('Telegram send attempt %s/%s failed: %s', attempt, attempts, exc)

            if attempt < attempts and self.retry_delay_seconds > 0:
                sleep(self.retry_delay_seconds)

        return False, 'send_failed'

    def get_last_send_detail(self) -> Optional[Dict[str, Any]]:
        return self._last_send_detail

    def _user_notifications_enabled(self, user_id: Optional[int]) -> bool:
        if not isinstance(user_id, int):
            return True

        now = time()
        with self._lock:
            cached = self._user_pref_cache.get(int(user_id))
            if cached and cached[1] > now:
                return bool(cached[0])

        enabled = True
        if has_app_context():
            try:
                from app.models.user import User
                user = User.query.get(int(user_id))
                if user is not None:
                    enabled = bool(getattr(user, 'notification_services_enabled', True))
            except Exception:
                enabled = True

        ttl = max(0, self.user_pref_cache_seconds)
        with self._lock:
            self._user_pref_cache[int(user_id)] = (bool(enabled), now + float(ttl))
        return bool(enabled)

    def invalidate_user_pref_cache(self, user_id: Optional[int] = None) -> None:
        with self._lock:
            if isinstance(user_id, int):
                self._user_pref_cache.pop(int(user_id), None)
            else:
                self._user_pref_cache.clear()

    def _allow_send(self, key: str) -> Tuple[bool, str]:
        now = time()
        cutoff = now - 60.0
        cooldown_seconds = float(self.cooldown_minutes) * 60.0
        with self._lock:
            while self._send_times and self._send_times[0] < cutoff:
                self._send_times.popleft()

            if len(self._send_times) >= self.max_messages_per_min:
                return False, 'rate_limited'

            if cooldown_seconds > 0:
                last_sent = self._last_sent_by_key.get(key)
                if last_sent and (now - last_sent) < cooldown_seconds:
                    return False, 'cooldown'

        return True, 'allowed'

    def _mark_sent(self, key: str) -> None:
        now = time()
        with self._lock:
            self._send_times.append(now)
            self._last_sent_by_key[key] = now

    def _cooldown_key(self, signal: Dict[str, Any], mode: str, user_id: Optional[int] = None) -> str:
        symbol = str(signal.get('symbol') or '').strip().upper()
        strike = str(signal.get('strike_price') if signal.get('strike_price') is not None else '').strip()
        option_type = str(signal.get('option_type') or '').strip().upper()
        timeframe = str(signal.get('timeframe') or '').strip().lower()
        user_part = str(user_id) if user_id is not None else 'na'
        return '|'.join([self.app_env, user_part, mode, symbol, strike, option_type, timeframe])

    def _build_message(self, signal: Dict[str, Any], mode: str, user_id: Optional[int] = None) -> str:
        if self.parse_mode == 'HTML':
            return self._build_html_message(signal, mode, user_id=user_id)
        return self._build_plain_text_message(signal, mode, user_id=user_id)

    def _build_cycle_summary_message(self, summary: Dict[str, Any], mode: str = 'SHORT') -> str:
        mode = str(mode or 'SHORT').strip().upper()
        title = 'LIVE CYCLE COMPLETE'
        tf = html_escape(str(summary.get('timeframe') or '-'))
        multiplier = html_escape(self._fmt_num(summary.get('multiplier')))
        duration = html_escape(self._fmt_num(summary.get('duration_seconds')))
        stocks = html_escape(f"{summary.get('stocks_scanned', 0)}/{summary.get('stocks_total', 0)}")
        strikes = html_escape(f"{summary.get('strikes_scanned', 0)}/{summary.get('strikes_total', 0)}")
        signals = html_escape(str(summary.get('signals', 0)))
        skipped = html_escape(str(summary.get('skipped_candles', 0)))
        breakdown = summary.get('skip_breakdown') or {}
        breakdown_text = (
            f"Prefilter {breakdown.get('prefilter', 0)}, "
            f"CMP {breakdown.get('cmp', 0)}, "
            f"No Quote {breakdown.get('missing_quotes', 0)}, "
            f"No Expiry {breakdown.get('missing_expiry', 0)}, "
            f"No Spot {breakdown.get('missing_spot', 0)}, "
            f"Fast Stocks {breakdown.get('fast_stocks', 0)}, "
            f"Fast Strikes {breakdown.get('fast_strikes', 0)}"
        )
        breakdown_text = html_escape(breakdown_text)

        if self.parse_mode == 'HTML':
            if mode == 'SHORT':
                return (
                    f'<b>{title}</b>\n'
                    f'TF: <b>{tf}</b> | Mult: <b>{multiplier}</b> | Dur: <b>{duration}s</b>\n'
                    f'Stocks: <b>{stocks}</b> | Strikes: <b>{strikes}</b> | Signals: <b>{signals}</b>\n'
                    f'Skip Candles: <b>{skipped}</b>'
                )
            return (
                f'<b>{title}</b>\n'
                f'Timeframe: <b>{tf}</b>\n'
                f'Multiplier: <b>{multiplier}</b>\n'
                f'Duration: <b>{duration}s</b>\n'
                f'Stocks: <b>{stocks}</b>\n'
                f'Strikes: <b>{strikes}</b>\n'
                f'Signals: <b>{signals}</b>\n'
                f'Skip Candles: <b>{skipped}</b>\n'
                f'Skip Breakdown: {breakdown_text}'
            )

        if mode == 'SHORT':
            return (
                f'{title}\n'
                f'TF: {tf} | Mult: {multiplier} | Dur: {duration}s\n'
                f'Stocks: {stocks} | Strikes: {strikes} | Signals: {signals}\n'
                f'Skip Candles: {skipped}'
            )
        return (
            f'{title}\n'
            f'Timeframe: {tf}\n'
            f'Multiplier: {multiplier}\n'
            f'Duration: {duration}s\n'
            f'Stocks: {stocks}\n'
            f'Strikes: {strikes}\n'
            f'Signals: {signals}\n'
            f'Skip Candles: {skipped}\n'
            f'Skip Breakdown: {breakdown_text}'
        )

    def _build_html_message(self, signal: Dict[str, Any], mode: str, user_id: Optional[int] = None) -> str:
        env_text = html_escape(self.app_env)
        contract_label = html_escape(
            self._fmt_contract_label(
                signal.get('symbol'),
                signal.get('strike_price'),
                signal.get('option_type')
            )
        )
        timeframe = html_escape(str(signal.get('timeframe') or '-'))
        entry = html_escape(self._fmt_num(signal.get('entry_price')))
        current = html_escape(self._fmt_num(signal.get('current_price')))
        spot = html_escape(self._fmt_num(signal.get('spot_price')))
        change_pct = html_escape(self._fmt_pct(signal.get('price_change_percent')))
        volume = html_escape(self._fmt_num(signal.get('volume')))
        rsi = html_escape(self._fmt_num(signal.get('rsi')))
        expiry = html_escape(self._fmt_expiry(signal.get('expiry_date')))
        time_block = self._build_signal_time_block(signal, mode)
        signal_title = f'🚨🚨 <b>{mode.upper()} TRADE ALERT</b> 🚨🚨'
        env_block = ''
        if self.app_env and self.app_env != 'LIVE':
            env_block = f'<code>{env_text}</code>\n\n'

        if self.message_mode == 'SHORT':
            return (
                f'{signal_title}\n'
                f'{env_block}'
                f'🎯 <b>{contract_label}</b>\n'
                f'⏱️ Timeframe: <b>{timeframe}</b>\n\n'
                f'💰 Entry Zone: <b>{entry}</b>\n'
                f'📈 LTP: <b>{current}</b>\n'
                f'🎯 CMP: <b>{spot}</b>\n'
                f'🚀 Move: <b>{change_pct}</b>\n\n'
                f'📊 Volume: {volume}\n'
                f'📉 RSI: {rsi}\n'
                f'📅 Expiry: {expiry}\n\n'
                f'{time_block}'
            )

        return (
            f'{signal_title}\n'
            f'{env_block}'
            f'🎯 <b>{contract_label}</b>\n'
            f'⏱️ Timeframe: <b>{timeframe}</b>\n\n'
            f'💰 Entry Zone: <b>{entry}</b>\n'
            f'📈 LTP: <b>{current}</b>\n'
            f'🎯 CMP: <b>{spot}</b>\n'
            f'🚀 Move: <b>{change_pct}</b>\n\n'
            f'📊 Volume: {volume}\n'
            f'📉 RSI: {rsi}\n'
            f'📅 Expiry: {expiry}\n\n'
            f'{time_block}'
        )

    def _build_plain_text_message(self, signal: Dict[str, Any], mode: str, user_id: Optional[int] = None) -> str:
        contract_label = self._fmt_contract_label(
            signal.get('symbol'),
            signal.get('strike_price'),
            signal.get('option_type')
        )
        lines = [
            f'🚨 {mode.upper()} SIGNAL [{self.app_env}]',
            f'📌 {contract_label}',
            f'🕒 Timeframe: {signal.get("timeframe", "-")}',
            f'💰 Entry: {self._fmt_num(signal.get("entry_price"))}',
            f'📈 Current: {self._fmt_num(signal.get("current_price"))}',
            f'🎯 CMP: {self._fmt_num(signal.get("spot_price"))}',
            f'🔺 Change: {self._fmt_pct(signal.get("price_change_percent"))}',
            f'📊 Volume: {self._fmt_num(signal.get("volume"))}',
            f'📉 RSI: {self._fmt_num(signal.get("rsi"))}',
            self._build_signal_time_block(signal, mode, plain_text=True)
        ]
        if self.message_mode != 'SHORT':
            lines.insert(2, f'📅 Expiry: {self._fmt_expiry(signal.get("expiry_date"))}')
        return '\n'.join(lines)

    def _build_signal_time_block(self, signal: Dict[str, Any], mode: str, plain_text: bool = False) -> str:
        live_logic = str(signal.get('live_candle_logic') or '').strip().lower()
        detected_at = self._fmt_time(signal.get('detected_at'))

        if mode == 'live' and live_logic == 'cmp':
            previous_candle_time = self._fmt_time(signal.get('previous_candle_time'))
            candle_time = self._fmt_time(signal.get('candle_time'))
            if previous_candle_time != '-' and candle_time != '-':
                if plain_text:
                    return (
                        f'Candle Time: {previous_candle_time} to {candle_time}\n'
                        f'⏰ Signal Time: {detected_at}'
                    )
                return (
                    f'⏰ Candle Time: <b>{html_escape(previous_candle_time)} to {html_escape(candle_time)}</b>\n'
                    f'⏰ Signal Time: <b>{html_escape(detected_at)}</b>'
                )

        previous_candle_time = self._fmt_time(signal.get('previous_candle_time'))
        candle_time = self._fmt_time(signal.get('candle_time'))
        if previous_candle_time != '-' and candle_time != '-':
            if plain_text:
                return (
                    f'Previous Candle Time: {previous_candle_time}\n'
                    f'Current Candle Time: {candle_time}\n'
                    f'⏰ Signal Time: {detected_at}'
                )
            return (
                f'Previous Candle Time: <b>{html_escape(previous_candle_time)}</b>\n'
                f'Current Candle Time: <b>{html_escape(candle_time)}</b>\n'
                f'⏰ Signal Time: <b>{html_escape(detected_at)}</b>'
            )

        if plain_text:
            return f'⏰ Signal Time: {detected_at}'
        return f'⏰ Signal Time: <b>{html_escape(detected_at)}</b>'

    def _fmt_contract_label(self, symbol: Any, strike_price: Any, option_type: Any) -> str:
        symbol_text = str(symbol or '').strip().upper()
        strike_text = self._fmt_num(strike_price)
        option_text = str(option_type or '').strip().upper() or '-'
        if not symbol_text:
            return f'- {strike_text} {option_text}'.strip()

        try:
            import re
            match = re.match(r'^(.*?)(\d{2})([A-Z]{3})(\d{2})(\d+(?:\.\d+)?)(CE|PE)$', symbol_text)
        except Exception:
            match = None
        if match:
            underlying = match.group(1).strip()
            if underlying:
                return f'{underlying} {strike_text} {option_text}'.strip()
        return f'{symbol_text} {strike_text} {option_text}'.strip()

    def _fmt_pct(self, value: Any) -> str:
        if value in (None, ''):
            return '-'
        try:
            num = float(value)
            return f'{num:.2f}%'
        except Exception:
            return f'{value}%'

    def _fmt_expiry(self, value: Any) -> str:
        if value in (None, ''):
            return '-'
        if isinstance(value, datetime):
            return value.date().isoformat()
        return str(value)

    def _fmt_time(self, value: Any) -> str:
        parsed = self._parse_dt(value)
        if not parsed:
            return '-'
        try:
            target_tz = ZoneInfo(self.timezone_name) if ZoneInfo else timezone.utc
            parsed = parsed.astimezone(target_tz)
            suffix = self.timezone_name
        except Exception:
            parsed = parsed.astimezone(timezone.utc)
            suffix = 'UTC'
        return parsed.strftime('%Y-%m-%d %I:%M:%S %p') + f' ({suffix})'

    def _parse_dt(self, value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            dt = value
        elif value in (None, ''):
            return None
        else:
            text = str(value).strip()
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            try:
                dt = datetime.fromisoformat(text)
            except Exception:
                return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


_notifier_instance: Optional[TelegramNotifier] = None
_notifier_lock = Lock()


def get_telegram_notifier() -> TelegramNotifier:
    global _notifier_instance
    if _notifier_instance is None:
        with _notifier_lock:
            if _notifier_instance is None:
                _notifier_instance = TelegramNotifier()
    return _notifier_instance


def invalidate_notification_pref_cache(user_id: Optional[int] = None) -> None:
    try:
        notifier = get_telegram_notifier()
        notifier.invalidate_user_pref_cache(user_id)
    except Exception:
        pass


def send_signal_notification(signal: Dict[str, Any], mode: str = 'live', user_id: Optional[int] = None) -> bool:
    try:
        notifier = get_telegram_notifier()
        sent, reason = notifier.notify_signal(signal or {}, mode=mode, user_id=user_id)
        if not sent and reason not in (
            'telegram_disabled_or_unconfigured',
            'telegram_live_only',
            'cooldown',
            'notification_services_disabled'
        ):
            logger.info('Telegram notification skipped: %s', reason)
        return bool(sent)
    except Exception as exc:
        logger.warning('Telegram notification error: %s', exc)
        return False


def send_cycle_summary_notification(text: str, user_id: Optional[int] = None) -> Tuple[bool, str]:
    """Send a live cycle summary message to Telegram without applying cooldown."""
    try:
        notifier = get_telegram_notifier()
        signal_mode = 'live'
        if not notifier.is_active_for_user(user_id):
            return False, 'telegram_disabled_or_unconfigured'
        if notifier.live_only and signal_mode != 'live':
            return False, 'telegram_live_only'
        if not notifier._user_notifications_enabled(user_id):
            return False, 'notification_services_disabled'
        enabled, mode, channel_override = notifier.resolve_cycle_summary_prefs(user_id)
        if not enabled:
            return False, 'cycle_summary_disabled'

        bot_token, channel_id = notifier.resolve_credentials(user_id)
        if channel_override:
            channel_id = channel_override
        endpoint = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        if isinstance(text, dict):
            message = notifier._build_cycle_summary_message(text, mode=mode)
        else:
            message = str(text or '')
        payload = {
            'chat_id': channel_id,
            'text': message,
            'disable_web_page_preview': True
        }
        if notifier.parse_mode in ('HTML', 'MARKDOWN', 'MARKDOWNV2'):
            payload['parse_mode'] = notifier.parse_mode

        response = notifier._session.post(endpoint, json=payload, timeout=10)
        body = response.json() if response.content else {}
        if response.status_code == 200 and bool(body.get('ok', False)):
            return True, 'sent'
        return False, f'http_{response.status_code}'
    except Exception as exc:
        return False, f'exception:{exc}'


def send_test_notification(text: str, user_id: Optional[int] = None) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    try:
        notifier = get_telegram_notifier()
        signal_mode = 'live'
        if not notifier.is_active_for_user(user_id):
            return False, 'telegram_disabled_or_unconfigured', None
        if notifier.live_only and signal_mode != 'live':
            return False, 'telegram_live_only', None
        if not notifier._user_notifications_enabled(user_id):
            return False, 'notification_services_disabled', None

        bot_token, channel_id = notifier.resolve_credentials(user_id)
        endpoint = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        payload = {
            'chat_id': channel_id,
            'text': text,
            'disable_web_page_preview': True
        }
        if notifier.parse_mode in ('HTML', 'MARKDOWN', 'MARKDOWNV2'):
            payload['parse_mode'] = notifier.parse_mode

        response = notifier._session.post(endpoint, json=payload, timeout=10)
        body = response.json() if response.content else {}
        if response.status_code == 200 and bool(body.get('ok', False)):
            return True, 'sent', body
        return False, f'http_{response.status_code}', body
    except Exception as exc:
        return False, f'exception:{exc}', None

    def _fmt_num(self, value: Any) -> str:
        if value in (None, ''):
            return '-'
        try:
            num = float(value)
            if abs(num) >= 100:
                return f'{num:.2f}'.rstrip('0').rstrip('.')
            return f'{num:.3f}'.rstrip('0').rstrip('.')
        except Exception:
            return str(value)

    def _fmt_pct(self, value: Any) -> str:
        if value in (None, ''):
            return '-'
        try:
            num = float(value)
            return f'{num:.2f}%'
        except Exception:
            return f'{value}%'

    def _fmt_expiry(self, value: Any) -> str:
        if value in (None, ''):
            return '-'
        if isinstance(value, datetime):
            return value.date().isoformat()
        return str(value)

    def _fmt_time(self, value: Any) -> str:
        parsed = self._parse_dt(value)
        if not parsed:
            return '-'
        try:
            target_tz = ZoneInfo(self.timezone_name) if ZoneInfo else timezone.utc
            parsed = parsed.astimezone(target_tz)
            suffix = self.timezone_name
        except Exception:
            parsed = parsed.astimezone(timezone.utc)
            suffix = 'UTC'
        return parsed.strftime('%Y-%m-%d %I:%M:%S %p') + f' ({suffix})'

    def _parse_dt(self, value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            dt = value
        elif value in (None, ''):
            return None
        else:
            text = str(value).strip()
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            try:
                dt = datetime.fromisoformat(text)
            except Exception:
                return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


_notifier_instance: Optional[TelegramNotifier] = None
_notifier_lock = Lock()


def get_telegram_notifier() -> TelegramNotifier:
    global _notifier_instance
    if _notifier_instance is None:
        with _notifier_lock:
            if _notifier_instance is None:
                _notifier_instance = TelegramNotifier()
    return _notifier_instance


def invalidate_notification_pref_cache(user_id: Optional[int] = None) -> None:
    try:
        notifier = get_telegram_notifier()
        notifier.invalidate_user_pref_cache(user_id)
    except Exception:
        pass


def send_signal_notification(signal: Dict[str, Any], mode: str = 'live', user_id: Optional[int] = None) -> bool:
    try:
        notifier = get_telegram_notifier()
        sent, reason = notifier.notify_signal(signal or {}, mode=mode, user_id=user_id)
        if not sent and reason not in (
            'telegram_disabled_or_unconfigured',
            'telegram_live_only',
            'cooldown',
            'notification_services_disabled'
        ):
            logger.info('Telegram notification skipped: %s', reason)
        return bool(sent)
    except Exception as exc:
        logger.warning('Telegram notification error: %s', exc)
        return False


def send_cycle_summary_notification(text: str, user_id: Optional[int] = None) -> Tuple[bool, str]:
    """Send a live cycle summary message to Telegram without applying cooldown."""
    try:
        notifier = get_telegram_notifier()
        signal_mode = 'live'
        if not notifier.is_active_for_user(user_id):
            return False, 'telegram_disabled_or_unconfigured'
        if notifier.live_only and signal_mode != 'live':
            return False, 'telegram_live_only'
        if not notifier._user_notifications_enabled(user_id):
            return False, 'notification_services_disabled'
        enabled, mode, channel_override = notifier.resolve_cycle_summary_prefs(user_id)
        if not enabled:
            return False, 'cycle_summary_disabled'

        bot_token, channel_id = notifier.resolve_credentials(user_id)
        if channel_override:
            channel_id = channel_override
        endpoint = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        if isinstance(text, dict):
            message = notifier._build_cycle_summary_message(text, mode=mode)
        else:
            message = str(text or '')
        payload = {
            'chat_id': channel_id,
            'text': message,
            'disable_web_page_preview': True
        }
        if notifier.parse_mode in ('HTML', 'MARKDOWN', 'MARKDOWNV2'):
            payload['parse_mode'] = notifier.parse_mode

        response = notifier._session.post(endpoint, json=payload, timeout=10)
        body = response.json() if response.content else {}
        if response.status_code == 200 and bool(body.get('ok', False)):
            return True, 'sent'
        return False, f'http_{response.status_code}'
    except Exception as exc:
        return False, f'exception:{exc}'


def send_test_notification(text: str, user_id: Optional[int] = None) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    try:
        notifier = get_telegram_notifier()
        signal_mode = 'live'
        if not notifier.is_active_for_user(user_id):
            return False, 'telegram_disabled_or_unconfigured', None
        if notifier.live_only and signal_mode != 'live':
            return False, 'telegram_live_only', None
        if not notifier._user_notifications_enabled(user_id):
            return False, 'notification_services_disabled', None

        bot_token, channel_id = notifier.resolve_credentials(user_id)
        endpoint = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        payload = {
            'chat_id': channel_id,
            'text': text,
            'disable_web_page_preview': True
        }
        if notifier.parse_mode in ('HTML', 'MARKDOWN', 'MARKDOWNV2'):
            payload['parse_mode'] = notifier.parse_mode

        response = notifier._session.post(endpoint, json=payload, timeout=10)
        body = response.json() if response.content else {}
        if response.status_code == 200 and bool(body.get('ok', False)):
            return True, 'sent', body
        return False, f'http_{response.status_code}', body
    except Exception as exc:
        return False, f'exception:{exc}', None

