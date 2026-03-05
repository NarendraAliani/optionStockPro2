"""
SmartAPI WebSocket streaming manager for live candle pairs.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock, Thread
import logging
import os
import time
from typing import Dict, Optional, Tuple

try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except Exception:
    SmartWebSocketV2 = None

logger = logging.getLogger(__name__)


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def _as_int(value, default, minimum=None, maximum=None):
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


@dataclass
class _CandleState:
    end_time: datetime
    open: float
    high: float
    low: float
    close: float


class _CandleAggregator:
    def __init__(self, interval_minutes: int) -> None:
        self.interval_minutes = max(1, int(interval_minutes))
        self.current: Optional[_CandleState] = None
        self.last_closed: Optional[_CandleState] = None
        self.prev_closed: Optional[_CandleState] = None

    def update(self, ts: datetime, price: float) -> None:
        end_time = _bucket_end(ts, self.interval_minutes)
        if self.current is None:
            self.current = _CandleState(end_time, price, price, price, price)
            return
        if end_time == self.current.end_time:
            self.current.close = price
            if price > self.current.high:
                self.current.high = price
            if price < self.current.low:
                self.current.low = price
            return
        if end_time > self.current.end_time:
            self.prev_closed = self.last_closed
            self.last_closed = self.current
            self.current = _CandleState(end_time, price, price, price, price)

    def get_pair(self) -> Optional[Tuple[float, float, datetime]]:
        if not self.last_closed or not self.prev_closed:
            return None
        return self.prev_closed.close, self.last_closed.close, self.last_closed.end_time


def _bucket_end(ts: datetime, interval_minutes: int) -> datetime:
    interval = max(1, int(interval_minutes))
    minutes = ts.hour * 60 + ts.minute
    bucket_end_min = ((minutes + interval - 1) // interval) * interval
    day = ts.date()
    if bucket_end_min >= 24 * 60:
        bucket_end_min -= 24 * 60
        day = day + timedelta(days=1)
    hour = bucket_end_min // 60
    minute = bucket_end_min % 60
    return datetime(day.year, day.month, day.day, hour, minute, 0)


class LiveStreamingManager:
    def __init__(self) -> None:
        self._enabled = _as_bool(os.getenv('LIVE_STREAMING_ENABLED', 'false'), False)
        self._max_tokens = _as_int(os.getenv('LIVE_STREAMING_MAX_TOKENS', '1500'), 1500, minimum=100, maximum=50000)
        self._mode = _as_int(os.getenv('LIVE_STREAMING_MODE', '2'), 2, minimum=1, maximum=3)
        self._instances: Dict[int, SmartWebSocketV2] = {}
        self._token_sets: Dict[int, set[str]] = {}
        self._agg: Dict[str, Dict[int, _CandleAggregator]] = {}
        self._lock = Lock()

    def enabled(self) -> bool:
        return bool(self._enabled and SmartWebSocketV2 is not None)

    def start(self, user_id: int, angel_api, tokens_by_exchange: Dict[int, list[str]]) -> bool:
        if not self.enabled():
            return False
        if not angel_api or not angel_api.auth_token or not angel_api.feed_token:
            logger.warning('Live streaming start skipped: missing auth/feed tokens.')
            return False

        tokens = []
        for _, vals in (tokens_by_exchange or {}).items():
            tokens.extend([str(v) for v in vals])
        tokens = list(dict.fromkeys(tokens))
        if not tokens:
            return False
        if len(tokens) > self._max_tokens:
            logger.warning('Live streaming skipped: token count %s exceeds limit %s.', len(tokens), self._max_tokens)
            return False

        ws = SmartWebSocketV2(
            auth_token=angel_api.auth_token,
            api_key=angel_api.api_key,
            client_code=angel_api.client_id,
            feed_token=angel_api.feed_token
        )
        correlation_id = f'live_stream_{user_id}_{int(time.time())}'

        def on_open(_wsapp):
            token_list = [
                {"exchangeType": int(exch_type), "tokens": [str(t) for t in token_list]}
                for exch_type, token_list in tokens_by_exchange.items()
            ]
            try:
                ws.subscribe(correlation_id, self._mode, token_list)
            except Exception as exc:
                logger.warning('Streaming subscribe failed: %s', str(exc))

        def on_data(_wsapp, message):
            try:
                token = str(message.get('token'))
                price_raw = message.get('last_traded_price') or message.get('ltp') or message.get('LTP')
                if token is None or price_raw is None:
                    return
                price = _normalize_price(price_raw)
                ts_val = message.get('exchange_timestamp') or message.get('exchangeTimestamp') or message.get('exchange_time')
                ts = _normalize_ts(ts_val) or datetime.now()
                self._update_candle(token, ts, price)
            except Exception:
                return

        def on_error(_wsapp, error):
            logger.warning('Streaming error: %s', str(error))

        def on_close(_wsapp):
            logger.info('Streaming closed for user %s.', user_id)

        ws.on_open = on_open
        ws.on_data = on_data
        ws.on_error = on_error
        ws.on_close = on_close

        with self._lock:
            self._instances[user_id] = ws
            self._token_sets[user_id] = set(tokens)

        Thread(target=ws.connect, daemon=True, name=f'live-stream-{user_id}').start()
        return True

    def stop(self, user_id: int) -> None:
        with self._lock:
            ws = self._instances.pop(user_id, None)
            self._token_sets.pop(user_id, None)
        try:
            if ws:
                ws.close_connection()
        except Exception:
            pass

    def get_pair(self, token: str, timeframe: int) -> Optional[Dict[str, float]]:
        with self._lock:
            per_token = self._agg.get(str(token))
        if not per_token:
            return None
        agg = per_token.get(int(timeframe))
        if not agg:
            return None
        pair = agg.get_pair()
        if not pair:
            return None
        prev_close, curr_close, candle_time = pair
        return {
            'previous_candle_close': prev_close,
            'current_candle_close': curr_close,
            'candle_time': candle_time
        }

    def _update_candle(self, token: str, ts: datetime, price: float) -> None:
        with self._lock:
            per_token = self._agg.setdefault(str(token), {})
            for tf in (1, 3, 5, 15, 30, 60):
                agg = per_token.get(tf)
                if not agg:
                    agg = _CandleAggregator(tf)
                    per_token[tf] = agg
                agg.update(ts, price)


_manager = LiveStreamingManager()


def start_live_stream(user_id: int, angel_api, tokens_by_exchange: Dict[int, list[str]]) -> bool:
    return _manager.start(user_id, angel_api, tokens_by_exchange)


def stop_live_stream(user_id: int) -> None:
    _manager.stop(user_id)


def get_streaming_candle_pair(token: str, timeframe: int) -> Optional[Dict[str, float]]:
    return _manager.get_pair(token, timeframe)


def _normalize_price(value) -> float:
    try:
        num = float(value)
    except Exception:
        return 0.0
    # SmartAPI prices often arrive as paise integers.
    return num / 100.0 if num >= 1000 else num


def _normalize_ts(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        num = int(value)
        # Handle epoch in ms
        if num > 10_000_000_000:
            return datetime.fromtimestamp(num / 1000.0)
        return datetime.fromtimestamp(num)
    except Exception:
        return None
