"""
Scanner Engine Service
"""
from app.services.angel_api import AngelOneAPI
from app.services.signal_detector import SignalDetector
from app.services.data_processor import DataProcessor
from app.models.signal import Signal
from app.models.signal_detail import SignalDetail
from app import db, socketio
from datetime import datetime, date, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock
import os
import logging
import re
from sqlalchemy import inspect as sa_inspect

logger = logging.getLogger(__name__)


class ScannerEngine:
    """Core scanner logic"""
    NIFTY_50_SYMBOLS = [
        'ADANIENT', 'ADANIPORTS', 'APOLLOHOSP', 'ASIANPAINT', 'AXISBANK',
        'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV', 'BEL', 'BHARTIARTL',
        'BPCL', 'BRITANNIA', 'CIPLA', 'COALINDIA', 'DIVISLAB', 'DRREDDY',
        'EICHERMOT', 'GRASIM', 'HCLTECH', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO',
        'HINDALCO', 'HINDUNILVR', 'ICICIBANK', 'ITC', 'INDUSINDBK', 'INFY',
        'JSWSTEEL', 'KOTAKBANK', 'LT', 'M&M', 'MARUTI', 'NESTLEIND', 'NTPC',
        'ONGC', 'POWERGRID', 'RELIANCE', 'SBILIFE', 'SHRIRAMFIN', 'SBIN',
        'SUNPHARMA', 'TCS', 'TATACONSUM', 'TATAMOTORS', 'TATASTEEL', 'TECHM',
        'TITAN', 'TRENT', 'ULTRACEMCO', 'WIPRO'
    ]
    
    def __init__(
        self,
        user_id,
        config,
        angel_api,
        workers=8,
        live_prefilter_enabled=None,
        live_prefilter_top_movers=None,
        live_prefilter_top_volume=None,
        live_prefilter_max_stocks=None
    ):
        """
        Initialize scanner engine
        
        Args:
            user_id: User ID
            config: ScannerConfig instance
            angel_api: AngelOneAPI instance
        """
        self.user_id = user_id
        self.config = config
        self.config_id = config.id if config else None
        self.angel_api = angel_api
        self.is_running = False
        self._last_live_signal_candle = {}
        self.last_cycle_stocks_scanned = 0
        self.last_cycle_stocks_total = 0
        self.last_cycle_strikes_scanned = 0
        self.last_cycle_strikes_total = 0
        self.last_backtest_scripts_scanned = 0
        self.last_backtest_scripts_total = 0
        self.last_backtest_signals = 0
        self._last_processed_live_candle = None
        self._expiry_cache = {}
        self._strike_universe_cache = {}
        self._stock_selection_cache = None
        self._stock_selection_cache_day = None
        self._has_signal_details_table = None
        env_prefilter_enabled = str(os.getenv('LIVE_PREFILTER_ENABLED', 'true')).strip().lower() in ('1', 'true', 'yes', 'on')
        self._prefilter_enabled = env_prefilter_enabled if live_prefilter_enabled is None else bool(live_prefilter_enabled)
        self._prefilter_top_movers = self._safe_int(
            live_prefilter_top_movers if live_prefilter_top_movers is not None else os.getenv('LIVE_PREFILTER_TOP_MOVERS', '30'),
            30
        )
        self._prefilter_top_volume = self._safe_int(
            live_prefilter_top_volume if live_prefilter_top_volume is not None else os.getenv('LIVE_PREFILTER_TOP_VOLUME', '30'),
            30
        )
        self._prefilter_max_stocks = self._safe_int(
            live_prefilter_max_stocks if live_prefilter_max_stocks is not None else os.getenv('LIVE_PREFILTER_MAX_STOCKS', '50'),
            50
        )
        self._candle_close_delay_seconds = self._safe_int(os.getenv('LIVE_CANDLE_CLOSE_DELAY_SECONDS', '8'), 8)
        self.max_workers = max(1, min(16, self._safe_int(workers, 8)))
        self._stop_event = Event()
        self._live_signal_lock = Lock()
    
    def start_live_scan(self):
        """Start live scanning"""
        self.is_running = True
        self._stop_event.clear()
        logger.info(f'Starting live scan for user {self.user_id} with config {self.config_id}')
        
        self.run_live_loop()
    
    def stop_live_scan(self):
        """Stop live scanning"""
        self.is_running = False
        self._stop_event.set()
        logger.info(f'Stopped live scan for user {self.user_id}')

    def stop_backtest(self):
        """Stop running backtest gracefully."""
        self.is_running = False
        self._stop_event.set()
        logger.info(f'Stopped backtest for user {self.user_id}')

    def run_live_loop(self, status_callback=None):
        """Background live scan loop"""
        refresh_interval = int(self.config.refresh_interval or 300)
        timeframe = self._normalize_timeframe()
        self._stop_event.clear()
        while self.is_running and not self._stop_event.is_set():
            is_new_candle, candle_slot = self._claim_live_candle_slot(timeframe)
            if not is_new_candle:
                if self._wait_with_abort(min(refresh_interval, 10)):
                    break
                continue
            try:
                signals = self.scan_once(progress_callback=status_callback, reference_time=candle_slot)
                if status_callback:
                    status_callback(
                        self.user_id,
                        last_run_at=datetime.utcnow().isoformat() + 'Z',
                        signals=len(signals),
                        stocks_scanned=self.last_cycle_stocks_scanned,
                        stocks_total=self.last_cycle_stocks_total,
                        strikes_scanned=self.last_cycle_strikes_scanned,
                        strikes_total=self.last_cycle_strikes_total
                    )
                if signals:
                    for signal in signals:
                        payload = self._serialize_realtime_signal(signal)
                        socketio.emit(
                            'new_signal',
                            payload,
                            to=f'user_{self.user_id}'
                        )
            except Exception as e:
                logger.error(f'Live scan error: {str(e)}')
                if status_callback:
                    status_callback(self.user_id, error=str(e))
            if self._wait_with_abort(refresh_interval):
                break

    def scan_once(self, progress_callback=None, reference_time=None):
        """Run a single live scan cycle and persist detected signals."""
        cycle_time = reference_time or datetime.now()
        self._refresh_daily_caches(cycle_time)
        stocks_all = self._resolve_stock_selection()
        quotes = self._get_live_quotes(stocks_all)
        stocks = self._prefilter_live_stocks(stocks_all, quotes)
        self.last_cycle_stocks_total = len(stocks)
        self.last_cycle_stocks_scanned = 0
        self.last_cycle_strikes_total = 0
        self.last_cycle_strikes_scanned = 0
        if progress_callback:
            progress_callback(
                self.user_id,
                stocks_scanned=0,
                stocks_total=self.last_cycle_stocks_total,
                strikes_scanned=0,
                strikes_total=0
            )
        timeframe = self._normalize_timeframe()
        symbol_jobs = []
        for symbol in stocks:
            expiry = self._resolve_expiry(symbol, reference_date=cycle_time)
            if not expiry:
                symbol_jobs.append({'symbol': symbol, 'expiry': None, 'strikes_by_type': {'CE': [], 'PE': []}})
                continue

            spot = quotes.get(symbol)
            if not spot:
                spot = self.angel_api.get_live_quote(symbol, exchange='NSE')
            if not spot or spot.get('ltp') is None:
                symbol_jobs.append({'symbol': symbol, 'expiry': expiry, 'strikes_by_type': {'CE': [], 'PE': []}})
                continue

            strikes_by_type = self._resolve_strikes_by_type(symbol, expiry, float(spot['ltp']))
            symbol_jobs.append({'symbol': symbol, 'expiry': expiry, 'strikes_by_type': strikes_by_type})

        self.last_cycle_strikes_total = sum(
            len(job['strikes_by_type'].get('CE', [])) + len(job['strikes_by_type'].get('PE', []))
            for job in symbol_jobs
        )
        if progress_callback:
            progress_callback(
                self.user_id,
                stocks_scanned=self.last_cycle_stocks_scanned,
                stocks_total=self.last_cycle_stocks_total,
                strikes_scanned=self.last_cycle_strikes_scanned,
                strikes_total=self.last_cycle_strikes_total
            )

        detected = []
        if not symbol_jobs:
            return detected

        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        aborted = False
        try:
            future_map = {
                executor.submit(
                    self._scan_live_symbol,
                    job['symbol'],
                    job['expiry'],
                    job['strikes_by_type'],
                    timeframe
                ): job['symbol']
                for job in symbol_jobs
            }
            for future in as_completed(future_map):
                self.last_cycle_stocks_scanned += 1
                symbol_detected = []
                strikes_scanned = 0
                try:
                    symbol_detected, strikes_scanned = future.result()
                except Exception as e:
                    logger.error('Live scan worker failed for %s: %s', future_map.get(future), str(e))
                self.last_cycle_strikes_scanned += self._safe_int(strikes_scanned, 0)
                if symbol_detected:
                    detected.extend(symbol_detected)
                if progress_callback:
                    progress_callback(
                        self.user_id,
                        stocks_scanned=self.last_cycle_stocks_scanned,
                        stocks_total=self.last_cycle_stocks_total,
                        strikes_scanned=self.last_cycle_strikes_scanned,
                        strikes_total=self.last_cycle_strikes_total
                    )
                if not self.is_running or self._stop_event.is_set():
                    aborted = True
                    break
        finally:
            executor.shutdown(wait=not aborted, cancel_futures=aborted)

        if detected:
            self._persist_signals(detected, mode='live')

        return detected

    def run_backtest(self, from_date, to_date, progress_callback=None):
        """
        Run backtest on historical data
        
        Args:
            from_date: Start date
            to_date: End date
        
        Returns:
            list: Detected signals
        """
        logger.info(f'Running backtest from {from_date} to {to_date}')
        self.is_running = True
        self._stop_event.clear()

        detected = []
        stocks = self._resolve_stock_selection()
        self.last_backtest_scripts_scanned = 0
        self.last_backtest_scripts_total = len(stocks)
        self.last_backtest_signals = 0
        timeframe = self._normalize_timeframe()
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        aborted = False
        try:
            future_map = {
                executor.submit(self._run_backtest_symbol, symbol, from_date, to_date, timeframe): symbol
                for symbol in stocks
            }
            for future in as_completed(future_map):
                self.last_backtest_scripts_scanned += 1
                socketio.emit(
                    'backtest_progress',
                    {
                        'mode': 'backtest',
                        'scripts_scanned': int(self.last_backtest_scripts_scanned),
                        'scripts_total': int(self.last_backtest_scripts_total),
                        'signals': int(self.last_backtest_signals),
                        'running': bool(self.is_running),
                        'updated_at': datetime.utcnow().isoformat() + 'Z'
                    },
                    to=f'user_{self.user_id}'
                )

                symbol_detected = []
                try:
                    symbol_detected = future.result() or []
                except Exception as e:
                    logger.error('Backtest worker failed for %s: %s', future_map.get(future), str(e))

                if symbol_detected:
                    self._persist_signals(symbol_detected, mode='backtest')
                    self.last_backtest_signals += len(symbol_detected)
                    detected.extend(symbol_detected)
                    for signal in symbol_detected:
                        socketio.emit(
                            'new_signal',
                            self._serialize_realtime_signal(signal),
                            to=f'user_{self.user_id}'
                        )

                if progress_callback:
                    progress_callback(
                        self.user_id,
                        scripts_scanned=self.last_backtest_scripts_scanned,
                        signals=self.last_backtest_signals
                    )

                if not self.is_running or self._stop_event.is_set():
                    aborted = True
                    break
        finally:
            executor.shutdown(wait=not aborted, cancel_futures=aborted)

        # Keep explicit final counters available in status.
        self.last_backtest_signals = int(self.last_backtest_signals or 0)
        self.is_running = False

        return detected

    def _scan_live_symbol(self, symbol, expiry, strikes_by_type, timeframe):
        if not self.is_running or self._stop_event.is_set() or not expiry:
            return [], 0

        strikes_processed = 0
        symbol_detected = []
        for option_type in ('CE', 'PE'):
            for strike in strikes_by_type.get(option_type, []):
                if not self.is_running or self._stop_event.is_set():
                    return symbol_detected, strikes_processed
                strikes_processed += 1
                option_snapshot = self.angel_api.get_recent_option_candle_pair(
                    underlying=symbol,
                    expiry=expiry,
                    strike=strike,
                    option_type=option_type,
                    timeframe=timeframe,
                    exchange='NFO'
                )
                if not option_snapshot:
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
                signal = SignalDetector.detect_signal(signal_payload, self.config)
                if not signal:
                    continue
                signal['strike_price'] = self._normalize_signal_strike(
                    signal.get('strike_price'),
                    signal.get('symbol')
                )
                candle_time = option_snapshot.get('candle_time')
                if not self._is_new_live_candle_signal(signal['symbol'], candle_time):
                    continue
                signal['expiry_date'] = expiry
                signal['detected_at'] = candle_time or datetime.utcnow()
                signal['displayed_at'] = datetime.utcnow()
                symbol_detected.append(signal)
        return symbol_detected, strikes_processed

    def _run_backtest_symbol(self, symbol, from_date, to_date, timeframe):
        if not self.is_running or self._stop_event.is_set():
            return []

        symbol_detected = []
        expiry = self._resolve_expiry(symbol, reference_date=from_date)
        if not expiry:
            return symbol_detected

        spot = self.angel_api.get_live_quote(symbol, exchange='NSE')
        if not spot or spot.get('ltp') is None:
            return symbol_detected

        strikes_by_type = self._resolve_strikes_by_type(symbol, expiry, float(spot['ltp']))
        for option_type in ('CE', 'PE'):
            for strike in strikes_by_type.get(option_type, []):
                if not self.is_running or self._stop_event.is_set():
                    return symbol_detected

                instrument = self.angel_api.lookup_option_instrument(
                    underlying=symbol,
                    expiry=expiry,
                    strike=strike,
                    option_type=option_type,
                    exchange='NFO'
                )
                if not instrument:
                    continue

                candles = self.angel_api.get_historical_data(
                    symbol=instrument['symbol'],
                    timeframe=timeframe,
                    from_date=from_date,
                    to_date=to_date,
                    exchange='NFO',
                    symbol_token=instrument['token']
                )
                if not candles or len(candles) < 2:
                    continue

                for i in range(1, len(candles)):
                    if not self.is_running or self._stop_event.is_set():
                        return symbol_detected
                    prev_time = self._parse_candle_time(candles[i - 1][0])
                    current_time = self._parse_candle_time(candles[i][0])
                    if not prev_time or not current_time:
                        continue
                    previous_close = float(candles[i - 1][4])
                    current_close = float(candles[i][4])
                    rsi = self._calculate_rsi(candles[:i + 1], period=14)
                    option_data = {
                        'symbol': instrument['symbol'],
                        'option_type': option_type,
                        'strike_price': strike,
                        'ltp': current_close,
                        'previous_candle_close': previous_close,
                        'candle_open': float(candles[i][1]) if len(candles[i]) > 1 else None,
                        'candle_high': float(candles[i][2]) if len(candles[i]) > 2 else None,
                        'candle_low': float(candles[i][3]) if len(candles[i]) > 3 else None,
                        'candle_close': current_close,
                        'volume': int(float(candles[i][5])) if len(candles[i]) > 5 else 0,
                        'open_interest': 0,
                        'rsi': rsi
                    }
                    signal = SignalDetector.detect_signal(option_data, self.config)
                    if not signal:
                        continue
                    signal['strike_price'] = self._normalize_signal_strike(
                        signal.get('strike_price'),
                        signal.get('symbol')
                    )
                    signal['expiry_date'] = expiry
                    signal['detected_at'] = current_time
                    signal['displayed_at'] = datetime.utcnow()
                    signal['mode'] = 'backtest'
                    symbol_detected.append(signal)

        return symbol_detected

    def get_selected_expiries(self, reference_date=None):
        """Return selected expiry per symbol for current config."""
        selected = {}
        stocks = self._resolve_stock_selection()
        ref = reference_date or datetime.now()
        for symbol in stocks:
            expiry = self._resolve_expiry(symbol, reference_date=ref)
            selected[symbol] = expiry.isoformat() if expiry else None
        return selected

    def _resolve_expiry(self, symbol, reference_date=None):
        """
        Resolve expiry from scrip master first, then fallback to static calendar rules.
        """
        ref = reference_date or datetime.now()
        ref_date = ref.date() if isinstance(ref, datetime) else ref
        cache_key = (str(symbol or '').upper(), ref_date.isoformat() if ref_date else '')
        if cache_key in self._expiry_cache:
            return self._expiry_cache.get(cache_key)

        try:
            expiry = self.angel_api.get_nearest_option_expiry(
                underlying=symbol,
                reference_date=ref,
                exchange='NFO'
            )
            if expiry:
                self._expiry_cache[cache_key] = expiry
                return expiry
        except Exception as e:
            logger.warning(f'Failed to resolve expiry from scrip master for {symbol}: {str(e)}')

        fallback = DataProcessor.get_next_expiry(
            symbol,
            DataProcessor.get_expiry_type(symbol),
            reference_date=ref
        )
        if isinstance(fallback, datetime):
            fallback = fallback.date()
        self._expiry_cache[cache_key] = fallback
        return fallback

    def _persist_signals(self, signals, mode):
        signal_records = []
        for signal in signals:
            detected_at = signal.get('detected_at')
            if isinstance(detected_at, datetime) and detected_at.tzinfo is not None:
                detected_at = detected_at.astimezone(timezone.utc).replace(tzinfo=None)
            record = Signal(
                user_id=self.user_id,
                scanner_config_id=self.config_id,
                mode=mode,
                symbol=signal['symbol'],
                option_type=signal['option_type'],
                strike_price=self._normalize_signal_strike(signal.get('strike_price'), signal.get('symbol')),
                expiry_date=signal['expiry_date'],
                entry_price=signal['entry_price'],
                current_price=signal['current_price'],
                price_change_percent=signal['price_change_percent'],
                volume=signal.get('volume'),
                open_interest=signal.get('open_interest'),
                rsi=signal.get('rsi'),
                timeframe=signal.get('timeframe'),
                detected_at=detected_at
            )
            db.session.add(record)
            signal_records.append((record, signal))

        try:
            if signal_records and self._signal_details_table_exists():
                db.session.flush()
                for record, signal in signal_records:
                    displayed_at = signal.get('displayed_at')
                    if isinstance(displayed_at, datetime) and displayed_at.tzinfo is not None:
                        displayed_at = displayed_at.astimezone(timezone.utc).replace(tzinfo=None)
                    db.session.add(
                        SignalDetail(
                            signal_id=record.id,
                            candle_open=signal.get('candle_open'),
                            candle_high=signal.get('candle_high'),
                            candle_low=signal.get('candle_low'),
                            candle_close=signal.get('candle_close'),
                            displayed_at=displayed_at
                        )
                    )
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.error(f'Failed to persist signals: {str(e)}')

    def _serialize_realtime_signal(self, signal):
        """Convert signal values to JSON-safe payload for websocket emit."""
        payload = {}
        for key, value in (signal or {}).items():
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    payload[key] = value.isoformat() + 'Z'
                else:
                    payload[key] = value.isoformat()
            elif isinstance(value, date):
                payload[key] = value.isoformat()
            else:
                payload[key] = value
        return payload

    def _resolve_stock_selection(self):
        today = datetime.now().date()
        if self._stock_selection_cache is not None and self._stock_selection_cache_day == today:
            return list(self._stock_selection_cache)

        selection = self.config.get_stock_selection()
        if not selection:
            resolved = ['NIFTY']
            self._stock_selection_cache = list(resolved)
            self._stock_selection_cache_day = today
            return resolved
        if isinstance(selection, str):
            key = selection.lower()
            dynamic_optstk = self.angel_api.get_available_option_underlyings(
                exchange='NFO',
                instrument_type='OPTSTK'
            ) or []
            available = set(dynamic_optstk)
            mapping = {
                # "All" => full NFO OPTSTK stock-option universe from scrip master.
                'all': dynamic_optstk or [s for s in self.NIFTY_50_SYMBOLS if (not available or s in available)],
                # "NIFTY 50" => index options
                'nifty50': ['NIFTY'],
                'nifty': ['NIFTY'],
                'nifty index': ['NIFTY'],
                'nifty 50': ['NIFTY'],
                'banknifty': ['BANKNIFTY'],
                'bank nifty': ['BANKNIFTY'],
                'banknifty index': ['BANKNIFTY'],
                'bank nifty index': ['BANKNIFTY'],
                'nifty bank': ['BANKNIFTY']
            }
            resolved = mapping.get(key)
            if resolved:
                self._stock_selection_cache = list(resolved)
                self._stock_selection_cache_day = today
                return resolved
            resolved = [selection.upper()]
            self._stock_selection_cache = list(resolved)
            self._stock_selection_cache_day = today
            return resolved
        if isinstance(selection, list):
            resolved = [s.upper() for s in selection]
            self._stock_selection_cache = list(resolved)
            self._stock_selection_cache_day = today
            return resolved
        resolved = ['NIFTY']
        self._stock_selection_cache = list(resolved)
        self._stock_selection_cache_day = today
        return resolved

    def _normalize_timeframe(self):
        if isinstance(self.config.timeframe, str):
            if self.config.timeframe.endswith('min'):
                return int(self.config.timeframe.replace('min', ''))
            return self.config.timeframe
        return int(self.config.timeframe or 5)

    def _strike_step(self, symbol):
        symbol = symbol.upper()
        if symbol == 'BANKNIFTY':
            return 100
        if symbol == 'FINNIFTY':
            return 50
        if symbol == 'MIDCPNIFTY':
            return 25
        return 50

    def _parse_candle_time(self, value):
        if isinstance(value, datetime):
            return value
        try:
            text = str(value).strip()
            # SmartAPI candle timestamps are usually ISO8601 with timezone.
            text = text.replace('Z', '+00:00')
            return datetime.fromisoformat(text)
        except Exception:
            try:
                return datetime.strptime(str(value), '%Y-%m-%d %H:%M')
            except Exception:
                return None

    def _is_new_live_candle_signal(self, symbol, candle_time):
        if not candle_time:
            return True
        key = symbol
        stamp = candle_time.isoformat()
        with self._live_signal_lock:
            last = self._last_live_signal_candle.get(key)
            if last == stamp:
                return False
            self._last_live_signal_candle[key] = stamp
            return True

    def _calculate_rsi(self, candles, period=14):
        """
        Calculate RSI from a candle list where close is index 4.
        Returns None when there is insufficient or invalid data.
        """
        try:
            if not candles or len(candles) <= period:
                return None
            closes = [float(c[4]) for c in candles if len(c) > 4]
            if len(closes) <= period:
                return None
            recent = closes[-(period + 1):]
            gains = 0.0
            losses = 0.0
            for i in range(1, len(recent)):
                change = recent[i] - recent[i - 1]
                if change > 0:
                    gains += change
                elif change < 0:
                    losses += abs(change)
            avg_gain = gains / period
            avg_loss = losses / period
            if avg_loss == 0:
                return 100.0
            rs = avg_gain / avg_loss
            return 100.0 - (100.0 / (1.0 + rs))
        except Exception:
            return None

    def _resolve_strikes(self, symbol, expiry, spot_price):
        """
        Resolve strike list from scrip master first (most accurate), fallback to step-based.
        """
        strike_range = self._effective_strike_range()
        expiry_date = expiry if not isinstance(expiry, datetime) else expiry.date()
        cache_key = (str(symbol or '').upper(), expiry_date.isoformat() if expiry_date else '')
        try:
            available = self._strike_universe_cache.get(cache_key)
            if available is None:
                available = self.angel_api.get_option_strikes_for_expiry(
                    underlying=symbol,
                    expiry=expiry_date,
                    exchange='NFO'
                )
                self._strike_universe_cache[cache_key] = list(available or [])
            if available:
                return self._pick_strikes_around_spot(
                    spot_price=spot_price,
                    strikes=available,
                    strike_range=strike_range
                )
        except Exception as e:
            logger.warning(f'Failed strike discovery for {symbol}: {str(e)}')

        strike_step = self._strike_step(symbol)
        return SignalDetector.calculate_strike_range(
            spot_price=spot_price,
            strike_range=strike_range,
            strike_step=strike_step
        )

    def _resolve_strikes_by_type(self, symbol, expiry, spot_price):
        strike_count = self._effective_strike_range()
        expiry_date = expiry if not isinstance(expiry, datetime) else expiry.date()
        cache_key = (str(symbol or '').upper(), expiry_date.isoformat() if expiry_date else '')

        candidates = []
        try:
            available = self._strike_universe_cache.get(cache_key)
            if available is None:
                available = self.angel_api.get_option_strikes_for_expiry(
                    underlying=symbol,
                    expiry=expiry_date,
                    exchange='NFO'
                )
                self._strike_universe_cache[cache_key] = list(available or [])
            candidates = [float(s) for s in (available or [])]
        except Exception:
            candidates = []

        if not candidates:
            strike_step = self._strike_step(symbol)
            # Generate enough fallback strikes to support both CE and PE sides.
            fallback_span = max(2, strike_count * 2)
            atm_strike = round(float(spot_price) / float(strike_step)) * float(strike_step)
            candidates = [
                atm_strike + (idx * float(strike_step))
                for idx in range(-fallback_span, fallback_span + 1)
            ]
        normalized_candidates = self._normalize_strike_candidates_for_spot(spot_price, candidates)
        return self._split_strikes_for_option_types(spot_price, normalized_candidates, strike_count)

    def _safe_int(self, value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    def _wait_with_abort(self, seconds):
        timeout = max(0, self._safe_int(seconds, 0))
        if timeout <= 0:
            return self._stop_event.is_set() or (not self.is_running)
        return self._stop_event.wait(timeout=timeout)

    def _effective_strike_range(self):
        configured = self._safe_int(getattr(self.config, 'strike_range', 0), 0)
        if configured > 0:
            return configured
        # Live mode defaults to tighter strike range for faster cycles.
        config_name = str(getattr(self.config, 'config_name', '') or '').lower()
        return 2 if config_name.startswith('live ') else 5

    def _refresh_daily_caches(self, reference_time=None):
        ref = reference_time or datetime.now()
        today = ref.date() if isinstance(ref, datetime) else ref
        if self._stock_selection_cache_day == today:
            return
        self._stock_selection_cache_day = today
        self._stock_selection_cache = None
        self._expiry_cache = {}
        self._strike_universe_cache = {}

    def _get_live_quotes(self, symbols):
        quotes = {}
        for symbol in symbols:
            quote = self.angel_api.get_live_quote(symbol, exchange='NSE')
            if quote and quote.get('ltp') is not None:
                quotes[symbol] = quote
        return quotes

    def _prefilter_live_stocks(self, stocks, quotes):
        if not stocks:
            return []
        if not self._prefilter_enabled:
            return list(stocks)

        ranked = []
        for symbol in stocks:
            quote = quotes.get(symbol)
            if not quote:
                continue
            ltp = float(quote.get('ltp') or 0)
            close = float(quote.get('close') or 0)
            volume = self._safe_int(quote.get('volume', 0), 0)
            move_pct = abs(((ltp - close) / close) * 100.0) if close else 0.0
            ranked.append({
                'symbol': symbol,
                'move_pct': move_pct,
                'volume': volume
            })

        if not ranked:
            return list(stocks)

        max_stocks = self._safe_int(self._prefilter_max_stocks, 50)
        top_movers = self._safe_int(self._prefilter_top_movers, 30)
        top_volume = self._safe_int(self._prefilter_top_volume, 30)

        movers = sorted(
            ranked,
            key=lambda item: (item['move_pct'], item['volume']),
            reverse=True
        )
        volumes = sorted(
            ranked,
            key=lambda item: (item['volume'], item['move_pct']),
            reverse=True
        )

        selected = []
        seen = set()
        for item in movers[:max(top_movers, 0)]:
            symbol = item['symbol']
            if symbol not in seen:
                selected.append(symbol)
                seen.add(symbol)
        for item in volumes[:max(top_volume, 0)]:
            symbol = item['symbol']
            if symbol not in seen:
                selected.append(symbol)
                seen.add(symbol)

        if not selected:
            selected = [item['symbol'] for item in movers]

        if max_stocks > 0 and len(selected) > max_stocks:
            selected = selected[:max_stocks]

        logger.debug(
            'Live prefilter selected %s stocks out of %s candidates.',
            len(selected),
            len(stocks)
        )
        return selected

    def _claim_live_candle_slot(self, timeframe_minutes):
        now = datetime.now()
        interval = max(1, self._safe_int(timeframe_minutes, 5))
        bucket_minute = (now.minute // interval) * interval
        current_bucket = now.replace(minute=bucket_minute, second=0, microsecond=0)
        close_ready_at = current_bucket + timedelta(seconds=max(0, self._candle_close_delay_seconds))
        if now < close_ready_at:
            return False, None

        latest_closed_candle = current_bucket - timedelta(minutes=interval)
        if self._last_processed_live_candle and latest_closed_candle <= self._last_processed_live_candle:
            return False, latest_closed_candle

        self._last_processed_live_candle = latest_closed_candle
        return True, latest_closed_candle

    def _pick_strikes_around_spot(self, spot_price, strikes, strike_range):
        if not strikes:
            return []
        strike_count = self._safe_int(strike_range, 0)
        if strike_count <= 0:
            return []
        sorted_strikes = sorted(float(s) for s in strikes)
        try:
            spot_val = float(spot_price or 0)
        except Exception:
            spot_val = 0.0
        if spot_val > 0 and sorted_strikes and min(sorted_strikes) > (spot_val * 10):
            sorted_strikes = [round(strike / 100.0, 2) for strike in sorted_strikes]
        ordered = sorted(
            sorted_strikes,
            key=lambda strike: (abs(strike - float(spot_price)), strike)
        )
        return ordered[:strike_count]

    def _split_strikes_for_option_types(self, spot_price, strikes, strike_count):
        normalized = sorted(set(float(s) for s in (strikes or [])))
        count = max(1, self._safe_int(strike_count, 0))
        if not normalized:
            return {'CE': [], 'PE': []}

        spot = float(spot_price or 0)
        pe_pool = [s for s in normalized if s <= spot]
        ce_pool = [s for s in normalized if s >= spot]

        # PE should move downward from CMP-nearest, CE should move upward.
        pe_ordered = sorted(pe_pool, reverse=True)
        ce_ordered = sorted(ce_pool)
        if not pe_ordered:
            pe_ordered = sorted(normalized, key=lambda s: (abs(s - spot), -s))
        if not ce_ordered:
            ce_ordered = sorted(normalized, key=lambda s: (abs(s - spot), s))
        return {
            'PE': pe_ordered[:count],
            'CE': ce_ordered[:count]
        }

    def _normalize_strike_candidates_for_spot(self, spot_price, strikes):
        """
        Normalize strike scales against spot.
        Handles master files that store strikes as 100x values (e.g. 10600 vs 106).
        """
        if not strikes:
            return []
        try:
            spot = float(spot_price or 0)
        except Exception:
            spot = 0.0
        raw = [float(s) for s in strikes if s is not None]
        if not raw:
            return []
        if spot <= 0:
            return sorted(set(raw))

        best_scaled = raw
        best_score = float('inf')
        for factor in (1.0, 0.01, 0.1, 10.0, 100.0):
            scaled = [round(s * factor, 4) for s in raw]
            if not scaled:
                continue
            nearest = min(abs(s - spot) for s in scaled)
            median = sorted(scaled)[len(scaled) // 2]
            score = nearest + (abs(median - spot) * 0.02)
            if score < best_score:
                best_score = score
                best_scaled = scaled

        return sorted(set(round(s, 2) for s in best_scaled))

    def _signal_details_table_exists(self):
        if self._has_signal_details_table is not None:
            return self._has_signal_details_table
        try:
            inspector = sa_inspect(db.engine)
            exists = inspector.has_table('signal_details')
            if not exists:
                # Auto-create lightweight extension table for OHLC/display timestamps.
                SignalDetail.__table__.create(bind=db.engine, checkfirst=True)
                exists = sa_inspect(db.engine).has_table('signal_details')
            self._has_signal_details_table = bool(exists)
        except Exception:
            self._has_signal_details_table = False
        return self._has_signal_details_table

    def _extract_strike_from_contract_symbol(self, symbol):
        if not symbol:
            return None
        text = str(symbol).strip().upper()
        match = re.match(r'^(.*?)(\d{2})([A-Z]{3})(\d{2})(\d+(?:\.\d+)?)(CE|PE)$', text)
        if match:
            try:
                return float(match.group(5))
            except Exception:
                return None
        tail = re.search(r'(\d+(?:\.\d+)?)(CE|PE)$', text)
        if not tail:
            return None
        try:
            return float(tail.group(1))
        except Exception:
            return None

    def _normalize_signal_strike(self, strike_price, symbol):
        try:
            strike = float(strike_price)
        except Exception:
            return strike_price

        symbol_strike = self._extract_strike_from_contract_symbol(symbol)
        if symbol_strike is not None:
            candidates = [strike, strike / 100.0, strike / 10.0, strike * 10.0, strike * 100.0]
            strike = min(candidates, key=lambda val: abs(val - symbol_strike))
        elif strike >= 10000 and abs(strike % 100) < 0.001:
            strike = strike / 100.0

        return round(strike, 2)
