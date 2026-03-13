"""
Scanner Engine Service
"""
from app.services.angel_api import AngelOneAPI
from app.services.signal_detector import SignalDetector
from app.services.data_processor import DataProcessor
from app.services.telegram_notifier import send_signal_notification
from app.services.live_scan_jobs import scan_live_symbol_job
from app.models.signal import Signal
from app.models.signal_detail import SignalDetail
from app import db, socketio
from datetime import datetime, date, timezone, timedelta, time as dt_time
from collections import OrderedDict
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock
import os
import csv
import json
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
        live_prefilter_max_stocks=None,
        live_use_queue=None,
        live_fast_mode_enabled=None,
        live_fast_max_stocks=None,
        live_fast_max_strikes_per_stock=None,
        backtest_rebalance_frequency='weekly',
        backtest_strict_first_candle=True,
        backtest_liquidity_filter_enabled=True,
        backtest_min_candles_per_strike=5,
        backtest_min_avg_volume=1,
        backtest_scan_log_enabled=False,
        api_error_log_enabled=False
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
        self.last_cycle_skipped_candles = 0
        self.last_backtest_scripts_scanned = 0
        self.last_backtest_scripts_total = 0
        self.last_backtest_strikes_scanned = 0
        self.last_backtest_strikes_total = 0
        self.last_backtest_instruments_missing = 0
        self.last_backtest_candles_missing = 0
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
        self._live_use_queue = live_use_queue
        env_fast_mode = str(os.getenv('LIVE_FAST_MODE_ENABLED', 'true')).strip().lower() in ('1', 'true', 'yes', 'on')
        self._live_fast_enabled = env_fast_mode if live_fast_mode_enabled is None else bool(live_fast_mode_enabled)
        self._live_fast_max_stocks = self._safe_int(
            live_fast_max_stocks if live_fast_max_stocks is not None else os.getenv('LIVE_FAST_MAX_STOCKS', '60'),
            60
        )
        self._live_fast_max_strikes_per_stock = self._safe_int(
            live_fast_max_strikes_per_stock if live_fast_max_strikes_per_stock is not None else os.getenv('LIVE_FAST_MAX_STRIKES_PER_STOCK', '24'),
            24
        )
        self._live_fast_cursor = 0
        self._candle_close_delay_seconds = self._safe_int(os.getenv('LIVE_CANDLE_CLOSE_DELAY_SECONDS', '8'), 8)
        self.max_workers = max(1, min(16, self._safe_int(workers, 8)))
        self._stop_event = Event()
        self._live_signal_lock = Lock()
        freq_token = str(backtest_rebalance_frequency or 'weekly').strip().lower()
        if freq_token not in ('daily', 'weekly', 'monthly'):
            freq_token = 'weekly'
        self._backtest_rebalance_frequency = freq_token
        self._backtest_strict_first_candle = bool(backtest_strict_first_candle)
        self._backtest_liquidity_filter_enabled = bool(backtest_liquidity_filter_enabled)
        self._backtest_min_candles_per_strike = max(2, self._safe_int(backtest_min_candles_per_strike, 5))
        self._backtest_min_avg_volume = max(0, self._safe_int(backtest_min_avg_volume, 1))
        self._backtest_scan_log_enabled = bool(backtest_scan_log_enabled)
        self._backtest_log_lock = Lock()
        self._backtest_log_handle = None
        self._backtest_log_path = None
        self._backtest_csv_handle = None
        self._backtest_csv_writer = None
        self._backtest_csv_path = None
        self._api_error_log_enabled = bool(api_error_log_enabled)
        self._api_error_log_lock = Lock()
        self._cmp_min = float(os.getenv('LIVE_CMP_MIN', '200') or 200)
        self._cmp_max = float(os.getenv('LIVE_CMP_MAX', '20000') or 20000)
        self._historical_cache = OrderedDict()
        self._historical_cache_max = self._safe_int(os.getenv('HISTORICAL_CACHE_MAX', '2000'), 2000)
        self._last_api_stats_log_at = 0.0
        self._queue_missing_since = None
        self._queue_force_disabled = False
        self.last_cycle_skip_reason = None
        self.last_cycle_prefilter_skipped = 0
        self.last_cycle_missing_quotes = 0
        self.last_cycle_cmp_skipped = 0
        self.last_cycle_missing_expiry = 0
        self.last_cycle_missing_spot = 0
        self.last_cycle_fast_stock_skipped = 0
        self.last_cycle_fast_strike_skipped = 0
        self.last_cycle_strikes_planned = 0
    
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
                signals = self.scan_once(
                    progress_callback=status_callback,
                    reference_time=candle_slot,
                    emit_signals=True
                )
                if status_callback:
                    status_callback(
                        self.user_id,
                        last_run_at=datetime.utcnow().isoformat() + 'Z',
                        signals=len(signals),
                        stocks_scanned=self.last_cycle_stocks_scanned,
                        stocks_total=self.last_cycle_stocks_total,
                        strikes_scanned=self.last_cycle_strikes_scanned,
                        strikes_total=self.last_cycle_strikes_total,
                        skipped_candles=self.last_cycle_skipped_candles,
                        skip_reason=self.last_cycle_skip_reason,
                        prefilter_skipped=self.last_cycle_prefilter_skipped,
                        missing_quotes=self.last_cycle_missing_quotes,
                        cmp_skipped=self.last_cycle_cmp_skipped,
                        missing_expiry=self.last_cycle_missing_expiry,
                        missing_spot=self.last_cycle_missing_spot,
                        fast_stock_skipped=self.last_cycle_fast_stock_skipped,
                        fast_strike_skipped=self.last_cycle_fast_strike_skipped,
                        strikes_planned=self.last_cycle_strikes_planned
                    )
                self._log_api_activity('live')
            except Exception as e:
                logger.error(f'Live scan error: {str(e)}')
                self._write_api_error_log(
                    mode='live',
                    event='live_loop_exception',
                    details={'error': str(e)}
                )
                if status_callback:
                    status_callback(self.user_id, error=str(e))
            if self._wait_with_abort(refresh_interval):
                break

    def scan_once(self, progress_callback=None, reference_time=None, emit_signals=False):
        """Run a single live scan cycle and persist detected signals."""
        cycle_time = reference_time or self._market_now()
        self._refresh_daily_caches(cycle_time)
        stocks_all = self._resolve_stock_selection()
        quotes = self._get_live_quotes(stocks_all)
        stocks, prefilter_stats = self._prefilter_live_stocks(stocks_all, quotes)
        self.last_cycle_stocks_total = len(stocks)
        self.last_cycle_stocks_scanned = 0
        self.last_cycle_strikes_total = 0
        self.last_cycle_strikes_planned = 0
        self.last_cycle_strikes_scanned = 0
        self.last_cycle_skipped_candles = 0
        self.last_cycle_skip_reason = None
        self.last_cycle_prefilter_skipped = self._safe_int(prefilter_stats.get('prefilter_skipped', 0), 0)
        self.last_cycle_missing_quotes = self._safe_int(prefilter_stats.get('missing_quotes', 0), 0)
        self.last_cycle_cmp_skipped = self._safe_int(prefilter_stats.get('cmp_skipped', 0), 0)
        self.last_cycle_missing_expiry = 0
        self.last_cycle_missing_spot = 0
        self.last_cycle_fast_stock_skipped = 0
        self.last_cycle_fast_strike_skipped = 0
        cmp_skipped = 0
        if progress_callback:
            progress_callback(
                self.user_id,
                stocks_scanned=0,
                stocks_total=self.last_cycle_stocks_total,
                strikes_scanned=0,
                strikes_total=0,
                skipped_candles=0,
                prefilter_skipped=self.last_cycle_prefilter_skipped,
                missing_quotes=self.last_cycle_missing_quotes,
                cmp_skipped=self.last_cycle_cmp_skipped,
                missing_expiry=self.last_cycle_missing_expiry,
                missing_spot=self.last_cycle_missing_spot,
                fast_stock_skipped=self.last_cycle_fast_stock_skipped,
                fast_strike_skipped=self.last_cycle_fast_strike_skipped,
                strikes_planned=self.last_cycle_strikes_planned
            )
        timeframe = self._normalize_timeframe()
        if self._live_fast_enabled and self._live_fast_max_stocks > 0 and len(stocks) > self._live_fast_max_stocks:
            total = len(stocks)
            start = self._live_fast_cursor % total
            end = start + self._live_fast_max_stocks
            if end <= total:
                selected = stocks[start:end]
            else:
                selected = stocks[start:] + stocks[:end - total]
            self._live_fast_cursor = (start + self._live_fast_max_stocks) % total
            self.last_cycle_fast_stock_skipped = max(0, total - len(selected))
            stocks = selected
            self.last_cycle_stocks_total = len(stocks)
        symbol_jobs = []
        for symbol in stocks:
            expiry = self._resolve_expiry(symbol, reference_date=cycle_time)
            if not expiry:
                self.last_cycle_missing_expiry += 1
                symbol_jobs.append({'symbol': symbol, 'expiry': None, 'strikes_by_type': {'CE': [], 'PE': []}})
                continue

            spot = quotes.get(symbol)
            if not spot:
                spot = self.angel_api.get_live_quote(symbol, exchange='NSE')
            if not spot or spot.get('ltp') is None:
                self.last_cycle_missing_spot += 1
                symbol_jobs.append({'symbol': symbol, 'expiry': expiry, 'strikes_by_type': {'CE': [], 'PE': []}})
                continue
            spot_ltp = float(spot.get('ltp') or 0)
            if spot_ltp < self._cmp_min or spot_ltp > self._cmp_max:
                cmp_skipped += 1
                continue

            strikes_by_type = self._resolve_strikes_by_type(symbol, expiry, spot_ltp)
            strikes_by_type = self._apply_live_strike_exclusions(symbol, spot_ltp, strikes_by_type)
            strikes_by_type, skipped_fast = self._limit_live_strikes_by_fast_mode(strikes_by_type)
            if skipped_fast:
                self.last_cycle_fast_strike_skipped += skipped_fast
            symbol_jobs.append({
                'symbol': symbol,
                'expiry': expiry,
                'strikes_by_type': strikes_by_type,
                'spot_price': spot_ltp
            })

        self.last_cycle_strikes_total = sum(
            len(job['strikes_by_type'].get('CE', [])) + len(job['strikes_by_type'].get('PE', []))
            for job in symbol_jobs
        )
        self.last_cycle_strikes_planned = self.last_cycle_strikes_total + self.last_cycle_fast_strike_skipped
        if progress_callback:
            progress_callback(
                self.user_id,
                stocks_scanned=self.last_cycle_stocks_scanned,
                stocks_total=self.last_cycle_stocks_total,
                strikes_scanned=self.last_cycle_strikes_scanned,
                strikes_total=self.last_cycle_strikes_total,
                skipped_candles=self.last_cycle_skipped_candles,
                prefilter_skipped=self.last_cycle_prefilter_skipped,
                missing_quotes=self.last_cycle_missing_quotes,
                cmp_skipped=self.last_cycle_cmp_skipped,
                missing_expiry=self.last_cycle_missing_expiry,
                missing_spot=self.last_cycle_missing_spot,
                fast_stock_skipped=self.last_cycle_fast_stock_skipped,
                fast_strike_skipped=self.last_cycle_fast_strike_skipped,
                strikes_planned=self.last_cycle_strikes_planned
            )
        if cmp_skipped and not self._prefilter_enabled:
            logger.info(
                'Live scan skipped %s stocks due to CMP outside range [%s, %s].',
                cmp_skipped,
                int(self._cmp_min),
                int(self._cmp_max)
            )
        self.last_cycle_cmp_skipped += self._safe_int(cmp_skipped, 0)

        detected = []
        if not symbol_jobs:
            return detected

        if self._live_use_queue is None:
            use_queue = str(os.getenv('LIVE_SCAN_USE_QUEUE', 'false')).strip().lower() in ('1', 'true', 'yes', 'on')
        else:
            use_queue = bool(self._live_use_queue)
        if self._queue_force_disabled:
            use_queue = False
        queue = None
        if use_queue:
            try:
                from app.services.queue_manager import get_queue
                queue = get_queue()
            except Exception:
                queue = None
        if queue:
            try:
                from rq import Worker
                active_workers = Worker.all(queue=queue) or []
                if active_workers:
                    self._queue_missing_since = None
                    self._queue_force_disabled = False
                else:
                    now = time.time()
                    if self._queue_missing_since is None:
                        self._queue_missing_since = now
                    elif now - self._queue_missing_since >= 30:
                        self._queue_force_disabled = True
                    queue = None
                    if progress_callback:
                        if self._queue_force_disabled:
                            msg = 'RQ worker missing >30s. Queue disabled; using local scan.'
                        else:
                            msg = 'RQ worker not running. Falling back to local scan.'
                        progress_callback(self.user_id, error=msg)
            except Exception:
                queue = None

        if queue:
            jobs = []
            for job in symbol_jobs:
                payload = {
                    'user_id': self.user_id,
                    'symbol': job['symbol'],
                    'expiry': job['expiry'].isoformat() if hasattr(job['expiry'], 'isoformat') else job['expiry'],
                    'strikes_by_type': job['strikes_by_type'],
                    'timeframe': timeframe,
                    'price_multiplier': float(self.config.price_multiplier or 0),
                    'spot_price': job.get('spot_price')
                }
                jobs.append(queue.enqueue(scan_live_symbol_job, payload))

            pending = set(jobs)
            aborted = False
            while pending:
                finished = [job for job in list(pending) if job.is_finished or job.is_failed or job.is_stopped]
                if not finished:
                    if not self.is_running or self._stop_event.is_set():
                        aborted = True
                        break
                    time.sleep(0.2)
                    continue
                for job in finished:
                    pending.discard(job)
                    self.last_cycle_stocks_scanned += 1
                    symbol_detected = []
                    strikes_scanned = 0
                    skipped_candles = 0
                    try:
                        result = job.result or ()
                        if isinstance(result, tuple):
                            symbol_detected = result[0] or []
                            strikes_scanned = self._safe_int(result[1], 0) if len(result) > 1 else 0
                            skipped_candles = self._safe_int(result[2], 0) if len(result) > 2 else 0
                        else:
                            symbol_detected = result or []
                    except Exception:
                        pass
                    self.last_cycle_strikes_scanned += self._safe_int(strikes_scanned, 0)
                    self.last_cycle_skipped_candles += self._safe_int(skipped_candles, 0)
                    if symbol_detected:
                        for signal in symbol_detected:
                            try:
                                signal['strike_price'] = self._normalize_signal_strike(
                                    signal.get('strike_price'),
                                    signal.get('symbol')
                                )
                                candle_time = signal.get('detected_at')
                                if self._is_new_live_candle_signal(signal.get('symbol'), candle_time):
                                    detected.append(signal)
                                    if emit_signals:
                                        self._emit_live_signal(signal)
                            except Exception:
                                continue
                    if progress_callback:
                        progress_callback(
                            self.user_id,
                            stocks_scanned=self.last_cycle_stocks_scanned,
                            stocks_total=self.last_cycle_stocks_total,
                            strikes_scanned=self.last_cycle_strikes_scanned,
                            strikes_total=self.last_cycle_strikes_total,
                            skipped_candles=self.last_cycle_skipped_candles,
                            prefilter_skipped=self.last_cycle_prefilter_skipped,
                            missing_quotes=self.last_cycle_missing_quotes,
                            cmp_skipped=self.last_cycle_cmp_skipped,
                            missing_expiry=self.last_cycle_missing_expiry,
                            missing_spot=self.last_cycle_missing_spot,
                            fast_stock_skipped=self.last_cycle_fast_stock_skipped,
                            fast_strike_skipped=self.last_cycle_fast_strike_skipped,
                            strikes_planned=self.last_cycle_strikes_planned
                        )
                if not self.is_running or self._stop_event.is_set():
                    aborted = True
                    break
            if aborted:
                return detected
        else:
            executor = ThreadPoolExecutor(max_workers=self.max_workers)
            aborted = False
            try:
                future_map = {
                    executor.submit(
                        self._scan_live_symbol,
                        job['symbol'],
                        job['expiry'],
                        job['strikes_by_type'],
                        timeframe,
                        job.get('spot_price')
                    ): job['symbol']
                    for job in symbol_jobs
                }
                for future in as_completed(future_map):
                    self.last_cycle_stocks_scanned += 1
                    symbol_detected = []
                    strikes_scanned = 0
                    skipped_candles = 0
                    try:
                        result = future.result()
                        if isinstance(result, tuple):
                            symbol_detected = result[0] or []
                            strikes_scanned = self._safe_int(result[1], 0) if len(result) > 1 else 0
                            skipped_candles = self._safe_int(result[2], 0) if len(result) > 2 else 0
                        else:
                            symbol_detected = result or []
                    except Exception as e:
                        logger.error('Live scan worker failed for %s: %s', future_map.get(future), str(e))
                    self.last_cycle_strikes_scanned += self._safe_int(strikes_scanned, 0)
                    self.last_cycle_skipped_candles += self._safe_int(skipped_candles, 0)
                    if symbol_detected:
                        for signal in symbol_detected:
                            detected.append(signal)
                            if emit_signals:
                                self._emit_live_signal(signal)
                    if progress_callback:
                        progress_callback(
                            self.user_id,
                            stocks_scanned=self.last_cycle_stocks_scanned,
                            stocks_total=self.last_cycle_stocks_total,
                            strikes_scanned=self.last_cycle_strikes_scanned,
                            strikes_total=self.last_cycle_strikes_total,
                            skipped_candles=self.last_cycle_skipped_candles,
                            prefilter_skipped=self.last_cycle_prefilter_skipped,
                            missing_quotes=self.last_cycle_missing_quotes,
                            cmp_skipped=self.last_cycle_cmp_skipped,
                            missing_expiry=self.last_cycle_missing_expiry,
                            missing_spot=self.last_cycle_missing_spot,
                            fast_stock_skipped=self.last_cycle_fast_stock_skipped,
                            fast_strike_skipped=self.last_cycle_fast_strike_skipped,
                            strikes_planned=self.last_cycle_strikes_planned
                        )
                    if not self.is_running or self._stop_event.is_set():
                        aborted = True
                        break
            finally:
                executor.shutdown(wait=not aborted, cancel_futures=aborted)

        if detected:
            self._persist_signals(detected, mode='live')

        if self.last_cycle_strikes_scanned > 0 and self.last_cycle_skipped_candles > 0:
            if self.last_cycle_skipped_candles >= self.last_cycle_strikes_scanned:
                self.last_cycle_skip_reason = 'market_closed' if not self._market_is_open(cycle_time) else 'no_candles'
            else:
                self.last_cycle_skip_reason = 'partial_missing'

        return detected

    def _emit_live_signal(self, signal):
        try:
            payload = self._serialize_realtime_signal(signal)
            socketio.emit('new_signal', payload, to=f'user_{self.user_id}')
            send_signal_notification(payload, mode='live', user_id=self.user_id)
        except Exception:
            pass

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
        self.last_backtest_strikes_scanned = 0
        self.last_backtest_strikes_total = 0
        self.last_backtest_instruments_missing = 0
        self.last_backtest_candles_missing = 0
        self.last_backtest_signals = 0
        timeframe = self._normalize_timeframe()
        self._open_backtest_scan_log(from_date=from_date, to_date=to_date, timeframe=timeframe, stocks=stocks)
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
                        'strikes_scanned': int(self.last_backtest_strikes_scanned),
                        'strikes_total': int(self.last_backtest_strikes_total),
                        'signals': int(self.last_backtest_signals),
                        'running': bool(self.is_running),
                        'updated_at': datetime.utcnow().isoformat() + 'Z'
                    },
                    to=f'user_{self.user_id}'
                )

                symbol_detected = []
                symbol_strikes_scanned = 0
                symbol_strikes_total = 0
                symbol_instruments_missing = 0
                symbol_candles_missing = 0
                try:
                    result = future.result()
                    if isinstance(result, tuple):
                        symbol_detected = result[0] or []
                        symbol_strikes_scanned = self._safe_int(result[1], 0) if len(result) > 1 else 0
                        symbol_strikes_total = self._safe_int(result[2], 0) if len(result) > 2 else 0
                        symbol_instruments_missing = self._safe_int(result[3], 0) if len(result) > 3 else 0
                        symbol_candles_missing = self._safe_int(result[4], 0) if len(result) > 4 else 0
                    else:
                        symbol_detected = result or []
                except Exception as e:
                    logger.error('Backtest worker failed for %s: %s', future_map.get(future), str(e))
                self.last_backtest_strikes_scanned += self._safe_int(symbol_strikes_scanned, 0)
                self.last_backtest_strikes_total += self._safe_int(symbol_strikes_total, 0)
                self.last_backtest_instruments_missing += self._safe_int(symbol_instruments_missing, 0)
                self.last_backtest_candles_missing += self._safe_int(symbol_candles_missing, 0)

                if symbol_detected:
                    detected.extend(symbol_detected)
                self._log_api_activity('backtest')

                if progress_callback:
                    progress_callback(
                        self.user_id,
                        scripts_scanned=self.last_backtest_scripts_scanned,
                        strikes_scanned=self.last_backtest_strikes_scanned,
                        strikes_total=self.last_backtest_strikes_total,
                        signals=self.last_backtest_signals
                    )

                if not self.is_running or self._stop_event.is_set():
                    aborted = True
                    break
        finally:
            executor.shutdown(wait=not aborted, cancel_futures=aborted)
            self._close_backtest_scan_log(
                summary={
                    'scripts_scanned': int(self.last_backtest_scripts_scanned or 0),
                    'scripts_total': int(self.last_backtest_scripts_total or 0),
                    'strikes_scanned': int(self.last_backtest_strikes_scanned or 0),
                    'strikes_total': int(self.last_backtest_strikes_total or 0),
                    'instruments_missing': int(self.last_backtest_instruments_missing or 0),
                    'candles_missing': int(self.last_backtest_candles_missing or 0),
                    'signals': int(self.last_backtest_signals or 0),
                    'aborted': bool(aborted or self._stop_event.is_set())
                }
            )

        # Keep explicit final counters available in status.
        self.last_backtest_signals = int(self.last_backtest_signals or 0)
        self.is_running = False

        return detected

    def _scan_live_symbol(self, symbol, expiry, strikes_by_type, timeframe, spot_price=None):
        if not self.is_running or self._stop_event.is_set() or not expiry:
            return [], 0, 0

        strikes_processed = 0
        skipped_candles = 0
        symbol_detected = []
        for option_type in ('CE', 'PE'):
            for strike in strikes_by_type.get(option_type, []):
                if not self.is_running or self._stop_event.is_set():
                    return symbol_detected, strikes_processed, skipped_candles
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
                    option_snapshot = self._get_live_option_snapshot_force_check(
                        symbol=symbol,
                        expiry=expiry,
                        strike=strike,
                        option_type=option_type,
                        timeframe=timeframe
                    )
                if not option_snapshot:
                    skipped_candles += 1
                    self._write_api_error_log(
                        mode='live',
                        event='option_snapshot_missing',
                        details={
                            'symbol': symbol,
                            'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                            'option_type': option_type,
                            'strike': float(strike),
                            'timeframe': timeframe,
                            'api_last_error': getattr(self.angel_api, 'last_error', None)
                        }
                    )
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
                if spot_price is not None:
                    signal['spot_price'] = float(spot_price)
                signal['detected_at'] = candle_time or datetime.utcnow()
                signal['displayed_at'] = datetime.utcnow()
                symbol_detected.append(signal)
        return symbol_detected, strikes_processed, skipped_candles

    def _run_backtest_symbol(self, symbol, from_date, to_date, timeframe):
        if not self.is_running or self._stop_event.is_set():
            return [], 0, 0, 0, 0

        symbol_detected = []
        strikes_scanned = 0
        strikes_total = 0
        instruments_missing = 0
        candles_missing = 0
        segments = self._build_backtest_expiry_segments(symbol, from_date, to_date)
        if not segments:
            self._write_backtest_scan_log(
                'skip_symbol',
                {
                    'symbol': symbol,
                    'reason': 'expiry_schedule_not_found',
                    'from_date': str(from_date),
                    'to_date': str(to_date)
                }
            )
            self._write_backtest_scan_csv(
                'skip_symbol',
                {
                    'symbol': symbol,
                    'window_from': str(from_date),
                    'window_to': str(to_date),
                    'skip_reason': 'expiry_schedule_not_found',
                    'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                    'note': ''
                }
            )
            return symbol_detected, strikes_scanned, strikes_total, instruments_missing, candles_missing

        for segment_from, segment_to, expiry in segments:
            if not self.is_running or self._stop_event.is_set():
                return symbol_detected, strikes_scanned, strikes_total, instruments_missing, candles_missing

            self._write_backtest_scan_log(
                'scan_expiry_segment',
                {
                    'symbol': symbol,
                    'segment_from': segment_from.isoformat() if hasattr(segment_from, 'isoformat') else str(segment_from),
                    'segment_to': segment_to.isoformat() if hasattr(segment_to, 'isoformat') else str(segment_to),
                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry)
                }
            )
            self._write_backtest_scan_csv(
                'scan_expiry_segment',
                {
                    'symbol': symbol,
                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                    'window_from': segment_from.isoformat() if hasattr(segment_from, 'isoformat') else str(segment_from),
                    'window_to': segment_to.isoformat() if hasattr(segment_to, 'isoformat') else str(segment_to),
                    'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                    'note': ''
                }
            )

            segment_spot_fallback = self._infer_spot_from_strike_universe(symbol, expiry)
            rebalance_schedule = self._resolve_backtest_cmp_schedule(
                symbol=symbol,
                start_dt=segment_from,
                end_dt=segment_to,
                base_timeframe=timeframe
            )
            if not rebalance_schedule:
                rebalance_schedule = [(segment_from, segment_to, None)]

            for window_from, window_to, baseline_cmp in rebalance_schedule:
                if not self.is_running or self._stop_event.is_set():
                    return symbol_detected, strikes_scanned, strikes_total, instruments_missing, candles_missing

                active_spot_ltp = baseline_cmp if baseline_cmp is not None else segment_spot_fallback
                if active_spot_ltp is None:
                    self._write_backtest_scan_log(
                        'skip_rebalance_window',
                        {
                            'symbol': symbol,
                            'reason': 'baseline_cmp_not_available',
                            'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                            'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                            'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                            'frequency': self._backtest_rebalance_frequency
                        }
                    )
                    self._write_backtest_scan_csv(
                        'skip_rebalance_window',
                        {
                            'symbol': symbol,
                            'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                            'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                            'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                            'skip_reason': 'baseline_cmp_not_available',
                            'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                            'note': f'frequency={self._backtest_rebalance_frequency}'
                        }
                    )
                    self._write_api_error_log(
                        mode='backtest',
                        event='spot_not_available',
                        details={
                            'symbol': symbol,
                            'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                            'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                            'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                            'frequency': self._backtest_rebalance_frequency,
                            'api_last_error': getattr(self.angel_api, 'last_error', None)
                        }
                    )
                    continue

                self._write_backtest_scan_log(
                    'rebalance_cmp_selected',
                    {
                        'symbol': symbol,
                        'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                        'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                        'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                        'frequency': self._backtest_rebalance_frequency,
                        'strict_first_candle': bool(self._backtest_strict_first_candle),
                        'cmp': float(active_spot_ltp),
                        'source': 'historical_underlying' if baseline_cmp is not None else 'strike_universe_fallback'
                    }
                )
                self._write_backtest_scan_csv(
                    'rebalance_cmp_selected',
                    {
                        'symbol': symbol,
                        'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                        'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                        'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                        'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                        'note': f'cmp={float(active_spot_ltp):.4f};source={"historical_underlying" if baseline_cmp is not None else "strike_universe_fallback"}'
                    }
                )

                strikes_by_type = self._resolve_strikes_by_type(symbol, expiry, float(active_spot_ltp))
                strikes_total += len(strikes_by_type.get('CE', [])) + len(strikes_by_type.get('PE', []))
                for option_type in ('CE', 'PE'):
                    for strike in strikes_by_type.get(option_type, []):
                        if not self.is_running or self._stop_event.is_set():
                            return symbol_detected, strikes_scanned, strikes_total, instruments_missing, candles_missing
                        strikes_scanned += 1

                        instrument = self.angel_api.lookup_option_instrument(
                            underlying=symbol,
                            expiry=expiry,
                            strike=strike,
                            option_type=option_type,
                            exchange='NFO'
                        )
                        if not instrument:
                            instruments_missing += 1
                            self._write_backtest_scan_log(
                                'skip_strike',
                                {
                                    'symbol': symbol,
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'reason': 'instrument_not_found',
                                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                    'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                    'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to)
                                }
                            )
                            self._write_backtest_scan_csv(
                                'skip_strike',
                                {
                                    'symbol': symbol,
                                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                    'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                    'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'skip_reason': 'instrument_not_found',
                                    'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                                    'note': ''
                                }
                            )
                            self._write_api_error_log(
                                mode='backtest',
                                event='option_instrument_missing',
                                details={
                                    'symbol': symbol,
                                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                    'option_type': option_type,
                                    'strike': float(strike)
                                }
                            )
                            continue

                        self._write_backtest_scan_log(
                            'scan_strike',
                            {
                                'symbol': symbol,
                                'option_type': option_type,
                                'strike': float(strike),
                                'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                                'instrument_symbol': instrument.get('symbol'),
                                'instrument_token': instrument.get('token')
                            }
                        )
                        self._write_backtest_scan_csv(
                            'scan_strike',
                            {
                                'symbol': symbol,
                                'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                                'option_type': option_type,
                                'strike': float(strike),
                                'instrument_symbol': instrument.get('symbol'),
                                'instrument_token': instrument.get('token'),
                                'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                                'note': ''
                            }
                        )

                        interval_minutes = max(1, self._safe_int(timeframe, 5))
                        fetch_from = window_from
                        if self._backtest_strict_first_candle:
                            # Include the immediate prior candle so day-1 close is available.
                            fetch_from = window_from - timedelta(minutes=interval_minutes)
                        candles = self._get_historical_candles_cached(
                            instrument=instrument,
                            timeframe=timeframe,
                            from_date=fetch_from,
                            to_date=window_to,
                            exchange='NFO'
                        )
                        if not candles or len(candles) < 2:
                            candles = self._get_historical_candles_force_check(
                                instrument=instrument,
                                timeframe=timeframe,
                                from_date=window_from,
                                to_date=window_to,
                                exchange='NFO'
                            )
                        min_candles_needed = max(2, self._backtest_min_candles_per_strike)
                        if not candles or len(candles) < min_candles_needed:
                            candles_missing += 1
                            self._write_backtest_scan_log(
                                'skip_strike',
                                {
                                    'symbol': symbol,
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'instrument_symbol': instrument.get('symbol'),
                                    'reason': 'insufficient_candles',
                                    'candles_count': len(candles or []),
                                    'min_required_candles': int(min_candles_needed)
                                }
                            )
                            self._write_backtest_scan_csv(
                                'skip_strike',
                                {
                                    'symbol': symbol,
                                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                    'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                    'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'instrument_symbol': instrument.get('symbol'),
                                    'instrument_token': instrument.get('token'),
                                    'candles_count': len(candles or []),
                                    'min_required_candles': int(min_candles_needed),
                                    'skip_reason': 'insufficient_candles',
                                    'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                                    'note': ''
                                }
                            )
                            self._write_api_error_log(
                                mode='backtest',
                                event='historical_candles_missing',
                                details={
                                    'symbol': symbol,
                                    'instrument_symbol': instrument.get('symbol'),
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'candles_count': len(candles or []),
                                    'min_required_candles': int(min_candles_needed),
                                    'api_last_error': getattr(self.angel_api, 'last_error', None)
                                }
                            )
                            continue

                        avg_volume = self._average_candle_volume(candles)
                        self._write_backtest_scan_csv(
                            'candles_loaded',
                            {
                                'symbol': symbol,
                                'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                                'option_type': option_type,
                                'strike': float(strike),
                                'instrument_symbol': instrument.get('symbol'),
                                'instrument_token': instrument.get('token'),
                                'candles_count': len(candles or []),
                                'min_required_candles': int(min_candles_needed),
                                'avg_volume': round(avg_volume, 3),
                                'min_avg_volume': int(self._backtest_min_avg_volume),
                                'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                                'note': ''
                            }
                        )
                        if self._backtest_liquidity_filter_enabled and avg_volume < float(self._backtest_min_avg_volume):
                            self._write_backtest_scan_log(
                                'skip_strike',
                                {
                                    'symbol': symbol,
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'instrument_symbol': instrument.get('symbol'),
                                    'reason': 'liquidity_filter',
                                    'avg_volume': round(avg_volume, 3),
                                    'min_avg_volume': int(self._backtest_min_avg_volume)
                                }
                            )
                            self._write_backtest_scan_csv(
                                'skip_strike',
                                {
                                    'symbol': symbol,
                                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                    'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                    'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'instrument_symbol': instrument.get('symbol'),
                                    'instrument_token': instrument.get('token'),
                                    'candles_count': len(candles or []),
                                    'min_required_candles': int(min_candles_needed),
                                    'avg_volume': round(avg_volume, 3),
                                    'min_avg_volume': int(self._backtest_min_avg_volume),
                                    'skip_reason': 'liquidity_filter',
                                    'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                                    'note': ''
                                }
                            )
                            continue

                        for i in range(1, len(candles)):
                            if not self.is_running or self._stop_event.is_set():
                                return symbol_detected, strikes_scanned, strikes_total, instruments_missing, candles_missing
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
                            threshold = float(previous_close) * float(getattr(self.config, 'price_multiplier', 0) or 0)
                            change_percent = ((current_close - previous_close) / previous_close * 100.0) if previous_close else 0.0
                            self._write_backtest_scan_log(
                                'scan_candle',
                                {
                                    'symbol': symbol,
                                    'instrument_symbol': instrument.get('symbol'),
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'scan_time_utc': datetime.utcnow().isoformat() + 'Z',
                                    'previous_candle_time': prev_time.isoformat() if prev_time else None,
                                    'candle_time': current_time.isoformat() if current_time else None,
                                    'ohlc': {
                                        'open': option_data.get('candle_open'),
                                        'high': option_data.get('candle_high'),
                                        'low': option_data.get('candle_low'),
                                        'close': option_data.get('candle_close')
                                    },
                                    'previous_close': previous_close,
                                    'current_close': current_close,
                                    'volume': option_data.get('volume'),
                                    'rsi': rsi,
                                    'signal_detected': bool(signal)
                                }
                            )
                            self._write_backtest_scan_csv(
                                'scan_candle',
                                {
                                    'symbol': symbol,
                                    'expiry': expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry),
                                    'window_from': window_from.isoformat() if hasattr(window_from, 'isoformat') else str(window_from),
                                    'window_to': window_to.isoformat() if hasattr(window_to, 'isoformat') else str(window_to),
                                    'option_type': option_type,
                                    'strike': float(strike),
                                    'instrument_symbol': instrument.get('symbol'),
                                    'instrument_token': instrument.get('token'),
                                    'candles_count': len(candles or []),
                                    'min_required_candles': int(min_candles_needed),
                                    'avg_volume': round(avg_volume, 3),
                                    'min_avg_volume': int(self._backtest_min_avg_volume),
                                    'previous_candle_time': prev_time.isoformat() if prev_time else '',
                                    'candle_time': current_time.isoformat() if current_time else '',
                                    'candle_open': option_data.get('candle_open'),
                                    'candle_high': option_data.get('candle_high'),
                                    'candle_low': option_data.get('candle_low'),
                                    'candle_close': option_data.get('candle_close'),
                                    'previous_close': previous_close,
                                    'current_close': current_close,
                                    'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                                    'threshold_upper': threshold,
                                    'change_percent': round(change_percent, 4),
                                    'comparison': f'{current_close:.6f}>{threshold:.6f}',
                                    'signal_detected': bool(signal),
                                    'note': ''
                                }
                            )
                            if not signal:
                                continue
                            signal['strike_price'] = self._normalize_signal_strike(
                                signal.get('strike_price'),
                                signal.get('symbol')
                            )
                            signal['expiry_date'] = expiry
                            if active_spot_ltp is not None:
                                signal['spot_price'] = float(active_spot_ltp)
                            signal['detected_at'] = current_time
                            signal['displayed_at'] = datetime.utcnow()
                            signal['mode'] = 'backtest'

                            # Persist and emit immediately for backtest.
                            try:
                                self._persist_signals([signal], mode='backtest')
                                self.last_backtest_signals += 1
                                payload = self._serialize_realtime_signal(signal)
                                socketio.emit('new_signal', payload, to=f'user_{self.user_id}')
                                send_signal_notification(payload, mode='backtest', user_id=self.user_id)
                            except Exception:
                                pass

                            symbol_detected.append(signal)

        return symbol_detected, strikes_scanned, strikes_total, instruments_missing, candles_missing

    def _get_historical_candles_force_check(self, instrument, timeframe, from_date, to_date, exchange='NFO'):
        """Force re-check candles before marking a strike as skipped."""
        symbol = instrument.get('symbol')
        token = instrument.get('token')
        windows = [
            (from_date, to_date),
            (from_date - timedelta(days=1), to_date),
            (from_date - timedelta(days=3), to_date)
        ]
        best = []
        for win_from, win_to in windows:
            try:
                candles = self._get_historical_candles_cached(
                    instrument=instrument,
                    timeframe=timeframe,
                    from_date=win_from,
                    to_date=win_to,
                    exchange=exchange
                ) or []
                if len(candles) > len(best):
                    best = candles
                if len(candles) >= 2:
                    return candles
            except Exception:
                continue
        return best

    def _get_historical_candles_cached(self, instrument, timeframe, from_date, to_date, exchange='NFO'):
        symbol = instrument.get('symbol') if isinstance(instrument, dict) else None
        token = instrument.get('token') if isinstance(instrument, dict) else None
        key = (
            str(symbol or ''),
            str(token or ''),
            str(exchange or ''),
            int(timeframe),
            from_date.strftime('%Y-%m-%d %H:%M'),
            to_date.strftime('%Y-%m-%d %H:%M')
        )
        cached = self._historical_cache.get(key)
        if cached is not None:
            return list(cached)

        candles = self.angel_api.get_historical_data(
            symbol=symbol,
            timeframe=timeframe,
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            symbol_token=token
        )
        if candles:
            self._historical_cache[key] = list(candles)
            if self._historical_cache_max > 0 and len(self._historical_cache) > self._historical_cache_max:
                self._historical_cache.popitem(last=False)
        return candles

    def _get_live_option_snapshot_force_check(self, symbol, expiry, strike, option_type, timeframe):
        """Fallback snapshot construction when API snapshot call returns None."""
        try:
            instrument = self.angel_api.lookup_option_instrument(
                underlying=symbol,
                expiry=expiry,
                strike=strike,
                option_type=option_type,
                exchange='NFO'
            )
            if not instrument:
                return None
            interval_minutes = max(1, self._safe_int(timeframe, 5))
            now = self._market_now()
            latest_start, previous_start = self.angel_api._live_candle_window(now, interval_minutes)
            candles = self.angel_api.get_historical_data(
                symbol=instrument['symbol'],
                timeframe=timeframe,
                from_date=previous_start,
                to_date=latest_start,
                exchange='NFO',
                symbol_token=instrument['token']
            ) or []
            if len(candles) < 2:
                for multiplier in (2, 4):
                    fallback_from = latest_start - timedelta(minutes=interval_minutes * multiplier)
                    candles = self.angel_api.get_historical_data(
                        symbol=instrument['symbol'],
                        timeframe=timeframe,
                        from_date=fallback_from,
                        to_date=latest_start,
                        exchange='NFO',
                        symbol_token=instrument['token']
                    ) or []
                    if len(candles) >= 2:
                        break
            if len(candles) < 2:
                return None
            prev = candles[-2]
            curr = candles[-1]
            prev_close = float(prev[4])
            curr_close = float(curr[4])
            curr_open = float(curr[1]) if len(curr) > 1 else None
            curr_high = float(curr[2]) if len(curr) > 2 else None
            curr_low = float(curr[3]) if len(curr) > 3 else None
            volume = int(float(curr[5])) if len(curr) > 5 else 0
            ts = self._parse_candle_time(curr[0]) if len(curr) > 0 else None
            return {
                'symbol': instrument['symbol'],
                'option_type': option_type,
                'strike_price': strike,
                'previous_candle_close': prev_close,
                'current_candle_open': curr_open,
                'current_candle_high': curr_high,
                'current_candle_low': curr_low,
                'current_candle_close': curr_close,
                'volume': volume,
                'open_interest': 0,
                'rsi': None,
                'candle_time': ts
            }
        except Exception:
            return None

    def _open_backtest_scan_log(self, from_date, to_date, timeframe, stocks):
        if not self._backtest_scan_log_enabled:
            return
        try:
            os.makedirs('logs', exist_ok=True)
            stamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            file_name = f'backtest_scan_user_{self.user_id}_{stamp}.log'
            self._backtest_log_path = os.path.join('logs', file_name)
            self._backtest_log_handle = open(self._backtest_log_path, 'a', encoding='utf-8')
            csv_file_name = f'backtest_scan_user_{self.user_id}_{stamp}.csv'
            self._backtest_csv_path = os.path.join('logs', csv_file_name)
            self._backtest_csv_handle = open(self._backtest_csv_path, 'w', newline='', encoding='utf-8')
            self._backtest_csv_writer = csv.DictWriter(
                self._backtest_csv_handle,
                fieldnames=self._backtest_csv_fields()
            )
            self._backtest_csv_writer.writeheader()
            self._backtest_csv_handle.flush()
            self._write_backtest_scan_log(
                'backtest_start',
                {
                    'user_id': self.user_id,
                    'config_id': self.config_id,
                    'from_date': str(from_date),
                    'to_date': str(to_date),
                    'timeframe': timeframe,
                    'workers': self.max_workers,
                    'rebalance_frequency': self._backtest_rebalance_frequency,
                    'strict_first_candle': bool(self._backtest_strict_first_candle),
                    'liquidity_filter_enabled': bool(self._backtest_liquidity_filter_enabled),
                    'min_candles_per_strike': int(self._backtest_min_candles_per_strike),
                    'min_avg_volume': int(self._backtest_min_avg_volume),
                    'stocks_total': len(stocks or []),
                    'stocks': list(stocks or []),
                    'csv_file': self._backtest_csv_path
                }
            )
            self._write_backtest_scan_csv(
                'backtest_start',
                {
                    'symbol': '',
                    'expiry': '',
                    'window_from': str(from_date),
                    'window_to': str(to_date),
                    'option_type': '',
                    'strike': '',
                    'instrument_symbol': '',
                    'instrument_token': '',
                    'candles_count': '',
                    'min_required_candles': int(self._backtest_min_candles_per_strike),
                    'avg_volume': '',
                    'min_avg_volume': int(self._backtest_min_avg_volume),
                    'price_multiplier': float(getattr(self.config, 'price_multiplier', 0) or 0),
                    'comparison': '',
                    'signal_detected': '',
                    'skip_reason': '',
                    'note': (
                        f'workers={self.max_workers}, frequency={self._backtest_rebalance_frequency}, '
                        f'strict_first_candle={bool(self._backtest_strict_first_candle)}, '
                        f'liquidity_filter={bool(self._backtest_liquidity_filter_enabled)}'
                    )
                }
            )
        except Exception as e:
            logger.error('Failed to open backtest scan log: %s', str(e))
            self._backtest_log_handle = None
            self._backtest_log_path = None
            self._backtest_csv_handle = None
            self._backtest_csv_writer = None
            self._backtest_csv_path = None

    def _write_api_error_log(self, mode, event, details=None):
        if not self._api_error_log_enabled:
            return
        try:
            os.makedirs('logs', exist_ok=True)
            stamp = datetime.utcnow().strftime('%Y%m%d')
            file_path = os.path.join('logs', f'api_error_user_{self.user_id}_{stamp}.log')
            row = {
                'logged_at_utc': datetime.utcnow().isoformat() + 'Z',
                'mode': str(mode or ''),
                'event': str(event or ''),
                'details': details or {}
            }
            line = json.dumps(row, ensure_ascii=True) + '\n'
            with self._api_error_log_lock:
                with open(file_path, 'a', encoding='utf-8') as handle:
                    handle.write(line)
        except Exception:
            pass

    def _write_backtest_scan_log(self, event, payload):
        if not self._backtest_scan_log_enabled or not self._backtest_log_handle:
            return
        row = {
            'event': str(event),
            'logged_at_utc': datetime.utcnow().isoformat() + 'Z',
            'data': payload or {}
        }
        try:
            line = json.dumps(row, ensure_ascii=True) + '\n'
            with self._backtest_log_lock:
                self._backtest_log_handle.write(line)
                self._backtest_log_handle.flush()
        except Exception as e:
            logger.error('Failed to write backtest scan log: %s', str(e))

    def _backtest_csv_fields(self):
        return [
            'logged_at_utc',
            'event',
            'symbol',
            'expiry',
            'window_from',
            'window_to',
            'option_type',
            'strike',
            'instrument_symbol',
            'instrument_token',
            'candles_count',
            'min_required_candles',
            'avg_volume',
            'min_avg_volume',
            'previous_candle_time',
            'candle_time',
            'candle_open',
            'candle_high',
            'candle_low',
            'candle_close',
            'previous_close',
            'current_close',
            'price_multiplier',
            'threshold_upper',
            'change_percent',
            'comparison',
            'signal_detected',
            'skip_reason',
            'note'
        ]

    def _write_backtest_scan_csv(self, event, payload):
        if not self._backtest_scan_log_enabled or not self._backtest_csv_writer or not self._backtest_csv_handle:
            return
        source = payload or {}
        row = {key: '' for key in self._backtest_csv_fields()}
        row['logged_at_utc'] = datetime.utcnow().isoformat() + 'Z'
        row['event'] = str(event or '')
        for key in row.keys():
            if key in ('logged_at_utc', 'event'):
                continue
            value = source.get(key, '')
            if isinstance(value, bool):
                row[key] = '1' if value else '0'
            else:
                row[key] = value
        try:
            with self._backtest_log_lock:
                self._backtest_csv_writer.writerow(row)
                self._backtest_csv_handle.flush()
        except Exception as e:
            logger.error('Failed to write backtest CSV log: %s', str(e))

    def _close_backtest_scan_log(self, summary=None):
        json_handle = self._backtest_log_handle
        csv_handle = self._backtest_csv_handle
        if not json_handle and not csv_handle:
            return
        try:
            self._write_backtest_scan_log(
                'backtest_end',
                {
                    'summary': summary or {},
                    'log_file': self._backtest_log_path,
                    'csv_file': self._backtest_csv_path
                }
            )
            self._write_backtest_scan_csv(
                'backtest_end',
                {
                    'note': json.dumps(summary or {}, ensure_ascii=True)
                }
            )
        finally:
            try:
                with self._backtest_log_lock:
                    if json_handle:
                        json_handle.close()
                    if csv_handle:
                        csv_handle.close()
            except Exception:
                pass
            self._backtest_log_handle = None
            self._backtest_csv_handle = None
            self._backtest_csv_writer = None

    def get_selected_expiries(self, reference_date=None):
        """Return selected expiry per symbol for current config."""
        selected = {}
        stocks = self._resolve_stock_selection()
        ref = reference_date or self._market_now()
        for symbol in stocks:
            expiry = self._resolve_expiry(symbol, reference_date=ref)
            selected[symbol] = expiry.isoformat() if expiry else None
        return selected

    def _last_thursday_of_month(self, year, month):
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        last_day = next_month - timedelta(days=1)
        while last_day.weekday() != 3:
            last_day -= timedelta(days=1)
        return last_day

    def _next_calendar_expiry(self, symbol, reference_date):
        ref_date = reference_date.date() if isinstance(reference_date, datetime) else reference_date
        expiry_type = DataProcessor.get_expiry_type(symbol)
        if expiry_type == 'weekly':
            days_ahead = 3 - ref_date.weekday()
            if days_ahead < 0:
                days_ahead += 7
            return ref_date + timedelta(days=days_ahead)

        # Monthly: last Thursday of current month; if already passed, roll to next month.
        monthly = self._last_thursday_of_month(ref_date.year, ref_date.month)
        if monthly >= ref_date:
            return monthly
        if ref_date.month == 12:
            return self._last_thursday_of_month(ref_date.year + 1, 1)
        return self._last_thursday_of_month(ref_date.year, ref_date.month + 1)

    def _resolve_backtest_expiry(self, symbol, reference_date):
        """
        Resolve expiry for backtest with rolling semantics.
        Prefers scrip-master expiry near calendar expiry (holiday-shift tolerant),
        otherwise falls back to strict calendar next expiry (never far-future jump).
        """
        ref_date = reference_date.date() if isinstance(reference_date, datetime) else reference_date
        if not isinstance(ref_date, date):
            return None

        calendar_expiry = self._next_calendar_expiry(symbol, ref_date)
        expiry_type = DataProcessor.get_expiry_type(symbol)
        tolerance_days = 10 if expiry_type == 'monthly' else 3

        available = []
        try:
            available = self.angel_api.get_available_option_expiries(symbol, exchange='NFO') or []
        except Exception:
            available = []

        if available:
            candidates = [e for e in available if e >= ref_date]
            if candidates:
                for candidate in candidates:
                    if calendar_expiry and abs((candidate - calendar_expiry).days) <= tolerance_days:
                        return candidate
                # If no candidate aligns with expected cycle, avoid wrong far-future contract.
                return calendar_expiry

        return calendar_expiry

    def _build_backtest_expiry_segments(self, symbol, from_date, to_date):
        """
        Build rolling expiry segments across the backtest window.
        Each segment scans candles only until its expiry date, then rolls.
        """
        start_dt = from_date if isinstance(from_date, datetime) else datetime.combine(from_date, datetime.min.time())
        end_dt = to_date if isinstance(to_date, datetime) else datetime.combine(to_date, datetime.max.time())
        cursor = start_dt.date()
        end_date = end_dt.date()
        segments = []
        guard = 0

        while cursor <= end_date and guard < 500:
            guard += 1
            expiry = self._resolve_backtest_expiry(symbol, cursor)
            if not expiry or expiry < cursor:
                break

            seg_start = start_dt if cursor == start_dt.date() else datetime.combine(cursor, datetime.min.time())
            seg_end_date = min(end_date, expiry)
            seg_end = end_dt if seg_end_date == end_date else datetime.combine(seg_end_date, datetime.max.time())
            if seg_end < seg_start:
                break

            segments.append((seg_start, seg_end, expiry))
            cursor = seg_end_date + timedelta(days=1)

        return segments

    def _build_backtest_rebalance_windows(self, start_dt, end_dt, frequency=None):
        freq = str(frequency or self._backtest_rebalance_frequency or 'weekly').strip().lower()
        if freq not in ('daily', 'weekly', 'monthly'):
            freq = 'weekly'
        windows = []
        cursor = start_dt
        guard = 0
        while cursor <= end_dt and guard < 500:
            guard += 1
            if freq == 'daily':
                period_end = datetime.combine(cursor.date(), datetime.max.time())
            elif freq == 'monthly':
                if cursor.month == 12:
                    next_month = date(cursor.year + 1, 1, 1)
                else:
                    next_month = date(cursor.year, cursor.month + 1, 1)
                month_end = next_month - timedelta(days=1)
                period_end = datetime.combine(month_end, datetime.max.time())
            else:
                week_end = cursor.date() + timedelta(days=(6 - cursor.weekday()))
                period_end = datetime.combine(week_end, datetime.max.time())

            if period_end > end_dt:
                period_end = end_dt
            windows.append((cursor, period_end))
            cursor = period_end + timedelta(seconds=1)
        return windows

    def _resolve_backtest_cmp_schedule(self, symbol, start_dt, end_dt, base_timeframe):
        """
        Resolve baseline CMP values from historical underlying candles (NSE).
        Returns list of tuples: (window_start_dt, window_end_dt, cmp_value_or_none).
        """
        windows = self._build_backtest_rebalance_windows(start_dt, end_dt)
        if not windows:
            return []

        cmp_timeframe = max(15, min(60, self._safe_int(base_timeframe, 5)))
        candles = []
        try:
            candles = self.angel_api.get_historical_data(
                symbol=symbol,
                timeframe=cmp_timeframe,
                from_date=start_dt,
                to_date=end_dt,
                exchange='NSE'
            ) or []
        except Exception:
            candles = []

        parsed = []
        for row in candles:
            try:
                ts = self._parse_candle_time(row[0]) if len(row) > 0 else None
                if ts and ts.tzinfo is not None:
                    ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
                close_val = float(row[4]) if len(row) > 4 else None
                if ts and close_val is not None:
                    parsed.append((ts, close_val))
            except Exception:
                continue
        parsed.sort(key=lambda item: item[0])

        schedule = []
        for window_start, window_end in windows:
            baseline_cmp = None
            if self._backtest_strict_first_candle:
                for ts, close_val in parsed:
                    if ts < window_start:
                        continue
                    if ts > window_end:
                        break
                    baseline_cmp = close_val
                    break
            else:
                prior_close = None
                first_inside_close = None
                for ts, close_val in parsed:
                    if ts <= window_start:
                        prior_close = close_val
                        continue
                    if ts > window_end:
                        break
                    if first_inside_close is None:
                        first_inside_close = close_val
                baseline_cmp = prior_close if prior_close is not None else first_inside_close

            schedule.append((window_start, window_end, baseline_cmp))
        return schedule

    def _build_weekly_windows(self, start_dt, end_dt):
        return self._build_backtest_rebalance_windows(start_dt, end_dt, frequency='weekly')

    def _resolve_backtest_weekly_cmp_schedule(self, symbol, start_dt, end_dt, base_timeframe):
        return self._resolve_backtest_cmp_schedule(symbol, start_dt, end_dt, base_timeframe)

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
                spot_price=signal.get('spot_price'),
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

    def _average_candle_volume(self, candles):
        if not candles:
            return 0.0
        volumes = []
        for row in candles:
            try:
                if len(row) > 5:
                    volumes.append(float(row[5] or 0))
            except Exception:
                continue
        if not volumes:
            return 0.0
        return float(sum(volumes) / len(volumes))

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
        normalized_candidates = self._normalize_strike_candidates_for_spot(spot_price, candidates, symbol=symbol)
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
        if configured >= 0:
            return configured
        # Fallback for malformed values.
        return 0

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
            return [], {'missing_quotes': 0, 'cmp_skipped': 0, 'prefilter_skipped': 0}
        if not self._prefilter_enabled:
            return list(stocks), {'missing_quotes': 0, 'cmp_skipped': 0, 'prefilter_skipped': 0}

        ranked = []
        missing_quotes = 0
        skipped_by_cmp = 0
        for symbol in stocks:
            quote = quotes.get(symbol)
            if not quote:
                missing_quotes += 1
                continue
            ltp = float(quote.get('ltp') or 0)
            if ltp < self._cmp_min or ltp > self._cmp_max:
                skipped_by_cmp += 1
                continue
            close = float(quote.get('close') or 0)
            volume = self._safe_int(quote.get('volume', 0), 0)
            move_pct = abs(((ltp - close) / close) * 100.0) if close else 0.0
            ranked.append({
                'symbol': symbol,
                'move_pct': move_pct,
                'volume': volume
            })

        if not ranked:
            return list(stocks), {
                'missing_quotes': missing_quotes,
                'cmp_skipped': skipped_by_cmp,
                'prefilter_skipped': max(0, len(stocks) - len(stocks))
            }

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
        if skipped_by_cmp:
            logger.info(
                'Live prefilter skipped %s stocks due to CMP outside range [%s, %s].',
                skipped_by_cmp,
                int(self._cmp_min),
                int(self._cmp_max)
            )
        return selected, {
            'missing_quotes': missing_quotes,
            'cmp_skipped': skipped_by_cmp,
            'prefilter_skipped': max(0, len(stocks) - len(selected))
        }

    def _limit_live_strikes_by_fast_mode(self, strikes_by_type):
        if not self._live_fast_enabled:
            return strikes_by_type, 0
        cap = self._safe_int(self._live_fast_max_strikes_per_stock, 0)
        if cap <= 0:
            return strikes_by_type, 0
        ce = list((strikes_by_type or {}).get('CE', []) or [])
        pe = list((strikes_by_type or {}).get('PE', []) or [])
        total = len(ce) + len(pe)
        if total <= cap:
            return strikes_by_type, 0
        ce_cap = max(0, cap // 2)
        pe_cap = max(0, cap - ce_cap)
        if len(ce) < ce_cap:
            pe_cap = min(len(pe), cap - len(ce))
            ce_cap = len(ce)
        elif len(pe) < pe_cap:
            ce_cap = min(len(ce), cap - len(pe))
            pe_cap = len(pe)
        trimmed = {
            'CE': ce[:ce_cap],
            'PE': pe[:pe_cap]
        }
        skipped = max(0, total - (len(trimmed['CE']) + len(trimmed['PE'])))
        return trimmed, skipped

    def _log_api_activity(self, mode):
        now = time.time()
        if (now - self._last_api_stats_log_at) < 30:
            return
        self._last_api_stats_log_at = now
        try:
            stats = self.angel_api.get_rate_limit_stats(window_seconds=60, clear=True)
            logger.info(
                'API activity (%s): requests/min=%s rate_limit_hits/min=%s',
                mode,
                stats.get('requests', 0),
                stats.get('rate_limit_hits', 0)
            )
        except Exception:
            pass

    def _market_now(self):
        try:
            return self.angel_api.market_now()
        except Exception:
            return datetime.now()

    def _market_is_open(self, dt=None):
        try:
            value = dt or self._market_now()
            if isinstance(value, datetime) and value.tzinfo is not None:
                local = value
            else:
                local = value
            if local.weekday() >= 5:
                return False
            open_time = dt_time(9, 15)
            close_time = dt_time(15, 30)
            return open_time <= local.time() <= close_time
        except Exception:
            return True

    def _claim_live_candle_slot(self, timeframe_minutes):
        now = self._market_now()
        interval = max(1, self._safe_int(timeframe_minutes, 5))
        bucket_minute = (now.minute // interval) * interval
        current_bucket = now.replace(minute=bucket_minute, second=0, microsecond=0)
        close_ready_at = current_bucket + timedelta(seconds=max(0, self._candle_close_delay_seconds))
        if now < close_ready_at:
            return False, None

        latest_closed_candle = current_bucket
        if self._last_processed_live_candle and latest_closed_candle <= self._last_processed_live_candle:
            return False, latest_closed_candle

        self._last_processed_live_candle = latest_closed_candle
        return True, latest_closed_candle

    def _pick_strikes_around_spot(self, spot_price, strikes, strike_range):
        if not strikes:
            return []
        strike_count = self._safe_int(strike_range, 0)
        sorted_strikes = sorted(float(s) for s in strikes)
        if strike_count <= 0:
            return sorted_strikes
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
        # strike_range means strikes above/below ATM, so total = (2 * strike_count + 1).
        total = (strike_count * 2) + 1
        return ordered[:total]

    def _split_strikes_for_option_types(self, spot_price, strikes, strike_count):
        normalized = sorted(set(float(s) for s in (strikes or [])))
        count = self._safe_int(strike_count, 0)
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
        if count <= 0:
            return {
                'PE': pe_ordered,
                'CE': ce_ordered
            }
        return {
            'PE': pe_ordered[:count],
            'CE': ce_ordered[:count]
        }

    def _apply_live_strike_exclusions(self, symbol, spot_price, strikes_by_type):
        """Live-mode strike exclusion: remove ATM and next N strikes on both sides."""
        try:
            exclude_count = self._safe_int(getattr(self.config, 'live_exclude_atm_strikes', 0), 0)
        except Exception:
            exclude_count = 0
        if exclude_count <= 0:
            return strikes_by_type

        ce_strikes = [float(s) for s in (strikes_by_type or {}).get('CE', []) if s is not None]
        pe_strikes = [float(s) for s in (strikes_by_type or {}).get('PE', []) if s is not None]
        combined = sorted(set(ce_strikes + pe_strikes))
        if not combined:
            return strikes_by_type

        try:
            spot = float(spot_price or 0)
        except Exception:
            spot = 0.0
        atm_idx = min(range(len(combined)), key=lambda i: abs(combined[i] - spot))

        exclude_indices = {atm_idx}
        for i in range(1, exclude_count + 1):
            if atm_idx - i >= 0:
                exclude_indices.add(atm_idx - i)
            if atm_idx + i < len(combined):
                exclude_indices.add(atm_idx + i)
        exclude_values = {combined[i] for i in exclude_indices}

        return {
            'CE': [s for s in ce_strikes if s not in exclude_values],
            'PE': [s for s in pe_strikes if s not in exclude_values]
        }

    def _normalize_strike_candidates_for_spot(self, spot_price, strikes, symbol=None):
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
            return self._normalize_strike_candidates_without_spot(raw, symbol=symbol)

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

    def _normalize_strike_candidates_without_spot(self, strikes, symbol=None):
        if not strikes:
            return []
        raw = [float(s) for s in strikes if s is not None]
        if not raw:
            return []

        step = float(self._strike_step(symbol or 'NIFTY'))
        best_scaled = raw
        best_score = float('inf')
        for factor in (1.0, 0.01, 0.1, 10.0, 100.0):
            scaled = [round(s * factor, 4) for s in raw]
            if not scaled:
                continue
            positive = [s for s in scaled if s > 0]
            if not positive:
                continue
            median = sorted(positive)[len(positive) // 2]
            grid_errors = [abs((s / step) - round(s / step)) for s in positive[: min(len(positive), 200)]]
            grid_score = (sum(grid_errors) / len(grid_errors)) if grid_errors else 0.0
            range_penalty = 0.0 if (step <= median <= 200000.0) else 5.0
            score = grid_score + range_penalty
            if score < best_score:
                best_score = score
                best_scaled = scaled

        return sorted(set(round(s, 2) for s in best_scaled if s > 0))

    def _infer_spot_from_strike_universe(self, symbol, expiry):
        try:
            expiry_date = expiry if not isinstance(expiry, datetime) else expiry.date()
            strikes = self.angel_api.get_option_strikes_for_expiry(
                underlying=symbol,
                expiry=expiry_date,
                exchange='NFO'
            ) or []
            normalized = self._normalize_strike_candidates_for_spot(0, strikes, symbol=symbol)
            if not normalized:
                return None
            ordered = sorted(normalized)
            mid = len(ordered) // 2
            if len(ordered) % 2 == 0 and mid > 0:
                return float((ordered[mid - 1] + ordered[mid]) / 2.0)
            return float(ordered[mid])
        except Exception:
            return None

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
