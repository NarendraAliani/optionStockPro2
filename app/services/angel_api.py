"""
Angel One SmartAPI Integration Service
"""
from smartapi import SmartConnect
try:
    import pyotp
except Exception:
    pyotp = None
from datetime import datetime, timedelta, date, timezone
from collections import deque
from threading import Lock
import time
import logging
import os
import json
import requests
import re
from urllib.parse import urljoin
import socket
import uuid
import subprocess
import platform
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

logger = logging.getLogger(__name__)


def get_scrip_master_path():
    return os.getenv(
        'ANGEL_SCRIP_MASTER_PATH',
        os.path.join(os.getcwd(), 'data', 'OpenAPIScripMaster.json')
    )


def get_scrip_master_url():
    return os.getenv(
        'ANGEL_SCRIP_MASTER_URL',
        'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    )


def download_scrip_master(path=None, url=None, timeout=20, force=True):
    """
    Download the scrip master file to the given path.

    Returns:
        tuple: (success: bool, message: str, path: str)
    """
    target_path = path or get_scrip_master_path()
    source_url = url or get_scrip_master_url()

    if not source_url:
        return False, 'Scrip master URL is not configured.', target_path

    if os.path.exists(target_path) and not force:
        return True, 'Scrip master already exists.', target_path

    try:
        response = requests.get(source_url, timeout=timeout)
        response.raise_for_status()
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, 'wb') as f:
            f.write(response.content)
        return True, 'Scrip master downloaded successfully.', target_path
    except Exception as e:
        return False, f'Failed to download scrip master: {str(e)}', target_path


def get_scrip_master_status(path=None):
    target_path = path or get_scrip_master_path()
    if not os.path.exists(target_path):
        return {
            'exists': False,
            'path': target_path,
            'size': None,
            'updated_at': None
        }

    stat = os.stat(target_path)
    return {
        'exists': True,
        'path': target_path,
        'size': stat.st_size,
        'updated_at': datetime.fromtimestamp(stat.st_mtime)
    }


class AngelOneAPI:
    """Wrapper for Angel One SmartAPI"""
    
    def __init__(self, api_key, client_id, password, totp_secret=None, totp_code=None, use_mpin=False):
        """
        Initialize Angel One API client
        
        Args:
            api_key: Angel One API key
            client_id: Angel One client ID
            password: Angel One password
            totp_secret: TOTP secret for 2FA (optional)
        """
        self.api_key = api_key
        self.client_id = client_id
        self.password = password
        self.totp_secret = totp_secret
        self.totp_code = totp_code
        self.use_mpin = use_mpin
        self.smart_api = None
        self.auth_token = None
        self.feed_token = None
        self.refresh_token = None
        self.cache = {}
        self.cache_expiry = {}
        self._candle_pair_cache = {}
        self._candle_pair_lock = Lock()
        self.scrip_master = None
        self.scrip_index = {}
        self._strike_cache = {}
        self._strike_cache_loaded = False
        self._strike_cache_lock = Lock()
        self._strike_cache_enabled = str(os.getenv('STRIKE_CACHE_ENABLED', 'true')).strip().lower() in ('1', 'true', 'yes', 'on')
        self._strike_cache_ttl_seconds = int(os.getenv('STRIKE_CACHE_TTL_SECONDS', '21600'))  # 6 hours
        self._strike_cache_path = os.getenv(
            'STRIKE_CACHE_PATH',
            os.path.join(os.getcwd(), 'data', 'strike_universe_cache.json')
        )
        self._strike_cache_precompute = str(os.getenv('STRIKE_CACHE_PRECOMPUTE', 'false')).strip().lower() in ('1', 'true', 'yes', 'on')
        self._strike_cache_exchange = str(os.getenv('STRIKE_CACHE_EXCHANGE', 'NFO') or 'NFO').strip().upper()
        self.scrip_master_path = os.getenv(
            'ANGEL_SCRIP_MASTER_PATH',
            os.path.join(os.getcwd(), 'data', 'OpenAPIScripMaster.json')
        )
        self.scrip_master_url = os.getenv(
            'ANGEL_SCRIP_MASTER_URL',
            'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        )
        self.root_url = os.getenv('SMARTAPI_ROOT_URL')
        self.rate_limit_rps = float(os.getenv('SMARTAPI_RATE_LIMIT_RPS', '2'))
        self.retry_max = int(os.getenv('SMARTAPI_RETRY_MAX', '3'))
        self.retry_base = float(os.getenv('SMARTAPI_RETRY_BASE_SECONDS', '1.0'))
        self.rate_limit_retry_max = int(os.getenv('SMARTAPI_RATE_LIMIT_RETRY_MAX', '7'))
        self.rate_limit_retry_base = float(os.getenv('SMARTAPI_RATE_LIMIT_RETRY_BASE_SECONDS', '2.0'))
        self._last_call_at = None
        self._last_rate_limit_log_at = 0.0
        self._request_times = deque()
        self._rate_limit_times = deque()
        self.last_error = None
        self._client_public_ip = os.getenv('SMARTAPI_CLIENT_PUBLIC_IP')
        self._client_local_ip = os.getenv('SMARTAPI_CLIENT_LOCAL_IP')
        self._client_mac = os.getenv('SMARTAPI_CLIENT_MAC')
        self._use_real_ips = os.getenv('SMARTAPI_USE_REAL_IPS', 'true').lower() == 'true'
        self._market_tz_name = str(os.getenv('TIMEZONE', 'Asia/Kolkata') or 'Asia/Kolkata').strip()
        self._market_tz = None
        if ZoneInfo:
            try:
                self._market_tz = ZoneInfo(self._market_tz_name)
            except Exception:
                self._market_tz = None

    def market_now(self):
        tz = self._market_tz or timezone.utc
        return datetime.now(tz)

    def _ensure_market_time(self, dt):
        if not dt:
            return dt
        tz = self._market_tz or timezone.utc
        if isinstance(dt, datetime) and dt.tzinfo is None:
            return dt.replace(tzinfo=tz)
        if isinstance(dt, datetime):
            try:
                return dt.astimezone(tz)
            except Exception:
                return dt
        return dt
    
    def authenticate(self):
        """
        Authenticate with Angel One API
        
        Returns:
            bool: True if authentication successful
        """
        try:
            if self.root_url:
                self.smart_api = SmartConnect(api_key=self.api_key, root=self.root_url)
            else:
                self.smart_api = SmartConnect(api_key=self.api_key)
            self._apply_client_headers()
            
            # Generate TOTP if secret provided
            totp = None
            if self.totp_code:
                totp = str(self.totp_code).strip()
            elif self.totp_secret and pyotp:
                totp = pyotp.TOTP(self.totp_secret).now()
            elif self.totp_secret and not pyotp:
                logger.warning('pyotp not installed; continuing without TOTP.')

            # Login
            # NOTE: Many users report loginByMPIN returning HTTP 200 with empty body.
            # Use loginByPassword endpoint for both password and PIN (MPIN) login.
            if self.use_mpin and not totp:
                self.last_error = 'TOTP is required when using PIN login.'
                logger.error(f'Angel One authentication error: {self.last_error}')
                return False

            if totp:
                login_params = {
                    'clientcode': self.client_id,
                    'password': self.password,  # PIN or password
                    'totp': totp
                }
                try:
                    data = self.smart_api._postRequest('api.login', login_params)
                except Exception as e:
                    data = self._direct_login('api.login', login_params, error=e)
                    if not data:
                        return False
            else:
                # Login (SmartAPI SDK signature may vary)
                try:
                    data = self.smart_api.generateSession(
                        clientCode=self.client_id,
                        password=self.password
                    )
                except TypeError:
                    # Fallback for SDKs that don't accept keyword args
                    data = self.smart_api.generateSession(self.client_id, self.password)
            
            if data['status']:
                payload = data.get('data', {})
                self.auth_token = payload.get('jwtToken')
                self.feed_token = payload.get('feedToken')
                self.refresh_token = payload.get('refreshToken')
                try:
                    if self.auth_token:
                        self.smart_api.setAccessToken(self.auth_token)
                    if self.refresh_token:
                        self.smart_api.setRefreshToken(self.refresh_token)
                    if self.feed_token:
                        self.smart_api.setFeedToken(self.feed_token)
                    self.smart_api.setUserId(self.client_id)
                except Exception:
                    pass
                self.last_error = None
                logger.info(f'Angel One authentication successful for client {self.client_id}')
                return True
            else:
                self.last_error = data.get('message') or 'Authentication failed'
                logger.error(f'Angel One authentication failed: {self.last_error}')
                return False
        
        except Exception as e:
            self.last_error = str(e)
            logger.error(f'Angel One authentication error: {self.last_error}')
            return False

    def _direct_login(self, route_key, params, error=None):
        """Fallback login request with raw HTTP for better error visibility."""
        try:
            if error and 'parse the JSON response' not in str(error):
                self.last_error = str(error)
                logger.error(f'Angel One authentication error: {self.last_error}')
                return None

            route = self.smart_api._routes.get(route_key)
            if not route:
                self.last_error = f'Unknown login route: {route_key}'
                return None

            root = self.root_url or getattr(self.smart_api, 'root', None) or SmartConnect._rootUrl
            url = urljoin(root, route)
            headers = self.smart_api.requestHeaders()
            headers['Content-type'] = 'application/json'
            headers['Accept'] = 'application/json'
            headers['User-Agent'] = 'OptionSignalPro/1.0'

            payload = json.dumps(params)
            response = requests.post(url, data=payload, headers=headers, timeout=10)
            try:
                data = response.json()
            except Exception:
                text = (response.text or '').strip()
                content_len = response.headers.get('Content-Length') or 'unknown'
                snippet = text[:200] if text else '<empty>'
                self.last_error = (
                    f'Empty/invalid response (HTTP {response.status_code}, len={content_len}) '
                    f'from {root}. Response: {snippet}'
                )
                logger.error(f'Angel One authentication error: {self.last_error}')
                # Try fallback root if different
                fallback_root = 'https://apiconnect.angelbroking.com'
                if root != fallback_root:
                    try:
                        url_fb = urljoin(fallback_root, route)
                        response_fb = requests.post(url_fb, data=payload, headers=headers, timeout=10)
                        try:
                            data_fb = response_fb.json()
                            return data_fb
                        except Exception:
                            text_fb = (response_fb.text or '').strip()
                            content_len_fb = response_fb.headers.get('Content-Length') or 'unknown'
                            snippet_fb = text_fb[:200] if text_fb else '<empty>'
                            self.last_error = (
                                f'Empty/invalid response (HTTP {response_fb.status_code}, len={content_len_fb}) '
                                f'from {fallback_root}. Response: {snippet_fb}'
                            )
                            logger.error(f'Angel One authentication error: {self.last_error}')
                    except Exception as e:
                        self.last_error = str(e)
                        logger.error(f'Angel One authentication error: {self.last_error}')
                return None

            return data
        except Exception as e:
            self.last_error = str(e)
            logger.error(f'Angel One authentication error: {self.last_error}')
            return None

    def attach_tokens(self, auth_token, refresh_token, feed_token):
        """Attach cached tokens to SmartConnect instance."""
        if self.root_url:
            self.smart_api = SmartConnect(api_key=self.api_key, root=self.root_url)
        else:
            self.smart_api = SmartConnect(api_key=self.api_key)
        self._apply_client_headers()
        self.auth_token = auth_token
        self.refresh_token = refresh_token
        self.feed_token = feed_token
        try:
            if self.auth_token:
                self.smart_api.setAccessToken(self.auth_token)
            if self.refresh_token:
                self.smart_api.setRefreshToken(self.refresh_token)
            if self.feed_token:
                self.smart_api.setFeedToken(self.feed_token)
            self.smart_api.setUserId(self.client_id)
        except Exception:
            pass

    def _throttle(self):
        if not self.rate_limit_rps or self.rate_limit_rps <= 0:
            return
        min_interval = 1.0 / self.rate_limit_rps
        now = datetime.now().timestamp()
        if self._last_call_at is None:
            self._last_call_at = now
            return
        elapsed = now - self._last_call_at
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_call_at = datetime.now().timestamp()

    def _apply_client_headers(self):
        """Optionally override client IP/MAC headers for SmartAPI."""
        public_ip = self._client_public_ip
        local_ip = self._client_local_ip
        mac = self._client_mac

        def is_ipv4(value):
            if not value:
                return False
            parts = value.strip().split('.')
            if len(parts) != 4:
                return False
            for part in parts:
                if not part.isdigit():
                    return False
                num = int(part)
                if num < 0 or num > 255:
                    return False
            return True

        def resolve_local_ip():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.connect(('8.8.8.8', 80))
                ip = sock.getsockname()[0]
                sock.close()
                if is_ipv4(ip):
                    return ip
            except Exception:
                pass
            try:
                ip = socket.gethostbyname(socket.gethostname())
                if is_ipv4(ip) and ip != '127.0.0.1':
                    return ip
            except Exception:
                pass
            return None

        def resolve_public_ip():
            urls = [
                'https://api.ipify.org',
                'https://ifconfig.me/ip',
                'https://checkip.amazonaws.com'
            ]
            for url in urls:
                try:
                    ip = requests.get(url, timeout=4).text.strip()
                    if is_ipv4(ip):
                        return ip
                except Exception:
                    continue
            return None

        def is_valid_mac(value):
            if not value:
                return False
            cleaned = value.replace('-', ':').lower()
            parts = cleaned.split(':')
            if len(parts) != 6:
                return False
            try:
                nums = [int(part, 16) for part in parts]
            except Exception:
                return False
            if all(num == 0 for num in nums):
                return False
            if all(num == nums[0] for num in nums):
                return False
            return True

        def resolve_mac():
            # Try uuid.getnode first
            try:
                candidate = ':'.join(re.findall('..', '%012x' % uuid.getnode()))
                if is_valid_mac(candidate):
                    return candidate
            except Exception:
                pass

            # Windows: use getmac
            if platform.system().lower() == 'windows':
                try:
                    output = subprocess.check_output(['getmac', '/fo', 'csv', '/nh'], text=True, timeout=3)
                    for line in output.splitlines():
                        parts = [p.strip().strip('"') for p in line.split(',')]
                        if not parts:
                            continue
                        mac_candidate = parts[0]
                        if is_valid_mac(mac_candidate):
                            return mac_candidate.replace('-', ':')
                except Exception:
                    pass
            return None

        if self._use_real_ips:
            if not local_ip:
                local_ip = resolve_local_ip()
            if not public_ip:
                public_ip = resolve_public_ip()
            if not mac:
                mac = resolve_mac()

        if public_ip:
            self.smart_api.clientPublicIp = public_ip
        if local_ip:
            self.smart_api.clientLocalIp = local_ip
        if mac:
            self.smart_api.clientMacAddress = mac

    def _with_retry(self, func, *args, **kwargs):
        delay = self.retry_base
        rate_delay = max(self.rate_limit_retry_base, self.retry_base)
        last_error = None
        max_attempts = max(self.retry_max, self.rate_limit_retry_max)
        for attempt in range(max_attempts + 1):
            try:
                self._throttle()
                self._record_request()
                result = func(*args, **kwargs)
                if self._is_rate_limit_response(result):
                    message = self._extract_error_message(result) or 'Access denied because of exceeding access rate'
                    raise RuntimeError(message)
                return result
            except Exception as e:
                last_error = e
                is_rate_limit = self._is_rate_limit_error(e)
                if is_rate_limit:
                    self._record_rate_limit()
                allowed_retries = self.rate_limit_retry_max if is_rate_limit else self.retry_max
                if attempt < allowed_retries:
                    sleep_for = rate_delay if is_rate_limit else delay
                    time.sleep(sleep_for)
                    if is_rate_limit:
                        rate_delay = min(rate_delay * 1.8, 12.0)
                    else:
                        delay *= 2
        raise last_error

    def _extract_error_message(self, response):
        if isinstance(response, dict):
            return (
                response.get('message')
                or response.get('error')
                or response.get('errorcode')
                or response.get('detail')
            )
        return None

    def _is_rate_limit_response(self, response):
        if not isinstance(response, dict):
            return False
        status = response.get('status')
        if status is True:
            return False
        message = self._extract_error_message(response)
        return self._is_rate_limit_error(message)

    def _is_rate_limit_error(self, error):
        text = str(error or '').lower()
        indicators = (
            'exceeding access rate',
            'access rate',
            'rate limit',
            'too many requests',
            'http 429',
            'status 429'
        )
        return any(marker in text for marker in indicators)

    def _log_rate_limit_warning(self, message):
        now = time.time()
        # Avoid flooding terminal/logs with one warning per symbol.
        if now - self._last_rate_limit_log_at >= 30:
            logger.warning(message)
            self._last_rate_limit_log_at = now

    def _record_request(self):
        now = time.time()
        self._request_times.append(now)
        self._trim_times(self._request_times, now, window_seconds=60)

    def _record_rate_limit(self):
        now = time.time()
        self._rate_limit_times.append(now)
        self._trim_times(self._rate_limit_times, now, window_seconds=60)

    @staticmethod
    def _trim_times(queue, now, window_seconds=60):
        cutoff = now - float(window_seconds)
        while queue and queue[0] < cutoff:
            queue.popleft()

    def get_rate_limit_stats(self, window_seconds=60, clear=False):
        now = time.time()
        self._trim_times(self._request_times, now, window_seconds=window_seconds)
        self._trim_times(self._rate_limit_times, now, window_seconds=window_seconds)
        stats = {
            'window_seconds': int(window_seconds),
            'requests': len(self._request_times),
            'rate_limit_hits': len(self._rate_limit_times)
        }
        if clear:
            self._request_times.clear()
            self._rate_limit_times.clear()
        return stats

    def load_scrip_master(self):
        """
        Load the Angel One scrip master file for symbol token lookup.

        Returns:
            bool: True if loaded, False otherwise
        """
        if self.scrip_master is not None:
            return True

        path = self.scrip_master_path
        if not os.path.exists(path):
            if self.scrip_master_url:
                try:
                    response = requests.get(self.scrip_master_url, timeout=10)
                    response.raise_for_status()
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, 'wb') as f:
                        f.write(response.content)
                except Exception as e:
                    logger.error(f'Failed to download scrip master: {str(e)}')
                    return False
            else:
                logger.warning('Scrip master file not found. Set ANGEL_SCRIP_MASTER_PATH or ANGEL_SCRIP_MASTER_URL.')
                return False

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f'Failed to load scrip master: {str(e)}')
            return False

        if isinstance(data, dict) and 'data' in data:
            data = data['data']

        if not isinstance(data, list):
            logger.error('Scrip master format invalid: expected list.')
            return False

        self.scrip_master = data
        self._build_scrip_index()
        if self._strike_cache_enabled and self._strike_cache_precompute:
            try:
                self._precompute_strike_cache()
            except Exception as exc:
                logger.warning('Strike cache precompute failed: %s', str(exc))
        return True

    def _build_scrip_index(self):
        """Build a quick lookup index for options."""
        self.scrip_index = {}
        for item in self.scrip_master:
            try:
                exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
                name = (item.get('name') or '').upper()
                symbol = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
                option_type = item.get('optiontype') or item.get('option_type')
                if not option_type and (symbol.endswith('CE') or symbol.endswith('PE')):
                    option_type = symbol[-2:]
                if not option_type:
                    continue
                option_type = option_type.upper()

                expiry = self._parse_expiry(item.get('expiry'))
                strike = self._normalize_strike(item.get('strike'))
                token = item.get('token') or item.get('symboltoken')
                if not (name and expiry and strike and token):
                    continue

                key = (exch, name, expiry, strike, option_type)
                self.scrip_index[key] = {
                    'token': str(token),
                    'symbol': item.get('symbol') or item.get('tradingsymbol') or '',
                    'exch_seg': exch,
                    'expiry': expiry,
                    'strike': strike,
                    'option_type': option_type,
                    'name': name
                }
            except Exception:
                continue

    def _parse_expiry(self, value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value.date()
        if hasattr(value, 'date'):
            return value.date()
        val = str(value).strip()
        for fmt in ('%Y-%m-%d', '%d%b%Y', '%d%b%y', '%d-%m-%Y'):
            try:
                return datetime.strptime(val, fmt).date()
            except Exception:
                continue
        return None

    def get_available_option_expiries(self, underlying, exchange='NFO'):
        """Return sorted unique option expiries available for an underlying."""
        if not self.load_scrip_master():
            return []

        underlying = (underlying or '').upper()
        exchange = (exchange or 'NFO').upper()
        expiries = set()

        for item in self.scrip_master:
            exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
            if exch != exchange:
                continue

            name = (item.get('name') or '').upper()
            symbol = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
            if name != underlying and not symbol.startswith(underlying):
                continue

            option_type = item.get('optiontype') or item.get('option_type')
            if not option_type and not (symbol.endswith('CE') or symbol.endswith('PE')):
                continue

            exp = self._parse_expiry(item.get('expiry'))
            if exp:
                expiries.add(exp)

        return sorted(expiries)

    def get_nearest_option_expiry(self, underlying, reference_date=None, exchange='NFO'):
        """
        Pick nearest available expiry from scrip master on/after reference_date.
        Falls back to latest known expiry if all are before reference_date.
        """
        expiries = self.get_available_option_expiries(underlying, exchange=exchange)
        if not expiries:
            return None

        if reference_date is None:
            ref_date = datetime.now().date()
        elif isinstance(reference_date, datetime):
            ref_date = reference_date.date()
        else:
            ref_date = reference_date

        future = [e for e in expiries if e >= ref_date]
        if future:
            return future[0]
        return expiries[-1]

    def get_available_option_underlyings(self, exchange='NFO', instrument_type=None):
        """
        Return sorted unique option underlyings available in scrip master.

        Args:
            exchange: Exchange segment (e.g. NFO)
            instrument_type: Optional instrument type filter
                (e.g. OPTSTK, OPTIDX). When provided, filter by this field.
        """
        if not self.load_scrip_master():
            return []

        exchange = (exchange or 'NFO').upper()
        instrument_type = (instrument_type or '').upper() or None
        names = set()
        for item in self.scrip_master:
            exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
            if exch != exchange:
                continue
            item_instrument_type = (
                item.get('instrumenttype')
                or item.get('instrument_type')
                or ''
            ).upper()
            if instrument_type and item_instrument_type != instrument_type:
                continue
            symbol = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
            if not instrument_type:
                option_type = item.get('optiontype') or item.get('option_type')
                if not option_type and not (symbol.endswith('CE') or symbol.endswith('PE')):
                    continue
            name = (item.get('name') or '').upper()
            if name:
                names.add(name)
        return sorted(names)

    def get_option_strikes_for_expiry(self, underlying, expiry, exchange='NFO'):
        """
        Return sorted unique strikes available for an underlying/expiry.
        """
        if not self.load_scrip_master():
            return []

        underlying = (underlying or '').upper()
        exchange = (exchange or 'NFO').upper()
        expiry_date = expiry if not isinstance(expiry, datetime) else expiry.date()
        cache_key = self._strike_cache_key(exchange, underlying, expiry_date)
        cached = self._get_strike_cache(cache_key)
        if cached is not None:
            return list(cached)
        strikes = set()

        for item in self.scrip_master:
            exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
            if exch != exchange:
                continue

            name = (item.get('name') or '').upper()
            symbol = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
            if name != underlying and not symbol.startswith(underlying):
                continue

            item_expiry = self._parse_expiry(item.get('expiry'))
            if not item_expiry or item_expiry != expiry_date:
                continue

            option_type = item.get('optiontype') or item.get('option_type')
            if not option_type and not (symbol.endswith('CE') or symbol.endswith('PE')):
                continue

            strike = self._normalize_strike(item.get('strike'))
            if strike is None:
                continue

            # Some masters store strikes scaled by 100.
            candidate = strike / 100.0 if strike > 100000 else strike
            strikes.add(round(float(candidate), 2))

        result = sorted(strikes)
        self._set_strike_cache(cache_key, result)
        return result

    def get_option_tokens_for_expiry(self, underlying, expiry, exchange='NFO'):
        """
        Return option instrument tokens for an underlying/expiry.
        """
        if not self.load_scrip_master():
            return []

        underlying = (underlying or '').upper()
        exchange = (exchange or 'NFO').upper()
        expiry_date = expiry if not isinstance(expiry, datetime) else expiry.date()
        tokens = []

        for item in self.scrip_master:
            exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
            if exch != exchange:
                continue

            name = (item.get('name') or '').upper()
            symbol = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
            if name != underlying and not symbol.startswith(underlying):
                continue

            item_expiry = self._parse_expiry(item.get('expiry'))
            if not item_expiry or item_expiry != expiry_date:
                continue

            option_type = item.get('optiontype') or item.get('option_type')
            if not option_type and not (symbol.endswith('CE') or symbol.endswith('PE')):
                continue

            token = item.get('token') or item.get('symboltoken')
            if token:
                tokens.append(str(token))

        return tokens

    def _normalize_strike(self, value):
        if value is None:
            return None
        try:
            strike = float(value)
            return round(strike, 2)
        except Exception:
            return None

    def _strike_matches(self, candidate, target):
        if candidate is None:
            return False
        if abs(candidate - target) < 0.01:
            return True
        # try scaled variants seen in some master files
        if abs((candidate / 100) - target) < 0.01:
            return True
        if abs((candidate * 100) - target) < 0.01:
            return True
        return False

    def lookup_instrument(self, symbol, exchange='NSE'):
        """Lookup instrument token for equity/index symbol."""
        if not self.load_scrip_master():
            return None
        symbol = symbol.upper()
        exchange = exchange.upper()
        for item in self.scrip_master:
            exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
            sym = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
            name = (item.get('name') or '').upper()
            token = item.get('token') or item.get('symboltoken')
            if exch == exchange and token and (sym == symbol or name == symbol):
                return {
                    'token': str(token),
                    'symbol': item.get('symbol') or item.get('tradingsymbol') or symbol,
                    'exch_seg': exch
                }
        return None

    def _strike_cache_key(self, exchange, underlying, expiry_date):
        exp = expiry_date.isoformat() if hasattr(expiry_date, 'isoformat') else str(expiry_date)
        return f'{exchange}:{underlying}:{exp}'

    def _load_strike_cache(self):
        if self._strike_cache_loaded or not self._strike_cache_enabled:
            return
        self._strike_cache_loaded = True
        path = self._strike_cache_path
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, 'r', encoding='utf-8') as handle:
                data = json.load(handle) or {}
            entries = data.get('entries') if isinstance(data, dict) else {}
            if isinstance(entries, dict):
                self._strike_cache = entries
        except Exception as exc:
            logger.warning('Failed to load strike cache: %s', str(exc))

    def _save_strike_cache(self):
        if not self._strike_cache_enabled:
            return
        path = self._strike_cache_path
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = {'entries': self._strike_cache}
            with open(path, 'w', encoding='utf-8') as handle:
                json.dump(payload, handle, ensure_ascii=True)
        except Exception as exc:
            logger.warning('Failed to save strike cache: %s', str(exc))

    def _get_strike_cache(self, key):
        if not self._strike_cache_enabled:
            return None
        self._load_strike_cache()
        with self._strike_cache_lock:
            entry = self._strike_cache.get(key)
        if not entry or not isinstance(entry, dict):
            return None
        ts = entry.get('ts')
        try:
            if ts is None:
                return None
            if (time.time() - float(ts)) > float(self._strike_cache_ttl_seconds):
                return None
        except Exception:
            return None
        strikes = entry.get('strikes')
        return strikes if isinstance(strikes, list) else None

    def _set_strike_cache(self, key, strikes):
        if not self._strike_cache_enabled:
            return
        now = time.time()
        with self._strike_cache_lock:
            self._strike_cache[key] = {'ts': now, 'strikes': list(strikes or [])}
            if len(self._strike_cache) > 50000:
                self._strike_cache.clear()
        self._save_strike_cache()

    def _precompute_strike_cache(self):
        if not self.scrip_master or not self._strike_cache_enabled:
            return
        exchange = self._strike_cache_exchange or 'NFO'
        cache = {}
        for item in self.scrip_master:
            exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
            if exch != exchange:
                continue
            option_type = item.get('optiontype') or item.get('option_type')
            symbol = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
            if not option_type and not (symbol.endswith('CE') or symbol.endswith('PE')):
                continue
            underlying = (item.get('name') or '').upper()
            if not underlying:
                continue
            expiry = self._parse_expiry(item.get('expiry'))
            if not expiry:
                continue
            strike = self._normalize_strike(item.get('strike'))
            if strike is None:
                continue
            candidate = strike / 100.0 if strike > 100000 else strike
            key = self._strike_cache_key(exchange, underlying, expiry)
            cache.setdefault(key, set()).add(round(float(candidate), 2))

        now = time.time()
        with self._strike_cache_lock:
            self._strike_cache = {
                key: {'ts': now, 'strikes': sorted(list(values))}
                for key, values in cache.items()
            }
        self._save_strike_cache()

    def lookup_option_instrument(self, underlying, expiry, strike, option_type, exchange='NFO'):
        """Lookup option instrument token using scrip master."""
        if not self.load_scrip_master():
            return None

        underlying = underlying.upper()
        option_type = option_type.upper()
        exchange = exchange.upper()
        expiry_date = expiry if not isinstance(expiry, datetime) else expiry.date()
        strike_val = float(strike)

        key = (exchange, underlying, expiry_date, round(strike_val, 2), option_type)
        if key in self.scrip_index:
            return self.scrip_index[key]

        for item in self.scrip_master:
            exch = (item.get('exch_seg') or item.get('exchange') or '').upper()
            name = (item.get('name') or '').upper()
            symbol = (item.get('symbol') or item.get('tradingsymbol') or '').upper()
            token = item.get('token') or item.get('symboltoken')
            if exch != exchange or not token:
                continue
            if name != underlying and not symbol.startswith(underlying):
                continue
            item_expiry = self._parse_expiry(item.get('expiry'))
            if not item_expiry or item_expiry != expiry_date:
                continue
            item_option_type = item.get('optiontype') or item.get('option_type')
            if not item_option_type and (symbol.endswith('CE') or symbol.endswith('PE')):
                item_option_type = symbol[-2:]
            if not item_option_type or item_option_type.upper() != option_type:
                continue
            item_strike = self._normalize_strike(item.get('strike'))
            if not self._strike_matches(item_strike, strike_val):
                continue

            return {
                'token': str(token),
                'symbol': item.get('symbol') or item.get('tradingsymbol') or symbol,
                'exch_seg': exch,
                'expiry': item_expiry,
                'strike': item_strike,
                'option_type': option_type,
                'name': name
            }

        return None
    
    def get_live_quote(self, symbol, exchange='NSE', symbol_token=None):
        """
        Get live quote for a symbol
        
        Args:
            symbol: Trading symbol (e.g., 'NIFTY', 'BANKNIFTY')
            exchange: Exchange (NSE, NFO, etc.)
        
        Returns:
            dict: Quote data or None
        """
        cache_key = f'{exchange}:{symbol}'
        
        # Check cache (5 second expiry)
        if cache_key in self.cache:
            if datetime.now() < self.cache_expiry[cache_key]:
                return self.cache[cache_key]
        
        try:
            if not self.smart_api:
                logger.error('SmartAPI client not initialized. Call authenticate() first.')
                return None

            token = symbol_token
            if not token:
                instrument = self.lookup_instrument(symbol, exchange=exchange)
                if not instrument:
                    logger.error(f'Symbol token not found for {exchange}:{symbol}')
                    return None
                token = instrument['token']
                symbol = instrument['symbol']

            response = self._with_retry(self.smart_api.ltpData, exchange, symbol, token)
            data = response.get('data') if isinstance(response, dict) else None
            if not data:
                return None

            quote = {
                'symbol': symbol,
                'ltp': float(data.get('ltp', 0)),
                'open': float(data.get('open', 0)),
                'high': float(data.get('high', 0)),
                'low': float(data.get('low', 0)),
                'close': float(data.get('close', 0)),
                'volume': int(float(data.get('volume', 0))) if data.get('volume') is not None else 0,
                'token': token,
                'exchange': exchange
            }

            self.cache[cache_key] = quote
            self.cache_expiry[cache_key] = datetime.now() + timedelta(seconds=5)
            return quote

        except Exception as e:
            logger.error(f'Error fetching live quote for {symbol}: {str(e)}')
            return None
    
    def get_historical_data(self, symbol, timeframe, from_date, to_date, exchange='NSE', symbol_token=None):
        """
        Get historical OHLCV data
        
        Args:
            symbol: Trading symbol
            timeframe: Candle interval (ONE_MINUTE, FIVE_MINUTE, etc.)
            from_date: Start date (datetime)
            to_date: End date (datetime)
            exchange: Exchange
        
        Returns:
            list: Historical candle data
        """
        try:
            if not self.smart_api:
                logger.error('SmartAPI client not initialized. Call authenticate() first.')
                return []

            token = symbol_token
            if not token:
                instrument = self.lookup_instrument(symbol, exchange=exchange)
                if not instrument:
                    logger.error(f'Symbol token not found for {exchange}:{symbol}')
                    return []
                token = instrument['token']
                symbol = instrument['symbol']

            interval = self._map_interval(timeframe)
            from_date = self._ensure_market_time(from_date)
            to_date = self._ensure_market_time(to_date)
            params = {
                'exchange': exchange,
                'symboltoken': token,
                'interval': interval,
                'fromdate': from_date.strftime('%Y-%m-%d %H:%M'),
                'todate': to_date.strftime('%Y-%m-%d %H:%M')
            }

            response = self._with_retry(self.smart_api.getCandleData, params)
            return response.get('data', []) if isinstance(response, dict) else []

        except Exception as e:
            if self._is_rate_limit_error(e):
                self._log_rate_limit_warning(
                    f'Historical API rate limit hit for {symbol}. Backing off and skipping this cycle.'
                )
                return []
            logger.error(f'Error fetching historical data for {symbol}: {str(e)}')
            return []

    def get_option_snapshot(self, underlying, expiry, strike, option_type, timeframe, exchange='NFO'):
        """
        Get live option snapshot including LTP and previous candle close.

        Returns:
            dict or None
        """
        instrument = self.lookup_option_instrument(
            underlying=underlying,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            exchange=exchange
        )
        if not instrument:
            logger.error(f'Option token not found for {underlying} {expiry} {strike} {option_type}')
            return None

        quote = self.get_live_quote(
            symbol=instrument['symbol'],
            exchange=exchange,
            symbol_token=instrument['token']
        )
        if not quote:
            return None

        previous_close = self.get_previous_close(
            symbol=instrument['symbol'],
            exchange=exchange,
            symbol_token=instrument['token'],
            timeframe=timeframe
        )

        return {
            'symbol': instrument['symbol'],
            'option_type': option_type,
            'strike_price': strike,
            'ltp': quote.get('ltp', 0),
            'previous_candle_close': previous_close or 0,
            'volume': quote.get('volume', 0),
            'open_interest': 0
        }

    def get_recent_option_candle_pair(self, underlying, expiry, strike, option_type, timeframe, exchange='NFO'):
        """
        Return previous and current closed candle values for live signal checks.

        This intentionally uses closed candles only (not current LTP) so live and
        backtest follow the same candle-close logic.
        """
        instrument = self.lookup_option_instrument(
            underlying=underlying,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            exchange=exchange
        )
        if not instrument:
            return None

        # Prefer streaming candle cache when enabled.
        try:
            from app.services.streaming_manager import get_streaming_candle_pair
            streaming = get_streaming_candle_pair(instrument.get('token'), self._interval_minutes(timeframe))
            if streaming:
                return {
                    'symbol': instrument['symbol'],
                    'option_type': option_type,
                    'strike_price': strike,
                    'previous_candle_close': streaming.get('previous_candle_close'),
                    'current_candle_open': None,
                    'current_candle_high': None,
                    'current_candle_low': None,
                    'current_candle_close': streaming.get('current_candle_close'),
                    'volume': 0,
                    'open_interest': 0,
                    'rsi': None,
                    'candle_time': streaming.get('candle_time')
                }
        except Exception:
            pass

        interval_minutes = self._interval_minutes(timeframe)
        now = self.market_now()
        latest_start, previous_start = self._live_candle_window(now, interval_minutes)
        cache_key = (str(instrument.get('token')), str(exchange or ''), int(interval_minutes), latest_start.isoformat())
        with self._candle_pair_lock:
            cached = self._candle_pair_cache.get(cache_key)
        if cached:
            return dict(cached)

        candles = self.get_historical_data(
            symbol=instrument['symbol'],
            timeframe=timeframe,
            from_date=previous_start,
            to_date=latest_start,
            exchange=exchange,
            symbol_token=instrument['token']
        )
        if not candles or len(candles) < 2:
            for multiplier in (2, 4):
                fallback_from = latest_start - timedelta(minutes=interval_minutes * multiplier)
                candles = self.get_historical_data(
                    symbol=instrument['symbol'],
                    timeframe=timeframe,
                    from_date=fallback_from,
                    to_date=latest_start,
                    exchange=exchange,
                    symbol_token=instrument['token']
                )
                if candles and len(candles) >= 2:
                    break

        if not candles or len(candles) < 2:
            return None

        prev = candles[-2]
        curr = candles[-1]
        try:
            prev_close = float(prev[4])
            curr_close = float(curr[4])
            curr_open = float(curr[1]) if len(curr) > 1 else None
            curr_high = float(curr[2]) if len(curr) > 2 else None
            curr_low = float(curr[3]) if len(curr) > 3 else None
            volume = int(float(curr[5])) if len(curr) > 5 else 0
        except Exception:
            return None

        prev_ts = self._parse_candle_time(prev[0]) if len(prev) > 0 else None
        ts = self._parse_candle_time(curr[0]) if len(curr) > 0 else None
        payload = {
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
        with self._candle_pair_lock:
            self._candle_pair_cache[cache_key] = dict(payload)
            if len(self._candle_pair_cache) > 5000:
                self._candle_pair_cache.clear()
        return payload

    def get_previous_close(self, symbol, exchange, symbol_token, timeframe):
        """Fetch previous candle close for the given symbol."""
        interval_minutes = self._interval_minutes(timeframe)
        to_date = self.market_now()
        from_date = to_date - timedelta(minutes=interval_minutes * 3)
        candles = self.get_historical_data(
            symbol=symbol,
            timeframe=timeframe,
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            symbol_token=symbol_token
        )
        if not candles or len(candles) < 2:
            return None
        # Candle format: [timestamp, open, high, low, close, volume]
        try:
            return float(candles[-2][4])
        except Exception:
            return None

    def _map_interval(self, timeframe):
        """Map timeframe to SmartAPI interval string."""
        if isinstance(timeframe, str) and timeframe.upper().endswith('_MINUTE'):
            return timeframe
        minutes = self._interval_minutes(timeframe)
        mapping = {
            1: 'ONE_MINUTE',
            3: 'THREE_MINUTE',
            5: 'FIVE_MINUTE',
            10: 'TEN_MINUTE',
            15: 'FIFTEEN_MINUTE',
            30: 'THIRTY_MINUTE',
            60: 'ONE_HOUR'
        }
        return mapping.get(minutes, 'FIVE_MINUTE')

    def _interval_minutes(self, timeframe):
        if isinstance(timeframe, int):
            return timeframe
        if isinstance(timeframe, str):
            cleaned = timeframe.lower().replace('min', '').replace('minutes', '')
            try:
                return int(cleaned)
            except Exception:
                return 5
        return 5

    def _live_candle_window(self, now, interval_minutes):
        now = self._ensure_market_time(now)
        interval = max(1, int(interval_minutes))
        bucket_minute = (now.minute // interval) * interval
        latest_start = now.replace(minute=bucket_minute, second=0, microsecond=0)
        previous_start = latest_start - timedelta(minutes=interval)
        return latest_start, previous_start

    def _parse_candle_time(self, value):
        """Parse SmartAPI candle timestamp text into a datetime."""
        if isinstance(value, datetime):
            return value
        try:
            text = str(value).strip().replace('Z', '+00:00')
            return datetime.fromisoformat(text)
        except Exception:
            pass
        for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(str(value), fmt)
            except Exception:
                continue
        return None

    def _calculate_rsi(self, candles, period=14):
        """
        Calculate RSI from candle list where close is index 4.
        Returns None when not enough data.
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
    
    def get_option_chain(self, symbol, expiry):
        """
        Get option chain for a symbol and expiry
        
        Args:
            symbol: Underlying symbol
            expiry: Expiry date
        
        Returns:
            dict: Option chain data
        """
        try:
            # TODO: Implement actual API call
            
            # Placeholder response
            return {
                'calls': [],
                'puts': []
            }
        
        except Exception as e:
            logger.error(f'Error fetching option chain for {symbol}: {str(e)}')
            return None

    def _format_greeks_expiry(self, expiry):
        """Convert input expiry to SmartAPI optionGreek format: DDMMMYYYY (e.g., 27FEB2025)."""
        parsed = self._parse_expiry(expiry)
        if isinstance(parsed, date):
            return parsed.strftime('%d%b%Y').upper()
        return None

    def get_option_greeks(self, symbol, expiry, exchange='NFO'):
        """
        Fetch option Greeks matrix for an underlying and expiry.

        Args:
            symbol: Underlying symbol (e.g., NIFTY, BANKNIFTY)
            expiry: Expiry as date/datetime or parseable string
            exchange: Reserved for compatibility (SmartAPI endpoint uses name + expirydate)

        Returns:
            dict|None: {
                'symbol': str,
                'expiry': str (DDMMMYYYY),
                'exchange': str,
                'rows': list[dict]
            }
        """
        try:
            if not self.smart_api:
                logger.error('SmartAPI client not initialized. Call authenticate() first.')
                return None

            name = (symbol or '').strip().upper()
            if not name:
                logger.error('Option Greeks fetch failed: symbol is required.')
                return None

            expiry_text = self._format_greeks_expiry(expiry)
            if not expiry_text:
                logger.error(f'Option Greeks fetch failed: invalid expiry "{expiry}".')
                return None

            params = {
                'name': name,
                'expirydate': expiry_text
            }
            response = self._with_retry(self.smart_api.optionGreek, params)
            if not isinstance(response, dict) or not response.get('status'):
                msg = self._extract_error_message(response) if isinstance(response, dict) else 'Unknown response'
                logger.error(f'Option Greeks fetch failed for {name} {expiry_text}: {msg}')
                return None

            rows = response.get('data')
            if not isinstance(rows, list):
                rows = []

            return {
                'symbol': name,
                'expiry': expiry_text,
                'exchange': (exchange or 'NFO').upper(),
                'rows': rows
            }
        except Exception as e:
            logger.error(f'Error fetching option Greeks for {symbol}: {str(e)}')
            return None
    
    def subscribe_live_feed(self, symbols, callback):
        """
        Subscribe to live data feed via WebSocket
        
        Args:
            symbols: List of symbols to subscribe
            callback: Callback function for data updates
        """
        try:
            # TODO: Implement WebSocket subscription
            pass
        
        except Exception as e:
            logger.error(f'Error subscribing to live feed: {str(e)}')
