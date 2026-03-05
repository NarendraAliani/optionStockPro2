"""
RQ-backed queue manager for scan tasks.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

try:
    import redis
    from rq import Queue
except Exception:
    redis = None
    Queue = None

logger = logging.getLogger(__name__)


def get_queue() -> Optional["Queue"]:
    if not redis or not Queue:
        return None
    url = str(os.getenv('REDIS_URL', '') or '').strip()
    if not url:
        return None
    try:
        conn = redis.Redis.from_url(url)
        conn.ping()
    except Exception as exc:
        logger.warning('Redis unavailable: %s', str(exc))
        return None
    return Queue('scan', connection=conn)


def get_redis_conn():
    if not redis:
        return None
    url = str(os.getenv('REDIS_URL', '') or '').strip()
    if not url:
        return None
    try:
        conn = redis.Redis.from_url(url)
        conn.ping()
        return conn
    except Exception as exc:
        logger.warning('Redis unavailable: %s', str(exc))
        return None


def set_live_stop_flag(user_id: int) -> None:
    conn = get_redis_conn()
    if not conn:
        return
    try:
        conn.setex(f'live_stop:{int(user_id)}', 120, '1')
    except Exception:
        pass


def clear_live_stop_flag(user_id: int) -> None:
    conn = get_redis_conn()
    if not conn:
        return
    try:
        conn.delete(f'live_stop:{int(user_id)}')
    except Exception:
        pass


def is_live_stop_requested(user_id: int) -> bool:
    conn = get_redis_conn()
    if not conn:
        return False
    try:
        return bool(conn.get(f'live_stop:{int(user_id)}'))
    except Exception:
        return False
