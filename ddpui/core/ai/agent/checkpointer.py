"""Shared AsyncPostgresSaver for conversation memory.

One psycopg3 AsyncConnectionPool per process, created lazily inside the running
event loop and shared by all consumers — never per-message (research §3.3). It is
fully separate from Django's psycopg2 connections. The checkpoint tables are
created once per environment by `manage.py chat_with_data_setup`.
"""

import asyncio
from urllib.parse import quote

from django.conf import settings
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

# Checkpointer pool is small: one connection is held only during state reads/writes,
# not for the duration of an agent turn
POOL_MIN_SIZE = 1
POOL_MAX_SIZE = 4

_lock = asyncio.Lock()
_pool: AsyncConnectionPool | None = None
_saver: AsyncPostgresSaver | None = None


def build_conninfo(db: dict) -> str:
    """postgresql:// conninfo from a Django DATABASES entry."""
    user = quote(db["USER"], safe="")
    password = quote(db["PASSWORD"], safe="")
    return f"postgresql://{user}:{password}@{db['HOST']}:{db['PORT']}/{db['NAME']}"


def default_conninfo() -> str:
    return build_conninfo(settings.DATABASES["default"])


async def get_checkpointer() -> AsyncPostgresSaver:
    """The process-wide saver, creating the pool on first use (loop-bound —
    always call from the ASGI event loop, never from asyncio.run() elsewhere)."""
    global _pool, _saver  # pylint: disable=global-statement
    async with _lock:
        if _saver is None:
            _pool = AsyncConnectionPool(
                conninfo=default_conninfo(),
                min_size=POOL_MIN_SIZE,
                max_size=POOL_MAX_SIZE,
                open=False,
                kwargs={"autocommit": True, "row_factory": dict_row},
            )
            await _pool.open()
            _saver = AsyncPostgresSaver(_pool)
        return _saver


async def close_checkpointer() -> None:
    """Close the pool (tests / graceful shutdown)."""
    global _pool, _saver  # pylint: disable=global-statement
    async with _lock:
        if _pool is not None:
            await _pool.close()
        _pool = None
        _saver = None


async def setup_tables() -> None:
    """Create the checkpointer's tables. Standalone connection so it can run from
    a management command's own event loop."""
    async with AsyncPostgresSaver.from_conn_string(default_conninfo()) as saver:
        await saver.setup()
