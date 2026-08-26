"""
Process-local registry of pooled SQLAlchemy engines, one per set of postgres
credentials. Endpoints used to build an Engine per request, each with its own pool
that nothing disposed -- hence idle connections climbing, then draining in a lump.

Process-local because a socket belongs to one process; peak connections to a
warehouse are `n_processes * (POOL_SIZE + POOL_MAX_OVERFLOW)`.

Postgres only: BigQuery connections are REST clients, so there is no connection
limit to protect and nothing to retire.

TODO: add an LRU cap if cached engines ever approach the process fd limit; the idle
TTL is currently the only bound on how many warehouses one process caches.
"""

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.warehouse.engine_registry")

# Ceiling on sockets kept per warehouse per process; a pool only grows to peak concurrency.
POOL_SIZE = int(os.getenv("WAREHOUSE_POOL_SIZE", "8"))
# Burst allowance above POOL_SIZE; these close on return, so they cost nothing when idle.
POOL_MAX_OVERFLOW = 7
# Wait for a free slot before TimeoutError -- under the ~60s gateway timeout, over a normal burst.
POOL_TIMEOUT = 30
# Retire a warehouse's whole pool once it has been untouched this long.
ENGINE_IDLE_TTL_SECONDS = 600
# Sweeper wake-up; coarser than the TTL, so a pool lives at most TTL + this.
SWEEP_INTERVAL_SECONDS = 300


@dataclass
class EngineEntry:
    """A cached engine plus what is needed to retire and observe it."""

    engine: Engine
    last_used_at: float

    def in_use(self) -> bool:
        """
        Whether a connection is checked out. last_used_at is stamped at hand-out, so
        without this a query outliving the idle TTL would have its pool retired.
        """
        try:
            return self.engine.pool.checkedout() > 0
        except Exception:  # skipcq: PYL-W0703
            # Never let pool introspection stop a sweep; assume idle.
            return False


_engines: dict[str, EngineEntry] = {}
_lock = threading.RLock()
# Set once this process's sweeper thread is running.
_sweeper_started = threading.Event()


def pool_kwargs() -> dict:
    """
    Pool settings a warehouse engine must be built with. pool_pre_ping costs ~1ms and
    turns a server-side disconnect into a retry rather than a failed request.
    """
    return {
        "pool_size": POOL_SIZE,
        "max_overflow": POOL_MAX_OVERFLOW,
        "pool_timeout": POOL_TIMEOUT,
        "pool_pre_ping": True,
    }


def fingerprint(wtype: str, creds: dict) -> str:
    """
    Cache key for a set of warehouse credentials. Hashes the full creds, not a subset:
    trial warehouses differ only by `database`, so a narrower key would hand one org
    an engine pointed at another's. Keying on creds also puts rotated ones on a new
    key. Call before the creds reach a client, which normalises sslmode.
    """
    digest = hashlib.sha256(
        json.dumps(creds, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"{wtype}:{digest}"


def _dispose(entries: list[EngineEntry]) -> None:
    """
    Close the pools of engines already removed from the registry. Called with the lock
    released: dispose() closes sockets, and holding the lock would stall every thread
    waiting for an engine. Checked-out connections close when returned.
    """
    for entry in entries:
        try:
            entry.engine.dispose()
        except Exception:  # skipcq: PYL-W0703
            # Discarding a pool that fails to close must not fail a request.
            logger.warning("failed to dispose warehouse engine")


def _sweep() -> int:
    """Retire every engine past the idle TTL. Returns how many were retired."""
    now = time.time()
    detached = []

    with _lock:
        for key in list(_engines):
            entry = _engines[key]
            if now - entry.last_used_at <= ENGINE_IDLE_TTL_SECONDS:
                continue
            if entry.in_use():
                # Long query on a quiet warehouse: count the checkout as activity.
                entry.last_used_at = now
                continue
            detached.append(_engines.pop(key))

    _dispose(detached)
    if detached:
        logger.info(
            "retired idle warehouse engines",
            extra={"count": len(detached), **registry_stats()},
        )
    return len(detached)


def _sweep_loop() -> None:
    """Body of the daemon sweeper thread."""
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)
        try:
            _sweep()
        except Exception:  # skipcq: PYL-W0703
            # The sweeper must outlive any single failure, or this process stops
            # retiring pools for the rest of its life.
            logger.exception("warehouse engine sweep failed")


def _ensure_sweeper() -> None:
    """
    Start this process's sweeper thread, once. Lazy rather than at import: gunicorn
    and celery fork their workers, and a thread created before the fork does not
    survive into the child.
    """
    if _sweeper_started.is_set():
        return
    with _lock:  # thread lock
        if _sweeper_started.is_set():
            return
        threading.Thread(target=_sweep_loop, name="warehouse-engine-sweeper", daemon=True).start()
        _sweeper_started.set()
        logger.info(
            "started warehouse engine sweeper",
            extra={
                "interval_seconds": SWEEP_INTERVAL_SECONDS,
                "idle_ttl_seconds": ENGINE_IDLE_TTL_SECONDS,
            },
        )


def get_or_create_engine(cache_key: str, create) -> Engine:
    """
    Return the cached engine for `cache_key`, building it via `create` on a miss.
    `create` runs under the lock: create_engine() opens no connection, so it is short.
    """
    _ensure_sweeper()

    now = time.time()

    with _lock:
        entry = _engines.get(cache_key)
        if entry is not None:
            entry.last_used_at = now
            return entry.engine

        engine = create()
        _engines[cache_key] = EngineEntry(engine=engine, last_used_at=now)
        logger.info("created warehouse engine", extra={"cached_engines": len(_engines)})

    return engine


def registry_stats() -> dict:
    """
    Snapshot of the registry, logged on each sweep that retires something. checkedout /
    checkedin come straight off each pool.
    """
    now = time.time()
    with _lock:
        entries = [
            {
                "idle_seconds": round(now - entry.last_used_at, 1),
                "checkedout": entry.engine.pool.checkedout(),
                "checkedin": entry.engine.pool.checkedin(),
            }
            for entry in _engines.values()
        ]

    return {
        "cached_engines": len(entries),
        "per_warehouse_ceiling": POOL_SIZE + POOL_MAX_OVERFLOW,
        "idle_ttl_seconds": ENGINE_IDLE_TTL_SECONDS,
        "engines": entries,
    }
