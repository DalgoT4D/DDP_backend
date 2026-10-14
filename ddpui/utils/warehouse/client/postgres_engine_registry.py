"""
Process-local registry of pooled SQLAlchemy engines, one per set of postgres
credentials. Endpoints used to build an Engine per request, each with its own pool
that nothing disposed -- hence idle connections climbing, then draining in a lump.

Process-local because a socket belongs to one process; peak connections to a
warehouse are `n_processes * (POOL_SIZE + POOL_MAX_OVERFLOW)`.

Postgres only: BigQuery connections are REST clients, so there is no connection
limit to protect and nothing to retire.

TODO: add an LRU cap if cached engines ever approach the process fd limit. Idle
engines are disposed (sockets closed) but their entries are never removed.
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
POOL_SIZE = int(os.getenv("WAREHOUSE_POOL_SIZE", "5"))
# Burst allowance above POOL_SIZE; these close on return, so they cost nothing when idle.
POOL_MAX_OVERFLOW = 7
# Wait for a free slot before TimeoutError -- under the ~60s gateway timeout, over a normal burst.
POOL_TIMEOUT = 30
# Retire a warehouse's whole pool once it has been untouched this long.
ENGINE_IDLE_TTL_SECONDS = 600
# Sweeper wake-up; coarser than the TTL, so a pool lives at most TTL + this.
SWEEP_INTERVAL_SECONDS = 300


@dataclass
class CachedEngine:
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
        except Exception as err:  # skipcq: PYL-W0703
            # Never let pool introspection stop a sweep; assume idle.
            logger.warning(f"failed to read checked-out connections: {err}")
            return False

    def has_open_connections(self) -> bool:
        """
        Whether the pool holds idle sockets. A disposed pool has none, so the sweeper
        skips it instead of disposing it again every sweep.
        """
        try:
            return self.engine.pool.checkedin() > 0
        except Exception as err:  # skipcq: PYL-W0703
            # Can't tell; dispose to be safe -- it is harmless on an empty pool.
            logger.warning(f"failed to read checked-in connections: {err}")
            return True


# Entries are never removed, only disposed, so this dict only ever grows.
_engines: dict[str, CachedEngine] = {}
# One lock per cache_key, so one org never builds two engines and orgs never wait on each other.
_engine_locks: dict[str, threading.Lock] = {}
# Guards creating a per-key lock and starting the sweeper; never held while building an engine.
_all_lock = threading.Lock()
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


def _sweep() -> int:
    """
    Dispose every engine past the idle TTL. Returns how many were disposed. The engine
    stays in the registry: dispose() swaps in a fresh empty pool, so the same engine
    opens new connections on its next checkout.
    """
    now = time.time()
    disposed_count = 0

    # list(): new orgs may still add keys while we iterate
    for cache_key, cached in list(_engines.items()):
        with _all_lock:
            if cache_key not in _engine_locks:
                _engine_locks[cache_key] = threading.Lock()

        engine_lock = _engine_locks[cache_key]

        # same lock as get_or_create_engine: a request either stamps last_used_at
        # before this check, or gets the engine after dispose() swapped in a fresh pool
        with engine_lock:
            if now - cached.last_used_at <= ENGINE_IDLE_TTL_SECONDS:
                continue
            if cached.in_use():
                # Long query on a quiet warehouse: count the checkout as activity.
                cached.last_used_at = now
                continue
            if not cached.has_open_connections():
                continue
            try:
                cached.engine.dispose()
            except Exception as err:  # skipcq: PYL-W0703
                # Discarding a pool that fails to close must not fail the sweep.
                logger.warning(f"failed to dispose warehouse engine: {err}")
            disposed_count += 1

    if disposed_count:
        logger.info(
            "retired idle warehouse engines",
            extra={"count": disposed_count, **registry_stats()},
        )
    return disposed_count


def _sweep_loop() -> None:
    """Body of the daemon sweeper thread."""
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)
        try:
            _sweep()
        except Exception as err:  # skipcq: PYL-W0703
            # The sweeper must outlive any single failure, or this process stops
            # retiring pools for the rest of its life.
            logger.exception(f"warehouse engine sweep failed: {err}")


def _ensure_sweeper() -> None:
    """
    Start this process's sweeper thread, once. Lazy rather than at import: gunicorn
    and celery fork their workers, and a thread created before the fork does not
    survive into the child.
    """
    if _sweeper_started.is_set():
        return
    with _all_lock:  # thread lock
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


def get_or_create_engine(cache_key: str, create_engine) -> Engine:
    """
    Return the cached engine for `cache_key`, building it via `create_engine` on a miss.
    Only this key's lock is held while building, so other orgs never wait.
    """
    _ensure_sweeper()

    now = time.time()

    # get or create this key's lock; _all_lock makes the check-and-set atomic
    with _all_lock:
        if cache_key not in _engine_locks:
            _engine_locks[cache_key] = threading.Lock()

    engine_lock = _engine_locks[cache_key]

    with engine_lock:
        cached = _engines.get(cache_key)
        if cached is not None:
            cached.last_used_at = now
            return cached.engine

        engine = create_engine()
        _engines[cache_key] = CachedEngine(engine=engine, last_used_at=now)
        logger.info("created warehouse engine", extra={"cached_engines": len(_engines)})

    return engine


def registry_stats() -> dict:
    """
    Snapshot of the registry, logged on each sweep that retires something. checkedout /
    checkedin come straight off each pool.
    """
    now = time.time()
    engine_stats = [
        {
            "idle_seconds": round(now - cached.last_used_at, 1),
            "checkedout": cached.engine.pool.checkedout(),
            "checkedin": cached.engine.pool.checkedin(),
        }
        for cached in list(_engines.values())
    ]

    return {
        "cached_engines": len(engine_stats),
        "per_warehouse_ceiling": POOL_SIZE + POOL_MAX_OVERFLOW,
        "idle_ttl_seconds": ENGINE_IDLE_TTL_SECONDS,
        "engines": engine_stats,
    }
