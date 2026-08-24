"""
Process-local registry of pooled SQLAlchemy engines for client warehouses.

Chart / dashboard / KPI / filter endpoints used to build an Engine per request,
each with its own pool that nothing disposed. Reference cycles meant only a gen-2
GC reclaimed them -- hence idle connections climbing, then draining in a lump.

So: one engine per distinct set of credentials, retired once that warehouse goes
quiet. POOL_SIZE + POOL_MAX_OVERFLOW cap one warehouse in one process, and
ENGINE_IDLE_TTL_SECONDS retires a whole idle pool; a dropped socket is caught by
pool_pre_ping, not by age.

Nothing caps how many distinct warehouses one process caches -- the idle TTL is the
only bound. TODO: add an LRU cap if cached engines ever approach the process fd limit.

Peak connections to a warehouse are `n_processes * (POOL_SIZE + POOL_MAX_OVERFLOW)`.
The cache must be process-local: a socket belongs to one process, and cannot be
shared with the others.
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
class EngineEntry:
    """A cached engine plus what is needed to retire and observe it."""

    engine: Engine
    wtype: str
    last_used_at: float

    def in_use(self) -> bool:
        """
        Whether a connection from this pool is currently checked out. last_used_at is
        stamped at hand-out, so without this a query running longer than the idle TTL
        would look abandoned and have its pool retired underneath it.
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


def pool_kwargs(wtype: str) -> dict:
    """
    Pool settings a warehouse engine must be built with.

    pool_pre_ping is postgres-only. Both dialects ping with `SELECT 1`; only the cost
    differs -- ~1ms down an open socket on postgres, versus a whole REST job on
    BigQuery, on every checkout, to test a session BigQuery does not even have.
    """
    kwargs = {
        "pool_size": POOL_SIZE,
        "max_overflow": POOL_MAX_OVERFLOW,
        "pool_timeout": POOL_TIMEOUT,
    }
    if wtype == "postgres":
        kwargs["pool_pre_ping"] = True
    return kwargs


def fingerprint(wtype: str, creds: dict) -> str:
    """
    Cache key for a set of warehouse credentials. Hashes the full creds, not a subset:
    trial warehouses share one RDS host and differ only by `database`, so a narrower
    key would hand one org an engine pointed at another's database.

    Keying on creds rather than org id also covers callers holding raw creds with no
    OrgWarehouse in scope, and puts rotated credentials on a new key. Call before the
    creds reach a client, which normalises the dict (sslmode aliasing).
    """
    digest = hashlib.sha256(
        json.dumps(creds, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"{wtype}:{digest}"


def _dispose(entries: list[EngineEntry]) -> None:
    """
    Close the pools of engines already removed from the registry. Always called with
    the lock released: dispose() closes sockets, and holding the lock through that
    would stall every thread waiting for an engine. Checked-out connections are
    unaffected and close when returned.
    """
    for entry in entries:
        try:
            entry.engine.dispose()
        except Exception:  # skipcq: PYL-W0703
            # Discarding a pool that fails to close must not fail a request.
            logger.warning("failed to dispose warehouse engine", extra={"wtype": entry.wtype})


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
            # The sweeper must outlive any single failure, or the process silently
            # stops retiring pools for the rest of its life.
            logger.exception("warehouse engine sweep failed")


def _ensure_sweeper() -> None:
    """
    Start this process's sweeper thread, once. Lazy rather than at import because
    gunicorn and celery fork their workers, and a thread created before the fork does
    not survive into the child -- first use guarantees it belongs to the right process.
    """
    if _sweeper_started.is_set():
        return
    with _lock:
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


def _reset_after_fork() -> None:
    """
    Give a freshly forked child its own empty registry: it inherits the parent's
    _engines and a _sweeper_started flag claiming a thread that did not survive the
    fork. Insurance today, but the alternative is two processes on one socket.

    dispose(close=False) abandons inherited pools rather than closing them -- the
    child's sockets are dups, so closing kills sessions the parent is still using.
    """
    global _lock  # pylint: disable=global-statement # skipcq: PYL-W0603
    # A lock held at fork time stays locked forever in the child, where its owning
    # thread does not exist. The child is single-threaded here, so a fresh one is safe.
    _lock = threading.RLock()

    for entry in _engines.values():
        try:
            entry.engine.dispose(close=False)
        except Exception:  # skipcq: PYL-W0703
            pass
    _engines.clear()
    # the parent's sweeper thread did not come with us; let the child start one
    _sweeper_started.clear()


os.register_at_fork(after_in_child=_reset_after_fork)


def get_or_create_engine(cache_key: str, create, wtype: str) -> Engine:
    """
    Return the cached engine for `cache_key`, building it via `create` on a miss.
    Retiring idle pools is left to the sweeper. `create` runs under the lock, which is
    fine: create_engine() is lazy and opens no connection, so the section stays short.
    """
    _ensure_sweeper()

    now = time.time()

    with _lock:
        entry = _engines.get(cache_key)
        if entry is not None:
            entry.last_used_at = now
            return entry.engine

        engine = create()
        _engines[cache_key] = EngineEntry(engine=engine, wtype=wtype, last_used_at=now)
        logger.info(
            "created warehouse engine",
            extra={"wtype": wtype, "cached_engines": len(_engines)},
        )

    return engine


def invalidate_all() -> int:
    """Drop every cached engine, closing its pool. Returns how many."""
    with _lock:
        detached = list(_engines.values())
        _engines.clear()

    _dispose(detached)
    return len(detached)


def registry_stats() -> dict:
    """
    Snapshot of the registry, logged on each sweep that retires something. checkedout /
    checkedin come straight off each pool -- the cheapest way to see how many warehouse
    sockets a process is actually holding.
    """
    now = time.time()
    with _lock:
        entries = [
            {
                "wtype": entry.wtype,
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
