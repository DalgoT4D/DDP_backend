"""
Process-local registry of pooled SQLAlchemy engines for client warehouses.

An Engine owns a connection pool and is meant to be long-lived and shared.
Building one per request -- which is what the chart / dashboard / KPI / filter
endpoints all used to do -- left a pool of open sockets on the client's warehouse
after every response, because nothing disposed it. Those engines end up in
reference cycles under load, so only a gen-2 GC reclaimed them: hence idle
connections climbing into the hundreds and draining in a lump much later.

So: one engine per distinct set of credentials, retired once that warehouse goes
quiet. Open connections are bounded four independent ways -- POOL_SIZE plus
POOL_MAX_OVERFLOW cap one warehouse in one process, POOL_RECYCLE_SECONDS caps the
age of a single socket, ENGINE_IDLE_TTL_SECONDS retires a whole idle pool, and
MAX_CACHED_ENGINES caps how many warehouses one process holds at once.

Peak connections to a warehouse are `n_processes * (POOL_SIZE + POOL_MAX_OVERFLOW)`.
The cache has to be process-local: a socket belongs to one process and can only
be counted by the others, not shared.
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

# Sockets kept open per warehouse per process once the pool is warm.
POOL_SIZE = int(os.getenv("WAREHOUSE_POOL_SIZE", "3"))
# Burst allowance above POOL_SIZE. These close on return instead of being pooled,
# so they add nothing to the idle footprint -- POOL_SIZE is the steady state and
# the sum is only the concurrent ceiling. Kept generous on purpose: before this
# registry every request built its own pool, so concurrency per warehouse was
# effectively unbounded. A dashboard fanning out to a dozen charts at once now
# shares one pool, and a ceiling as low as 5 would serialise it into waves.
POOL_MAX_OVERFLOW = int(os.getenv("WAREHOUSE_POOL_MAX_OVERFLOW", "7"))
# How long a caller waits for a free slot before SQLAlchemy raises TimeoutError.
POOL_TIMEOUT = int(os.getenv("WAREHOUSE_POOL_TIMEOUT", "30"))
# Max age of one pooled socket, enforced when it is next checked out. Keep below
# the shortest idle timeout on the path to the warehouse (pgbouncer, NAT, LB).
POOL_RECYCLE_SECONDS = int(os.getenv("WAREHOUSE_POOL_RECYCLE_SECONDS", "1800"))
# Retire a warehouse's whole pool once it has been untouched this long.
ENGINE_IDLE_TTL_SECONDS = int(os.getenv("WAREHOUSE_ENGINE_IDLE_TTL_SECONDS", "600"))
# How often the sweeper thread wakes up to retire idle pools.
SWEEP_INTERVAL_SECONDS = int(os.getenv("WAREHOUSE_SWEEP_INTERVAL_SECONDS", "60"))
# Cap on distinct warehouses cached per process.
MAX_CACHED_ENGINES = int(os.getenv("WAREHOUSE_MAX_CACHED_ENGINES", "20"))


@dataclass
class EngineEntry:
    """A cached engine plus what is needed to retire and observe it."""

    engine: Engine
    wtype: str
    last_used_at: float
    org_warehouse_id: int | None = None

    def in_use(self) -> bool:
        """
        Whether a connection from this pool is currently checked out.

        Guards retirement and eviction: last_used_at is stamped when the engine is
        handed out, so a query running longer than the idle TTL would otherwise
        look abandoned.
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

    pool_pre_ping is postgres-only, and deliberately so. On postgres it is one
    cheap round trip per checkout that turns a socket silently dropped by a
    pgbouncer or NAT into a transparent reconnect instead of a failed request --
    well worth it for pools that sit idle between dashboard loads.

    On BigQuery it would be actively harmful. sqlalchemy-bigquery does not
    override DefaultDialect.do_ping, so pre-ping falls through to executing
    `SELECT 1` -- which on BigQuery is a real query job, costing a job slot, API
    quota and a few hundred milliseconds on *every* connection checkout. BigQuery
    also has no persistent session to go stale, so there is nothing to ping for.
    """
    kwargs = {
        "pool_size": POOL_SIZE,
        "max_overflow": POOL_MAX_OVERFLOW,
        "pool_timeout": POOL_TIMEOUT,
        "pool_recycle": POOL_RECYCLE_SECONDS,
    }
    if wtype == "postgres":
        kwargs["pool_pre_ping"] = True
    return kwargs


def fingerprint(wtype: str, creds: dict) -> str:
    """
    Cache key for a set of warehouse credentials.

    Hashes the full credentials, not a chosen subset, because under-specifying
    this key is a tenant data leak rather than a performance bug: trial
    warehouses share one RDS host and differ only by `database` (see
    core/trial/clone_service.py), so a key built from, say, host and password
    would collide across orgs and hand one org an engine connected to another
    org's database. Over-specifying merely costs a redundant engine.

    Keying on credentials rather than org id also covers the call sites that
    build a client from raw creds with no OrgWarehouse in scope (warehouse_api,
    celeryworkers.tasks, dbt_service, visualizationfunctions,
    datainsights.generate_result), and means rotated credentials land on a new
    key -- so a pool authenticated with an old password can never serve a request
    carrying the new one.

    Call before the creds reach a client, which normalises the dict (sslmode
    aliasing) on its way to connect_args.
    """
    digest = hashlib.sha256(
        json.dumps(creds, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"{wtype}:{digest}"


def _dispose(entries: list[EngineEntry]) -> None:
    """
    Close the pools of engines already removed from the registry.

    Always called with the lock released: dispose() closes sockets, and holding
    the lock through that would stall every other thread waiting for an engine.
    Connections still checked out are unaffected and close when returned.
    """
    for entry in entries:
        try:
            entry.engine.dispose()
        except Exception:  # skipcq: PYL-W0703
            # A pool we are discarding failing to close is not worth failing a
            # request over -- the sockets go when the process does.
            logger.warning(
                "failed to dispose warehouse engine",
                extra={"wtype": entry.wtype, "org_warehouse_id": entry.org_warehouse_id},
            )


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
                # Long query on an otherwise quiet warehouse: count the checkout
                # as activity so it is reconsidered next sweep.
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
            # The sweeper must outlive any single failure, or the process quietly
            # stops retiring pools for the rest of its life.
            logger.exception("warehouse engine sweep failed")


def _ensure_sweeper() -> None:
    """
    Start this process's sweeper thread, once.

    Lazy rather than started at import because gunicorn and celery fork their
    workers, and a thread created before the fork does not survive into the
    child. Starting it on first use guarantees it belongs to the process that
    owns the engines.
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
    Give a freshly forked child its own empty registry.

    gunicorn and celery both fork their workers, so a child inherits the parent's
    module state: the _engines dict, and a _sweeper_started flag claiming a thread
    is running when the parent's thread did not survive the fork. Nothing builds
    an engine before the fork today, so this is insurance -- but the alternative
    to insurance here is two processes issuing queries down one socket.

    Inherited pools are dropped with dispose(close=False), which abandons the
    connections instead of closing them: the child's sockets are dups of the
    parent's, so closing them would tear down sessions the parent is still using.
    """
    global _lock  # pylint: disable=global-statement # skipcq: PYL-W0603
    # A lock held by some thread at fork time stays locked forever in the child,
    # where that thread does not exist. The child is single-threaded here, so
    # swapping in a fresh lock is safe.
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


def get_or_create_engine(
    cache_key: str,
    create,
    wtype: str,
    org_warehouse_id: int | None = None,
) -> Engine:
    """
    Return the cached engine for `cache_key`, building it via `create` on a miss.

    Retiring idle pools is left entirely to the sweeper thread; doing it here too
    would only shave up to SWEEP_INTERVAL_SECONDS off a pool's life.

    `create` runs while the lock is held, which is fine because create_engine()
    is lazy -- it opens no connection -- so the critical section stays in the
    microseconds.
    """
    _ensure_sweeper()

    now = time.time()
    evicted: list[EngineEntry] = []

    with _lock:
        entry = _engines.get(cache_key)
        if entry is not None:
            entry.last_used_at = now
            return entry.engine

        while len(_engines) >= MAX_CACHED_ENGINES:
            idle = [(key, item) for key, item in _engines.items() if not item.in_use()]
            if not idle:
                # Every cached engine is mid-query. Going briefly over the cap
                # beats blocking the request or stealing a pool in use; the next
                # sweep brings us back under.
                logger.warning(
                    "warehouse engine cache over capacity, all engines busy",
                    extra={"cached_engines": len(_engines)},
                )
                break
            coldest, _ = min(idle, key=lambda item: item[1].last_used_at)
            evicted.append(_engines.pop(coldest))

        engine = create()
        _engines[cache_key] = EngineEntry(
            engine=engine,
            wtype=wtype,
            last_used_at=now,
            org_warehouse_id=org_warehouse_id,
        )
        logger.info(
            "created warehouse engine",
            extra={
                "wtype": wtype,
                "org_warehouse_id": org_warehouse_id,
                "cached_engines": len(_engines),
            },
        )

    _dispose(evicted)
    return engine


def invalidate_for_warehouse(org_warehouse_id: int) -> int:
    """
    Drop every cached engine belonging to a warehouse. Returns how many.

    Rotated credentials already land on a new fingerprint, so this is
    belt-and-braces: it hands a superseded pool back now instead of leaving it to
    the idle TTL, and it is the right hook for warehouse deletion.
    """
    with _lock:
        keys = [
            key for key, entry in _engines.items() if entry.org_warehouse_id == org_warehouse_id
        ]
        detached = [_engines.pop(key) for key in keys]

    _dispose(detached)
    if detached:
        logger.info(
            "invalidated warehouse engines",
            extra={"org_warehouse_id": org_warehouse_id, "count": len(detached)},
        )
    return len(detached)


def invalidate_all() -> int:
    """Drop every cached engine, closing its pool. Returns how many."""
    with _lock:
        detached = list(_engines.values())
        _engines.clear()

    _dispose(detached)
    return len(detached)


def registry_stats() -> dict:
    """
    Snapshot of the registry, logged on each sweep that retires something.

    checkedout/checkedin come straight off each pool, making this the cheapest
    way to see how many warehouse sockets a process is actually holding.
    """
    now = time.time()
    with _lock:
        entries = [
            {
                "wtype": entry.wtype,
                "org_warehouse_id": entry.org_warehouse_id,
                "idle_seconds": round(now - entry.last_used_at, 1),
                "checkedout": entry.engine.pool.checkedout(),
                "checkedin": entry.engine.pool.checkedin(),
            }
            for entry in _engines.values()
        ]

    return {
        "cached_engines": len(entries),
        "max_cached_engines": MAX_CACHED_ENGINES,
        "per_warehouse_ceiling": POOL_SIZE + POOL_MAX_OVERFLOW,
        "idle_ttl_seconds": ENGINE_IDLE_TTL_SECONDS,
        "engines": entries,
    }
