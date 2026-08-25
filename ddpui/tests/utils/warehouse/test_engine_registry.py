import os
import threading

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.utils.warehouse.client import engine_registry


class FakePool:
    """Stands in for a SQLAlchemy pool; only checkedout() is read by the registry."""

    def __init__(self, checkedout=0):
        self._checkedout = checkedout

    def checkedout(self):
        return self._checkedout

    def checkedin(self):
        return 0


class FakeEngine:
    """Records whether the registry disposed it."""

    def __init__(self, label="engine", checkedout=0):
        self.label = label
        self.pool = FakePool(checkedout)
        self.disposed = False

    def dispose(self):
        self.disposed = True


@pytest.fixture(autouse=True)
def clean_registry():
    """The registry is module-level state; every test starts and ends empty."""
    engine_registry._engines.clear()
    yield
    engine_registry._engines.clear()


PG_CREDS = {
    "host": "trials-rds.example.com",
    "port": 5432,
    "database": "trial_abc",
    "username": "dalgo",
    "password": "s3cret",
}


def make(label="engine", checkedout=0):
    """A create-fn for get_or_create_engine that hands back a fresh FakeEngine."""
    return lambda: FakeEngine(label, checkedout)


def test_same_credentials_reuse_one_engine():
    """The whole point: repeated requests for one warehouse share a single pool."""
    key = engine_registry.fingerprint("postgres", PG_CREDS)

    first = engine_registry.get_or_create_engine(key, make("first"), wtype="postgres")
    second = engine_registry.get_or_create_engine(key, make("second"), wtype="postgres")

    assert first is second
    assert first.label == "first"  # the second create-fn was never called
    assert len(engine_registry._engines) == 1


def test_warehouses_differing_only_by_database_get_separate_engines():
    """
    The tenant-isolation guard. Trial warehouses share one RDS host and differ
    only by `database`, so a key built from a subset of fields would collide and
    serve one org an engine pointed at another org's data.
    """
    org_a = dict(PG_CREDS, database="trial_abc")
    org_b = dict(PG_CREDS, database="trial_xyz")

    engine_a = engine_registry.get_or_create_engine(
        engine_registry.fingerprint("postgres", org_a), make("a"), wtype="postgres"
    )
    engine_b = engine_registry.get_or_create_engine(
        engine_registry.fingerprint("postgres", org_b), make("b"), wtype="postgres"
    )

    assert engine_a is not engine_b
    assert len(engine_registry._engines) == 2


def test_rotated_password_gets_a_fresh_engine():
    """A pool authenticated with the old password must never serve new creds."""
    old = dict(PG_CREDS, password="old-password")
    new = dict(PG_CREDS, password="new-password")

    engine_old = engine_registry.get_or_create_engine(
        engine_registry.fingerprint("postgres", old), make("old"), wtype="postgres"
    )
    engine_new = engine_registry.get_or_create_engine(
        engine_registry.fingerprint("postgres", new), make("new"), wtype="postgres"
    )

    assert engine_old is not engine_new
    assert engine_new.label == "new"


def test_fingerprint_is_order_independent_but_type_scoped():
    """Dict ordering must not split the cache; warehouse type must not merge it."""
    same = engine_registry.fingerprint("postgres", {"host": "h", "database": "d", "password": "p"})
    reordered = engine_registry.fingerprint(
        "postgres", {"password": "p", "database": "d", "host": "h"}
    )
    other_wtype = engine_registry.fingerprint(
        "bigquery", {"host": "h", "database": "d", "password": "p"}
    )

    assert same == reordered
    assert same != other_wtype


def test_fingerprint_does_not_leak_the_password():
    """The key lands in a long-lived global we enumerate for stats and logs."""
    key = engine_registry.fingerprint("postgres", PG_CREDS)

    assert "s3cret" not in key
    assert "dalgo" not in key


def test_pool_kwargs_bound_the_connection_ceiling():
    """max_overflow especially: SQLAlchemy defaults it to 10, we want it small."""
    kwargs = engine_registry.pool_kwargs()

    assert kwargs["pool_size"] == engine_registry.POOL_SIZE
    assert kwargs["max_overflow"] == engine_registry.POOL_MAX_OVERFLOW
    assert kwargs["pool_timeout"] == engine_registry.POOL_TIMEOUT


def test_pre_ping_is_enabled():
    """
    A pooled connection outlives the server's willingness to keep it: without
    pre-ping, a backend killed by an RDS failover or an idle timeout surfaces as a
    failed request on the next checkout instead of a transparent reconnect.
    """
    assert engine_registry.pool_kwargs()["pool_pre_ping"] is True


def age_entry(key, seconds):
    """Backdate an entry's last-used stamp to simulate `seconds` of inactivity."""
    engine_registry._engines[key].last_used_at -= seconds


def test_idle_engine_is_retired_and_its_connections_closed():
    """10 minutes of inactivity retires the pool -- the whole ask."""
    key = engine_registry.fingerprint("postgres", PG_CREDS)
    engine = engine_registry.get_or_create_engine(key, make(), wtype="postgres")

    age_entry(key, engine_registry.ENGINE_IDLE_TTL_SECONDS + 1)
    retired = engine_registry._sweep()

    assert retired == 1
    assert engine.disposed is True
    assert engine_registry._engines == {}


def test_engine_within_the_ttl_is_left_alone():
    key = engine_registry.fingerprint("postgres", PG_CREDS)
    engine = engine_registry.get_or_create_engine(key, make(), wtype="postgres")

    age_entry(key, engine_registry.ENGINE_IDLE_TTL_SECONDS - 5)

    assert engine_registry._sweep() == 0
    assert engine.disposed is False


def test_next_request_after_retirement_rebuilds_the_pool():
    """'Next time user comes again, pool comes back.'"""
    key = engine_registry.fingerprint("postgres", PG_CREDS)
    first = engine_registry.get_or_create_engine(key, make("first"), wtype="postgres")

    age_entry(key, engine_registry.ENGINE_IDLE_TTL_SECONDS + 1)
    engine_registry._sweep()

    second = engine_registry.get_or_create_engine(key, make("second"), wtype="postgres")

    assert first.disposed is True
    assert second is not first
    assert second.label == "second"


def test_idle_engine_running_a_query_is_not_retired():
    """
    last_used_at is stamped at hand-out, so a query running longer than the TTL
    looks abandoned. A checked-out connection means the pool is still in service.
    """
    key = engine_registry.fingerprint("postgres", PG_CREDS)
    engine = engine_registry.get_or_create_engine(key, make("busy", checkedout=1), wtype="postgres")

    age_entry(key, engine_registry.ENGINE_IDLE_TTL_SECONDS + 1)

    assert engine_registry._sweep() == 0
    assert engine.disposed is False
    assert key in engine_registry._engines

    # once the query finishes, the next sweep retires it
    engine.pool._checkedout = 0
    age_entry(key, engine_registry.ENGINE_IDLE_TTL_SECONDS + 1)

    assert engine_registry._sweep() == 1
    assert engine.disposed is True


def test_caching_an_engine_starts_the_sweeper_thread():
    """
    Retirement depends entirely on the sweeper, so an engine must never be cached
    without it running. Started lazily, after gunicorn/celery have forked.
    """
    key = engine_registry.fingerprint("postgres", PG_CREDS)
    engine_registry.get_or_create_engine(key, make(), wtype="postgres")

    assert engine_registry._sweeper_started.is_set()
    assert any(
        thread.name == "warehouse-engine-sweeper" and thread.daemon
        for thread in threading.enumerate()
    )


def add_engine(name, checkedout=0):
    """Cache one engine under a distinct warehouse and return (key, engine)."""
    key = engine_registry.fingerprint("postgres", dict(PG_CREDS, database=name))
    engine = engine_registry.get_or_create_engine(key, make(name, checkedout), wtype="postgres")
    return key, engine


def test_nothing_caps_how_many_warehouses_are_cached():
    """Cache growth is bounded by the idle TTL alone -- no LRU eviction."""
    for name in ("a", "b", "c", "d", "e"):
        add_engine(name)

    assert len(engine_registry._engines) == 5
    assert all(not entry.engine.disposed for entry in engine_registry._engines.values())


def test_invalidate_all_disposes_everything():
    _, one = add_engine("one")
    _, two = add_engine("two")

    assert engine_registry.invalidate_all() == 2
    assert one.disposed is True
    assert two.disposed is True
    assert engine_registry._engines == {}


def test_registry_stats_reports_the_connection_ceiling():
    add_engine("stats_org", checkedout=2)

    stats = engine_registry.registry_stats()

    assert stats["cached_engines"] == 1
    assert stats["per_warehouse_ceiling"] == (
        engine_registry.POOL_SIZE + engine_registry.POOL_MAX_OVERFLOW
    )
    assert stats["engines"][0]["wtype"] == "postgres"
    assert stats["engines"][0]["checkedout"] == 2


def test_dispose_failure_does_not_break_the_sweep():
    """A pool we are discarding must not be able to fail a request."""

    class ExplodingEngine(FakeEngine):
        def dispose(self):
            raise RuntimeError("socket already gone")

    key = engine_registry.fingerprint("postgres", PG_CREDS)
    engine_registry.get_or_create_engine(key, lambda: ExplodingEngine(), wtype="postgres")
    age_entry(key, engine_registry.ENGINE_IDLE_TTL_SECONDS + 1)

    assert engine_registry._sweep() == 1
    assert engine_registry._engines == {}
