"""Single source of truth for Redis DB index assignments.

Every concern that touches Redis gets its own numbered DB so memory metrics,
`FLUSHDB` blast radius, and key inspection stay clean. To add a new concern,
add an enum member here and import `RedisDB` where you pick a DB.

Standalone module — imports only stdlib — so it can be imported from
settings.py, celery.py, and util modules without circular-import risk.
"""

from enum import IntEnum


class RedisDB(IntEnum):
    """Redis DB index per concern. The integer value IS the DB number."""

    BROKER = 0  # Celery broker — task messages flowing producer → worker
    BEAT = 1  # Redbeat — periodic schedule state
    RESULTS = 2  # Celery result backend — AsyncResult lookups, return values
    CHANNELS = 3  # Django channels — websocket pub/sub
    APP_CACHE = 4  # App-level cache — task progress, auth caches, ad-hoc keys
