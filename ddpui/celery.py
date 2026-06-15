"""Celery app and configuration — single source of truth.

All Celery-related configuration lives here. Do NOT add CELERY_* prefixed
variables to settings.py; this module no longer pulls them in. If a future
contributor needs a new Celery setting, add it as `app.conf.X = ...` below.
"""

import os

from celery import Celery

from ddpui.utils.redis_db import RedisDB

# Django must be configured before any task module imports Django ORM bits.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")


# ── Redis connection ──────────────────────────────────────────────────────
# See ddpui/utils/redis_db.py for the canonical DB index assignments.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"

BROKER_URL = f"{REDIS_URL}/{int(RedisDB.BROKER)}"
RESULT_BACKEND_URL = f"{REDIS_URL}/{int(RedisDB.RESULTS)}"
REDBEAT_URL = f"{REDIS_URL}/{int(RedisDB.BEAT)}"


# ── Celery app ────────────────────────────────────────────────────────────
app = Celery("ddpui", broker=BROKER_URL, backend=RESULT_BACKEND_URL)
app.autodiscover_tasks()


# ── Routing ───────────────────────────────────────────────────────────────
# Long-running dbt jobs and time-sensitive alerts get their own queues so
# they can't starve each other or the default queue.
app.conf.task_default_queue = "default"
app.conf.task_routes = {
    "ddpui.celeryworkers.tasks.run_dbt_commands": {"queue": "canvas_dbt"},
    "alerts.dispatch_due_alerts": {"queue": "alerts"},
    "alerts.evaluate_alert": {"queue": "alerts"},
}


# ── Worker behaviour ──────────────────────────────────────────────────────
# Fair task distribution — workers don't hoard tasks they can't run yet.
app.conf.worker_prefetch_multiplier = 1


# ── Beat (redbeat, Redis-backed) ──────────────────────────────────────────
# Replaces the default PersistentScheduler which writes celerybeat-schedule.db
# locally and corrupts under hard kills. All beat state now lives in Redis.
app.conf.beat_scheduler = "redbeat.RedBeatScheduler"
app.conf.redbeat_redis_url = REDBEAT_URL
