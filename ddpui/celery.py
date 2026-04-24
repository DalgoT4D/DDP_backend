import os

from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Here we use redis as both Celery message broker(delivering task messages) and backend(for task status storage)
app = Celery(
    "ddpui",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}",
    broker=f"redis://{REDIS_HOST}:{REDIS_PORT}",
)

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Task routing configuration
app.conf.task_routes = {
    "ddpui.celeryworkers.tasks.run_dbt_commands": {"queue": "canvas_dbt"},
}

# Default queue
app.conf.task_default_queue = "default"

# Worker configuration for better task distribution
app.conf.worker_prefetch_multiplier = 1  # Fair task distribution
