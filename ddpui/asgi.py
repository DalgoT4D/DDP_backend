"""
ASGI config for ddpui project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.1/howto/deployment/asgi/
"""

import os
import sys
import signal
import threading
import tracemalloc
import traceback
from datetime import datetime

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")

tracemalloc.start(25)


def _dump_memory_profile(signum, frame):
    """Send SIGUSR1 to dump memory and thread info: kill -SIGUSR1 <pid>"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_path = f"/tmp/memory_dump_{timestamp}.txt"

    with open(dump_path, "w") as f:
        threads = threading.enumerate()
        f.write(f"=== THREADS: {len(threads)} ===\n\n")
        for t in threads:
            f.write(f"Thread: {t.name} (daemon={t.daemon}, alive={t.is_alive()})\n")

        f.write("\n=== THREAD STACK TRACES ===\n\n")
        for thread_id, stack in sys._current_frames().items():
            f.write(f"--- Thread {thread_id} ---\n")
            f.write("".join(traceback.format_stack(stack)))
            f.write("\n")

        snapshot = tracemalloc.take_snapshot()
        f.write("=== TOP 30 MEMORY ALLOCATIONS (by line) ===\n\n")
        for stat in snapshot.statistics("lineno")[:30]:
            f.write(f"{stat}\n")

        f.write("\n=== TOP 30 MEMORY ALLOCATIONS (by traceback) ===\n\n")
        for stat in snapshot.statistics("traceback")[:30]:
            f.write(f"{stat.size / 1024 / 1024:.2f} MB\n")
            for line in stat.traceback.format():
                f.write(f"  {line}\n")
            f.write("\n")

    print(f"Memory dump written to {dump_path}", flush=True)


signal.signal(signal.SIGUSR1, _dump_memory_profile)

from django.core.asgi import get_asgi_application

django_asgi_app = get_asgi_application()

from django.urls import path
from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter, ChannelNameRouter
from channels.security.websocket import AllowedHostsOriginValidator
from ddpui.urls import ws_urlpatterns


application = ProtocolTypeRouter(
    {
        "http": django_asgi_app,
        "websocket": AllowedHostsOriginValidator(AuthMiddlewareStack(URLRouter(ws_urlpatterns))),
    }
)
