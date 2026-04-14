# main urls
from django.contrib import admin
from django.urls import include, path
from django.http import HttpResponse

from ddpui.routes import src_api, public_api
from ddpui.html.docs import get_dbt_docs
from ddpui.html.elementary import get_elementary_report

from ddpui.core.datainsights.generate_result import DataInsightsConsumer
from ddpui.websockets.airbyte_consumer import SchemaCatalogConsumer, SourceCheckConnectionConsumer
from ddpui.websockets.airbyte_consumer import DestinationCheckConnectionConsumer


def trigger_error(request):  # pylint: disable=unused-argument # skipcq PYK-W0612
    """endpoint to test sentry"""
    division_by_zero = 1 / 0  # pylint: disable=unused-variable


def healthcheck(request):  # pylint:disable=unused-argument
    """Healthcheck endpoint for load balancers"""
    return HttpResponse("OK")


def memory_snapshot(request):
    import tracemalloc, os, psutil

    process = psutil.Process(os.getpid())
    rss = process.memory_info().rss / 1024 / 1024  # MB

    if not tracemalloc.is_tracing():
        tracemalloc.start()
        return {"rss_mb": rss, "status": "tracing started, call again after spike"}

    snapshot = tracemalloc.take_snapshot()
    top = snapshot.statistics("lineno")[:30]
    return {
        "rss_mb": rss,
        "top_allocations": [str(s) for s in top],
    }


urlpatterns = [
    path("admin/", admin.site.urls),
    path("healthcheck", healthcheck),
    path("debug/memory", memory_snapshot),
    path("docs/<tokenhex>/", get_dbt_docs),
    path("elementary/<tokenhex>/", get_elementary_report),
    path("prometheus/", include("django_prometheus.urls")),
    path("sentry-debug/", trigger_error),
    path("", src_api.urls),
    path("", public_api.urls),  # Public API without auth
]

# socket endpoints
ws_urlpatterns = [
    path("wss/data_insights/", DataInsightsConsumer.as_asgi()),
    path("wss/airbyte/source/check_connection", SourceCheckConnectionConsumer.as_asgi()),
    path(
        "wss/airbyte/destination/check_connection",
        DestinationCheckConnectionConsumer.as_asgi(),
    ),
    path("wss/airbyte/connection/schema_catalog", SchemaCatalogConsumer.as_asgi()),
]
