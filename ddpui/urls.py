# main urls
from django.contrib import admin
from django.urls import include, path
from django.http import HttpResponse
from django.conf import settings
from django.conf.urls.static import static

from ddpui.routes import src_api
from ddpui.html.docs import get_dbt_docs
from ddpui.html.elementary import get_elementary_report

from ddpui.datainsights.generate_result import DataInsightsConsumer
from ddpui.websockets.airbyte_consumer import SchemaCatalogConsumer, SourceCheckConnectionConsumer
from ddpui.websockets.airbyte_consumer import DestinationCheckConnectionConsumer

from ddpui.admin.views.custom_views import orgs_index_view


def trigger_error(request):  # pylint: disable=unused-argument # skipcq PYK-W0612
    """endpoint to test sentry"""
    division_by_zero = 1 / 0  # pylint: disable=unused-variable


def healthcheck(request):  # pylint:disable=unused-argument
    """Healthcheck endpoint for load balancers"""
    return HttpResponse("OK")


urlpatterns = [
    path("admin/orgs/", orgs_index_view, name="orgs_index_view"),
    path("admin/", admin.site.urls),
    path("healthcheck", healthcheck),
    path("docs/<tokenhex>/", get_dbt_docs),
    path("elementary/<tokenhex>/", get_elementary_report),
    path("prometheus/", include("django_prometheus.urls")),
    path("sentry-debug/", trigger_error),
    path("", src_api.urls),
]

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

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
