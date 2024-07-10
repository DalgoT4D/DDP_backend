# main urls
from django.contrib import admin
from django.urls import include, path

from ddpui.api.airbyte_api import airbyteapi
from ddpui.api.dashboard_api import dashboardapi
from ddpui.api.data_api import dataapi
from ddpui.api.dbt_api import dbtapi
from ddpui.api.orgtask_api import orgtaskapi
from ddpui.api.pipeline_api import pipelineapi
from ddpui.api.superset_api import supersetapi
from ddpui.api.task_api import taskapi
from ddpui.api.transform_api import transformapi
from ddpui.api.user_org_api import user_org_api
from ddpui.api.warehouse_api import warehouseapi
from ddpui.api.webhook_api import webhookapi
from ddpui.api.user_preferences_api import userpreferencesapi
from ddpui.healthcheck import healthcheck
from ddpui.html.docs import get_dbt_docs
from ddpui.html.elementary import get_elementary_report

from ddpui.datainsights.generate_result import DataInsightsConsumer
from ddpui.websockets.airbyte_consumer import SourceCheckConnectionConsumer
from ddpui.websockets.airbyte_consumer import DestinationCheckConnectionConsumer

urlpatterns = [
    path("admin/", admin.site.urls),  # Uncomment if you want to use django-admin app
    path("api/dashboard/", dashboardapi.urls),
    path("api/airbyte/", airbyteapi.urls),
    path("api/data/", dataapi.urls),
    path("api/dbt/", dbtapi.urls),
    path("api/prefect/tasks/", orgtaskapi.urls),
    path("api/prefect/", pipelineapi.urls),
    path("api/tasks/", taskapi.urls),
    path("api/warehouse/", warehouseapi.urls),
    path("api/superset/", supersetapi.urls),
    path("api/transform/", transformapi.urls),
    path("healthcheck", healthcheck),
    path("api/", user_org_api.urls),
    path("docs/<tokenhex>/", get_dbt_docs),
    path("elementary/<tokenhex>/", get_elementary_report),
    path("prometheus/", include("django_prometheus.urls")),
    path("webhooks/", webhookapi.urls),
    path("api/userpreferences/", userpreferencesapi.urls),
]

# socket endpoints
ws_urlpatterns = [
    path("wss/data_insights/", DataInsightsConsumer.as_asgi()),
    path(
        "wss/airbyte/source/check_connection", SourceCheckConnectionConsumer.as_asgi()
    ),
    path(
        "wss/airbyte/destination/check_connection",
        DestinationCheckConnectionConsumer.as_asgi(),
    ),
]
