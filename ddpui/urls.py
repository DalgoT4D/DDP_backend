# main urls
# from django.contrib import admin
from django.urls import path, include
from ddpui.api.airbyte_api import airbyteapi
from ddpui.api.dbt_api import dbtapi
from ddpui.api.orgtask_api import orgtaskapi
from ddpui.api.user_org_api import user_org_api
from ddpui.api.task_api import taskapi
from ddpui.api.warehouse_api import warehouseapi
from ddpui.api.dashboard_api import dashboardapi
from ddpui.html.docs import get_dbt_docs
from ddpui.api.webhook_api import webhookapi
from ddpui.api.superset_api import supersetapi
from ddpui.api.pipeline_api import pipelineapi
from ddpui.api.data_api import dataapi
from ddpui.healthcheck import healthcheck

urlpatterns = [
    # path("admin/", admin.site.urls), # Uncomment if you want to use django-admin app
    path("api/dashboard/", dashboardapi.urls),
    path("api/airbyte/", airbyteapi.urls),
    path("api/data/", dataapi.urls),
    path("api/dbt/", dbtapi.urls),
    path("api/prefect/tasks/", orgtaskapi.urls),
    path("api/prefect/", pipelineapi.urls),
    path("api/tasks/", taskapi.urls),
    path("api/warehouse/", warehouseapi.urls),
    path("api/superset/", supersetapi.urls),
    path("healthcheck", healthcheck),
    path("api/", user_org_api.urls),
    path("docs/<tokenhex>/", get_dbt_docs),
    path("prometheus/", include("django_prometheus.urls")),
    path("webhooks/", webhookapi.urls),
]
