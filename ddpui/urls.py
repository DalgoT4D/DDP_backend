# main urls
# from django.contrib import admin
from django.urls import path, include
from ddpui.api.admin.user_org_api import adminapi
from ddpui.api.client.airbyte_api import airbyteapi
from ddpui.api.client.dbt_api import dbtapi
from ddpui.api.client.prefect_api import prefectapi
from ddpui.api.client.user_org_api import user_org_api
from ddpui.api.client.task_api import taskapi
from ddpui.api.client.dashboard_api import dashboardapi
from ddpui.html.docs import get_dbt_docs

urlpatterns = [
    # path("admin/", admin.site.urls), # Uncomment if you want to use django-admin app
    path("adminapi/", adminapi.urls),
    path("api/dashboard/", dashboardapi.urls),
    path("api/airbyte/", airbyteapi.urls),
    path("api/dbt/", dbtapi.urls),
    path("api/prefect/", prefectapi.urls),
    path("api/tasks/", taskapi.urls),
    path("api/", user_org_api.urls),
    path("docs/<tokenhex>/", get_dbt_docs),
    path("prometheus/", include("django_prometheus.urls")),
]
