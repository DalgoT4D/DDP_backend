# from django.contrib import admin
from django.urls import path
from ddpui.api.client_controller import clientapi
from ddpui.api.admin_controller import adminapi

urlpatterns = [
    # can enable if we want to use the django-admin app
    # path("admin/", admin.site.urls),
    path("adminapi/", adminapi.urls),
    path("api/", clientapi.urls),
]
