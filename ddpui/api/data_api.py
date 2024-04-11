"""All the master data api can be found here"""

from ninja import NinjaAPI
from ninja.errors import HttpError
from django.forms.models import model_to_dict

from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError


from ddpui import auth
from ddpui.models.tasks import Task
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.constants import (
    TASK_DBTRUN,
    TASK_GITPULL,
)
from ddpui.auth import has_permission
from ddpui.ddpdbt import dbt_service

dataapi = NinjaAPI(urls_namespace="master_data")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")


@dataapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@dataapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    print(exc)
    return Response({"detail": exc.errors()}, status=500)


@dataapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    logger.info(exc)
    return Response({"detail": "something went wrong"}, status=500)


@dataapi.get("/tasks/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_master_tasks"])
def get_tasks(request):
    """Fetch master list of tasks related to transformation"""
    tasks = [
        model_to_dict(task, exclude=["id"])
        for task in Task.objects.filter(type__in=["dbt", "git"]).all()
    ]
    return tasks


@dataapi.get("/tasks/{slug}/config/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_master_task"])
def get_task_config(request, slug):
    """Get task config which details about the parameters that can be added/used while running it"""
    task = Task.objects.filter(slug=slug).first()

    if not task:
        raise HttpError(404, "Task not found")

    return dbt_service.task_config_params(task)


@dataapi.get("/roles/", auth=auth.CustomAuthMiddleware())
def get_roles(request):
    """Fetch master list of roles"""
    orguser: OrgUser = request.orguser

    roles = Role.objects.filter(level__lte=orguser.new_role.level).all()

    return [{"uuid": role.uuid, "slug": role.slug, "name": role.name} for role in roles]
