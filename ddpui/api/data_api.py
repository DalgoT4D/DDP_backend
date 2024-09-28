"""All the master data api can be found here"""

from ninja import NinjaAPI, Router
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
from ddpui.auth import has_permission
from ddpui.ddpdbt import dbt_service
from ddpui.models.llm import UserPrompt

from ddpui.utils.constants import LIMIT_ROWS_TO_SEND_TO_LLM

data_router = Router()
logger = CustomLogger("ddpui")


@data_router.get("/tasks/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_master_tasks"])
def get_tasks(request):
    """Fetch master list of tasks related to transformation"""
    tasks = [
        model_to_dict(task, exclude=["id"])
        for task in Task.objects.filter(type__in=["dbt", "git"]).all()
    ]
    return tasks


@data_router.get("/tasks/{slug}/config/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_master_task"])
def get_task_config(request, slug):
    """Get task config which details about the parameters that can be added/used while running it"""
    task = Task.objects.filter(slug=slug).first()

    if not task:
        raise HttpError(404, "Task not found")

    return dbt_service.task_config_params(task)


@data_router.get("/roles/", auth=auth.CustomAuthMiddleware())
def get_roles(request):
    """Fetch master list of roles"""
    orguser: OrgUser = request.orguser

    roles = Role.objects.filter(level__lte=orguser.new_role.level).all()

    return [{"uuid": role.uuid, "slug": role.slug, "name": role.name} for role in roles]


@data_router.get("/user_prompts/", auth=auth.CustomAuthMiddleware())
def get_user_prompts(request):
    """Fetch master list of roles"""
    return list(map(model_to_dict, UserPrompt.objects.all()))


@data_router.get("/llm_data_analysis_query_limit/", auth=auth.CustomAuthMiddleware())
def get_row_limit(request):
    """Fetch master list of roles"""
    return LIMIT_ROWS_TO_SEND_TO_LLM
