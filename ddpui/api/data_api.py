"""All the master data api can be found here"""

from ninja import Router
from ninja.errors import HttpError
from django.forms.models import model_to_dict

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


@data_router.get("/tasks/")
@has_permission(["can_view_master_tasks"])
def get_tasks(request):
    """
    Fetch the master list of available transformation tasks.

    Returns all system-defined tasks that can be used for data transformations,
    including dbt, git, and dbtcloud task types. Each task contains configuration
    parameters that define how it can be executed.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        list: List of task dictionaries (excluding internal IDs) containing
              task definitions for transformation operations

    Raises:
        HttpError: 403 if user lacks permission to view master tasks
    """
    tasks = [
        model_to_dict(task, exclude=["id"])
        for task in Task.objects.filter(type__in=["dbt", "git", "dbtcloud"]).all()
    ]
    return tasks


@data_router.get("/tasks/{slug}/config/")
@has_permission(["can_view_master_task"])
def get_task_config(request, slug):
    """
    Get detailed configuration parameters for a specific task.

    Returns the complete configuration schema for a task, including all
    parameters that can be customized when executing the task. This includes
    required parameters, optional settings, and their expected data types.

    Args:
        request: HTTP request object containing orguser authentication data
        slug (str): Unique slug identifier of the task

    Returns:
        dict: Task configuration parameters and schema

    Raises:
        HttpError: 404 if task not found
        HttpError: 403 if user lacks permission to view the task
    """
    task = Task.objects.filter(slug=slug).first()

    if not task:
        raise HttpError(404, "Task not found")

    return dbt_service.task_config_params(task)


@data_router.get("/roles/")
def get_roles(request):
    """
    Fetch available user roles based on the requesting user's permission level.

    Returns roles that are at or below the requesting user's role level,
    ensuring users can only assign roles they have permission to manage.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        list: List of role dictionaries containing uuid, slug, and name
              for roles the user can access or assign
    """
    orguser: OrgUser = request.orguser

    roles = Role.objects.filter(level__lte=orguser.new_role.level).all()

    return [{"uuid": role.uuid, "slug": role.slug, "name": role.name} for role in roles]


@data_router.get("/user_prompts/")
def get_user_prompts(request):
    """
    Fetch all available user prompts for LLM interactions.

    Returns the master list of predefined prompts that users can utilize
    when interacting with the Large Language Model features of the platform.

    Args:
        request: HTTP request object containing authentication data

    Returns:
        list: List of user prompt dictionaries containing prompt templates
              and metadata for LLM interactions
    """
    return list(map(model_to_dict, UserPrompt.objects.all()))


@data_router.get("/llm_data_analysis_query_limit/")
def get_row_limit(request):
    """
    Get the maximum number of rows that can be sent to LLM for data analysis.

    Returns the configured limit for the number of data rows that will be
    included when sending datasets to the Large Language Model for analysis.
    This helps control costs and API limits.

    Args:
        request: HTTP request object containing authentication data

    Returns:
        int: Maximum number of rows allowed for LLM data analysis queries
    """
    return LIMIT_ROWS_TO_SEND_TO_LLM
