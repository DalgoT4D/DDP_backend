"""
functions to work with pipelies/dataflows
do not raise http errors here
"""


from redis import Redis
from ddpui.models.tasks import OrgTask
from ddpui.models.org_user import OrgUser
from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect.schema import PrefectFlowAirbyteConnection2, PrefectDbtTaskSetup
from ddpui.ddpprefect import AIRBYTECONNECTION
from ddpui.utils.constants import AIRBYTE_SYNC_TIMEOUT, TRANSFORM_TASKS_SEQ


logger = CustomLogger("ddpui")


def prepare_airbyte_sync_task_config(
    org_task: OrgTask, server_block: OrgPrefectBlockv1, seq: int = 0
):
    return {
        "seq": seq,
        "slug": org_task.task.slug,
        "type": AIRBYTECONNECTION,
        "airbyte_server_block": server_block.block_name,
        "connection_id": org_task.connection_id,
        "timeout": AIRBYTE_SYNC_TIMEOUT,
    }


def pipeline_sync_tasks(
    org: Org,
    connections: list[PrefectFlowAirbyteConnection2],
    server_block: OrgPrefectBlockv1,
):
    """Returns a list of org tasks with their configs"""
    task_configs = []
    org_tasks = []  # org tasks found related to the connections
    seq = 0

    connections.sort(key=lambda conn: conn.seq)
    for connection in connections:
        logger.info(connection)
        org_task = OrgTask.objects.filter(org=org, connection_id=connection.id).first()
        if org_task is None:
            logger.info(
                f"connection id {connection.id} not found in org tasks; ignoring this airbyte sync"
            )
            continue
        # map this org task to dataflow
        org_tasks.append(org_task)

        logger.info(
            f"connection id {connection.id} found in org tasks; pushing to pipeline"
        )
        seq += 1
        task_configs.append(
            prepare_airbyte_sync_task_config(org_task, server_block, seq)
        )

    return org_tasks, task_configs


def pipeline_dbt_git_tasks(
    org: Org,
):
    """Returns a list of org tasks with their config"""
    task_configs = []
    org_tasks = []  # org tasks found related to the dbt, git
    for org_task in OrgTask.objects.filter(
        org=org, task__type__in=["dbt", "git"]
    ).all():
        logger.info(f"found transform task {org_task.task.slug}; pushing to pipeline")
        # map this org task to dataflow
        org_tasks.append(org_task)

        dbt_core_task_setup = PrefectDbtTaskSetup(
            seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
            slug=org_task.task.slug,
            commands=[f"{dbt_binary} {org_task.task.command} --target {target}"],
            type=DBTCORE,
            env={},
            working_dir=project_dir,
            profiles_dir=f"{project_dir}/profiles/",
            project_dir=project_dir,
            cli_profile_block=cli_block.block_name,
            cli_args=[],
        )

        task_config = dict(dbt_core_task_setup)

        # update task_config its a git pull task
        if org_task.task.slug == TASK_GITPULL:
            shell_env = {"secret-git-pull-url-block": ""}

            gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
                org=orguser.org, block_type=SECRET, block_name__contains="git-pull"
            ).first()

            if gitpull_secret_block is not None:
                shell_env["secret-git-pull-url-block"] = gitpull_secret_block.block_name

            shell_task_setup = PrefectShellTaskSetup(
                commands=["git pull"],
                working_dir=project_dir,
                env=shell_env,
                slug=org_task.task.slug,
                type=SHELLOPERATION,
                seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
            )

            task_config = dict(shell_task_setup)

        tasks.append(task_config)
