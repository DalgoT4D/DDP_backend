"""these are tasks which we run through celery"""

import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from time import sleep
from subprocess import CompletedProcess
import pytz
from django.core.management import call_command
from django.utils.text import slugify

import yaml
from celery.schedules import crontab
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.celery import app, Celery
from ddpui.settings import PRODUCTION


from ddpui.utils import timezone, awsses
from ddpui.utils.webhook_helpers import (
    notify_org_managers,
    do_handle_prefect_webhook,
    get_org_from_flow_run,
)

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.awsses import send_text_message
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org import (
    Org,
    OrgDbt,
    OrgSchemaChange,
    OrgWarehouse,
    OrgPrefectBlockv1,
    OrgDataFlowv1,
    TransformType,
)
from ddpui.models.airbyte import AirbyteJob

from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import (
    TaskLock,
    OrgTask,
    TaskProgressHashPrefix,
    TaskProgressStatus,
)
from ddpui.models.canvaslock import CanvasLock
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.llm import (
    AssistantPrompt,
    LlmAssistantType,
    LlmSession,
    LogsSummarizationType,
    LlmSessionStatus,
)
from ddpui.utils.helpers import runcmd, runcmd_with_output, subprocess, get_integer_env_var
from ddpui.utils import secretsmanager
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.utils.constants import (
    TASK_AIRBYTESYNC,
    TASK_AIRBYTERESET,
    TASK_AIRBYTECLEAR,
    AIRBYTE_CONNECTION_DEPRECATED,
    AIRBYTE_JOB_STATUS_FAILED,
    TASK_DBTRUN,
    SYSTEM_USER_EMAIL,
)
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.ddpprefect.prefect_service import (
    get_flow_run_graphs,
    update_dbt_core_block_schema,
    get_dbt_cli_profile_block,
    prefect_get,
    recurse_flow_run_logs,
    get_long_running_flow_runs,
    compute_dataflow_run_times_from_history,
    get_flow_run_poll,
)
from ddpui.ddpprefect import DBTCLIPROFILE, AIRBYTECONNECTION
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.core import llm_service
from ddpui.utils.helpers import (
    find_key_in_dictionary,
    convert_sqlalchemy_rows_to_csv_string,
)

logger = CustomLogger("ddpui")
UTC = timezone.UTC


@app.task(bind=True)
def clone_github_repo(
    self,
    org_slug: str,
    gitrepo_url: str,
    gitrepo_access_token: str | None,
    org_dir: str,
    taskprogress: TaskProgress | None,
) -> bool:
    """clones an org's github repo"""
    if taskprogress is None:
        child = False
        taskprogress = TaskProgress(
            self.request.id, f"{TaskProgressHashPrefix.CLONEGITREPO}-{org_slug}"
        )
    else:
        child = True

    taskprogress.add(
        {
            "message": "started cloning github repository",
            "status": "running",
        }
    )

    # clone the client's dbt repo into "dbtrepo/" under the project_dir
    # if we have an access token with the "contents" and "metadata" permissions then
    #   git clone https://oauth2:[TOKEN]@github.com/[REPO-OWNER]/[REPO-NAME]
    if gitrepo_access_token is not None:
        gitrepo_url = gitrepo_url.replace(
            "github.com", "oauth2:" + gitrepo_access_token + "@github.com"
        )

    org_dir: Path = Path(org_dir)
    dbtrepo_dir = org_dir / "dbtrepo"
    if not org_dir.exists():
        org_dir.mkdir()
        taskprogress.add(
            {
                "message": "created project_dir",
                "status": "running",
            }
        )
        logger.info("created project_dir %s", org_dir)

    elif dbtrepo_dir.exists():
        shutil.rmtree(str(dbtrepo_dir))

    cmd = f"git clone {gitrepo_url} dbtrepo"

    try:
        runcmd(cmd, org_dir)
    except Exception as error:
        taskprogress.add(
            {
                "message": "git clone failed",
                "error": str(error),
                "status": "failed",
            }
        )
        logger.exception(error)
        return None

    taskprogress.add(
        {
            "message": "cloned git repo",
            "status": "running" if child else "completed",
        }
    )
    return dbtrepo_dir


@app.task(bind=True)
def setup_dbtworkspace(self, org_id: int, payload: dict) -> str:
    """sets up an org's dbt workspace, recreating it if it already exists"""
    org = Org.objects.filter(id=org_id).first()
    logger.info("found org %s", org.name)

    taskprogress = TaskProgress(
        self.request.id, f"{TaskProgressHashPrefix.DBTWORKSPACE}-{org.slug}"
    )

    taskprogress.add(
        {
            "message": "started",
            "status": "running",
        }
    )
    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        taskprogress.add(
            {
                "message": "need to set up a warehouse first",
                "status": "failed",
            }
        )
        logger.error("need to set up a warehouse first for org %s", org.name)
        raise Exception("need to set up a warehouse first for org %s" % org.name)

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    # this client'a dbt setup happens here
    org_dir = DbtProjectManager.get_org_dir(org)

    # five parameters here is correct despite vscode thinking otherwise
    dbtcloned_repo_path = clone_github_repo(
        org.slug,
        payload["gitrepoUrl"],
        payload["gitrepoAccessToken"],
        org_dir,
        taskprogress,
    )
    if not dbtcloned_repo_path:
        raise Exception("Failed to clone git repo")

    logger.info("git clone succeeded for org %s", org.name)

    dbt = OrgDbt(
        gitrepo_url=payload["gitrepoUrl"],
        project_dir=DbtProjectManager.get_dbt_repo_relative_path(dbtcloned_repo_path),
        dbt_venv=DbtProjectManager.DEFAULT_DBT_VENV_REL_PATH,
        target_type=warehouse.wtype,
        default_schema=payload["profile"]["target_configs_schema"],
        transform_type=TransformType.GIT,
    )
    dbt.save()
    logger.info("created orgdbt for org %s", org.name)
    org.dbt = dbt
    org.save()
    logger.info("set org.dbt for org %s", org.name)

    if payload["gitrepoAccessToken"] is not None:
        secretsmanager.delete_github_token(org)
        secretsmanager.save_github_token(org, payload["gitrepoAccessToken"])

    taskprogress.add(
        {
            "message": "wrote OrgDbt entry",
            "status": "completed",
        }
    )
    logger.info("set dbt workspace completed for org %s", org.name)


@app.task(bind=True)
def run_dbt_commands(self, org_id: int, task_id: str, dbt_run_params: dict = None):
    """run a dbt command via celery instead of via prefect"""
    dbtrun_orgtask = OrgTask.objects.filter(org__id=org_id, task__slug=TASK_DBTRUN).first()
    system_user = OrgUser.objects.filter(user__email=SYSTEM_USER_EMAIL).first()
    task_lock = TaskLock.objects.create(
        orgtask=dbtrun_orgtask,
        locked_by=system_user,
    )

    try:
        org: Org = Org.objects.filter(id=org_id).first()

        logger.info("found org %s", org.name)

        taskprogress = TaskProgress(
            task_id, f"{TaskProgressHashPrefix.RUNDBTCMDS.value}-{org.slug}"
        )

        taskprogress.add(
            {
                "message": "started",
                "status": "running",
            }
        )

        orgdbt = OrgDbt.objects.filter(org=org).first()
        if orgdbt is None:
            taskprogress.add(
                {
                    "message": "need to set up a dbt workspace first",
                    "status": "failed",
                }
            )
            logger.error("need to set up a dbt workspace first for org %s", org.name)
            return

        dbt_cli_profile = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=DBTCLIPROFILE
        ).first()
        if dbt_cli_profile is None:
            taskprogress.add(
                {
                    "message": "need to set up a dbt cli profile first",
                    "status": "failed",
                }
            )
            logger.error("need to set up a dbt cli profile first for org %s", org.name)
            return

        dbt_project_params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(
            org, orgdbt
        )

        profile = get_dbt_cli_profile_block(dbt_cli_profile.block_name)["profile"]
        profile_dirname = Path(dbt_project_params.project_dir) / "profiles"
        os.makedirs(profile_dirname, exist_ok=True)
        profile_filename = profile_dirname / "profiles.yml"
        logger.info("writing dbt profile to " + str(profile_filename))
        with open(profile_filename, "w", encoding="utf-8") as f:
            yaml.safe_dump(profile, f)

        dbt_binary = dbt_project_params.dbt_binary
        project_dir = dbt_project_params.project_dir

        # dbt clean
        taskprogress.add({"message": "starting dbt clean", "status": "running"})
        try:
            process: CompletedProcess = runcmd_with_output(
                f"{dbt_binary} clean --profiles-dir=profiles", project_dir
            )
            command_output = process.stdout.decode("utf-8").split("\n")
            taskprogress.add(
                {
                    "message": "dbt clean output",
                    "status": "running",
                }
            )
            for cmd_out in command_output:
                taskprogress.add(
                    {
                        "message": cmd_out,
                        "status": "running",
                    }
                )
        except subprocess.CalledProcessError as error:
            taskprogress.add(
                {
                    "message": "dbt clean failed",
                    "status": "failed",
                }
            )
            taskprogress.add(
                {
                    "message": str(error),
                    "status": "failed",
                }
            )
            logger.exception(error)
            raise Exception("Dbt clean failed")

        # dbt deps
        try:
            taskprogress.add({"message": "starting dbt deps", "status": "running"})
            process: CompletedProcess = runcmd_with_output(
                f"{dbt_binary} deps --profiles-dir=profiles", project_dir
            )
            command_output = process.stdout.decode("utf-8").split("\n")
            taskprogress.add(
                {
                    "message": "dbt deps output",
                    "status": "running",
                }
            )
            for cmd_out in command_output:
                taskprogress.add(
                    {
                        "message": cmd_out,
                        "status": "running",
                    }
                )
        except subprocess.CalledProcessError as error:
            taskprogress.add(
                {
                    "message": "dbt deps failed",
                    "status": "failed",
                }
            )
            taskprogress.add(
                {
                    "message": str(error),
                    "status": "failed",
                }
            )
            logger.exception(error)
            raise Exception("Dbt deps failed")

        # dbt run
        try:
            cmd = f"{dbt_binary} run"
            if dbt_run_params is not None:
                for flag in dbt_run_params.get("flags") or []:
                    cmd += " --" + flag
                for optname, optval in (dbt_run_params.get("options") or {}).items():
                    cmd += f" --{optname} {optval}"

            taskprogress.add({"message": "starting dbt run", "status": "running"})
            process: CompletedProcess = runcmd_with_output(
                f"{cmd} --profiles-dir=profiles", project_dir
            )
            command_output = process.stdout.decode("utf-8").split("\n")
            taskprogress.add(
                {
                    "message": "dbt run output",
                    "status": "running",
                }
            )
            for cmd_out in command_output:
                taskprogress.add(
                    {
                        "message": cmd_out,
                        "status": "running",
                    }
                )
        except subprocess.CalledProcessError as error:
            taskprogress.add(
                {
                    "message": "dbt run failed",
                    "status": "failed",
                }
            )
            taskprogress.add(
                {
                    "message": str(error),
                    "status": "failed",
                }
            )
            logger.exception(error)
            raise Exception("Dbt run failed")

        # done
        taskprogress.add({"message": "dbt run completed", "status": "completed"})
    except Exception as e:
        logger.error(e)

    finally:
        task_lock.delete()


def detect_schema_changes_for_org(org: Org, delay=0):
    """detect schema changes for all connections of this org"""
    org_tasks = OrgTask.objects.filter(org=org, task__slug=TASK_AIRBYTESYNC)

    # remove invalid schema changes whose connections are no longer in our db
    org_conn_ids = [org_task.connection_id for org_task in org_tasks]
    for schema_change in OrgSchemaChange.objects.filter(org=org).exclude(
        connection_id__in=org_conn_ids
    ):
        schema_change.delete()

    deprecated_org_tasks: list[OrgTask] = []
    tag = " [STAGING]" if not PRODUCTION else ""

    # check for schema changes
    for org_task in org_tasks:
        connection_catalog, err = airbytehelpers.fetch_and_update_org_schema_changes(
            org, org_task.connection_id
        )
        if delay:
            sleep(delay)

        if err:
            if os.getenv("ADMIN_EMAIL"):
                send_text_message(
                    os.getenv("ADMIN_EMAIL"),
                    f"Schema change detection errors for {org.slug}{tag}",
                    err,
                )
            logger.error(err)
            continue

        if connection_catalog is None:
            if os.getenv("ADMIN_EMAIL"):
                send_text_message(
                    os.getenv("ADMIN_EMAIL"),
                    f"Schema change detection errors for {org.slug}{tag}",
                    f"connection_catalog is None for {org_task.connection_id}",
                )
            logger.error(err)
            continue

        if connection_catalog["status"] == AIRBYTE_CONNECTION_DEPRECATED:
            deprecated_org_tasks.append(org_task)
            continue

        change_type = connection_catalog.get("schemaChange")
        catalog_diff: dict = connection_catalog.get("catalogDiff")

        logger.info(
            "Found schema changes for %s connection %s of type %s",
            org.slug,
            org_task.connection_id,
            change_type,
        )

        # notify users
        if change_type == "breaking" or (
            change_type == "non_breaking"
            and catalog_diff
            and len(catalog_diff.get("transforms", [])) > 0
        ):
            try:
                frontend_url = os.getenv("FRONTEND_URL")
                if frontend_url.endswith("/"):
                    frontend_url = frontend_url[:-1]
                connections_page = f"{frontend_url}/pipeline/ingest?tab=connections"
                connection_name = connection_catalog["name"]
                notify_org_managers(
                    org,
                    f"To the admins of {org.name},\n\nThis email is to let you know that"
                    f' schema changes have been detected in your Dalgo sources for "{connection_name}".'
                    f"\n\nPlease visit {connections_page} and review the Pending Actions",
                    f"{org.name}: Schema changes detected in your Dalgo sources",
                )
            except Exception as err:
                logger.error(err)

    if len(deprecated_org_tasks) > 0:
        deprecated_connection_ids = [
            deprecated_org_task.connection_id for deprecated_org_task in deprecated_org_tasks
        ]
        logger.info(
            f"deleting OrgSchemaChange for deprecated connections {','.join(deprecated_connection_ids)} for {org.slug}"
        )
        OrgSchemaChange.objects.filter(
            org=org, connection_id__in=deprecated_connection_ids
        ).delete()


@app.task(bind=False)
def schema_change_detection():
    """detects schema changes for all the orgs and sends an email to admins if there is a change"""
    delay = get_integer_env_var("SCHEMA_CHANGE_DETECTION_INTER_ORG_DELAY", 0, logger, False)
    for org in Org.objects.all():
        detect_schema_changes_for_org(org, delay)


@app.task(bind=False)
def get_connection_catalog_task(task_key, org_id, connection_id):
    """Fetch a connection in the user organization workspace as a Celery task"""
    org = Org.objects.get(id=org_id)
    taskprogress = SingleTaskProgress(task_key, int(os.getenv("SCHEMA_REFRESH_TTL", "180")))
    taskprogress.add({"message": "started", "status": TaskProgressStatus.RUNNING, "result": None})

    connection_catalog, err = airbytehelpers.fetch_and_update_org_schema_changes(org, connection_id)
    # unsure how to handle a deprecated connection, ideally we would never get here

    if err:
        if os.getenv("ADMIN_EMAIL"):
            send_text_message(
                os.getenv("ADMIN_EMAIL"),
                f"Unhandled schema change detection errors for {org.slug}",
                err,
            )
        logger.error(err)
        taskprogress.add(
            {
                "message": err,
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        return

    taskprogress.add(
        {
            "message": "fetched catalog data",
            "status": TaskProgressStatus.COMPLETED,
            "result": {
                "name": connection_catalog["name"],
                "connectionId": connection_catalog["connectionId"],
                "catalogId": connection_catalog["catalogId"],
                "syncCatalog": connection_catalog["syncCatalog"],
                "schemaChange": connection_catalog["schemaChange"],
                "catalogDiff": connection_catalog.get("catalogDiff"),
            },
        }
    )
    return connection_catalog


@app.task()
def get_schema_catalog_task(task_key, workspace_id, source_id):
    """Fetch a schema_catalog while creating a connection as a Celery task"""
    # the STP has to live longer than the task will take
    taskprogress = SingleTaskProgress(task_key, 600)
    taskprogress.add({"message": "started", "status": TaskProgressStatus.RUNNING, "result": None})

    try:
        res = airbyte_service.get_source_schema_catalog(workspace_id, source_id)
        taskprogress.add(
            {
                "message": "Fetched catalog data",
                "status": TaskProgressStatus.COMPLETED,
                "result": res,
            }
        )
        return res
    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": "Failed to get schema catalog",
                "status": TaskProgressStatus.FAILED,
                "result": str(err),
            }
        )


@app.task(bind=False)
def update_dbt_core_block_schema_task(block_name, default_schema):
    """single http PUT request to the prefect-proxy"""
    logger.info("updating default_schema of %s to %s", block_name, default_schema)
    update_dbt_core_block_schema(block_name, default_schema)


@app.task()
def delete_old_canvaslocks():
    """delete canvas locks which were created over 10 minutes ago"""
    tenminutesago = UTC.localize(datetime.now() - timedelta(seconds=600))
    CanvasLock.objects.filter(locked_at__lt=tenminutesago).delete()


@app.task(bind=False)
def sync_flow_runs_of_deployments(deployment_ids: list[str] = None, look_back_hours: int = 24):
    """
    This function will sync (create) latest flow runs of deployment(s) if missing from our db
    """

    query = OrgDataFlowv1.objects
    if deployment_ids:
        query = query.filter(deployment_id__in=deployment_ids)

    # sync recent 50 flow runs of each deployment
    start_time_gt = UTC.localize(datetime.now() - timedelta(hours=look_back_hours))
    for dataflow in query.all():
        try:
            deployment_id = dataflow.deployment_id
            params = {
                "deployment_id": deployment_id,
                "limit": 50,
                "start_time_gt": start_time_gt.isoformat(),
            }
            res = prefect_get("flow_runs", params=params, timeout=60)

            # iterate so that start-time is ASC
            for flow_run in res["flow_runs"][::-1]:
                PrefectFlowRun.objects.update_or_create(
                    flow_run_id=flow_run["id"],
                    defaults={
                        "deployment_id": deployment_id,
                        "name": flow_run["name"],
                        "start_time": (
                            flow_run["startTime"]
                            if flow_run["startTime"] not in ["", None]
                            else flow_run["expectedStartTime"]
                        ),
                        "expected_start_time": flow_run["expectedStartTime"],
                        "total_run_time": flow_run["totalRunTime"],
                        "status": flow_run["status"],
                        "state_name": flow_run["state_name"],
                    },
                )
            logger.info(
                "synced flow runs for deployment %s | org %s",
                deployment_id,
                dataflow.org.slug,
            )
        except Exception as e:
            logger.error(
                "failed to sync flow runs for deployment %s ; moving to next one",
                deployment_id,
            )
            logger.exception(e)
            continue


@app.task(bind=True)
def add_custom_connectors_to_workspace(self, workspace_id, custom_sources: list[dict]):
    """
    This function will add custom sources to a workspace
    """
    for source in custom_sources:
        airbyte_service.create_custom_source_definition(
            workspace_id=workspace_id,
            name=source["name"],
            docker_repository=source["docker_repository"],
            docker_image_tag=source["docker_image_tag"],
            documentation_url=source["documentation_url"],
        )
        logger.info(
            f"added custom source {source['name']} [{source['docker_repository']}:{source['docker_image_tag']}]"
        )


@app.task(bind=True)
def summarize_logs(
    self,
    orguser_id: str = None,  # Make optional for system calls
    type: str = LogsSummarizationType.DEPLOYMENT,  # deployment or airbyte_sync (two types of logs in Dalgo)
    flow_run_id: str = None,
    task_id: str = None,
    job_id: int = None,
    connection_id: str = None,
    attempt_number: int = 0,
    regenerate: bool = False,
):
    """
    Fetch logs from either prefect or airbyte
    1. Fetch all subtasks or subflowruns from prefect along with their logs
    2. Upload logs as a file to llm service
    3. Query the llm service with two prompts one for the summarizing & other for figuring out how to resolve errors

    If regenerate is True and the summary is not found, the program will generate it again & return
    Modified to support system-triggered summarization
    """
    taskprogress = SingleTaskProgress(self.request.id, 60 * 10)

    taskprogress.add({"message": "Started", "status": "running", "result": []})

    # Handle both user and system triggered calls
    orguser = None
    org = None

    if orguser_id:
        orguser = OrgUser.objects.filter(id=orguser_id).first()
        org = orguser.org if orguser else None
    else:
        # System-triggered call - extract org from flow_run
        if job_id and connection_id:
            orgtask = OrgTask.objects.filter(
                connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
            ).first()
            org = orgtask.org if orgtask else None
        elif flow_run_id:
            flow_run = get_flow_run_poll(flow_run_id)
            org = get_org_from_flow_run(flow_run)

    if not org:
        taskprogress.add(
            {
                "message": "Unable to determine organization",
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        return

    # validations
    if type == LogsSummarizationType.AIRBYTE_SYNC:
        try:
            job_info = airbyte_service.get_job_info(str(job_id))

            if job_info["job"]["configId"] != connection_id:
                taskprogress.add(
                    {
                        "message": "Invalid job id",
                        "status": TaskProgressStatus.FAILED,
                        "result": None,
                    }
                )
                return
        except Exception as err:
            logger.error(err)
            taskprogress.add(
                {
                    "message": "Failed to fetch the sync job",
                    "status": TaskProgressStatus.FAILED,
                    "result": None,
                }
            )
            return

    # regenrate or return saved
    if not regenerate:
        # try to fetch response from db
        if type == LogsSummarizationType.DEPLOYMENT:
            llm_session_filter = {
                "org": org,
                "flow_run_id": flow_run_id,
                "task_id": task_id,
            }
            llm_session = LlmSession.objects.filter(**llm_session_filter)

        elif type == LogsSummarizationType.AIRBYTE_SYNC:
            llm_session_filter = {"org": org, "airbyte_job_id": job_id}
            llm_session = LlmSession.objects.filter(**llm_session_filter)

        llm_session = llm_session.order_by("-created_at").first()

        if llm_session:
            if llm_session.response and llm_session.session_status == LlmSessionStatus.COMPLETED:
                taskprogress.add(
                    {
                        "message": "Retrieved saved summary for the run",
                        "status": TaskProgressStatus.COMPLETED,
                        "result": llm_session.response,
                    }
                )
                return
            else:
                # delete this session if it has no response
                llm_session.delete()

    # create a partial session
    llm_session = LlmSession.objects.create(
        request_uuid=self.request.id,
        orguser=orguser,  # Can be None for system calls
        org=org,
        flow_run_id=flow_run_id,
        task_id=task_id,
        airbyte_job_id=job_id,
        session_status=LlmSessionStatus.RUNNING,
        session_type=LlmAssistantType.LOG_SUMMARIZATION,
    )

    # logs
    logs_text = ""
    try:
        if type == LogsSummarizationType.DEPLOYMENT:
            task_runs = get_flow_run_graphs(flow_run_id)

            tasks_to_summarize = [task for task in task_runs if task["id"] == task_id]
            if not tasks_to_summarize:
                taskprogress.add(
                    {
                        "message": "No logs found for the task",
                        "status": TaskProgressStatus.FAILED,
                        "result": None,
                    }
                )
                return

            task = tasks_to_summarize[0]
            task["logs"] = recurse_flow_run_logs(flow_run_id, task_id)
            logs_text = "\n".join([log["message"] for log in task["logs"]])

        elif type == LogsSummarizationType.AIRBYTE_SYNC:
            log_lines = airbyte_service.get_logs_for_job(
                job_id=job_id, attempt_number=attempt_number
            )
            logs_text = "\n".join(log_lines)
    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": str(err),
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        llm_session.session_status = LlmSessionStatus.FAILED
        llm_session.save()
        return

    taskprogress.add(
        {
            "message": "Fetched all logs to summarize",
            "status": TaskProgressStatus.RUNNING,
            "result": None,
        }
    )

    # for each task run these prompts
    user_prompts = [
        "Summarize the primary error that occurred in this run",
        "What steps can I take to solve the error you identified?",
    ]

    try:
        assistant_prompt = AssistantPrompt.objects.filter(
            type=LlmAssistantType.LOG_SUMMARIZATION
        ).first()
        if not assistant_prompt:
            raise Exception("Assistant/System prompt not found for log summarization")

        # for task in dbt_failed_tasks:
        logger.info(f"Uploading logs for {type} to llm service")

        # upload logs for the task & start the session
        fpath, session_id = llm_service.upload_text_as_file(logs_text, f"logs")
        logger.info("Uploaded file successfully to LLM service at " + str(fpath))
        logger.info("Session ID: " + session_id)

        # start a file search session in the llm service
        logger.info(f"Querying the uploaded file: total queries {len(user_prompts)}")
        result = llm_service.file_search_query_and_poll(
            assistant_prompt=assistant_prompt.prompt,
            queries=user_prompts,
            session_id=session_id,
        )

        # close the session
        logger.info("Closing the session")
        llm_service.close_file_search_session(result["session_id"])

        llm_session.user_prompts = user_prompts
        llm_session.assistant_prompt = assistant_prompt.prompt
        llm_session.response = [
            {"prompt": prompt, "response": response}
            for prompt, response in zip(user_prompts, result["result"])
        ]
        llm_session.session_id = result["session_id"]
        llm_session.session_status = LlmSessionStatus.COMPLETED
        llm_session.save()

        logger.info("Completed log summarization")
        taskprogress.add(
            {
                "message": f"Generated summary for the {type} job",
                "status": TaskProgressStatus.COMPLETED,
                "result": llm_session.response,
            }
        )
    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": str(err),
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        llm_session.session_status = LlmSessionStatus.FAILED
        llm_session.save()


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def trigger_log_summarization_for_failed_flow(self, flow_run_id: str, flow_run: dict = None):
    """
    Triggers automatic summarization for failed pipeline runs.
    Handles both regular pipelines and airbyte syncs.
    """
    try:
        # Get flow run if not provided
        if not flow_run:
            flow_run = get_flow_run_poll(flow_run_id)

        # Check if summary already exists to prevent duplicates
        existing_summary = LlmSession.objects.filter(
            flow_run_id=flow_run_id, session_type=LlmAssistantType.LOG_SUMMARIZATION
        ).exists()

        if existing_summary:
            logger.info(f"Summary already exists for flow_run {flow_run_id}")
            return

        # Get failed task information
        task_runs = get_flow_run_graphs(flow_run_id)

        # Find the failed task
        failed_task = None
        for task in task_runs:
            if task.get("state_type") == "FAILED" or task.get("state_name") == "DBT_TEST_FAILED":
                failed_task = task
                break

        if not failed_task:
            logger.warning(f"No failed task found in flow_run {flow_run_id}")
            return

        # Check if this is an airbyte sync task
        is_airbyte_task = False
        connection_id = None

        for task in flow_run.get("parameters", {}).get("config", {}).get("tasks", []):
            if task.get("slug", "") in [TASK_AIRBYTESYNC, TASK_AIRBYTERESET, TASK_AIRBYTECLEAR]:
                is_airbyte_task = True
                connection_id = task.get("connection_id", None)
                break

        # Trigger appropriate summarization
        if is_airbyte_task and connection_id:
            # For airbyte syncs, get the latest failed job from database
            # Get the latest failed job for this connection
            # Job is already synced by the time we reach here
            airbyte_job = (
                AirbyteJob.objects.filter(config_id=connection_id, status=AIRBYTE_JOB_STATUS_FAILED)
                .order_by("-created_at")
                .first()
            )

            if not airbyte_job:
                logger.error(f"No failed airbyte job found for connection {connection_id}")
                return

            latest_failed_attempt_no = airbyte_job.latest_failed_attempt_id

            logger.info(
                f"Triggering summarization for airbyte job {airbyte_job.job_id} with attempt no {latest_failed_attempt_no}"
            )

            # Trigger airbyte sync summarization
            summarize_logs.delay(
                orguser_id=None,  # System call
                type=LogsSummarizationType.AIRBYTE_SYNC,
                flow_run_id=flow_run_id,
                task_id=failed_task.get("id"),
                job_id=airbyte_job.job_id,
                connection_id=connection_id,
                attempt_number=latest_failed_attempt_no,
            )

            logger.info(
                f"Triggered airbyte summarization for job_id {airbyte_job.job_id} with attempt no {latest_failed_attempt_no}"
            )
        else:
            # Trigger deployment summarization
            summarize_logs.delay(
                orguser_id=None,  # System call
                type=LogsSummarizationType.DEPLOYMENT,
                flow_run_id=flow_run_id,
                task_id=failed_task.get("id"),
            )

            logger.info(f"Triggered deployment summarization for task {failed_task.get('id')}")

        logger.info(f"Successfully triggered summarization for flow_run {flow_run_id}")

    except Exception as e:
        logger.exception(f"Error triggering summarization for {flow_run_id}: {str(e)}")
        raise self.retry(exc=e)


@app.task(bind=True)
def summarize_warehouse_results(
    self,
    orguser_id: int,
    org_warehouse_id: int,
    sql: str,
    user_prompt: str,
):
    """
    This function will summarize the results of a warehouse query
    1. Fetch the results of the query
    2. Upload the results as a file to llm service as text file
    3. Query the llm service with the user prompt
    """

    taskprogress = SingleTaskProgress(self.request.id, 60 * 10)
    taskprogress.add({"message": "Started", "status": "running", "result": {}})

    org_warehouse = OrgWarehouse.objects.filter(id=org_warehouse_id).first()

    if not org_warehouse:
        logger.error("Warehouse not found")
        taskprogress.add(
            {
                "message": "Warehouse not found",
                "status": TaskProgressStatus.FAILED,
                "results": {},
            }
        )
        return

    orguser = OrgUser.objects.filter(id=orguser_id).first()
    org = orguser.org

    # create a partial session
    llm_session = LlmSession.objects.create(
        request_uuid=self.request.id,
        orguser=orguser,
        org=org,
        session_status=LlmSessionStatus.RUNNING,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
        request_meta={"sql": sql},
    )

    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

    try:
        wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)
    except Exception as err:
        logger.error("Failed to connect to the warehouse - %s", err)
        taskprogress.add(
            {
                "message": "Failed to connect to the warehouse",
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        llm_session.session_status = LlmSessionStatus.FAILED
        llm_session.save()
        return

    # fetch the results of the query
    logger.info(f"Submitting query to warehouse for execution \n '''{sql}'''")
    rows = []
    try:
        rows = wclient.execute(sql)
    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": str(err),
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        llm_session.session_status = LlmSessionStatus.FAILED
        llm_session.save()
        return

    if len(rows) == 0:
        taskprogress.add(
            {
                "message": "No results found for the query",
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        llm_session.session_status = LlmSessionStatus.FAILED
        llm_session.save()
        return

    try:
        # upload the results as a file to llm service
        fpath, session_id = llm_service.upload_text_as_file(
            convert_sqlalchemy_rows_to_csv_string(rows), "warehouse_results"
        )
        logger.info("Uploaded file successfully to LLM service at " + str(fpath))
        logger.info("Session ID: " + session_id)

        # query the llm service with two prompts
        user_prompts = [user_prompt]

        assistant_prompt = AssistantPrompt.objects.filter(
            type=LlmAssistantType.LONG_TEXT_SUMMARIZATION
        ).first()
        if not assistant_prompt:
            raise Exception("Assistant/System prompt not found for warehouse summarization")

        llm_session.user_prompts = user_prompts
        llm_session.assistant_prompt = assistant_prompt.prompt

        # start a file search session in the llm service
        logger.info("Querying the uploaded file: total queries 2")
        result = llm_service.file_search_query_and_poll(
            assistant_prompt=assistant_prompt.prompt,
            queries=user_prompts,
            session_id=session_id,
        )

        llm_session.session_id = result["session_id"]

        # close the session
        logger.info("Closing the session")
        llm_service.close_file_search_session(result["session_id"])

        llm_session.user_prompts = user_prompts
        llm_session.assistant_prompt = assistant_prompt.prompt
        llm_session.response = [
            {"prompt": prompt, "response": response}
            for prompt, response in zip(user_prompts, result["result"])
        ]
        llm_session.session_status = LlmSessionStatus.COMPLETED
        llm_session.save()

        logger.info("Completed summarization")
        taskprogress.add(
            {
                "message": f"Generated summary response",
                "status": TaskProgressStatus.COMPLETED,
                "result": {
                    "response": llm_session.response,
                    "session_id": llm_session.session_id,
                },
            }
        )

    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": str(err),
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )
        llm_session.session_status = LlmSessionStatus.FAILED
        llm_session.save()
        return


@app.task(bind=True)
def handle_prefect_webhook(self, flow_run_id: str, state: str):  # skipcq: PYL-W0613
    """this is the webhook handler for prefect flow runs"""
    do_handle_prefect_webhook(flow_run_id, state)


@app.task()
def check_org_plan_expiry_notify_people():
    """sends an email to the org's account manager to notify them that their plan will expire in a week"""
    roles_to_notify = [ACCOUNT_MANAGER_ROLE]
    first_reminder = 7
    second_reminder = 2

    for org in Org.objects.all():
        org_plan = OrgPlans.objects.filter(org=org).first()
        if not org_plan or not org_plan.end_date:
            continue
        if org_plan.base_plan != OrgPlanType.FREE_TRIAL:
            continue
        # send a notification 7 days before the plan expires
        if (org_plan.end_date - datetime.now(pytz.utc)).days in [first_reminder, second_reminder]:
            try:
                org_users = OrgUser.objects.filter(
                    org=org,
                    new_role__slug__in=roles_to_notify,
                )
                message = f"""Your Dalgo plan for {org.name} will expire on {org_plan.end_date.strftime("%b %d, %Y")}. Please reach out to the Dalgo team at support@dalgo.org and renew your subscription to continue using the platform's services."""
                subject = "Dalgo plan expiry"
                for orguser in org_users:
                    send_text_message(orguser.user.email, subject, message)
            except Exception as err:
                logger.error(err)


@app.task(bind=False)
def check_for_long_running_flow_runs():
    """checks for long-running flow runs in prefect"""
    flow_runs = get_long_running_flow_runs(2)

    email_body = ""
    prefect_url = os.getenv("PREFECT_URL_FOR_NOTIFICATIONS")
    airbyte_url = os.getenv("AIRBYTE_URL_FOR_NOTIFICATIONS")

    for flow_run in flow_runs:
        logger.info(f"Found long running flow run {flow_run['id']} in prefect")

        flow_run_url = f'{prefect_url}/flow-runs/flow-run/{flow_run["id"]}'
        email_body += f"Flow Run ID: {flow_run['id']} \n"
        email_body += f"Flow Run URL: {flow_run_url} \n"

        org_slug = find_key_in_dictionary(flow_run["parameters"], "org_slug")
        if org_slug:
            email_body += f"Org: {org_slug} \n"

        tasks = find_key_in_dictionary(flow_run["parameters"], "tasks")
        if tasks:
            email_body += f"Tasks: {tasks} \n"
            for x in tasks:
                email_body += f"- {x['slug']} \n"

        connection_id = find_key_in_dictionary(flow_run["parameters"], "connection_id")
        if connection_id:
            orgtask = OrgTask.objects.filter(connection_id=connection_id).first()
            if orgtask:
                email_body += (
                    f"Org: {orgtask.org.slug} \n"  # might appear above as well, we don't care
                )
                connection_url = f"{airbyte_url}/workspaces/{orgtask.org.airbyte_workspace_id}/connections/{connection_id}"
                email_body += f"Connection URL: {connection_url} \n"
            else:
                email_body += f"Connection ID: {connection_id} \n"

        email_body += "=" * 20

    if email_body != "":
        awsses.send_text_message(
            os.getenv("ADMIN_EMAIL"),
            "Long Running Flow Runs",
            email_body,
        )


@app.task(bind=True)
def sync_single_airbyte_job_stats(self, job_id: int):
    """Syncs a single Airbyte job stats"""
    logger.info("Syncing details from airbyte for job %s", job_id)
    airbytehelpers.fetch_and_update_airbyte_job_details(job_id)


@app.task(bind=True)
def sync_airbyte_job_stats_for_all_connections(
    self, last_n_days: int = 7, last_n_hours: int = 0, connection_id: str = None, org_id: int = None
):
    """Syncs Airbyte job stats for all connections in the orgs"""

    org = None
    if org_id:
        org = Org.objects.filter(id=org_id).first()
        if not org:
            logger.error("Org with id %s not found", org_id)
            raise Exception(f"Org with id {org_id} not found")

    airbytehelpers.fetch_and_update_airbyte_jobs_for_all_connections(
        last_n_days=last_n_days, last_n_hours=last_n_hours, connection_id=connection_id, org=org
    )


@app.task()
def compute_dataflow_run_times(org: Org = None):
    """Computes run times for all dataflows"""
    dataflows = OrgDataFlowv1.objects

    if org:
        dataflows = dataflows.filter(org=org)

    for dataflow in dataflows.all():
        compute_dataflow_run_times_from_history(dataflow)


@app.task()
def flush_blacklisted_tokens():
    """Flush expired tokens from the blacklist app"""
    call_command("flushexpiredtokens")


@app.on_after_finalize.connect
def setup_periodic_tasks(sender: Celery, **kwargs):
    """periodic celery tasks"""
    # schema change detection; once a day
    sender.add_periodic_task(
        crontab(
            hour=get_integer_env_var("SCHEMA_CHANGE_DETECTION_SCHEDULE_HOUR", 18, logger, False),
            minute=get_integer_env_var(
                "SCHEMA_CHANGE_DETECTION_SCHEDULE_MINUTE", 30, logger, False
            ),
        ),
        schema_change_detection.s(),
        name="schema change detection",
    )

    # clear canvas locks every; every 60 seconds or 1 minute
    sender.add_periodic_task(60 * 1.0, delete_old_canvaslocks.s(), name="remove old canvaslocks")

    # sync flow runs of deployment; every 6 hours
    sender.add_periodic_task(
        crontab(minute=0, hour="*/6"),
        sync_flow_runs_of_deployments.s(),
        name="sync flow runs of deployments into our db",
    )

    if os.getenv("ADMIN_EMAIL"):
        # check for long running flow runs; every 3600 seconds or 1 hour
        sender.add_periodic_task(
            3600 * 1.0,
            check_for_long_running_flow_runs.s(),
            name="check for long-running flow-runs",
        )

    # check org plan expiry & notify users; daily at midnight
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        check_org_plan_expiry_notify_people.s(),
        name="check org plan expiry and notify the right people",
    )

    # flush expired blacklisted tokens every 24 hours
    # this is a custom command in the token_blacklist app
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        flush_blacklisted_tokens.s(),
        name="flush expired blacklisted tokens",
    )

    # compute run times for each deployment; every 3 hours
    if not os.getenv("ESTIMATE_TIME_FOR_QUEUE_RUNS", "false").lower() == "true":
        sender.add_periodic_task(
            crontab(minute=0, hour="*/3"),
            compute_dataflow_run_times.s(),
            name="compute run times of each deployment based on its past flow runs",
        )

    # sync airbyte job stats for connections; every 24 hours
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        sync_airbyte_job_stats_for_all_connections.s(last_n_days=2),
        name="sync airbyte job stats for all connections",
    )
