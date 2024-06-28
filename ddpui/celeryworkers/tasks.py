import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from subprocess import CompletedProcess

import yaml
from celery.schedules import crontab
from django.utils.text import slugify
from ddpui.auth import ACCOUNT_MANAGER_ROLE, PIPELINE_MANAGER_ROLE
from ddpui.celery import app
from ddpui.ddpairbyte.airbyte_service import abreq
from ddpui.utils.sendgrid import send_schema_changes_email, send_email_notification
from ddpui.utils.timezone import UTC
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org import (
    Org,
    OrgDbt,
    OrgSchemaChange,
    OrgWarehouse,
    OrgPrefectBlockv1,
    OrgDataFlowv1,
    TransformType,
)
from ddpui.models.notifications import NotificationRecipient
from django.utils import timezone
from ddpui.utils.discord import send_discord_notification
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import TaskLock, OrgTask, TaskProgressHashPrefix
from ddpui.models.canvaslock import CanvasLock
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.llm import AssistantPrompt, LlmAssistantType, LlmSession
from ddpui.utils.helpers import runcmd, runcmd_with_output, subprocess
from ddpui.utils import secretsmanager
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.ddpprefect.prefect_service import (
    update_dbt_core_block_schema,
    get_dbt_cli_profile_block,
    prefect_get,
    get_flow_run_logs_v2,
)
from ddpui.utils.constants import (
    TASK_DBTRUN,
    TASK_DBTCLEAN,
    TASK_DBTDEPS,
    TASK_AIRBYTESYNC,
)
from ddpui.ddpprefect import DBTCLIPROFILE, FLOW_RUN_FAILED, FLOW_RUN_CRASHED
from ddpui.core import llm_service

logger = CustomLogger("ddpui")


@app.task(bind=True)
def clone_github_repo(
    self,
    org_slug: str,
    gitrepo_url: str,
    gitrepo_access_token: str | None,
    project_dir: str,
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

    project_dir: Path = Path(project_dir)
    dbtrepo_dir = project_dir / "dbtrepo"
    if not project_dir.exists():
        project_dir.mkdir()
        taskprogress.add(
            {
                "message": "created project_dir",
                "status": "running",
            }
        )
        logger.info("created project_dir %s", project_dir)

    elif dbtrepo_dir.exists():
        shutil.rmtree(str(dbtrepo_dir))

    cmd = f"git clone {gitrepo_url} dbtrepo"

    try:
        runcmd(cmd, project_dir)
    except Exception as error:
        taskprogress.add(
            {
                "message": "git clone failed",
                "error": str(error),
                "status": "failed",
            }
        )
        logger.exception(error)
        return False

    taskprogress.add(
        {
            "message": "cloned git repo",
            "status": "running" if child else "completed",
        }
    )
    return True


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
    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug

    # four parameters here is correct despite vscode thinking otherwise
    if not clone_github_repo(
        org.slug,
        payload["gitrepoUrl"],
        payload["gitrepoAccessToken"],
        str(project_dir),
        taskprogress,
    ):
        raise Exception("Failed to clone git repo")

    logger.info("git clone succeeded for org %s", org.name)

    dbt = OrgDbt(
        gitrepo_url=payload["gitrepoUrl"],
        project_dir=str(project_dir),
        dbt_venv=os.getenv("DBT_VENV"),
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
def run_dbt_commands(self, orguser_id: int):
    """run a dbt command via celery instead of via prefect"""
    try:

        orguser: OrgUser = OrgUser.objects.filter(id=orguser_id).first()

        org: Org = orguser.org
        logger.info("found org %s", org.name)

        taskprogress = TaskProgress(
            self.request.id, f"{TaskProgressHashPrefix.RUNDBTCMDS}-{org.slug}"
        )

        taskprogress.add(
            {
                "message": "started",
                "status": "running",
            }
        )

        task_locks: list[TaskLock] = []

        # acquire locks for clean, deps and run
        org_tasks = OrgTask.objects.filter(
            org=org,
            task__slug__in=[TASK_DBTCLEAN, TASK_DBTDEPS, TASK_DBTRUN],
            generated_by="system",
        ).all()
        for org_task in org_tasks:
            task_lock = TaskLock.objects.create(
                orgtask=org_task, locked_by=orguser, celery_task_id=self.request.id
            )
            task_locks.append(task_lock)

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

        profile = get_dbt_cli_profile_block(dbt_cli_profile.block_name)["profile"]
        profile_dirname = (
            Path(os.getenv("CLIENTDBT_ROOT")) / org.slug / "dbtrepo" / "profiles"
        )
        os.makedirs(profile_dirname, exist_ok=True)
        profile_filename = profile_dirname / "profiles.yml"
        logger.info("writing dbt profile to " + str(profile_filename))
        with open(profile_filename, "w", encoding="utf-8") as f:
            yaml.safe_dump(profile, f)

        dbt_binary = Path(os.getenv("DBT_VENV")) / "venv/bin/dbt"
        project_dir = Path(orgdbt.project_dir) / "dbtrepo"

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
            taskprogress.add({"message": "starting dbt run", "status": "running"})
            process: CompletedProcess = runcmd_with_output(
                f"{dbt_binary} run --profiles-dir=profiles", project_dir
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
        # clear all locks
        for lock in task_locks:
            lock.delete()


@app.task()
def schema_change_detection():
    """detects schema changes for all the orgs and sends an email to admins if there is a change"""
    orgs = Org.objects.all()
    schema_changes = {}

    for org in orgs:
        org_conn = OrgTask.objects.filter(org=org, task__slug=TASK_AIRBYTESYNC)
        for org_task in org_conn:
            try:
                response = abreq(
                    "web_backend/connections/get",
                    {
                        "withRefreshedCatalog": True,
                        "connectionId": org_task.connection_id,
                    },
                    timeout=60,
                )

                change_type = response.get("schemaChange")
                logger.info(f"Schema change detected for org {org.name}: {change_type}")

                if change_type in ["breaking", "non_breaking"]:
                    schema_change, created = OrgSchemaChange.objects.get_or_create(
                        connection_id=org_task.connection_id,
                        defaults={"change_type": change_type, "org": org},
                    )
                    if not created:
                        # If the record already exists, update the change_type
                        schema_change.change_type = change_type
                        schema_change.save()
                    if org not in schema_changes:
                        schema_changes[org] = {"breaking": 0, "non_breaking": 0}
                    schema_changes[org][change_type] += 1
            except Exception as e:
                logger.error(f"Error checking connection for org {org.name}: {e}")
                continue

    for org in schema_changes:
        org_users = OrgUser.objects.filter(
            org=org, new_role__slug__in=[ACCOUNT_MANAGER_ROLE, PIPELINE_MANAGER_ROLE]
        )
        message = """This email is to let you know that schema changes have been detected in your Dalgo pipeline, which require your review."""
        for orguser in org_users:
            logger.info(f"sending notification email to {orguser.user.email}")
            send_schema_changes_email(org.name, orguser.user.email, message)


@app.task(bind=False)
def get_connection_catalog_task(task_key, org_id, connection_id):
    """Fetch a connection in the user organization workspace as a Celery task"""
    org = Org.objects.get(id=org_id)
    taskprogress = SingleTaskProgress(
        task_key, int(os.getenv("SCHEMA_REFRESH_TTL", "180"))
    )
    taskprogress.add(
        {
            "message": "started",
            "status": "running",
        }
    )

    res, error = airbytehelpers.get_connection_catalog(org, connection_id)
    if error:
        logger.error(
            "unable to fetch schema catalog for %s %s", org.slug, connection_id
        )
        taskprogress.add(
            {
                "message": "unable to fetch catalog response",
                "status": "failed",
            }
        )

    taskprogress.add(
        {"message": "fetched catalog data", "status": "completed", "result": res}
    )
    return res


@app.task(bind=False)
def create_elementary_report(task_key: str, org_id: int, bucket_file_path: str):
    """run edr report to create the elementary report and write to s3"""
    taskprogress = SingleTaskProgress(task_key, int(os.getenv("EDR_TTL", "180")))

    edr_binary = Path(os.getenv("DBT_VENV")) / "venv/bin/edr"
    org = Org.objects.filter(id=org_id).first()
    orgdbt = OrgDbt.objects.filter(org=org).first()
    project_dir = Path(orgdbt.project_dir) / "dbtrepo"
    profiles_dir = project_dir / "elementary_profiles"
    aws_access_key_id = os.getenv("ELEMENTARY_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("ELEMENTARY_AWS_SECRET_ACCESS_KEY")
    s3_bucket_name = os.getenv("ELEMENTARY_S3_BUCKET")

    os.environ["PATH"] += ":" + str(Path(os.getenv("DBT_VENV")) / "venv/bin")
    cmd = [
        str(edr_binary),
        "send-report",
        "--aws-access-key-id",
        aws_access_key_id,
        "--aws-secret-access-key",
        aws_secret_access_key,
        "--s3-bucket-name",
        s3_bucket_name,
        "--bucket-file-path",
        bucket_file_path,
        "--profiles-dir",
        str(profiles_dir),
    ]
    taskprogress.add(
        {
            "message": "started",
            "status": "running",
        }
    )
    try:
        runcmd(" ".join(cmd), project_dir)
    except subprocess.CalledProcessError:
        taskprogress.add(
            {
                "message": "edr failed",
                # "error": str(error), # error contains the aws secrets
                "status": "failed",
            }
        )
        # logger.exception(error)  # error contains the aws secrets
        return
    taskprogress.add(
        {
            "message": "generated edr report",
            "status": "completed",
        }
    )


@app.task(bind=False)
def update_dbt_core_block_schema_task(block_name, default_schema):
    """single http PUT request to the prefect-proxy"""
    logger.info("updating default_schema of %s to %s", block_name, default_schema)
    update_dbt_core_block_schema(block_name, default_schema)


@app.task()
def delete_old_tasklocks():
    """delete task locks which were created over an hour ago"""
    onehourago = UTC.localize(datetime.now() - timedelta(seconds=3600))
    TaskLock.objects.filter(locked_at__lt=onehourago).delete()


@app.task()
def delete_old_canvaslocks():
    """delete canvas locks which were created over 10 minutes ago"""
    tenminutesago = UTC.localize(datetime.now() - timedelta(seconds=600))
    CanvasLock.objects.filter(locked_at__lt=tenminutesago).delete()


@app.task(bind=True)
def sync_flow_runs_of_deployments(self, deployment_ids: list[str] = None):
    """
    This function will sync (create) latest flow runs of deployment(s) if missing from our db
    """

    query = OrgDataFlowv1.objects
    if deployment_ids:
        query = query.filter(deployment_id__in=deployment_ids)

    # sync recent 50 flow runs of each deployment
    start_time_gt = UTC.localize(datetime.now() - timedelta(hours=24))
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
                if not PrefectFlowRun.objects.filter(
                    flow_run_id=flow_run["id"]
                ).exists():
                    if flow_run["startTime"] in ["", None]:
                        flow_run["startTime"] = flow_run["expectedStartTime"]
                    PrefectFlowRun.objects.create(
                        deployment_id=deployment_id,
                        flow_run_id=flow_run["id"],
                        name=flow_run["name"],
                        start_time=flow_run["startTime"],
                        expected_start_time=flow_run["expectedStartTime"],
                        total_run_time=flow_run["totalRunTime"],
                        status=flow_run["status"],
                        state_name=flow_run["state_name"],
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
def summarize_airbyte_logs(
    self,
    connection_id: str,
    orguser_id: str,
    job_id: int,
    attempt_number: int = 0,
    regenerate: bool = False,
):
    """
    Summarize airbyte logs
    """
    taskprogress = SingleTaskProgress(self.request.id, 60 * 10)

    taskprogress.add({"message": "Started", "status": "running", "result": []})

    orguser = OrgUser.objects.filter(id=orguser_id).first()

    try:
        job_info = airbyte_service.get_job_info(str(job_id))

        if job_info["job"]["configId"] != connection_id:
            taskprogress.add(
                {
                    "message": "Invalid job id",
                    "status": "failed",
                    "result": None,
                }
            )
            return
    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": "Failed to fetch the sync job",
                "status": "failed",
                "result": None,
            }
        )
        return

    if not regenerate:
        # try to fetch response from db
        llm_session = LlmSession.objects.filter(
            orguser=orguser, org=orguser.org, airbyte_job_id=job_id
        ).first()

        if llm_session:
            taskprogress.add(
                {
                    "message": "Retrieved saved summary for the run",
                    "status": "completed",
                    "result": llm_session.response,
                }
            )
            return

    logs = airbyte_service.get_logs_for_job(
        job_id=job_id, attempt_number=attempt_number
    )

    taskprogress.add(
        {
            "message": "Fetched logs need for summarization",
            "status": "running",
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

        # upload logs for the task
        logs_text = "\n".join(logs["logs"]["logLines"])
        fpath = llm_service.upload_text_as_file(logs_text, f"{job_id}_{attempt_number}")
        logger.info("Uploaded file successfully to LLM service at " + str(fpath))

        # start a file search session in the llm service
        logger.info(f"Querying the uploaded file: total queries {len(user_prompts)}")
        result = llm_service.file_search_query_and_poll(
            fpath, assistant_prompt.prompt, user_prompts
        )
        summary = "\n\n".join(result["result"])  # merge the query results

        # close the session
        logger.info("Closing the session")
        llm_service.close_file_search_session(result["session_id"])

        llm_session = LlmSession.objects.create(
            orguser=orguser,
            org=orguser.org,
            airbyte_job_id=job_id,
            assistant_prompt=assistant_prompt.prompt,
            user_prompts=user_prompts,
            response={
                "job_id": job_id,
                "attempt_number": attempt_number,
                "summary": summary,
            },
            session_id=result["session_id"],
        )

        logger.info("Completed log summarization for airbyte logs")
        taskprogress.add(
            {
                "message": "Generated summary for the run",
                "status": "completed",
                "result": llm_session.response,
            }
        )
    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": str(err),
                "status": "failed",
                "result": None,
            }
        )


@app.task(bind=True)
def summarize_deployment_flow_run_logs(
    self, flow_run_id: str, orguser_id: str, regenerate: bool = False
):
    """
    For subtask or subflowrun in the flow run; this function will
    1. Fetch all subtasks or subflowruns from prefect along with their logs
    2. Upload logs as a file to llm service
    3. Query the llm service with two prompts one for the summarizing & other for figuring out how to resolve errors
    4. Only summarize failures and dbt tasks (deps, clean, run, test)

    If regenerate is True and the summary is not found, the program will generate it again & return
    """
    taskprogress = SingleTaskProgress(self.request.id, 60 * 10)

    taskprogress.add({"message": "Started", "status": "running", "result": []})

    orguser = OrgUser.objects.filter(id=orguser_id).first()

    if not regenerate:
        # try to fetch response from db
        llm_sessions = LlmSession.objects.filter(
            orguser=orguser, org=orguser.org, flow_run_id=flow_run_id
        )

        if llm_sessions.count() > 0:
            taskprogress.add(
                {
                    "message": "Retrieved saved summary for the run",
                    "status": "completed",
                    "result": [llm_session.response for llm_session in llm_sessions],
                }
            )
            return

    all_task_logs = get_flow_run_logs_v2(flow_run_id)
    dbt_failed_tasks = [
        task
        for task in all_task_logs
        if (
            task["state_type"] in [FLOW_RUN_CRASHED, FLOW_RUN_FAILED]
            or task["state_name"] == "DBT_TEST_FAILED"
        )
        and task["kind"] == "task-run"
        and "dbt-" in task["label"]
    ]

    taskprogress.add(
        {
            "message": "Fetched all logs for subtasks",
            "status": "running",
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

        summary_result = []
        for task in dbt_failed_tasks:
            logger.info(
                f"Uploading logs for flow_run_id : {flow_run_id} and for task : {task['label']} to llm service"
            )

            # upload logs for the task
            logs_text = "\n".join([log["message"] for log in task["logs"]])
            fpath = llm_service.upload_text_as_file(logs_text, f"{task['label']}_logs")
            logger.info("Uploaded file successfully to LLM service at " + str(fpath))

            # start a file search session in the llm service
            logger.info(
                f"Querying the uploaded file: total queries {len(user_prompts)}"
            )
            result = llm_service.file_search_query_and_poll(
                fpath, assistant_prompt.prompt, user_prompts
            )
            task["summary"] = "\n\n".join(result["result"])  # merge the query results

            # close the session
            logger.info("Closing the session")
            llm_service.close_file_search_session(result["session_id"])

            del task["logs"]

            llm_session = LlmSession.objects.create(
                orguser=orguser,
                org=orguser.org,
                flow_run_id=flow_run_id,
                assistant_prompt=assistant_prompt.prompt,
                user_prompts=user_prompts,
                response=task,  # {"id": ... ,"label": ..., "summary": ... }
                session_id=result["session_id"],
            )
            summary_result.append(llm_session.response)

        logger.info("Completed log summarization for all failed tasks")
        taskprogress.add(
            {
                "message": "Generated summary for the run",
                "status": "completed",
                "result": summary_result,
            }
        )
    except Exception as err:
        logger.error(err)
        taskprogress.add(
            {
                "message": str(err),
                "status": "failed",
                "result": None,
            }
        )


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    """check for old locks every minute"""
    sender.add_periodic_task(
        crontab(hour=18, minute=30),
        schema_change_detection.s(),
        name="schema change detection",
    )
    sender.add_periodic_task(
        60 * 1.0, delete_old_tasklocks.s(), name="remove old tasklocks"
    )
    sender.add_periodic_task(
        60 * 1.0, delete_old_canvaslocks.s(), name="remove old canvaslocks"
    )

@app.task()
def schedule_notification_task(notification, recipient, user_preference):
    if user_preference.enable_email_notifications:
        send_email_notification(user_preference.email, notification.message)
    if user_preference.enable_discord_notifications and user_preference.discord_webhook:
        try:
            send_discord_notification(user_preference.discord_webhook, notification.message)
        except Exception as e:
            raise Exception(f"Error sending discord notification: {str(e)}")
    
    NotificationRecipient.objects.filter(notification=notification, recipient=recipient).update(read_status=True)
    notification.sent_time = timezone.now()
    notification.save()