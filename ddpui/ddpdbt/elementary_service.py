"""functions to set up elementary"""

import os
import re
from pathlib import Path
import subprocess
from uuid import uuid4
from datetime import datetime, timedelta
import yaml
from ninja.errors import HttpError
from django.utils import timezone as djantotimezone

from ddpui import settings
from ddpui.utils.s3_utils import download_file, list_objects
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgDataFlowv1

from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.constants import TASK_GENERATE_EDR, TASK_GITCLONE
from ddpui.core.pipelinefunctions import (
    setup_edr_send_report_task_config,
    setup_git_clone_shell_task_config,
)
from ddpui.ddpdbt.dbthelpers import write_dbt_profiles_yml
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpprefect import prefect_service
from ddpui.utils.helpers import generate_hash_id, compare_semver
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.utils.timezone import as_ist
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def make_edr_report_s3_path(org: Org, report_date: datetime | None = None):
    """S3 path for the elementary report on a given date (default: today)."""
    date_str = (report_date or datetime.today()).strftime("%Y-%m-%d")
    return f"reports/{org.slug}.{date_str}.html"


# Days to walk back looking for the most recent Elementary report in S3 when
# today's isn't there yet. If nothing's landed in this window, EDR is likely
# broken (or the org just set up) — surface empty state so the user notices.
EDR_REPORT_LOOKBACK_DAYS = 3


def get_edr_schedule(org: Org) -> dict | None:
    """Return the cron schedule for this org's EDR send-report deployment,
    or None if not configured."""
    edr_orgtask = OrgTask.objects.filter(org=org, task__slug=TASK_GENERATE_EDR).first()
    if edr_orgtask is None:
        return None
    dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=edr_orgtask).first()
    if dataflow_orgtask is None or dataflow_orgtask.dataflow is None:
        return None
    return {"cron": dataflow_orgtask.dataflow.cron}


def elementary_setup_status(org: Org) -> dict:
    """returns if elementary setup is complete"""
    if org.dbt is None:
        return {"error": "dbt is not configured for this client"}

    project_dir = Path(DbtProjectManager.get_dbt_project_dir(org.dbt))

    if not os.path.exists(project_dir / "elementary_profiles/profiles.yml"):
        return {"status": "not-set-up"}

    orgtask = OrgTask.objects.filter(org=org, task__slug=TASK_GENERATE_EDR).first()
    if orgtask:
        dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=orgtask).first()
        if dataflow_orgtask and dataflow_orgtask.dataflow:
            logger.info(f"Generate edr deployment found for org {org.slug}")
            return {"status": "set-up"}

    return {"status": "not-set-up"}


def get_elementary_target_schema(dbt_project_yml: str) -> dict | None:
    """{'schema': 'elementary'} or {'+schema': 'elementary'}"""
    with open(dbt_project_yml, "r", encoding="utf-8") as dbt_project_yml_f:  # skipcq: PTC-W6004
        dbt_project_obj = yaml.safe_load(dbt_project_yml_f)
        if "elementary" not in dbt_project_obj["models"]:
            return None
        if "schema" in dbt_project_obj["models"]["elementary"]:
            return {"schema": dbt_project_obj["models"]["elementary"]["schema"]}
        if "+schema" in dbt_project_obj["models"]["elementary"]:
            return {"+schema": dbt_project_obj["models"]["elementary"]["+schema"]}
        return None


def get_elementary_package_version(packages_yml: str) -> dict | None:
    """{'package': 'elementary-data/elementary', 'version': '0.15.2'}"""
    with open(packages_yml, "r", encoding="utf-8") as packages_yml_f:  # skipcq: PTC-W6004
        packages_obj = yaml.safe_load(packages_yml_f)
        if (
            packages_obj is not None
            and "packages" in packages_obj
            and isinstance(packages_obj["packages"], list)
        ):
            for package in packages_obj["packages"]:
                if package["package"] == "elementary-data/elementary":
                    return package
    return None


def check_dbt_files(org: Org):
    """checks for the existence of the required lines in dbt_project.yml and packages.yml"""
    if org.dbt is None:
        return "dbt is not configured for this client"

    dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

    dbt_project_yml = Path(dbt_project_params.project_dir) / "dbt_project.yml"
    packages_yml = Path(dbt_project_params.project_dir) / "packages.yml"

    if not dbt_project_yml.exists():
        return str(dbt_project_yml) if settings.DEBUG else "dbt_project.yml not found", None

    if not packages_yml.exists():
        return str(packages_yml) if settings.DEBUG else "packages.yml not found", None

    elementary_package = get_elementary_package_version(packages_yml)
    elementary_target_schema = get_elementary_target_schema(dbt_project_yml)
    latest_elementary_package_version = os.getenv("LATEST_ELEMENTARY_PACKAGE_VERSION", "0.16.1")

    retval = {"exists": {}, "missing": {}}

    if elementary_package is not None:
        retval["exists"]["elementary_package"] = elementary_package
        if (
            latest_elementary_package_version
            and compare_semver(elementary_package["version"], latest_elementary_package_version) < 0
        ):
            retval["exists"]["elementary_package"][
                "needs_upgrade"
            ] = latest_elementary_package_version
    else:
        retval["missing"][
            "elementary_package"
        ] = f"""
            # Add this to packages.yml file
            packages:
            - package: elementary-data/elementary
                version: {latest_elementary_package_version}
                ## Docs: https://docs.elementary-data.com
        """

    if elementary_target_schema is not None:
        retval["exists"]["elementary_target_schema"] = elementary_target_schema
    else:
        retval["missing"][
            "elementary_target_schema"
        ] = """models:
        # Add this to dbt_project.yml file
        ## see docs: https://docs.elementary-data.com/
        elementary:
            ## elementary models will be created in the schema '<your_schema>_elementary'
            +schema: "elementary"
            ## To disable elementary for dev, uncomment this:
            # enabled: "{{ target.name in ['prod','analytics'] }}"

        # Required from dbt 1.8 and above for certain Elementary features
        flags:
        require_explicit_package_overrides_for_builtin_materializations: False
        source_freshness_run_project_hooks: True
        """

    # logger.info(retval)
    return None, retval


_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def extract_profile_from_generate_elementary_cli_profile(lines: list[str]):
    """skips the first few lines of the output until the profile yaml begins"""
    buffer = ""
    gather = False
    for line in lines:
        line = _ANSI_ESCAPE.sub("", line)
        if line == "elementary:":
            gather = True
        if gather:
            # a non-empty non-indented line after the first means we've hit a
            # dbt log/warning — the YAML block is done
            if buffer and line and not line[0].isspace():
                break
            buffer += line + "\n"

    if buffer == "":
        logger.error(
            "macro elementary.generate_elementary_cli_profile returned nothing\n" + "\n".join(lines)
        )
        return {"error": "macro elementary.generate_elementary_cli_profile returned nothing"}, None

    elementary_profile = yaml.safe_load(buffer)
    logger.info(elementary_profile)  # safe since there are no secrets here
    return None, elementary_profile


def create_elementary_profile(org: Org):
    """creates elementary's dbt profile"""
    if org.dbt is None:
        return {"error": "dbt is not configured for this client"}

    dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

    # Ensure profiles.yml exists on disk — elementary CLI reads it to shape its
    # own profile. Build it from warehouse creds if missing (same source-of-truth
    # as the runner-flow Secret block).
    dbt_profile_file = Path(dbt_project_params.project_dir) / "profiles/profiles.yml"
    if not os.path.exists(dbt_profile_file):
        logger.info("profiles.yml missing; generating from warehouse creds")
        write_dbt_profiles_yml(org)

    with open(dbt_profile_file, "r", encoding="utf-8") as dbt_profile_file_f:
        dbt_profile = yaml.safe_load(dbt_profile_file_f)
        logger.info("read dbt profile from %s", dbt_profile_file)

    # now we have to fix up the auth section by copying the dbt profile's auth section
    r = subprocess.check_output(
        [
            dbt_project_params.dbt_binary,
            "run-operation",
            "elementary.generate_elementary_cli_profile",
            f"--profiles-dir={Path(dbt_project_params.project_dir) / 'profiles'}",
        ],
        cwd=dbt_project_params.project_dir,
        text=True,
    )

    error, elementary_profile = extract_profile_from_generate_elementary_cli_profile(r.split("\n"))
    if error:
        return error

    # get the profile from dbt_project.yaml
    dbt_project_filename = str(Path(dbt_project_params.project_dir) / "dbt_project.yml")
    if not os.path.exists(dbt_project_filename):
        raise HttpError(400, dbt_project_filename + " is missing")

    with open(dbt_project_filename, "r", encoding="utf-8") as dbt_project_file:
        dbt_project = yaml.safe_load(dbt_project_file)
        if "profile" not in dbt_project:
            raise HttpError(400, "could not find 'profile:' in dbt_project.yml")

    dbt_profile_name = dbt_project["profile"]

    # elementary_target: what the macro emitted (used to index into the macro output)
    elementary_target = elementary_profile["elementary"].get("target", "default")

    # Extract elementary's schema from the macro output. BQ emits it under
    # `dataset` (BQ terminology), postgres/snowflake under `schema`.
    if elementary_profile["elementary"]["outputs"][elementary_target]["type"] == "bigquery":
        elementary_schema = elementary_profile["elementary"]["outputs"][elementary_target][
            "dataset"
        ]
    else:
        elementary_schema = elementary_profile["elementary"]["outputs"][elementary_target]["schema"]

    # dbt_target: the target configured in the dbt profile on disk — this is
    # the source of truth for warehouse credentials. The elementary macro may
    # emit a different target name (e.g. "default") when the dbt profile uses
    # a custom one, so we must look up creds by the dbt profile's own target.
    dbt_target = dbt_profile[dbt_profile_name].get("target", elementary_target)
    dbt_output = dbt_profile[dbt_profile_name]["outputs"][dbt_target]

    # Base = dbt's output (all warehouse params — host/user/password/port/…).
    # Override the schema with elementary's own, so elementary writes to its
    # dedicated schema while reusing the same warehouse connection.
    elementary_profile["elementary"]["outputs"][elementary_target] = {
        **dbt_output,
        "schema": elementary_schema,
    }

    elementary_profile_dir = Path(dbt_project_params.project_dir) / "elementary_profiles"

    if not elementary_profile_dir.exists():
        elementary_profile_dir.mkdir()

    elementary_profile_file = elementary_profile_dir / "profiles.yml"
    with open(elementary_profile_file, "w", encoding="utf-8") as elementary_profile_file_f:
        yaml.dump(elementary_profile, elementary_profile_file_f)

    logger.info("wrote elementary profile to %s", elementary_profile_file)

    return {"status": "success"}


def fetch_elementary_report(org: Org):
    """Fetch a previously generated Elementary report from S3.

    Returns (error, result):
      - No dbt / no elementary set up → (error_str, None)  — real preconditions
      - No report in S3 yet (expected empty state) → (None, {"report_exists": False})
      - Report present → (None, {"report_exists": True, "token", ...})
      - S3 download failure → (error_str, None)  — genuine failure
    """
    if org.dbt is None:
        return "dbt is not configured for this client", None

    project_dir = Path(DbtProjectManager.get_dbt_project_dir(org.dbt))

    if not os.path.exists(project_dir / "elementary_profiles"):
        return "set up elementary profile first", None

    bucket = os.getenv("ELEMENTARY_S3_BUCKET")
    schedule = get_edr_schedule(org)

    # Find the newest report within the lookback window in a single S3 API
    # call. Our key format `reports/<slug>.<YYYY-MM-DD>.html` puts ISO dates
    # in lexicographical = chronological order, so:
    #   - Prefix narrows to this org's reports
    #   - StartAfter is a sentinel one day older than the cutoff — S3 returns
    #     only keys strictly greater than this, so keys for the last
    #     EDR_REPORT_LOOKBACK_DAYS days come through
    #   - MaxKeys caps the response (window is small, ~3 keys expected)
    # The last entry in the returned list is the newest report.
    today = datetime.today()
    prefix = f"reports/{org.slug}."
    cutoff_sentinel = (
        f"{prefix}{(today - timedelta(days=EDR_REPORT_LOOKBACK_DAYS)).strftime('%Y-%m-%d')}"
    )

    try:
        contents = list_objects(bucket, prefix=prefix, start_after=cutoff_sentinel, max_keys=10)
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.error("failed to list elementary reports: %s", err)
        return "error fetching elementary report", None

    if not contents:
        return None, {"report_exists": False, "schedule": schedule}

    latest = contents[-1]
    bucket_file_path = latest["Key"]

    try:
        s3response = download_file(bucket, bucket_file_path)
        logger.info("fetched s3response for %s", bucket_file_path)
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.error("failed to download elementary report: %s", err)
        return "error fetching elementary report", None

    report_html = s3response["Body"].read().decode("utf-8")
    htmlfilename = str(project_dir / "elementary-report.html")
    with open(htmlfilename, "w", encoding="utf-8") as indexfile:
        indexfile.write(report_html)
        indexfile.close()
    logger.info("wrote elementary report to %s", htmlfilename)

    redis = RedisClient.get_instance()
    token = uuid4()
    redis_key = f"elementary-report-{token.hex}"
    redis.set(redis_key, htmlfilename.encode("utf-8"), 600)
    logger.info("created redis key %s", redis_key)

    return None, {
        "report_exists": True,
        "token": token.hex,
        "created_on_utc": s3response["LastModified"].isoformat(),  # e.g. 2024-06-07T00:44:08+00:00
        "created_on_ist": as_ist(
            s3response["LastModified"]
        ).isoformat(),  # e.g. 2024-06-07T06:14:08+05:30
        "schedule": schedule,
    }


def refresh_elementary_report_via_prefect(orguser: OrgUser) -> dict:
    """refreshes the elementary report for the current date using the prefect deployment"""
    org: Org = orguser.org
    orgtask = OrgTask.objects.filter(org=org, task__slug=TASK_GENERATE_EDR).first()
    if orgtask is None:
        return {"error": "orgtask generate-edr not found for " + org.slug}
    datafloworgtask = DataflowOrgTask.objects.filter(orgtask=orgtask).first()
    if datafloworgtask is None:
        return {"error": "datafloworgtask not found for " + org.slug}
    odf = datafloworgtask.dataflow

    if odf is None:
        return {"error": "pipeline not found"}

    locks = prefect_service.lock_tasks_for_deployment(odf.deployment_id, orguser)

    try:
        res = prefect_service.create_deployment_flow_run(odf.deployment_id)
        for tasklock in locks:
            tasklock.flow_run_id = res["flow_run_id"]
            tasklock.save()
        PrefectFlowRun.objects.create(
            deployment_id=odf.deployment_id,
            flow_run_id=res["flow_run_id"],
            name=res.get("name", ""),
            start_time=None,
            expected_start_time=djantotimezone.now(),
            total_run_time=-1,
            status="Scheduled",
            state_name="Scheduled",
            retries=0,
            orguser=orguser,
        )

    except Exception as error:
        for task_lock in locks:
            logger.info("deleting TaskLock %s", task_lock.orgtask.task.slug)
            task_lock.delete()
        logger.exception(error)
        raise HttpError(400, "failed to start a run") from error

    return res


def get_dbt_version(org: Org):
    """get dbt version"""
    if not org.dbt:
        return "Not available"
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
        dbt_version_command = [str(dbt_project_params.dbt_binary), "--version"]
        dbt_output = subprocess.check_output(dbt_version_command, text=True)
        for line in dbt_output.splitlines():
            if "installed:" in line:
                return line.split(":")[1].strip()
        return "Not available"
    except Exception as err:
        logger.info("Error getting dbt version: %s", err)
        return "Not available"


def get_edr_version(org: Org):
    """get elementary report version"""
    if not org.dbt:
        return "Not available"
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
        elementary_version_command = [
            os.path.join(dbt_project_params.venv_binary, "edr"),
            "--version",
        ]
        elementary_output = subprocess.check_output(elementary_version_command, text=True)
        for line in elementary_output.splitlines():
            if line.startswith("Elementary version"):
                return line.split()[-1].strip()[:-1]
        return "Not available"
    except Exception as err:
        logger.info("Error getting elementary version: %s", err)
        return "Not available"


def ensure_edr_sendreport_dataflow(org: Org, cron: str):
    """Create the EDR send-report Prefect dataflow for the org. Idempotent:
    if the dataflow already exists, returns success without touching Prefect.
    Ensures the underlying OrgTask exists too — creates it if missing.
    To change the schedule or task_config, delete the existing dataflow and
    re-run.

    On EKS (edr_queue.is_workpool_eks=True) a git-clone task is prepended
    because each pod starts with an empty filesystem — the dbt project must
    be cloned before _prepare_elementary_profile can run.
    """
    from ddpui.core.orgtaskfunctions import get_edr_send_report_task
    from ddpui.models.org import OrgPrefectBlockv1
    from ddpui.ddpprefect import SECRET
    from ddpui.models.tasks import Task

    dbt_project_params: DbtProjectParams = None
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
    except Exception as error:
        logger.error(error)
        return None

    org_task = get_edr_send_report_task(org) or get_edr_send_report_task(org, create=True)
    if org_task is None:
        return {"error": "could not get or create EDR send-report OrgTask"}

    edr_queue = org.get_queue_config().edr_queue
    tasks = []

    if getattr(edr_queue, "is_workpool_eks", False):
        # EKS pods start with an empty filesystem — clone the repo first so
        # _prepare_elementary_profile can find dbt_project.yml and packages.yml.
        git_clone_task = Task.objects.filter(slug=TASK_GITCLONE).first()
        if git_clone_task is None:
            return {"error": "git-clone task not found in database"}

        git_clone_orgtask, _ = OrgTask.objects.get_or_create(
            org=org, task=git_clone_task, dbt=org.dbt, defaults={"parameters": {}}
        )
        gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=SECRET, block_name__contains="git-pull"
        ).first()
        git_clone_config = setup_git_clone_shell_task_config(
            git_clone_orgtask,
            dbt_project_params.clients_base_dir,
            dbt_project_params.project_dir_relative,
            gitpull_secret_block,
            seq=0,
            gitrepo_url=org.dbt.gitrepo_url or "",
        ).to_json()
        tasks.append(git_clone_config)
        logger.info(f"EKS edr deployment: prepending git-clone step for {org.slug}")

    edr_task_config = setup_edr_send_report_task_config(
        org_task, dbt_project_params.project_dir, seq=len(tasks)
    )
    tasks.append(edr_task_config.to_json())

    deployment_params = {
        "config": {
            "tasks": tasks,
            "org_slug": org_task.org.slug,
        }
    }

    existing_dfot = DataflowOrgTask.objects.filter(orgtask=org_task).first()
    if existing_dfot is not None:
        orgdataflow = existing_dfot.dataflow
        prefect_service.update_dataflow_v1(
            orgdataflow.deployment_id,
            PrefectDataFlowUpdateSchema3(
                cron=cron,
                deployment_params=deployment_params,
            ),
        )
        orgdataflow.cron = cron
        orgdataflow.save(update_fields=["cron"])
        logger.info(f"updated EDR dataflow {orgdataflow.name} for {org.slug}")
        return {"status": "success", "dataflow": orgdataflow.name, "updated": True}

    hash_code = generate_hash_id(8)
    deployment_name = f"pipeline-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"
    logger.info(f"creating deployment {deployment_name}")

    dataflow = prefect_service.create_dataflow_v1(
        PrefectDataFlowCreateSchema3(
            deployment_name=deployment_name,
            flow_name=deployment_name,
            orgslug=org_task.org.slug,
            deployment_params=deployment_params,
            cron=cron,
        ),
        edr_queue,
    )

    logger.info(
        f"creating OrgDataFlowv1 named {dataflow['deployment']['name']} with deployment_id {dataflow['deployment']['id']}"
    )
    orgdataflow = OrgDataFlowv1.objects.create(
        org=org,
        name=dataflow["deployment"]["name"],
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        dataflow_type="manual",
        cron=cron,
    )
    DataflowOrgTask.objects.create(dataflow=orgdataflow, orgtask=org_task)
    return {"status": "success", "dataflow": orgdataflow.name}
