import os
import tempfile
from dataclasses import dataclass, field

from django.conf import settings
from django.contrib.auth.models import User

from ddpui.models.org import Org, OrgWarehouse
from ddpui.core.trial.exceptions import TrialAccountExistsError
from ddpui.core.trial.timing import step_timer
from ddpui.core.trial.warehouse_provision import (
    provision_trial_database,
    drop_trial_database,
    email_hash8,
)
from ddpui.core.trial.warehouse_data import copy_warehouse_data
from ddpui.services.org_cleanup_service import OrgCleanupService
from ddpui.core.orgfunctions import create_organization, create_org_plan
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.schemas.org_warehouse_schema import OrgWarehouseSchema
from ddpui.models.org_plans import OrgPlanType
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.airbytehelpers import create_warehouse
from ddpui.ddpairbyte.airbytehelpers import create_connection as ab_create_connection
from ddpui.ddpairbyte.schema import AirbyteConnectionCreate
from ddpui.core.trial.source_config import (
    load_template_source_config,
    validate_template_source_configs,
)
from ddpui.core.trial.dbt_clone import copy_dbt_dag, copy_dbt_repo_files, regenerate_and_push
from ddpui.models.dbt_workflow import OrgDbtOperation
from ddpui.core.trial.prefect_clone import clone_orchestrate_dataflows
from ddpui.core.trial.viz_clone import clone_viz
from ddpui.ddpdbt.dbt_service import setup_managed_git_workspace
from ddpui.core.orgtaskfunctions import create_default_transform_tasks
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.clone_service")


@dataclass
class CloneRun:
    """in-memory state carrier for a single template→trial clone run — no DB row.

    Per-run state (timings, manifest of created resource ids, the trial org once it
    exists) lives only for the lifetime of this call; nothing is persisted for it.
    """

    template: Org
    trial_email: str
    org_name: str | None = None
    role_slug: str | None = None
    trial_org: Org | None = None
    trial_orguser: OrgUser | None = None
    current_step: str | None = None
    timings: dict = field(default_factory=dict)
    manifest: dict = field(default_factory=dict)


STEP_LABELS = {
    1: "Creating your workspace",
    2: "Setting up your warehouse",
    3: "Copying your data",
    4: "Connecting your sources",
    5: "Building your pipelines",
    6: "Setting up transforms",
    7: "Scheduling syncs",
    8: "Preparing your dashboards",
}


def account_exists_for_email(email: str) -> bool:
    """True if a real Dalgo account exists for this email — a User WITH at least one OrgUser.

    A bare User row with zero OrgUsers (e.g. left dangling by a failed/reaped trial clone,
    since teardown/deleteorg remove the OrgUser but not the Django User) is NOT an account and
    must not block a retry.
    """
    return OrgUser.objects.filter(user__username=email).exists()


def _step_org_and_user(run: CloneRun) -> None:
    """Step 1 — create the trial org (+ Airbyte workspace + plan) and an admin user."""
    template = run.template
    # The org name/slug is derived from the EMAIL only — the email is the single unique thing a
    # trial user supplies (org_name is free-text and NOT unique: two users can both type "test").
    # `create_organization` derives org.slug = slugify(org.name)[:20], so the 8-char email hash
    # sits right after "Trial " (chars 6-14) to guarantee it survives the 20-char slug truncation
    # → the slug (and the Airbyte workspace name / Prefect block prefix built from it) is unique
    # per email. The email's local part follows the hash so the name is human-identifiable (mirrors
    # the ft_<local>_<hash>_db warehouse name). The caller-supplied org_name is NOT used.
    email_local = run.trial_email.split("@")[0]
    trial_name = f"Trial {email_hash8(run.trial_email)} {email_local} {template.name}"[:50]
    org_payload = CreateOrgSchema(
        name=trial_name,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        can_upgrade_plan=False,
        subscription_duration="trial",
        superset_included=False,
    )
    trial_org, err = create_organization(org_payload)
    if err:
        raise RuntimeError(f"create_organization failed: {err}")

    # record the teardown marker immediately — the Org + Airbyte workspace already exist at
    # this point, so any failure below must still trigger OrgCleanupService on the way out.
    run.trial_org = trial_org

    _, plan_err = create_org_plan(org_payload, trial_org)
    if plan_err:
        raise RuntimeError(f"create_org_plan failed: {plan_err}")

    # admin user — password is set later via the activation flow (Try Now), so unusable now.
    # NB: a brand-new Django User has password == "" which has_usable_password() treats as
    # "usable" (only None / the "!"-prefixed sentinel count as unusable), so we key off the
    # get_or_create `created` flag rather than has_usable_password() to decide when to reset it.
    user, created = User.objects.get_or_create(
        username=run.trial_email,
        defaults={"email": run.trial_email},
    )
    if created:
        user.set_unusable_password()
        user.save(update_fields=["password"])

    role_slug = run.role_slug or ACCOUNT_MANAGER_ROLE
    admin_role = Role.objects.filter(slug=role_slug).first()
    if admin_role is None:
        raise RuntimeError(f"role {role_slug} not found (load role fixtures)")

    orguser = OrgUser.objects.create(
        user=user, org=trial_org, new_role=admin_role, email_verified=False
    )
    run.trial_orguser = orguser
    UserAttributes.objects.get_or_create(user=user, defaults={"email_verified": False})
    UserPreferences.objects.get_or_create(
        orguser=orguser, defaults={"enable_email_notifications": True}
    )

    run.manifest["trial_org_slug"] = trial_org.slug
    run.manifest["trial_workspace_id"] = trial_org.airbyte_workspace_id
    run.manifest["trial_orguser_id"] = orguser.id
    run.manifest["custom_connectors"] = "queued_async_not_awaited"


def _step_warehouse(run: CloneRun) -> None:
    """Step 2 — provision a trial warehouse db and register it via create_warehouse."""
    template = run.template
    template_wh = OrgWarehouse.objects.filter(org=template).first()
    if template_wh is None:
        raise RuntimeError("template org has no warehouse")
    if template_wh.wtype != "postgres":
        raise RuntimeError(f"v1 supports postgres only; template is {template_wh.wtype}")

    # carry the template's non-connection config forward (schema/ssl), overriding host/db creds
    # below; also used to decide whether the template warehouse lives on the SAME trials-RDS
    # instance, which allows a fast server-side CREATE DATABASE ... TEMPLATE ... copy instead
    # of an empty db + pg_dump/restore in step 3.
    template_creds = retrieve_warehouse_credentials(template_wh) or {}
    same_instance = template_creds.get("host") == settings.TRIALS_RDS_HOST
    trial_db_params = provision_trial_database(
        run.trial_email, template_db=template_creds.get("database") if same_instance else None
    )
    run.manifest["warehouse_copied_server_side"] = same_instance

    # record the teardown marker immediately — the RDS database already exists at this point,
    # so any failure below must still trigger drop_trial_database on the way out.
    run.manifest["trial_warehouse_db"] = trial_db_params["database"]
    run.manifest["trial_warehouse_role"] = trial_db_params["username"]

    # reuse the template destination's definition id (not stored on OrgWarehouse)
    template_dest = airbyte_service.get_destination(
        template.airbyte_workspace_id, template_wh.airbyte_destination_id
    )
    dest_def_id = template_dest["destinationDefinitionId"]

    airbyte_config = dict(template_creds)
    airbyte_config.update(
        {
            "host": trial_db_params["host"],
            "port": trial_db_params["port"],
            "database": trial_db_params["database"],
            "username": trial_db_params["username"],
            "password": trial_db_params["password"],
        }
    )
    # the template's SSH-tunnel config points at the template's own bastion — the trial
    # warehouse lives on the trials-RDS host with no such tunnel, so none of this can carry
    # over. ssl_mode/schema are left as-is (same-instance measurement keeps those valid).
    for tunnel_key in (
        "tunnel_method",
        "tunnel_host",
        "tunnel_port",
        "tunnel_user",
        "ssh_key",
        "tunnel_user_password",
    ):
        airbyte_config.pop(tunnel_key, None)

    wh_payload = OrgWarehouseSchema(
        wtype="postgres",
        name=template_wh.name or "trial warehouse",
        destinationDefId=dest_def_id,
        airbyteConfig=airbyte_config,
    )
    _, err = create_warehouse(run.trial_org, wh_payload)
    if err:
        raise RuntimeError(f"create_warehouse failed: {err}")

    run.manifest["trial_destination_defid"] = dest_def_id


def _step_warehouse_data(run: CloneRun) -> None:
    """Step 3 — pg_dump the template warehouse and restore into the trial warehouse.

    Skipped entirely when step 2 already did a server-side `CREATE DATABASE ... TEMPLATE ...`
    copy (template warehouse + trials-RDS on the same host) — the trial db already has the
    template's data, so pg_dump/restore here would be redundant (and much slower).
    """
    if run.manifest.get("warehouse_copied_server_side"):
        logger.info(
            "warehouse data already copied server-side (CREATE DATABASE TEMPLATE); "
            "skipping pg_dump"
        )
        return

    template_wh = OrgWarehouse.objects.filter(org=run.template).first()
    trial_wh = OrgWarehouse.objects.filter(org=run.trial_org).first()
    if template_wh is None or trial_wh is None:
        raise RuntimeError("missing template or trial warehouse for data copy")

    src = retrieve_warehouse_credentials(template_wh)
    dst = retrieve_warehouse_credentials(trial_wh)
    if not src or not dst:
        raise RuntimeError("could not retrieve warehouse credentials for data copy")

    with tempfile.NamedTemporaryFile(suffix=".pgc", delete=False) as tmp:
        dump_path = tmp.name
    try:
        copy_warehouse_data(src, dst, dump_path)
    finally:
        # the dump is a full copy of the template warehouse's data sitting on local disk —
        # remove it regardless of success/failure so nothing is left at rest.
        if os.path.exists(dump_path):
            os.remove(dump_path)

    run.manifest["warehouse_dump_path"] = dump_path


def _step_sources(run: CloneRun) -> None:
    """Step 4 — recreate the template's Airbyte sources in the trial workspace.

    Validates that every template source has a config entry in the (gitignored)
    TEMPLATE_SOURCE_CREDS_FILE before creating anything — Airbyte masks source configs on
    read-back, so the only source of real credentials is that Dalgo-controlled store.
    """
    template_sources = airbyte_service.get_sources(run.template.airbyte_workspace_id)["sources"]
    names = [s["name"] for s in template_sources]
    missing = validate_template_source_configs(names)
    if missing:
        raise RuntimeError(f"missing source config for template sources: {missing}")

    source_map: dict = {}
    for src in template_sources:
        config = load_template_source_config(src["name"])
        created = airbyte_service.create_source(
            run.trial_org.airbyte_workspace_id, src["name"], src["sourceDefinitionId"], config
        )
        source_map[src["sourceId"]] = created["sourceId"]
    run.manifest["source_map"] = source_map
    run.manifest["source_ids"] = list(source_map.values())


def _selected_stream_names(connection: dict) -> set:
    """the set of stream names the TEMPLATE connection had selected.

    Used to mirror the template's curated scope onto the trial connection instead of
    over-syncing every stream discovered on the (freshly-created) trial source.
    """
    names = set()
    for entry in connection.get("syncCatalog", {}).get("streams", []):
        if entry.get("config", {}).get("selected"):
            names.add(entry["stream"]["name"])
    return names


def _normalize_streams_overwrite(catalog: dict, selected_names: set) -> list:
    """flatten the discovered catalog to the selection list, restricted to the streams the
    TEMPLATE connection had selected, all still forced to full_refresh|overwrite.

    A clone must mirror the template's scope: if the template curated only a subset of the
    source's streams, the trial connection must select that same subset — not every stream in
    the freshly-discovered catalog. The trial's first sync must be a full read against an empty
    Airbyte state (no prior sync history in the trial workspace) landing on top of the
    pg_dump'd warehouse rows copied in Step 3 — Full Refresh|Overwrite is the only mode that's
    safe there regardless of what sync mode the template connection used.

    If `selected_names` is empty (the template connection exposed no syncCatalog selection
    info to key off of), fall back to selecting every discovered stream so the clone never ends
    up with zero streams selected.
    """
    streams = []
    for entry in catalog.get("streams", []):
        stream = entry["stream"]
        name = stream["name"]
        if selected_names and name not in selected_names:
            continue
        streams.append(
            {
                "name": name,
                "selected": True,
                "syncMode": "full_refresh",
                "destinationSyncMode": "overwrite",
                "cursorField": [],
                "primaryKey": [],
            }
        )
    return streams


def _step_connections(run: CloneRun) -> None:
    """Step 5 — recreate connections on remapped sources; first-sync Full Refresh|Overwrite.

    Catalog ids are workspace-scoped, so the catalog is re-discovered on the NEW source rather
    than reusing the template connection's catalogId. Uses the airbytehelpers wrapper (not the
    raw airbyte_service call) so OrgTasks/dataflows/ConnectionMeta get created too.
    """
    source_map = run.manifest.get("source_map", {})
    template_conns = airbyte_service.get_webbackend_connections(run.template.airbyte_workspace_id)
    trial_ws = run.trial_org.airbyte_workspace_id
    connection_map: dict = {}
    for conn in template_conns:
        old_source_id = conn["source"]["sourceId"]
        new_source_id = source_map.get(old_source_id)
        if not new_source_id:
            raise RuntimeError(f"no remapped source for template source {old_source_id}")
        discovered = airbyte_service.get_source_schema_catalog(trial_ws, new_source_id)
        selected = _selected_stream_names(conn)
        payload = AirbyteConnectionCreate(
            name=conn["name"],
            sourceId=new_source_id,
            streams=_normalize_streams_overwrite(discovered["catalog"], selected),
            catalogId=discovered["catalogId"],
            syncCatalog=discovered["catalog"],
            destinationSchema=conn.get("namespaceFormat") or None,
        )
        res, err = ab_create_connection(run.trial_org, payload)
        if err:
            raise RuntimeError(f"create_connection failed for {conn['name']}: {err}")
        connection_map[conn["connectionId"]] = res["connectionId"]
    run.manifest["connection_map"] = connection_map
    run.manifest["connection_ids"] = list(connection_map.values())


def _step_dbt(run: CloneRun) -> None:
    """Step 6 — set up a fresh managed dbt workspace on the trial org, deep-copy the template's
    transform DAG (legacy OrgDbtModel/Operation/Edge rows + the active-path CanvasNode/Edge
    rows) onto it, then materialize the trial's `.sql`/`sources.yml` files via one of two paths
    and push to the new repo.

    `setup_managed_git_workspace` creates a brand-new managed GitHub repo, an `OrgDbt` row (with
    an EMPTY dbt scaffold — no `.sql` files yet), and the cli-profile block, then sets
    `run.trial_org.dbt`.

    Branches on whether the TEMPLATE dbt has any `OrgDbtOperation` rows:
    - UI-operation-built templates (>=1 OrgDbtOperation): the REGEN path. `copy_dbt_dag` rebuilds
      the DB-side DAG with `sql_path=None` on every copied model (the scaffold has no files yet);
      `regenerate_and_push` then walks the copied operation chains to write real `.sql`/
      `sources.yml` files and pushes them to the managed repo.
    - github/file-based templates (ZERO OrgDbtOperation rows, e.g. `health_org` — models are
      `.sql` files in a repo, canvas only displays them, no operation chain to regenerate from):
      the COPY path. `copy_dbt_dag(preserve_sql_path=True)` keeps each copied model's `sql_path`
      as-is (the files will land at those same project-relative paths); `copy_dbt_repo_files`
      then clones the template's own repo and copies its `models/` directory straight into the
      trial repo, then commits + pushes.

    After EITHER branch completes, creates the dbt system OrgTasks (git-pull/dbt-clean/
    dbt-deps/...) for the trial org via `create_default_transform_tasks`, mirroring what a
    normal dbt-enabled org gets — otherwise the trial org has a dbt project but no OrgTasks for
    the UI to run dbt with.
    """
    template_dbt = run.template.dbt
    if template_dbt is None:
        raise RuntimeError(f"template org {run.template.slug} has no dbt workspace")

    # project_name must be a valid dbt project name (letters/digits/underscore — NO hyphens,
    # which the trial slug has). The normal Dalgo flow (transform_api.py) uses the literal
    # "dbtrepo"; match it.
    setup_managed_git_workspace(
        run.trial_org,
        project_name="dbtrepo",
        default_schema=template_dbt.default_schema,
    )

    trial_dbt = run.trial_org.dbt
    if trial_dbt is None:
        # setup_managed_git_workspace mutates the passed-in org in place on success, but fall
        # back to a DB refresh in case a caller only persisted the FK without updating this
        # in-memory instance.
        run.trial_org.refresh_from_db()
        trial_dbt = run.trial_org.dbt
    if trial_dbt is None:
        raise RuntimeError("setup_managed_git_workspace did not set the trial org's dbt workspace")

    is_file_based = not OrgDbtOperation.objects.filter(dbtmodel__orgdbt=template_dbt).exists()

    if is_file_based:
        model_map = copy_dbt_dag(template_dbt, trial_dbt, preserve_sql_path=True)
        copy_dbt_repo_files(template_dbt, trial_dbt)
        run.manifest["dbt_copy_path"] = True
        run.manifest["dbt_regenerated"] = 0
    else:
        model_map = copy_dbt_dag(template_dbt, trial_dbt)
        regenerated = regenerate_and_push(run.trial_org, trial_dbt)
        run.manifest["dbt_copy_path"] = False
        run.manifest["dbt_regenerated"] = regenerated

    run.manifest["dbt_repo"] = trial_dbt.gitrepo_url
    run.manifest["dbt_models"] = len(model_map)

    # Create the dbt system OrgTasks (git-pull/dbt-clean/dbt-deps/dbt-run/...) for the trial
    # org, same as the normal Dalgo dbt setup flow (transform_api.py) does — without this the
    # trial dbt project exists on disk/in the DB but has no OrgTasks for the UI to run. Runs
    # AFTER both the copy and regen branches above, once the trial dbt project is fully set up
    # and pushed.
    run.trial_org.refresh_from_db()
    trial_dbt = run.trial_org.dbt
    if trial_dbt is None:
        raise RuntimeError(
            "trial org lost its dbt workspace before transform tasks could be created"
        )
    if trial_dbt.cli_profile_block is None:
        raise RuntimeError(
            "trial org's dbt workspace has no cli_profile_block; "
            "setup_managed_git_workspace should have set it"
        )

    dbt_project_params = DbtProjectManager.gather_dbt_project_params(run.trial_org, trial_dbt)
    create_default_transform_tasks(run.trial_org, trial_dbt.cli_profile_block, dbt_project_params)
    run.manifest["dbt_transform_tasks_created"] = True


def _step_prefect(run: CloneRun) -> None:
    """Step 7 — rebuild the template's orchestrate Prefect deployments on the trial org.

    Delegates to `ddpui.core.trial.prefect_clone.clone_orchestrate_dataflows`, which reconstructs
    each template `OrgDataFlowv1(dataflow_type="orchestrate")` as a fresh
    `PrefectDataFlowCreateSchema4` payload — connections remapped via
    `run.manifest["connection_map"]` (built by `_step_connections`), transform tasks resolved
    against the trial org — and hands it to `PipelineService.create_pipeline`, which mints the
    new deployment + `OrgDataFlowv1` + `DataflowOrgTask` rows. Must run LAST: it needs both the
    trial connections (P3) and the trial dbt workspace (P4) to already exist.
    """
    deployment_ids = clone_orchestrate_dataflows(
        run.template, run.trial_org, run.manifest.get("connection_map", {})
    )
    run.manifest["deployment_ids"] = deployment_ids


def _step_viz(run: CloneRun) -> None:
    """Step 8 — clone native viz objects (metrics/KPIs/charts/dashboards/filters/alerts/report
    snapshots) onto the trial org, rewriting cross-object id references via old->new maps.

    Delegates to `ddpui.core.trial.viz_clone.clone_viz`. Only needs the trial org + admin
    OrgUser from Step 1 — independent of Steps 4-7 (Airbyte/dbt/Prefect); cloned charts/dashboards
    only render real data once Step 3's warehouse copy has run.
    """
    run.manifest["viz"] = clone_viz(run.template, run.trial_org, run.trial_orguser)


def delete_trial_org(org: Org) -> None:
    """Fully delete a trial org, including the viz rows OrgCleanupService can't.

    `OrgCleanupService.delete_org()` does NOT delete `Metric`/`KPI` rows, and `Metric.org` is an
    on_delete=PROTECT FK (KPIs in turn PROTECT their Metric) — so the moment a trial is cloned
    from a template that has metrics/KPIs (they get copied by viz_clone), the final `org.delete()`
    inside `delete_org()` raises ProtectedError, leaving the org row + OrgUser half-removed and the
    org NAME still taken (which then blocks the next clone for that email). Reap KPIs, then Metrics
    (Alerts CASCADE off Metric), before handing the rest to OrgCleanupService.
    """
    from ddpui.models.metric import Metric, KPI  # local import: avoid a heavy import at module load

    KPI.objects.filter(org=org).delete()  # KPI.metric is PROTECT → KPIs must go before Metrics
    Metric.objects.filter(org=org).delete()  # Metric.org is PROTECT; Alerts CASCADE off the Metric
    OrgCleanupService(org, dry_run=False).delete_org()


def _teardown(run: CloneRun) -> None:
    """Best-effort teardown of whatever got created before a mid-run failure.

    Guarded on what actually exists (run.trial_org / manifest markers). Wrapped by the
    caller in its own try/except so a teardown problem never masks the original exception.
    """
    # The two teardown actions are INDEPENDENTLY guarded: the trial RDS db+role live OUTSIDE
    # the org/Airbyte/Prefect graph, so if delete_org() throws mid-teardown (e.g. Airbyte
    # unreachable) the RDS drop must still run — otherwise that db+role leak. Drop the RDS
    # resources first (isolated, cheap) so they can't be stranded by a delete_org() failure.
    if run.manifest.get("trial_warehouse_db"):
        try:
            logger.info(f"dropping trial database for failed clone {run.trial_email}")
            drop_trial_database(run.trial_email)
        except Exception as rds_err:  # skipcq PYL-W0703
            logger.error(f"failed to drop trial database for {run.trial_email}: {rds_err}")

    if run.trial_org:
        # Metric/KPI have PROTECT FKs that OrgCleanupService.delete_org() doesn't handle — so
        # delete_trial_org() reaps those first (see its docstring). Chart/Dashboard/DashboardFilter/
        # Alert/ReportSnapshot are org=CASCADE (or have no org FK) and are reaped by delete_org().
        # sources+connections (Steps 4-5) are also reaped by delete_org() -> delete_warehouse()
        # (deletes each connection's airbyte OrgTask) -> delete_airbyte_workspace() (sources +
        # workspace) — no extra Airbyte teardown needed here as long as run.trial_org exists.
        try:
            logger.info(
                f"tearing down org+workspace for failed clone (template={run.template.slug})"
            )
            delete_trial_org(run.trial_org)
        except Exception as org_err:  # skipcq PYL-W0703
            logger.error(f"failed to delete_org during teardown ({run.template.slug}): {org_err}")

    # the Django User is created before the OrgUser in step 1; delete_org removes the OrgUser
    # but not the User, so clean up an accountless trial User to keep retries unblocked.
    try:
        user = User.objects.filter(username=run.trial_email).first()
        if user and not OrgUser.objects.filter(user=user).exists():
            user.delete()
    except Exception as user_err:  # skipcq PYL-W0703
        logger.error(f"failed to delete dangling trial user {run.trial_email}: {user_err}")


def clone_template_org(
    template_org_id: int,
    trial_email: str,
    org_name: str | None = None,
    role_slug: str | None = None,
    progress=None,
) -> CloneRun:
    """Deep-clone a template org into a new trial org (Steps 1–8), timing each step.

    Serial chain: org+user → warehouse → warehouse-data → sources → connections → dbt → prefect →
    viz. State for the run lives only in the returned in-memory `CloneRun` — nothing is persisted
    for it. On any failure the exception is re-raised (after best-effort teardown) so the caller
    (management command / future Celery task) sees it.

    `org_name`/`role_slug` are optional overrides applied in Step 1 (see `_step_org_and_user`).
    `progress`, if given, is called as `progress(step_number, label)` right before each of the
    8 steps runs (labels from `STEP_LABELS`) — e.g. to stream progress to a client. Optional;
    when None, behavior is unchanged from before this parameter existed.
    """
    if account_exists_for_email(trial_email):
        raise TrialAccountExistsError(
            f"an account already exists for {trial_email}; direct the user to log in"
        )

    template = Org.objects.get(id=template_org_id)
    run = CloneRun(
        template=template, trial_email=trial_email, org_name=org_name, role_slug=role_slug
    )
    logger.info(f"starting clone from template {template.slug} for {trial_email}")

    def _do(step_number, timing_key, step_fn):
        if progress:
            progress(step_number, STEP_LABELS[step_number])
        with step_timer(run, timing_key):
            step_fn(run)

    try:
        _do(1, "step1_org_user", _step_org_and_user)
        _do(2, "step2_warehouse", _step_warehouse)
        _do(3, "step3_warehouse_data", _step_warehouse_data)
        _do(4, "step4_sources", _step_sources)
        _do(5, "step5_connections", _step_connections)
        _do(6, "step6_dbt", _step_dbt)
        _do(7, "step7_prefect", _step_prefect)
        _do(8, "step8_viz", _step_viz)
    except Exception as err:
        logger.error(f"clone from template {template.slug} failed: {err}")
        # best-effort teardown of whatever got created before the failure — never let a
        # teardown problem mask the original exception, which must still propagate.
        try:
            _teardown(run)
        except Exception as cleanup_err:
            logger.error(f"best-effort teardown failed for template {template.slug}: {cleanup_err}")
        raise
    logger.info(f"clone from template {template.slug} completed; timings={run.timings}")
    return run
