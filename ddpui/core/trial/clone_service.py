from dataclasses import dataclass, field
from datetime import timedelta

from django.conf import settings
from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.models.org import Org, OrgWarehouse, OrgFeatureFlag
from ddpui.core.trial.exceptions import TrialAccountExistsError, TrialCloneError
from ddpui.core.trial.timing import step_timer
from ddpui.core.trial.warehouse_provision import (
    provision_trial_database,
    drop_trial_database,
    email_hash8,
)
from ddpui.services.org_cleanup_service import OrgCleanupService
from ddpui.core.orgfunctions import create_organization, create_org_plan
from ddpui.core.trial.constants import TRIAL_DURATION_DAYS  # noqa: F401 re-exported
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.schemas.org_warehouse_schema import OrgWarehouseSchema
from ddpui.schemas.trial_schema import TrialCloneRequest
from ddpui.models.org_plans import OrgPlanType
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.org_preferences import OrgPreferences
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
from ddpui.core.trial.dbt_clone import copy_dbt_dag, copy_repo_models_from_template
from ddpui.models.metric import Metric, KPI
from ddpui.core.trial.prefect_clone import (
    clone_orchestrate_dataflows,
    sync_transform_tasks_and_deployments,
)
from ddpui.core.trial.viz_clone import clone_viz
from ddpui.ddpdbt.dbt_service import setup_managed_git_workspace
from ddpui.core.orgtaskfunctions import create_default_transform_tasks
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials
from ddpui.utils import feature_flags
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
    work_domain: str | None = None
    trial_org: Org | None = None
    trial_orguser: OrgUser | None = None
    timings: dict = field(default_factory=dict)
    manifest: dict = field(default_factory=dict)


STEP_LABELS = {
    1: "Creating your workspace",
    2: "Setting up your warehouse",
    3: "Connecting your sources",
    4: "Building your pipelines",
    5: "Setting up transforms",
    6: "Scheduling syncs",
    7: "Preparing your dashboards",
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
    # Org name shape: "Trial {email_hash8} {org_name}", truncated to Org.name's 50-char limit.
    # The 8-char email hash sits RIGHT AFTER "Trial " (chars 6-14) on purpose: create_organization
    # derives org.slug = slugify(org.name)[:20], and the org is resolved by slug on every request
    # (auth.py, x-dalgo-org header) while Org.slug is NOT DB-unique — so the slug MUST be unique
    # per trial or two trials silently collide. Keeping the hash inside the first 20 chars
    # guarantees the slug (e.g. "trial-a1b2c3d4-acme-") stays unique per email regardless of how
    # long the org name is. The user-supplied org_name follows the hash for human readability; a
    # long name simply truncates at 50 (no error). Falls back to the template name if org_name is
    # blank. The hash CANNOT go at the end — a long org name would push it past the 20-char cut and
    # break slug uniqueness.
    org_label = (run.org_name or template.name).strip()
    trial_name = f"Trial {email_hash8(run.trial_email)} {org_label}"[:50]
    # the trial plan gets a real validity window (now → now + TRIAL_DURATION_DAYS) — ISO strings
    # because CreateOrgSchema types these as str; Django parses them into the DateTimeFields on
    # the OrgPlans row `create_org_plan` creates.
    trial_start = timezone.now()
    org_payload = CreateOrgSchema(
        name=trial_name,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        can_upgrade_plan=False,
        subscription_duration="trial",
        superset_included=False,
        start_date=trial_start.isoformat(),
        end_date=(trial_start + timedelta(days=TRIAL_DURATION_DAYS)).isoformat(),
    )
    trial_org, err = create_organization(org_payload)
    if err:
        raise TrialCloneError(f"create_organization failed: {err}")

    # record the teardown marker immediately — the Org + Airbyte workspace already exist at
    # this point, so any failure below must still trigger OrgCleanupService on the way out.
    run.trial_org = trial_org

    _, plan_err = create_org_plan(org_payload, trial_org)
    if plan_err:
        raise TrialCloneError(f"create_org_plan failed: {plan_err}")

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
        raise TrialCloneError(f"role {role_slug} not found (load role fixtures)")

    # work_domain is the signup form's job-title pick (M&E / Program Manager / …) — the same
    # field the post-invitation signup writes. Metadata only; `new_role` above is the ONLY
    # thing that grants permissions, and it never comes from client input.
    orguser = OrgUser.objects.create(
        user=user,
        org=trial_org,
        new_role=admin_role,
        email_verified=False,
        work_domain=run.work_domain,
    )
    run.trial_orguser = orguser
    UserAttributes.objects.get_or_create(user=user, defaults={"email_verified": False})
    UserPreferences.objects.get_or_create(
        orguser=orguser, defaults={"enable_email_notifications": True}
    )

    # Copy the template's feature flags so the trial unlocks the SAME features. Critically,
    # REPORTS gates the Reports nav in the frontend (main-layout hides it when the flag is off) —
    # without this the cloned report snapshots would exist but be invisible. The feature_flags
    # helpers validate against the FEATURE_FLAGS allowlist (a stale/unknown flag_name on the
    # template returns None and is skipped) and are idempotent on a retry.
    flags_copied = 0
    for ff in OrgFeatureFlag.objects.filter(org=template):
        if ff.flag_value:
            copied = feature_flags.enable_feature_flag(ff.flag_name, trial_org)
        else:
            copied = feature_flags.disable_feature_flag(ff.flag_name, trial_org)
        if copied:
            flags_copied += 1
        else:
            logger.warning(f"template flag {ff.flag_name} not in FEATURE_FLAGS allowlist; skipped")
    run.manifest["feature_flags_copied"] = flags_copied

    # Copy the template's OrgPreferences LLM opt-in state — without this the trial gets the
    # lazily-created default row (llm_optin=False) and the AI-analysis features stay dead even
    # though the AI feature FLAGS above were copied enabled. Deliberately NOT copied: the
    # discord webhook + notification toggle (a trial must never post to the template's Discord)
    # and the template's approved-by/requested-by OrgUsers (template-org identities) — the
    # trial's own admin is stamped as approver instead.
    template_prefs = OrgPreferences.objects.filter(org=template).first()
    if template_prefs is not None:
        OrgPreferences.objects.create(
            org=trial_org,
            llm_optin=template_prefs.llm_optin,
            llm_optin_approved_by=orguser if template_prefs.llm_optin else None,
            llm_optin_date=timezone.now() if template_prefs.llm_optin else None,
            enable_llm_request=template_prefs.enable_llm_request,
        )
        run.manifest["org_preferences_copied"] = True

    run.manifest["trial_org_slug"] = trial_org.slug
    run.manifest["trial_workspace_id"] = trial_org.airbyte_workspace_id
    run.manifest["trial_orguser_id"] = orguser.id


def _step_warehouse(run: CloneRun) -> None:
    """Step 2 — provision a trial warehouse db and register it via create_warehouse."""
    template = run.template
    template_wh = OrgWarehouse.objects.filter(org=template).first()
    if template_wh is None:
        raise TrialCloneError("template org has no warehouse")
    if template_wh.wtype != "postgres":
        raise TrialCloneError(f"v1 supports postgres only; template is {template_wh.wtype}")

    # carry the template's non-connection config forward (schema/ssl), overriding host/db creds
    # below. The template warehouse MUST live on the trials-RDS instance itself — the data copy
    # is a server-side `CREATE DATABASE ... TEMPLATE ...` (warehouse_provision terminates any
    # blocking sessions first); there is no cross-host dump/restore path.
    template_creds = retrieve_warehouse_credentials(template_wh) or {}
    if template_creds.get("host") != settings.TRIALS_RDS_HOST:
        raise TrialCloneError(
            f"template warehouse host {template_creds.get('host')!r} is not the trials-RDS "
            f"instance {settings.TRIALS_RDS_HOST!r}; the template org's warehouse must live "
            "on the trials RDS for the server-side CREATE DATABASE ... TEMPLATE copy"
        )
    trial_db_params = provision_trial_database(
        run.trial_email, template_db=template_creds.get("database")
    )

    # record the teardown marker immediately — the RDS database already exists at this point,
    # so any failure below must still trigger drop_trial_database on the way out.
    run.manifest["trial_warehouse_db"] = trial_db_params.database
    run.manifest["trial_warehouse_role"] = trial_db_params.username

    # reuse the template destination's definition id (not stored on OrgWarehouse)
    template_dest = airbyte_service.get_destination(
        template.airbyte_workspace_id, template_wh.airbyte_destination_id
    )
    dest_def_id = template_dest["destinationDefinitionId"]

    airbyte_config = dict(template_creds)
    airbyte_config.update(
        {
            "host": trial_db_params.host,
            "port": trial_db_params.port,
            "database": trial_db_params.database,
            "username": trial_db_params.username,
            "password": trial_db_params.password,
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
    # legacy secrets-manager blobs sometimes carry a stale `dbname` key alongside `database`
    # (see airbytehelpers.update_destination's dbname/database handling) — carried verbatim it
    # would point the trial destination at the TEMPLATE's db. `database` above is authoritative.
    airbyte_config.pop("dbname", None)

    wh_payload = OrgWarehouseSchema(
        wtype="postgres",
        name=template_wh.name or "trial warehouse",
        destinationDefId=dest_def_id,
        airbyteConfig=airbyte_config,
    )
    _, err = create_warehouse(run.trial_org, wh_payload)
    if err:
        raise TrialCloneError(f"create_warehouse failed: {err}")

    run.manifest["trial_destination_defid"] = dest_def_id


def _remap_source_definition_id(run: CloneRun, template_defid: str, defs_cache: dict) -> str:
    """Return the TRIAL workspace's sourceDefinitionId equivalent to a template source's.

    Public (grid) connector definitions share one global id across workspaces, so the template's
    id usually exists in the trial workspace as-is. CUSTOM definitions (Kobo/CommCare/Avni —
    `source_definitions/create_custom`) are WORKSPACE-SCOPED: Airbyte mints a fresh id per
    workspace, so the template's id does NOT exist in the trial workspace and `create_source`
    with it would 500 ("could not find spec for this source type"). `create_organization` queues
    `add_custom_connectors_to_workspace` async (fire-and-forget Celery), so by the time this step
    runs the trial's custom definitions may exist, partially exist, or not exist at all — and
    even when they exist their ids differ from the template's.

    Resolution, matched by dockerRepository (the stable identity of a connector):
    1. template id already known to the trial workspace → use as-is (public connectors);
    2. a trial definition with the same dockerRepository exists (the Celery task already
       registered it) → use ITS id;
    3. otherwise create the custom definition in the trial workspace SYNCHRONOUSLY, from the
       template definition's own repo/tag (not settings — the template's pinned version wins),
       and use the fresh id. Also removes the Celery-task race entirely for this source.

    `defs_cache` (built once per run) holds `trial_def_ids` (set), `trial_by_repo`
    ({dockerRepository: trial defid}) and `template_def_by_id`; step 3 updates it as
    definitions get created so repeated sources of the same type reuse one definition.
    """
    if template_defid in defs_cache["trial_def_ids"]:
        return template_defid

    template_def = defs_cache["template_def_by_id"].get(template_defid)
    if template_def is None:
        raise TrialCloneError(
            f"template source definition {template_defid} not found in the template workspace's "
            "definition list; cannot remap it onto the trial workspace"
        )

    repo = template_def["dockerRepository"]
    if repo in defs_cache["trial_by_repo"]:
        return defs_cache["trial_by_repo"][repo]

    created_def = airbyte_service.create_custom_source_definition(
        workspace_id=run.trial_org.airbyte_workspace_id,
        name=template_def["name"],
        docker_repository=repo,
        docker_image_tag=template_def["dockerImageTag"],
        documentation_url=template_def.get("documentationUrl") or "",
    )
    new_defid = created_def["sourceDefinitionId"]
    defs_cache["trial_def_ids"].add(new_defid)
    defs_cache["trial_by_repo"][repo] = new_defid
    run.manifest.setdefault("custom_definitions_created", []).append(repo)
    logger.info(f"registered custom source definition {repo} in trial workspace as {new_defid}")
    return new_defid


def _step_sources(run: CloneRun) -> None:
    """Step 3 — recreate the template's Airbyte sources in the trial workspace.

    Validates that every template source has a config entry in the (gitignored)
    TEMPLATE_SOURCE_CREDS_FILE before creating anything — Airbyte masks source configs on
    read-back, so the only source of real credentials is that Dalgo-controlled store.

    Source definition ids are remapped per-source via `_remap_source_definition_id` — custom
    connector definitions are workspace-scoped in Airbyte, so the template's id cannot be
    reused blindly (see that helper's docstring).
    """
    template_ws = run.template.airbyte_workspace_id
    trial_ws = run.trial_org.airbyte_workspace_id
    template_sources = airbyte_service.get_sources(template_ws)["sources"]
    names = [s["name"] for s in template_sources]
    missing = validate_template_source_configs(names)
    if missing:
        raise TrialCloneError(f"missing source config for template sources: {missing}")

    template_defs = airbyte_service.get_source_definitions(template_ws)["sourceDefinitions"]
    trial_defs = airbyte_service.get_source_definitions(trial_ws)["sourceDefinitions"]
    defs_cache = {
        "trial_def_ids": {d["sourceDefinitionId"] for d in trial_defs},
        "trial_by_repo": {d["dockerRepository"]: d["sourceDefinitionId"] for d in trial_defs},
        "template_def_by_id": {d["sourceDefinitionId"]: d for d in template_defs},
    }

    source_map: dict = {}
    for src in template_sources:
        config = load_template_source_config(src["name"])
        trial_defid = _remap_source_definition_id(run, src["sourceDefinitionId"], defs_cache)
        created = airbyte_service.create_source(trial_ws, src["name"], trial_defid, config)
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
    warehouse rows copied server-side in Step 2 — Full Refresh|Overwrite is the only mode
    that's safe there regardless of what sync mode the template connection used.

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
    """Step 4 — recreate connections on remapped sources; first-sync Full Refresh|Overwrite.

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
            raise TrialCloneError(f"no remapped source for template source {old_source_id}")
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
            raise TrialCloneError(f"create_connection failed for {conn['name']}: {err}")
        connection_map[conn["connectionId"]] = res["connectionId"]
    run.manifest["connection_map"] = connection_map
    run.manifest["connection_ids"] = list(connection_map.values())


def _step_dbt(run: CloneRun) -> None:
    """Step 5 — fresh managed dbt workspace + the template's UI4T DAG rows. No dbt-content copy.

    `setup_managed_git_workspace` gives the trial the SAME dbt setup every new org gets: a
    managed GitHub repo with an empty scaffold, the cli-profile block, `org.dbt` set. Then
    `copy_dbt_dag` copies the template's UI4T transform DAG as Django DB rows (legacy
    OrgDbtModel/Operation/Edge rows AND the active CanvasNode/Edge rows, `sql_path=None` —
    no files exist for them) so the trial's transform canvas renders the template's DAG.
    Charts/dashboards read the warehouse tables Step 2 already copied server-side.

    The template's dbt CONTENT is deliberately NOT cloned in v1 — no repo-file copy, no
    `.sql`/`sources.yml` regeneration. The content-cloning paths (`copy_dbt_repo_files` /
    `regenerate_and_push`) were removed as dead code; recover them from git history if
    dbt-content cloning is added later.

    Finishes by creating the dbt system OrgTasks (git-pull/dbt-clean/dbt-deps/dbt-run/...)
    via `create_default_transform_tasks`, mirroring the normal dbt-enabled-org setup, so the
    Transform page works like any other org's (dbt runs against the empty scaffold).
    """
    template_dbt = run.template.dbt
    if template_dbt is None:
        raise TrialCloneError(f"template org {run.template.slug} has no dbt workspace")

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
        raise TrialCloneError(
            "setup_managed_git_workspace did not set the trial org's dbt workspace"
        )

    # setup_managed_git_workspace hardcodes transform_type=GIT; mirror the TEMPLATE's value
    # instead — a UI4T template must stay `ui` on the trial, otherwise the repo-to-canvas sync
    # path (`sync_remote_dbtproject_to_canvas`, gated on GIT) becomes active and can re-parse
    # the empty scaffold repo right over the copied CanvasNode/CanvasEdge rows.
    if trial_dbt.transform_type != template_dbt.transform_type:
        trial_dbt.transform_type = template_dbt.transform_type
        trial_dbt.save(update_fields=["transform_type"])

    model_map = copy_dbt_dag(template_dbt, trial_dbt)

    # Copy the template repo's models/ directory VERBATIM into the trial repo (+ push) —
    # byte-identical dbt content (.sql files, sources.yml, model docs), guaranteed parity
    # with the template. copy_dbt_dag preserved each row's sql_path, which stays valid
    # because the files land at the same project-relative paths. Without this content the
    # copied canvas is view-only — new models on copied sources fail dbt compilation and
    # `dbt run` has nothing to build.
    copied_files = copy_repo_models_from_template(template_dbt, trial_dbt)

    run.manifest["dbt_mode"] = "repo_models_copy"
    run.manifest["dbt_repo"] = trial_dbt.gitrepo_url
    run.manifest["dbt_models"] = len(model_map)
    run.manifest["dbt_files_copied"] = copied_files

    if trial_dbt.cli_profile_block is None:
        raise TrialCloneError(
            "trial org's dbt workspace has no cli_profile_block; "
            "setup_managed_git_workspace should have set it"
        )
    dbt_project_params = DbtProjectManager.gather_dbt_project_params(run.trial_org, trial_dbt)
    create_default_transform_tasks(run.trial_org, trial_dbt.cli_profile_block, dbt_project_params)
    run.manifest["dbt_transform_tasks_created"] = True


def _step_prefect(run: CloneRun) -> None:
    """Step 6 — rebuild the template's orchestrate Prefect deployments on the trial org.

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

    # then carry standalone template transform OrgTasks (not linked into any pipeline) and make
    # every manual Transform-page deployment's baked params match the copied OrgTask parameters —
    # see sync_transform_tasks_and_deployments.
    run.manifest["transform_task_sync"] = sync_transform_tasks_and_deployments(
        run.template, run.trial_org
    )


def _step_viz(run: CloneRun) -> None:
    """Step 7 — clone native viz objects (metrics/KPIs/charts/dashboards/filters/alerts/report
    snapshots) onto the trial org, rewriting cross-object id references via old->new maps.

    Delegates to `ddpui.core.trial.viz_clone.clone_viz`. Only needs the trial org + admin
    OrgUser from Step 1 — independent of Steps 3-6 (Airbyte/dbt/Prefect); cloned charts/dashboards
    only render real data once Step 2's server-side warehouse copy has run.
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

    # The Django User (+ its UserAttributes and the password set at /activate) is deliberately
    # KEPT across teardown. delete_org removes the OrgUser — so account_exists_for_email (which
    # keys on OrgUser, not User) still returns False for a failed trial, leaving retry unblocked
    # — while the person's email/password/verified state survives so "Try again" (POST
    # /trial/retry) can re-clone without re-asking email, verification, or password.


def clone_template_org(payload: TrialCloneRequest, progress=None) -> CloneRun:
    """Deep-clone a template org into a new trial org (Steps 1–7), timing each step.

    Serial chain: org+user → warehouse (incl. server-side data copy) → sources → connections →
    dbt → prefect → viz. State for the run lives only in the returned in-memory `CloneRun` —
    nothing is persisted for it. On any failure the exception is re-raised (after best-effort
    teardown) so the caller (management command / Celery task) sees it.

    `payload.org_name`/`payload.role_slug` are optional overrides applied in Step 1 (see
    `_step_org_and_user`). `progress`, if given, is called as `progress(step_number, label)`
    right before each of the 7 steps runs (labels from `STEP_LABELS`) — e.g. to stream progress
    to a client. Optional; when None, behavior is unchanged from before this parameter existed.
    """
    if account_exists_for_email(payload.trial_email):
        raise TrialAccountExistsError(
            f"an account already exists for {payload.trial_email}; direct the user to log in"
        )

    template = Org.objects.get(id=payload.template_org_id)
    run = CloneRun(
        template=template,
        trial_email=payload.trial_email,
        org_name=payload.org_name,
        role_slug=payload.role_slug,
        work_domain=payload.work_domain,
    )
    logger.info(f"starting clone from template {template.slug} for {payload.trial_email}")

    def _do(step_number, timing_key, step_fn):
        if progress:
            progress(step_number, STEP_LABELS[step_number])
        with step_timer(run, timing_key):
            step_fn(run)

    try:
        _do(1, "step1_org_user", _step_org_and_user)
        _do(2, "step2_warehouse", _step_warehouse)
        _do(3, "step3_sources", _step_sources)
        _do(4, "step4_connections", _step_connections)
        _do(5, "step5_dbt", _step_dbt)
        _do(6, "step6_prefect", _step_prefect)
        _do(7, "step7_viz", _step_viz)
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
