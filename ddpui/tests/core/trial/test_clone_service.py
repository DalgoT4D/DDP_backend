import uuid as uuid_module
from unittest.mock import patch, Mock

import pytest
from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgDbt, OrgWarehouse, OrgPrefectBlockv1
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType, OrgDbtOperation
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.trial import clone_service
from ddpui.core.trial.clone_service import CloneRun
from ddpui.core.trial.exceptions import TrialAccountExistsError

pytestmark = pytest.mark.django_db


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_runs_all_steps_and_completes(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7
):
    template = Org.objects.create(name="tmpl", slug="tmpl")
    run = clone_service.clone_template_org(template.id, "a@b.org")
    assert isinstance(run, CloneRun)
    assert run.template == template
    assert run.trial_email == "a@b.org"
    mock_s1.assert_called_once()
    mock_s2.assert_called_once()
    mock_s3.assert_called_once()
    mock_s4.assert_called_once()
    mock_s5.assert_called_once()
    mock_s6.assert_called_once()
    mock_s7.assert_called_once()
    assert set(run.timings.keys()) == {
        "step1_org_user",
        "step2_warehouse",
        "step3_sources",
        "step4_connections",
        "step5_dbt",
        "step6_prefect",
        "step7_viz",
    }


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_invokes_progress_callback_for_all_steps_in_order(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7
):
    template = Org.objects.create(name="tmpl-progress", slug="tmpl-progress")
    calls = []
    clone_service.clone_template_org(
        template.id, "progress@b.org", progress=lambda n, label: calls.append((n, label))
    )
    assert [n for n, _ in calls] == [1, 2, 3, 4, 5, 6, 7]
    for n, label in calls:
        assert label == clone_service.STEP_LABELS[n]
    # timing keys must be unaffected by the progress refactor
    assert set(clone_service.STEP_LABELS.keys()) == {1, 2, 3, 4, 5, 6, 7}


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_without_progress_callback_still_works(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7
):
    """progress is optional — existing management-command path (no progress) must be unaffected."""
    template = Org.objects.create(name="tmpl-noprogress", slug="tmpl-noprogress")
    run = clone_service.clone_template_org(template.id, "noprogress@b.org")
    assert isinstance(run, CloneRun)
    mock_s1.assert_called_once()
    mock_s7.assert_called_once()


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_passes_org_name_and_role_slug_onto_run(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7
):
    template = Org.objects.create(name="tmpl-orgrole", slug="tmpl-orgrole")

    captured = {}

    def fake_step1(run):
        captured["org_name"] = run.org_name
        captured["role_slug"] = run.role_slug

    mock_s1.side_effect = fake_step1

    run = clone_service.clone_template_org(
        template.id, "orgrole@b.org", org_name="Acme Co", role_slug="custom-role"
    )
    assert run.org_name == "Acme Co"
    assert run.role_slug == "custom-role"
    assert captured["org_name"] == "Acme Co"
    assert captured["role_slug"] == "custom-role"


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_org_and_user", side_effect=RuntimeError("kaboom"))
def test_clone_reraises_and_skips_teardown_when_step1_fails_immediately(
    mock_s1, mock_cleanup_cls, mock_drop
):
    template = Org.objects.create(name="tmpl2", slug="tmpl2")
    with pytest.raises(RuntimeError, match="kaboom"):
        clone_service.clone_template_org(template.id, "a@b.org")

    # step1 (org+user) never got far enough to create a trial_org or warehouse — nothing to
    # tear down.
    mock_cleanup_cls.assert_not_called()
    mock_drop.assert_not_called()


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_sources", side_effect=RuntimeError("boom"))
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_tears_down_created_resources_on_later_failure(
    mock_s1, mock_s2, mock_s3, mock_cleanup_cls, mock_drop
):
    template = Org.objects.create(name="tmpl3", slug="tmpl3")
    trial_org = Org.objects.create(name="Trial X", slug="trial-x")

    def fake_step1(run):
        run.trial_org = trial_org

    def fake_step2(run):
        run.manifest["trial_warehouse_db"] = "trial_123"

    mock_s1.side_effect = fake_step1
    mock_s2.side_effect = fake_step2
    mock_cleanup_instance = mock_cleanup_cls.return_value

    with pytest.raises(RuntimeError, match="boom"):
        clone_service.clone_template_org(template.id, "a@b.org")

    mock_cleanup_cls.assert_called_once_with(trial_org, dry_run=False)
    mock_cleanup_instance.delete_org.assert_called_once()
    mock_drop.assert_called_once_with("a@b.org")


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_teardown_failure_does_not_mask_original_error(mock_s1, mock_cleanup_cls, mock_drop):
    """Teardown itself blowing up must never hide the original exception."""
    template = Org.objects.create(name="tmpl4", slug="tmpl4")
    trial_org = Org.objects.create(name="Trial Y", slug="trial-y")

    def fake_step1(run):
        run.trial_org = trial_org
        raise RuntimeError("original failure")

    mock_s1.side_effect = fake_step1
    mock_cleanup_cls.return_value.delete_org.side_effect = Exception("teardown exploded")

    with pytest.raises(RuntimeError, match="original failure"):
        clone_service.clone_template_org(template.id, "a@b.org")

    mock_cleanup_cls.assert_called_once_with(trial_org, dry_run=False)
    mock_drop.assert_not_called()


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_clone_tears_down_org_on_step1_mid_failure(
    mock_create_org, mock_create_plan, mock_cleanup_cls, mock_drop
):
    """create_organization succeeds (the Org + Airbyte workspace now exist), but the
    subsequent create_org_plan call fails. FIX 1 requires run.trial_org to be set right
    after create_organization returns, so the teardown guard still fires even though the
    failure happened later in the same step."""
    template = Org.objects.create(name="tmpl6", slug="tmpl6")
    trial_org = Org.objects.create(
        name="Trial 1 tmpl6", slug="trial-1-tmpl6", airbyte_workspace_id="ws-6"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (None, "plan blew up")
    mock_cleanup_instance = mock_cleanup_cls.return_value

    with pytest.raises(RuntimeError, match="create_org_plan failed"):
        clone_service.clone_template_org(template.id, "a@b.org")

    mock_cleanup_cls.assert_called_once_with(trial_org, dry_run=False)
    mock_cleanup_instance.delete_org.assert_called_once()
    mock_drop.assert_not_called()


@patch("ddpui.core.trial.clone_service.settings")
@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_tears_down_db_on_step2_mid_failure(
    mock_s1,
    mock_provision,
    mock_retrieve,
    mock_ab,
    mock_create_wh,
    mock_cleanup_cls,
    mock_drop,
    mock_settings,
):
    """provision_trial_database succeeds (the RDS database now exists), but the
    subsequent create_warehouse call fails. FIX 2 requires manifest["trial_warehouse_db"]
    to be set right after provision_trial_database returns, so drop_trial_database still
    fires even though the failure happened later in the same step."""
    template = Org.objects.create(name="tmpl7", slug="tmpl7", airbyte_workspace_id="ws-tmpl7")
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl7", credentials="x"
    )
    trial_org = Org.objects.create(name="Trial Z", slug="trial-z", airbyte_workspace_id="ws-z")

    captured = {}

    def fake_step1(run):
        run.trial_org = trial_org
        captured["run"] = run

    mock_s1.side_effect = fake_step1
    mock_provision.return_value = {
        "host": "h",
        "port": 5432,
        "database": "trial_z_db",
        "username": "u",
        "password": "p",
    }
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_settings.TRIALS_RDS_HOST = "trials-rds-host"
    mock_retrieve.return_value = {"host": "trials-rds-host", "database": "tmpl_db"}
    mock_create_wh.return_value = (None, "create_warehouse blew up")
    mock_cleanup_instance = mock_cleanup_cls.return_value

    with pytest.raises(RuntimeError, match="create_warehouse failed"):
        clone_service.clone_template_org(template.id, "a@b.org")

    assert captured["run"].manifest["trial_warehouse_db"] == "trial_z_db"

    mock_drop.assert_called_once_with("a@b.org")
    mock_cleanup_cls.assert_called_once_with(trial_org, dry_run=False)
    mock_cleanup_instance.delete_org.assert_called_once()


@patch("ddpui.core.trial.clone_service.settings")
@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_teardown_rds_drop_independent_of_delete_org_failure(
    mock_s1,
    mock_provision,
    mock_retrieve,
    mock_ab,
    mock_create_wh,
    mock_cleanup_cls,
    mock_drop,
    mock_settings,
):
    """The trial RDS db+role live outside the org/Airbyte graph, so if delete_org() throws
    mid-teardown the RDS drop must STILL run (independent guards) — otherwise the db leaks.
    The original exception must still propagate."""
    template = Org.objects.create(name="tmpl8", slug="tmpl8", airbyte_workspace_id="ws-tmpl8")
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl8", credentials="x"
    )
    trial_org = Org.objects.create(name="Trial Q", slug="trial-q", airbyte_workspace_id="ws-q")

    def fake_step1(run):
        run.trial_org = trial_org

    mock_s1.side_effect = fake_step1
    mock_provision.return_value = {
        "host": "h",
        "port": 5432,
        "database": "trial_q_db",
        "username": "u",
        "password": "p",
    }
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_settings.TRIALS_RDS_HOST = "trials-rds-host"
    mock_retrieve.return_value = {"host": "trials-rds-host", "database": "tmpl_db"}
    mock_create_wh.return_value = (None, "create_warehouse blew up")
    # delete_org() itself explodes during teardown
    mock_cleanup_cls.return_value.delete_org.side_effect = Exception("airbyte unreachable")

    with pytest.raises(RuntimeError, match="create_warehouse failed"):
        clone_service.clone_template_org(template.id, "a@b.org")

    # RDS drop still fired despite delete_org blowing up — no stranded db
    mock_drop.assert_called_once_with("a@b.org")
    mock_cleanup_cls.return_value.delete_org.assert_called_once()


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_warehouse", side_effect=RuntimeError("boom"))
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_teardown_keeps_the_person_for_retry(mock_s1, mock_s2, mock_cleanup_cls, mock_drop):
    """On failure, teardown removes the OrgUser (via delete_org) but KEEPS the Django User —
    its password and UserAttributes — so POST /trial/retry can re-clone without re-signup. With
    the OrgUser gone, account_exists_for_email stays False, so the retry is not blocked."""
    template = Org.objects.create(name="tmpl-keep", slug="tmpl-keep")
    trial_org = Org.objects.create(name="Trial Keep", slug="trial-keep")

    def fake_step1(run):
        user = User.objects.create(username=run.trial_email, email=run.trial_email)
        user.set_password("kept-secret")  # set at /activate in real life
        user.save()
        UserAttributes.objects.create(user=user, email_verified=True)
        orguser = OrgUser.objects.create(user=user, org=trial_org, email_verified=False)
        run.trial_org = trial_org
        run.trial_orguser = orguser

    mock_s1.side_effect = fake_step1

    # OrgCleanupService.delete_org is mocked out (no real infra) — but it WOULD remove the
    # OrgUser in real life, so mimic that side effect here.
    def fake_delete_org():
        OrgUser.objects.filter(org=trial_org).delete()

    mock_cleanup_cls.return_value.delete_org.side_effect = fake_delete_org

    with pytest.raises(RuntimeError, match="boom"):
        clone_service.clone_template_org(template.id, "keep@x.org")

    mock_cleanup_cls.return_value.delete_org.assert_called_once()
    # the OrgUser is gone → account_exists_for_email stays False → retry allowed
    assert not OrgUser.objects.filter(user__username="keep@x.org").exists()
    assert not clone_service.account_exists_for_email("keep@x.org")
    # ...but the person survives: User + password + verified UserAttributes kept
    user = User.objects.filter(username="keep@x.org").first()
    assert user is not None
    assert user.check_password("kept-secret")
    assert UserAttributes.objects.filter(user=user, email_verified=True).exists()


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_warehouse", side_effect=RuntimeError("boom"))
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_teardown_keeps_user_that_still_has_an_orguser(
    mock_s1, mock_s2, mock_cleanup_cls, mock_drop
):
    """Safety: if the trial email's User still has an OrgUser after delete_org() (e.g. a
    pre-existing real user whose email collided with the trial email), teardown must NOT
    delete that User."""
    template = Org.objects.create(name="tmpl-collide", slug="tmpl-collide")
    trial_org = Org.objects.create(name="Trial Collide", slug="trial-collide")
    other_org = Org.objects.create(name="Other Org", slug="other-org")

    def fake_step1(run):
        # the trial email already has a real account on a DIFFERENT org — delete_org() on
        # the trial_org won't touch that OrgUser.
        user = User.objects.create(username=run.trial_email, email=run.trial_email)
        OrgUser.objects.create(user=user, org=other_org, email_verified=False)
        run.trial_org = trial_org

    mock_s1.side_effect = fake_step1
    mock_cleanup_cls.return_value.delete_org.return_value = None

    with pytest.raises(RuntimeError, match="boom"):
        clone_service.clone_template_org(template.id, "collide@x.org")

    mock_cleanup_cls.return_value.delete_org.assert_called_once()
    assert User.objects.filter(username="collide@x.org").exists()


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_timeout_tears_down_keeping_user_and_reraises(
    mock_s1, mock_s2, mock_cleanup_cls, mock_drop
):
    """A soft-time-limit timeout mid-clone raises SoftTimeLimitExceeded. clone_template_org's
    `except Exception` must catch it like any failure: teardown runs (org purged, RDS dropped),
    the person is kept, and the exception is re-raised so the task reports "failed"."""
    from celery.exceptions import SoftTimeLimitExceeded

    template = Org.objects.create(name="tmpl-timeout", slug="tmpl-timeout")
    trial_org = Org.objects.create(name="Trial Timeout", slug="trial-timeout")

    def fake_step1(run):
        user = User.objects.create(username=run.trial_email, email=run.trial_email)
        UserAttributes.objects.create(user=user, email_verified=True)
        orguser = OrgUser.objects.create(user=user, org=trial_org, email_verified=False)
        run.trial_org = trial_org
        run.trial_orguser = orguser
        run.manifest["trial_warehouse_db"] = "ft_x_db"  # so the RDS drop guard fires

    mock_s1.side_effect = fake_step1
    mock_s2.side_effect = SoftTimeLimitExceeded()  # the clone blows the soft time limit at step 2
    mock_cleanup_cls.return_value.delete_org.side_effect = lambda: OrgUser.objects.filter(
        org=trial_org
    ).delete()

    with pytest.raises(SoftTimeLimitExceeded):
        clone_service.clone_template_org(template.id, "slow@x.org")

    # teardown happened: org delete + RDS drop both fired
    mock_cleanup_cls.return_value.delete_org.assert_called_once()
    mock_drop.assert_called_once_with("slow@x.org")
    # ...but the person survives and stays retryable
    assert User.objects.filter(username="slow@x.org").exists()
    assert not clone_service.account_exists_for_email("slow@x.org")


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_creates_org_and_admin(mock_create_org, mock_create_plan):
    Role.objects.get_or_create(slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1})
    template = Org.objects.create(name="tmpl", slug="tmpl")
    trial_org = Org.objects.create(
        name="Trial 1 tmpl", slug="trial-1-tmpl", airbyte_workspace_id="ws-9"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email="admin@b.org")
    clone_service._step_org_and_user(run)

    assert run.trial_org == trial_org
    orguser = OrgUser.objects.filter(org=trial_org).first()
    assert orguser is not None
    assert orguser.user.email == "admin@b.org"
    assert orguser.new_role.slug == ACCOUNT_MANAGER_ROLE
    assert not orguser.user.has_usable_password()
    user_attrs = UserAttributes.objects.filter(user=orguser.user).first()
    assert user_attrs is not None
    assert user_attrs.email_verified is False
    assert run.trial_orguser is not None
    assert run.trial_orguser.id == orguser.id


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_copies_template_feature_flags(mock_create_org, mock_create_plan):
    """The trial must inherit the template's feature flags — REPORTS in particular gates the
    Reports nav in the frontend, so without the copy the cloned report snapshots are invisible."""
    from ddpui.models.org import OrgFeatureFlag

    Role.objects.get_or_create(slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1})
    template = Org.objects.create(name="tmpl-ff", slug="tmpl-ff")
    OrgFeatureFlag.objects.create(org=template, flag_name="REPORTS", flag_value=True)
    OrgFeatureFlag.objects.create(org=template, flag_name="DATA_QUALITY", flag_value=False)
    trial_org = Org.objects.create(
        name="Trial ff tmpl", slug="trial-ff-tmpl", airbyte_workspace_id="ws-ff"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email="ff@b.org")
    clone_service._step_org_and_user(run)

    trial_flags = {f.flag_name: f.flag_value for f in OrgFeatureFlag.objects.filter(org=trial_org)}
    assert trial_flags == {"REPORTS": True, "DATA_QUALITY": False}
    assert run.manifest["feature_flags_copied"] == 2


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_name_uses_org_name_prefixed_by_email_hash(
    mock_create_org, mock_create_plan
):
    """Name shape is "Trial {email_hash8} {org_name}". The user-supplied org_name IS used (for
    human readability), but the per-email hash sits right after "Trial" so two users typing the
    SAME org_name still get unique names/slugs — the backend auto-uniquifies, no frontend error."""
    Role.objects.get_or_create(slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1})
    template = Org.objects.create(name="Health Demo", slug="tmpl-orgname")
    expected_hash = clone_service.email_hash8("acme@b.org")
    trial_org = Org.objects.create(
        name=f"Trial {expected_hash} test",
        slug=f"trial-{expected_hash}-tes",
        airbyte_workspace_id="ws-11",
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email="acme@b.org", org_name="test")
    clone_service._step_org_and_user(run)

    args, _ = mock_create_org.call_args
    payload = args[0]
    assert payload.name == f"Trial {expected_hash} test"
    # a DIFFERENT email typing the same org_name "test" gets a different hash → unique name
    other_hash = clone_service.email_hash8("other@b.org")
    assert other_hash != expected_hash


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_slug_is_email_hash_unique(mock_create_org, mock_create_plan):
    """create_organization derives org.slug = slugify(org.name)[:20]. The email hash sits right
    after "Trial " so it always survives the 20-char truncation → the slug stays unique per email
    even when a LONG org_name would otherwise push everything else out of the 20-char window. This
    is why the hash must lead the name and not trail it."""
    Role.objects.get_or_create(slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1})
    template = Org.objects.create(name="Health Demo Org", slug="tmpl-long")
    email = "chf@b.org"
    expected_hash = clone_service.email_hash8(email)

    from django.utils.text import slugify

    def fake_create_org(payload):
        trial_org = Org.objects.create(
            name=payload.name, slug=slugify(payload.name)[:20], airbyte_workspace_id="ws-chf"
        )
        return trial_org, None

    mock_create_org.side_effect = fake_create_org
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email=email, org_name="A Very Long Org Name Here")
    clone_service._step_org_and_user(run)

    payload = mock_create_org.call_args.args[0]
    assert payload.name == f"Trial {expected_hash} A Very Long Org Name Here"[:50]
    trial_org = run.trial_org
    assert len(trial_org.slug) <= 20
    # hash survives the 20-char slug cut despite the long org_name → slug unique per email
    assert trial_org.slug.startswith(f"trial-{expected_hash}")


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_defaults_to_trial_name_when_no_org_name(
    mock_create_org, mock_create_plan
):
    """Without org_name, the name falls back to Trial <hash> <template.name>."""
    Role.objects.get_or_create(slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1})
    template = Org.objects.create(name="tmpl-default-name", slug="tmpl-default-name")
    trial_org = Org.objects.create(
        name="Trial default", slug="trial-default", airbyte_workspace_id="ws-13"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email="default@b.org")
    clone_service._step_org_and_user(run)

    args, _ = mock_create_org.call_args
    payload = args[0]
    expected_hash = clone_service.email_hash8("default@b.org")
    assert payload.name == f"Trial {expected_hash} {template.name}"[:50]


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_uses_given_role_slug(mock_create_org, mock_create_plan):
    Role.objects.get_or_create(slug="custom-role", defaults={"name": "custom", "level": 2})
    template = Org.objects.create(name="tmpl-role", slug="tmpl-role")
    trial_org = Org.objects.create(
        name="Trial 1 tmpl-role", slug="trial-1-tmpl-role", airbyte_workspace_id="ws-12"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email="role@b.org", role_slug="custom-role")
    clone_service._step_org_and_user(run)

    orguser = OrgUser.objects.filter(org=trial_org).first()
    assert orguser is not None
    assert orguser.new_role.slug == "custom-role"


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_raises_when_given_role_slug_not_found(mock_create_org, mock_create_plan):
    template = Org.objects.create(name="tmpl-role-missing", slug="tmpl-role-missing")
    trial_org = Org.objects.create(
        name="Trial role missing", slug="trial-role-missing", airbyte_workspace_id="ws-14"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email="rolemissing@b.org", role_slug="no-such-role")
    with pytest.raises(RuntimeError, match="role"):
        clone_service._step_org_and_user(run)


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_is_idempotent_for_userattributes(mock_create_org, mock_create_plan):
    """A recurring trial_email makes User.objects.get_or_create return an existing User —
    UserAttributes.objects.create would then raise/duplicate; get_or_create must not."""
    Role.objects.get_or_create(slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1})
    template = Org.objects.create(name="tmpl-idem", slug="tmpl-idem")
    existing_user = User.objects.create(username="repeat@b.org", email="repeat@b.org")
    UserAttributes.objects.create(user=existing_user, email_verified=True)

    trial_org = Org.objects.create(
        name="Trial 2 tmpl", slug="trial-2-tmpl", airbyte_workspace_id="ws-10"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    run = CloneRun(template=template, trial_email="repeat@b.org")
    clone_service._step_org_and_user(run)

    rows = UserAttributes.objects.filter(user=existing_user)
    assert rows.count() == 1
    # get_or_create must not overwrite the pre-existing row's fields via `defaults`.
    assert rows.first().email_verified is True


@patch("ddpui.core.trial.clone_service.settings")
@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
def test_step_warehouse_registers_trial_warehouse(
    mock_provision, mock_retrieve, mock_ab, mock_create_wh, mock_settings
):
    mock_settings.TRIALS_RDS_HOST = "trials-rds-host"
    template = Org.objects.create(name="tmpl", slug="tmpl", airbyte_workspace_id="ws-tmpl")
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl", credentials="x"
    )
    trial_org = Org.objects.create(
        name="Trial 1 tmpl", slug="trial-1", airbyte_workspace_id="ws-tr"
    )

    mock_retrieve.return_value = {
        "host": "trials-rds-host",
        "port": 5432,
        "database": "tmpl_db",
        "username": "tmpl_u",
        "password": "tmpl_p",
    }
    mock_provision.return_value = {
        "host": "h",
        "port": 5432,
        "database": "trial_1",
        "username": "u",
        "password": "p",
    }
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_create_wh.return_value = (None, None)

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_warehouse(run)

    mock_provision.assert_called_once_with(run.trial_email, template_db="tmpl_db")
    # create_warehouse called with the trial org + a schema carrying the new db + def id
    args, _ = mock_create_wh.call_args
    assert args[0] == trial_org
    assert args[1].destinationDefId == "pg-def-1"
    assert args[1].airbyteConfig["database"] == "trial_1"
    assert run.manifest["trial_warehouse_db"] == "trial_1"
    assert run.manifest["trial_warehouse_role"] == "u"


@patch("ddpui.core.trial.clone_service.settings")
@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
def test_step_warehouse_drops_template_ssh_tunnel_config(
    mock_provision, mock_retrieve, mock_ab, mock_create_wh, mock_settings
):
    """The template's SSH-tunnel config points at the template's bastion — it must not be
    carried into the trial warehouse's Airbyte destination, which points at the trials-RDS
    host with no such tunnel."""
    mock_settings.TRIALS_RDS_HOST = "tmpl-host"
    template = Org.objects.create(name="tmpl", slug="tmpl", airbyte_workspace_id="ws-tmpl")
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl", credentials="x"
    )
    trial_org = Org.objects.create(
        name="Trial 1 tmpl", slug="trial-1", airbyte_workspace_id="ws-tr"
    )

    mock_provision.return_value = {
        "host": "h",
        "port": 5432,
        "database": "trial_1",
        "username": "u",
        "password": "p",
    }
    mock_retrieve.return_value = {
        "host": "tmpl-host",
        "port": 5432,
        "database": "tmpl_db",
        "username": "tmpl_u",
        "password": "tmpl_p",
        "schema": "public",
        "ssl_mode": {"mode": "require"},
        "tunnel_method": {
            "tunnel_method": "SSH_KEY_AUTH",
            "tunnel_host": "bastion.tmpl",
            "tunnel_port": 22,
            "tunnel_user": "ec2-user",
            "ssh_key": "-----BEGIN...",
        },
    }
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_create_wh.return_value = (None, None)

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_warehouse(run)

    args, _ = mock_create_wh.call_args
    config = args[1].airbyteConfig
    assert "tunnel_method" not in config
    assert config["schema"] == "public"
    assert config["host"] == "h"
    assert config["database"] == "trial_1"


@patch("ddpui.core.trial.clone_service.settings")
@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
def test_step_warehouse_uses_server_side_copy_when_same_instance(
    mock_provision, mock_retrieve, mock_ab, mock_create_wh, mock_settings
):
    """provision_trial_database must be called with template_db=<template's db name> so it does
    a server-side CREATE DATABASE ... TEMPLATE ... copy of the template warehouse."""
    mock_settings.TRIALS_RDS_HOST = "same-host"
    template = Org.objects.create(
        name="tmpl-same", slug="tmpl-same", airbyte_workspace_id="ws-tmpl-same"
    )
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl-same", credentials="x"
    )
    trial_org = Org.objects.create(
        name="Trial same", slug="trial-same", airbyte_workspace_id="ws-tr-same"
    )

    mock_retrieve.return_value = {
        "host": "same-host",
        "port": 5432,
        "database": "himanshu_wh",
        "username": "tmpl_u",
        "password": "tmpl_p",
        "schema": "public",
    }
    mock_provision.return_value = {
        "host": "same-host",
        "port": 5432,
        "database": "trial_same_db",
        "username": "u",
        "password": "p",
    }
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_create_wh.return_value = (None, None)

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_warehouse(run)

    mock_provision.assert_called_once_with(run.trial_email, template_db="himanshu_wh")


@patch("ddpui.core.trial.clone_service.settings")
@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
def test_step_warehouse_raises_when_template_on_different_instance(
    mock_provision, mock_retrieve, mock_ab, mock_create_wh, mock_settings
):
    """The template warehouse MUST live on the trials-RDS instance — there is no cross-host
    dump/restore fallback, so a different host must fail loudly before provisioning anything."""
    mock_settings.TRIALS_RDS_HOST = "trials-rds-host"
    template = Org.objects.create(
        name="tmpl-diff", slug="tmpl-diff", airbyte_workspace_id="ws-tmpl-diff"
    )
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl-diff", credentials="x"
    )
    trial_org = Org.objects.create(
        name="Trial diff", slug="trial-diff", airbyte_workspace_id="ws-tr-diff"
    )

    mock_retrieve.return_value = {
        "host": "template-own-host",
        "port": 5432,
        "database": "himanshu_wh",
        "username": "tmpl_u",
        "password": "tmpl_p",
        "schema": "public",
    }

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    with pytest.raises(RuntimeError, match="trials-RDS"):
        clone_service._step_warehouse(run)

    mock_provision.assert_not_called()
    mock_create_wh.assert_not_called()


def test_account_exists_for_email_true_when_user_exists():
    """A real account = a User WITH at least one OrgUser."""
    org = Org.objects.create(name="guard-org", slug="guard-org")
    _make_orguser(org, "dup@x.org")
    assert clone_service.account_exists_for_email("dup@x.org") is True
    assert clone_service.account_exists_for_email("new@x.org") is False


def test_account_exists_true_only_with_orguser():
    """A bare Django User with zero OrgUsers (e.g. left dangling by a failed/reaped trial
    clone) must NOT count as an existing account — only a User WITH an OrgUser does."""
    org = Org.objects.create(name="guard-org2", slug="guard-org2")
    _make_orguser(org, "has-account@x.org")
    assert clone_service.account_exists_for_email("has-account@x.org") is True

    User.objects.create(username="dangling@x.org", email="dangling@x.org")
    assert clone_service.account_exists_for_email("dangling@x.org") is False

    assert clone_service.account_exists_for_email("never-existed@x.org") is False


@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_rejects_existing_account(mock_step1):
    org = Org.objects.create(name="guard-existing-org", slug="guard-existing-org")
    _make_orguser(org, "dup@x.org")
    template = Org.objects.create(name="tmpl-guard", slug="tmpl-guard")

    with pytest.raises(TrialAccountExistsError):
        clone_service.clone_template_org(template.id, "dup@x.org")

    mock_step1.assert_not_called()
    # the guard fires before any resource (trial org, etc.) is created beyond the
    # pre-existing template + guard orgs.
    assert Org.objects.exclude(id__in=[template.id, org.id]).count() == 0


@patch("ddpui.core.trial.clone_service.validate_template_source_configs", return_value=[])
@patch("ddpui.core.trial.clone_service.load_template_source_config")
@patch("ddpui.core.trial.clone_service.airbyte_service")
def test_step_sources_recreates_from_config(mock_ab, mock_load, mock_validate):
    template = Org.objects.create(name="tmpl-src", slug="tmpl-src", airbyte_workspace_id="ws-t")
    trial_org = Org.objects.create(name="Trial src", slug="trial-src", airbyte_workspace_id="ws-r")
    mock_ab.get_sources.return_value = {
        "sources": [
            {"sourceId": "old-1", "name": "PG", "sourceDefinitionId": "def-pg"},
        ]
    }
    mock_load.return_value = {"host": "h", "password": "real"}
    mock_ab.create_source.return_value = {"sourceId": "new-1"}
    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_sources(run)
    mock_ab.create_source.assert_called_once_with(
        "ws-r", "PG", "def-pg", {"host": "h", "password": "real"}
    )
    assert run.manifest["source_map"] == {"old-1": "new-1"}
    assert run.manifest["source_ids"] == ["new-1"]


@patch("ddpui.core.trial.clone_service.validate_template_source_configs", return_value=["PG"])
@patch("ddpui.core.trial.clone_service.airbyte_service")
def test_step_sources_fails_on_missing_config(mock_ab, mock_validate):
    template = Org.objects.create(name="tmpl-src2", slug="tmpl-src2", airbyte_workspace_id="ws-t")
    trial_org = Org.objects.create(
        name="Trial src2", slug="trial-src2", airbyte_workspace_id="ws-r"
    )
    mock_ab.get_sources.return_value = {
        "sources": [{"sourceId": "o", "name": "PG", "sourceDefinitionId": "d"}]
    }
    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    with pytest.raises(RuntimeError, match="missing source config"):
        clone_service._step_sources(run)


@patch("ddpui.core.trial.clone_service.ab_create_connection")
@patch("ddpui.core.trial.clone_service.airbyte_service")
def test_step_connections_mirrors_template_stream_selection(mock_ab, mock_create_conn):
    """template connection's sourceId must be remapped via source_map, the catalog must be
    re-discovered on the NEW source, but the built payload must select ONLY the streams the
    TEMPLATE connection had selected (here: "table_a", not "table_b") even though the freshly
    discovered catalog contains both — a clone must mirror the template's scope, not over-sync
    the source's whole schema. Selected streams are still normalized to full_refresh/overwrite,
    and connection_map is built from the (res, err) tuple."""
    template = Org.objects.create(name="tmpl-conn", slug="tmpl-conn", airbyte_workspace_id="ws-t")
    trial_org = Org.objects.create(
        name="Trial conn", slug="trial-conn", airbyte_workspace_id="ws-r"
    )
    mock_ab.get_webbackend_connections.return_value = [
        {
            "connectionId": "old-conn-1",
            "name": "PG -> warehouse",
            "source": {"sourceId": "old-1"},
            "namespaceFormat": None,
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {"name": "table_a"},
                        "config": {"selected": True},
                    },
                    {
                        "stream": {"name": "table_b"},
                        "config": {"selected": False},
                    },
                ]
            },
        }
    ]
    mock_ab.get_source_schema_catalog.return_value = {
        "catalogId": "cat-new-1",
        "catalog": {
            "streams": [
                {
                    "stream": {"name": "table_a"},
                    "config": {
                        "selected": True,
                        "syncMode": "incremental",
                        "destinationSyncMode": "append_dedup",
                        "cursorField": ["updated_at"],
                        "primaryKey": [["id"]],
                    },
                },
                {
                    "stream": {"name": "table_b"},
                    "config": {
                        "selected": True,
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                        "cursorField": [],
                        "primaryKey": [],
                    },
                },
            ]
        },
    }
    mock_create_conn.return_value = ({"connectionId": "new-conn-1"}, None)

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    run.manifest["source_map"] = {"old-1": "new-1"}

    clone_service._step_connections(run)

    mock_ab.get_source_schema_catalog.assert_called_once_with("ws-r", "new-1")
    mock_create_conn.assert_called_once()
    args, _ = mock_create_conn.call_args
    assert args[0] == trial_org
    payload = args[1]
    assert payload.sourceId == "new-1"
    assert payload.catalogId == "cat-new-1"
    assert payload.streams == [
        {
            "name": "table_a",
            "selected": True,
            "syncMode": "full_refresh",
            "destinationSyncMode": "overwrite",
            "cursorField": [],
            "primaryKey": [],
        }
    ]
    assert run.manifest["connection_map"] == {"old-conn-1": "new-conn-1"}
    assert run.manifest["connection_ids"] == ["new-conn-1"]


@patch("ddpui.core.trial.clone_service.ab_create_connection")
@patch("ddpui.core.trial.clone_service.airbyte_service")
def test_step_connections_falls_back_to_all_streams_when_template_has_no_selection(
    mock_ab, mock_create_conn
):
    """If the template connection exposes no syncCatalog selection info at all (empty set),
    fall back to selecting every discovered stream so a clone never ends up with zero
    streams selected."""
    template = Org.objects.create(
        name="tmpl-conn-noselect", slug="tmpl-conn-noselect", airbyte_workspace_id="ws-t"
    )
    trial_org = Org.objects.create(
        name="Trial conn noselect", slug="trial-conn-noselect", airbyte_workspace_id="ws-r"
    )
    mock_ab.get_webbackend_connections.return_value = [
        {
            "connectionId": "old-conn-1",
            "name": "PG -> warehouse",
            "source": {"sourceId": "old-1"},
            "namespaceFormat": None,
            "syncCatalog": {"streams": []},
        }
    ]
    mock_ab.get_source_schema_catalog.return_value = {
        "catalogId": "cat-new-1",
        "catalog": {
            "streams": [
                {"stream": {"name": "table_a"}, "config": {"selected": True}},
                {"stream": {"name": "table_b"}, "config": {"selected": True}},
            ]
        },
    }
    mock_create_conn.return_value = ({"connectionId": "new-conn-1"}, None)

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    run.manifest["source_map"] = {"old-1": "new-1"}

    clone_service._step_connections(run)

    args, _ = mock_create_conn.call_args
    payload = args[1]
    stream_names = {s["name"] for s in payload.streams}
    assert stream_names == {"table_a", "table_b"}
    for s in payload.streams:
        assert s["selected"] is True
        assert s["syncMode"] == "full_refresh"
        assert s["destinationSyncMode"] == "overwrite"


@patch("ddpui.core.trial.clone_service.ab_create_connection")
@patch("ddpui.core.trial.clone_service.airbyte_service")
def test_step_connections_raises_when_source_not_remapped(mock_ab, mock_create_conn):
    template = Org.objects.create(name="tmpl-conn2", slug="tmpl-conn2", airbyte_workspace_id="ws-t")
    trial_org = Org.objects.create(
        name="Trial conn2", slug="trial-conn2", airbyte_workspace_id="ws-r"
    )
    mock_ab.get_webbackend_connections.return_value = [
        {"connectionId": "old-conn-1", "name": "PG -> wh", "source": {"sourceId": "unmapped"}}
    ]
    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    run.manifest["source_map"] = {}
    with pytest.raises(RuntimeError, match="no remapped source"):
        clone_service._step_connections(run)
    mock_create_conn.assert_not_called()


def _make_orguser(org: Org, email: str) -> OrgUser:
    user = User.objects.create(username=email, email=email)
    return OrgUser.objects.create(user=user, org=org, email_verified=False)


def _make_orgdbt(slug: str) -> OrgDbt:
    return OrgDbt.objects.create(
        gitrepo_url=f"https://github.com/dalgo/{slug}.git",
        project_dir=f"test_project_dir_{slug}",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )


@patch("ddpui.core.trial.clone_service.DbtProjectManager")
@patch("ddpui.core.trial.clone_service.create_default_transform_tasks")
@patch("ddpui.core.trial.clone_service.regenerate_and_push")
@patch("ddpui.core.trial.clone_service.setup_managed_git_workspace")
def test_step_dbt_uses_regen_path_for_operation_built_template(
    mock_setup, mock_regen, mock_create_transform_tasks, mock_dbt_project_manager
):
    """template dbt WITH an operation -> uses the existing regen path (nulls sql_path);
    guards the branch that must still work for UI-operation-built template orgs. The dbt
    system OrgTasks (git-pull/dbt-clean/dbt-deps/...) must still be created for the trial
    org afterwards, mirroring how a normal org's dbt setup wires transform tasks."""
    template = Org.objects.create(name="tmpl-dbt", slug="tmpl-dbt")
    trial_org = Org.objects.create(name="Trial dbt", slug="trial-dbt")

    template_dbt = _make_orgdbt("tmpl-dbt")
    template.dbt = template_dbt
    template.save()
    dest_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="customers",
        display_name="customers",
        schema="analytics",
        sql_path="models/analytics/customers.sql",
        type=OrgDbtModelType.MODEL,
    )
    OrgDbtOperation.objects.create(
        dbtmodel=dest_model,
        uuid=uuid_module.uuid4(),
        seq=1,
        output_cols=["id"],
        config={"type": "rename"},
    )

    cli_profile_block = OrgPrefectBlockv1.objects.create(
        org=trial_org,
        block_type=DBTCLIPROFILE,
        block_id="trial-dbt-cli-block-id",
        block_name="trial-dbt-cli-block-name",
    )
    trial_dbt = _make_orgdbt("trial-dbt")
    trial_dbt.cli_profile_block = cli_profile_block
    trial_dbt.save()

    def fake_setup(org, project_name, default_schema):
        # mirror setup_managed_git_workspace's real behaviour: mutate org.dbt in place.
        org.dbt = trial_dbt
        org.save()

    mock_setup.side_effect = fake_setup
    mock_regen.return_value = 1
    gathered_params = Mock(spec=DbtProjectParams)
    mock_dbt_project_manager.gather_dbt_project_params.return_value = gathered_params

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_dbt(run)

    mock_setup.assert_called_once_with(
        trial_org, project_name="dbtrepo", default_schema="default_schema"
    )
    mock_regen.assert_called_once_with(trial_org, trial_dbt)
    assert OrgDbtModel.objects.filter(orgdbt=trial_dbt).count() == 1
    # regen path nulls sql_path — the trial's scaffold has no files yet at this point
    assert OrgDbtModel.objects.filter(orgdbt=trial_dbt).first().sql_path is None
    assert run.manifest["dbt_repo"] == trial_dbt.gitrepo_url
    assert run.manifest["dbt_models"] == 1
    assert run.manifest["dbt_regenerated"] == 1

    mock_dbt_project_manager.gather_dbt_project_params.assert_called_once_with(trial_org, trial_dbt)
    mock_create_transform_tasks.assert_called_once_with(
        trial_org, cli_profile_block, gathered_params
    )
    assert run.manifest["dbt_transform_tasks_created"] is True


@patch("ddpui.core.trial.clone_service.DbtProjectManager")
@patch("ddpui.core.trial.clone_service.create_default_transform_tasks")
@patch("ddpui.core.trial.clone_service.copy_dbt_repo_files")
@patch("ddpui.core.trial.clone_service.setup_managed_git_workspace")
def test_step_dbt_uses_copy_path_for_file_based_template(
    mock_setup, mock_copy_files, mock_create_transform_tasks, mock_dbt_project_manager
):
    """template dbt with models that have sql_path set and ZERO operations (github/file-based,
    like health_org) -> uses the COPY path (copy_dbt_repo_files), NOT regenerate_and_push; DAG
    rows are copied with sql_path PRESERVED. The dbt system OrgTasks must still be created for
    the trial org afterwards, same as the regen path."""
    template = Org.objects.create(name="tmpl-filedbt", slug="tmpl-filedbt")
    trial_org = Org.objects.create(name="Trial filedbt", slug="trial-filedbt")

    template_dbt = _make_orgdbt("tmpl-filedbt")
    template.dbt = template_dbt
    template.save()
    template_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="customers",
        display_name="customers",
        schema="analytics",
        sql_path="models/analytics/customers.sql",
        type=OrgDbtModelType.MODEL,
    )

    cli_profile_block = OrgPrefectBlockv1.objects.create(
        org=trial_org,
        block_type=DBTCLIPROFILE,
        block_id="trial-filedbt-cli-block-id",
        block_name="trial-filedbt-cli-block-name",
    )
    trial_dbt = _make_orgdbt("trial-filedbt")
    trial_dbt.cli_profile_block = cli_profile_block
    trial_dbt.save()

    def fake_setup(org, project_name, default_schema):
        org.dbt = trial_dbt
        org.save()

    mock_setup.side_effect = fake_setup
    gathered_params = Mock(spec=DbtProjectParams)
    mock_dbt_project_manager.gather_dbt_project_params.return_value = gathered_params

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)

    with patch("ddpui.core.trial.clone_service.regenerate_and_push") as mock_regen:
        clone_service._step_dbt(run)

    mock_regen.assert_not_called()
    mock_copy_files.assert_called_once_with(template_dbt, trial_dbt)

    trial_model = OrgDbtModel.objects.get(orgdbt=trial_dbt)
    assert trial_model.sql_path == template_model.sql_path
    assert run.manifest["dbt_repo"] == trial_dbt.gitrepo_url
    assert run.manifest["dbt_models"] == 1

    mock_dbt_project_manager.gather_dbt_project_params.assert_called_once_with(trial_org, trial_dbt)
    mock_create_transform_tasks.assert_called_once_with(
        trial_org, cli_profile_block, gathered_params
    )
    assert run.manifest["dbt_transform_tasks_created"] is True


def test_step_dbt_raises_when_template_has_no_dbt():
    template = Org.objects.create(name="tmpl-nodbt", slug="tmpl-nodbt")
    trial_org = Org.objects.create(name="Trial nodbt", slug="trial-nodbt")
    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    with pytest.raises(RuntimeError, match="no dbt workspace"):
        clone_service._step_dbt(run)


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_wires_step_dbt_after_connections(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7
):
    template = Org.objects.create(name="tmpl-wire", slug="tmpl-wire")
    run = clone_service.clone_template_org(template.id, "a@b.org")
    mock_s5.assert_called_once()
    assert "step5_dbt" in run.timings


@patch("ddpui.core.trial.clone_service.clone_orchestrate_dataflows")
def test_step_prefect_delegates_and_records_deployment_ids(mock_clone_dataflows):
    template = Org.objects.create(name="tmpl-pf", slug="tmpl-pf")
    trial_org = Org.objects.create(name="Trial pf", slug="trial-pf")
    mock_clone_dataflows.return_value = ["dep-new"]

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    run.manifest["connection_map"] = {"tmpl-conn-1": "trial-conn-1"}
    clone_service._step_prefect(run)

    mock_clone_dataflows.assert_called_once_with(
        template, trial_org, {"tmpl-conn-1": "trial-conn-1"}
    )
    assert run.manifest["deployment_ids"] == ["dep-new"]


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_wires_step_prefect_after_dbt(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7
):
    template = Org.objects.create(name="tmpl-wire-pf", slug="tmpl-wire-pf")
    run = clone_service.clone_template_org(template.id, "a@b.org")
    mock_s6.assert_called_once()
    assert "step6_prefect" in run.timings
    timing_keys = list(run.timings.keys())
    assert timing_keys.index("step6_prefect") == timing_keys.index("step5_dbt") + 1


@patch("ddpui.core.trial.clone_service.clone_viz")
def test_step_viz_delegates_and_records_manifest(mock_clone_viz):
    template = Org.objects.create(name="tmpl-viz", slug="tmpl-viz")
    trial_org = Org.objects.create(name="Trial viz", slug="trial-viz")
    trial_user = _make_orguser(trial_org, "trial-viz-admin@x.org")
    mock_clone_viz.return_value = {"metrics": 2, "kpis": 1}

    run = CloneRun(
        template=template, trial_email="a@b.org", trial_org=trial_org, trial_orguser=trial_user
    )
    clone_service._step_viz(run)

    mock_clone_viz.assert_called_once_with(template, trial_org, trial_user)
    assert run.manifest["viz"] == {"metrics": 2, "kpis": 1}


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_wires_step_viz_last(mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7):
    template = Org.objects.create(name="tmpl-wire-viz", slug="tmpl-wire-viz")
    run = clone_service.clone_template_org(template.id, "a@b.org")
    mock_s7.assert_called_once()
    assert "step7_viz" in run.timings
    assert list(run.timings.keys())[-1] == "step7_viz"


@patch("ddpui.core.trial.clone_service.OrgCleanupService")
def test_delete_trial_org_reaps_kpis_metrics_before_delete_org(mock_cleanup_cls):
    """Metric.org is PROTECT and KPIs PROTECT their Metric, so delete_trial_org must remove KPIs
    then Metrics before OrgCleanupService.delete_org() — otherwise org.delete() raises
    ProtectedError and leaves an orphan whose name blocks the next clone."""
    from ddpui.models.metric import Metric, KPI

    org = Org.objects.create(name="Trial reap", slug="trial-reap")
    metric = Metric.objects.create(
        org=org, name="m1", schema_name="s", table_name="t", column="c", aggregation="sum"
    )
    KPI.objects.create(org=org, metric=metric, name="k1", extra_config={})

    clone_service.delete_trial_org(org)

    assert KPI.objects.filter(org=org).count() == 0
    assert Metric.objects.filter(org=org).count() == 0
    mock_cleanup_cls.assert_called_once_with(org, dry_run=False)
    mock_cleanup_cls.return_value.delete_org.assert_called_once()
