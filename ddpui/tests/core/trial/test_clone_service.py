import os
from unittest.mock import patch, Mock

import pytest
from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.trial_clone import TrialCloneStatus, TrialClone
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.trial import clone_service
from ddpui.core.trial.exceptions import TrialAccountExistsError

pytestmark = pytest.mark.django_db


@patch("ddpui.core.trial.clone_service._step_warehouse_data")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_runs_all_steps_and_completes(mock_s1, mock_s2, mock_s3):
    template = Org.objects.create(name="tmpl", slug="tmpl")
    tc = clone_service.clone_template_org(template.id, "a@b.org")
    assert tc.status == TrialCloneStatus.COMPLETED.value
    mock_s1.assert_called_once()
    mock_s2.assert_called_once()
    mock_s3.assert_called_once()
    assert set(tc.timings.keys()) == {
        "step1_org_user",
        "step2_warehouse",
        "step3_warehouse_data",
    }


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_org_and_user", side_effect=RuntimeError("kaboom"))
def test_clone_marks_failed_and_reraises(mock_s1, mock_cleanup_cls, mock_drop):
    template = Org.objects.create(name="tmpl2", slug="tmpl2")
    with pytest.raises(RuntimeError):
        clone_service.clone_template_org(template.id, "a@b.org")

    tc = TrialClone.objects.filter(template_org=template).first()
    assert tc.status == TrialCloneStatus.FAILED.value
    assert "kaboom" in tc.error
    # step1 (org+user) never got far enough to create a trial_org or warehouse — nothing to
    # tear down.
    mock_cleanup_cls.assert_not_called()
    mock_drop.assert_not_called()


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_warehouse_data", side_effect=RuntimeError("boom"))
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_tears_down_created_resources_on_later_failure(
    mock_s1, mock_s2, mock_s3, mock_cleanup_cls, mock_drop
):
    template = Org.objects.create(name="tmpl3", slug="tmpl3")
    trial_org = Org.objects.create(name="Trial X", slug="trial-x")

    def fake_step1(template_arg, trialclone):
        trialclone.trial_org = trial_org
        trialclone.save(update_fields=["trial_org", "updated_at"])

    def fake_step2(template_arg, trialclone):
        trialclone.manifest["trial_warehouse_db"] = "trial_123"
        trialclone.save(update_fields=["manifest", "updated_at"])

    mock_s1.side_effect = fake_step1
    mock_s2.side_effect = fake_step2
    mock_cleanup_instance = mock_cleanup_cls.return_value

    with pytest.raises(RuntimeError):
        clone_service.clone_template_org(template.id, "a@b.org")

    tc = TrialClone.objects.filter(template_org=template).first()
    assert tc.status == TrialCloneStatus.FAILED.value
    assert "boom" in tc.error

    mock_cleanup_cls.assert_called_once_with(trial_org, dry_run=False)
    mock_cleanup_instance.delete_org.assert_called_once()
    mock_drop.assert_called_once_with(tc.trial_email)


@patch("ddpui.core.trial.clone_service.drop_trial_database")
@patch("ddpui.core.trial.clone_service.OrgCleanupService")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_teardown_failure_does_not_mask_original_error(mock_s1, mock_cleanup_cls, mock_drop):
    """Teardown itself blowing up must never hide the original exception."""
    template = Org.objects.create(name="tmpl4", slug="tmpl4")
    trial_org = Org.objects.create(name="Trial Y", slug="trial-y")

    def fake_step1(template_arg, trialclone):
        trialclone.trial_org = trial_org
        trialclone.save(update_fields=["trial_org", "updated_at"])
        raise RuntimeError("original failure")

    mock_s1.side_effect = fake_step1
    mock_cleanup_cls.return_value.delete_org.side_effect = Exception("teardown exploded")

    with pytest.raises(RuntimeError, match="original failure"):
        clone_service.clone_template_org(template.id, "a@b.org")

    tc = TrialClone.objects.filter(template_org=template).first()
    assert tc.status == TrialCloneStatus.FAILED.value
    assert "original failure" in tc.error
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
    subsequent create_org_plan call fails. FIX 1 requires trialclone.trial_org to be
    persisted right after create_organization returns, so the teardown guard still fires
    even though the failure happened later in the same step."""
    template = Org.objects.create(name="tmpl6", slug="tmpl6")
    trial_org = Org.objects.create(
        name="Trial 1 tmpl6", slug="trial-1-tmpl6", airbyte_workspace_id="ws-6"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (None, "plan blew up")
    mock_cleanup_instance = mock_cleanup_cls.return_value

    with pytest.raises(RuntimeError, match="create_org_plan failed"):
        clone_service.clone_template_org(template.id, "a@b.org")

    tc = TrialClone.objects.filter(template_org=template).first()
    assert tc.status == TrialCloneStatus.FAILED.value
    assert tc.trial_org_id == trial_org.id

    mock_cleanup_cls.assert_called_once_with(trial_org, dry_run=False)
    mock_cleanup_instance.delete_org.assert_called_once()
    mock_drop.assert_not_called()


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
):
    """provision_trial_database succeeds (the RDS database now exists), but the
    subsequent create_warehouse call fails. FIX 2 requires manifest["trial_warehouse_db"]
    to be persisted right after provision_trial_database returns, so drop_trial_database
    still fires even though the failure happened later in the same step."""
    template = Org.objects.create(name="tmpl7", slug="tmpl7", airbyte_workspace_id="ws-tmpl7")
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl7", credentials="x"
    )
    trial_org = Org.objects.create(name="Trial Z", slug="trial-z", airbyte_workspace_id="ws-z")

    def fake_step1(template_arg, trialclone):
        trialclone.trial_org = trial_org
        trialclone.save(update_fields=["trial_org", "updated_at"])

    mock_s1.side_effect = fake_step1
    mock_provision.return_value = {
        "host": "h",
        "port": 5432,
        "database": "trial_z_db",
        "username": "u",
        "password": "p",
    }
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_retrieve.return_value = {}
    mock_create_wh.return_value = (None, "create_warehouse blew up")
    mock_cleanup_instance = mock_cleanup_cls.return_value

    with pytest.raises(RuntimeError, match="create_warehouse failed"):
        clone_service.clone_template_org(template.id, "a@b.org")

    tc = TrialClone.objects.filter(template_org=template).first()
    assert tc.status == TrialCloneStatus.FAILED.value
    assert tc.manifest["trial_warehouse_db"] == "trial_z_db"

    mock_drop.assert_called_once_with(tc.trial_email)
    mock_cleanup_cls.assert_called_once_with(trial_org, dry_run=False)
    mock_cleanup_instance.delete_org.assert_called_once()


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

    tc = TrialClone.objects.create(template_org=template, trial_email="admin@b.org")
    clone_service._step_org_and_user(template, tc)

    tc.refresh_from_db()
    assert tc.trial_org_id == trial_org.id
    orguser = OrgUser.objects.filter(org=trial_org).first()
    assert orguser is not None
    assert orguser.user.email == "admin@b.org"
    assert orguser.new_role.slug == ACCOUNT_MANAGER_ROLE
    assert not orguser.user.has_usable_password()
    user_attrs = UserAttributes.objects.filter(user=orguser.user).first()
    assert user_attrs is not None
    assert user_attrs.email_verified is False


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

    tc = TrialClone.objects.create(template_org=template, trial_email="repeat@b.org")
    clone_service._step_org_and_user(template, tc)

    rows = UserAttributes.objects.filter(user=existing_user)
    assert rows.count() == 1
    # get_or_create must not overwrite the pre-existing row's fields via `defaults`.
    assert rows.first().email_verified is True


@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
def test_step_warehouse_registers_trial_warehouse(
    mock_provision, mock_retrieve, mock_ab, mock_create_wh
):
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
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_create_wh.return_value = (None, None)

    tc = TrialClone.objects.create(
        template_org=template, trial_email="a@b.org", trial_org=trial_org
    )
    clone_service._step_warehouse(template, tc)

    mock_provision.assert_called_once_with(tc.trial_email)
    # create_warehouse called with the trial org + a schema carrying the new db + def id
    args, _ = mock_create_wh.call_args
    assert args[0] == trial_org
    assert args[1].destinationDefId == "pg-def-1"
    assert args[1].airbyteConfig["database"] == "trial_1"
    tc.refresh_from_db()
    assert tc.manifest["trial_warehouse_db"] == "trial_1"
    assert tc.manifest["trial_warehouse_role"] == "u"


@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
def test_step_warehouse_drops_template_ssh_tunnel_config(
    mock_provision, mock_retrieve, mock_ab, mock_create_wh
):
    """The template's SSH-tunnel config points at the template's bastion — it must not be
    carried into the trial warehouse's Airbyte destination, which points at the trials-RDS
    host with no such tunnel."""
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

    tc = TrialClone.objects.create(
        template_org=template, trial_email="a@b.org", trial_org=trial_org
    )
    clone_service._step_warehouse(template, tc)

    args, _ = mock_create_wh.call_args
    config = args[1].airbyteConfig
    assert "tunnel_method" not in config
    assert config["schema"] == "public"
    assert config["host"] == "h"
    assert config["database"] == "trial_1"


@patch("ddpui.core.trial.clone_service.copy_warehouse_data")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
def test_step_warehouse_data_copies(mock_retrieve, mock_copy):
    template = Org.objects.create(name="tmpl", slug="tmpl")
    trial_org = Org.objects.create(name="Trial 1 tmpl", slug="trial-1")
    OrgWarehouse.objects.create(org=template, wtype="postgres", credentials="tmpl-sec")
    OrgWarehouse.objects.create(org=trial_org, wtype="postgres", credentials="trial-sec")

    mock_retrieve.side_effect = [
        {"host": "sh", "port": 5432, "database": "sdb", "username": "su", "password": "sp"},
        {"host": "dh", "port": 5432, "database": "trial_1", "username": "du", "password": "dp"},
    ]

    tc = TrialClone.objects.create(
        template_org=template, trial_email="a@b.org", trial_org=trial_org
    )
    clone_service._step_warehouse_data(template, tc)

    mock_copy.assert_called_once()
    src, dst, dump_path = mock_copy.call_args.args
    assert src["database"] == "sdb"
    assert dst["database"] == "trial_1"

    tc.refresh_from_db()
    assert tc.manifest["warehouse_dump_path"] == dump_path
    # the pg_dump temp file must be removed after the (mocked) copy, success or failure —
    # it's a full copy of the template warehouse's data sitting on local disk.
    assert not os.path.exists(dump_path)


def test_account_exists_for_email_true_when_user_exists():
    User.objects.create(username="dup@x.org", email="dup@x.org")
    assert clone_service.account_exists_for_email("dup@x.org") is True
    assert clone_service.account_exists_for_email("new@x.org") is False


@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_rejects_existing_account(mock_step1):
    User.objects.create(username="dup@x.org", email="dup@x.org")
    template = Org.objects.create(name="tmpl-guard", slug="tmpl-guard")

    with pytest.raises(TrialAccountExistsError):
        clone_service.clone_template_org(template.id, "dup@x.org")

    mock_step1.assert_not_called()
    assert TrialClone.objects.filter(template_org=template).count() == 0


@patch("ddpui.core.trial.clone_service.copy_warehouse_data")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
def test_step_warehouse_data_removes_dump_file_even_on_failure(mock_retrieve, mock_copy):
    template = Org.objects.create(name="tmpl", slug="tmpl")
    trial_org = Org.objects.create(name="Trial 1 tmpl", slug="trial-1")
    OrgWarehouse.objects.create(org=template, wtype="postgres", credentials="tmpl-sec")
    OrgWarehouse.objects.create(org=trial_org, wtype="postgres", credentials="trial-sec")

    mock_retrieve.side_effect = [
        {"host": "sh", "port": 5432, "database": "sdb", "username": "su", "password": "sp"},
        {"host": "dh", "port": 5432, "database": "trial_1", "username": "du", "password": "dp"},
    ]
    mock_copy.side_effect = RuntimeError("pg_restore failed")

    tc = TrialClone.objects.create(
        template_org=template, trial_email="a@b.org", trial_org=trial_org
    )
    with pytest.raises(RuntimeError):
        clone_service._step_warehouse_data(template, tc)

    dump_path = mock_copy.call_args.args[2]
    assert not os.path.exists(dump_path)
