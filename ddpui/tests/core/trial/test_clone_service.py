import os
from unittest.mock import patch, Mock

import pytest
from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.trial import clone_service
from ddpui.core.trial.clone_service import CloneRun
from ddpui.core.trial.exceptions import TrialAccountExistsError

pytestmark = pytest.mark.django_db


@patch("ddpui.core.trial.clone_service._step_viz")
@patch("ddpui.core.trial.clone_service._step_prefect")
@patch("ddpui.core.trial.clone_service._step_dbt")
@patch("ddpui.core.trial.clone_service._step_connections")
@patch("ddpui.core.trial.clone_service._step_sources")
@patch("ddpui.core.trial.clone_service._step_warehouse_data")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_runs_all_steps_and_completes(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7, mock_s8
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
    mock_s8.assert_called_once()
    assert set(run.timings.keys()) == {
        "step1_org_user",
        "step2_warehouse",
        "step3_warehouse_data",
        "step4_sources",
        "step5_connections",
        "step6_dbt",
        "step7_prefect",
        "step8_viz",
    }


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
@patch("ddpui.core.trial.clone_service._step_warehouse_data", side_effect=RuntimeError("boom"))
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
    mock_retrieve.return_value = {}
    mock_create_wh.return_value = (None, "create_warehouse blew up")
    mock_cleanup_instance = mock_cleanup_cls.return_value

    with pytest.raises(RuntimeError, match="create_warehouse failed"):
        clone_service.clone_template_org(template.id, "a@b.org")

    assert captured["run"].manifest["trial_warehouse_db"] == "trial_z_db"

    mock_drop.assert_called_once_with("a@b.org")
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

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_warehouse(run)

    mock_provision.assert_called_once_with(run.trial_email)
    # create_warehouse called with the trial org + a schema carrying the new db + def id
    args, _ = mock_create_wh.call_args
    assert args[0] == trial_org
    assert args[1].destinationDefId == "pg-def-1"
    assert args[1].airbyteConfig["database"] == "trial_1"
    assert run.manifest["trial_warehouse_db"] == "trial_1"
    assert run.manifest["trial_warehouse_role"] == "u"


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

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_warehouse(run)

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

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_warehouse_data(run)

    mock_copy.assert_called_once()
    src, dst, dump_path = mock_copy.call_args.args
    assert src["database"] == "sdb"
    assert dst["database"] == "trial_1"

    assert run.manifest["warehouse_dump_path"] == dump_path
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
    # the guard fires before any resource (trial org, etc.) is created
    assert Org.objects.exclude(id=template.id).count() == 0


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

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    with pytest.raises(RuntimeError):
        clone_service._step_warehouse_data(run)

    dump_path = mock_copy.call_args.args[2]
    assert not os.path.exists(dump_path)


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


@patch("ddpui.core.trial.clone_service.regenerate_and_push")
@patch("ddpui.core.trial.clone_service.setup_managed_git_workspace")
def test_step_dbt_sets_up_workspace_then_copies(mock_setup, mock_regen):
    template = Org.objects.create(name="tmpl-dbt", slug="tmpl-dbt")
    trial_org = Org.objects.create(name="Trial dbt", slug="trial-dbt")

    template_dbt = _make_orgdbt("tmpl-dbt")
    template.dbt = template_dbt
    template.save()
    OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="customers",
        display_name="customers",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )

    trial_dbt = _make_orgdbt("trial-dbt")

    def fake_setup(org, project_name, default_schema):
        # mirror setup_managed_git_workspace's real behaviour: mutate org.dbt in place.
        org.dbt = trial_dbt
        org.save()

    mock_setup.side_effect = fake_setup
    mock_regen.return_value = 1

    run = CloneRun(template=template, trial_email="a@b.org", trial_org=trial_org)
    clone_service._step_dbt(run)

    mock_setup.assert_called_once_with(
        trial_org, project_name=trial_org.slug, default_schema="default_schema"
    )
    mock_regen.assert_called_once_with(trial_org, trial_dbt)
    assert OrgDbtModel.objects.filter(orgdbt=trial_dbt).count() == 1
    assert run.manifest["dbt_repo"] == trial_dbt.gitrepo_url
    assert run.manifest["dbt_models"] == 1
    assert run.manifest["dbt_regenerated"] == 1


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
@patch("ddpui.core.trial.clone_service._step_warehouse_data")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_wires_step_dbt_after_connections(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7, mock_s8
):
    template = Org.objects.create(name="tmpl-wire", slug="tmpl-wire")
    run = clone_service.clone_template_org(template.id, "a@b.org")
    mock_s6.assert_called_once()
    assert "step6_dbt" in run.timings


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
@patch("ddpui.core.trial.clone_service._step_warehouse_data")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_wires_step_prefect_after_dbt(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7, mock_s8
):
    template = Org.objects.create(name="tmpl-wire-pf", slug="tmpl-wire-pf")
    run = clone_service.clone_template_org(template.id, "a@b.org")
    mock_s7.assert_called_once()
    assert "step7_prefect" in run.timings
    timing_keys = list(run.timings.keys())
    assert timing_keys.index("step7_prefect") == timing_keys.index("step6_dbt") + 1


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
@patch("ddpui.core.trial.clone_service._step_warehouse_data")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_wires_step_viz_last(
    mock_s1, mock_s2, mock_s3, mock_s4, mock_s5, mock_s6, mock_s7, mock_s8
):
    template = Org.objects.create(name="tmpl-wire-viz", slug="tmpl-wire-viz")
    run = clone_service.clone_template_org(template.id, "a@b.org")
    mock_s8.assert_called_once()
    assert "step8_viz" in run.timings
    assert list(run.timings.keys())[-1] == "step8_viz"
