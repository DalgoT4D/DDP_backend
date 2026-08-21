import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier

import pytest
from django.db import close_old_connections
from django.utils import timezone
from ninja.errors import HttpError

from ddpui.api.transform_api import (
    delete_canvas_node,
    delete_orgdbtmodel,
    lock_canvas,
    post_add_operation_node,
    post_create_src_model_node,
    post_terminate_operation_node,
    put_operation_node,
    refresh_canvas_lock,
    sync_remote_dbtproject_to_canvas,
)
from ddpui.models.canvaslock import CanvasLock
from ddpui.models.org import OrgDbt, OrgWarehouse, TransformType
from ddpui.models.org_user import OrgUser
from ddpui.schemas.dbt_workflow_schema import (
    CreateOperationNodePayload,
    EditOperationNodePayload,
    TerminateChainAndCreateModelPayload,
)
from ddpui.tests.api_tests.test_user_org_api import (
    authuser,
    mock_request,
    nonadminorguser,
    org_without_workspace,
    orguser,
    seed_db,
)


pytestmark = pytest.mark.django_db


def _setup_workspace(orguser):
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="test_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.GIT,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save(update_fields=["dbt"])
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
    return orgdbt


def _create_lock(orgdbt, locked_by, *, expired=False):
    expiry_delta = timedelta(minutes=-1 if expired else 2)
    return CanvasLock.objects.create(
        dbt=orgdbt,
        locked_by=locked_by,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + expiry_delta,
    )


def test_acquire_replaces_an_expired_lock(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    expired_lock = _create_lock(orgdbt, orguser, expired=True)

    response = lock_canvas(mock_request(orguser))

    assert response.lock_token != expired_lock.lock_token
    assert CanvasLock.objects.filter(dbt=orgdbt, lock_token=response.lock_token).count() == 1


def test_refresh_reports_expired_lock(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    _create_lock(orgdbt, orguser, expired=True)

    with pytest.raises(HttpError) as excinfo:
        refresh_canvas_lock(mock_request(orguser))

    assert excinfo.value.status_code == 410


def test_refresh_reports_missing_lock(seed_db, orguser):
    _setup_workspace(orguser)

    with pytest.raises(HttpError) as excinfo:
        refresh_canvas_lock(mock_request(orguser))

    assert excinfo.value.status_code == 404


def test_refresh_rejects_another_users_lock(seed_db, orguser, nonadminorguser):
    orgdbt = _setup_workspace(orguser)
    _create_lock(orgdbt, nonadminorguser)

    with pytest.raises(HttpError) as excinfo:
        refresh_canvas_lock(mock_request(orguser))

    assert excinfo.value.status_code == 403


def _unlocked_mutations(request):
    create_payload = CreateOperationNodePayload(
        config={},
        input_node_uuid=str(uuid.uuid4()),
        op_type="aggregate",
        source_columns=["id"],
    )
    edit_payload = EditOperationNodePayload(
        config={},
        op_type="aggregate",
        source_columns=["id"],
    )
    terminate_payload = TerminateChainAndCreateModelPayload(
        name="new_model",
        display_name="New model",
        dest_schema="analytics",
    )
    node_uuid = str(uuid.uuid4())
    return [
        ("add source/model", lambda: post_create_src_model_node(request, node_uuid)),
        ("add operation", lambda: post_add_operation_node(request, create_payload)),
        ("edit operation", lambda: put_operation_node(request, node_uuid, edit_payload)),
        (
            "terminate operation",
            lambda: post_terminate_operation_node(request, node_uuid, terminate_payload),
        ),
        ("remove from canvas", lambda: delete_canvas_node(request, node_uuid)),
        ("delete dbt model", lambda: delete_orgdbtmodel(request, node_uuid)),
        ("sync remote graph", lambda: sync_remote_dbtproject_to_canvas(request)),
    ]


def test_every_v2_canvas_mutation_requires_a_lock(seed_db, orguser):
    _setup_workspace(orguser)
    request = mock_request(orguser)

    for mutation_name, mutate in _unlocked_mutations(request):
        with pytest.raises(HttpError) as excinfo:
            mutate()
        assert excinfo.value.status_code == 423, mutation_name


@pytest.mark.django_db(transaction=True)
def test_concurrent_same_user_acquisition_is_idempotent(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    barrier = Barrier(2)

    def acquire():
        close_old_connections()
        try:
            fresh_orguser = OrgUser.objects.select_related("org__dbt", "user", "new_role").get(
                pk=orguser.pk
            )
            barrier.wait()
            return lock_canvas(mock_request(fresh_orguser)).lock_token
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=2) as executor:
        tokens = list(executor.map(lambda _index: acquire(), range(2)))

    assert len(set(tokens)) == 1
    assert CanvasLock.objects.filter(dbt=orgdbt).count() == 1
