import uuid
from datetime import timedelta

import pytest
from django.utils import timezone
from ninja.errors import HttpError

from ddpui.api.transform_api import put_canvas_layout
from ddpui.core.dbtautomation_service import convert_canvas_node_to_frontend_format
from ddpui.models.canvas_models import CanvasNode, CanvasNodeType
from ddpui.models.canvaslock import CanvasLock
from ddpui.models.org import OrgDbt, TransformType
from ddpui.schemas.dbt_workflow_schema import UpdateCanvasLayoutPayload
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
    return orgdbt


def _lock_workspace(orgdbt, orguser):
    return CanvasLock.objects.create(
        dbt=orgdbt,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=2),
    )


def _payload(*updates):
    return UpdateCanvasLayoutPayload(
        nodes=[
            {
                "uuid": node_uuid,
                "position": {"x": x, "y": y},
            }
            for node_uuid, x, y in updates
        ]
    )


def test_canvas_node_serializer_returns_nullable_position(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source",
    )

    assert convert_canvas_node_to_frontend_format(node)["position"] is None

    node.position_x = -125.5
    node.position_y = 240.25
    node.save(update_fields=["position_x", "position_y"])

    assert convert_canvas_node_to_frontend_format(node)["position"] == {
        "x": -125.5,
        "y": 240.25,
    }


def test_put_canvas_layout_updates_batch_atomically(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    _lock_workspace(orgdbt, orguser)
    first = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="first",
    )
    second = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.OPERATION,
        name="second",
    )

    response = put_canvas_layout(
        mock_request(orguser),
        _payload((first.uuid, -10.5, 20.25), (second.uuid, 300.0, -400.0)),
    )

    first.refresh_from_db()
    second.refresh_from_db()
    assert response["updated"] == 2
    assert (first.position_x, first.position_y) == (-10.5, 20.25)
    assert (second.position_x, second.position_y) == (300.0, -400.0)


def test_put_canvas_layout_rejects_unknown_node_without_partial_update(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    _lock_workspace(orgdbt, orguser)
    node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source",
        position_x=1.0,
        position_y=2.0,
    )

    with pytest.raises(HttpError) as excinfo:
        put_canvas_layout(
            mock_request(orguser),
            _payload((node.uuid, 50.0, 60.0), (uuid.uuid4(), 70.0, 80.0)),
        )

    assert excinfo.value.status_code == 422
    node.refresh_from_db()
    assert (node.position_x, node.position_y) == (1.0, 2.0)


def test_put_canvas_layout_requires_owned_lock(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source",
    )

    with pytest.raises(HttpError) as excinfo:
        put_canvas_layout(mock_request(orguser), _payload((node.uuid, 10.0, 20.0)))

    assert excinfo.value.status_code == 423
    node.refresh_from_db()
    assert node.position_x is None
    assert node.position_y is None


def test_put_canvas_layout_rejects_another_users_lock(seed_db, orguser, nonadminorguser):
    orgdbt = _setup_workspace(orguser)
    _lock_workspace(orgdbt, nonadminorguser)
    node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source",
    )

    with pytest.raises(HttpError) as excinfo:
        put_canvas_layout(mock_request(orguser), _payload((node.uuid, 10.0, 20.0)))

    assert excinfo.value.status_code == 423
    node.refresh_from_db()
    assert node.position_x is None
    assert node.position_y is None


def test_put_canvas_layout_rejects_expired_lock(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    lock = _lock_workspace(orgdbt, orguser)
    lock.expires_at = timezone.now() - timedelta(seconds=1)
    lock.save(update_fields=["expires_at"])
    node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source",
    )

    with pytest.raises(HttpError) as excinfo:
        put_canvas_layout(mock_request(orguser), _payload((node.uuid, 10.0, 20.0)))

    assert excinfo.value.status_code == 410
    node.refresh_from_db()
    assert node.position_x is None
    assert node.position_y is None


def test_put_canvas_layout_rejects_duplicate_uuids(seed_db, orguser):
    orgdbt = _setup_workspace(orguser)
    _lock_workspace(orgdbt, orguser)
    node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source",
    )
    duplicate_updates = ((node.uuid, 1.0, 2.0), (node.uuid, 3.0, 4.0))

    with pytest.raises(HttpError) as excinfo:
        put_canvas_layout(mock_request(orguser), _payload(*duplicate_updates))

    assert excinfo.value.status_code == 422
    assert "duplicate" in str(excinfo.value)


@pytest.mark.parametrize(
    "x,y",
    [
        (float("nan"), 0.0),
        (0.0, float("inf")),
        (10_000_001.0, 0.0),
    ],
)
def test_put_canvas_layout_rejects_invalid_coordinates(seed_db, orguser, x, y):
    orgdbt = _setup_workspace(orguser)
    _lock_workspace(orgdbt, orguser)
    node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source",
    )

    with pytest.raises(HttpError) as excinfo:
        put_canvas_layout(mock_request(orguser), _payload((node.uuid, x, y)))

    assert excinfo.value.status_code == 422
    node.refresh_from_db()
    assert node.position_x is None
    assert node.position_y is None
