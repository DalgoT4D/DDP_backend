import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    nonadminorguser,
    mock_request,
    authuser,
    org_without_workspace,
)
from ddpui.api.transform_api import (
    create_dbt_project,
    delete_dbt_project,
    sync_sources,
    post_construct_dbt_model_operation,
    put_operation,
    get_operation,
    post_save_model,
    get_input_sources_and_models,
    get_dbt_project_DAG,
    delete_model,
    delete_operation,
    get_warehouse_datatypes,
    post_lock_canvas,
    LockCanvasRequestSchema,
    CanvasLock,
    post_unlock_canvas,
)

pytestmark = pytest.mark.django_db


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_post_lock_canvas_acquire(orguser):
    """a test to lock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    response = post_lock_canvas(request, payload)
    assert response is not None
    assert response.locked_by == orguser.user.email
    assert response.lock_id is not None


def test_post_lock_canvas_locked_by_another(orguser, nonadminorguser):
    """a test to lock the canvas"""
    CanvasLock.objects.create(locked_by=nonadminorguser, locked_at=datetime.now())
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    response = post_lock_canvas(request, payload)
    assert response is not None
    assert response.locked_by == nonadminorguser.user.email
    assert response.lock_id is None


def test_post_lock_canvas_locked_by_self(orguser):
    """a test to lock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    post_lock_canvas(request, payload)
    # the second call won't acquire a lock
    response = post_lock_canvas(request, payload)
    assert response is not None
    assert response.locked_by == orguser.user.email
    assert response.lock_id is None


def test_post_unlock_canvas_unlocked(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    response = post_unlock_canvas(request, payload)
    assert response == {"status": "unlocked"}


def test_post_unlock_canvas_locked(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    post_lock_canvas(request, payload)

    response = post_unlock_canvas(request, payload)
    assert response == {"error": "wrong-lock-id"}


def test_post_unlock_canvas_locked_unlocked(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    lock = post_lock_canvas(request, payload)

    payload.lock_id = lock.lock_id
    response = post_unlock_canvas(request, payload)
    assert response == {"status": "unlocked"}


def test_post_unlock_canvas_locked_wrong_lock_id(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    post_lock_canvas(request, payload)

    payload.lock_id = "wrong-lock-id"
    response = post_unlock_canvas(request, payload)
    assert response == {"error": "wrong-lock-id"}
