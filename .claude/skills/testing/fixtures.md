# Fixtures — import, don't redefine

The canonical fixtures live in one place and are imported by every test file that needs them. Don't recreate them locally.

## The canonical import

```python
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    org_with_workspace,
    org_without_workspace,
    authuser,
    orguser,
    nonadminorguser,
    mock_request,
)
```

Pull only the names you actually use. If the import path moves in the future, grep for `def seed_db` to find the new home — but for now this is the location used everywhere.

## What each one gives you

| Name                    | What it provides                                                                  |
|-------------------------|-----------------------------------------------------------------------------------|
| `seed_db`               | Session-scoped fixture. Loads roles, permissions, and role-permission mappings into the test DB. Depend on this in any test that exercises `@has_permission` (i.e. anything calling a view function through `mock_request`). |
| `org_with_workspace`    | An `Org` with `airbyte_workspace_id="FAKE-WORKSPACE-ID"`. Use when the code under test expects an Airbyte workspace to be present. |
| `org_without_workspace` | An `Org` with `airbyte_workspace_id=None`. Use for tests that exercise the "not yet configured" path. |
| `authuser`              | A plain Django `User` (no org binding).                                            |
| `orguser`               | An `OrgUser` with the **account-manager** role — has full permissions.            |
| `nonadminorguser`       | An `OrgUser` with the **guest** role — for permission-denial tests.                |
| `mock_request(orguser)` | Helper (not a fixture). Returns a `Mock` request shaped like the real one — sets `request.orguser` and populates `request.permissions` from the orguser's role. |

## `mock_request` — what it actually does

```python
def mock_request(orguser=None):
    req = Mock()
    req.orguser = orguser
    req.permissions = []
    if orguser and orguser.new_role:
        slugs = RolePermission.objects.filter(role=orguser.new_role).values_list(
            "permission__slug", flat=True
        )
        req.permissions = list(slugs)
    return req
```

It mirrors what the JWT middleware sets on every authenticated request:
- `request.orguser` — the OrgUser instance
- `request.permissions` — list of permission slugs from the orguser's role

If you build a bare `Mock()` request without going through `mock_request`, the `@has_permission` decorator will see an empty permissions list and raise 403.

## Pairing them: typical test

```python
from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

from ddpui.api.widget_api import create_widget
from ddpui.schemas.widget_schema import WidgetCreate
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    mock_request,
)

pytestmark = pytest.mark.django_db


def test_create_widget(orguser, seed_db):
    payload = WidgetCreate(title="hello")
    request = mock_request(orguser)
    response = create_widget(request, payload)
    assert response["success"] is True
```

The two fixture args (`orguser`, `seed_db`) pull in everything underneath them — `seed_db` loads roles, `orguser` depends on `authuser` + `org_with_workspace`, so those get set up automatically.

## When you need a fixture that doesn't exist

If your test needs a one-off setup (e.g. a model the canonical fixtures don't cover), define a local `@pytest.fixture` in your test file with `yield` + cleanup. Don't add it to the shared file unless multiple test files will use it.

```python
@pytest.fixture
def f_my_thing(org_with_workspace):
    thing = MyModel.objects.create(org=org_with_workspace, ...)
    yield thing
    thing.delete()
```

## Common mistakes

| Bad                                                              | Good                                                              |
|------------------------------------------------------------------|-------------------------------------------------------------------|
| Redefining `seed_db` / `mock_request` in your test file          | Import from the canonical location                                |
| Building `Mock()` directly for the request                       | Use `mock_request(orguser)` — it populates `permissions` correctly |
| Skipping `seed_db` when the test calls a view with `@has_permission` | Always depend on `seed_db` for any permission-checked view        |
| Using `orguser` when you need to test 403 denial                  | Use `nonadminorguser` (guest role)                                |
