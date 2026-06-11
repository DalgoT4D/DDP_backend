# API tests — patterns

We don't use Ninja's `TestClient`. API tests import the view function and call it directly with a `Mock` request built by the canonical `mock_request` helper.

## Required imports

```python
from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    nonadminorguser,
    mock_request,
)
```

Pull additional fixtures (`org_with_workspace`, `authuser`, etc.) from the same import if you need them.

## Anatomy of an API test

```python
from ddpui.api.widget_api import create_widget
from ddpui.schemas.widget_schema import WidgetCreate


def test_create_widget(orguser, seed_db):
    payload = WidgetCreate(title="hello")           # 1. construct payload
    request = mock_request(orguser)                  # 2. build mock request
    response = create_widget(request, payload)       # 3. call view directly
    assert response["success"] is True               # 4. assert envelope
    assert response["data"]["title"] == "hello"      # 5. assert payload
```

## Happy path

```python
def test_create_widget_success(orguser, seed_db):
    """Widget is created and persisted."""
    payload = WidgetCreate(title="hello", description="world")
    request = mock_request(orguser)

    response = create_widget(request, payload)

    assert response["success"] is True
    assert response["data"]["title"] == "hello"
    assert Widget.objects.filter(org=orguser.org, title="hello").exists()
```

Assert on **both** the response and the DB state — a view that returns 200 but doesn't persist is a real class of bug.

## When the view hits an external service

If the view (or anything it calls) makes an external call — Airbyte, Prefect, warehouse, Redis, AWS SES, dbt — **always mock it**. Tests must not perform real I/O. They'll be flaky, slow, and may corrupt remote state.

Patch at the **import site** in the file under test, not where the external symbol is defined. (Full rules and stencils for each service: see [mocking.md](mocking.md).)

```python
def test_create_widget_calls_airbyte(orguser, seed_db):
    """create_widget creates an Airbyte source under the hood."""
    payload = WidgetCreate(title="hello", connector="postgres")
    request = mock_request(orguser)

    with patch(
        "ddpui.core.widget.widget_service.abreq",
        return_value={"sourceId": "src-abc"},
    ) as mock_abreq:
        response = create_widget(request, payload)

    assert response["success"] is True
    assert response["data"]["title"] == "hello"
    mock_abreq.assert_called_once()
```

Two assertions worth making in this kind of test:

1. The view returned correctly.
2. The external call was made (or *not* made, in failure paths) — use `assert_called_once_with(...)` or check `call_args`.

For multi-call sequences (e.g. create workspace → create source → create connection), use `side_effect=[...]`:

```python
with patch("ddpui.core.widget.widget_service.abreq") as mock_abreq:
    mock_abreq.side_effect = [
        {"workspaceId": "ws-1"},
        {"sourceId": "src-2"},
        {"connectionId": "conn-3"},
    ]
    response = create_widget(request, payload)

assert mock_abreq.call_count == 3
```

If the external service fails, that path also needs a test:

```python
def test_create_widget_airbyte_failure(orguser, seed_db):
    payload = WidgetCreate(title="hello", connector="postgres")
    request = mock_request(orguser)

    with patch(
        "ddpui.core.widget.widget_service.abreq",
        side_effect=Exception("Airbyte unreachable"),
    ):
        with pytest.raises(HttpError) as excinfo:
            create_widget(request, payload)

    assert excinfo.value.status_code == 502
```

## Validation failure → 400

```python
def test_create_widget_duplicate_title(orguser, seed_db):
    """Duplicate title raises 400."""
    Widget.objects.create(org=orguser.org, title="dup", created_by=orguser)
    payload = WidgetCreate(title="dup")
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        create_widget(request, payload)

    assert excinfo.value.status_code == 400
    assert "already exists" in str(excinfo.value)
```

## Not found → 404

```python
def test_get_widget_not_found(orguser, seed_db):
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_widget(request, 99999)
    assert excinfo.value.status_code == 404
```

## Permission denial → 403

```python
def test_delete_widget_as_guest(nonadminorguser, seed_db):
    """Guest role lacks can_delete_widgets."""
    request = mock_request(nonadminorguser)
    with pytest.raises(HttpError) as excinfo:
        delete_widget(request, 1)
    assert excinfo.value.status_code == 403
```

The 403 comes from `@has_permission` reading `request.permissions` — which `mock_request` populates from the orguser's role. The guest role doesn't have `can_delete_widgets`, so the decorator raises before the view body runs.

## Cross-org isolation → 404

Multi-tenancy is a critical invariant. An OrgUser must not be able to read or mutate another org's data. The service-level `filter(org=...)` should make foreign objects look "not found":

```python
def test_get_widget_other_org(orguser, seed_db):
    """OrgUser can't see widgets from another org."""
    other_org = Org.objects.create(name="other", slug="other-slug")
    other_user = OrgUser.objects.create(
        user=orguser.user, org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    widget = Widget.objects.create(org=other_org, title="theirs", created_by=other_user)

    request = mock_request(orguser)  # the original orguser, not other_user

    with pytest.raises(HttpError) as excinfo:
        get_widget(request, widget.id)
    assert excinfo.value.status_code == 404
```

Write at least one cross-org test for any view that reads or mutates org-scoped data.

## List + pagination

```python
def test_list_widgets_pagination(orguser, seed_db):
    for i in range(15):
        Widget.objects.create(org=orguser.org, title=f"w{i}", created_by=orguser)

    request = mock_request(orguser)
    response = list_widgets(request, page=2, page_size=10)

    assert response["success"] is True
    assert response["data"]["total"] == 15
    assert response["data"]["page"] == 2
    assert len(response["data"]["data"]) == 5
```

## Asserting on `HttpError`

```python
with pytest.raises(HttpError) as excinfo:
    create_widget(request, payload)
assert excinfo.value.status_code == 400         # HTTP code
assert str(excinfo.value) == "Widget ..."       # message
```

`HttpError(status_code, message)` — `str(excinfo.value)` is the message, `excinfo.value.status_code` is the code.

## Asserting on the `api_response` envelope

The view returns a dict shaped by `api_response`. Treat it as a dict:

```python
assert response["success"] is True
assert response["message"] == "Widget created successfully"
assert response["data"]["id"] == widget_id

# List response: nested
assert response["data"]["total"] == 42
assert response["data"]["page"] == 1
```

Prefer `is True` over `== True` for the boolean field — lint catches the latter.

## Class-based vs. function-based tests

Both styles exist in the codebase. Group tests in a `TestX` class when:
- They share substantial setup (use a class-scoped fixture or `setup_method`)
- They test one logical behavior with many variations

```python
class TestCreateWidget:
    def test_success(self, orguser, seed_db):
        ...
    def test_duplicate_title(self, orguser, seed_db):
        ...
    def test_missing_title(self, orguser, seed_db):
        ...
```

Plain function tests are equally fine and more common — pick whichever reads more clearly.

## Common mistakes

| Bad                                                          | Good                                                             |
|--------------------------------------------------------------|------------------------------------------------------------------|
| Calling view via HTTP / `TestClient`                         | Import the view function and call directly                       |
| `request = Mock()` (no orguser / permissions)                | `request = mock_request(orguser)`                                |
| Letting the view make a real external call (Airbyte / Prefect / warehouse) | Always mock external I/O with `patch(...)` at the import site |
| Asserting only `response["success"]`                          | Also assert the DB state (created/updated/deleted)               |
| Asserting only the response, not that the external mock was called | Use `mock.assert_called_once_with(...)` to verify the call was made |
| `assert response == {"success": True, "data": ...}` (rigid)  | Assert one field at a time — order/extra keys won't break tests  |
| Forgetting `seed_db` when using `mock_request`               | `seed_db` is needed for permission lookups                       |
| No cross-org isolation test for org-scoped endpoints         | Always test that another org's data returns 404                  |
