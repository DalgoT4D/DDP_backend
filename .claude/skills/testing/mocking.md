# Mocking — patterns and stencils

## The patching rule (read this first)

**Patch where the name is looked up at call time.** Three common shapes in this codebase:

### Shape A — module-attribute call

The code under test imports a module and calls a function on it:

```python
# caller
from ddpui.ddpprefect import prefect_service
prefect_service.get_flow_run(run_id)
```

Patch the function on the source module:

```python
@patch("ddpui.ddpprefect.prefect_service.get_flow_run")
def test_X(mock_get_flow_run, ...):
    mock_get_flow_run.return_value = {"state": {"type": "COMPLETED"}}
```

This works because `prefect_service` in the caller is a reference to the same module object, and `get_flow_run` is looked up on the module at call time.

### Shape B — direct function import

```python
# caller (e.g. ddpui.api.dbt_api.py)
from ddpui.core.git_manager import GitManager
GitManager(...)  # name was rebound in the caller's namespace
```

Patch at the caller's namespace (the file under test):

```python
@patch("ddpui.api.dbt_api.GitManager")
def test_X(mock_git_manager, ...):
    ...
```

Patching `ddpui.core.git_manager.GitManager` wouldn't affect the binding already in `dbt_api`.

### Shape C — class method

The class is the same object regardless of where it's imported, so patching the classmethod on the class works everywhere:

```python
# Either of these works for WarehouseFactory.get_warehouse_client()
@patch("ddpui.core.charts.charts_service.WarehouseFactory.get_warehouse_client")
@patch("ddpui.utils.warehouse.client.warehouse_factory.WarehouseFactory.get_warehouse_client")
```

### When in doubt

Try the source-module path (Shape A) first. If your mock doesn't get called during the test, the binding has been rebound in the caller (Shape B) — switch to patching the caller's namespace.

## Decorator: patch one symbol for a whole test

```python
@patch("ddpui.api.dbt_api.GitManager")
def test_dbt_git_failure(mock_git_manager, orguser, seed_db):
    mock_git_manager.return_value.pull_changes.side_effect = Exception("git failed")
    request = mock_request(orguser)
    # ...
```

The patched name becomes the first argument, before fixtures.

## `patch.multiple` — patch several attrs of one module

```python
@patch.multiple(
    "ddpui.utils.awsses",
    send_signup_email=Mock(return_value=1),
    send_invite_user_email=Mock(return_value=1),
)
def test_signup_and_invite(...):
    ...
```

Doesn't add args (unless you pass `DEFAULT` sentinel — usually unnecessary).

## Stacked decorators — argument order is bottom-up

Decorators apply bottom-up, so the innermost (closest to the function) decorator binds the first argument:

```python
@patch("os.path.exists", Mock(side_effect=[True, True]))
@patch("ddpui.api.dbt_api.create_single_html", Mock(return_value="html"))
@patch("builtins.open", Mock(write=Mock(), close=Mock()))
@patch("ddpui.api.dbt_api.RedisClient")
def test_dbt_docs(mock_redis, mock_open, mock_create, mock_exists, orguser):
    # mock_redis    ← innermost decorator (closest to function)
    # mock_open
    # mock_create
    # mock_exists   ← outermost decorator
    ...
```

If the order feels brittle, switch to context-manager patches — easier to read.

## Context-manager patch (preferred for fine control)

```python
def test_dbt_git_failure(orguser, seed_db):
    request = mock_request(orguser)
    with patch("ddpui.api.dbt_api.GitManager") as mock_git, \
         pytest.raises(HttpError) as excinfo:
        mock_git.return_value.pull_changes.side_effect = Exception("git pull failed")
        post_dbt_git_pull(request)
    assert str(excinfo.value) == "git pull failed"
```

Use this when you want to:
- Configure the mock dynamically (per-test branch)
- Scope the patch to a small block
- Combine with `pytest.raises` cleanly

## Sequential return values via `side_effect`

```python
@patch("os.path.exists", Mock(side_effect=[True, False, True]))
def test_three_path_checks(...):
    # 1st call → True, 2nd → False, 3rd → True
    # 4th call → StopIteration
```

## Mixed returns and exceptions

```python
mock_api.side_effect = [
    {"ok": True},               # 1st call returns
    Exception("boom"),          # 2nd call raises
    {"ok": True},               # 3rd call returns
]
```

## Asserting on mock calls

```python
mock_create.assert_called_once()                                    # called exactly once
mock_create.assert_called_once_with("expected", arg=42)             # called once with these args
mock_create.assert_called_with(...)                                 # last call had these args
mock_create.assert_any_call(...)                                    # at least one call had these args
assert mock_create.call_count == 3
assert mock_create.call_args_list[0].args[0] == "first-call-arg"
```

---

## Stencils for external services

All paths below are real and currently work. Swap the *caller* path (e.g. `ddpui.api.dbt_api`) for whichever module your code under test lives in. Keep the source paths (`ddpui.ddpairbyte.airbyte_service`, etc.) as-is — they reference the actual modules.

### Airbyte

Callers do `from ddpui.ddpairbyte import airbyte_service` and then call `airbyte_service.<func>(...)`. Patch the function on the source module:

```python
@patch(
    "ddpui.ddpairbyte.airbyte_service.create_source",
    Mock(return_value={"sourceId": "src-abc"}),
)
def test_creates_airbyte_source(orguser, seed_db):
    ...
```

Multi-call sequences:

```python
with patch("ddpui.ddpairbyte.airbyte_service.create_source") as mock_create_source, \
     patch("ddpui.ddpairbyte.airbyte_service.create_destination") as mock_create_dest, \
     patch("ddpui.ddpairbyte.airbyte_service.create_connection") as mock_create_conn:
    mock_create_source.return_value = {"sourceId": "src-1"}
    mock_create_dest.return_value = {"destinationId": "dst-2"}
    mock_create_conn.return_value = {"connectionId": "conn-3"}
    ...
```

When the caller imports the helpers module instead (e.g. `from ddpui.ddpairbyte import airbytehelpers`), patch the helper module's function:

```python
@patch("ddpui.ddpairbyte.airbytehelpers.create_connection", Mock(return_value={...}))
```

### Prefect

Callers do `from ddpui.ddpprefect import prefect_service` and then call `prefect_service.<func>(...)`. Patch the function on the source module:

```python
with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_run:
    mock_run.return_value = {"state": {"type": "COMPLETED"}, "id": "fr-1"}
    ...

with patch("ddpui.ddpprefect.prefect_service.create_deployment_flow_run") as mock_dep:
    mock_dep.return_value = {"id": "deploy-uuid"}
    ...
```

For webhook tests (poll-based):

```python
with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_poll:
    mock_poll.return_value = {"state_type": "COMPLETED"}
    ...
```

### Warehouse (Postgres / BigQuery via `WarehouseFactory`)

Callers do `from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory` and call `WarehouseFactory.get_warehouse_client(...)` or `WarehouseFactory.connect(...)`. Patch the classmethod at the source:

```python
with patch(
    "ddpui.utils.warehouse.client.warehouse_factory.WarehouseFactory.get_warehouse_client"
) as mock_get_client:
    mock_wh = Mock()
    mock_wh.execute.return_value = [{"col1": "value"}]
    mock_wh.get_wtype.return_value = "postgres"
    mock_wh.get_table_columns.return_value = {"col1": "VARCHAR"}
    mock_get_client.return_value = mock_wh
    ...
```

Patching at the caller's namespace also works for class imports — both of these are equivalent:

```python
@patch("ddpui.utils.warehouse.client.warehouse_factory.WarehouseFactory.get_warehouse_client")
@patch("ddpui.core.charts.charts_service.WarehouseFactory.get_warehouse_client")
```

### Redis client

`RedisClient` is a class imported with `from ddpui.utils.redis_client import RedisClient`. Real patches use the import site of the file under test:

```python
@patch("ddpui.auth.RedisClient.get_instance")
def test_X(mock_redis_inst, ...):
    mock_redis_inst.return_value.get.return_value = b'{"cached": "value"}'
    ...
```

When you need to read what was written:

```python
fake_redis = Mock()
with patch("ddpui.auth.RedisClient.get_instance", return_value=fake_redis):
    ...
    fake_redis.set.assert_called_with("key", b"value", ex=3600)
```

### Outbound email (AWS SES)

Callers do `from ddpui.utils import awsses` and call `awsses.send_signup_email(...)`. Patch the function on the source module:

```python
@patch("ddpui.utils.awsses.send_signup_email", Mock(return_value=1))
def test_signup_sends_email(...):
    ...

@patch.multiple(
    "ddpui.utils.awsses",
    send_signup_email=Mock(return_value=1),
    send_invite_user_email=Mock(return_value=1),
)
def test_signup_and_invite(...):
    ...
```

If you need to assert on what was sent, patch the lower-level SES client:

```python
@patch("ddpui.utils.awsses._get_ses_client")
def test_email_payload(mock_ses_client, ...):
    ...
    mock_ses_client.return_value.send_email.assert_called_once()
```

### Filesystem (`os.path.exists`, `open`)

```python
@patch("os.path.exists", Mock(side_effect=[True, False]))     # 2 sequential calls
@patch("builtins.open", Mock())
def test_file_check(...):
    ...
```

For real file content:

```python
from unittest.mock import mock_open
@patch("builtins.open", mock_open(read_data="line1\nline2\n"))
def test_reads_file(...):
    ...
```

### Celery task (don't actually queue)

Patch `.delay` at the caller's import site:

```python
@patch("ddpui.api.dbt_api.run_dbt_command.delay")
def test_endpoint_queues_task(mock_delay, orguser, seed_db):
    ...
    mock_delay.assert_called_once_with(task_id=1)
```

---

## Common mistakes

| Bad                                                              | Good                                                                   |
|------------------------------------------------------------------|------------------------------------------------------------------------|
| `@patch("ddpui.ddpairbyte.airbyte_service.X")` when the caller did `from X import foo` and the function was rebound in their namespace | Patch the caller's namespace (Shape B)                                |
| `mock.return_value = ...` when you need a sequence              | Use `side_effect=[...]`                                                |
| `assert mock.called` (weak)                                      | `assert_called_once_with(...)` or check `call_args`                    |
| Forgetting `Mock(...)` wrapper inside `@patch.multiple(...)`      | Each attr value should be a `Mock(...)` or `MagicMock`                 |
| Mocking the view function itself                                 | Mock the *dependency* (airbyte_service.X, prefect_service.Y, WarehouseFactory.Z) |
| Missing `from unittest.mock import patch` at top of test file    | Always: `from unittest.mock import Mock, patch`                        |
| Patching but never asserting the call was made                   | Use `mock.assert_called_with(...)` to lock the contract                |
