---
name: testing
description: Write and run pytest tests for the Django/Ninja backend — service-level tests, API endpoint tests with mocked requests, fixture conventions, and mocking external services (Airbyte, Prefect, warehouses, Redis). Use when writing tests, adding tests, debugging test failures, or asked to test an endpoint or service.
---

# testing

Always use `uv run pytest`. Write tests one at a time, run each immediately.

## Running tests

```bash
# One test (most common during development)
uv run pytest ddpui/tests/api_tests/test_widget_api.py::test_create_widget -v

# One file
uv run pytest ddpui/tests/api_tests/test_widget_api.py -v

# One test class
uv run pytest ddpui/tests/core/test_widget_service.py::TestWidgetService -v

# Full suite with coverage
uv run pytest ddpui/tests --cov
```

## File-top boilerplate

Every test file starts with this — Django needs explicit setup before model imports:

```python
import os
from unittest.mock import Mock, patch

import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

# ...your imports here...

pytestmark = pytest.mark.django_db
```

`pytestmark` at module level enables DB access for every test in the file.

## API tests: call view functions directly

We don't use Ninja's `TestClient`. Tests import the view function and call it with a `Mock` request built by `mock_request(orguser)`:

```python
from ddpui.api.widget_api import create_widget
from ddpui.schemas.widget_schema import WidgetCreate

def test_create_widget(orguser, seed_db):
    payload = WidgetCreate(title="hello")
    request = mock_request(orguser)
    response = create_widget(request, payload)
    assert response["success"] is True
```

Full happy/validation-fail/not-found/permission-denied patterns and `HttpError` assertions: see [api-tests.md](api-tests.md).

## Standard fixtures

The codebase uses reusable fixtures: `seed_db` (session-scoped, loads roles & permissions), `org_with_workspace`, `authuser`, `orguser` (account-manager), `nonadminorguser` (guest), and `mock_request(orguser)`. Copy them into a new test file, or import from a sibling test file that defines them.

Copy-paste recipes: see [fixtures.md](fixtures.md).

## Mocking external services

Patch the **import site**, not the definition site. If a service does `from ddpui.ddpairbyte.airbyte_service import abreq`, you patch `ddpui.core.widget.widget_service.abreq` — not `ddpui.ddpairbyte.airbyte_service.abreq`.

Decorator stacking order, `side_effect` sequences, and stencils for Airbyte / Prefect / warehouse / Redis / email: see [mocking.md](mocking.md).

## The iterative test workflow

**Never batch-write multiple tests without running each one.**

1. Write one test method
2. Run it (it should fail in the right way — wrong assertion, missing impl)
3. Implement / fix the code
4. Run it again (should pass)
5. Run the whole class/file to check for regressions
6. Move to the next test method

This catches type errors, import mistakes, and wrong assumptions immediately — instead of compounding them across five broken tests.

## Pre-flight checklist

- [ ] Each test runs green individually with `uv run pytest <path>::<test> -v`
- [ ] Whole file/class runs green together
- [ ] No external HTTP calls — Airbyte / Prefect / warehouse / Redis all mocked
- [ ] Edge cases covered: empty inputs, missing config, permission denial, external service failure
- [ ] `uv run black .` clean
