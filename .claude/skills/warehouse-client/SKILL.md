---
name: warehouse-client
description: Connect to a client's warehouse (Postgres or BigQuery) from service-layer code using `WarehouseFactory.get_warehouse_client(org_warehouse)`. Use when the code needs to read or write a client's data, execute SQL against their warehouse, inspect schema, or work with `OrgWarehouse`. Never use the legacy `old_client` namespace.
---

# warehouse-client

Use this skill any time code talks to a client's warehouse — running SQL, introspecting schema, or anything driven by an `OrgWarehouse`.

## NEVER use `old_client`

The legacy module under `ddpui.utils.warehouse.old_client.*` is deprecated. If you see code reaching for it, refactor to the current interface. If a new task seems to require it, you're on the wrong path.

```python
# ✗ NEVER
from ddpui.utils.warehouse.old_client.warehouse_factory import get_client
from ddpui.utils.warehouse.old_client.postgres import PostgresClient

# ✓ ALWAYS
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse, WarehouseType
```

## Canonical usage

```python
from ddpui.models.org import OrgWarehouse
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory


def count_rows(org_warehouse: OrgWarehouse, sql: str) -> int:
    warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
    rows = warehouse.execute(sql)  # → list[dict]
    return rows[0]["total"] if rows else 0
```

`get_warehouse_client(org_warehouse)` is the primary entry point. It fetches credentials from AWS Secrets Manager (the `OrgWarehouse.credentials` field is a secret ID, not raw creds) and returns the right concrete client.

For low-level usage when you already hold raw credentials (mostly management commands): `WarehouseFactory.connect(creds, wtype)`.

## Know what's on the interface — read the source

The returned object is a `Warehouse` ABC instance. Before assuming a method exists, **read the interface**:

- ABC: `ddpui/utils/warehouse/client/warehouse_interface.py`
- Factory + concrete Postgres / BigQuery clients: `ddpui/utils/warehouse/client/`

`execute(sql)` is the workhorse and returns `list[dict]` (one dict per row; keys = column aliases).

## Only Postgres and BigQuery exist

`WarehouseType` has exactly two values: `POSTGRES` and `BIGQUERY`. The factory raises `ValueError` for anything else. Don't write code that branches for Snowflake or Redshift — they aren't implemented.

## Dialect-specific quoting

SQL identifier quoting differs:
- Postgres → `"column"` / `"schema"."table"`
- BigQuery → `` `column` `` / `` `schema`.`table` ``

Use the helpers in `ddpui.core.dbt_automation.utils.columnutils` (`quote_columnname`, `quote_constvalue`):

```python
from ddpui.core.dbt_automation.utils.columnutils import quote_columnname

col = quote_columnname("email", "postgres")    # → '"email"'
col = quote_columnname("email", "bigquery")    # → '`email`'
```

These are SQL-building helpers — **not** part of the warehouse client. Pass the wtype string (`"postgres"` / `"bigquery"`), which is `org_warehouse.wtype` or `warehouse.get_wtype().value`.

## SQL injection

`execute()` takes a raw SQL string. SQLAlchemy parameter binding is not exposed through this interface. If any part of the SQL is built from user input (schema/table/filter values), you are responsible for sanitizing or quoting it. When the SQL itself is user-driven, prefer the dbt_automation operations layer over hand-built strings.

## Pre-flight checklist

- [ ] Imports come from `ddpui.utils.warehouse.client.*` — never `old_client`
- [ ] Service uses `WarehouseFactory.get_warehouse_client(org_warehouse)`
- [ ] Code does not branch for warehouses other than Postgres / BigQuery
- [ ] Dialect-specific SQL uses `wtype` to pick quoting
- [ ] User input never interpolated raw into `execute()` SQL strings
- [ ] Tests mock `WarehouseFactory.get_warehouse_client` — see the `testing` skill for the stencil

## Deeper material

For Postgres SSL/SSH-tunnel config, the BigQuery `STRUCT` column gotcha, the `bq_location` field, the full credential-resolution flow through AWS Secrets Manager, and a longer common-mistakes table — see [REFERENCE.md](REFERENCE.md).
