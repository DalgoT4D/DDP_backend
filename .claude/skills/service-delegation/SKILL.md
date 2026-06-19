---
name: service-delegation
description: When computing a value, status, or derived property of a domain entity (KPI, metric, alert, chart, dashboard, etc.) from outside that entity's own service module, ask the owning service instead of rebuilding the logic. Use when writing a feature that touches another domain's data, fixing a bug whose root cause is parallel implementations drifting, or reviewing code that copies SQL/query-builder patterns from another module.
---

# Service Delegation — Single Source of Truth per Entity

Every domain entity in `DDP_backend/` has one service module that owns its business logic (`MetricService`, `KPIService`, `AlertService`, `ChartsService`, etc.). When code in service A needs a value or derived property that belongs to entity B, **ask B's service**. Don't rebuild B's logic in A.

This prevents the bug class where parallel implementations of "what is a KPI worth right now" drift over time — a fix lands in one place but not the other, and users see the value as X on the dashboard but the alert fires on Y.

## When this applies

Trigger this skill when about to:

- Write code in `core/alerts/`, `core/reports/`, `core/dashboard/`, etc. that computes a Metric value, KPI current value/RAG status, Chart preview, or any other entity's domain output.
- Copy a SQL builder or query-construction block from a sibling service module.
- Write a docstring that says "Mirrors X in YService" — that phrase is the giveaway.
- Import a service's pure helper (e.g. `compute_rag_status`) next to a locally-built query for the same entity.
- Fix a bug where two views of the same value disagree.

Skip for: cross-cutting helpers owned by no entity (timezone, logging), explicit pure-function helpers designed for reuse (`compute_rag_status` itself), and plain ORM attribute reads (`metric.column`, `kpi.target_value`).

## The procedure

**1. Find the owning service.** Look in `ddpui/core/<entity>/<entity>_service.py` or `ddpui/services/<entity>_service.py`. If a `<Entity>Service` class exists, it owns the entity's logic.

**2. Check if it already exposes what you need.** Grep for `compute_*`, `get_*`, `preview_*`, `evaluate_*`. If a method returns the value, call it.

**3. If it doesn't return enough, extend the service.** Add a sibling method that builds on the existing one. The existing method stays unchanged; the new one exposes the richer return shape (e.g., add `compute_metric_value_with_sql` alongside `compute_metric_value` when you need the SQL string for an audit log).

**4. Keep caller-specific concerns in the caller.** Delegation ≠ "everything moves." Audit log shape, alert scheduling, delivery channels, template rendering — those stay in `core/alerts/` because no other entity needs them. Rule: if no other domain needs the concept, it belongs in the caller; if it's "what is entity X worth," it belongs in X's service.

**5. Verify imports point the right way.** The caller imports the service (`MetricService`, `KPIService`). It should NOT import the service's internal building blocks (`AggQueryBuilder`, `apply_time_grain`, `TIME_GRAIN_TO_SQL`) for the delegated path — those are implementation details of the service.

## Detection patterns to refactor on sight

| Code smell | Likely root cause |
|---|---|
| `AggQueryBuilder()` + `fetch_from(metric.table_name, ...)` outside `metric_service.py` | Reinventing `MetricService.compute_metric_value` |
| `apply_time_grain` + `TIME_GRAIN_TO_SQL` outside `kpi_service.py` | Reinventing `KPIService._compute_trend` |
| Docstring says "Mirrors X" | Author knew it was duplicate |
| Importing a service's helper next to locally-built same-entity query | Surrounding logic also belongs in the service |
| Bug: "alert fires on wrong value" / "dashboard shows X but report shows Y" | Two implementations drifted |

## Anti-patterns

**Don't copy-and-tweak** when the service "doesn't quite return what I need." Extend the service instead.

**Don't import-and-rebuild** — using `compute_rag_status` while reimplementing the surrounding KPI query is a half-measure. If you're reaching for a service's helper, the surrounding logic likely belongs in that service.

**Don't add an entity-domain method to the caller module** — `_compute_kpi_latest_value(...)` in `alert_query.py` belongs in `KPIService`. The method belongs to the entity it's about.

## Quick reference

| Situation | Do |
|---|---|
| Computing entity X's value outside X's service | Ask `XService` |
| `XService` doesn't return what you need | Extend `XService` with a sibling method |
| Service returns value but you need audit SQL too | Add `*_with_sql` variant on the service |
| Operation is genuinely caller-specific (e.g. alert's standalone case) | Keep it in the caller |
| Pure helper (e.g. `compute_rag_status`) needed | Import and use; don't rebuild around it |
| Copy-pasting an `AggQueryBuilder` chain | Stop — delegate instead |
