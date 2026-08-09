# Expired free-trial reaper — design

Date: 2026-08-09
Status: approved

## Problem

Free trials get a 14-day window (`TRIAL_DURATION_DAYS`), written onto `OrgPlans.start_date` /
`end_date` at clone time. Nothing reaps them when the window closes. Every expired trial leaves
behind an Airbyte workspace, a managed dbt GitHub repo, Prefect deployments and blocks, a
dedicated database and owner role on the shared trials RDS instance, an `Org`, an `OrgUser` and a
Django `User`. Today the only way to clear one is a human running
`python manage.py cleanup_trial_clone --email <email>`.

That does not scale, and the RDS instance is shared — abandoned trial databases accumulate on it
indefinitely.

## Solution

A nightly Celery task at midnight UTC that reaps every trial whose `end_date` has passed, using
the same command a human already runs by hand.

The management command gains a second selector rather than the logic being extracted into a new
service module:

```bash
python manage.py cleanup_trial_clone --email hdube1497@gmail.com   # manual, unchanged
python manage.py cleanup_trial_clone --expired                     # reap everything past end_date
```

One code path, one thing to test. The Celery task is a one-line wrapper around
`call_command("cleanup_trial_clone", expired=True)`, so the scheduled job and the manual job
cannot drift apart, and the nightly behaviour can be exercised by hand at any time.

### Rejected alternative

Extracting the command body into `ddpui/core/trial/teardown.py` and having both the command and
the task call it. Correct by the API → Core → Models layering, but it means moving ~40 lines of
verified teardown into a new module and maintaining two entrypoints into it, to gain nothing the
`--expired` flag doesn't already give. `call_command` is already imported and used in
`ddpui/celeryworkers/tasks.py`.

## Selection

```python
OrgPlans.objects.filter(
    base_plan=OrgPlanType.FREE_TRIAL.value,
    end_date__isnull=False,
    end_date__lte=now,
    org__slug__startswith=TRIAL_ORG_SLUG_PREFIX,   # "trial-"
).select_related("org")
```

Three filters, each load-bearing:

- **`base_plan=FREE_TRIAL`** — only trials.
- **`end_date <= now`** — no grace period. A trial is reaped on the first nightly run after its
  window closes. The pre-end lifecycle email already warns the user two days out
  (`PRE_END_DAYS_BEFORE` in `ddpui/core/trial/lifecycle_emails.py`).
- **`org.slug` starts with `trial-`** — the critical one. `base_plan` alone is **not** safe:
  `create_org_plan` (`ddpui/core/orgfunctions.py`) lets an admin put any org, including a real
  customer, on the Free Trial plan. Filtering on `base_plan` + `end_date` alone would delete
  those orgs and their warehouses once the plan lapsed. The slug prefix is the only marker that
  a clone created the org — `clone_service` builds every trial slug as
  `trial-<email_hash8>-<label>`.

A user who upgrades falls out of the query for free: an upgrade moves `base_plan` off
`FREE_TRIAL`, so the row stops matching.

`TRIAL_ORG_SLUG_PREFIX = "trial-"` moves into `ddpui/core/trial/constants.py`, next to
`TRIAL_DURATION_DAYS`, and both `clone_service` (which builds the slug) and the command (which
filters on it) use it. Two places deriving the same prefix independently is how the reaper
silently stops matching after a slug-format change.

## Load shaping

Deleting one trial is expensive and touches four external systems: Airbyte (workspace delete),
GitHub (repo delete), Prefect (deployments + blocks), and the shared trials RDS
(`DROP DATABASE` + `DROP ROLE`). Six expirations firing back to back would put six of each within
a few seconds, on instances that are also serving live users and live clones.

The loop therefore sleeps `TRIAL_REAP_STAGGER_SECONDS = 30` between orgs:

```python
for i, email in enumerate(emails):
    if i > 0:
        time.sleep(TRIAL_REAP_STAGGER_SECONDS)
    try:
        purge(email)
    except Exception as err:           # skipcq PYL-W0703
        logger.error("trial reap failed for %s: %s", email, err, exc_info=True)
```

The sleep sits at the **top** of the iteration, not the bottom, so a failed org still gets its
gap before the next one starts — a failing org is frequently a loaded or unreachable one, which
is exactly when hammering the next request is worst.

### Why `sleep` and not `countdown`

`apply_async(countdown=i*30)` would enqueue one task per org and hold no worker while waiting,
which is better on every axis *inside* Celery: per-org retries, survives a worker restart, one
slot occupied only during the actual deletion.

It is rejected because it only exists inside Celery. `python manage.py cleanup_trial_clone
--expired` run from a terminal has no broker to schedule against, so the command would need one
loop for humans and another for Celery — reintroducing exactly the two-path drift that the
`--expired` flag is meant to avoid.

The cost of `sleep` is one worker process held for the duration of the reap (~10 minutes for six
orgs). Acceptable: the task is routed to the `trial_clone` queue, whose worker runs
`--autoscale=4,1` and so still has three processes free for a live signup, and it runs at
midnight.

## Failure handling

Per-org `try/except` with `exc_info=True`. Org 3 failing (Airbyte down, RDS refusing connections)
logs a traceback; orgs 4–6 still run. `exc_info=True` keeps a logic bug distinguishable from a
genuine outage — the same reasoning as the sweep in `lifecycle_emails.py`.

A mid-teardown failure leaves partial state: for example the Airbyte workspace deleted but the
RDS database still present. This is already true of the existing `--email` path, and every step
inside the command is independently guarded, so re-running `--email <email>` on that org cleans
up the remainder.

**Orphan orgs.** An expired org with zero `OrgUser` rows yields no email, and both
`drop_trial_database` and the Django `User` delete are keyed by email. Those orgs get
`delete_trial_org(org)` only, with a warning logged that the `ft_*` database and role were left
behind. This matches the existing manual command's limits — it cannot reach a database whose
name it cannot compute.

The email for a reaped org is resolved from its earliest `OrgUser` (`order_by("id")`), the same
pinning rule `lifecycle_emails.process_trial` uses. The email is then handed back to the existing
`--email` code path, which re-discovers orgs from that email — so if one email ever owns two
trial orgs, both are reaped in one pass.

## Task and schedule

```python
@app.task()
def reap_expired_trial_orgs():
    """delete free-trial orgs whose 14-day window has ended; runs nightly at midnight UTC"""
    return call_command("cleanup_trial_clone", expired=True)


sender.add_periodic_task(
    crontab(minute=0, hour=0),
    reap_expired_trial_orgs.s(),
    name="reap expired free-trial orgs",
)
```

Routed to the `trial_clone` queue in `ddpui/celery.py`, alongside `clone_trial_org_task`:

```python
"ddpui.celeryworkers.tasks.reap_expired_trial_orgs": {"queue": "trial_clone"},
```

Teardown is minutes of external I/O per org. On the `default` queue it would block notifications,
webhooks and lock cleanup. `trial_clone` is already the isolated, autoscaling queue for exactly
this kind of work, and it requires no new worker in deploy.

`settings.TIME_ZONE` is `"UTC"`, so `crontab(minute=0, hour=0)` is midnight UTC.

## Deliberately out of scope

- **A per-run cap.** Considered, and rejected by the user: nothing writes `end_date` except the
  clone itself, so a mass-expiry event would have to come from a manual SQL edit, which is
  accepted as an operator's own risk.
- **A kill-switch setting.** Ceremony; a staging environment pointed at a production dump is a
  larger problem than this task.
- **A grace period after `end_date`.** Rejected in favour of reaping on the first run after
  expiry; the pre-end email is the warning.

## Testing

- selection excludes a non-`trial-` slug org on the Free Trial plan (the customer-org footgun)
- selection excludes an unexpired trial and a non-`FREE_TRIAL` plan
- an expired trial org is purged
- one org raising does not abort the loop — later orgs still purged, error logged
- the stagger sleep runs between orgs and not before the first (patched `time.sleep`)
- orphan org (no `OrgUser`) → org deleted, warning logged, no `drop_trial_database` call
- `--email` behaviour unchanged
- `--email` and `--expired` are mutually exclusive, and one is required
