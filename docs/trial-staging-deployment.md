# Free Trial — staging deployment checklist

Everything the `feature/trial-clone-foundation` branch needs before a trial signup works on
staging. Ordered so each step can be verified before the next.

Nothing here is optional in the sense of "the app still boots" — the app boots fine without any
of it. It fails at the first trial signup instead, which is why every section ends with a check.

---

## 0. Infrastructure prerequisites

Confirm these BEFORE touching env or code — several are hard architectural constraints, not
configuration.

- [ ] **Trials RDS instance exists** and is reachable from the staging workers (security group
      must allow the worker IPs, not just your laptop).
- [ ] **The template org's warehouse lives on that same trials-RDS instance.** This is a hard
      requirement, enforced in code: the data copy is a server-side
      `CREATE DATABASE ... TEMPLATE ...`, which cannot cross instances. If the template
      warehouse is on a different host, every clone fails at step 2 with an explicit error.
      There is no dump/restore fallback — that path was removed.
- [ ] **The template org exists on staging** with its slug matching `TEMPLATE_ORG_SLUG`, and is
      fully set up: Postgres warehouse, Airbyte sources + connections, dbt repo, Prefect
      deployments, and whatever metrics/KPIs/charts/dashboards the trial should show. The clone
      copies what the template has; anything missing there is missing in every trial.
- [ ] **Template warehouse is `postgres`.** v1 rejects BigQuery/Snowflake templates outright.
- [ ] **Airbyte staging reachable** from the workers (`AIRBYTE_SERVER_HOST`).
- [ ] **Prefect proxy reachable** from the workers.
- [ ] **Redis reachable** — clone progress and the per-email clone lock both live there.
- [ ] **SES is out of the sandbox**, and `SES_SENDER_EMAIL` is a verified identity. In sandbox
      mode SES only delivers to pre-verified addresses, so trial verification emails to real
      signups silently fail. This is the single most likely thing to be missed.

---

## 1. Environment variables

**Six of these are missing from `.env.template`** — they exist only in `settings.py`, so a
deployer following the template alone will not know to set them. They are marked ⚠️.

### Trial clone — required

| Variable | Notes |
|---|---|
| ⚠️ `TRIALS_RDS_HOST` | trials RDS endpoint |
| ⚠️ `TRIALS_RDS_PORT` | defaults to `5432` |
| ⚠️ `TRIALS_RDS_ADMIN_USER` | master user; needs `CREATEDB` and `CREATEROLE` |
| ⚠️ `TRIALS_RDS_ADMIN_PASSWORD` | |
| ⚠️ `TEMPLATE_ORG_SLUG` | slug of the template org; signup 503s if no org matches |
| ⚠️ `TEMPLATE_SOURCE_CREDS_FILE` | path to the creds JSON, see §2. Defaults to `<repo>/.template_source_creds.json` |

### Trial lifecycle emails

| Variable | Notes |
|---|---|
| `TRIAL_SCHEDULE_CALL_URL` | falls back to the team's booking page when blank; set to override |
| `FRONTEND_URL_V2` | already required elsewhere; the emails and the verification link use it |
| `SES_SENDER_EMAIL` | already required; see the sandbox note above |

### Carried in from the merged Airbyte OAuth work

| Variable | Notes |
|---|---|
| `AIRBYTE_OAUTH_REDIRECT_URL` | must match the staging backend's public URL |
| `AIRBYTE_GOOGLE_OAUTH_CLIENT_ID` | |
| `AIRBYTE_GOOGLE_OAUTH_CLIENT_SECRET` | |
| `BIZ_DEV_EMAILS` | recipients for subscription requests and new-org notifications; blank makes the upgrade endpoint error, and silently skips the new-org mail |

**Check:**

```bash
python manage.py shell -c "
from django.conf import settings
from ddpui.models.org import Org
print('template org found:', Org.objects.filter(slug=settings.TEMPLATE_ORG_SLUG).exists())
print('trials rds host  :', settings.TRIALS_RDS_HOST)
print('call url set     :', bool(settings.TRIAL_SCHEDULE_CALL_URL))
"
```

---

## 2. The template source credentials file

Airbyte returns source configs with secrets masked, so the clone cannot read them back. Real
configs live in a **gitignored JSON file**, keyed by template source name:

```json
{
  "My Google Sheet source": { "credentials": { ... }, "spreadsheet_id": "..." },
  "My Postgres source":     { "host": "...", "password": "..." }
}
```

- [ ] File placed on **every host running the `trial_clone` worker** (not just the web host).
- [ ] Path matches `TEMPLATE_SOURCE_CREDS_FILE`.
- [ ] Readable by the worker's user; keep it out of any image layer or config repo.
- [ ] Contains an entry for **every** source on the template org.

**Check:**

```bash
python manage.py shell -c "
from django.conf import settings
from ddpui.models.org import Org
from ddpui.ddpairbyte import airbyte_service
from ddpui.core.trial.source_config import validate_template_source_configs
org = Org.objects.get(slug=settings.TEMPLATE_ORG_SLUG)
names = [s['name'] for s in airbyte_service.get_sources(org.airbyte_workspace_id)['sources']]
missing = validate_template_source_configs(names)
print('template sources:', names)
print('MISSING creds   :', missing or 'none')
"
```

A missing entry does not fail loudly at deploy — it fails inside the clone, per source.

---

## 3. Migrations

Three new migrations on this branch:

```
0172_orgwarehouse_dbt_profile_secret_block
0173_orgtask_post_sync_transform
0174_userpreferences_trial_walkthrough_and_emails_sent
```

```bash
python manage.py migrate
```

- [ ] `0174` backfills both trial JSON columns to `{}` on every existing `UserPreferences` row.
      Additive, no downtime concern.
- [ ] **`0143` was edited on this branch.** It only replaces two imported constants with inline
      string literals — no schema or data change, and Django will not re-run an applied
      migration. Safe on an environment that already ran it. Worth knowing it appears in the
      diff so nobody panics.

### Fixtures

No fixture changes on this branch. `can_initiate_org_plan_upgrade` (used by the billing upgrade
endpoint) already exists on `main`. If staging predates it:

```bash
python manage.py loaddata seed/002_permissions.json seed/003_role_permissions.json
```

**Check:**

```bash
python manage.py shell -c "
from ddpui.models.role_based_access import Permission
print('upgrade permission:', Permission.objects.filter(slug='can_initiate_org_plan_upgrade').exists())
"
```

---

## 4. Celery

This is the part most likely to be silently skipped, because nothing errors — work just never
runs.

### A new worker is required

```bash
celery -A ddpui worker -Q trial_clone -n trial_clone --autoscale=4,1 -l info
```

Without it, `clone_trial_org_task` sits in the `trial_clone` queue forever and every signup
hangs on the progress screen until the frontend's 420s ceiling.

The hourly expired-trial deletion (`delete_expired_trial_orgs`) is routed to this queue too.

### Beat is required

```bash
celery -A ddpui beat -l info
```

Beat drives both new periodic tasks:

| Task | Schedule | Effect if beat is missing |
|---|---|---|
| `send_trial_lifecycle_emails` | hourly | no day-3 / completion / midpoint / pre-end emails |
| `delete_expired_trial_orgs` | `crontab(minute=0)` — every hour on the hour, UTC | expired trials accumulate forever |

- [ ] Exactly **one** beat process across the deployment. Two beats double-fire every periodic
      task, including the deletion sweep — though that one also takes a Redis mutex
      (`TRIAL_DELETE_LOCK_KEY`), so a double-fire exits instead of deleting the same org twice.
- [ ] Existing `default` and `canvas_dbt` workers unchanged — but they must be **restarted** to
      pick up the new task registrations.
- [ ] Add the new worker to whatever supervises the others (systemd / supervisor / k8s
      Deployment), with the same env and the creds file mounted.

**Check:**

```bash
python manage.py shell -c "
import ddpui.celeryworkers.tasks
from ddpui.celery import app
app.finalize()
for n, e in app.conf.beat_schedule.items():
    if 'trial' in n: print(n, '->', e['schedule'])
print('route:', app.conf.task_routes.get('ddpui.celeryworkers.tasks.delete_expired_trial_orgs'))
"
```

Expect the hourly email entry, the hourly `crontab(minute=0)` deletion entry, and
`{'queue': 'trial_clone'}`.

---

## 5. Post-deploy verification

Run in order; each is safe.

1. **Deletion dry pass** — prints what it would delete and deletes only genuinely expired trials.
   On a fresh staging this should report `0 expired trial(s) to delete`:
   ```bash
   python manage.py cleanup_trial_clone --expired
   ```
   If it reports a non-zero count you did not expect, stop and inspect before the next hourly tick
   tick arrives.

2. **Admin RDS connectivity** — the clone's step 2 needs `CREATEDB` and `CREATEROLE`:
   ```bash
   python manage.py shell -c "
   from ddpui.core.trial.warehouse_provision import _admin_connect
   c = _admin_connect('postgres'); print('admin connect OK'); c.close()
   "
   ```

3. **End-to-end signup** through the frontend: signup → verification email → activate →
   progress screen → auto-login → the cloned org's dashboards render.

4. **Tear the test trial down** and confirm the deletion path works:
   ```bash
   python manage.py cleanup_trial_clone --email <the-test-email>
   ```

---

## 6. Known gaps — decide before enabling, not after

- **Trial email CTA.** `TRIAL_SCHEDULE_CALL_URL` falls back to the team's booking page in
  `settings.py`; set it explicitly if staging should point somewhere else.
- **Frontend nudges 422.** The frontend sends `reports_nudge`, `alerts_nudge` and
  `metrics_nudge` to `PUT /api/userpreferences/trial-walkthrough`; the backend `Literal` accepts
  only `product_tour`, `insights`, `automate_pipeline`. The write fails silently (the frontend
  swallows it), so those nudges re-show on every visit for the whole trial. Fix is widening the
  `Literal`.
- **Two trial clocks.** The backend counts trial days from `OrgPlans.start_date`; the frontend
  badge and day-7/13/14 nudges count from `org.created_at` with its own `TRIAL_PERIOD_DAYS = 14`.
  They agree today because the clone writes both within milliseconds, and diverge the moment a
  plan window is edited by hand. The day-13/14 modal names a deletion date the backend does not
  use.
- **`.env.template` is incomplete** — the six ⚠️ variables above are absent from it.

---

## 7. Turning it off

- **Stop new trials:** unset `TEMPLATE_ORG_SLUG`. Signup then returns 503 with a clear message
  rather than half-creating anything.
- **Stop the deletion sweep:** stop beat, or drop its `add_periodic_task` entry. Nothing else deletes
  orgs on a schedule.
- **Stop the emails:** same beat entry removal, or blank the CTA URLs and accept dead buttons.

Note that stopping beat stops **both** periodic tasks, plus the pre-existing ones (flow-run
sync, alert dispatch, lock cleanup). Prefer removing a single `add_periodic_task` call over
killing beat wholesale.
