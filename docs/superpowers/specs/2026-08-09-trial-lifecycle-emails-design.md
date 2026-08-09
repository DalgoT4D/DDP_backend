# Trial lifecycle emails — automated sends

Date: 2026-08-09
Repo: DDP_backend (branch `feature/trial-clone-foundation`)

## Problem

Five free-trial email templates already exist in `ddpui/utils/email_templates.py`. Only two are
wired: verification and welcome, both fired directly from the signup/clone path. The mid-trial and
pre-end templates are written but nothing sends them, and there are no templates at all for the
three progress-driven emails product wants:

- **A — "Ready to see Dalgo in action?"** — user is 3 days in and has completed no walkthrough.
- **B — "Pick up where you left off"** — 3 days in, exactly one walkthrough completed.
- **C — "Congratulations you've completed your tour of Dalgo"** — both walkthroughs completed,
  on or after day 3.

This spec covers building A/B/C and wiring all five of A, B, C, midpoint and pre-end onto a single
automated trigger.

## Scope

**In scope**

- Three new email templates (A, B, C) plus the shared render helpers they need.
- One Celery beat task that decides and sends all five automated emails.
- Per-user send-state so nothing double-sends.
- Two new settings for the UPGRADE and SCHEDULE-A-CALL destinations.
- Retiring `check_org_plan_expiry_notify_people`, which the new emails supersede (see below).

**Out of scope**

- The post-deletion email (template 5). It needs an automated teardown job as its trigger, which
  does not exist yet. Left unwired.
- Verification and welcome emails. They already fire correctly from the signup/clone path and are
  not touched.
- Making the UPGRADE button actually register an upgrade. The button renders; where it points is
  deferred (see Open decisions).

## Existing pieces this builds on

| Piece | Location | Note |
|---|---|---|
| Walkthrough state | `UserPreferences.trial_walkthrough` (JSON) | Keys `product_tour \| insights \| automate_pipeline`, each `{"skipped": bool, "completed": bool}` |
| Trial clock | `OrgPlans.start_date` / `end_date` | Set at clone time, `TRIAL_DURATION_DAYS = 14` |
| Trial identification | `OrgPlans.base_plan == OrgPlanType.FREE_TRIAL.value` | `Org` has **no** `type` column; the `OrgType` enum is legacy and `org.base_plan()` reads through to `OrgPlans` |
| Email shell + helpers | `ddpui/utils/email_templates.py` | `_render_trial_email_shell`, `_render_trial_header`, `_render_trial_cta_button`, `_render_trial_action_list`, `_render_trial_progress_bar` |
| Send helpers | `ddpui/utils/awsses.py` | Existing `send_trial_*_email` functions to follow as a pattern |
| Scheduler | `ddpui/celery.py` | RedBeat is already the configured beat scheduler |
| Beat registration | `setup_periodic_tasks()` in `ddpui/celeryworkers/tasks.py` | Existing `@app.on_after_finalize.connect` hook; the new schedule is one `add_periodic_task` call, no `celery.py` change |

## Superseded: `check_org_plan_expiry_notify_people`

`check_org_plan_expiry_notify_people` (`ddpui/celeryworkers/tasks.py`) runs daily at midnight and
emails a plain-text expiry reminder at exactly `end_date − 7 days` and `end_date − 2 days`. It
already skips every plan whose `base_plan` is not `FREE_TRIAL`, so free trials are its only
audience — and on a 14-day trial those two dates are day 7 and day 12, the same days as the
midpoint and pre-end emails specced here. Left in place, every trial user gets two emails on both
days.

It is therefore **deleted**, along with its `add_periodic_task` entry. Nothing else references it
(two occurrences in the codebase, no tests). The new emails cover both dates with branded HTML, the
correct trial copy, and per-user dedupe flags the old task never had.

One copy difference to be aware of: the old email directed users to
`support@dalgo.org` to renew. The pre-end template instead offers UPGRADE and SCHEDULE-A-CALL
buttons, whose destinations are still open (see Open decisions). Until those settings are
populated, trial users lose a working renewal path that exists today — so the CTA URLs should be
decided before this ships, not after.

## Walkthrough flows that count

Only `insights` and `automate_pipeline`. `product_tour` is **not** tracked by any of these emails —
it does not appear as a checklist row and does not affect any count.

A flow counts as done only when its entry has `completed is True`. `skipped: true` does not count.

So `completed_count` ∈ {0, 1, 2} for every trial user.

## Architecture

New module `ddpui/core/trial/lifecycle_emails.py`, holding the whole decision ladder. A single
Celery task in `ddpui/celeryworkers/tasks.py` calls it.

### Why one sweep, not per-email tasks or per-user countdowns

The day-3 rules are interdependent: C must beat B when both are eligible on day 3, and once C has
been sent, A and B must never fire. Splitting them into separate beat tasks smears that
interdependence across tasks that cannot see each other's decisions. A per-user `countdown` task
scheduled at clone time gives exact timing but is silently lost on a worker restart, and C is
event-driven (fires whenever the second flow completes) so a sweep is needed regardless.

One sweep, one ladder, one place to read the rules.

### Cadence

Hourly. `OrgPlans` has no index on `base_plan`, `start_date` or `end_date`, so the query is a
sequential scan — but the table is one row per Org, so that scan is cheap and returns a handful of
rows. The cost is the same as daily, but
the completion email lands within an hour of the user finishing rather than up to a day later. The
day-3 and day-7 emails land at whatever hour corresponds to the user's signup time either way.

### Finding live trials

```python
OrgPlans.objects.filter(
    base_plan=OrgPlanType.FREE_TRIAL.value,
    start_date__isnull=False,
    end_date__gt=timezone.now(),
).select_related("org")
```

Postgres does the date filtering; the task never scans all orgs in Python. Every trial past day 3
keeps matching on every run, so the send-state flags — not the date filter — are what prevent
duplicates.

### Recipient

The trial org's OrgUser (a trial has exactly one), via `orguser.user.email`. A trial org with no
OrgUser is skipped with a warning — that state means a half-reaped trial, not a live user.

## The trial window

Both endpoints come from the same `OrgPlans` row, and neither is hardcoded:

```python
day_number = (now - start_date).days          # "day 3" == 72 hours elapsed
total_days = (end_date - start_date).days     # 14 for a normal trial
```

`total_days` is derived rather than read from `TRIAL_DURATION_DAYS` because `start_date` and
`end_date` are both rewritable by the `createorgplan` management command — an admin extending
someone's trial must shift the whole schedule, not just its end. Deriving both numbers from the
same row also guarantees `day_number` and `days_left` can never disagree.

`Org.created_at` is deliberately not used. It matches `start_date` to the millisecond at clone time
(both are stamped inside the same `create_organization` call) but cannot follow a later adjustment.

**Guard.** `_render_trial_progress_bar` computes `100 * day_number / total_days`, so a
`total_days` of 0 raises `ZeroDivisionError` and kills the sweep for that trial. `createorgplan`
sets `start_date` and `end_date` in independent `if` blocks with no validation that `end > start`
and no requirement that both be given, so an admin can produce a zero-length or inverted window.

The fallback applies **only when the window is non-positive**:

```python
total_days = (end_date - start_date).days
if total_days <= 0:
    logger.warning(...)
    total_days = TRIAL_DURATION_DAYS
```

Not `max(total_days, TRIAL_DURATION_DAYS)` — that would silently round a deliberately shorter
trial (say a 7-day one set by an admin) up to 14 and render "Day 7 of 14" for someone whose trial
ends that day.

Rules 1–3 depend only on `day_number` and are unaffected by the fallback; rule 4 uses `end_date`
directly.

## Decision ladder

Per trial, per run. `day = (timezone.now() - start_date).days`, so "day 3" means 72 hours elapsed.
Rules are checked in order and **the first match wins — at most one email per trial per run**. Any
other rule that was also eligible fires on a later run, an hour apart, so two emails never land in
the inbox simultaneously.

| # | Condition | Email | Flags stamped |
|---|---|---|---|
| 1 | `day >= 3` and `completed_count == 2` and no `completion` flag | **C** completion | `completion` **and** `day3` |
| 2 | `day >= 3` and no `day3` flag and no `completion` flag | **A** if `completed_count == 0`, **B** if `== 1` | `day3` |
| 3 | `day >= 7` and no `midpoint` flag | midpoint | `midpoint` |
| 4 | `now >= end_date - 2 days` and no `pre_end` flag | pre-end | `pre_end` |

Notes on the ordering:

- Rule 1 sits above rule 2 so a user who finished both walkthroughs by day 3 receives C, not B.
- Rule 1 stamps `day3` as well as `completion`. Without that, the next hourly run would see "no
  `day3` flag" and fire B on top of the congratulations email.
- Rules 3 and 4 are unconditional — they fire even if C has already gone out. They are expiry and
  upgrade nudges, a different job from the progress nudges, and suppressing the pre-end warning
  would leave a user unwarned that their workspace is about to be deleted.
- Rules 1–2 (progress) and rules 3–4 (lifecycle) gate on different flags, so one user can receive
  at most four emails across a trial: one of A/B, then C, plus midpoint and pre-end.
- The one-per-run cap matters in a catch-up case: a trial that is already day 7 with no `day3` flag
  (sweep was down, or the trial predates this feature) matches rules 2 and 3 at once. It gets the
  day-3 email this run and the midpoint email the next, rather than both together.

### Consequences worth stating

- A user who has completed nothing on day 3 gets A. A user who *started* a walkthrough but finished
  none also gets A — "started but zero completed" is treated identically to "not started", so
  nobody falls through the gap between rules a and b as originally stated.
- B is day-3-only. A user who gets A on day 3 and completes their first walkthrough on day 6 gets
  nothing at that point; they get C when they complete the second.
- C never fires before day 3, even if both walkthroughs are done on day 1. It waits for the day-3
  sweep.

## Send-state storage

New field on `UserPreferences`, alongside the existing walkthrough field:

```python
trial_emails_sent = models.JSONField(default=dict, blank=True)
# {"day3": "2026-08-09T10:00:00Z", "completion": ..., "midpoint": ..., "pre_end": ...}
```

Four keys, matching the four automated emails (A and B share `day3` because only one of them can
ever fire). Values are ISO timestamps rather than booleans, so "when did this go out" is answerable
without a separate audit trail.

A sibling field rather than nesting inside `trial_walkthrough`: that field is documented as "keyed
by flow name" and is read by the frontend's flow-gate logic, so mixing send-flags into the same
dict forces anything iterating flow keys to special-case them. Same model, same single migration
either way.

Add `trial_emails_sent` to `UserPreferences.to_json()` for parity with the other fields.

### Concurrency

The flag is written **after** a successful send, so an SES failure leaves the flag unset and the
next run retries.

The send happens **outside** any database transaction. An earlier draft of this spec wrapped the
whole read-decide-send-stamp sequence in `transaction.atomic()` with `select_for_update()`; that
was amended during implementation because it made the failure mode worse, not better. With the
send inside the transaction, a `save()` or commit failure *after* the email had already gone out
rolled the block back, left the flag unpersisted, and caused the next hourly run to send the same
email again — and it held a `FOR UPDATE` row lock open across an SES round trip, so one hung call
could stall the serial sweep.

The shipped shape is: read and decide, send, then stamp inside a short `transaction.atomic()` that
re-selects the row `select_for_update()` before writing.

The consequence, stated plainly: delivery is **at-least-once**, not exactly-once. Because the send
no longer happens under a held lock, two sweeps overlapping within the same hour could in principle
both send before either stamps. That is an accepted trade-off — the sweep is hourly and serial, and
a duplicate nudge is a milder failure than a missed expiry warning.

## Templates

Three new render functions in `ddpui/utils/email_templates.py`, following the existing
`render_trial_*` signature convention of returning a `(plain_text, html)` tuple. All three wrap
their body in the existing `_render_trial_email_shell`.

### New shared helpers

The mockups need three pieces the existing trial emails do not have:

- `_render_trial_checklist(items)` — the circle-or-green-tick + bold title + grey subtitle rows.
  Each item is `(done: bool, title: str, subtitle: str)`. Distinct from the existing
  `_render_trial_action_list`, which draws emoji icons inside a bordered box.
- `_render_trial_testimonial()` — the grey "SEE WHAT'S POSSIBLE" quote block. Identical across all
  three mockups, so it takes no arguments.
- `_render_trial_text_link(label, url)` — the "Schedule a call with us ↗" footer link.

### Per-email content

| Email | Headline | Checklist | CTAs |
|---|---|---|---|
| A | "Ready to see Dalgo in action?" | Both rows unticked | `OPEN WORKSPACE` |
| B | "Pick up where you left off" | Completed row ticked and listed first | `CONTINUE WHERE I LEFT OFF` |
| C | "Congratulations you've completed your tour of Dalgo." | Both rows ticked | `UPGRADE`, `KEEP EXPLORING` |

All three carry the testimonial block and the schedule-a-call footer link.

Checklist row copy, both flows:

- `insights` — "Build your first insight" / "Build out your first dashboard and share it"
- `automate_pipeline` — "Setup an automated data pipeline" / "Setup your data to be updated,
  cleaned and computed regularly"

Email A drops the "Explore the platform" row that appears in the mockup — `product_tour` is not
tracked, so A shows the same two rows as B and C, both unticked.

Email B's mockup subhead reads "You've built your [first insight/automated pipeline]. Next, go
ahead with [first insight/automated pipeline]" — placeholders. Both halves resolve from which flow
actually completed, and the checklist orders the finished flow first, matching the mockup.

### CTA destinations

Two new settings, both defaulting to empty:

- `TRIAL_UPGRADE_URL`
- `TRIAL_SCHEDULE_CALL_URL`

Buttons render regardless of whether the setting is populated. See Open decisions — an empty `href`
is a dead click.

`OPEN WORKSPACE` and `CONTINUE WHERE I LEFT OFF` both point at `settings.FRONTEND_URL_V2`, the same
destination the existing welcome email uses. `KEEP EXPLORING` points there too.

## Senders

Three new functions in `ddpui/utils/awsses.py`, matching the existing `send_trial_welcome_email`
shape — subject line, call the renderer, hand off to the SES send helper:

- `send_trial_day3_not_started_email(to_email, workspace_url)`
- `send_trial_day3_in_progress_email(to_email, completed_flow, workspace_url)`
- `send_trial_completion_email(to_email, upgrade_url, workspace_url)`

Midpoint and pre-end need senders too, since only the renderers exist today:

- `send_trial_midpoint_email(to_email, day_number, total_days, upgrade_url, schedule_call_url)`
- `send_trial_pre_end_email(to_email, day_number, total_days, end_date, upgrade_url, schedule_call_url)`

`render_trial_pre_end_email` takes `end_date` as a pre-formatted human-readable string and does no
date maths itself, so the sender formats `OrgPlans.end_date` before passing it.

## Error handling

- A single trial that raises is logged and skipped; the sweep continues to the next. One bad row
  must not stop the run.
- SES failure leaves the flag unset, so the next hourly run retries. There is no retry cap — a
  permanently bouncing address will be attempted hourly until the trial expires and drops out of
  the query. Acceptable for the expected volume; revisit if it becomes noisy.
- A trial org with no OrgUser is skipped with a warning — that state means a half-reaped trial.
- A missing `UserPreferences` row is created via `get_or_create` rather than skipped. The clone
  path already creates one for every trial user, so this is defensive; but skipping would deny
  email A to precisely the disengaged users it targets, which is the wrong failure direction.
- An unset CTA URL does not block the send; it produces a button with an empty href.

## Testing

New file `ddpui/tests/core/trial/test_lifecycle_emails.py`.

- **Decision-ladder matrix.** Parametrised over (day, completed_count, flags already set) → the
  email expected, or none. Covers each of the four rules, plus the orderings that matter: C beats B
  on day 3; B never fires after C; nothing fires before day 3; A fires for started-but-zero-completed.
- **Flag stamping.** C stamps both `completion` and `day3`. Every other rule stamps exactly its own.
- **One send per run.** A day-7 trial with no `day3` flag matches rules 2 and 3 at once; the first
  run sends only the day-3 email, the second sends midpoint.
- **Idempotency.** Two consecutive sweeps with unchanged state send exactly once.
- **Failure path.** A raising send leaves the flag unset and the next run retries.
- **Query scoping.** Non-trial plans, expired trials and trials with a null `start_date` are all
  excluded.
- **Window derivation.** A trial whose dates were adjusted by `createorgplan` produces
  `day_number` and `total_days` from the new window, not from `TRIAL_DURATION_DAYS`. A shorter
  admin-set window (e.g. 7 days) renders "of 7", **not** "of 14". A `start_date == end_date` row
  falls back to `TRIAL_DURATION_DAYS` instead of raising `ZeroDivisionError`.
- **Skipped ≠ completed.** A user who skipped both flows counts as `completed_count == 0`.

Time is controlled by freezing/injecting `now` rather than sleeping. SES is mocked throughout.

Renderer tests go in the existing email-template test file, following the golden-HTML fixture
pattern already used there.

## Files touched

| File | Change |
|---|---|
| `ddpui/models/userpreferences.py` | Add `trial_emails_sent` JSONField; extend `to_json()` |
| `ddpui/migrations/` | One new migration for the field |
| `ddpui/utils/email_templates.py` | Three helpers, three renderers |
| `ddpui/utils/awsses.py` | Five sender functions |
| `ddpui/core/trial/lifecycle_emails.py` | New — the decision ladder |
| `ddpui/celeryworkers/tasks.py` | New task calling the ladder + one `add_periodic_task` line in the existing `setup_periodic_tasks()`; **delete** `check_org_plan_expiry_notify_people` and its schedule entry |
| `ddpui/settings.py` + `.env.example` | `TRIAL_UPGRADE_URL`, `TRIAL_SCHEDULE_CALL_URL` |
| `ddpui/tests/core/trial/test_lifecycle_emails.py` | New |

## Open decisions

1. **UPGRADE button destination.** Deferred by product. The in-app upgrade is an authenticated POST
   to `/org-plan/upgrade` that emails `BIZ_DEV_EMAILS` and flips `OrgPlans.upgrade_requested` once
   per org — an email link cannot POST that. The zero-cost option is defaulting `TRIAL_UPGRADE_URL`
   to `{FRONTEND_URL_V2}/settings/billing`, the only page that calls the endpoint, so the button
   lands somewhere sane instead of being a dead click. A true one-click upgrade from the inbox
   needs either a `?upgrade=1` deep-link the frontend acts on, or a signed-token unauthenticated
   endpoint — both separate work.
2. **SCHEDULE-A-CALL destination.** Same shape. Setting stays empty until product supplies a link.

Both should be settled before ship, not after: retiring `check_org_plan_expiry_notify_people`
removes the `support@dalgo.org` renewal instruction that trial users get today, and empty CTA URLs
would leave nothing in its place.