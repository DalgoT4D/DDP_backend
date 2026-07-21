# Free-trial clone: retry without re-signup

**Date:** 2026-07-21
**Branch:** feature/trial-clone-foundation

## Problem

When a trial clone fails mid-run (say step 5 of 8), the current flow:

1. `_teardown` **purges everything** — RDS db+role, org, Airbyte workspace, sources,
   connections, dbt repo, viz, **and the Django User**.
2. Task marks Redis `"failed"`.
3. Frontend "Try again" is a `<Link href="/free-trial">` — a full restart: the user must
   **re-enter email, re-verify via a new email link, re-set a password**, then re-clone from
   scratch.

Goal: on failure, keep the *person* (email + password + verified state) so "Try again"
re-runs the clone **without** re-asking email/verification/password.

## Decision

We considered making all 8 steps idempotent (resume at the failed step, keep steps 1-4).
Rejected: idempotency for steps 3 (data copy — no natural "done" marker), 5 (connections —
also mints Dalgo-side OrgTask/dataflow rows), 6 (dbt — re-running would create a **second
GitHub repo**) and 8 (viz — large loop + cross-object id maps rebuilt by non-unique names) is
high-risk and hard to make "100% working".

**Chosen: full teardown on error, keep only the user identity. Retry re-clones from scratch.**
Simpler, zero idempotency, reuses the proven clone path. Trade-off: retry redoes the full
~90-120s clone instead of resuming — acceptable (trials fail rarely; that's the normal time).

## Design

### Backend

1. **Timeout moves into the Celery task.** `clone_trial_org_task` gets
   `soft_time_limit=300, time_limit=360`. At 300s Celery raises `SoftTimeLimitExceeded`
   *inside* the task — it subclasses `Exception`, so the existing `except Exception` in
   `clone_template_org` catches it → teardown → re-raise → task marks `"failed"`. No new code
   path; the timeout is just another exception. The 60s gap (`time_limit - soft_time_limit`)
   lets teardown finish before Celery SIGKILLs the worker.
   - Caveat: `SoftTimeLimitExceeded` fires at a Python bytecode boundary, so a mid-C-call block
     (pg_restore, a wedged HTTP socket) may not interrupt until it returns; the hard
     `time_limit` is the final backstop.

2. **Teardown keeps the person.** `_teardown` stops deleting the Django `User` /
   `UserAttributes`. `delete_org()` still removes the `OrgUser`, so
   `account_exists_for_email` (which keys on **OrgUser**, not User) stays `False` after a
   failed trial → retry is allowed, a completed trial (OrgUser present) is not. **No change to
   `account_exists_for_email`.**

3. **Store clone params at `/activate`**, keyed by task_id in Redis (email, org_name, role,
   template_id), TTL 24h — so `/retry` can re-enqueue without the consumed activation token.

4. **Lifetime lock.** Replace today's set-and-never-released `trial-activating:{email}` with
   `trial-clone-running:{email}`, `SET NX EX=360`, acquired before enqueue (by `/activate` and
   `/retry`), released by the task in a `finally`. Prevents two concurrent clones for one
   email; the TTL is the dead-worker backstop.

5. **`POST /trial/retry/{task_id}`** (public, no auth):
   - look up stored params by task_id → 400 if missing/expired;
   - `account_exists_for_email` → 409 "log in" if a real account now exists;
   - acquire lock → **409 "still finishing"** if held (guards the timeout/double-click path);
   - re-enqueue `clone_trial_org_task` with the **same** task_id (progress key reused; a fresh
     `TaskProgress` overwrites the old list on its first `add`), reset the start-time key.

### Frontend (webapp_v2)

6. `failed` card "Try again" → `POST /api/v1/public/trial/retry/{task_id}` instead of
   `<Link href="/free-trial">`; on success keep polling the same task_id.
7. Backend now owns the timeout, so raise/soften the frontend `TRIAL_HARD_TIMEOUT_SECONDS`
   (currently 300) above the backend total (~360+) — or reduce it to a pure
   backend-unreachable safety net — so the backend's `"failed"` always arrives first and the
   user lands on the single "Try again" card. The ambiguous "taking longer than expected" card
   (which could fire mid-clone) stops being a retry-race source.

## Non-goals

- Per-step resume / idempotency.
- Automated orphan reaping. A *rare* partial teardown (external service down at that instant)
  leaves a stray resource → next retry's fresh create collides → resolved manually with the
  existing `cleanup_trial_clone` management command (delete by email).
