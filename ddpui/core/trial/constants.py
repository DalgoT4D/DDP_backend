"""Leaf-module constants for the free-trial feature.

Dependency-free on purpose, so `lifecycle_emails` can import it without pulling in the
whole clone pipeline via `clone_service`.
"""

# trial window; written to OrgPlans.start_date/end_date at clone time
TRIAL_DURATION_DAYS = 14

# first word of every clone-created org's name — see `_step_org_and_user`
TRIAL_ORG_NAME_PREFIX = "Trial"

# slug prefix that follows, since create_organization does slugify(name)[:20].
# Keep in sync with TRIAL_ORG_NAME_PREFIX or the expired-trial deleter matches nothing.
TRIAL_ORG_SLUG_PREFIX = "trial-"

# THE marker that a clone made an org. The expired-trial deleter filters on this, not on
# base_plan — an admin can put a real customer on the FREE_TRIAL plan, and deleting those
# would destroy a paying org. The 8 hex chars matter: bare "trial-" would also match a real
# org named "Trial Foundation". Used as a Django __regex lookup and via `re`.
TRIAL_ORG_SLUG_REGEX = r"^trial-[0-9a-f]{8}-"

# gap between orgs in an `--expired` sweep. One teardown hits Airbyte, GitHub, Prefect and the
# shared trials RDS; back-to-back teardowns burst all four on instances also serving live users.
TRIAL_DELETE_STAGGER_SECONDS = 30

# A failed clone's teardown runs ONCE — nothing re-runs it — so one transient 502 from
# Airbyte/GitHub/Prefect would strand the org. Retry each teardown action this many times.
# The delay is tiny on purpose: teardown shares the celery task's hard-vs-soft time-limit
# window (see CLONE_SOFT/HARD_TIME_LIMIT in core/trial/tasks.py), so it cannot afford real
# backoff. This clears single-call blips, not an outage — for that, the org delete backdates
# OrgPlans.end_date so the hourly `--expired` sweep picks it up.
TEARDOWN_ATTEMPTS = 2
TEARDOWN_RETRY_DELAY_SECONDS = 3

# `--expired` runs with NO mutex, deliberately. Two overlapping sweeps would select the same
# not-yet-deleted orgs and tear one down twice — but that needs a >1h run (~120 orgs) or a
# second scheduler, and Dalgo runs one celery-beat on one EC2. If either changes, add a Redis
# mutex with a TTL and an owner-token check on release.
