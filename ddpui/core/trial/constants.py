"""Leaf-module constants for the free-trial feature.

Deliberately dependency-free: this module must be importable without pulling in
Airbyte/dbt/Prefect/viz or any other heavyweight integration, so that both
``clone_service`` (the full clone pipeline) and ``lifecycle_emails`` (the hourly
sweep) can depend on it without ``lifecycle_emails`` transitively importing the
whole clone pipeline. See ``ddpui/core/trial/clone_service.py`` and
``ddpui/core/trial/lifecycle_emails.py`` for the two consumers.
"""

# every trial expires this many days after the clone; OrgPlans.start_date/end_date are set from
# this at clone time so plan-expiry checks (and any expiry-based reaping) have real dates to work
# with instead of the None/None an unbounded plan would get.
TRIAL_DURATION_DAYS = 14

# First word of every clone-created org's name — see `_step_org_and_user` in clone_service.
TRIAL_ORG_NAME_PREFIX = "Trial"

# ...and the slug prefix that follows from it, since `create_organization` derives
# `org.slug = slugify(org.name)[:20]`. This is the ONLY marker that a clone created an org, so
# it is what the expired-trial reaper filters on: `OrgPlans.base_plan == FREE_TRIAL` alone is
# NOT safe, because `create_org_plan` lets an admin put a real customer org on the Free Trial
# plan, and reaping those would delete a paying org and its warehouse. Keep in sync with
# TRIAL_ORG_NAME_PREFIX above — a change to the org-name shape that isn't mirrored here makes
# the reaper silently match nothing.
TRIAL_ORG_SLUG_PREFIX = "trial-"

# The prefix alone is too loose to delete on: a real org named "Trial Foundation" slugs to
# "trial-foundation" and would match. Every clone slug is `trial-<email_hash8>-<label>` and
# `email_hash8` is an 8-char slice of a sha256 hexdigest, so requiring those 8 hex chars is a
# marker no human-chosen org name realistically collides with. Used both as a Django `__regex`
# lookup (Postgres `~`) and via `re` for in-Python checks; slugs are always lowercase.
TRIAL_ORG_SLUG_REGEX = r"^trial-[0-9a-f]{8}-"

# gap between orgs in a `--expired` reap. One teardown hits Airbyte, GitHub, Prefect AND the
# shared trials RDS; firing several back-to-back would burst all four at once, on instances also
# serving live users and live clones. Spacing them keeps the load flat.
TRIAL_REAP_STAGGER_SECONDS = 30
