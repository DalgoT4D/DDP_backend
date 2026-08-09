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
