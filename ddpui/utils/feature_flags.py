import logging

from django.db import IntegrityError, transaction
from django.db.models import Q
from ddpui.models.org import OrgFeatureFlag, Org

logger = logging.getLogger(__name__)

FEATURE_FLAGS = {
    "DATA_QUALITY": "Elementary data quality reports",
    "USAGE_DASHBOARD": "Superset usage dashboard for org",
    "EMBED_SUPERSET": "Embed superset dashboards",
    "LOG_SUMMARIZATION": "Summarize logs using AI",
    "AI_DATA_ANALYSIS": "Enable data analysis using AI",
    "DATA_STATISTICS": "Enable detailed data statistics in explore",
    "REPORTS": "Enable reports feature",
}


def _safe_update_or_create(org, flag_name, flag_value):
    """
    Wrapper around update_or_create that retries once on IntegrityError.

    This handles the case where the PostgreSQL auto-increment sequence for
    the primary key is out of sync with the table data (e.g. due to rows
    inserted with explicit IDs via fixtures or manual SQL). On the first
    attempt the sequence may generate a colliding ID; the retry succeeds
    because the sequence advances past the conflict.

    The first attempt is wrapped in a savepoint (transaction.atomic) so that
    if it fails, the outer transaction is not aborted and the retry can proceed.
    """
    try:
        with transaction.atomic():
            return OrgFeatureFlag.objects.update_or_create(
                org=org,
                flag_name=flag_name,
                defaults={"flag_value": flag_value},
            )
    except IntegrityError:
        logger.warning(
            "IntegrityError on OrgFeatureFlag for org=%s flag=%s, retrying",
            org,
            flag_name,
        )
        return OrgFeatureFlag.objects.update_or_create(
            org=org,
            flag_name=flag_name,
            defaults={"flag_value": flag_value},
        )


def enable_feature_flag(flag_name: str, org: Org = None) -> bool:
    """
    Create a flag for the org (or global) in the db if not exists, and enable it.

    Returns:
        True if the flag was enabled successfully
        None if the flag_name is not in the allowed feature list
    """

    # only allow enable of flags in the feature list
    if flag_name not in FEATURE_FLAGS.keys():
        return None

    _safe_update_or_create(org, flag_name, True)

    return True


def disable_feature_flag(flag_name: str, org: Org = None) -> bool:
    """
    Disable a feature flag for the org (or global).
    Creates the entry as disabled if it doesn't exist.

    Returns:
        True if the flag was disabled successfully
        None if the flag_name is not in the allowed feature list
    """

    # only allow disable of flags in the feature list
    if flag_name not in FEATURE_FLAGS.keys():
        return None

    _safe_update_or_create(org, flag_name, False)

    return True


def is_feature_flag_enabled(flag_name: str, org: Org = None) -> bool:
    """Check if a flag is enabled for an org (or globally)"""
    try:
        flag = OrgFeatureFlag.objects.get(org=org, flag_name=flag_name)
        return flag.flag_value
    except OrgFeatureFlag.DoesNotExist:
        return None


def get_all_feature_flags_for_org(org: Org = None) -> dict:
    """
    Get all feature flags for an organization or globally.
    Returns a dict with flag_name as key and enabled status as value.
    Org-specific flags override global flags.

    Args:
        org: Organization object (optional). If None, returns only global flags.

    Returns:
        dict: {flag_name: bool} mapping of all flags and their status
    """
    # Get both global (org=None) and org-specific flags in one query
    flags = OrgFeatureFlag.objects.filter(Q(org__isnull=True) | Q(org=org))

    global_flags = {}
    for flag in flags:
        if flag.org is None:
            global_flags[flag.flag_name] = flag.flag_value

    for flag_name in FEATURE_FLAGS:
        if flag_name not in global_flags:
            global_flags[flag_name] = False

    org_flags = {}
    for flag in flags:
        if flag.org == org:
            org_flags[flag.flag_name] = flag.flag_value

    return {**global_flags, **org_flags}


def enable_all_global_feature_flags() -> dict:
    """
    Enable all available feature flags globally.

    Returns:
        dict: {flag_name: bool} mapping of all flags that were enabled
    """
    results = {}
    for flag_name in FEATURE_FLAGS:
        result = enable_feature_flag(flag_name, org=None)
        results[flag_name] = result
    return results
