from django.db.models import Q
from ddpui.models.org import OrgFeatureFlag, Org

FEATURE_FLAGS = {
    "DATA_QUALITY": "Elementary data quality reports",
    "USAGE_DASHBOARD": "Superset usage dashboard for org",
    "EMBED_SUPERSET": "Embed superset dashboards",
    "LOG_SUMMARIZATION": "Summarize logs using AI",
    "AI_DATA_ANALYSIS": "Enable data analysis using AI",
    "DATA_STATISTICS": "Enable detailed data statistics in explore",
}


def enable_feature_flag(flag_name: str, org: Org = None) -> bool:
    """Create a flag for the org (or global) in the db if not exists, and return its value. If its there toggle it on."""

    # only allow enable of flags in the feature list
    if flag_name not in FEATURE_FLAGS.keys():
        return None

    flag, created = OrgFeatureFlag.objects.get_or_create(
        org=org,
        flag_name=flag_name,
        defaults={"flag_value": True},
    )
    if not created:
        flag.flag_value = True
        flag.save()

    return flag.flag_value


def disable_feature_flag(flag_name: str, org: Org = None) -> bool:
    """If the entry exists, toggle it off and return its value"""

    try:
        flag = OrgFeatureFlag.objects.get(org=org, flag_name=flag_name)
        flag.flag_value = False
        flag.save()
        return flag.flag_value
    except OrgFeatureFlag.DoesNotExist:
        return None


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
    result = {}

    # Get both global (org=None) and org-specific flags in one query
    flags = OrgFeatureFlag.objects.filter(Q(org__isnull=True) | Q(org=org))

    # Sort in memory: global flags (org=None) first, then org-specific flags
    sorted_flags = sorted(flags, key=lambda f: (f.org is not None, f.flag_name))

    # Process flags - org-specific will override global due to sorting
    for flag in sorted_flags:
        result[flag.flag_name] = flag.flag_value

    # For any flags that don't exist in DB but are in FEATURE_FLAGS, default to False
    for flag_name in FEATURE_FLAGS:
        if flag_name not in result:
            result[flag_name] = False

    return result


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
