from django.db.models import Q
from ddpui.models.org import OrgFeatureFlag, Org

FEATURE_FLAGS = {
    "DATA_QUALITY": "Elementary data quality reports",
    "USAGE_DASHBOARD": "Superset usage dashboard for org",
    "EMBED_SUPERSET": "Embed superset dashboards",
    "LOG_SUMMARIZATION": "Summarize logs using AI",
    "AI_DATA_ANALYSIS": "Enable data analysis using AI",
    "DATA_STATISTICS": "Enable detailed data statistics in explore",
    "AI_CHATBOT": "AI Chatbot for dashboard analysis",
}


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

    flag, created = OrgFeatureFlag.objects.get_or_create(
        org=org,
        flag_name=flag_name,
        defaults={"flag_value": True},
    )
    if not created:
        flag.flag_value = True
        flag.save()

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

    try:
        flag = OrgFeatureFlag.objects.get(org=org, flag_name=flag_name)
        flag.flag_value = False
        flag.save()
    except OrgFeatureFlag.DoesNotExist:
        # create an entry with disabling it
        OrgFeatureFlag.objects.create(org=org, flag_name=flag_name, flag_value=False)

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
