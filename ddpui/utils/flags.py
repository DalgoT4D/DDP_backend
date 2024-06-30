from flags import conditions


@conditions.register("org_slug")
def org_condition(org_slug, request_org_slug=None):
    """Def"""
    return request_org_slug == org_slug
