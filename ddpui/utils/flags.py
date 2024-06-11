from flags import conditions


@conditions.register("org")
def org_condition(org_id, request_org_id=None):
    """Def"""
    return request_org_id == org_id
