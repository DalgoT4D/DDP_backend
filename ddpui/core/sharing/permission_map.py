"""The one map between shareable rtypes and Layer-2 permission slugs.

Used by: the grant FK backfill/dual-write, the pool builder's floor and
grant contributions, and (later) the share modal's dropdown mapping.
Implication (edit satisfies view) is derived from this same map — one
source, no separate vocabulary.

Note: "report" has no view/edit slugs in seeds (only ``can_share_reports``),
so it is absent here; report grants keep their varchar level until the full
v1.2 rollout adds those slugs.
"""

from typing import Dict, Optional, Set, Tuple

RTYPE_LEVEL_SLUG: Dict[Tuple[str, str], str] = {
    ("dashboard", "view"): "can_view_dashboards",
    ("dashboard", "edit"): "can_edit_dashboards",
    ("chart", "view"): "can_view_charts",
    ("chart", "edit"): "can_edit_charts",
    ("alert", "view"): "can_view_alerts",
    ("alert", "edit"): "can_edit_alerts",
    ("metric", "view"): "can_view_metrics",
    ("metric", "edit"): "can_edit_metrics",
    ("kpi", "view"): "can_view_kpis",
    ("kpi", "edit"): "can_edit_kpis",
}

# edit ⊇ view, per rtype: {"can_edit_dashboards": "can_view_dashboards", ...}
IMPLIES: Dict[str, str] = {
    RTYPE_LEVEL_SLUG[(rtype, "edit")]: RTYPE_LEVEL_SLUG[(rtype, "view")]
    for (rtype, level) in RTYPE_LEVEL_SLUG
    if level == "edit"
}

# Process cache of Permission slug -> pk. The table only changes via seeds,
# so a per-process load is safe; reset_permission_id_cache() for tests.
_slug_to_id: Optional[Dict[str, int]] = None


def slug_for(rtype: str, level: str) -> Optional[str]:
    """Permission slug for (rtype, view|edit); None for unmapped rtypes."""
    return RTYPE_LEVEL_SLUG.get((rtype, level))


def implied_closure(slugs: Set[str]) -> Set[str]:
    """`slugs` plus everything they imply (edit slugs add their view slug)."""
    out = set(slugs)
    for slug in slugs:
        implied = IMPLIES.get(slug)
        while implied is not None and implied not in out:
            out.add(implied)
            implied = IMPLIES.get(implied)
    return out


def permission_id_for(rtype: str, level: str) -> Optional[int]:
    """Pk of the Permission row for (rtype, level); None when the rtype is
    unmapped or the row is absent (e.g. a test DB without seeds)."""
    global _slug_to_id
    slug = RTYPE_LEVEL_SLUG.get((rtype, level))
    if slug is None:
        return None
    if _slug_to_id is None:
        from ddpui.models.role_based_access import Permission

        loaded = dict(Permission.objects.values_list("slug", "id"))
        if not loaded:  # unseeded DB (some test setups) — don't poison the cache
            return None
        _slug_to_id = loaded
    return _slug_to_id.get(slug)


def reset_permission_id_cache() -> None:
    """Drop the cached slug->pk map (tests re-seed Permission rows)."""
    global _slug_to_id
    _slug_to_id = None
