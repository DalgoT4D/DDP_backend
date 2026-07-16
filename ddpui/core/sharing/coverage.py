"""Read-only chart-coverage verdicts for dashboards.

A chart renders inline wherever its dashboard renders, so a dashboard's
audience can be wider than a tile chart's own. For one dashboard and a set
of charts this module reports where the two diverge:

- role gap: a role the dashboard admits that the chart's levels don't
  (analyst gaps are extendable; member gaps are informational — charts
  can't admit Members yet).
- principal gap: a dashboard grant whose principal can't see the chart
  standalone (Member principals flagged ``skipped_member``, never copied).
- public exposure: the dashboard's public link is on — acknowledgeable only,
  charts have no public links.

Deliberately dashboard+chart specific: dashboards are the only container
rtype (reports copy chart configs by value). Pure reads over batched
queries — a fixed number of queries regardless of tile/grant counts.
"""

from typing import Dict, Iterable, List, Optional, Set

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.core.sharing.access_resolver import _is_owner, _role_slug, principal_match_q
from ddpui.core.sharing.chart_access import dashboard_chart_ids
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import ACCESS_LEVEL_RANK, AccessLevel
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.user_group import UserGroup, UserGroupMember, UserGroupMemberStatus
from ddpui.models.visualization import Chart
from ddpui.schemas.access_schema import ChartCoverageOut, PrincipalGapOut

_ADMIN_SLUGS = (ADMIN_ROLE, SUPER_ADMIN_ROLE)

# Role-gap identifiers in ``ChartCoverageOut.role_gaps``.
ANALYST_GAP = "analyst"
MEMBER_GAP = "member"


def _level_admits(level: Optional[str]) -> bool:
    """True when a general-access level admits its role to at least view."""
    return bool(level) and level != AccessLevel.NONE


def _orguser_display(orguser: OrgUser) -> str:
    user = orguser.user
    return f"{user.first_name} {user.last_name}".strip() or user.email


class _CoverageContext:
    """Batched lookups shared by every per-chart verdict: built with a fixed
    number of queries up front, then consulted in memory."""

    def __init__(self, viewer: OrgUser, dashboard: Dashboard, charts: List[Chart]):
        self.viewer = viewer
        self.dashboard = dashboard
        self.charts = charts
        org_id = dashboard.org_id
        chart_id_strs = [str(c.pk) for c in charts]

        # Active direct grants ON the dashboard — the audience the tiles
        # must cover. Pending rows aren't people yet; skipped.
        self.dashboard_grants = list(
            ResourceShare.objects.filter(
                org_id=org_id,
                resource_type="dashboard",
                resource_id=str(dashboard.pk),
                status="active",
            ).order_by("id")
        )

        # Active grants ON the charts — what the tiles offer back.
        self.chart_user_grants: Dict[int, Set[int]] = {}  # chart_id -> orguser ids
        self.chart_group_grants: Dict[int, Set[int]] = {}  # chart_id -> group ids
        for share in ResourceShare.objects.filter(
            org_id=org_id,
            resource_type="chart",
            resource_id__in=chart_id_strs,
            status="active",
        ):
            try:
                chart_pk = int(share.resource_id)
            except (TypeError, ValueError):
                continue
            if share.principal_type == "user" and share.principal_id is not None:
                self.chart_user_grants.setdefault(chart_pk, set()).add(share.principal_id)
            elif share.principal_type == "group" and share.principal_id is not None:
                self.chart_group_grants.setdefault(chart_pk, set()).add(share.principal_id)

        # Principal OrgUsers named by the dashboard's user grants.
        user_ids = [
            s.principal_id
            for s in self.dashboard_grants
            if s.principal_type == "user" and s.principal_id is not None
        ]
        self.users_by_id: Dict[int, OrgUser] = {
            ou.id: ou
            for ou in OrgUser.objects.filter(id__in=user_ids).select_related("user", "new_role")
        }

        # Those principals' active group memberships — a chart GROUP grant
        # covers a dashboard USER principal who belongs to that group.
        self.groups_by_orguser: Dict[int, Set[int]] = {}
        for orguser_id, group_id in UserGroupMember.objects.filter(
            orguser_id__in=user_ids, status=UserGroupMemberStatus.ACTIVE
        ).values_list("orguser_id", "group_id"):
            self.groups_by_orguser.setdefault(orguser_id, set()).add(group_id)

        # Group principals named by the dashboard's group grants.
        group_ids = [
            s.principal_id
            for s in self.dashboard_grants
            if s.principal_type == "group" and s.principal_id is not None
        ]
        self.groups_by_id: Dict[int, UserGroup] = {
            g.id: g for g in UserGroup.objects.filter(id__in=group_ids, org_id=org_id)
        }

        # The viewer's own chart grants (for ``viewer_can_edit``), one query.
        self.viewer_edit_grant_chart_ids: Set[int] = set()
        viewer_slug = _role_slug(viewer)
        if viewer_slug not in _ADMIN_SLUGS:
            for resource_id, permission in ResourceShare.objects.filter(
                principal_match_q(viewer),
                org_id=org_id,
                resource_type="chart",
                resource_id__in=chart_id_strs,
            ).values_list("resource_id", "permission"):
                if permission == "edit":
                    try:
                        self.viewer_edit_grant_chart_ids.add(int(resource_id))
                    except (TypeError, ValueError):
                        continue

    # -- per-chart rules ---------------------------------------------------

    def chart_covers_orguser(self, chart: Chart, principal: OrgUser) -> bool:
        """Does `principal` resolve to >= view on `chart` standalone? Mirrors
        `access_resolver.effective_permission` on batched data — keep in lockstep."""
        slug = _role_slug(principal)
        if slug in _ADMIN_SLUGS:
            return True
        if _is_owner(principal, chart):
            return True
        # Member viewers get nothing from general access or grants on charts
        # (member_sharing=False) — resolver rule, mirrored here.
        if slug == MEMBER_ROLE:
            return False
        if slug == ANALYST_ROLE and _level_admits(chart.analyst_level):
            return True
        if principal.id in self.chart_user_grants.get(chart.id, set()):
            return True
        principal_groups = self.groups_by_orguser.get(principal.id, set())
        return bool(principal_groups & self.chart_group_grants.get(chart.id, set()))

    def chart_covers_group(self, chart: Chart, group_id: int) -> bool:
        """A group principal is only covered by a matching group grant on the
        chart — a group can mix roles, so general access can't vouch for all members."""
        return group_id in self.chart_group_grants.get(chart.id, set())

    def viewer_can_edit_chart(self, chart: Chart) -> bool:
        """Resolver-edit on the chart for the calling viewer, off batched data."""
        slug = _role_slug(self.viewer)
        if slug in _ADMIN_SLUGS:
            return True
        if _is_owner(self.viewer, chart):
            return True
        if slug == MEMBER_ROLE:
            return False
        if slug == ANALYST_ROLE and chart.analyst_level == AccessLevel.EDIT:
            return True
        return chart.id in self.viewer_edit_grant_chart_ids

    def principal_gaps_for_chart(self, chart: Chart) -> List[PrincipalGapOut]:
        """Every dashboard direct-grant principal (deduplicated) that this
        chart does not admit standalone."""
        gaps: List[PrincipalGapOut] = []
        seen: Set[tuple] = set()
        for share in self.dashboard_grants:
            key = (share.principal_type, share.principal_id)
            if key in seen:
                continue
            seen.add(key)
            if share.principal_type == "user":
                principal = self.users_by_id.get(share.principal_id)
                if principal is None:
                    continue  # pending/dangling rows aren't people yet
                if self.chart_covers_orguser(chart, principal):
                    continue
                gaps.append(
                    PrincipalGapOut(
                        principal_type="user",
                        principal_id=principal.id,
                        name=_orguser_display(principal),
                        email=principal.user.email,
                        skipped_member=_role_slug(principal) == MEMBER_ROLE,
                    )
                )
            elif share.principal_type == "group":
                group = self.groups_by_id.get(share.principal_id)
                if group is None:
                    continue
                if self.chart_covers_group(chart, group.id):
                    continue
                gaps.append(
                    PrincipalGapOut(
                        principal_type="group",
                        principal_id=group.id,
                        name=group.name,
                        email=None,
                        skipped_member=False,
                    )
                )
        return gaps


def _verdict(
    ctx: _CoverageContext,
    chart: Chart,
    role_gaps: List[str],
    principal_gaps: List[PrincipalGapOut],
    public_exposure: bool,
) -> ChartCoverageOut:
    extendable = ANALYST_GAP in role_gaps or any(not gap.skipped_member for gap in principal_gaps)
    return ChartCoverageOut(
        chart_id=chart.id,
        title=chart.title,
        covered=not (role_gaps or principal_gaps or public_exposure),
        role_gaps=role_gaps,
        principal_gaps=principal_gaps,
        public_exposure=public_exposure,
        extendable=extendable,
        viewer_can_edit=ctx.viewer_can_edit_chart(chart),
    )


def _full_verdict(ctx: _CoverageContext, chart: Chart) -> ChartCoverageOut:
    """Coverage against the dashboard's current audience."""
    dashboard = ctx.dashboard
    role_gaps: List[str] = []
    if _level_admits(dashboard.analyst_level) and not _level_admits(chart.analyst_level):
        role_gaps.append(ANALYST_GAP)
    # member_level is pinned to "none" on charts, so a member-visible
    # dashboard always exposes past the chart's own levels.
    if _level_admits(dashboard.member_level):
        role_gaps.append(MEMBER_GAP)
    return _verdict(
        ctx,
        chart,
        role_gaps,
        ctx.principal_gaps_for_chart(chart),
        bool(dashboard.is_public),
    )


def _tile_charts(dashboard: Dashboard) -> List[Chart]:
    tile_ids = dashboard_chart_ids(dashboard)
    if not tile_ids:
        return []
    return list(Chart.objects.filter(id__in=tile_ids, org_id=dashboard.org_id).order_by("id"))


def coverage_for_charts(
    viewer: OrgUser, dashboard: Dashboard, charts: Iterable[Chart]
) -> List[ChartCoverageOut]:
    """Full verdicts for specific charts — the charts need not be tiles yet."""
    charts = list(charts)
    ctx = _CoverageContext(viewer, dashboard, charts)
    return [_full_verdict(ctx, chart) for chart in charts]


def dashboard_under_covering_charts(
    viewer: OrgUser, dashboard: Dashboard
) -> List[ChartCoverageOut]:
    """Every tile of `dashboard` whose verdict is not `covered`."""
    return [
        v for v in coverage_for_charts(viewer, dashboard, _tile_charts(dashboard)) if not v.covered
    ]


# ================================================================================
# Action-scoped variants — which tiles would THIS widening newly expose?
# The prompt names only the charts the action is about, not every existing gap.
# ================================================================================


def under_covering_for_general_widening(
    viewer: OrgUser, dashboard: Dashboard, new_analyst_level: str, new_member_level: str
) -> List[ChartCoverageOut]:
    """Tiles a general-access raise would expose. Analyst raise: tiles whose
    `analyst_level` is "none". Member raise: every tile (charts can't admit
    Members). A view -> edit raise still checks against the post-change levels."""
    analyst_widened = ACCESS_LEVEL_RANK.get(new_analyst_level, 0) > ACCESS_LEVEL_RANK.get(
        dashboard.analyst_level, 0
    )
    member_widened = ACCESS_LEVEL_RANK.get(new_member_level, 0) > ACCESS_LEVEL_RANK.get(
        dashboard.member_level, 0
    )
    if not (analyst_widened or member_widened):
        return []

    charts = _tile_charts(dashboard)
    ctx = _CoverageContext(viewer, dashboard, charts)
    verdicts = []
    for chart in charts:
        role_gaps: List[str] = []
        if (
            analyst_widened
            and _level_admits(new_analyst_level)
            and not _level_admits(chart.analyst_level)
        ):
            role_gaps.append(ANALYST_GAP)
        if member_widened and _level_admits(new_member_level):
            role_gaps.append(MEMBER_GAP)
        if role_gaps:
            verdicts.append(_verdict(ctx, chart, role_gaps, [], False))
    return verdicts


def under_covering_for_new_principal(
    viewer: OrgUser,
    dashboard: Dashboard,
    principal_orguser: Optional[OrgUser] = None,
    principal_group: Optional[UserGroup] = None,
    invite_role: Optional[str] = None,
) -> List[ChartCoverageOut]:
    """Tiles a new direct grant on the dashboard would expose to that principal.
    Pass exactly one of `principal_orguser` / `principal_group` / `invite_role`
    (the unknown-email path — coverage judged by the role the invite would mint)."""
    charts = _tile_charts(dashboard)
    if not charts:
        return []
    ctx = _CoverageContext(viewer, dashboard, charts)

    if principal_orguser is not None:
        # The context only batch-loads memberships for the existing audience —
        # load the new principal's too, or a group-covered chart would falsely warn.
        ctx.groups_by_orguser[principal_orguser.id] = set(
            UserGroupMember.objects.filter(
                orguser_id=principal_orguser.id, status=UserGroupMemberStatus.ACTIVE
            ).values_list("group_id", flat=True)
        )

    verdicts = []
    for chart in charts:
        gap: Optional[PrincipalGapOut] = None
        if principal_orguser is not None:
            if not ctx.chart_covers_orguser(chart, principal_orguser):
                gap = PrincipalGapOut(
                    principal_type="user",
                    principal_id=principal_orguser.id,
                    name=_orguser_display(principal_orguser),
                    email=principal_orguser.user.email,
                    skipped_member=_role_slug(principal_orguser) == MEMBER_ROLE,
                )
        elif principal_group is not None:
            if not ctx.chart_covers_group(chart, principal_group.id):
                gap = PrincipalGapOut(
                    principal_type="group",
                    principal_id=principal_group.id,
                    name=principal_group.name,
                    email=None,
                    skipped_member=False,
                )
        else:
            role = invite_role or MEMBER_ROLE
            covered = role in _ADMIN_SLUGS or (
                role == ANALYST_ROLE and _level_admits(chart.analyst_level)
            )
            if not covered:
                gap = PrincipalGapOut(
                    principal_type="invite",
                    principal_id=None,
                    name=None,
                    email=None,
                    skipped_member=role == MEMBER_ROLE,
                )
        if gap is not None:
            verdicts.append(_verdict(ctx, chart, [], [gap], False))
    return verdicts


def under_covering_for_public_enable(
    viewer: OrgUser, dashboard: Dashboard
) -> List[ChartCoverageOut]:
    """Every tile, flagged as public exposure — enabling the public link
    exposes all inline content anonymously."""
    charts = _tile_charts(dashboard)
    if not charts:
        return []
    ctx = _CoverageContext(viewer, dashboard, charts)
    return [_verdict(ctx, chart, [], [], True) for chart in charts]
