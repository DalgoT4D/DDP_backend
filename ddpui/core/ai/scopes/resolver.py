"""Session scope resolution — dispatch (org, scope_type, scope_id) to a scope module.

One module per scope type lives beside this resolver (dashboard_scope.py, a
future report_scope.py); shared types are in base.py.
"""

from ddpui.core.ai.scopes.base import ORG_SCOPE, ResolvedScope, ScopeUnavailable
from ddpui.core.ai.scopes.dashboard_scope import resolve_dashboard_scope
from ddpui.models.org import Org


def resolve_scope(org: Org, scope_type: str, scope_id: int | None) -> ResolvedScope:
    """Resolve a session's scope into tables + prompt context. Raises
    ScopeUnavailable when the scope target is gone or has nothing to query."""
    if scope_type == "org" or scope_type is None:
        return ORG_SCOPE
    if scope_type == "dashboard":
        return resolve_dashboard_scope(org, scope_id)
    raise ScopeUnavailable(f"Unsupported chat scope '{scope_type}'.")
