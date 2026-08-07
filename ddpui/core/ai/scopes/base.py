"""Shared types for session scopes — what an AI session may see."""

from dataclasses import dataclass


class ScopeUnavailable(Exception):
    """The session's scope can't be resolved (dashboard deleted, empty, …).
    The message is user-facing — the transport layer sends it as an error event."""


@dataclass(frozen=True)
class ResolvedScope:
    """What a scoped session may see: the table allowlist for the SQL guard and
    the prompt context block. allowed_tables=None means org-wide (no restriction)."""

    scope_type: str
    allowed_tables: list[str] | None = None
    scope_context: str = ""


ORG_SCOPE = ResolvedScope(scope_type="org")
