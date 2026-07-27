"""Google OAuth provider registry.

Every Airbyte source connector that authenticates via Google OAuth is registered here, keyed
by its Airbyte source-definition NAME (e.g. "Google Sheets"), not its `sourceDefinitionId` —
the id differs per connector version / Airbyte install, so it is not a stable key across
deployments. The caller resolves the id to a name against its own workspace catalog before
looking a connector up here. All Google connectors share one Google Cloud OAuth app
(client_id/secret from env); only the requested `scopes` and the connector-specific
`credentials` block shape vary. To add another Google connector (Drive, Analytics), add one
registry entry — no changes to the oauth service, API, or callback.

Scope note: this is Google-only (Google is the single identity provider). A different provider
(Salesforce, Zoho — different auth/token endpoints and auth params) would be a separate
provider module with its own connector registry; the oauth service would pick between them.
Not built here (YAGNI).
"""

import os
from typing import Callable

from ninja.errors import HttpError
from pydantic import BaseModel, ConfigDict

# Google's identity endpoints — shared by every Google connector below.
GOOGLE_OAUTH_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"

# Airbyte's source-definition name for the Google Sheets connector, as it appears in the
# workspace catalog. Names are stable across connector versions and workspaces; ids are not.
GSHEETS_SOURCE_NAME = "Google Sheets"


class GoogleOAuthConnector(BaseModel):
    """Per-connector Google OAuth config. `credentials_builder(client_id, client_secret,
    refresh_token)` returns the connector-specific `credentials` block Airbyte validates on
    source save."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    scopes: list[str]
    credentials_builder: Callable[[str, str, str], dict]


def _gsheets_credentials(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """Google Sheets connector's OAuth "Client" branch — requires all three."""
    return {
        "auth_type": "Client",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }


def _normalize_source_name(source_name: str) -> str:
    """Registry lookup key: case- and whitespace-insensitive source-definition name, so
    "Google Sheets", "google sheets" and "Google  Sheets" all resolve to the same entry."""
    return " ".join(source_name.split()).lower()


# Registry: normalized source-definition name -> GoogleOAuthConnector. The single source of
# truth for "which connectors support the Dalgo-driven Google OAuth flow".
GOOGLE_OAUTH_CONNECTORS: dict[str, GoogleOAuthConnector] = {
    _normalize_source_name(GSHEETS_SOURCE_NAME): GoogleOAuthConnector(
        # Least privilege: read-only Sheets data (no Drive). If a real sync against the
        # target gsheets connector version demands Drive, add
        # "https://www.googleapis.com/auth/drive.readonly" (a restricted scope — heavier
        # Google verification).
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        credentials_builder=_gsheets_credentials,
    ),
}


def get_connector(source_name: str) -> GoogleOAuthConnector:
    """Look up the Google OAuth connector by source-definition name. Raises 400 for an
    unsupported source rather than silently falling back to a wrong scope/credentials
    shape."""
    connector = GOOGLE_OAUTH_CONNECTORS.get(_normalize_source_name(source_name))
    if connector is None:
        raise HttpError(400, "oauth is not supported for this source")
    return connector


def oauth_client_id() -> str:
    """Google Cloud OAuth app client_id, shared by every Google connector"""
    client_id = os.getenv("AIRBYTE_GOOGLE_OAUTH_CLIENT_ID")
    if not client_id:
        raise HttpError(500, "google oauth client id is not configured")
    return client_id


def oauth_client_secret() -> str:
    """Google Cloud OAuth app client_secret — never leaves the backend"""
    client_secret = os.getenv("AIRBYTE_GOOGLE_OAUTH_CLIENT_SECRET")
    if not client_secret:
        raise HttpError(500, "google oauth client secret is not configured")
    return client_secret
