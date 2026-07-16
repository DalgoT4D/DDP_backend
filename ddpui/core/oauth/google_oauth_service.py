"""Google OAuth flow (Variant A — Dalgo performs the token exchange).

Dalgo builds the Google consent URL itself, receives Google's redirect on its own backend
callback, exchanges the auth code for a refresh_token server-side, and stashes it in Redis
under a single-use opaque `ref`. The browser never sees the auth code, the client_secret, or
the refresh_token — only the `ref`. Airbyte's source_oauths/* is not involved; the
per-source scopes and credentials shape come from the provider registry.
"""

import json
import os
import secrets
from urllib.parse import urlencode

import requests
from ninja.errors import HttpError

from ddpui.core.oauth.google_oauth_provider import (
    GOOGLE_OAUTH_AUTH_URL,
    GOOGLE_OAUTH_TOKEN_URL,
    get_connector,
    oauth_client_id,
    oauth_client_secret,
)
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.redis_client import RedisClient

logger = CustomLogger("ddpui.core.oauth")

# state: CSRF nonce bound to the orguser that started the flow (consent -> callback window)
OAUTH_STATE_REDIS_PREFIX = "airbyte_oauth_state"
OAUTH_STATE_TTL_SECONDS = 600  # 10 minutes
# ref: opaque handle to a stashed refresh_token (callback -> create-source window)
OAUTH_REF_REDIS_PREFIX = "airbyte_oauth_ref"
OAUTH_REF_TTL_SECONDS = 300  # 5 minutes


def _oauth_redirect_url() -> str:
    """Dalgo's BACKEND callback URL that Google redirects to after consent. Must match a
    redirect URI registered in the Google OAuth app, and be byte-for-byte identical in the
    consent URL and the token exchange. Read from AIRBYTE_OAUTH_REDIRECT_URL (ends in
    /api/airbyte/sources/oauth/callback)."""
    redirect_url = os.getenv("AIRBYTE_OAUTH_REDIRECT_URL")
    if not redirect_url:
        raise HttpError(500, "oauth redirect url is not configured")
    return redirect_url


def _frontend_oauth_callback_url() -> str:
    """The webapp_v2 page the backend redirects the popup to after the exchange, carrying
    only the opaque `ref` (or an `error`). Derived from FRONTEND_URL_V2 (falls back to
    FRONTEND_URL)."""
    frontend_url = os.getenv("FRONTEND_URL_V2") or os.getenv("FRONTEND_URL")
    if not frontend_url:
        raise HttpError(500, "frontend url is not configured")
    return f"{frontend_url.rstrip('/')}/oauth/airbyte/callback"


def _store_oauth_state(orguser: OrgUser, source_def_id: str) -> str:
    """Generate a state nonce, remember who/what it belongs to, auto-expire in 10 min"""
    state = secrets.token_urlsafe(48)
    RedisClient.get_instance().set(
        f"{OAUTH_STATE_REDIS_PREFIX}:{state}",
        json.dumps(
            {
                "orguser_id": orguser.id,
                "workspace_id": orguser.org.airbyte_workspace_id,
                "source_definition_id": source_def_id,
            }
        ),
        ex=OAUTH_STATE_TTL_SECONDS,
    )
    return state


def _atomic_pop(key: str):
    """Atomically read and delete a key, returning its value (or None). Uses a MULTI/EXEC
    pipeline rather than GETDEL so it works on Redis < 6.2 while keeping the single-use /
    replay guarantee (the get and delete run as one transaction)."""
    pipe = RedisClient.get_instance().pipeline()
    pipe.get(key)
    pipe.delete(key)
    value, _ = pipe.execute()
    return value


def _pop_oauth_state(state: str) -> dict:
    """Validate + consume a state nonce on the (JWT-less) backend callback. The unguessable
    nonce IS the authentication — it identifies the orguser that started the flow. Rejects
    missing/expired/reused states; the atomic pop closes the replay window."""
    raw = _atomic_pop(f"{OAUTH_STATE_REDIS_PREFIX}:{state}")
    if raw is None:
        raise HttpError(400, "invalid or expired oauth state")
    return json.loads(raw)


def _store_oauth_ref(orguser_id: int, source_def_id: str, refresh_token: str) -> str:
    """Stash the refresh_token server-side under a fresh single-use handle. The browser
    only ever receives this `ref`, never the token."""
    ref = secrets.token_urlsafe(48)
    RedisClient.get_instance().set(
        f"{OAUTH_REF_REDIS_PREFIX}:{ref}",
        json.dumps(
            {
                "orguser_id": orguser_id,
                "source_definition_id": source_def_id,
                "refresh_token": refresh_token,
            }
        ),
        ex=OAUTH_REF_TTL_SECONDS,
    )
    return ref


def redeem_ref(orguser: OrgUser, ref: str, source_def_id: str) -> str:
    """Redeem a ref at create-source time; return the stashed refresh_token. Rejects a
    missing/expired ref, or one minted for a different orguser or source definition."""
    raw = _atomic_pop(f"{OAUTH_REF_REDIS_PREFIX}:{ref}")
    if raw is None:
        raise HttpError(400, "invalid or expired oauth session")
    stored = json.loads(raw)
    if stored["orguser_id"] != orguser.id or stored["source_definition_id"] != source_def_id:
        raise HttpError(403, "oauth session does not match this request")
    return stored["refresh_token"]


def get_source_oauth_consent(orguser: OrgUser, source_def_id: str) -> dict:
    """Start the Google OAuth flow: mint a state nonce and build the Google consent URL.

    Dalgo (not Airbyte) owns the flow. client_id comes from env, redirect_uri is Dalgo's
    own backend callback, scopes come from the source's provider entry. `access_type=offline`
    + `prompt=consent` guarantee Google returns a refresh_token on every authorization
    (including re-auth of an existing source)."""
    connector = get_connector(source_def_id)
    state = _store_oauth_state(orguser, source_def_id)
    params = {
        "client_id": oauth_client_id(),
        "redirect_uri": _oauth_redirect_url(),
        "response_type": "code",
        "scope": " ".join(connector.scopes),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return {"authUrl": f"{GOOGLE_OAUTH_AUTH_URL}?{urlencode(params)}"}


def exchange_google_oauth_code(code: str) -> dict:
    """Exchange the authorization code for tokens directly with Google (server-side).

    Returns Google's token response. Raises if the exchange failed or no refresh_token came
    back (which happens if access_type=offline/prompt=consent were dropped)."""
    try:
        response = requests.post(
            GOOGLE_OAUTH_TOKEN_URL,
            data={
                "code": code,
                "client_id": oauth_client_id(),
                "client_secret": oauth_client_secret(),
                "redirect_uri": _oauth_redirect_url(),
                "grant_type": "authorization_code",
            },
            timeout=30,
        )
    except requests.exceptions.RequestException as err:
        # timeout / connection error reaching Google — surface as a normal oauth failure
        logger.error("google token exchange request error: %s", err)
        raise HttpError(500, "failed to complete the oauth flow") from err
    if response.status_code != 200:
        # the body may carry error detail; log it, don't surface it to the caller
        logger.error("google token exchange failed: %s %s", response.status_code, response.text)
        raise HttpError(500, "failed to complete the oauth flow")
    try:
        tokens = response.json()
    except ValueError as err:
        # a 200 with a non-JSON body — treat as a failed exchange, not a 500 crash
        logger.error("google token exchange returned non-JSON body: %s", response.text[:200])
        raise HttpError(500, "failed to complete the oauth flow") from err
    if "refresh_token" not in tokens:
        logger.error("google token exchange returned no refresh_token: keys=%s", list(tokens))
        raise HttpError(400, "oauth did not return a refresh token")
    return tokens


def complete_source_oauth(state: str, code: str) -> str:
    """Backend-callback step (no JWT): validate the state nonce, exchange the code with
    Google, stash the refresh_token under a fresh ref, and return the ref. The state nonce
    is the authentication — it recovers the orguser that started the flow."""
    stored = _pop_oauth_state(state)
    tokens = exchange_google_oauth_code(code)
    return _store_oauth_ref(
        stored["orguser_id"], stored["source_definition_id"], tokens["refresh_token"]
    )


def oauth_callback_redirect_url(state: str, code: str, error: str) -> str:
    """Build the frontend redirect URL for the backend OAuth callback.

    On success the URL carries the opaque `ref`; on any failure (Google denial, missing
    code/state, or a failed exchange) it carries an `error` code. Never raises for OAuth
    failures — the callback must always redirect the popup somewhere it can close and report.
    (A missing frontend-url config still 500s, as that is a deployment error, not an OAuth
    outcome.)"""
    frontend_url = _frontend_oauth_callback_url()
    if error or not code or not state:
        reason = error or "missing_code_or_state"
        return f"{frontend_url}?{urlencode({'error': reason})}"
    try:
        ref = complete_source_oauth(state, code)
    except HttpError as err:
        logger.error("oauth callback failed: %s", err)
        return f"{frontend_url}?{urlencode({'error': 'oauth_failed'})}"
    return f"{frontend_url}?{urlencode({'ref': ref})}"
