import json
import os
import requests
from urllib.parse import urlparse, parse_qs
import django

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.api.airbyte_api import (
    get_airbyte_source_definitions,
    get_airbyte_source_definition_specifications,
    post_airbyte_source,
    put_airbyte_source,
    post_airbyte_check_source,
    post_airbyte_check_source_for_update,
    get_airbyte_sources,
    get_airbyte_source,
    get_airbyte_source_schema_catalog,
    get_airbyte_destination_definitions,
    get_airbyte_destination_definition_specifications,
    post_airbyte_destination,
    post_airbyte_check_destination,
    post_airbyte_check_destination_for_update,
    get_airbyte_destinations,
    get_airbyte_destination,
    get_job_status,
    post_cancel_connection_job,
    post_source_oauth_consent,
    get_source_oauth_callback,
    post_source_oauth_create,
    put_source_oauth_update,
)
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.core.oauth.google_oauth_provider import GSHEETS_SOURCE_NAME as GSHEETS_NAME
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import (
    AirbyteSourceCreate,
    AirbyteSourceUpdate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationCreate,
    AirbyteDestinationUpdateCheckConnection,
    SourceGoogleOAuthConsentCreate,
    SourceGoogleOAuthCreate,
    SourceGoogleOAuthUpdate,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui import ddpprefect
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui-pytest")

pytestmark = pytest.mark.django_db

# the oauth registry is keyed by source-definition NAME, so the id here is deliberately an
# arbitrary value — it only has to resolve to GSHEETS_NAME in this workspace's catalog
GSHEETS_DEF_ID = "workspace-specific-gsheets-def-id"


# ================================================================================
@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug")
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def orguser_workspace(authuser, org_with_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def org_with_workspace_b():
    """a pytest fixture which creates a second Org (org B) having its own airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id="FAKE-WORKSPACE-ID-B", slug="test-org-b-slug")
    yield org
    org.delete()


@pytest.fixture
def orguser_workspace_b(org_with_workspace_b):
    """a pytest fixture representing an OrgUser in a different org (org B), having the
    account-manager role — used to prove cross-org oauth state nonces are rejected"""
    user_b = User.objects.create(
        username="tempusername-b", email="tempuseremail-b", password="tempuserpassword"
    )
    orguser = OrgUser.objects.create(
        user=user_b,
        org=org_with_workspace_b,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()
    user_b.delete()


# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 4
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


# ================================================================================
def test_get_airbyte_source_definitions_without_airbyte_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_definitions(request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_definitions=Mock(
        return_value={
            "sourceDefinitions": [
                {"name": "name1"},
                {"name": "name2"},
                {"name": "name3"},
            ]
        }
    ),
)
def test_get_airbyte_source_definitions_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source_definitions(request)

    assert len(result) == 3


# ================================================================================
def test_get_airbyte_source_definition_specifications_without_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_definition_specifications(request, "fake-sourcedef-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_definition_specification=Mock(
        return_value={"connectionSpecification": "srcdefspeec_val"}
    ),
)
def test_get_airbyte_source_definition_specifications_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source_definition_specifications(request, "fake-sourcedef-id")

    assert result == "srcdefspeec_val"


# ================================================================================
def test_post_airbyte_source_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceCreate(name="temp-name", sourceDefId="fake-id", config={})
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_source(request, fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_source=Mock(return_value={"sourceId": "fake-source-id"}),
)
def test_post_airbyte_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceCreate(name="temp-name", sourceDefId="fake-id", config={})
    source = post_airbyte_source(request, fake_payload)

    assert source["sourceId"] == "fake-source-id"


# ================================================================================
# Google Sheets OAuth flow (Variant A — Dalgo performs the token exchange)
# ================================================================================
class FakePipeline:
    """Minimal MULTI/EXEC pipeline stand-in: queues get/delete, runs them on execute()"""

    def __init__(self, redis):
        self._redis = redis
        self._ops = []

    def get(self, key):
        self._ops.append(("get", key))
        return self

    def delete(self, key):
        self._ops.append(("delete", key))
        return self

    def execute(self):
        results = []
        for op, key in self._ops:
            if op == "get":
                results.append(self._redis.store.get(key))
            else:
                results.append(1 if self._redis.store.pop(key, None) is not None else 0)
        return results


class FakeRedis:
    """Minimal in-memory stand-in for RedisClient.get_instance() in tests"""

    def __init__(self):
        self.store = {}

    def set(self, key, value, ex=None):  # pylint: disable=unused-argument
        self.store[key] = value

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        self.store.pop(key, None)

    def pipeline(self, transaction=True):  # pylint: disable=unused-argument
        return FakePipeline(self)


class FakeResponse:
    """Minimal requests.Response stand-in for the Google token endpoint"""

    def __init__(self, status_code, payload=None, text=None, json_raises=False):
        self.status_code = status_code
        self._payload = payload
        self._json_raises = json_raises
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        if self._json_raises:
            raise ValueError("response body is not valid json")
        return self._payload


def _oauth_env(monkeypatch):
    """Set the OAuth env vars the Variant-A flow reads per request"""
    monkeypatch.setenv(
        "AIRBYTE_OAUTH_REDIRECT_URL",
        "https://api.dalgo.org/api/airbyte/sources/oauth/callback",
    )
    monkeypatch.setenv("AIRBYTE_GOOGLE_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("AIRBYTE_GOOGLE_OAUTH_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("FRONTEND_URL_V2", "https://app.dalgo.org")


def _use_fake_redis(monkeypatch):
    fake_redis = FakeRedis()
    monkeypatch.setattr(
        "ddpui.core.oauth.google_oauth_service.RedisClient.get_instance", lambda: fake_redis
    )
    return fake_redis


def _mock_source_definition(monkeypatch, name=GSHEETS_NAME):
    """the oauth flow resolves the sourceDefId to a source-definition NAME against the org's
    own workspace catalog — that name is the oauth registry key"""
    monkeypatch.setattr(
        "ddpui.ddpairbyte.airbyte_service.get_source_definition",
        lambda workspace_id, sourcedef_id: {
            "sourceDefinitionId": sourcedef_id,
            "name": name,
        },
    )


# ---- consent: Dalgo builds the Google URL -----------------------------------
def test_post_source_oauth_consent_without_workspace(seed_db, orguser):
    """consent endpoint requires an airbyte workspace"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_source_oauth_consent(request, SourceGoogleOAuthConsentCreate(sourceDefId="fake-id"))

    assert str(excinfo.value) == "create an airbyte workspace first"


def test_post_source_oauth_consent_builds_google_url(seed_db, orguser_workspace, monkeypatch):
    """consent builds the Google consent URL itself and mints a state nonce; Airbyte is only
    consulted to resolve the sourceDefId to the connector name"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    _mock_source_definition(monkeypatch)
    request = mock_request(orguser_workspace)

    result = post_source_oauth_consent(
        request, SourceGoogleOAuthConsentCreate(sourceDefId=GSHEETS_DEF_ID)
    )

    parsed = urlparse(result["authUrl"])
    assert parsed.netloc == "accounts.google.com"
    assert parsed.path == "/o/oauth2/v2/auth"
    q = parse_qs(parsed.query)
    assert q["client_id"] == ["cid"]
    assert q["redirect_uri"] == ["https://api.dalgo.org/api/airbyte/sources/oauth/callback"]
    assert q["response_type"] == ["code"]
    assert q["access_type"] == ["offline"]
    assert q["prompt"] == ["consent"]
    assert q["scope"] == ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    state = q["state"][0]
    assert state
    # the nonce is stored in redis bound to the caller's own org (CSRF + identity)
    stored = json.loads(fake_redis.store[f"airbyte_oauth_state:{state}"])
    assert stored["orguser_id"] == orguser_workspace.id
    assert stored["source_name"] == GSHEETS_NAME


# ---- callback: public, exchanges code server-side, stashes a ref ------------
def _seed_state(fake_redis, orguser, source_name, state="good-state"):
    """helper: pre-store a valid oauth state nonce in the fake redis"""
    fake_redis.store[f"airbyte_oauth_state:{state}"] = json.dumps(
        {
            "orguser_id": orguser.id,
            "source_name": source_name,
        }
    )
    return state


def test_oauth_callback_happy_path(seed_db, orguser_workspace, monkeypatch):
    """callback exchanges the code with Google, stashes a ref, redirects with ?ref"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    state = _seed_state(fake_redis, orguser_workspace, GSHEETS_NAME)
    monkeypatch.setattr(
        "ddpui.core.oauth.google_oauth_service.requests.post",
        lambda *a, **k: FakeResponse(200, {"refresh_token": "rt-123", "access_token": "at"}),
    )
    request = mock_request(orguser_workspace)

    response = get_source_oauth_callback(request, state=state, code="auth-code")

    # 302 to the frontend callback page carrying only an opaque ref (never the token)
    assert response.status_code == 302
    location = urlparse(response.url)
    assert f"{location.scheme}://{location.netloc}{location.path}" == (
        "https://app.dalgo.org/oauth/airbyte/callback"
    )
    assert "rt-123" not in response.url  # the refresh_token is NOT in the redirect
    refresh_token_ref = parse_qs(location.query)["refresh_token_ref"][0]
    # the ref maps (server-side) to the refresh_token + the state's orguser
    stored = json.loads(fake_redis.store[f"airbyte_oauth_refresh_token_ref:{refresh_token_ref}"])
    assert stored["refresh_token"] == "rt-123"
    assert stored["orguser_id"] == orguser_workspace.id
    assert stored["source_name"] == GSHEETS_NAME
    # state nonce is not consumed; it expires on its own short TTL
    assert f"airbyte_oauth_state:{state}" in fake_redis.store


def test_oauth_callback_bad_state_redirects_error(seed_db, orguser_workspace, monkeypatch):
    """an unknown/expired/reused state redirects with ?error and never exchanges"""
    _oauth_env(monkeypatch)
    _use_fake_redis(monkeypatch)
    called = {"post": False}

    def _post(*a, **k):
        called["post"] = True
        return FakeResponse(200, {"refresh_token": "rt"})

    monkeypatch.setattr("ddpui.core.oauth.google_oauth_service.requests.post", _post)
    request = mock_request(orguser_workspace)

    response = get_source_oauth_callback(request, state="never-issued", code="c")

    assert response.status_code == 302
    assert "error=" in response.url
    assert called["post"] is False  # no token exchange attempted on a bad state


def test_oauth_callback_user_denied_redirects_error(seed_db, orguser_workspace, monkeypatch):
    """Google denial (?error, no code) redirects with the error, no exchange"""
    _oauth_env(monkeypatch)
    _use_fake_redis(monkeypatch)
    request = mock_request(orguser_workspace)

    response = get_source_oauth_callback(request, error="access_denied")

    assert response.status_code == 302
    assert "error=access_denied" in response.url


def test_oauth_callback_no_refresh_token_redirects_error(seed_db, orguser_workspace, monkeypatch):
    """if Google returns no refresh_token, callback redirects with an error, stores no ref"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    state = _seed_state(fake_redis, orguser_workspace, GSHEETS_NAME)
    monkeypatch.setattr(
        "ddpui.core.oauth.google_oauth_service.requests.post",
        lambda *a, **k: FakeResponse(200, {"access_token": "at"}),  # no refresh_token
    )
    request = mock_request(orguser_workspace)

    response = get_source_oauth_callback(request, state=state, code="c")

    assert response.status_code == 302
    assert "error=" in response.url
    # no ref stashed
    assert not any(k.startswith("airbyte_oauth_refresh_token_ref:") for k in fake_redis.store)


def test_oauth_callback_request_timeout_redirects_error(seed_db, orguser_workspace, monkeypatch):
    """a network timeout reaching Google redirects with an error, not an uncaught 500"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    state = _seed_state(fake_redis, orguser_workspace, GSHEETS_NAME)

    def _raise_timeout(*a, **k):
        raise requests.exceptions.Timeout("google is slow")

    monkeypatch.setattr("ddpui.core.oauth.google_oauth_service.requests.post", _raise_timeout)
    request = mock_request(orguser_workspace)

    response = get_source_oauth_callback(request, state=state, code="c")

    assert response.status_code == 302
    assert "error=oauth_failed" in response.url
    # exchange failed before stashing anything — no ref stashed
    assert not any(k.startswith("airbyte_oauth_refresh_token_ref:") for k in fake_redis.store)


def test_oauth_callback_non_json_response_redirects_error(seed_db, orguser_workspace, monkeypatch):
    """a 200 with a non-JSON body redirects with an error, not an uncaught 500"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    state = _seed_state(fake_redis, orguser_workspace, GSHEETS_NAME)
    monkeypatch.setattr(
        "ddpui.core.oauth.google_oauth_service.requests.post",
        lambda *a, **k: FakeResponse(200, text="<html>gateway error</html>", json_raises=True),
    )
    request = mock_request(orguser_workspace)

    response = get_source_oauth_callback(request, state=state, code="c")

    assert response.status_code == 302
    assert "error=oauth_failed" in response.url
    assert not any(k.startswith("airbyte_oauth_refresh_token_ref:") for k in fake_redis.store)


# ---- create: redeem the ref, inject creds, save the source ------------------
def _seed_ref(fake_redis, orguser, source_name, refresh_token="rt-123", ref="good-ref"):
    """helper: pre-store a valid oauth ref (stashed refresh_token) in the fake redis"""
    fake_redis.store[f"airbyte_oauth_refresh_token_ref:{ref}"] = json.dumps(
        {
            "orguser_id": orguser.id,
            "source_name": source_name,
            "refresh_token": refresh_token,
        }
    )
    return ref


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_source=Mock(return_value={"sourceId": "new-src-id"}),
)
def test_post_source_oauth_create_success(seed_db, orguser_workspace, monkeypatch):
    """create redeems the ref, injects all-three credentials server-side, creates the source"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    _mock_source_definition(monkeypatch)
    ref = _seed_ref(fake_redis, orguser_workspace, GSHEETS_NAME)
    request = mock_request(orguser_workspace)

    result = post_source_oauth_create(
        request,
        SourceGoogleOAuthCreate(
            sourceDefId=GSHEETS_DEF_ID,
            name="my sheet",
            config={"spreadsheet_id": "https://sheet"},
            refresh_token_ref=ref,
        ),
    )

    # only the source id is returned — no credentials reach the caller
    assert result == {"sourceId": "new-src-id"}
    # backend built credentials from env + the stashed refresh_token (all three keys)
    airbyte_service.create_source.assert_called_once_with(
        orguser_workspace.org.airbyte_workspace_id,
        "my sheet",
        GSHEETS_DEF_ID,
        {
            "spreadsheet_id": "https://sheet",
            "credentials": {
                "auth_type": "Client",
                "client_id": "cid",
                "client_secret": "csecret",
                "refresh_token": "rt-123",
            },
        },
    )
    # ref is not consumed on redeem; it expires on its own short TTL
    assert f"airbyte_oauth_refresh_token_ref:{ref}" in fake_redis.store


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    update_source=Mock(return_value={"sourceId": "existing-src-id"}),
    get_source=Mock(
        return_value={"sourceId": "existing-src-id", "workspaceId": "FAKE-WORKSPACE-ID"}
    ),
)
def test_put_source_oauth_update_reauth(seed_db, orguser_workspace, monkeypatch):
    """the update endpoint re-authenticates an existing source in the caller's workspace"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    _mock_source_definition(monkeypatch)
    ref = _seed_ref(fake_redis, orguser_workspace, GSHEETS_NAME)
    request = mock_request(orguser_workspace)

    result = put_source_oauth_update(
        request,
        "existing-src-id",
        SourceGoogleOAuthUpdate(
            sourceDefId=GSHEETS_DEF_ID,
            name="my sheet",
            config={"spreadsheet_id": "https://sheet"},
            refresh_token_ref=ref,
        ),
    )

    assert result == {"sourceId": "existing-src-id"}
    airbyte_service.update_source.assert_called_once_with(
        "existing-src-id",
        "my sheet",
        {
            "spreadsheet_id": "https://sheet",
            "credentials": {
                "auth_type": "Client",
                "client_id": "cid",
                "client_secret": "csecret",
                "refresh_token": "rt-123",
            },
        },
        GSHEETS_DEF_ID,
    )


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    update_source=Mock(return_value={"sourceId": "foreign-src-id"}),
    get_source=Mock(
        return_value={"sourceId": "foreign-src-id", "workspaceId": "SOME-OTHER-WORKSPACE"}
    ),
)
def test_put_source_oauth_update_foreign_source_rejected(seed_db, orguser_workspace, monkeypatch):
    """a source_id living in another org's workspace cannot be updated (Airbyte's
    sources/update is not workspace-scoped, so the service guards ownership)"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    ref = _seed_ref(fake_redis, orguser_workspace, GSHEETS_NAME)
    request = mock_request(orguser_workspace)

    with pytest.raises(HttpError) as excinfo:
        put_source_oauth_update(
            request,
            "foreign-src-id",
            SourceGoogleOAuthUpdate(
                sourceDefId=GSHEETS_DEF_ID,
                name="my sheet",
                config={"spreadsheet_id": "https://sheet"},
                refresh_token_ref=ref,
            ),
        )

    assert str(excinfo.value) == "source not found"
    airbyte_service.update_source.assert_not_called()


def test_post_source_oauth_create_expired_ref(seed_db, orguser_workspace, monkeypatch):
    """a missing/expired ref is rejected"""
    _oauth_env(monkeypatch)
    _use_fake_redis(monkeypatch)
    _mock_source_definition(monkeypatch)
    request = mock_request(orguser_workspace)

    with pytest.raises(HttpError) as excinfo:
        post_source_oauth_create(
            request,
            SourceGoogleOAuthCreate(
                sourceDefId=GSHEETS_DEF_ID,
                name="my sheet",
                config={},
                refresh_token_ref="never-issued",
            ),
        )

    assert str(excinfo.value) == "invalid or expired oauth session"


def test_post_source_oauth_create_foreign_ref_rejected(
    seed_db, orguser_workspace, orguser_workspace_b, monkeypatch
):
    """a ref minted for org A's OrgUser cannot be redeemed by an OrgUser from a different org"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    _mock_source_definition(monkeypatch)
    ref = _seed_ref(fake_redis, orguser_workspace, GSHEETS_NAME)
    request = mock_request(orguser_workspace_b)

    with pytest.raises(HttpError) as excinfo:
        post_source_oauth_create(
            request,
            SourceGoogleOAuthCreate(
                sourceDefId=GSHEETS_DEF_ID, name="my sheet", config={}, refresh_token_ref=ref
            ),
        )

    assert str(excinfo.value) == "oauth session does not match this request"


def test_post_source_oauth_create_wrong_sourcedef_rejected(seed_db, orguser_workspace, monkeypatch):
    """a ref minted for one connector cannot create a source of another connector"""
    _oauth_env(monkeypatch)
    fake_redis = _use_fake_redis(monkeypatch)
    ref = _seed_ref(fake_redis, orguser_workspace, GSHEETS_NAME)
    # the def id passed in resolves to a different connector than the one the ref was minted for
    _mock_source_definition(monkeypatch, name="Google Analytics")
    request = mock_request(orguser_workspace)

    with pytest.raises(HttpError) as excinfo:
        post_source_oauth_create(
            request,
            SourceGoogleOAuthCreate(
                sourceDefId="a-different-id", name="my sheet", config={}, refresh_token_ref=ref
            ),
        )

    assert str(excinfo.value) == "oauth session does not match this request"


def test_post_source_oauth_consent_unsupported_source_rejected(
    seed_db, orguser_workspace, monkeypatch
):
    """a source whose definition name is not in the oauth registry is rejected by name, not id"""
    _oauth_env(monkeypatch)
    _use_fake_redis(monkeypatch)
    _mock_source_definition(monkeypatch, name="Postgres")
    request = mock_request(orguser_workspace)

    with pytest.raises(HttpError) as excinfo:
        post_source_oauth_consent(
            request, SourceGoogleOAuthConsentCreate(sourceDefId=GSHEETS_DEF_ID)
        )

    assert str(excinfo.value) == "oauth is not supported for this source"


# ================================================================================
def test_put_airbyte_source_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    with pytest.raises(HttpError) as excinfo:
        put_airbyte_source(request, "fake-source-id", fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    update_source=Mock(return_value={"sourceId": "fake-source-id"}),
)
def test_put_airbyte_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    source = put_airbyte_source(request, "fake-source-id", fake_payload)

    assert source["sourceId"] == "fake-source-id"


# ================================================================================
def test_post_airbyte_check_source_with_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_source(request, fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1]}}}
    ),
)
def test_post_airbyte_check_source_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    result = post_airbyte_check_source(request, fake_payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 1


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2]}}}
    ),
)
def test_post_airbyte_check_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    result = post_airbyte_check_source(request, fake_payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 2


# ================================================================================
def test_post_airbyte_check_source_for_update_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_source_for_update(request, "fake-source-id", fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1]}}}
    ),
)
def test_post_airbyte_check_source_for_update_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source_for_update(request, "fake-source-id", fake_payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 1


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2]}}}
    ),
)
def test_post_airbyte_check_source_for_update_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source_for_update(request, "fake-source-id", fake_payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 2


# ================================================================================
def test_get_airbyte_sources_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_sources(
            request,
        )

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_sources=Mock(return_value={"sources": [1, 2, 3]}),
)
def test_get_airbyte_sources_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_sources(
        request,
    )

    assert len(result) == 3


# ================================================================================
def test_get_airbyte_source_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source(request, "fake-source-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source=Mock(return_value={"fake-key": "fake-val"}),
)
def test_get_airbyte_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source(request, "fake-source-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_source_schema_catalog_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_schema_catalog(request, "fake-source-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_schema_catalog=Mock(return_value={"fake-key": "fake-val"}),
)
def test_get_airbyte_source_schema_catalog_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source_schema_catalog(request, "fake-source-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_destination_definitions_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination_definitions(request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination_definitions=Mock(
        return_value={"destinationDefinitions": [{"name": "dest1"}, {"name": "dest3"}]}
    ),
)
def test_get_airbyte_destination_definitions_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    os.environ["AIRBYTE_DESTINATION_TYPES"] = "dest1,dest2"
    result = get_airbyte_destination_definitions(request)

    assert len(result) == 1
    assert result[0]["name"] == "dest1"


# ================================================================================
def test_get_airbyte_destination_definition_specifications_without_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination_definition_specifications(request, "fake-dest-def-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination_definition_specification=Mock(
        return_value={"connectionSpecification": {"fake-key": "fake-val"}}
    ),
)
def test_get_airbyte_destination_definition_specifications_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    os.environ["AIRBYTE_DESTINATION_TYPES"] = "dest1,dest2"
    result = get_airbyte_destination_definition_specifications(request, "fake-dest-def-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_post_airbyte_destination_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_destination(request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_destination=Mock(return_value={"destinationId": "fake-dest-id"}),
)
def test_post_airbyte_destination_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_destination(request, payload)

    assert result["destinationId"] == "fake-dest-id"


# ================================================================================
def test_post_airbyte_check_destination_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_destination(request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2, 3]}}}
    ),
)
def test_post_airbyte_check_destination_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_check_destination(request, payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 3


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1, 2, 3, 4]}}}
    ),
)
def test_post_airbyte_check_destination_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_check_destination(request, payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 4


# ================================================================================
def test_post_airbyte_check_destination_for_update_without_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_destination_for_update(request, "fake-dest-id", payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2, 3]}}}
    ),
)
def test_post_airbyte_check_destination_for_update_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    result = post_airbyte_check_destination_for_update(request, "fake-dest-id", payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 3


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1, 2, 3, 4]}}}
    ),
)
def test_post_airbyte_check_destination_for_update_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    result = post_airbyte_check_destination_for_update(request, "fake-dest-id", payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 4


# ================================================================================
def test_get_airbyte_destinations_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destinations(request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destinations=Mock(return_value={"destinations": [{"fake-key": "fake-val"}]}),
)
def test_get_airbyte_destinations_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_destinations(request)

    assert len(result) == 1
    assert result[0]["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_destination_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination(request, "fake-dest-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination=Mock(return_value={"fake-key": "fake-val"}),
)
def test_get_airbyte_destination_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_destination(request, "fake-dest-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
@pytest.fixture
def warehouse_without_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(org=org_with_workspace)
    yield warehouse
    warehouse.delete()


@pytest.fixture
def warehouse_with_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="destination-id"
    )
    yield warehouse
    warehouse.delete()


# ================================================================================
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_job_info=Mock(
        return_value={
            "attempts": [{"logs": {"logLines": [1, 2, 3]}}],
            "job": {"status": "completed"},
        }
    ),
)
def test_get_job_status(orguser):
    request = mock_request(orguser)

    result = get_job_status(request, "fake-job-id")
    assert result["status"] == "completed"
    assert len(result["logs"]) == 3


def test_post_cancel_connection_job(orguser_workspace):
    """tests POST /cancel_connection_job"""
    request = mock_request(orguser_workspace)

    with patch(
        "ddpui.ddpairbyte.airbyte_service.cancel_connection_job"
    ) as mock_cancel_connection_job:
        post_cancel_connection_job(request, "fake-connection-id", "sync")
        mock_cancel_connection_job.assert_called_once_with(
            "FAKE-WORKSPACE-ID", "fake-connection-id", "sync"
        )
