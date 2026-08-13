"""Dalgo-managed Google service-account credentials for the Airbyte Google Sheets connector.

TEMPORARY BRIDGE — meant to be deleted.

Google OAuth verification for the sensitive `spreadsheets.readonly` scope takes 3-4 weeks. A
service account skips verification entirely (Google exempts apps accessing only their own data),
at the cost of the user sharing each spreadsheet with the service account's email.

Switched on by one env var pointing at the key file. Unset, it returns None and every call
site falls through to the existing OAuth / bring-your-own-key behaviour. A path rather than
inline JSON because python-dotenv mangles the escaped newlines in `private_key`.

To retire: unset the env var (no deploy), then delete this module and every `MANAGED-SA` call
site. `client_email` is the only part of the key that may leave the backend — the UI needs it to
tell users which address to share with.
"""

import os

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

# Path to the key file — the single switch for the whole bridge.
MANAGED_SA_PATH_ENV = "DALGO_MANAGED_GSHEETS_SERVICE_ACCOUNT_JSON_PATH"

# The connector spec's `credentials` oneOf, Service branch.
CREDENTIALS_KEY = "credentials"
AUTH_TYPE_KEY = "auth_type"
SERVICE_AUTH_TYPE = "Service"
SERVICE_INFO_KEY = "service_account_info"


def managed_service_account_json() -> str | None:
    """The service-account key JSON. None when the env var is unset or the file is unreadable or
    empty — a broken key must degrade to "bridge off", never to a half-built credentials block.
    Read per call, so swapping the file needs no restart."""
    path = os.getenv(MANAGED_SA_PATH_ENV)
    if not (path and path.strip()):
        return None

    try:
        with open(path.strip(), "r", encoding="utf-8") as keyfile:
            contents = keyfile.read().strip()
    except OSError as err:
        logger.error("managed google service-account key is unreadable at %s: %s", path, err)
        return None

    if not contents:
        logger.error("managed google service-account key at %s is empty", path)
        return None

    return contents
