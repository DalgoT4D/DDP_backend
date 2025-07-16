"""Centralized service for Superset API communication with token management and caching."""

import time
import json
import requests
from typing import Optional, Dict, List, Any
from ninja.errors import HttpError
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.models.org import Org

logger = CustomLogger("superset_service")


class SupersetService:
    """Service class for managing Superset API interactions with token caching and retry logic."""

    def __init__(self, org: Org):
        """Initialize SupersetService with organization and Redis client."""
        self.org = org
        self.redis = RedisClient.get_instance()
        self.max_retries = 3
        self.retry_delay = 1  # seconds

        # Different TTLs for different token types
        self.access_token_ttl = 3600  # 1 hour
        self.csrf_token_ttl = 1800  # 30 minutes
        self.guest_token_ttl = 240  # 4 minutes (refresh before 5 min expiry)

    def _make_request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with automatic retry on 401.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            **kwargs: Additional arguments to pass to requests

        Returns:
            Response object

        Raises:
            HttpError: On authentication failure or connection issues
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = requests.request(method, url, **kwargs)

                if response.status_code == 401:
                    # Clear cached tokens and retry
                    self._clear_cached_tokens()
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (attempt + 1))
                        # Get fresh token for next attempt
                        kwargs["headers"][
                            "Authorization"
                        ] = f"Bearer {self.get_access_token(force_refresh=True)}"
                        continue
                    else:
                        # Final attempt failed, raise HttpError
                        raise HttpError(401, "Authentication expired with Superset")

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue

        # All retries failed
        logger.error(
            f"Failed to connect to Superset after {self.max_retries} attempts: {str(last_error)}"
        )
        raise HttpError(503, "Failed to connect to Superset")

    def _clear_cached_tokens(self):
        """Clear all cached tokens for the organization."""
        pattern = f"superset:token:{self.org.id}:*"
        for key in self.redis.scan_iter(match=pattern):
            self.redis.delete(key)

    def get_access_token(self, force_refresh: bool = False) -> str:
        """Get access token with caching.

        Args:
            force_refresh: Force refresh token even if cached

        Returns:
            Access token string

        Raises:
            HttpError: On authentication failure
        """
        cache_key = f"superset:token:{self.org.id}:access"

        if not force_refresh:
            cached_token = self.redis.get(cache_key)
            if cached_token:
                return cached_token.decode("utf-8")

        # Get credentials from secrets manager
        if not self.org.dalgouser_superset_creds_key:
            raise HttpError(400, "Superset admin credentials not configured")

        credentials = secretsmanager.retrieve_dalgo_user_superset_credentials(
            self.org.dalgouser_superset_creds_key
        )
        if not credentials:
            raise HttpError(400, "Superset admin credentials not found")

        # Login to get access token
        try:
            response = requests.post(
                f"{self.org.viz_url}api/v1/security/login",
                json={
                    "password": credentials["password"],
                    "username": credentials["username"],
                    "refresh": True,
                    "provider": "db",
                },
                timeout=10,
            )
            response.raise_for_status()
            access_token = response.json()["access_token"]

            # Cache the token
            self.redis.set(cache_key, access_token, self.access_token_ttl)
            return access_token

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to authenticate with Superset: {str(e)}")
            raise HttpError(503, "Failed to authenticate with Superset")

    def get_csrf_token(self, access_token: str) -> tuple[str, str]:
        """Get CSRF token from Superset.

        Args:
            access_token: Valid access token

        Returns:
            Tuple of (csrf_token, session_cookie)

        Raises:
            HttpError: On connection failure
        """
        cache_key = f"superset:token:{self.org.id}:csrf"

        # Check cache first
        cached_data = self.redis.get(cache_key)
        if cached_data:
            data = json.loads(cached_data.decode("utf-8"))
            return data["csrf_token"], data["session_cookie"]

        try:
            response = requests.get(
                f"{self.org.viz_url}api/v1/security/csrf_token",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )

            session_cookie = ""
            if "Set-Cookie" in response.headers:
                # Extract session cookie value
                cookie_header = response.headers["Set-Cookie"]
                if "session=" in cookie_header:
                    session_cookie = cookie_header.split("session=")[1].split(";")[0]

            response.raise_for_status()
            csrf_token = response.json()["result"]

            # Cache both tokens
            cache_data = {"csrf_token": csrf_token, "session_cookie": session_cookie}
            self.redis.set(cache_key, json.dumps(cache_data), self.csrf_token_ttl)

            return csrf_token, session_cookie

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get CSRF token: {str(e)}")
            raise HttpError(503, "Failed to get CSRF token from Superset")

    def get_guest_token(self, dashboard_uuid: str, force_refresh: bool = False) -> Dict[str, Any]:
        """Generate guest token for dashboard embedding.

        Args:
            dashboard_uuid: UUID of the dashboard
            force_refresh: Force refresh token even if cached

        Returns:
            Dictionary with guest token data

        Raises:
            HttpError: On token generation failure
        """
        cache_key = f"superset:token:{self.org.id}:guest:{dashboard_uuid}"

        if not force_refresh:
            cached_token = self.redis.get(cache_key)
            if cached_token:
                return json.loads(cached_token.decode("utf-8"))

        # Generate new token
        access_token = self.get_access_token()
        csrf_token, session_cookie = self.get_csrf_token(access_token)

        # Get credentials for user info
        credentials = secretsmanager.retrieve_dalgo_user_superset_credentials(
            self.org.dalgouser_superset_creds_key
        )

        try:
            response = requests.post(
                f"{self.org.viz_url}api/v1/security/guest_token",
                json={
                    "user": {
                        "username": credentials["username"],
                    },
                    "resources": [{"type": "dashboard", "id": str(dashboard_uuid)}],
                    "rls": [],  # Row level security rules if needed
                },
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "X-CSRFToken": csrf_token,
                    "Content-Type": "application/json",
                    "Referer": f"{self.org.viz_url.rstrip('/')}",
                },
                cookies={"session": session_cookie} if session_cookie else {},
                timeout=10,
            )
            response.raise_for_status()

            guest_token_data = {"token": response.json()["token"], "generated_at": time.time()}

            # Cache with shorter TTL for security
            self.redis.set(cache_key, json.dumps(guest_token_data), self.guest_token_ttl)

            return guest_token_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to generate guest token: {str(e)}")
            raise HttpError(503, "Failed to generate guest token")

    def get_dashboards(
        self,
        page: int = 0,
        page_size: int = 20,
        search: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get dashboards with automatic token refresh on 401.

        Args:
            page: Page number (0-indexed)
            page_size: Number of items per page
            search: Search string for dashboard title
            status: Filter by status (published/draft)

        Returns:
            Dictionary with dashboard list and metadata

        Raises:
            HttpError: On API failure
        """
        access_token = self.get_access_token()

        # Build query parameters
        filters = []
        if search:
            filters.append({"col": "dashboard_title", "opr": "ct", "value": search})  # contains
        if status:
            filters.append({"col": "published", "opr": "eq", "value": status == "published"})

        query_params = {
            "page": page,
            "page_size": page_size,
        }
        if filters:
            query_params["filters"] = filters

        url = f"{self.org.viz_url}api/v1/dashboard/"

        response = self._make_request_with_retry(
            "GET",
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            params={"q": json.dumps(query_params)},
            timeout=30,
        )

        return response.json()

    def get_or_create_embedded_uuid(self, dashboard_id: str) -> Optional[str]:
        """Get or create embedded UUID for a dashboard.

        Args:
            dashboard_id: Dashboard ID

        Returns:
            Embedded UUID string, or None if failed

        Raises:
            HttpError: On API failure
        """
        access_token = self.get_access_token()
        url = f"{self.org.viz_url}api/v1/dashboard/{dashboard_id}/embedded"

        try:
            # First try GET to see if embedded info exists
            response = self._make_request_with_retry(
                "GET",
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                timeout=30,
            )

            embedded_info = response.json().get("result", {})
            embedded_uuid = embedded_info.get("uuid")

            if embedded_uuid:
                return embedded_uuid

        except HttpError as e:
            # If 404, embedded info doesn't exist, so we need to create it
            if e.status_code != 404:
                raise

        # Create embedded info if it doesn't exist
        try:
            csrf_token, session_cookie = self.get_csrf_token(access_token)

            response = self._make_request_with_retry(
                "POST",
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                    "X-CSRFToken": csrf_token,
                    "Referer": f"{self.org.viz_url.rstrip('/')}",
                },
                json={"allowed_domains": []},
                cookies={"session": session_cookie} if session_cookie else {},
                timeout=30,
            )

            embedded_info = response.json().get("result", {})
            embedded_uuid = embedded_info.get("uuid")

            return embedded_uuid

        except Exception as e:
            logger.error(f"Failed to create embedded UUID for dashboard {dashboard_id}: {str(e)}")
            return None

    def get_dashboard_by_id(self, dashboard_id: str) -> Dict[str, Any]:
        """Get single dashboard details.

        Args:
            dashboard_id: Dashboard ID

        Returns:
            Dashboard details dictionary

        Raises:
            HttpError: On API failure
        """
        access_token = self.get_access_token()

        url = f"{self.org.viz_url}api/v1/dashboard/{dashboard_id}"

        response = self._make_request_with_retry(
            "GET",
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

        return response.json().get("result", {})

    def get_dashboard_thumbnail(self, dashboard_id: str) -> Optional[bytes]:
        """Get dashboard thumbnail.

        Args:
            dashboard_id: Dashboard ID

        Returns:
            Thumbnail image bytes or None if not found

        Raises:
            HttpError: On API failure (except 404)
        """
        access_token = self.get_access_token()

        # First, check if dashboard has a cached thumbnail URL
        cache_key = f"superset:thumbnail:{self.org.id}:{dashboard_id}"
        cached_thumbnail = self.redis.get(cache_key)
        if cached_thumbnail:
            return cached_thumbnail

        url = f"{self.org.viz_url}api/v1/dashboard/{dashboard_id}/thumbnail/"

        try:
            response = self._make_request_with_retry(
                "GET",
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                },
                timeout=30,
            )

            # Cache the thumbnail for 1 hour
            if response.content:
                self.redis.set(cache_key, response.content, 3600)

            return response.content
        except HttpError as e:
            if e.status_code == 404:
                return None  # Dashboard doesn't have thumbnail
            raise
