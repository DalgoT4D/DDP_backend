import os
import json
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from datetime import datetime, timedelta
import pytest

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.datainsights.generate_result import GenerateResult

pytestmark = pytest.mark.django_db


def _make_mock_org(slug="test-org"):
    org = Mock()
    org.slug = slug
    return org


def _make_mock_query(query_id="q1", db_schema="public", db_table="users"):
    query = Mock()
    query.query_id.return_value = query_id
    query.db_schema = db_schema
    query.db_table = db_table
    return query


# =========================================================
# Tests for GenerateResult.merge_results
# =========================================================
class TestMergeResults:
    def test_merge_both_empty(self):
        result = GenerateResult.merge_results({}, {})
        assert result == {}

    def test_merge_current_none(self):
        result = GenerateResult.merge_results(None, {"col1": {"mean": 5}})
        assert result == {"col1": {"mean": 5}}

    def test_merge_parsed_none(self):
        result = GenerateResult.merge_results({"col1": {"mean": 5}}, None)
        assert result == {"col1": {"mean": 5}}

    def test_merge_overlapping_keys(self):
        current = {"col1": {"mean": 5, "min": 0}}
        parsed = {"col1": {"max": 10}}
        result = GenerateResult.merge_results(current, parsed)
        assert result == {"col1": {"mean": 5, "min": 0, "max": 10}}

    def test_merge_disjoint_keys(self):
        current = {"col1": {"mean": 5}}
        parsed = {"col2": {"mean": 10}}
        result = GenerateResult.merge_results(current, parsed)
        assert "col1" in result
        assert "col2" in result

    def test_merge_parsed_overwrites_current(self):
        current = {"col1": {"mean": 5}}
        parsed = {"col1": {"mean": 99}}
        result = GenerateResult.merge_results(current, parsed)
        assert result["col1"]["mean"] == 99


# =========================================================
# Tests for GenerateResult.validate_results
# =========================================================
class TestValidateResults:
    def test_validate_all_pass(self):
        q1 = Mock()
        q1.validate_query_results.return_value = True
        q2 = Mock()
        q2.validate_query_results.return_value = True
        assert GenerateResult.validate_results([q1, q2], {"data": True}) is True

    def test_validate_one_fails(self):
        q1 = Mock()
        q1.validate_query_results.return_value = True
        q2 = Mock()
        q2.validate_query_results.return_value = False
        assert GenerateResult.validate_results([q1, q2], {"data": True}) is False

    def test_validate_empty_queries(self):
        assert GenerateResult.validate_results([], {}) is True


# =========================================================
# Tests for GenerateResult.build_queries_locking_hash
# =========================================================
class TestBuildQueriesLockingHash:
    def test_hash_format(self):
        org = _make_mock_org("myorg")
        query = _make_mock_query(db_schema="analytics", db_table="events")
        result = GenerateResult.build_queries_locking_hash(org, query)
        assert result == "myorg-analytics-events-queries"


# =========================================================
# Tests for GenerateResult.build_insights_hash_to_store_results
# =========================================================
class TestBuildInsightsHash:
    def test_hash_format(self):
        org = _make_mock_org("myorg")
        result = GenerateResult.build_insights_hash_to_store_results(org, "public", "users")
        assert result == "myorg-public-users-insights"


# =========================================================
# Tests for GenerateResult.is_query_locked
# =========================================================
class TestIsQueryLocked:
    def test_none_payload(self):
        assert GenerateResult.is_query_locked(None) is False

    def test_fetching_status_is_locked(self):
        payload = json.dumps({"status": "fetching"})
        assert GenerateResult.is_query_locked(payload) is True

    def test_completed_status_not_locked(self):
        payload = json.dumps({"status": "completed"})
        assert GenerateResult.is_query_locked(payload) is False

    def test_completed_with_check_expiry_not_expired(self):
        payload = json.dumps(
            {
                "status": "completed",
                "expire_at": datetime.now().isoformat(),
            }
        )
        assert GenerateResult.is_query_locked(payload, check_expiry=True) is True

    def test_completed_with_check_expiry_expired(self):
        old_time = datetime.now() - timedelta(seconds=GenerateResult.QUERY_LOCK_TIME + 10)
        payload = json.dumps(
            {
                "status": "completed",
                "expire_at": old_time.isoformat(),
            }
        )
        assert GenerateResult.is_query_locked(payload, check_expiry=True) is False

    def test_invalid_json_returns_false(self):
        assert GenerateResult.is_query_locked("not-json") is False

    def test_error_status_is_locked(self):
        payload = json.dumps({"status": "error"})
        assert GenerateResult.is_query_locked(payload) is True


# =========================================================
# Tests for GenerateResult.fetch_results
# =========================================================
class TestFetchResults:
    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_fetch_all_columns(self, mock_redis):
        org = _make_mock_org()
        redis_instance = Mock()
        redis_instance.hget.return_value = json.dumps(
            {
                "col1": {"mean": 5},
                "col2": {"mean": 10},
            }
        )
        mock_redis.return_value = redis_instance

        result = GenerateResult.fetch_results(org, "public", "users")
        assert "col1" in result
        assert "col2" in result

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_fetch_specific_column(self, mock_redis):
        org = _make_mock_org()
        redis_instance = Mock()
        redis_instance.hget.return_value = json.dumps(
            {
                "col1": {"mean": 5},
                "col2": {"mean": 10},
            }
        )
        mock_redis.return_value = redis_instance

        result = GenerateResult.fetch_results(org, "public", "users", column_name="col1")
        assert result == {"mean": 5}

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_fetch_missing_column(self, mock_redis):
        org = _make_mock_org()
        redis_instance = Mock()
        redis_instance.hget.return_value = json.dumps({"col1": {"mean": 5}})
        mock_redis.return_value = redis_instance

        result = GenerateResult.fetch_results(org, "public", "users", column_name="nonexistent")
        assert result is None

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_fetch_no_results_in_redis(self, mock_redis):
        org = _make_mock_org()
        redis_instance = Mock()
        redis_instance.hget.return_value = None
        mock_redis.return_value = redis_instance

        result = GenerateResult.fetch_results(org, "public", "users")
        assert result is None


# =========================================================
# Tests for GenerateResult.save_results
# =========================================================
class TestSaveResults:
    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_save_results(self, mock_redis):
        org = _make_mock_org()
        query = _make_mock_query(db_schema="public", db_table="users")

        redis_instance = Mock()
        mock_redis.return_value = redis_instance

        mock_lock = Mock()
        mock_lock.acquire.return_value = True
        mock_lock.release.return_value = None

        redis_instance.hget.return_value = None
        redis_instance.ttl.return_value = -1

        with patch.object(GenerateResult, "get_org_lock", return_value=mock_lock):
            GenerateResult.save_results(org, query, {"col1": {"mean": 5}})

        redis_instance.hset.assert_called_once()
        redis_instance.expire.assert_called_once()
        mock_lock.release.assert_called_once()

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_save_results_merge_with_existing(self, mock_redis):
        org = _make_mock_org()
        query = _make_mock_query(db_schema="public", db_table="users")

        redis_instance = Mock()
        mock_redis.return_value = redis_instance

        mock_lock = Mock()
        mock_lock.acquire.return_value = True
        mock_lock.release.return_value = None

        redis_instance.hget.return_value = json.dumps({"col1": {"min": 0}})
        redis_instance.ttl.return_value = 100  # not expired

        with patch.object(GenerateResult, "get_org_lock", return_value=mock_lock):
            GenerateResult.save_results(org, query, {"col1": {"max": 10}})

        # hset should have been called with merged data
        call_args = redis_instance.hset.call_args
        saved_data = json.loads(call_args[0][2])
        assert saved_data["col1"]["min"] == 0
        assert saved_data["col1"]["max"] == 10
        # expire should NOT be called since ttl > 0
        redis_instance.expire.assert_not_called()


# =========================================================
# Tests for GenerateResult.release_query_lock
# =========================================================
class TestReleaseQueryLock:
    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_force_expire(self, mock_redis):
        org = _make_mock_org()
        query = _make_mock_query()
        redis_instance = Mock()
        mock_redis.return_value = redis_instance

        GenerateResult.release_query_lock(org, query, force_expire=True)
        redis_instance.hdel.assert_called_once()

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_release_lock_updates_status(self, mock_redis):
        org = _make_mock_org()
        query = _make_mock_query()
        redis_instance = Mock()
        mock_redis.return_value = redis_instance

        mock_lock = Mock()
        mock_lock.acquire.return_value = True
        redis_instance.lock.return_value = mock_lock

        # Current query status is "fetching"
        fetching_payload = json.dumps(
            {
                "status": "fetching",
                "expire_at": datetime.now().isoformat(),
            }
        )
        # After update it becomes "completed" - and since it's fresh, check_expiry should say locked
        completed_payload = json.dumps(
            {
                "status": "completed",
                "expire_at": datetime.now().isoformat(),
            }
        )
        redis_instance.hget.side_effect = [fetching_payload, completed_payload]

        GenerateResult.release_query_lock(org, query)

        mock_lock.release.assert_called_once()


# =========================================================
# Tests for GenerateResult.acquire_query_lock
# =========================================================
class TestAcquireQueryLock:
    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_acquire_when_not_locked(self, mock_redis):
        org = _make_mock_org()
        query = _make_mock_query()
        redis_instance = Mock()
        mock_redis.return_value = redis_instance
        redis_instance.hget.return_value = None  # no existing lock

        result = GenerateResult.acquire_query_lock(org, query)
        assert result is True
        redis_instance.hset.assert_called_once()

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_force_acquire(self, mock_redis):
        org = _make_mock_org()
        query = _make_mock_query()
        redis_instance = Mock()
        mock_redis.return_value = redis_instance
        redis_instance.hget.side_effect = [None, None]

        with patch.object(GenerateResult, "release_query_lock"):
            result = GenerateResult.acquire_query_lock(org, query, force_acquire=True)
        assert result is True


# =========================================================
# Tests for GenerateResult.get_org_lock
# =========================================================
class TestGetOrgLock:
    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_get_org_lock_creates_lock(self, mock_redis):
        redis_instance = Mock()
        mock_redis.return_value = redis_instance

        org = _make_mock_org("locktest-org")
        # Clear any cached lock
        GenerateResult.org_locks.pop("locktest-org", None)

        lock = GenerateResult.get_org_lock(org)
        assert lock is not None

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_get_org_lock_reuses(self, mock_redis):
        redis_instance = Mock()
        mock_redis.return_value = redis_instance

        org = _make_mock_org("reuse-org")
        GenerateResult.org_locks.pop("reuse-org", None)

        lock1 = GenerateResult.get_org_lock(org)
        lock2 = GenerateResult.get_org_lock(org)
        assert lock1 is lock2


# =========================================================
# Tests for GenerateResult.queries_to_execute
# =========================================================
class TestQueriesToExecute:
    @patch("ddpui.core.datainsights.generate_result.InsightsFactory.initiate_insight")
    @patch("ddpui.core.datainsights.generate_result.BaseInsights")
    def test_queries_no_filter(self, mock_base_insights, mock_factory):
        """Without filter, both base and column-specific insights are included."""
        org = _make_mock_org()

        mock_wclient = Mock()
        mock_wclient.get_table_columns.return_value = [
            {"name": "col1", "translated_type": "string"}
        ]
        mock_wclient.get_wtype.return_value = "postgres"

        base_insight = Mock()
        mock_base_instance = Mock()
        mock_base_instance.insights = [base_insight]
        mock_base_insights.return_value = mock_base_instance

        col_insight = Mock()
        mock_factory_instance = Mock()
        mock_factory_instance.insights = [col_insight]
        mock_factory.return_value = mock_factory_instance

        requestor = Mock()
        requestor.db_schema = "public"
        requestor.db_table = "users"
        requestor.column_name = "col1"
        requestor.filter = None

        result = GenerateResult.queries_to_execute(org, mock_wclient, requestor)
        assert len(result) == 2
        assert base_insight in result
        assert col_insight in result

    @patch("ddpui.core.datainsights.generate_result.InsightsFactory.initiate_insight")
    def test_queries_with_filter(self, mock_factory):
        """With filter, only column-specific insights are included (no base)."""
        org = _make_mock_org()

        mock_wclient = Mock()
        mock_wclient.get_table_columns.return_value = [
            {"name": "col1", "translated_type": "string"}
        ]
        mock_wclient.get_wtype.return_value = "postgres"

        col_insight = Mock()
        mock_factory_instance = Mock()
        mock_factory_instance.insights = [col_insight]
        mock_factory.return_value = mock_factory_instance

        requestor = Mock()
        requestor.db_schema = "public"
        requestor.db_table = "users"
        requestor.column_name = "col1"
        requestor.filter = {"type": "eq", "value": "test"}

        result = GenerateResult.queries_to_execute(org, mock_wclient, requestor)
        assert len(result) == 1
        assert col_insight in result

    def test_queries_column_not_found(self):
        """Column not found raises ValueError."""
        org = _make_mock_org()
        mock_wclient = Mock()
        mock_wclient.get_table_columns.return_value = [
            {"name": "other_col", "translated_type": "string"}
        ]
        mock_wclient.get_wtype.return_value = "postgres"

        requestor = Mock()
        requestor.db_schema = "public"
        requestor.db_table = "users"
        requestor.column_name = "nonexistent"
        requestor.filter = {"type": "eq", "value": "test"}

        with pytest.raises(ValueError, match="Couldnt find the column"):
            GenerateResult.queries_to_execute(org, mock_wclient, requestor)


# =========================================================
# Tests for GenerateResult.execute_insight_queries
# =========================================================
class TestExecuteInsightQueries:
    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_execute_with_filter(self, mock_redis):
        """When filter is present, force_acquire is True and results are not saved."""
        org = _make_mock_org()
        mock_wclient = Mock()

        query = Mock()
        query.query_id.return_value = "q1"
        query.db_schema = "public"
        query.db_table = "users"

        mock_stmt = Mock()
        query.generate_sql.return_value = mock_stmt
        mock_stmt.compile.return_value = mock_stmt
        mock_wclient.execute.return_value = [{"col1": 1}]
        query.parse_results.return_value = {"col1": {"mean": 5}}

        requestor = Mock()
        requestor.filter = {"type": "eq", "value": "test"}

        redis_instance = Mock()
        mock_redis.return_value = redis_instance
        redis_instance.hget.return_value = None

        with patch.object(GenerateResult, "acquire_query_lock", return_value=True):
            with patch.object(GenerateResult, "release_query_lock"):
                with patch.object(GenerateResult, "save_results") as mock_save:
                    result = GenerateResult.execute_insight_queries(
                        org, mock_wclient, [query], requestor
                    )

        assert len(result) == 1
        assert result[0] == {"col1": {"mean": 5}}
        mock_save.assert_not_called()

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_execute_without_filter(self, mock_redis):
        """Without filter, results are saved."""
        org = _make_mock_org()
        mock_wclient = Mock()

        query = Mock()
        query.query_id.return_value = "q1"
        query.db_schema = "public"
        query.db_table = "users"

        mock_stmt = Mock()
        query.generate_sql.return_value = mock_stmt
        mock_stmt.compile.return_value = mock_stmt
        mock_wclient.execute.return_value = [{"col1": 1}]
        query.parse_results.return_value = {"col1": {"mean": 5}}

        requestor = Mock()
        requestor.filter = None

        redis_instance = Mock()
        mock_redis.return_value = redis_instance

        with patch.object(GenerateResult, "acquire_query_lock", return_value=True):
            with patch.object(GenerateResult, "release_query_lock"):
                with patch.object(GenerateResult, "save_results") as mock_save:
                    result = GenerateResult.execute_insight_queries(
                        org, mock_wclient, [query], requestor
                    )

        assert len(result) == 1
        mock_save.assert_called_once()

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_execute_lock_not_acquired(self, mock_redis):
        """If lock not acquired, query is skipped."""
        org = _make_mock_org()
        mock_wclient = Mock()
        query = Mock()
        requestor = Mock()
        requestor.filter = None

        with patch.object(GenerateResult, "acquire_query_lock", return_value=False):
            result = GenerateResult.execute_insight_queries(org, mock_wclient, [query], requestor)

        assert result == []

    @patch("ddpui.core.datainsights.generate_result.RedisClient.get_instance")
    def test_execute_query_exception(self, mock_redis):
        """Exception during query execution releases lock with force_expire."""
        org = _make_mock_org()
        mock_wclient = Mock()

        query = Mock()
        query.query_id.return_value = "q1"
        query.generate_sql.side_effect = Exception("SQL error")

        requestor = Mock()
        requestor.filter = None

        with patch.object(GenerateResult, "acquire_query_lock", return_value=True):
            with patch.object(GenerateResult, "release_query_lock") as mock_release:
                result = GenerateResult.execute_insight_queries(
                    org, mock_wclient, [query], requestor
                )

        mock_release.assert_called_once_with(org, query, force_expire=True)
        assert result == []
