"""Tests for the Chat with Data SQL guard.

The guard is pure library code (sqlglot AST validation) — no Django, no warehouse.
Every test that rejects SQL is a red-team case from the old dashboard-chat review's
bypass catalog; see features/chat-with-data/v1/plan.md §4.3 in dalgo-core.
"""

import pytest

from ddpui.core.ai.guards.sql_guard import validate, GuardError


def test_plain_select_passes_and_gets_limit():
    result = validate(
        "SELECT district, COUNT(*) AS n FROM prod.surveys GROUP BY district",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
    )
    assert "LIMIT 100" in result.sql
    assert result.tables == ["prod.surveys"]


def test_multi_statement_is_rejected():
    with pytest.raises(GuardError, match="one SQL statement"):
        validate(
            "SELECT 1; DROP TABLE prod.surveys",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )


def test_comment_split_multi_statement_is_rejected():
    # bypass catalog: comments used to hide the statement separator from regex guards
    with pytest.raises(GuardError):
        validate(
            "SELECT/**/1;DROP/**/TABLE prod.surveys",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )


def test_dml_hidden_in_cte_is_rejected():
    # bypass catalog: root is a SELECT, but a CTE performs a DELETE
    with pytest.raises(GuardError, match="read-only|SELECT"):
        validate(
            "WITH d AS (DELETE FROM prod.surveys RETURNING *) SELECT * FROM d",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )


def test_legitimate_cte_passes_without_schema_check_on_alias():
    # 'monthly' is a CTE alias, not a physical table — must not trip the allowlist
    result = validate(
        "WITH monthly AS (SELECT district, COUNT(*) AS n FROM prod.surveys GROUP BY district) "
        "SELECT * FROM monthly ORDER BY n DESC",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
    )
    assert result.tables == ["prod.surveys"]


def test_cte_named_after_keyword_passes():
    # bypass-catalog false positive: regex guards rejected CTEs named 'delete'
    result = validate(
        'WITH delete AS (SELECT id FROM prod.surveys) SELECT COUNT(*) FROM "delete"',
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
    )
    assert result.tables == ["prod.surveys"]


def test_smaller_existing_limit_is_preserved():
    result = validate(
        "SELECT * FROM prod.surveys LIMIT 5",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
    )
    assert "LIMIT 5" in result.sql


def test_oversized_limit_is_clamped():
    result = validate(
        "SELECT * FROM prod.surveys LIMIT 99999",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
    )
    assert "LIMIT 100" in result.sql
    assert "99999" not in result.sql


def test_disallowed_schema_is_rejected():
    with pytest.raises(GuardError, match="not allowed"):
        validate(
            "SELECT * FROM information_schema.tables",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )


def test_unqualified_physical_table_is_rejected():
    with pytest.raises(GuardError, match="schema-qualified"):
        validate(
            "SELECT * FROM surveys",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )


@pytest.mark.parametrize(
    "sql",
    [
        "COPY prod.surveys TO '/tmp/out.csv'",
        "SET search_path TO prod",
        "SHOW server_version",
        "EXPLAIN SELECT * FROM prod.surveys",
        "VACUUM prod.surveys",
    ],
)
def test_verb_first_commands_are_rejected(sql):
    with pytest.raises(GuardError):
        validate(sql, dialect="postgres", allowed_schemas=["prod"], max_rows=100)


def test_select_for_update_is_rejected():
    with pytest.raises(GuardError, match="read-only"):
        validate(
            "SELECT * FROM prod.surveys FOR UPDATE",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )


def test_union_of_selects_passes():
    result = validate(
        "SELECT district FROM prod.surveys UNION ALL SELECT district FROM prod.households",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
    )
    assert result.tables == ["prod.households", "prod.surveys"]
    assert "LIMIT 100" in result.sql


def test_bigquery_backticked_table_passes():
    result = validate(
        "SELECT district, COUNT(*) AS n FROM `prod.surveys` GROUP BY district",
        dialect="bigquery",
        allowed_schemas=["prod"],
        max_rows=100,
    )
    assert result.tables == ["prod.surveys"]
    assert "LIMIT 100" in result.sql


def test_bigquery_dml_in_cte_is_rejected():
    with pytest.raises(GuardError):
        validate(
            "WITH d AS (SELECT 1) DELETE FROM prod.surveys WHERE TRUE",
            dialect="bigquery",
            allowed_schemas=["prod"],
            max_rows=100,
        )


def test_unparseable_sql_fails_closed():
    # unicode homoglyph in the keyword (Cyrillic Е) — parser must reject, guard fails closed
    with pytest.raises(GuardError):
        validate(
            "SЕLECT * FROM prod.surveys",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )


def test_allowed_table_passes_table_allowlist():
    # dashboard-scoped sessions restrict to the tables behind the dashboard's charts
    result = validate(
        "SELECT district, COUNT(*) AS n FROM prod.surveys GROUP BY district",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
        allowed_tables=["prod.surveys", "prod.field_visits"],
    )
    assert result.tables == ["prod.surveys"]


def test_out_of_scope_table_is_blocked_with_scope_message():
    # Priya's dashboard uses surveys + field_visits; donations is off-dashboard
    with pytest.raises(GuardError, match="prod.donations.*scoped to one dashboard") as err:
        validate(
            "SELECT SUM(amount) FROM prod.donations",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
            allowed_tables=["prod.surveys", "prod.field_visits"],
        )
    assert "prod.surveys" in str(err.value)  # names what IS available
    assert "full Chat with Data" in str(err.value)


@pytest.mark.parametrize(
    "sql",
    [
        # JOIN
        "SELECT s.district FROM prod.surveys s JOIN prod.donations d ON d.id = s.id",
        # subquery
        "SELECT * FROM prod.surveys WHERE id IN (SELECT id FROM prod.donations)",
        # UNION branch
        "SELECT district FROM prod.surveys UNION ALL SELECT district FROM prod.donations",
        # inside a CTE body
        "WITH d AS (SELECT id FROM prod.donations) SELECT * FROM d",
    ],
)
def test_out_of_scope_table_blocked_anywhere_in_tree(sql):
    with pytest.raises(GuardError, match="prod.donations"):
        validate(
            sql,
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
            allowed_tables=["prod.surveys"],
        )


def test_cte_alias_is_not_table_checked():
    # 'monthly' is a CTE alias — must not be rejected as an off-scope table
    result = validate(
        "WITH monthly AS (SELECT district FROM prod.surveys) SELECT * FROM monthly",
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
        allowed_tables=["prod.surveys"],
    )
    assert result.tables == ["prod.surveys"]


def test_empty_table_allowlist_blocks_everything():
    # [] means "nothing allowed" (fail-closed), never "no restriction"
    with pytest.raises(GuardError, match="not available in this chat"):
        validate(
            "SELECT * FROM prod.surveys",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
            allowed_tables=[],
        )


def test_table_allowlist_is_case_insensitive():
    result = validate(
        'SELECT * FROM prod."Surveys"',
        dialect="postgres",
        allowed_schemas=["prod"],
        max_rows=100,
        allowed_tables=["prod.surveys"],
    )
    assert result.tables == ["prod.Surveys"]


def test_insert_is_rejected():
    with pytest.raises(GuardError, match="SELECT"):
        validate(
            "INSERT INTO prod.surveys (id) VALUES (1)",
            dialect="postgres",
            allowed_schemas=["prod"],
            max_rows=100,
        )
