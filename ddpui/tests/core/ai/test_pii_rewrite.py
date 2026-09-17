"""Tests for deterministic PII rewriting.

Pure sqlglot — no Django, no warehouse. SCHEMA is the shape catalog.schema_map_for
returns, so these tests exercise the same input the tools pass in production.
"""

import pytest

from ddpui.core.ai.guards.pii_rewrite import (
    UnresolvableProjection,
    resolve_projection,
)

SCHEMA = {
    "prod": {
        "people": {"id": "integer", "name": "text"},
        "beneficiaries": {"person_id": "integer", "phone": "text", "district": "text"},
    }
}


def keys(sql, dialect="postgres"):
    return [column.key for column in resolve_projection(sql, dialect, SCHEMA)]


def test_table_aliases_resolve_to_schema_table_column():
    sql = (
        "SELECT p.name, b.phone FROM prod.people p "
        "JOIN prod.beneficiaries b ON b.person_id = p.id"
    )
    assert keys(sql) == ["prod.beneficiaries.phone", "prod.people.name"]


def test_select_star_expands_to_real_columns():
    assert keys("SELECT * FROM prod.beneficiaries") == [
        "prod.beneficiaries.district",
        "prod.beneficiaries.person_id",
        "prod.beneficiaries.phone",
    ]


def test_unqualified_column_resolves_via_the_schema_map():
    sql = "SELECT name FROM prod.people p " "JOIN prod.beneficiaries b ON b.person_id = p.id"
    assert keys(sql) == ["prod.people.name"]


def test_aggregate_over_a_column_still_lists_it():
    assert keys("SELECT COUNT(DISTINCT phone) FROM prod.beneficiaries") == [
        "prod.beneficiaries.phone"
    ]


def test_cte_columns_are_listed_from_the_cte_body():
    sql = (
        "WITH x AS (SELECT phone, district FROM prod.beneficiaries) "
        "SELECT phone, COUNT(*) FROM x GROUP BY phone"
    )
    # district is selected by the CTE but dropped by the outer query — listing it
    # is deliberate over-listing, safe because a stray tick only no-ops
    assert keys(sql) == ["prod.beneficiaries.district", "prod.beneficiaries.phone"]


def flags(sql):
    return {c.key: c.has_literal for c in resolve_projection(sql, "postgres", SCHEMA)}


def test_join_key_and_is_not_null_are_not_literals():
    sql = (
        "SELECT p.name FROM prod.people p "
        "JOIN prod.beneficiaries b ON b.person_id = p.id "
        "WHERE b.phone IS NOT NULL"
    )
    assert flags(sql) == {"prod.people.name": False}


def test_equality_against_a_literal_is_flagged():
    sql = "SELECT district FROM prod.beneficiaries WHERE phone = '9876543210'"
    resolved = flags(sql)
    assert resolved["prod.beneficiaries.district"] is False
    assert (
        resolve_projection(
            "SELECT phone FROM prod.beneficiaries WHERE phone = '9876543210'",
            "postgres",
            SCHEMA,
        )[0].has_literal
        is True
    )


def test_in_list_is_flagged():
    sql = "SELECT phone FROM prod.beneficiaries WHERE phone IN ('1', '2')"
    assert flags(sql) == {"prod.beneficiaries.phone": True}


def test_having_count_literal_is_not_a_column_literal():
    sql = "SELECT phone, COUNT(*) FROM prod.beneficiaries " "GROUP BY phone HAVING COUNT(*) > 1"
    assert flags(sql) == {"prod.beneficiaries.phone": False}


def test_sibling_subqueries_reusing_an_alias_resolve_independently():
    sql = (
        "SELECT x.phone, y.name "
        "FROM (SELECT t.phone FROM prod.beneficiaries t) x "
        "JOIN (SELECT t.name FROM prod.people t) y ON TRUE"
    )
    assert keys(sql) == ["prod.beneficiaries.phone", "prod.people.name"]


def test_two_ctes_reusing_an_alias_resolve_independently():
    sql = (
        "WITH a AS (SELECT t.phone FROM prod.beneficiaries t), "
        "b AS (SELECT t.name FROM prod.people t) "
        "SELECT a.phone, b.name FROM a JOIN b ON TRUE"
    )
    assert keys(sql) == ["prod.beneficiaries.phone", "prod.people.name"]


def test_unknown_column_raises_with_an_llm_readable_message():
    with pytest.raises(UnresolvableProjection, match="get_table_details"):
        resolve_projection("SELECT nope FROM prod.people", "postgres", SCHEMA)
