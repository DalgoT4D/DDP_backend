"""Tests for deterministic PII rewriting.

Pure sqlglot — no Django, no warehouse. SCHEMA is the shape catalog.schema_map_for
returns, so these tests exercise the same input the tools pass in production.
"""

import pytest
import sqlglot

from ddpui.core.ai.guards.pii_rewrite import (
    UnresolvableProjection,
    _qualified_tree,
    hash_projection,
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


def test_scalar_subquery_columns_do_not_leak_into_the_outer_scope():
    # the inner alias `t` shadows the outer `t`; each must resolve in its own scope
    sql = (
        "SELECT t.name, "
        "(SELECT t.phone FROM prod.beneficiaries t WHERE t.person_id = 5) AS sub "
        "FROM prod.people t"
    )
    assert keys(sql) == ["prod.beneficiaries.phone", "prod.people.name"]


def test_unknown_column_raises_with_an_llm_readable_message():
    with pytest.raises(UnresolvableProjection, match="get_table_details"):
        resolve_projection("SELECT nope FROM prod.people", "postgres", SCHEMA)


def rewrite(sql, ticked, dialect="postgres"):
    return hash_projection(sql, dialect, SCHEMA, set(ticked))


def test_ticked_column_is_wrapped_and_keeps_its_alias():
    out = rewrite(
        "SELECT phone AS contact FROM prod.beneficiaries",
        {"prod.beneficiaries.phone"},
    )
    assert "MD5(CAST(" in out
    assert 'AS "contact"' in out


def test_no_ticks_returns_the_sql_verbatim():
    sql = "SELECT phone FROM prod.beneficiaries"
    assert rewrite(sql, set()) == sql


def test_unticked_columns_are_left_alone():
    out = rewrite(
        "SELECT phone, district FROM prod.beneficiaries",
        {"prod.beneficiaries.phone"},
    )
    assert out.count("MD5") == 1
    assert '"district"' in out


def test_count_distinct_wraps_inside_the_aggregate():
    out = rewrite(
        "SELECT COUNT(DISTINCT phone) AS uniq FROM prod.beneficiaries",
        {"prod.beneficiaries.phone"},
    )
    assert "COUNT(DISTINCT MD5(CAST(" in out
    assert 'AS "uniq"' in out


def test_where_and_join_are_untouched_by_the_rewrite():
    sql = (
        "SELECT p.name, b.phone FROM prod.people p "
        "JOIN prod.beneficiaries b ON b.person_id = p.id "
        "WHERE b.phone = '9876543210'"
    )
    hashed = sqlglot.parse_one(rewrite(sql, {"prod.beneficiaries.phone"}), dialect="postgres")
    plain = _qualified_tree(sql, "postgres", SCHEMA)

    assert hashed.args["where"].sql() == plain.args["where"].sql()
    assert hashed.args["joins"][0].sql() == plain.args["joins"][0].sql()
    assert "MD5" in hashed.sql()  # the projection DID change


def test_cte_column_is_hashed_inside_the_cte_body():
    out = rewrite(
        "WITH x AS (SELECT phone FROM prod.beneficiaries) "
        "SELECT phone, COUNT(*) FROM x GROUP BY phone",
        {"prod.beneficiaries.phone"},
    )
    # hashed at the source, so the outer query's x.phone is already a hash
    assert out.index("MD5") < out.index('FROM "x"')
    assert out.count("MD5") == 1


def test_bigquery_uses_to_hex_over_md5():
    out = rewrite(
        "SELECT phone FROM prod.beneficiaries",
        {"prod.beneficiaries.phone"},
        dialect="bigquery",
    )
    assert "TO_HEX(MD5(CAST(" in out
    assert "STRING" in out
