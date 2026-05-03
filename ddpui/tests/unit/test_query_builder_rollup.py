import pytest
from sqlalchemy import text
from ddpui.core.datainsights.query_builder import AggQueryBuilder


class TestRollupSupport:
    """Tests for ROLLUP and GROUPING support in AggQueryBuilder"""

    def test_group_cols_by_rollup_produces_rollup_clause(self):
        """ROLLUP columns should appear in GROUP BY ROLLUP(...) in compiled SQL"""
        qb = AggQueryBuilder()
        qb.fetch_from("beneficiary_records", "public")
        qb.add_column(text("district"))
        qb.add_column(text("program"))
        qb.group_cols_by_rollup("district", "program")
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ROLLUP" in compiled.upper()

    def test_multiple_rollup_groups(self):
        """Two separate ROLLUP groups should both appear in GROUP BY"""
        qb = AggQueryBuilder()
        qb.fetch_from("beneficiary_records", "public")
        qb.add_column(text("district"))
        qb.add_column(text("pivot_col"))
        qb.group_cols_by_rollup("district", "program")
        qb.group_cols_by_rollup("pivot_col")
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert compiled.upper().count("ROLLUP") == 2

    def test_add_grouping_column(self):
        """GROUPING() function should appear in SELECT with alias"""
        qb = AggQueryBuilder()
        qb.fetch_from("beneficiary_records", "public")
        qb.add_column(text("district"))
        qb.add_grouping_column("district", "_grp_district")
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "grouping" in compiled.lower()
        assert "_grp_district" in compiled

    def test_rollup_with_regular_group_by(self):
        """ROLLUP columns and regular GROUP BY columns should coexist"""
        qb = AggQueryBuilder()
        qb.fetch_from("beneficiary_records", "public")
        qb.add_column(text("district"))
        qb.add_column(text("status"))
        qb.group_cols_by("status")
        qb.group_cols_by_rollup("district")
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ROLLUP" in compiled.upper()
