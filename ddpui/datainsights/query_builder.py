from sqlalchemy.sql.functions import Function
from sqlalchemy import func
from sqlalchemy.sql.expression import (
    table,
    TableClause,
    select,
    Select,
    ColumnClause,
    column,
    asc,
    desc,
)


class AggQueryBuilder:
    """
    Aggregate query builder
    All column clauses will have to be an aggregate function and cannot be a bare column itself
    """

    def __init__(self):
        self.column_clauses: list[Function | ColumnClause] = []
        self.select_from: TableClause = None
        self.group_by_clauses: list[ColumnClause] = []
        self.order_by_clauses: list[ColumnClause] = []
        self.limit_records: int = None
        self.offset_records: int = 0
        self.where_clauses: list = []
        self.having_clauses: list = []

    def add_column(self, agg_col: Function | ColumnClause):
        """Push a column to select"""
        self.column_clauses.append(agg_col)
        return self

    def add_aggregate_column(self, column_name: str, agg_func: str, alias: str = None):
        """Add an aggregate column with specified function"""
        agg_func_lower = agg_func.lower()

        # Handle count with None column - use COUNT(*) instead of COUNT(None)
        if agg_func_lower == "count" and column_name is None:
            agg_column = func.count()
        else:
            # Quote column name to preserve case
            col = column(column_name)

            agg_functions = {
                "sum": func.sum,
                "avg": func.avg,
                "count": func.count,
                "min": func.min,
                "max": func.max,
                "count_distinct": lambda c: func.count(func.distinct(c)),
            }

            if agg_func_lower not in agg_functions:
                raise ValueError(f"Unsupported aggregate function: {agg_func}")

            if agg_func_lower == "count_distinct":
                agg_column = agg_functions[agg_func_lower](col)
            else:
                agg_column = agg_functions[agg_func_lower](col)

        if alias:
            agg_column = agg_column.label(alias)

        self.column_clauses.append(agg_column)
        return self

    def fetch_from(self, db_table: str, db_schema: str):
        self.select_from = table(db_table, schema=db_schema)
        return self

    def fetch_from_subquery(self, subquery: Select):
        self.select_from = subquery
        return self

    def group_cols_by(self, *cols):
        """Group by the columns"""
        for col in cols:
            if isinstance(col, str):
                self.group_by_clauses.append(column(col))
            else:
                self.group_by_clauses.append(col)
        return self

    def having_clause(self, condition):
        """Having clause"""
        self.having_clauses.append(condition)
        return self

    def order_cols_by(self, cols: list[tuple[str, str]]):
        """Group by the columns"""
        for col, order in cols:
            if order.lower() == "asc":
                self.order_by_clauses.append(asc(column(col)))
            elif order.lower() == "desc":
                self.order_by_clauses.append(desc(column(col)))
        return self

    def where_clause(self, condition):
        """Add where clause"""
        self.where_clauses.append(condition)
        return self

    def limit_rows(self, limit: int):
        """Limit the number of rows"""
        self.limit_records = limit
        return self

    def offset_rows(self, offset: int):
        """Offset the number of rows"""
        self.offset_records = offset
        return self

    def subquery(self, alias=None):
        """return sql select query without compiling to be used another query"""
        if self.select_from is None:
            raise ValueError("Table to select from is not provided")

        stmt: Select = select(*self.column_clauses)
        stmt = stmt.select_from(self.select_from)

        if len(self.where_clauses) > 0:
            for where_clause in self.where_clauses:
                stmt = stmt.where(where_clause)

        if len(self.group_by_clauses) > 0:
            stmt = stmt.group_by(*self.group_by_clauses)
            if len(self.having_clauses) > 0:
                for having_clause in self.having_clauses:
                    stmt = stmt.having(having_clause)

        if len(self.order_by_clauses) > 0:
            stmt = stmt.order_by(*self.order_by_clauses)

        if self.limit_records:
            stmt = stmt.slice(self.offset_records, self.offset_records + self.limit_records)

        if alias:
            stmt = stmt.alias(alias)

        return stmt

    def build(self):
        """return the sql statement to be excuted"""

        return self.subquery()

    def reset(self):
        self.column_clauses: list[Function] = []
        self.select_from: TableClause = None
        self.group_by_clauses: list[ColumnClause] = []
        self.order_by_clauses: list[ColumnClause] = []
        self.limit_records: int = None
        self.offset_records: int = 0
        self.where_clauses: list = []
        self.having_clauses: list = []

        return self
