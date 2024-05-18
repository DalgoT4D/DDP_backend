from sqlalchemy.sql.functions import func, Function
from sqlalchemy.sql.expression import (
    table,
    TableClause,
    select,
    Select,
    ColumnClause,
    column,
    text,
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

    def add_column(self, agg_col: Function | ColumnClause):
        """Push a column to select"""
        self.column_clauses.append(agg_col)
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

    def order_cols_by(self, cols: list[tuple[str, str]]):
        """Group by the columns"""
        for col, order in cols:
            if order.lower() == "asc":
                self.order_by_clauses.append(asc(column(col)))
            elif order.lower() == "desc":
                self.order_by_clauses.append(desc(column(col)))
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

        stmt: Select = select(self.column_clauses)
        stmt = stmt.select_from(self.select_from)

        if len(self.group_by_clauses) > 0:
            stmt = stmt.group_by(*self.group_by_clauses)

        if len(self.order_by_clauses) > 0:
            stmt = stmt.order_by(*self.order_by_clauses)

        if self.limit_records:
            stmt = stmt.slice(self.offset_records, self.limit_records)

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

        return self
