from sqlalchemy.sql.functions import func, Function
from sqlalchemy.sql.expression import table, TableClause, select, Select


class AggQueryBuilder:
    """
    Aggregate query builder
    All column clauses will have to be an aggregate function and cannot be a bare column itself
    """

    def __init__(self):
        self.column_clauses: list[Function] = []
        self.select_from: TableClause = None

    def add_column(self, agg_col: Function):
        """Push a column to select"""
        self.column_clauses.append(agg_col)
        return self

    def fetch_from(self, db_table: str, db_schema: str):
        self.select_from = table(db_table, schema=db_schema)
        return self

    def group_cols_by(self, *cols):
        """Group by the columns"""
        self.column_clauses.append(func.group_by(*cols))
        return self

    def build(self):
        """return the sql statement to be excuted"""
        if self.select_from is None:
            raise ValueError("Table to select from is not provided")

        stmt: Select = select(self.column_clauses)
        stmt = stmt.select_from(self.select_from).compile(
            compile_kwargs={"literal_binds": True}
        )
        return stmt
