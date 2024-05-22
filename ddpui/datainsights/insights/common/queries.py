from datetime import datetime, date
from sqlalchemy.sql.expression import (
    column,
    ColumnClause,
    case,
    distinct,
    table,
    TableClause,
    cast,
    select,
    desc,
    literal_column,
)
from sqlalchemy.sql.functions import func, Function
from sqlalchemy import Float

from ddpui.datainsights.insights.insight_interface import ColInsight


class BaseDataStats(ColInsight):

    def query_id(self) -> str:
        return "base-query-id"

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        query = self.builder

        for col in self.columns:
            col_clause = column(col.name)
            query = (
                query.add_column(func.count().label(f"count_{col.name}"))
                .add_column(
                    func.sum(
                        case(
                            [(col_clause.is_(None), 1)],
                            else_=0,
                        )
                    ).label(f"countNull_{col.name}"),
                )
                .add_column(
                    func.count(distinct(col_clause)).label(f"countDistinct__{col.name}")
                )
                .add_column(
                    func.max(col_clause).label(f"maxVal_{col.name}"),
                )
                .add_column(
                    func.min(col_clause).label(f"minVal_{col.name}"),
                )
            )

        return query.fetch_from(self.db_table, self.db_schema).build()

    def parse_results(self, result: list[dict]):
        """
        Parses the result from the above executed sql query
        Serialize datetime/date objects
        Result:
        [
            {
                "count_col1": 399,
                "countNull_col1": 0,
                "countDistinct_col1": 1,
                "maxVal_col1": 1,
                "minVal_col1": 0,
                "count_col2": 399,
                "countNull_col2": 0,
                "countDistinct_col2": 1,
                "maxVal_col2": 1,
                "minVal_col2": 0,
                .....
            }
        ]
        Output:
        {
            "col1": {
                "count": 399,
                "countNull": 0,
                "countDistinct": 1,
                "maxVal": 100,
                "minVal": 100,
            },
            "col2": {
                ...
            },....
        }
        """
        res = {}
        for col in self.columns:
            res[f"{col.name}"] = {}

        if len(result) > 0:
            for col in self.columns:
                res[f"{col.name}"] = {
                    "count": result[0][f"count_{col.name}"],
                    "countNull": result[0][f"countNull_{col.name}"],
                    "countDistinct": result[0][f"countDistinct__{col.name}"],
                    "maxVal": (
                        str(result[0][f"maxVal_{col.name}"])
                        if isinstance(result[0][f"maxVal_{col.name}"], (datetime, date))
                        else result[0][f"maxVal_{col.name}"]
                    ),
                    "minVal": (
                        str(result[0][f"minVal_{col.name}"])
                        if isinstance(result[0][f"minVal_{col.name}"], (datetime, date))
                        else result[0][f"minVal_{col.name}"]
                    ),
                }

        return res
