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
)
from sqlalchemy.sql.functions import func, Function
from sqlalchemy import Float

from ddpui.datainsights.insights.insight_interface import ColInsight


class DataStats(ColInsight):

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        column_name = self.column_name
        bool_col: ColumnClause = column(column_name)

        query = (
            self.builder.add_column(func.count().label("count"))
            .add_column(
                func.sum(
                    case(
                        [(bool_col.is_(None), 1)],
                        else_=0,
                    )
                ).label("countNull"),
            )
            .add_column(func.count(distinct(bool_col)).label("countDistinct"))
            .add_column(
                func.sum(
                    case(
                        [(bool_col == True, 1)],
                        else_=0,
                    )
                ).label("countTrue"),
            )
            .add_column(
                func.sum(
                    case(
                        [(bool_col == False, 1)],
                        else_=0,
                    )
                ).label("countFalse"),
            )
        )

        return query.fetch_from(self.db_table, self.db_schema).build()

    def parse_results(self, result: list[dict]):
        """
        Parses the result from the above executed sql query
        Result:
        [
            {
                "count": 399,
                "countNull": 0,
                "countDistinct": 1,
                "countTrue": 50,
                "countFalse": 40
            }
        ]
        """
        if len(result) > 0:
            return {
                "count": result[0]["count"],
                "countNull": result[0]["countNull"],
                "countDistinct": result[0]["countDistinct"],
                "countTrue": result[0]["countTrue"],
                "countFalse": result[0]["countFalse"],
            }

        return {
            "count": 0,
            "countNull": 0,
            "countDistinct": 0,
            "countTrue": 0,
            "countFalse": 0,
        }
