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


class DataStats(ColInsight):

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        column_name = self.column_name
        string_col: ColumnClause = column(column_name)

        query = (
            self.builder.add_column(func.count().label("count"))
            .add_column(
                func.sum(
                    case(
                        [(string_col.is_(None), 1)],
                        else_=0,
                    )
                ).label("countNull"),
            )
            .add_column(func.count(distinct(string_col)).label("countDistinct"))
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
            }
        ]
        """
        if len(result) > 0:
            return {
                "count": result[0]["count"],
                "countNull": result[0]["countNull"],
                "countDistinct": result[0]["countDistinct"],
            }

        return {
            "count": 0,
            "countNull": 0,
            "countDistinct": 0,
        }


class DistributionChart(ColInsight):

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        column_name = self.column_name
        string_col: ColumnClause = column(column_name)

        subquery = (
            self.builder.add_column(string_col.label("category"))
            .add_column(func.count().label("count"))
            .fetch_from(self.db_table, self.db_schema)
            .group_cols_by(string_col.name)
            .order_cols_by([("count", "desc")])
            .limit_rows(5)
            .subquery()
        )

        query = (
            self.builder.reset()
            .add_column(
                case(
                    [(string_col.in_(select([subquery.c.category])), string_col)],
                    else_=literal_column("'other'"),
                ).label("category"),
            )
            .add_column(func.count().label("count"))
            .fetch_from(self.db_table, self.db_schema)
            .group_cols_by("category")
            .order_cols_by([("count", "desc")])
            .build()
        )

        return query

    def parse_results(self, result: list[dict]):
        """
        Parses the result from the above executed sql query
        Result:
        [
            {
                "count": 399,
                "countNull": 0,
                "countDistinct": 1,
            }
        ]
        """
        return {
            "chartType": self.chart_type(),
            "data": result,
        }

    def chart_type(self) -> str:
        return "bar"
