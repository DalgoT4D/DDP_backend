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
        numeric_col: ColumnClause = column(column_name)

        median_subquery = (
            self.builder.add_column(numeric_col)
            .add_column(func.count().over().label("total_rows"))
            .add_column(
                func.row_number().over(order_by=numeric_col).label("row_number")
            )
            .fetch_from(self.db_table, self.db_schema)
            .subquery(alias="subquery")
        )

        query = (
            self.builder.reset()
            .add_column(func.count().label("count"))
            .add_column(
                func.sum(
                    case(
                        [(numeric_col.is_(None), 1)],
                        else_=0,
                    )
                ).label("countNull"),
            )
            .add_column(func.count(distinct(numeric_col)).label("countDistinct"))
            .add_column(
                func.max(numeric_col).label("maxVal"),
            )
            .add_column(
                func.min(numeric_col).label("minVal"),
            )
            .add_column(
                cast(func.round(func.avg(numeric_col), 2), Float).label("mean"),
            )
            .add_column(
                select(
                    [
                        cast(
                            func.round(
                                func.avg(median_subquery.c[f"{column_name}"]), 2
                            ),
                            Float,
                        )
                    ]
                )
                .where(
                    median_subquery.c.row_number.in_(
                        [
                            (median_subquery.c.total_rows + 1) / 2,
                            (median_subquery.c.total_rows + 2) / 2,
                        ]
                    )
                )
                .label("median")
            )
            .add_column(
                select([numeric_col])
                .select_from(table(self.db_table, schema=self.db_schema))
                .group_by(numeric_col)
                .order_by(desc(func.count(numeric_col)))
                .limit(1)
                .label("mode")
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
                "maxVal": 100,
                "minVal": 100,
                "mean": 12.00,
                "mode": 100,
                "media": 50
            }
        ]
        """
        if len(result) > 0:
            return {
                "count": result[0]["count"],
                "countNull": result[0]["countNull"],
                "countDistinct": result[0]["countDistinct"],
                "maxVal": result[0]["maxVal"],
                "minVal": result[0]["minVal"],
                "mean": result[0]["mean"],
                "median": result[0]["median"],
                "mode": result[0]["mode"],
            }

        return {
            "count": 0,
            "countNull": 0,
            "countDistinct": 0,
            "maxVal": 0,
            "minVal": 0,
            "mean": 0,
            "median": 0,
            "mode": 0,
        }
