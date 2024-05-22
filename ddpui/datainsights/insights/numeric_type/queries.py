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
    literal,
    union_all,
)
from sqlalchemy.sql.functions import func, Function
from sqlalchemy import Float

from ddpui.datainsights.insights.insight_interface import (
    ColInsight,
    TranslateColDataType,
)


class DataStats(ColInsight):

    def query_id(self) -> str:
        return "numeric-query-id"

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        if len(self.columns) < 1:
            raise ValueError("No column specified")

        col = self.columns[0]
        numeric_col: ColumnClause = column(col.name)

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
            .add_column(
                cast(func.round(func.avg(numeric_col), 2), Float).label("mean"),
            )
            .add_column(
                select(
                    [
                        cast(
                            func.round(func.avg(median_subquery.c[f"{col.name}"]), 2),
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
                "mean": 12.00,
                "mode": 100,
                "media": 50
            }
        ]
        """
        if len(result) > 0:
            return {
                self.columns[0].name: {
                    "mean": result[0]["mean"],
                    "median": result[0]["median"],
                    "mode": result[0]["mode"],
                }
            }

        return {
            self.columns[0].name: {
                "mean": 0,
                "median": 0,
                "mode": 0,
            }
        }
