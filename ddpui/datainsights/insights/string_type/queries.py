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


class DistributionChart(ColInsight):

    def query_id(self) -> str:
        return "string-query-id"

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        if len(self.columns) < 1:
            raise ValueError("No column specified")

        col = self.columns[0]
        string_col: ColumnClause = column(col.name)

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
                "chartType": "bar",
                "data": []
            }
        ]
        """
        return {
            self.columns[0].name: {
                "charts": [
                    {
                        "chartType": self.chart_type(),
                        "data": result,
                    }
                ]
            }
        }

    def chart_type(self) -> str:
        return "bar"

    def validate_query_results(self, parsed_results) -> bool:
        """
        Validate the parsed results of the query
        This function assumes the parsed_results sent is for a single column
        """
        validate = False
        if (
            parsed_results
            and isinstance(parsed_results, dict)
            and "charts" in parsed_results
            and len(parsed_results["charts"]) > 0
        ):
            if all(
                key in parsed_results["charts"][0]
                for key in [
                    "chartType",
                    "data",
                ]
            ):
                validate = True

        return validate
