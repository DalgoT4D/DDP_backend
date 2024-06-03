from sqlalchemy.sql.expression import (
    column,
    ColumnClause,
    table,
    cast,
    select,
    desc,
)
from sqlalchemy.sql.functions import func
from sqlalchemy import NUMERIC

from ddpui.datainsights.insights.insight_interface import (
    ColInsight,
    TranslateColDataType,
)
from ddpui.utils.helpers import hash_dict


class DataStats(ColInsight):

    def query_id(self) -> str:
        """
        This will be dictate whether a query is unique or not
        Returns a hash string
        """
        hash_ = hash_dict(
            {
                "columns": ",".join([col.name for col in self.columns]),
                "type": TranslateColDataType.NUMERIC,
                "filter": self.filter,
                "chart_type": self.chart_type(),
            }
        )
        return hash_

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
            .where_clause(numeric_col.isnot(None))
            .fetch_from(self.db_table, self.db_schema)
            .subquery(alias="subquery")
        )

        query = (
            self.builder.reset()
            .add_column(
                func.round(cast(func.avg(numeric_col), NUMERIC), 2).label("mean"),
            )
            .add_column(
                select(
                    [
                        func.round(
                            cast(func.avg(median_subquery.c[f"{col.name}"]), NUMERIC),
                            2,
                        ),
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
                .where(numeric_col.isnot(None))
                .group_by(numeric_col)
                .order_by(desc(func.count(numeric_col)))
                .limit(1)
                .label("mode")
            )
        )

        return (
            query.fetch_from(self.db_table, self.db_schema)
            .where_clause(numeric_col.isnot(None))
            .build()
        )

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
                    "mean": float(result[0]["mean"]),
                    "median": float(result[0]["median"]),
                    "mode": float(result[0]["mode"]),
                }
            }

        return {
            self.columns[0].name: {
                "mean": 0,
                "median": 0,
                "mode": 0,
            }
        }

    def validate_query_results(self, parsed_results):
        """
        Validate the parsed results of the query
        This function assumes the parsed_results sent is for a single column
        """
        validate = False
        if (
            parsed_results
            and isinstance(parsed_results, dict)
            and all(key in parsed_results for key in ["mean", "median", "mode"])
        ):
            validate = True

        return validate

    def query_data_type(self) -> TranslateColDataType:
        return TranslateColDataType.NUMERIC
