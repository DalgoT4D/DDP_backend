from dataclasses import dataclass, asdict

from sqlalchemy.sql.expression import (
    column,
    ColumnClause,
)
from sqlalchemy.sql.functions import func
from sqlalchemy import extract

from ddpui.core.datainsights.insights.insight_interface import (
    ColInsight,
    TranslateColDataType,
)
from ddpui.utils.helpers import hash_dict


@dataclass
class BarChartFilter:
    range: str
    limit: int
    offset: int


class DistributionChart(ColInsight):
    """
    Returns a sqlalchemy query ready to be executed by an engine
    Computes the frequency chart
    """

    def __init__(self, column_name: str, db_table: str, db_schema: str, filter_: dict, wtype: str):
        super().__init__(column_name, db_table, db_schema, filter_, wtype)
        if filter_:
            self.filter: BarChartFilter = BarChartFilter(**filter_)
        else:
            self.filter: BarChartFilter = BarChartFilter("year", 10, 0)  # default

    def query_id(self) -> str:
        """
        This will be dictate whether a query is unique or not
        Returns a hash string
        """
        return hash_dict(
            {
                "columns": [col.name for col in self.columns],
                "type": TranslateColDataType.DATETIME,
                "filter": asdict(self.filter) if self.filter else self.filter,
                "chart_type": self.chart_type(),
            }
        )

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        if len(self.columns) < 1:
            raise ValueError("No column specified")

        col = self.columns[0]
        datetime_col: ColumnClause = column(col.name)

        query = self.builder
        groupby_cols = []
        orderby_cols = []

        if self.filter.range == "year":
            query = query.add_column(extract("year", datetime_col).label("year"))
            groupby_cols = [
                extract("year", datetime_col),
            ]
            orderby_cols = [("year", "desc")]

        if self.filter.range == "month":
            query = query.add_column(extract("year", datetime_col).label("year")).add_column(
                extract("month", datetime_col).label("month")
            )
            groupby_cols = [
                extract("year", datetime_col),
                extract("month", datetime_col),
            ]
            orderby_cols = [("year", "desc"), ("month", "desc")]

        if self.filter.range == "day":
            query = (
                query.add_column(extract("year", datetime_col).label("year"))
                .add_column(extract("month", datetime_col).label("month"))
                .add_column(extract("day", datetime_col).label("day"))
            )
            groupby_cols = [
                extract("year", datetime_col),
                extract("month", datetime_col),
                extract("day", datetime_col),
            ]
            orderby_cols = [("year", "desc"), ("month", "desc"), ("day", "desc")]

        query = (
            query.add_column(func.count().label("frequency"))
            .fetch_from(self.db_table, self.db_schema)
            .where_clause(datetime_col.isnot(None))
            .group_cols_by(*groupby_cols)
            .order_cols_by(orderby_cols)
            .limit_rows(self.filter.limit)
            .offset_rows(self.filter.offset)
        )

        return query.build()

    def parse_results(self, result: list[dict]):
        """
        Parses the result from the above executed sql query
        Result:
        [
            {
                "frequency": 10,
                "year": 2021,
                "month": 1,
                "day": 1
            },
            {
                "frequency": 10,
                "year": 2021,
                "month": 1,
                "day": 1
            }
        ]
        """
        return {
            self.columns[0].name: {
                "charts": [
                    {
                        "chartType": self.chart_type(),
                        "data": [
                            {key: int(value) for key, value in record.items()} for record in result
                        ],
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

    def query_data_type(self) -> TranslateColDataType:
        return TranslateColDataType.DATETIME
