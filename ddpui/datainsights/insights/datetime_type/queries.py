from dataclasses import dataclass

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
    text,
)
from sqlalchemy.sql.functions import func, Function, register_function, GenericFunction
from sqlalchemy import Float, extract, literal

from ddpui.datainsights.insights.insight_interface import ColInsight
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType


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

    def __init__(
        self, column_name: str, db_table: str, db_schema: str, filter: dict, wtype: str
    ):
        super().__init__(column_name, db_table, db_schema, filter, wtype)
        if filter:
            self.filter: BarChartFilter = BarChartFilter(**filter)
        else:
            self.filter: BarChartFilter = BarChartFilter("year", 10, 0)  # default

    def query_id(self) -> str:
        return f"chart-queryid-{self.columns[0].name}"

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
            groupby_cols = ["year"]
            orderby_cols = [("year", "desc")]

        if self.filter.range == "month":
            query = query.add_column(
                extract("year", datetime_col).label("year")
            ).add_column(extract("month", datetime_col).label("month"))
            groupby_cols = ["year", "month"]
            orderby_cols = [("year", "desc"), ("month", "desc")]

        if self.filter.range == "day":
            query = (
                query.add_column(extract("year", datetime_col).label("year"))
                .add_column(extract("month", datetime_col).label("month"))
                .add_column(extract("day", datetime_col).label("day"))
            )
            groupby_cols = ["year", "month", "day"]
            orderby_cols = [("year", "desc"), ("month", "desc"), ("day", "desc")]

        query = (
            query.add_column(func.count().label("frequency"))
            .fetch_from(self.db_table, self.db_schema)
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

            },
            {

            }
        ]
        """
        return {
            self.columns[0].name: {
                "charts": [
                    {
                        "chartType": self.chart_type(),
                        "data": [
                            {key: int(value) for key, value in record.items()}
                            for record in result
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
