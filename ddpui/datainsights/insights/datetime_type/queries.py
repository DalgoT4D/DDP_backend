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


class DataStats(ColInsight):

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        column_name = self.column_name
        datetime_col: ColumnClause = column(column_name)

        query = (
            self.builder.add_column(func.count().label("count"))
            .add_column(
                func.sum(
                    case(
                        [(datetime_col.is_(None), 1)],
                        else_=0,
                    )
                ).label("countNull"),
            )
            .add_column(func.count(distinct(datetime_col)).label("countDistinct"))
            .add_column(
                func.max(datetime_col).label("maxVal"),
            )
            .add_column(
                func.min(datetime_col).label("minVal"),
            )
        )

        if self.wtype == WarehouseType.POSTGRES:
            query = query.add_column(
                (
                    func.date(func.max(datetime_col))
                    - func.date(func.min(datetime_col))
                ).label("rangeInDays"),
            )
        elif self.wtype == WarehouseType.BIGQUERY:
            query = query.add_column(
                func.TIMESTAMP_DIFF(
                    func.max(datetime_col), func.min(datetime_col), text("DAY")
                ).label("rangeInDays")
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
                "rangeInDays": ""
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
                "rangeInDays": result[0]["rangeInDays"],
            }

        return {
            "count": 0,
            "countNull": 0,
            "countDistinct": 0,
            "maxVal": None,
            "minVal": None,
            "rangeInDays": 0,
        }


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

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
        column_name = self.column_name
        datetime_col: ColumnClause = column(column_name)

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
            "chartType": self.chart_type(),
            "data": result,
        }

    def chart_type(self) -> str:
        return "bar"
