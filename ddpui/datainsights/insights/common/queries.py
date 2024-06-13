from datetime import datetime, date
from sqlalchemy.sql.expression import (
    column,
    case,
    distinct,
    cast,
    literal,
)
from sqlalchemy.sql.functions import func
from sqlalchemy import Integer

from ddpui.datainsights.insights.insight_interface import (
    ColInsight,
    TranslateColDataType,
)
from ddpui.utils.helpers import hash_dict


class BaseDataStats(ColInsight):
    """
    Computes basic stats
    """

    def query_id(self) -> str:
        """
        This will be dictate whether a query is unique or not
        Returns a hash string
        """
        return hash_dict(
            {
                "type": TranslateColDataType.BASE,
                "filter": self.filter,
                "chart_type": self.chart_type(),
            }
        )

    def generate_sql(self):

        query = self.builder

        for col in self.columns:
            col_clause = column(col.name)
            query = query.add_column(
                func.count().label(f"count_{col.name}")
            ).add_column(
                func.sum(
                    case(
                        [(col_clause.is_(None), 1)],
                        else_=0,
                    )
                ).label(f"countNull_{col.name}"),
            )

            # distinct count
            if col.translated_type == TranslateColDataType.JSON:
                query = query.add_column(
                    literal(None).label(f"countDistinct__{col.name}")
                )
            else:
                query = query.add_column(
                    func.count(distinct(col_clause)).label(f"countDistinct__{col.name}")
                )

            # min max
            if col.translated_type == TranslateColDataType.BOOL:
                query = query.add_column(
                    func.max(cast(col_clause, Integer)).label(f"maxVal_{col.name}"),
                ).add_column(
                    func.min(cast(col_clause, Integer)).label(f"minVal_{col.name}"),
                )
            elif col.translated_type == TranslateColDataType.JSON:
                query = query.add_column(
                    literal(None).label(f"maxVal_{col.name}"),
                ).add_column(
                    literal(None).label(f"minVal_{col.name}"),
                )
            elif col.translated_type == TranslateColDataType.STRING:
                query = query.add_column(
                    func.max(func.length(col_clause)).label(f"maxVal_{col.name}"),
                ).add_column(
                    func.min(func.length(col_clause)).label(f"minVal_{col.name}"),
                )
            else:
                query = query.add_column(
                    func.max(col_clause).label(f"maxVal_{col.name}"),
                ).add_column(
                    func.min(col_clause).label(f"minVal_{col.name}"),
                )

        return query.fetch_from(self.db_table, self.db_schema).build()

    def parse_results(self, result: list[dict]):
        """
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

    def validate_query_results(self, parsed_results):
        """
        Validate the parsed results of the query
        This function assumes the parsed_results sent is for a single column
        """
        validate = False
        if (
            parsed_results
            and isinstance(parsed_results, dict)
            and all(
                key in parsed_results
                for key in ["count", "countNull", "countDistinct", "maxVal", "minVal"]
            )
        ):
            validate = True

        return validate

    def query_data_type(self) -> TranslateColDataType:
        return TranslateColDataType.BASE
