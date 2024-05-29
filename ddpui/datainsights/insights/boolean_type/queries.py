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

from ddpui.datainsights.insights.insight_interface import (
    ColInsight,
    TranslateColDataType,
)
from ddpui.utils.helpers import hash_dict


class DataStats(ColInsight):
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
                "columns": [col.name for col in self.columns],
                "type": TranslateColDataType.BOOL,
                "filter": self.filter,
                "chart_type": self.chart_type(),
            }
        )

    def generate_sql(self):

        if len(self.columns) < 1:
            raise ValueError("No column specified")

        col = self.columns[0]
        bool_col: ColumnClause = column(col.name)

        query = self.builder.add_column(
            func.sum(
                case(
                    [(bool_col == True, 1)],
                    else_=0,
                )
            ).label("countTrue"),
        ).add_column(
            func.sum(
                case(
                    [(bool_col == False, 1)],
                    else_=0,
                )
            ).label("countFalse"),
        )

        return query.fetch_from(self.db_table, self.db_schema).build()

    def parse_results(self, result: list[dict]):
        """
        Result:
        [
            {
                "countTrue": 50,
                "countFalse": 40
            }
        ]
        """
        if len(result) > 0:
            return {
                self.columns[0].name: {
                    "countTrue": result[0]["countTrue"],
                    "countFalse": result[0]["countFalse"],
                }
            }

        return {
            self.columns[0].name: {
                "countTrue": 0,
                "countFalse": 0,
            }
        }

    def validate_query_results(self, parsed_results):
        validate = False
        if (
            parsed_results
            and isinstance(parsed_results, dict)
            and all(key in parsed_results for key in ["countTrue", "countFalse"])
        ):
            validate = True

        return validate

    def query_data_type(self) -> TranslateColDataType:
        return TranslateColDataType.BOOL
