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

    def query_id(self) -> str:
        return f"bool-queryid-{self.columns[0].name}"

    def generate_sql(self):
        """
        Returns a sqlalchemy query ready to be executed by an engine
        Computes basic stats
        """
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
        Parses the result from the above executed sql query
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
        """
        Validate the parsed results of the query
        This function assumes the parsed_results sent is for a single column
        """
        validate = False
        if (
            parsed_results
            and isinstance(parsed_results, dict)
            and all(key in parsed_results for key in ["countTrue", "countFalse"])
        ):
            validate = True

        return validate
