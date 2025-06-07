from typing import Optional, Any, Literal
from pydantic import BaseModel, Field


class OperationInputSchema(BaseModel):
    pass


##################################################################################################################


class DropOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    columns: list[str] = Field(description="List of columns to drop", default=[])


##################################################################################################################


class RenameOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    columns: dict[str, str] = Field(description="Map of columns to rename", default={})


##################################################################################################################


class ConcatColumnSpec(BaseModel):
    is_col: bool = Field(description="Whether this is a column or a literal value")
    name: str = Field(description="Name of the column or literal value")


class ConcatOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    columns: list[ConcatColumnSpec] = Field(
        description="List of columns to concatenate", default=[]
    )
    output_column_name: str = Field(description="Name of the output concatenated column")


##################################################################################################################


class CoalesceOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    columns: list[str] = Field(description="List of columns to coalesce", default=[])
    output_column_name: str = Field(description="Name of the output coalesced column")
    default_value: Optional[Any] = Field(
        description="Default value if all columns are NULL", default=None
    )


##################################################################################################################


class CastColumnSpec(BaseModel):
    columntype: str = Field(description="Target column type")
    columnname: str = Field(description="Name of the column to cast")


class CastDataTypeOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    columns: list[CastColumnSpec] = Field(description="List of columns to cast", default=[])


##################################################################################################################


class ThenSpec(BaseModel):
    is_col: bool = Field(description="Whether this is a column or a literal value")
    value: str = Field(description="Value to use in the then clause")


class CaseWhenOperandSpec(BaseModel):
    is_col: bool = Field(description="Whether this is a column or a literal value")
    value: str = Field(description="Value to use in the operand clause")


class WhenClauseSpec(BaseModel):
    column: str = Field(description="Column to check")
    operator: str = Field(description="Operator to use for comparison")
    operands: list[CaseWhenOperandSpec] = Field(
        description="Operands for the operator; can be columns or literals", default=[]
    )
    then: ThenSpec = Field(description="Then clause", default={})


class CaseWhenOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    when_clauses: list[dict] = Field(description="List of when clauses", default=[])
    else_clause: dict = Field(description="Else clause", default=None)
    output_column_name: str = Field(description="Name of the output column", default="output_col")
    case_type: Literal["simple", "advance"] = Field(
        description="Type of case statement; advance requires the entire sql snippet of the case when",
        default="simple",
    )
    sql_snippet: str = Field(description="SQL snippet to use for the case statement", default="")


##################################################################################################################


class ArithmeticOperandSpec(BaseModel):
    is_col: bool = Field(description="Whether this is a column or a literal value")
    value: str = Field(description="Value to use in the operand clause")


class ArithmeticOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    operands: list[ArithmeticOperandSpec] = Field(description="List of operands", default=[])
    operator: Literal["add", "mul", "sub", "div"] = Field(description="Arithmetic operator to use")
    output_column_name: str = Field(description="Name of the output column", default="output_col")


##################################################################################################################


class AggregateOnSpec(BaseModel):
    operation: str = Field(description="Aggregate operation to perform")
    column: str = Field(description="Column to aggregate on")
    output_column_name: str = Field(description="Name of the output column")


class AggregateOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    aggregate_on: list[AggregateOnSpec] = Field(
        description="List of aggregate operations", default=[]
    )


##################################################################################################################


class FlattenJsonOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    json_column: str = Field(description="Name of the JSON column to flatten")
    json_columns_to_copy: list[str] = Field(
        description="List of columns to copy from the JSON column", default=[]
    )


##################################################################################################################


class GenericOperandSpec(BaseModel):
    is_col: bool = Field(description="Whether this is a column or a literal value")
    value: str = Field(description="Value to use in the operand clause")


class ComputedColumnSpec(BaseModel):
    function_name: str = Field(description="Name of the function to be used")
    operands: list[GenericOperandSpec] = Field(
        description="List of operands to be passed to the function", default=[]
    )
    output_column_name: str = Field(description="Name of the output column")


class GenericOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    computed_columns: list[ComputedColumnSpec] = Field(
        description="List of computed columns with function_name, operands, and output_column_name",
        default=[],
    )


##################################################################################################################


class GroupbyAggregateOnSpec(BaseModel):
    operation: str = Field(description="Aggregate operation to perform")
    column: str = Field(description="Column to aggregate on")
    output_column_name: str = Field(description="Name of the output column")


class GroupByOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    aggregate_on: list[GroupbyAggregateOnSpec] = Field(
        description="List of aggregate operations", default=[]
    )


##################################################################################################################


class JoinOnSpec(BaseModel):
    key1: str = Field(description="Key from the left table")
    key2: str = Field(description="Key from the right table")
    compare_with: str = Field(description="Comparison operator")


# TODO: need to think more about this
class JoinOperationInputSchema(OperationInputSchema):
    source_columns: list[str] = Field(description="List of input columns", default=[])
    join_type: Literal["inner", "left", "right", "full"] = Field(
        description="Type of join to perform", default="inner"
    )
    join_table: str = Field(description="Name of the table to join with")
    join_on: JoinOnSpec = Field(
        description="Join condition with key1, key2, and comparison operator",
        default={},
    )
