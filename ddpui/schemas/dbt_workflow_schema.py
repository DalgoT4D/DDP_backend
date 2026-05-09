from ninja import Field, Schema
from typing import Union, Any, Literal, Optional
from pydantic import ConfigDict, validator

from ddpui.models.dbt_workflow import OrgDbtModel
from ddpui.models.canvas_models import CanvasNode


class InputModelPayload(Schema):
    """
    Schema to be expected when we are creating models in a chain
    """

    uuid: str
    columns: list[str] = []
    seq: int = 1


class CreateDbtModelPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    config: dict
    op_type: str
    target_model_uuid: str = ""
    input_uuid: str = ""
    source_columns: list[str] = []
    other_inputs: list[InputModelPayload] = []
    canvas_lock_id: Optional[str] = None


class EditDbtOperationPayload(Schema):
    """
    schema to define the payload required to edit a dbt operation
    """

    config: dict
    op_type: str
    input_uuid: str = ""
    source_columns: list[str] = []
    other_inputs: list[InputModelPayload] = []
    canvas_lock_id: Optional[str] = None


class CompleteDbtModelPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    name: str
    display_name: str
    dest_schema: str
    canvas_lock_id: Optional[str] = None


class SyncSourcesSchema(Schema):
    """
    schema to sync sources from the schema
    """

    schema_name: Optional[str] = None
    source_name: Optional[str] = None


class LockCanvasResponseSchema(Schema):
    """schema representing lock on the ui4t canvas"""

    lock_token: str
    expires_at: str
    locked_by: str


# ==============================================================================
# UI4T V2 API - New unified architecture using CanvasNode and CanvasEdge
# ==============================================================================


# Operation-specific config schemas


class AggregateMetricConfig(Schema):
    """Schema for individual aggregation metric"""

    column: str
    operation: str
    output_column_name: str


class AggregateOperationConfig(Schema):
    """Config for aggregate operations"""

    aggregate_on: list[AggregateMetricConfig]


class ArithmeticOperand(Schema):
    """Schema for individual arithmetic operand"""

    is_col: bool
    value: Union[str, float, int]


class ArithmeticOperationConfig(Schema):
    """Config for arithmetic operations"""

    operator: Literal["add", "sub", "mul", "div"]
    operands: list[ArithmeticOperand]
    output_column_name: str


class CaseWhenClause(Schema):
    """Schema for individual case when clause"""

    column: str
    operator: str
    operands: list[ArithmeticOperand]
    then: ArithmeticOperand


class CaseWhenOperationConfig(Schema):
    """Config for case when operations"""

    when_clauses: list[CaseWhenClause]
    output_column_name: str
    case_type: Literal["simple", "advance"]
    else_clause: Optional[ArithmeticOperand] = None
    sql_snippet: str = ""


class CastDatatypeColumnConfig(Schema):
    """Schema for individual column in cast datatypes operation"""

    columnname: str
    columntype: str


class CastDatatypesOperationConfig(Schema):
    """Config for cast datatypes operations"""

    columns: list[CastDatatypeColumnConfig]


class CoalesceColumnsOperationConfig(Schema):
    """Config for coalesce columns operation"""

    columns: list[str]
    output_column_name: str
    default_value: Any = Field(
        None,
        description="Default value to use if all columns are NULL. If not provided, NULL will be used.",
    )


class ConcatColumnsOperationConfig(Schema):
    """Config for concat columns operation"""

    columns: list[str]
    output_column_name: str


class DropColumnOperationConfig(Schema):
    """Config for drop column operations"""

    columns: list[str]


class RenameColumnOperationConfig(Schema):
    """Config for rename column operations"""

    columns: dict[str, str]  # mapping of old column names to new column names


class FlattenJsonOperationConfig(Schema):
    """Config for flattening JSON columns"""

    json_column: str
    json_columns_to_copy: list[str]


class GenericColumnConfig(Schema):
    """Schema for individual generic column operation"""

    function_name: str
    operands: list[ArithmeticOperand]
    output_column_name: str


class GenericColumnOperationConfig(Schema):
    """Config for generic column operations"""

    computed_columns: list[GenericColumnConfig]


class GroupByOperationConfig(Schema):
    """Config for group by operations"""

    aggregate_on: list[AggregateMetricConfig]
    dimension_columns: list[str]


class JoinOnConditionConfig(Schema):
    """Schema for individual join on condition"""

    model_config = ConfigDict(str_strip_whitespace=True)

    key1: str
    key2: str
    compare_with: str

    @validator("compare_with")
    def validate_compare_with(cls, v):
        """Validate that the comparison operator is in the allowed whitelist"""
        allowed_operators = ["=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "ILIKE"]
        if v.upper() not in allowed_operators:
            raise ValueError(f"Operator {v} not allowed. Must be one of {allowed_operators}")
        return v.upper()


class JoinOperationConfig(Schema):
    """Config for join operations"""

    model_config = ConfigDict(str_strip_whitespace=True)

    join_type: Literal["inner", "left", "full outer"]
    join_on: Union[JoinOnConditionConfig, list[JoinOnConditionConfig]]

    @validator("join_on")
    def validate_join_on(cls, v):
        """Validate that the join_on list is not empty if provided as a list"""
        if isinstance(v, list) and len(v) == 0:
            raise ValueError("join_on list cannot be empty")
        return v


class UnionTablesOperationConfig(Schema):
    """Config for union tables operations"""

    pass


class PivotOperationConfig(Schema):
    """Config for pivot operations"""

    pivot_column_name: str
    pivot_column_values: list[str]
    groupby_columns: list[str]


class RawSqlOperationConfig(Schema):
    """Config for raw/generic SQL operations"""

    sql_statement_1: str  # select part
    sql_statement_2: str = ""  # optional where/group by etc.


class RegexExtractionOperationConfig(Schema):
    """Config for regex extraction operations"""

    columns: dict[str, str]  # mapping of column names to regex patterns


class ReplaceColOp(Schema):
    find: str
    replace: str


class ReplaceColumnValueConfig(Schema):
    """Schema for individual replace column value operation"""

    col_name: str
    output_column_name: str
    replace_ops: list[ReplaceColOp]


class ReplaceValueOperationConfig(Schema):
    """Config for replace value operations"""

    columns: list[ReplaceColumnValueConfig]


class UnpivotOperationConfig(Schema):
    """Config for unpivot operations"""

    exclude_columns: list[str] = []  # exclude from unpivot but keep in the resulting table
    unpivot_columns: list[str]  # columns to unpivot
    unpivot_field_name: str = "field_name"
    unpivot_value_name: str = "value"
    cast_to: Optional[str] = None  # datatype to cast the value column to


class FilterClauseConfig(Schema):
    """Schema for individual filter clause"""

    column: str
    operator: str
    operand: Optional[ArithmeticOperand] = None  # Optional for null operators


class WhereFilterOperationConfig(Schema):
    """Config for where filter operations"""

    where_type: Literal["and", "or", "sql"] = "and"
    clauses: list[FilterClauseConfig]
    sql_snippet: str = ""


op_config_mapping = {
    "aggregate": AggregateOperationConfig,
    "arithmetic": ArithmeticOperationConfig,
    "casewhen": CaseWhenOperationConfig,
    "castdatatypes": CastDatatypesOperationConfig,
    "coalescecolumns": CoalesceColumnsOperationConfig,
    "concat": ConcatColumnsOperationConfig,
    "dropcolumns": DropColumnOperationConfig,
    "renamecolumns": RenameColumnOperationConfig,
    "flattenjson": FlattenJsonOperationConfig,
    "generic": GenericColumnOperationConfig,
    "groupby": GroupByOperationConfig,
    "join": JoinOperationConfig,
    "unionall": UnionTablesOperationConfig,
    "pivot": PivotOperationConfig,
    "rawsql": RawSqlOperationConfig,
    "regexextraction": RegexExtractionOperationConfig,
    "replace": ReplaceValueOperationConfig,
    "unpivot": UnpivotOperationConfig,
    "where": WhereFilterOperationConfig,
}


def validate_operation_config_v2(op_type: str, config: dict) -> None:
    """Validate config based on operation type"""

    if op_type not in op_config_mapping:
        raise ValueError(f"Unsupported operation type: {op_type}")

    # Validate using the specific schema
    try:
        op_config_mapping[op_type](**config)
    except Exception as e:
        raise ValueError(f"Invalid config for {op_type} operation: {str(e)}") from e


# Operation-specific config schemas end here


class ModelSrcOtherInputPayload(Schema):
    """Schema to define inputs for a multi input operation. The uuid refers to the dbtmodel"""

    input_model_uuid: str
    columns: list[str] = []
    seq: int = 1


class ModelSrcInputsForMultiInputOp(Schema):
    """Schema to process inputs of multi input operations"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    seq: int
    src_model: OrgDbtModel


class SequencedNode(Schema):
    """
    Schema to process sequenced nodes
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    seq: int
    node: CanvasNode


class CreateOperationNodePayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    config: dict
    input_node_uuid: (
        str  # The CanvasNode (source/model/operation) on which this operation is applied
    )
    op_type: str
    source_columns: list[str]
    other_inputs: list[
        ModelSrcOtherInputPayload
    ] = []  # List of other CanvasNode inputs for multi-input operations
    canvas_lock_id: Optional[str] = None


class EditOperationNodePayload(Schema):
    """
    schema to define the payload required to edit a dbt operation
    """

    config: dict
    op_type: str
    source_columns: list[str] = []
    other_inputs: list[
        ModelSrcOtherInputPayload
    ] = []  # List of other CanvasNode inputs for multi-input operations


class TerminateChainAndCreateModelPayload(Schema):
    """
    schema to define the payload required to terminate an operation chain into a model
    """

    name: str
    display_name: str
    dest_schema: str
    rel_dir_to_models: Optional[str] = None
