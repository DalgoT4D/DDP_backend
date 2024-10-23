import os
import uuid
import json
from typing import Union
from ninja.errors import HttpError
from ddpui.models.dbt_workflow import OrgDbtModel, DbtEdge, OrgDbtOperation
from ddpui.models.org_user import OrgUser
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.models.canvaslock import CanvasLock
from ddpui.schemas.dbt_workflow_schema import (
    CreateDbtModelPayload,
    EditDbtOperationPayload,
    GenerateGraphSchema,
)
from ddpui.utils.transform_workflow_helpers import (
    from_orgdbtoperation,
)
from ddpui.core import dbtautomation_service
from autogen import ConversableAgent, initiate_chats
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def validate_operation_config(
    payload: Union[CreateDbtModelPayload, EditDbtOperationPayload],
    target_model: OrgDbtModel,
    is_multi_input_op: bool,
    operation_chained_before: int = 0,
    edit: bool = False,
):
    """
    - Validate if the operation config has correct input(s)
    - Raises http errors if validation fails
    - Return ordered list of input models and the final config of operation

    return [final_config, all_input_models]
    """

    # if edit:
    #     current_operations_chained -= 1

    primary_input_model: OrgDbtModel = None  # the first input model
    other_input_models: list[OrgDbtModel] = []
    seq: list[int] = []
    other_input_columns: list[list[str]] = []

    if operation_chained_before == 0:
        if not payload.input_uuid:
            raise HttpError(422, "input is required")

        model = OrgDbtModel.objects.filter(uuid=payload.input_uuid).first()
        if not model:
            raise HttpError(404, "input not found")

        primary_input_model = model

    if is_multi_input_op:  # multi input operation
        if len(payload.other_inputs) == 0:
            raise HttpError(422, "atleast 2 inputs are required for this operation")

        payload.other_inputs.sort(key=lambda x: x.seq)

        for other_input in payload.other_inputs:
            model = OrgDbtModel.objects.filter(uuid=other_input.uuid).first()
            if not model:
                raise HttpError(404, "input not found")
            seq.append(other_input.seq)
            other_input_columns.append(other_input.columns)
            other_input_models.append(model)

    all_input_models = ([primary_input_model] if primary_input_model else []) + other_input_models

    logger.info(
        "passed all validation; moving to %s operation",
        {"update" if edit else "create"},
    )

    # source columns or selected columns
    # there will be atleast one input
    OP_CONFIG = payload.config
    OP_CONFIG["source_columns"] = payload.source_columns
    OP_CONFIG["other_inputs"] = []

    # in case of mutli input; send the rest of the inputs in the config; dbt_automation will handle the rest
    for dbtmodel, seq, columns in zip(other_input_models, seq, other_input_columns):
        OP_CONFIG["other_inputs"].append(
            {
                "input": {
                    "input_type": dbtmodel.type,
                    "input_name": dbtmodel.name,
                    "source_name": dbtmodel.source_name,
                },
                "source_columns": columns,
                "seq": seq,
            }
        )

    input_config = {
        "config": OP_CONFIG,
        "type": payload.op_type,
        "input_models": [
            {
                "uuid": str(model.uuid),
                "name": model.name,
                "display_name": model.display_name,
                "source_name": model.source_name,
                "schema": model.schema,
                "type": model.type,
            }
            for model in all_input_models
        ],
    }

    return (input_config, all_input_models)


def check_canvas_locked(requestor_orguser: OrgUser, lock_id: str):
    """
    Checks if the requestor user of an org can access the canvas or not
    Raises error if the canvas is not accessible
    """
    if os.getenv("CANVAS_LOCK") in [False, "False", "false"]:
        return True

    canvas_lock = CanvasLock.objects.filter(locked_by__org=requestor_orguser.org).first()
    if canvas_lock:
        if canvas_lock.locked_by != requestor_orguser:
            raise HttpError(403, f"canvas is locked by {canvas_lock.locked_by.user.email}")
        elif str(canvas_lock.lock_id) != lock_id:
            raise HttpError(
                403,
                "canvas is locked, looks like you are using another device to access the canvas",
            )
    else:
        raise HttpError(403, "acquire a canvas lock first")


def create_operation_node(
    org,
    orguser: OrgUser,
    payload: CreateDbtModelPayload,
):
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, payload.canvas_lock_id)

    if payload.op_type not in dbtautomation_service.OPERATIONS_DICT.keys():
        raise HttpError(422, "Operation not supported")

    is_multi_input_op = payload.op_type in ["join", "unionall"]

    target_model = None
    if payload.target_model_uuid:
        target_model = OrgDbtModel.objects.filter(uuid=payload.target_model_uuid).first()

    if not target_model:
        target_model = OrgDbtModel.objects.create(
            uuid=uuid.uuid4(),
            orgdbt=orgdbt,
            under_construction=True,
        )

    # only under construction models can be modified
    if not target_model.under_construction:
        raise HttpError(422, "model is locked")

    current_operations_chained = OrgDbtOperation.objects.filter(dbtmodel=target_model).count()

    final_config, all_input_models = validate_operation_config(
        payload, target_model, is_multi_input_op, current_operations_chained
    )

    # we create edges only with tables/models
    for source in all_input_models:
        edge = DbtEdge.objects.filter(from_node=source, to_node=target_model).first()
        if not edge:
            DbtEdge.objects.create(
                from_node=source,
                to_node=target_model,
            )

    output_cols = dbtautomation_service.get_output_cols_for_operation(
        org_warehouse, payload.op_type, final_config["config"].copy()
    )
    logger.info("creating operation")

    dbt_op = OrgDbtOperation.objects.create(
        dbtmodel=target_model,
        uuid=uuid.uuid4(),
        seq=current_operations_chained + 1,
        config=final_config,
        output_cols=output_cols,
    )

    logger.info("created operation")

    # save the output cols of the latest operation to the dbt model
    target_model.output_cols = dbt_op.output_cols
    target_model.save()

    logger.info("updated output cols for the model")

    return from_orgdbtoperation(dbt_op, chain_length=dbt_op.seq)


def chat_to_graph(org, orguser: OrgUser, payload: GenerateGraphSchema):
    """
    1. Converts the query to dbt sql statement
    2. Creates the model file on disk
    3. Validates the model
    4. Generates nodes and edges
    """

    config = payload.config

    prompt = config["query"]

    op_types = dbtautomation_service.OPERATIONS_DICT.keys()
    optypes_str = "\n".join(op_types)

    # custom prompts for each operation
    # op_prompts = {"dropcolumns": ""}

    llm_config = {"model": "gpt-4", "cache_seed": None, "api_key": os.getenv("OPENAI_API_KEY")}

    classify_optype_agent = ConversableAgent(
        name="classify_optype_agent",
        system_message=f"You will be given a user prompt that you need to classify into one of the sql operations present in {optypes_str}",
        llm_config=llm_config,
        human_input_mode="NEVER",
    )

    user_proxy_agent = ConversableAgent(
        name="user_proxy_agent",
        llm_config=False,
        human_input_mode="NEVER",
        code_execution_config=False,
    )

    chats = [
        {
            "sender": user_proxy_agent,
            "recipient": classify_optype_agent,
            "message": prompt,
            "summary_method": "reflection_with_llm",
            "summary_args": {
                "summary_prompt": f"Return the operation type as single word from the following list: {optypes_str}, Just give the response as a single type among the follwing and no other sentence",
            },
            "max_turns": 1,
            "clear_history": True,
        },
    ]
    result = initiate_chats(chats)

    # chat_result =   # Extract the first (and only) ChatResult object

    # Access the summary directly from the ChatResult
    operation_type = result[0].summary

    print(operation_type)

    payload.op_type = operation_type

    if operation_type == "dropcolumns":

        drop_columns = [
            {
                "sender": user_proxy_agent,
                "recipient": classify_optype_agent,
                "message": prompt,
                "summary_method": "reflection_with_llm",
                "summary_args": {
                    "summary_prompt": "Return the column names to perform operations in the following array format: ['column name 1', 'column name 2', 'column name 3']",
                },
                "max_turns": 1,
                "clear_history": True,
            }
        ]

        result1 = initiate_chats(drop_columns)
        print(result1)
        columns = result1[0].summary
        payload.config = {"columns": columns}

    elif operation_type == "aggregate":
        aggregate_columns = [
            {
                "sender": user_proxy_agent,
                "recipient": classify_optype_agent,
                "message": prompt,
                "summary_method": "reflection_with_llm",
                "summary_args": {
                    "summary_prompt": "From the following list: [avg, sum, min, max, count], identify the aggregation operation mentioned in the input. Respond with only the operation name.",
                },
                "max_turns": 1,
                "clear_history": True,
            },
            {
                "sender": user_proxy_agent,
                "recipient": classify_optype_agent,
                "message": prompt,
                "summary_method": "reflection_with_llm",
                "summary_args": {
                    "summary_prompt": "Extract the column name mentioned in the input for aggregation. If no column is mentioned, respond with 'none'.",
                },
                "max_turns": 1,
                "clear_history": True,
            },
            # Step 3: Generate the output column name
            {
                "sender": user_proxy_agent,
                "recipient": classify_optype_agent,
                "message": prompt,
                "summary_method": "reflection_with_llm",
                "summary_args": {
                    "summary_prompt": "Generate the output column name by combining the target column with the aggregation operation. Example: 'salary' + 'avg' -> 'salary_avg'. If no column is given, return 'invalid'.",
                },
                "max_turns": 1,
                "clear_history": True,
            },
        ]

        result = initiate_chats(aggregate_columns)

        # Extract the operation, column, and output column name
        operation = result[0].summary  # Aggregation operation
        column = result[1].summary  # Target column
        output_column_name = result[2].summary

        payload.config = config = {
            "aggregate_on": [
                {
                    "operation": operation,
                    "column": column,
                    "output_column_name": output_column_name,
                }
            ]
        }

    elif operation_type == "where":
        clauses_chat = [
            {
                "sender": user_proxy_agent,
                "recipient": classify_optype_agent,
                "message": prompt,
                "summary_method": "reflection_with_llm",
                "summary_args": {
                    "summary_prompt": "Extract the following details from the sentence:\n"
                    "1. The column name (e.g., age, salary).\n"
                    "2. The logical operator out of these ('between','=', '!=', '>=', '<=', '>', '<').\n"
                    "3. The operand value (mention whether it is a column value or a constant).\n"
                    "Respond in the following JSON format:\n"
                    "{ \"column\": \"<column_name>\", \"operator\": \"<logical_operator>\", \"operand_value\": \"<value>\", \"is_col\": <true_or_false> }",
                },
                "max_turns": 1,
                "clear_history": True,
            }
        ]
        
        clauses_result = initiate_chats(clauses_chat)
        clauses_data = clauses_result[0].summary  # Extracted clauses as JSON-like string
        print(f"Clauses: {clauses_data}")
        clauses = json.loads(clauses_data)


        payload.config = {
            "where_type": "and",
            "clauses": [
                {
                    "column": clauses["column"],
                    "operator": clauses["operator"],
                    "operand": {
                        "value": clauses["operand_value"],
                        "is_col": clauses["is_col"],
                    },
                }
            ],
        }

    # print(columns, operation_type)

    return create_operation_node(org, orguser, payload)
