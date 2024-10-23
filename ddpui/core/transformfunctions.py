import os
import json
from typing import Union
from ninja.errors import HttpError
from ddpui.models.dbt_workflow import OrgDbtModel, DbtEdge, OrgDbtOperation
from ddpui.models.org_user import OrgUser
from ddpui.models.org import Org
from ddpui.models.canvaslock import CanvasLock
from ddpui.schemas.dbt_workflow_schema import (
    CreateDbtModelPayload,
    EditDbtOperationPayload,
    GenerateGraphSchema,
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


def chat_to_graph(payload):
    """
    1. Converts the query to dbt sql statement
    2. Creates the model file on disk
    3. Validates the model
    4. Generates nodes and edges
    """

    if "config" not in payload and "query" not in payload["config"]:
        raise HttpError(422, "query is required")

    query = payload["config"]["query"]

    def is_valid_json(message):
        try:
            print(message)
            json.loads(message.get("content", ""))
            return True
        except ValueError as e:
            print(f"Invalid JSON: {e}")
            return False

    op_types = dbtautomation_service.OPERATIONS_DICT.keys()
    optypes_str = "\n".join(op_types)

    # custom prompts for each operation
    op_prompts = {"dropcolumns": ""}

    llm_config = {"model": "gpt-4", "cache_seed": None, "api_key": os.getenv("OPENAI_API_KEY")}

    classify_optype_agent = ConversableAgent(
        name="classify_optype_agent",
        system_message=f"You will be given a user prompt that you need to classify into one of the sql operations present in {optypes_str}",
        llm_config=llm_config,
        human_input_mode="NEVER",
    )

    # sql_agent = ConversableAgent(
    #     name="sql_agent",
    #     system_message="You are very good at converting text to a sql query",
    #     llm_config=llm_config,
    #     human_input_mode="NEVER",  # "ALWAYS", "NEVER", "TERMINATE"
    # )

    dbt_agent = ConversableAgent(
        name="dbt_agent",
        system_message="You are very good at converting sql query to a dbt model",
        llm_config=llm_config,
        human_input_mode="NEVER",
        # is_termination_msg=is_valid_json,
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
            "message": payload.query,
            "summary_method": "reflection_with_llm",
            "summary_args": {
                "summary_prompt": f"Return the operation type as single word from the following list: {optypes_str}",
            },
            "summary_args": {
                "summary_prompt": f"""Return the following things as JSON object only\n
                1. operation type from the following list: {optypes_str} 2. which columns to drop in list array
                """
                + "with the following JSON schema: {'operation_type': '', 'columns_to_drop': []}",
            },
            "max_turns": 1,
            "clear_history": True,
        },
        {
            "sender": classify_optype_agent,
            "recipient": dbt_agent,
            "message": "The operation is to be performed on the following source/reference model. "
            "source columns: ['col1', 'col2', 'col3', 'col4'] \n"
            "model_name: 'table2' \n"
            "Using the operation type, return the dbt sql model information.",
            "summary_method": "reflection_with_llm",
            "summary_args": {
                "summary_prompt": "Return the dbt sql model information with materialized as table. Return 3 things 1. sql file content in one single line 2. instructions 3. model name"
                "into JSON object only: "
                "{'sql_file_content': '', 'instructions': '', model_file_name: ''}",
            },
            "max_turns": 1,
            "clear_history": True,
        },
    ]
    result = initiate_chats(chats)

    # create operation node
    print("creating op node")
    print(result[0])

    # operation node

    return result[-1].summary.sql_file_contentp
