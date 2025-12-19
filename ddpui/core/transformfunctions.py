import os

from typing import Union
from ninja.errors import HttpError
from ddpui.models.dbt_workflow import OrgDbtModel, DbtEdge, OrgDbtOperation
from ddpui.models.org_user import OrgUser
from ddpui.models.org import Org
from ddpui.models.canvaslock import CanvasLock
from ddpui.schemas.dbt_workflow_schema import (
    CreateDbtModelPayload,
    EditDbtOperationPayload,
)

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
