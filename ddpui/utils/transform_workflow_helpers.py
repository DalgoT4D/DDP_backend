"""
Helpers related to UI for transformation feature
"""

from ddpui.models.dbt_workflow import (
    OrgDbtModel,
    OrgDbtOperation,
    DbtEdge,
    OrgDbtNodeType,
)


def from_orgdbtoperation(
    orgdbt_op: OrgDbtOperation, chain_length: int = None, **kwargs
):
    """Helper to turn an OrgDbtOperation into a dict"""
    dbtop = {
        "id": orgdbt_op.uuid,
        "output_cols": orgdbt_op.output_cols,
        "config": orgdbt_op.config,
        "type": OrgDbtNodeType.OPERATION_NODE,
        "target_model_id": orgdbt_op.dbtmodel.uuid,
        "target_model_name": orgdbt_op.dbtmodel.name,
        "target_model_schema": orgdbt_op.dbtmodel.schema,
        "seq": orgdbt_op.seq,
        "chain_length": chain_length,
    }

    if not dbtop["chain_length"]:
        dbtop["chain_length"] = OrgDbtOperation.objects.filter(
            dbtmodel=orgdbt_op.dbtmodel
        ).count()

    dbtop["is_last_in_chain"] = dbtop["seq"] == dbtop["chain_length"]

    dbtop.update(kwargs)

    return dbtop


def from_orgdbtmodel(orgdbt_model: OrgDbtModel):
    """
    Helper to turn an OrgDbtModel into a dict
    """

    return {
        "id": orgdbt_model.uuid,
        "source_name": orgdbt_model.source_name,
        "input_name": orgdbt_model.name,
        "input_type": orgdbt_model.type,
        "schema": orgdbt_model.schema,
        "type": OrgDbtNodeType.SRC_MODEL_NODE,
    }
