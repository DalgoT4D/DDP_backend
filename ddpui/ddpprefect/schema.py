from typing import Optional
from ninja import Schema


class PrefectAirbyteSync(Schema):
    """request payload to trigger an airbyte sync in prefect by specifying the prefect blockname"""

    blockName: str
    flowName: str = None
    flowRunName: str = None

    def to_json(self):
        """JSON serialization"""
        return {
            "blockName": self.blockName,
            "flowName": self.flowName,
            "flowRunName": self.flowRunName,
        }


class PrefectDbtCore(Schema):
    """request payload to trigger a dbt core op flow in prefect by specifying the prefect blockname"""

    blockName: str
    flowName: str = None
    flowRunName: str = None

    def to_json(self):
        """JSON serialization"""
        return {
            "blockName": self.blockName,
            "flowName": self.flowName,
            "flowRunName": self.flowRunName,
        }


class PrefectAirbyteConnectionSetup(Schema):
    """create an airbyte connection block in prefect after creating the connection in airbyte"""

    serverBlockName: str
    connectionBlockName: str
    connectionId: str


class PrefectDbtCoreSetup(Schema):
    """Docstring"""

    block_name: str
    profiles_dir: str
    project_dir: str
    working_dir: str
    env: dict
    commands: list


class DbtProfile(Schema):
    """Docstring"""

    name: str
    target: str
    # target_configs_type: this is now orgwarehouse.wtype
    target_configs_schema: str


class PrefectShellSetup(Schema):
    """Docstring"""

    blockname: str
    commands: list
    workingDir: str
    env: dict


class OrgDbtSchema(Schema):
    """Docstring"""

    profile: DbtProfile
    gitrepoUrl: str
    gitrepoAccessToken: Optional[str]
    dbtVersion: str


class PrefectDbtRun(Schema):
    """Docstring"""

    profile: DbtProfile


class PrefectAirbyteConnectionBlockSchema(Schema):
    """Return necessary details of connection block in prefect, airbyte and your database"""

    name: str
    blockId: str
    blockName: str
    blockData: dict
    connectionId: str
    sourceId: str
    destinationId: str
    sourceCatalogId: str
    syncCatalog: dict
    status: str


class PrefectFlowAirbyteConnection(Schema):
    """Validate the airbyte connection object in flow/pipeline create"""

    blockName: str
    seq: int


class PrefectDataFlowCreateSchema(Schema):
    """Payload sent by the frontend to create a dataflow"""

    name: str
    connectionBlocks: list[PrefectFlowAirbyteConnection]
    dbtTransform: str
    cron: str


class PrefectDataFlowCreateSchema2(Schema):
    """Payload to be sent to the prefect-proxy"""

    deployment_name: str
    flow_name: str
    orgslug: str
    connection_blocks: list
    dbt_blocks: list
    cron: str
