from typing import Optional
from datetime import datetime

from ninja import Field, Schema


class PrefectAirbyteSync(Schema):
    """
    request payload to trigger an airbyte sync in prefect
    by specifying the prefect blockname
    """

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
    """
    request payload to trigger a dbt core op flow in prefect
    by specifying the prefect blockname
    """

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
    """
    create an airbyte connection block in prefect
    after creating the connection in airbyte
    """

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


class PrefectAirbyteRefreshSchemaTaskSetup(Schema):
    slug: str
    airbyte_server_block: str
    connection_id: str
    timeout: int
    type: str
    orgtask_uuid: str
    flow_name: str = None
    flow_run_name: str = None
    seq: int = 0
    catalog_diff: dict

    def to_json(self):
        """JSON serialization"""
        return {
            "slug": self.slug,
            "airbyte_server_block": self.airbyte_server_block,
            "connection_id": self.connection_id,
            "timeout": self.timeout,
            "type": self.type,
            "orgtask_uuid": self.orgtask_uuid,
            "flow_name": self.flow_name,
            "flow_run_name": self.flow_run_name,
            "seq": self.seq,
            "catalog_diff": self.catalog_diff,
        }


class PrefectAirbyteSyncTaskSetup(Schema):
    """task config payload in prefect for a airbyte sync task"""

    slug: str
    airbyte_server_block: str
    connection_id: str
    timeout: int
    type: str
    orgtask_uuid: str
    flow_name: str = None
    flow_run_name: str = None
    seq: int = 0

    def to_json(self):
        """JSON serialization"""
        return {
            "slug": self.slug,
            "airbyte_server_block": self.airbyte_server_block,
            "connection_id": self.connection_id,
            "timeout": self.timeout,
            "type": self.type,
            "orgtask_uuid": self.orgtask_uuid,
            "flow_name": self.flow_name,
            "flow_run_name": self.flow_run_name,
            "seq": self.seq,
        }


class PrefectAirbyteClearStreamsTaskSetup(Schema):
    """
    task config payload in prefect for a airbyte clear streams task
    """

    slug: str
    airbyte_server_block: str
    connection_id: str
    timeout: int
    type: str
    orgtask_uuid: str
    flow_name: str = None
    flow_run_name: str = None
    seq: int = 0
    streams: list[dict]

    def to_json(self):
        """JSON serialization"""
        return {
            "slug": self.slug,
            "airbyte_server_block": self.airbyte_server_block,
            "connection_id": self.connection_id,
            "timeout": self.timeout,
            "type": self.type,
            "orgtask_uuid": self.orgtask_uuid,
            "flow_name": self.flow_name,
            "flow_run_name": self.flow_run_name,
            "seq": self.seq,
            "streams": self.streams,
        }


class PrefectDbtTaskSetup(Schema):
    """
    request payload to trigger a dbt task (deps, clean, run, test) in prefect
    """

    type: str
    slug: str
    profiles_dir: str
    project_dir: str
    working_dir: str
    orgtask_uuid: str
    env: dict
    commands: list
    cli_profile_block: str
    cli_args: list = []
    flow_name: str = None
    flow_run_name: str = None
    seq: int = 0

    def to_json(self):
        """JSON serialization"""
        return {
            "type": self.type,
            "slug": self.slug,
            "profiles_dir": self.profiles_dir,
            "project_dir": self.project_dir,
            "working_dir": self.working_dir,
            "orgtask_uuid": self.orgtask_uuid,
            "env": self.env,
            "commands": self.commands,
            "cli_profile_block": self.cli_profile_block,
            "cli_args": self.cli_args,
            "flow_name": self.flow_name,
            "flow_run_name": self.flow_run_name,
            "seq": self.seq,
        }


class PrefectDbtCloudTaskSetup(Schema):
    """request payload to trigger a dbt cloud run task in prefect"""

    type: str
    slug: str
    dbt_cloud_job_id: int
    dbt_cloud_creds_block: str
    orgtask_uuid: str
    seq: int = 0

    def to_json(self):
        """JSON serialization"""
        return {
            "type": self.type,
            "slug": self.slug,
            "dbt_cloud_job_id": self.dbt_cloud_job_id,
            "dbt_cloud_creds_block": self.dbt_cloud_creds_block,
            "orgtask_uuid": self.orgtask_uuid,
            "seq": self.seq,
        }


class DbtProfile(Schema):
    """Docstring"""

    name: str
    # target_configs_type: this is now orgwarehouse.wtype
    target_configs_schema: str


class PrefectShellSetup(Schema):
    """Docstring"""

    blockname: str
    commands: list
    workingDir: str
    env: dict


class PrefectShellTaskSetup(Schema):
    """Docstring"""

    type: str
    slug: str
    commands: list
    working_dir: str
    env: dict
    orgtask_uuid: str
    flow_name: str = None
    flow_run_name: str = None
    seq: int = 0

    def to_json(self):
        """JSON serialization"""
        return {
            "seq": self.seq,
            "type": self.type,
            "commands": self.commands,
            "working_dir": self.working_dir,
            "env": self.env,
            "orgtask_uuid": self.orgtask_uuid,
            "slug": self.slug,
            "flow_name": self.flow_name,
            "flow_run_name": self.flow_run_name,
        }


class PrefectSecretBlockCreate(Schema):
    """Docstring"""

    secret: str
    block_name: str


class PrefectSecretBlockEdit(Schema):
    """Docstring"""

    secret: str
    block_name: str


class OrgDbtSchema(Schema):
    """Docstring"""

    profile: DbtProfile
    gitrepoUrl: str
    gitrepoAccessToken: Optional[str]


class OrgDbtGitHub(Schema):
    """Docstring"""

    gitrepoUrl: str
    gitrepoAccessToken: Optional[str]


class OrgDbtConnectGitRemote(Schema):
    """Schema for connecting an existing local git repo to a remote GitHub URL"""

    gitrepoUrl: str
    gitrepoAccessToken: str  # Required - PAT is mandatory for this endpoint


class OrgDbtTarget(Schema):
    """Docstring"""

    target_configs_schema: str


class PrefectDbtRun(Schema):
    """Docstring"""

    profile: DbtProfile


class PrefectAirbyteConnectionBlockSchema(Schema):
    """
    Return necessary details of connection block in prefect,
    airbyte and your database
    """

    name: str
    blockId: str
    blockName: str
    blockData: dict
    connectionId: str
    source: dict
    destination: dict
    catalogId: str
    syncCatalog: dict
    status: str
    deploymentId: str = None
    lastRun: Optional[dict | None]
    destinationSchema: str = ""
    lock: Optional[dict | None]


class PrefectFlowAirbyteConnection(Schema):
    """Validate the airbyte connection object in flow/pipeline create"""

    blockName: str
    seq: int


class PrefectFlowAirbyteConnection2(Schema):
    """Validate the airbyte connection object in flow/pipeline create"""

    id: str
    seq: int


class PrefectDataFlowCreateSchema3(Schema):
    """Payload to be sent to the prefect-proxy to go away with prefect blocks"""

    deployment_name: str
    flow_name: str
    orgslug: str
    deployment_params: dict
    cron: str = None


class PrefectDataFlowOrgTasks(Schema):
    """Org tasks related to the data flow"""

    uuid: str
    seq: int


class PrefectDataFlowCreateSchema4(Schema):
    """Payload sent by the frontend to create a dataflow"""

    name: str
    connections: list[PrefectFlowAirbyteConnection2]
    cron: str
    transformTasks: list[PrefectDataFlowOrgTasks]


class PrefectDataFlowUpdateSchema3(Schema):
    """Edit the data flow"""

    name: str = None
    connections: list[PrefectFlowAirbyteConnection2] = None
    cron: str = None
    transformTasks: list[PrefectDataFlowOrgTasks] = None
    deployment_params: dict = None


class PrefectFlowRunSchema(Schema):
    """Schema for field of a flow run fetched"""

    id: str
    name: str
    deployment_id: str
    flow_id: str
    state_type: str
    state_name: str


class StateSchema(Schema):
    """Schema to define the structure of the state field"""

    name: str = Field(..., description="The name of the state")
    type: str = Field(..., description="The type of the state")


class TaskStateSchema(Schema):
    """Schema to represent the state and force parameters"""

    state: StateSchema = Field(..., description="State with name and type")
    force: bool = Field(..., description="Force action flag")


class DeploymentRunTimes(Schema):
    """Schema for run times (seconds) of a deployment"""

    max_run_time: int = 0
    min_run_time: int = 0
    avg_run_time: int = 0
    wt_avg_run_time: int = 0


class FilterLateFlowRunsRequest(Schema):
    """search flow runs"""

    deployment_id: str = None
    work_pool_name: str = None
    work_queue_name: str = None
    limit: int = 1
    before_start_time: str = None
    after_start_time: str = None
    exclude_flow_run_ids: list[str] = []


class DeploymentCurrentQueueTime(Schema):
    """Queue time (seconds)"""

    queue_no: int
    max_wait_time: int
    min_wait_time: int


class PrefectGetDataflowsResponse(Schema):
    """Response schema for fetching dataflows"""

    name: str
    status: bool
    deploymentName: str
    deploymentId: str
    cron: Optional[str]
    lastRun: Optional[dict] = None
    lock: Optional[dict] = None
    queuedFlowRunWaitTime: DeploymentCurrentQueueTime = None
