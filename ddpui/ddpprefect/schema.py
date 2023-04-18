from typing import Optional
from ninja import Schema


class PrefectAirbyteSync(Schema):
    """Docstring"""

    blockName: str


class PrefectDbtCore(Schema):
    """Docstring"""

    blockName: str


class PrefectAirbyteConnectionSetup(Schema):
    """Docstring"""

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
    target_configs_type: str
    target_configs_schema: str


class DbtCredentialsPostgres(Schema):
    """Docstring"""

    host: str
    port: str
    username: str
    password: str
    database: str


class PrefectShellSetup(Schema):
    """Docstring"""

    blockname: str
    commands: list
    workingDir: str
    env: dict


class OrgDbtSchema(Schema):
    """Docstring"""

    profile: DbtProfile
    credentials: DbtCredentialsPostgres  # todo can this be a union
    gitrepoUrl: str
    gitrepoAccessToken: Optional[str]
    dbtVersion: str


class PrefectDbtRun(Schema):
    """Docstring"""

    profile: DbtProfile
    credentials: DbtCredentialsPostgres  # todo can this be a union


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
