from ninja import Schema


class PrefectAirbyteSync(Schema):
    """Docstring"""

    blockname: str


class PrefectDbtCore(Schema):
    """Docstring"""

    blockname: str


class PrefectAirbyteConnectionSetup(Schema):
    """Docstring"""

    serverblockname: str
    connectionblockname: str
    connection_id: str


class PrefectDbtCoreSetup(Schema):
    """Docstring"""

    blockname: str
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
    working_dir: str
    env: dict


class OrgDbtSchema(Schema):
    """Docstring"""

    profile: DbtProfile
    credentials: DbtCredentialsPostgres  # todo can this be a union
    gitrepo_url: str
    dbtversion: str


class PrefectDbtRun(Schema):
    """Docstring"""

    profile: DbtProfile
    credentials: DbtCredentialsPostgres  # todo can this be a union

    dbt_blockname: str
