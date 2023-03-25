from ninja import Schema

class PrefectAirbyteSync(Schema):
  flowname: str
  connection_id: str
  
class PrefectDbtCoreSetup(Schema):
  blockname: str
  profiles_dir: str
  project_dir: str
  working_dir: str
  env: dict
  commands: list

class DbtProfile(Schema):
  name: str
  target: str
  target_configs_type: str
  target_configs_schema: str

class DbtCredentialsPostgres(Schema):
  host: str
  port: str
  username: str
  password: str
  database: str

class PrefectShellSetup(Schema):
  blockname: str
  commands: list
  working_dir: str
  env: dict