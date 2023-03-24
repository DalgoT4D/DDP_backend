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

class PrefectShellSetup(Schema):
  blockname: str
  commands: list
  working_dir: str
  env: dict