import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ddpui.settings')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import argparse
import json
from ddpui import prefectapi, prefectschemas

parser = argparse.ArgumentParser()
args = parser.parse_args()



# == airbyte ==
x = prefectapi.get_blocktype(prefectapi.AIRBYTESERVER)
print("airbyte server block type = " + x['id'])

x = prefectapi.get_blockschematype(prefectapi.AIRBYTESERVER)
print("airbyte server block schema type = " + x['id'])

airbyte_blockname = 'absrvr-unittest'
x = prefectapi.get_block(prefectapi.AIRBYTESERVER, airbyte_blockname)
if x:
  print(f"found airbyte server block {airbyte_blockname}, deleting")
  prefectapi.delete_airbyteserver_block(x['id'])
else:
  print(f"no airbyte server block named {airbyte_blockname}, creating")

prefectapi.create_airbyteserver_block(airbyte_blockname)

x = prefectapi.get_block(prefectapi.AIRBYTESERVER, airbyte_blockname)
if x:
  print(f"creation success: found airbyte server block {airbyte_blockname}")
else:
  raise Exception(f"no airbyte server block named {airbyte_blockname}")

prefectapi.delete_airbyteserver_block(x['id'])

x = prefectapi.get_block(prefectapi.AIRBYTESERVER, airbyte_blockname)
if x:
  raise Exception(f"found airbyte server block {airbyte_blockname} after deletion")
else:
  print(f"deletion success: no airbyte server block named {airbyte_blockname}")



# == shell ==
x = prefectapi.get_blocktype(prefectapi.SHELLOPERATION)
print("shell operation block type = " + x['id'])

x = prefectapi.get_blockschematype(prefectapi.SHELLOPERATION)
print("shell operation block schema type = " + x['id'])

shellop_blockname = 'shellop-unittest'
x = prefectapi.get_block(prefectapi.SHELLOPERATION, shellop_blockname)
if x:
  print(f"found shell operation block {shellop_blockname}, deleting")
  prefectapi.delete_shell_block(x['id'])
else:
  print(f"no shell operation block named {shellop_blockname}, creating")

shellop_blockdata = prefectschemas.PrefectShellSetup(
  blockname=shellop_blockname,
  working_dir="WORKING_DIR",
  env={"VAR": "VAL"},
  commands=[
    "shellcmd1", "shellcmd2"
  ]
)
prefectapi.create_shell_block(shellop_blockdata)

x = prefectapi.get_block(prefectapi.SHELLOPERATION, shellop_blockname)
if x:
  print(f"creation success: found shell operation block {shellop_blockname}")
  print("  working_dir= " + x['data']['working_dir'])
  print("  commands= " + json.dumps(x['data']['commands']))
else:
  raise Exception(f"no shell operation block named {shellop_blockname}")

prefectapi.delete_shell_block(x['id'])

x = prefectapi.get_block(prefectapi.SHELLOPERATION, shellop_blockname)
if x:
  raise Exception(f"found shell operation block {shellop_blockname} after deletion")
else:
  print(f"deletion success: no shell operation block named {shellop_blockname}")





# == dbt ==
x = prefectapi.get_blocktype(prefectapi.DBTCORE)
print("dbt core block type = " + x['id'])

x = prefectapi.get_blockschematype(prefectapi.DBTCORE)
print("dbt core block schema type = " + x['id'])

dbt_blockname = 'dbt-unittest'
x = prefectapi.get_block(prefectapi.DBTCORE, dbt_blockname)
if x:
  print(f"found dbt core block {dbt_blockname}, deleting")
  prefectapi.delete_dbtcore_block(x['id'])
else:
  print(f"no dbt core block named {dbt_blockname}, creating")

blockdata = prefectschemas.PrefectDbtCoreSetup(
  blockname=dbt_blockname,
  profiles_dir="PROFILES_DIR",
  project_dir="PROJECT_DIR",
  working_dir="WORKING_DIR",
  env={"VAR":"VAL"},
  commands=["cmd1", "cmd2"]
)
prefectapi.create_dbtcore_block(blockdata)

x = prefectapi.get_block(prefectapi.DBTCORE, dbt_blockname)
if x:
  print(f"creation success: found dbt core block {dbt_blockname}")
  print("  working_dir= " + x['data']['working_dir'])
  print("  profiles_dir= " + x['data']['profiles_dir'])
  print("  project_dir= " + x['data']['project_dir'])
  print("  commands= " + json.dumps(x['data']['commands']))

else:
  raise Exception(f"no dbt core block named {dbt_blockname}")

prefectapi.delete_dbtcore_block(x['id'])

x = prefectapi.get_block(prefectapi.DBTCORE, dbt_blockname)
if x:
  raise Exception(f"found dbt core block {dbt_blockname} after deletion")
else:
  print(f"deletion success: no dbt core block named {dbt_blockname}")


