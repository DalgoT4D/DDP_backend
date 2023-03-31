import os
import requests

from dotenv import load_dotenv
load_dotenv()

from testclient import TestClient

tester = TestClient(8002)

tester.login('user1@ddp', 'password')
tester.clientget('currentuser')

TESTING_DBT_TEST_REPO = os.getenv('TESTING_DBT_TEST_REPO')

DBT_PROFILE = os.getenv('TESTING_DBT_PROFILE')
DBT_TARGETCONFIGS_TYPE = os.getenv('TESTING_DBT_TARGETCONFIGS_TYPE')
DBT_TARGETCONFIGS_SCHEMA = os.getenv('TESTING_DBT_TARGETCONFIGS_SCHEMA')
DBT_CREDENTIALS_USERNAME = os.getenv('TESTING_DBT_CREDENTIALS_USERNAME')
DBT_CREDENTIALS_PASSWORD = os.getenv('TESTING_DBT_CREDENTIALS_PASSWORD')
DBT_CREDENTIALS_DATABASE = os.getenv('TESTING_DBT_CREDENTIALS_DATABASE')
DBT_CREDENTIALS_HOST = os.getenv('TESTING_DBT_CREDENTIALS_HOST')
DBT_TEST_REPO = os.getenv('TESTING_DBT_TEST_REPO')

if False:
  tester.clientpost('dbt/deleteworkspace/')
  r = tester.clientpost('dbt/createworkspace/', json={
    'gitrepo_url': DBT_TEST_REPO,
    'dbtversion': "1.4.5",
    'profile': {
      "name": DBT_PROFILE,
      "target": "dev",
      "target_configs_type": DBT_TARGETCONFIGS_TYPE,
      "target_configs_schema": DBT_TARGETCONFIGS_SCHEMA,
    },
    'credentials': {
      'host': DBT_CREDENTIALS_HOST,
      'port': "5432",
      'username': DBT_CREDENTIALS_USERNAME,
      'password': DBT_CREDENTIALS_PASSWORD,
      'database': DBT_CREDENTIALS_DATABASE,
    },
  })

if False:
  tester.clientpost('dbt/git-pull/')

r = tester.clientpost('prefect/createdbtrunblock/', json={
  'dbt_blockname': 'test-blockname',
  'profile': {
    "name": DBT_PROFILE,
    "target": "dev",
    "target_configs_type": DBT_TARGETCONFIGS_TYPE,
    "target_configs_schema": DBT_TARGETCONFIGS_SCHEMA,
  },
  'credentials': {
    'host': DBT_CREDENTIALS_HOST,
    'port': "5432",
    'username': DBT_CREDENTIALS_USERNAME,
    'password': DBT_CREDENTIALS_PASSWORD,
    'database': DBT_CREDENTIALS_DATABASE,
  },
})
block_id = r['id']

tester.clientpost('prefect/createdbtcorejob/', json={
  'blockname': 'test-blockname'
})

# tester.clientdelete(f"prefect/dbtrunblock/{block_id}")

