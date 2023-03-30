import argparse
from testclient import ClientTester

import os
from dotenv import load_dotenv
load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument('--port', default=8000)
parser.add_argument('--email', required=True)
parser.add_argument('--password', required=True)
args = parser.parse_args()

tester = ClientTester(args.port)
tester.login(args.email, args.password)

DBT_TARGETCONFIGS_TYPE = os.getenv('TESTING_DBT_TARGETCONFIGS_TYPE')
DBT_TARGETCONFIGS_SCHEMA = os.getenv('TESTING_DBT_TARGETCONFIGS_SCHEMA')
DBT_CREDENTIALS_USERNAME = os.getenv('TESTING_DBT_CREDENTIALS_USERNAME')
DBT_CREDENTIALS_PASSWORD = os.getenv('TESTING_DBT_CREDENTIALS_PASSWORD')
DBT_CREDENTIALS_DATABASE = os.getenv('TESTING_DBT_CREDENTIALS_DATABASE')
DBT_CREDENTIALS_HOST = os.getenv('TESTING_DBT_CREDENTIALS_HOST')
DBT_TEST_REPO = os.getenv('TESTING_DBT_TEST_REPO')

r = tester.clientpost('dbt/create/', json={
  'gitrepo_url': DBT_TEST_REPO,
  'dbtversion': "1.4.5",
  'targetname': "dev",
  'targettype': DBT_TARGETCONFIGS_TYPE,
  'targetschema': DBT_TARGETCONFIGS_SCHEMA,
  'host': DBT_CREDENTIALS_HOST,
  'port': "5432",
  'username': DBT_CREDENTIALS_USERNAME,
  'password': DBT_CREDENTIALS_PASSWORD,
  'database': DBT_CREDENTIALS_DATABASE,
})

print(r)

tester.clientpost('dbt/delete/')
