import requests
import argparse
import sys
from faker import Faker
from testclient import ClientTester

parser = argparse.ArgumentParser()
parser.add_argument('--port', default=8000)
parser.add_argument('--email', required=True)
parser.add_argument('--password', required=True)
args = parser.parse_args()

tester = ClientTester(args.port)

tester.login(args.email, args.password)
r = tester.clientpost('dbt/create/', json={
  'gitrepo_url': "https://github.com/DevDataPlatform/dbt_basictest.git",
  'project_dir': "/Users/fatchat/openbrackets/ddp/dbt",
  'dbtversion': "1.4.5",
  'targetname': "dev",
  'targettype': "postgres",
  'targetschema': "public",
  'host': "dev-test.c4hvhyuxrcet.ap-south-1.rds.amazonaws.com",
  'port': "5432",
  'username': "usr_sim_warehouse",
  'password': "j7gqxhrRPBTthSI",
  'database': "sim_warehouse",
})

print(r)

tester.clientpost('dbt/delete/')
