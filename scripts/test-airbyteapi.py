import argparse
from time import sleep
from faker import Faker
from dotenv import load_dotenv
from testclient import TestClient

load_dotenv(".env.test")

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument(
    "-p", "--port", required=True, help="port where app server is listening"
)
args = parser.parse_args()

faker = Faker("en-IN")
tester = TestClient(args.port, verbose=args.verbose)
email = faker.email()
password = faker.password()
tester.clientpost("organizations/users/", json={"email": email, "password": password})
tester.login(email, password)
tester.clientget("currentuser")
tester.clientpost(
    "organizations/",
    json={
        "name": faker.company()[:20],
    },
)
# tester.clientdelete("organizations/warehouses/")

res1 = tester.clientpost(
    "airbyte/sources/",
    json={
        "name": "postgres",
        "sourceDefId": "decd338e-5647-4c0b-adf4-da0e75f5a750",
        "config": {
            "host": "dev-test.c4hvhyuxrcet.ap-south-1.rds.amazonaws.com",
            "port": 5432,
            "database": "src_ngo1",
            "username": "someuser",
            "password": "somepassword",
        },
    },
)


assert "task_id" in res1
task_id = res1["task_id"]

while True:
    sleep(5)
    res_poll = tester.clientget("tasks/" + task_id)
    if res_poll.get("detail") == "no such task id":
        continue
    if res_poll.get("progress"):
        # print(res_poll["progress"][-1]["message"])
        if "logs" in res_poll["progress"][-1]:
            for logmessage in res_poll["progress"][-1]["logs"]:
                print(logmessage)
        if res_poll["progress"][-1]["status"] in ["failed", "completed"]:
            break
