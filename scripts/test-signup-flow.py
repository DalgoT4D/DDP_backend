import argparse
import requests
from faker import Faker
from testclient import TestClient

parser = argparse.ArgumentParser()
parser.add_argument("--port", default=8000)
args = parser.parse_args()

if __name__ == "__main__":
    faker = Faker("en-IN")

    accountmanager = TestClient(args.port)
    orguser = accountmanager.clientpost(
        "organizations/users/", json={"email": faker.email(), "password": "password"}
    )

    accountmanager.login(orguser["email"], "password")

    org = accountmanager.clientpost("organizations/", json={"name": faker.company()})

    print(org)
    