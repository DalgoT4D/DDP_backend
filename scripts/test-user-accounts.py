import argparse
import requests
from faker import Faker
from testclient import TestClient

parser = argparse.ArgumentParser()
parser.add_argument("--admin-email", required=True)
parser.add_argument("--admin-password", required=True)
parser.add_argument("--port", default=8000)
args = parser.parse_args()


# ========================================================================================================================
class AdminTester:
    """utility class to make http requests to the /adminapi/ set of endpoints"""

    def __init__(self, adminuseremail, adminuserpassword):
        # get bearer token
        print("logging in admin user")
        req = requests.post(
            f"http://localhost:{args.port}/adminapi/login/",
            json={"username": adminuseremail, "password": adminuserpassword},
            timeout=10,
        )
        resp = req.json()
        if "token" not in resp:
            raise Exception(resp)

        self.adminheaders = {"Authorization": f"Bearer {resp['token']}"}

    def adminget(self, endpoint):
        """performs a GET request"""
        print(f"GET /adminapi/{endpoint}")
        req = requests.get(
            f"http://localhost:{args.port}/adminapi/{endpoint}",
            headers=self.adminheaders,
            timeout=10,
        )
        try:
            print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def adminpost(self, endpoint, **kwargs):
        """performs a POST request"""
        print(f"POST /adminapi/{endpoint}")
        req = requests.post(
            f"http://localhost:{args.port}/adminapi/{endpoint}",
            headers=self.adminheaders,
            json=kwargs.get("json"),
            timeout=10,
        )
        try:
            print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def admindelete(self, endpoint, **kwargs):
        """performs a DELETE request"""
        print(f"DELETE /adminapi/{endpoint}")
        req = requests.delete(
            f"http://localhost:{args.port}/adminapi/{endpoint}",
            headers=self.adminheaders,
            json=kwargs.get("json"),
            timeout=10,
        )
        try:
            req.raise_for_status()
        except Exception:
            print(req.text)


# ========================================================================================================================
if __name__ == "__main__":
    faker = Faker("en-IN")

    accountmanager = TestClient(args.port)
    orguser = accountmanager.clientpost(
        "organizations/users/", json={"email": faker.email(), "password": "password"}
    )

    accountmanager.login(orguser["email"], "password")

    org = accountmanager.clientpost("organizations/", json={"name": faker.company()})

    currentuser = accountmanager.clientget("currentuser")

    pm_invitation = accountmanager.clientpost(
        "organizations/users/invite/",
        json={"invited_email": faker.email(), "invited_role": 2},
    )

    pipelinemanager = TestClient(args.port)
    pipelinemanager.clientget(
        f"organizations/users/invite/{pm_invitation['invite_code']}"
    )

    orguser2 = pipelinemanager.clientpost(
        "organizations/users/invite/accept/",
        json={"invite_code": pm_invitation["invite_code"], "password": "new-password"},
    )

    pipelinemanager.login(orguser2["email"], "new-password")
    pipelinemanager.clientget("currentuser")

    # the pipeline manager should not be able to POST /organizations/
    error_response = pipelinemanager.clientpost(
        "organizations/", json={"name": "doesnt matter"}
    )
    assert error_response["error"] == "unauthorized"

    # now invite a report-viewer
    rv_invitation = accountmanager.clientpost(
        "organizations/users/invite/",
        json={"invited_email": faker.email(), "invited_role": 1},
    )

    reportviewer = TestClient(args.port)
    reportviewer.clientget(f"organizations/users/invite/{rv_invitation['invite_code']}")

    orguser3 = reportviewer.clientpost(
        "organizations/users/invite/accept/",
        json={"invite_code": rv_invitation["invite_code"], "password": "new-password"},
    )

    reportviewer.login(orguser3["email"], "new-password")
    reportviewer.clientget("currentuser")

    # the pipeline manager should not be able to GET /organizations/users
    error_response = reportviewer.clientget(
        "organizations/users",
    )
    assert error_response["error"] == "unauthorized"

    # cleanup
    admintester = AdminTester(args.admin_email, args.admin_password)
    admintester.adminheaders["x-ddp-confirmation"] = "yes"
    admintester.admindelete("organizations/", json={"name": org["name"]})
