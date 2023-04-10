import requests


class TestClient:
    """http client for api/ endpoints"""

    def __init__(self, port):
        self.clientheaders = None
        self.port = port

    def login(self, email, password):
        """Login"""
        req = self.clientpost("login/", json={"username": email, "password": password})
        if "token" not in req:
            raise Exception(req)
        self.clientheaders = {"Authorization": f"Bearer {req['token']}"}

    def clientget(self, endpoint):
        """GET"""
        print(f"GET /api/{endpoint}")
        req = requests.get(
            f"http://localhost:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=10,
        )
        try:
            print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientpost(self, endpoint, **kwargs):
        """POST"""
        print(f"POST /api/{endpoint}")
        req = requests.post(
            f"http://localhost:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=kwargs.get("timeout", 10),
            json=kwargs.get("json"),
        )
        try:
            print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientdelete(self, endpoint, **kwargs):
        """DELETE"""
        print(f"DELETE /api/{endpoint}")
        req = requests.delete(
            f"http://localhost:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=10,
        )
        try:
            req.raise_for_status()
        except Exception:
            print(req.text)
