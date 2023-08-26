import requests


class TestClient:
    """http client for api/ endpoints"""

    def __init__(self, port, **kwargs):
        self.clientheaders = None
        self.port = port
        self.verbose = kwargs.get("verbose")
        self.host = kwargs.get("host", "localhost")
        self.httpschema = "http" if self.host in ["localhost", "127.0.0.1"] else "https"

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
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=10,
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error
        try:
            if self.verbose:
                print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientpost(self, endpoint, **kwargs):
        """POST"""
        print(f"POST /api/{endpoint}")
        req = requests.post(
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=kwargs.get("timeout", 10),
            json=kwargs.get("json"),
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error
        try:
            if self.verbose:
                print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientput(self, endpoint, **kwargs):
        """PUT"""
        print(f"PUT /api/{endpoint}")
        req = requests.put(
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=kwargs.get("timeout", 10),
            json=kwargs.get("json"),
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error
        try:
            if self.verbose:
                print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientdelete(self, endpoint, **kwargs):
        """DELETE"""
        print(f"DELETE /api/{endpoint}")
        req = requests.delete(
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=10,
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error
