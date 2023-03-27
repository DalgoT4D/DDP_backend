import requests

class ClientTester:
  def __init__(self, port):
    self.clientheaders = None
    self.port = port

  def clientget(self, endpoint):
    print(f"GET /api/{endpoint}")
    r = requests.get(f'http://localhost:{self.port}/api/{endpoint}', headers=self.clientheaders)
    try:
      print(r.json())
      return r.json()
    except Exception:
      print(r.text)

  def clientpost(self, endpoint, **kwargs):
    print(f"POST /api/{endpoint}")
    r = requests.post(f'http://localhost:{self.port}/api/{endpoint}', headers=self.clientheaders, json=kwargs.get('json'))
    try:
      print(r.json())
      return r.json()
    except Exception:
      print(r.text)

  def login(self, email, password):
    r = self.clientpost('login/', json={'email': email, 'password': password})
    if 'token' not in r:
      print(r)
      return
    self.clientheaders = {'Authorization': f"Bearer {r['token']}"}
