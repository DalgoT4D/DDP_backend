import requests
import argparse
import sys
from faker import Faker

parser = argparse.ArgumentParser()
parser.add_argument('--admin-email', required=True)
args = parser.parse_args()

# ========================================================================================================================
class AdminTester:
  def __init__(self, adminuseremail):

    # get bearer token
    print("logging in admin user")
    r = requests.post('http://localhost:8000/adminapi/login/', json={
        'email': adminuseremail, 'password': 'password'
    })
    resp = r.json()
    if 'token' not in resp:
      print(resp)
      sys.exit(0)

    self.adminheaders = {'Authorization': f"Bearer {resp['token']}"}

  def adminget(self, endpoint):
    print(f"GET /adminapi/{endpoint}")
    r = requests.get(f'http://localhost:8000/adminapi/{endpoint}', headers=self.adminheaders)
    try:
      print(r.json())
      return r.json()
    except Exception:
      print(r.text)

  def adminpost(self, endpoint, **kwargs):
    print(f"POST /adminapi/{endpoint}")
    r = requests.post(f'http://localhost:8000/adminapi/{endpoint}', headers=self.adminheaders, json=kwargs.get('json'))
    try:
      print(r.json())
      return r.json()
    except Exception:
      print(r.text)


# ========================================================================================================================
class ClientTester:
  def __init__(self):
    self.clientheaders = None

  def clientget(self, endpoint):
    print(f"GET /api/{endpoint}")
    r = requests.get(f'http://localhost:8000/api/{endpoint}', headers=self.clientheaders)
    try:
      print(r.json())
      return r.json()
    except Exception:
      print(r.text)

  def clientpost(self, endpoint, **kwargs):
    print(f"POST /api/{endpoint}")
    r = requests.post(f'http://localhost:8000/api/{endpoint}', headers=self.clientheaders, json=kwargs.get('json'))
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


# ========================================================================================================================
if __name__ == '__main__':

  # look up the admin user
  faker = Faker('en-IN')

  admintester = AdminTester(args.admin_email)

  clientusertester = ClientTester()
  clientuser = clientusertester.clientpost(
    'createuser/', json={'email': faker.email()}
  )

  clientusertester.login(clientuser['email'], 'password')

  clientorg = clientusertester.clientpost('client/create/', json={'name': faker.company()})
  
  clientusertester.clientget('currentuser')

  invitation = clientusertester.clientpost(
    'user/invite/', json={'invited_email': faker.email()}
  )

  clientuser2tester = ClientTester()
  clientuser2tester.clientget(f"user/getinvitedetails/{invitation['invite_code']}")

  clientuser2 = clientuser2tester.clientpost('user/acceptinvite/', json={'invite_code': invitation['invite_code'], 'password': 'password'})

  clientuser2tester.login(clientuser2['email'], 'password')
  clientuser2tester.clientget('currentuser')

  # cleanup
  admintester.adminheaders['x-ddp-confirmation'] = 'yes'
  admintester.adminpost('deleteorg/', json={'name': clientorg['name']})
