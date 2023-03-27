import requests
import argparse
import sys
from faker import Faker
from testclient import ClientTester

parser = argparse.ArgumentParser()
parser.add_argument('--admin-email', required=True)
parser.add_argument('--port', default=8000)
args = parser.parse_args()

# ========================================================================================================================
class AdminTester:
  def __init__(self, adminuseremail):

    # get bearer token
    print("logging in admin user")
    r = requests.post(f'http://localhost:{args.port}/adminapi/login/', json={
        'email': adminuseremail, 'password': 'password'
    })
    resp = r.json()
    if 'token' not in resp:
      print(resp)
      sys.exit(0)

    self.adminheaders = {'Authorization': f"Bearer {resp['token']}"}

  def adminget(self, endpoint):
    print(f"GET /adminapi/{endpoint}")
    r = requests.get(f'http://localhost:{args.port}/adminapi/{endpoint}', headers=self.adminheaders)
    try:
      print(r.json())
      return r.json()
    except Exception:
      print(r.text)

  def adminpost(self, endpoint, **kwargs):
    print(f"POST /adminapi/{endpoint}")
    r = requests.post(f'http://localhost:{args.port}/adminapi/{endpoint}', headers=self.adminheaders, json=kwargs.get('json'))
    try:
      print(r.json())
      return r.json()
    except Exception:
      print(r.text)

# ========================================================================================================================
if __name__ == '__main__':

  # look up the admin user
  faker = Faker('en-IN')

  admintester = AdminTester(args.admin_email)

  clientusertester = ClientTester(args.port)
  clientuser = clientusertester.clientpost(
    'createuser/', json={'email': faker.email()}
  )

  clientusertester.login(clientuser['email'], 'password')

  clientorg = clientusertester.clientpost('client/create/', json={'name': faker.company()})
  
  clientusertester.clientget('currentuser')

  invitation = clientusertester.clientpost(
    'user/invite/', json={'invited_email': faker.email()}
  )

  clientuser2tester = ClientTester(args.port)
  clientuser2tester.clientget(f"user/getinvitedetails/{invitation['invite_code']}")

  clientuser2 = clientuser2tester.clientpost('user/acceptinvite/', json={'invite_code': invitation['invite_code'], 'password': 'password'})

  clientuser2tester.login(clientuser2['email'], 'password')
  clientuser2tester.clientget('currentuser')

  # cleanup
  admintester.adminheaders['x-ddp-confirmation'] = 'yes'
  admintester.adminpost('deleteorg/', json={'name': clientorg['name']})
