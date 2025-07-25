import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ddpui.settings')
django.setup()

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
import json

# List all orgs
orgs = Org.objects.all()
print(f"Found {orgs.count()} organizations:")
for o in orgs:
    print(f"  - ID: {o.id}, Name: {o.name}, Slug: {o.slug}")

if orgs.count() == 0:
    print("No organizations found! Please create one first.")
    exit(1)

# Get the Admin Org (the one used by adp1@gmail.com)
org = Org.objects.filter(name="Admin Org").first()
if not org:
    print("Admin Org not found!")
    exit(1)
print(f"\nUsing org: {org.name} (ID: {org.id})")

# Check if warehouse exists
warehouse = OrgWarehouse.objects.filter(org=org).first()

if not warehouse:
    print("Creating warehouse for org...")
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        credentials={
            "host": "localhost",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "postgres"
        }
    )
    print("Warehouse created successfully!")
else:
    print(f"Warehouse already exists: {warehouse.wtype}")
    # Update credentials if needed
    warehouse.credentials = {
        "host": "localhost",
        "port": 5432,
        "database": "postgres", 
        "username": "postgres",
        "password": "postgres"
    }
    warehouse.save()
    print("Warehouse credentials updated!")

print(f"Warehouse ID: {warehouse.id}")
print(f"Warehouse Type: {warehouse.wtype}")
print(f"Credentials: {json.dumps(warehouse.credentials, indent=2)}")