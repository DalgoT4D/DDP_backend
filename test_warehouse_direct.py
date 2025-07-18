#!/usr/bin/env python
"""Test warehouse connection directly"""
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ddpui.settings')
django.setup()

from ddpui.models.org import Org, OrgWarehouse
from ddpui.core import dbtautomation_service
from ddpui.utils.helpers import map_airbyte_keys_to_postgres_keys

# Get the admin org and warehouse
org = Org.objects.get(slug='admin-org')
warehouse = OrgWarehouse.objects.filter(org=org).first()

print(f"Testing warehouse: {warehouse.name}")
print(f"Warehouse type: {warehouse.wtype}")

try:
    # Get the client using the same logic as the API
    print("\nGetting warehouse client...")
    client = dbtautomation_service._get_wclient(warehouse)
    print("Client created successfully!")
    
    # Get schemas
    print("\nFetching schemas...")
    schemas = client.get_schemas()
    print(f"Schemas: {schemas}")
    
    # Test getting tables from first schema
    if schemas:
        print(f"\nFetching tables from schema '{schemas[0]}'...")
        tables = client.get_tables(schemas[0])
        print(f"Tables: {tables[:5]}...")  # Show first 5
        
except Exception as e:
    print(f"\nError: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()