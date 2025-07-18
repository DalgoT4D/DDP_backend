#!/usr/bin/env python
"""Test authentication and warehouse data flow"""
import requests
import json

BASE_URL = "http://localhost:8002"

# Step 1: Login
print("1. Testing login...")
login_response = requests.post(
    f"{BASE_URL}/api/login/",
    json={"username": "admin@gmail.com", "password": "test123"}
)
print(f"Login status: {login_response.status_code}")
if login_response.status_code == 200:
    auth_data = login_response.json()
    token = auth_data.get('token')
    print(f"Token received: {token[:20]}...")
else:
    print(f"Login failed: {login_response.text}")
    exit(1)

# Step 2: Get current user
print("\n2. Testing current user...")
headers = {
    "Authorization": f"Bearer {token}",
}
user_response = requests.get(f"{BASE_URL}/api/currentuserv2", headers=headers)
print(f"Current user status: {user_response.status_code}")
if user_response.status_code == 200:
    user_data = user_response.json()
    org_slug = user_data[0]['org']['slug']
    print(f"Organization: {user_data[0]['org']['name']} ({org_slug})")
else:
    print(f"Failed to get user: {user_response.text}")
    exit(1)

# Step 3: Test warehouse schemas
print("\n3. Testing warehouse schemas...")
headers['x-dalgo-org'] = org_slug
schemas_response = requests.get(f"{BASE_URL}/api/warehouse/schemas", headers=headers)
print(f"Schemas status: {schemas_response.status_code}")
print(f"Response: {schemas_response.text}")

# Step 4: If schemas work, test tables
if schemas_response.status_code == 200:
    schemas = schemas_response.json()
    print(f"Schemas found: {schemas}")
    
    # Test getting tables from first schema
    if schemas:
        first_schema = schemas[0]
        print(f"\n4. Testing tables in schema '{first_schema}'...")
        tables_response = requests.get(
            f"{BASE_URL}/api/warehouse/tables/{first_schema}",
            headers=headers
        )
        print(f"Tables status: {tables_response.status_code}")
        if tables_response.status_code == 200:
            tables = tables_response.json()
            print(f"Tables found: {tables[:5]}...")  # Show first 5 tables