# whitelist airbyte sources and destinations here
import json

DEMO_WHITELIST_SOURCES = [
    {
        "type": "Google Sheets",
        "config": {
            "row_batch_size": 500,
            "spreadsheet_id": "https://docs.google.com/spreadsheets/d/18GqjB6nFBxZbAPnk6UOAvERE3iMfi4AkidjV6HkFKQk/edit#gid=0",
            "credentials": {
                "auth_type": "Service",
                "service_account_info": json.dumps(
                    {"key": "big service account json dictionary"}
                ),
            },
        },
    },
    {
        "type": "Postgres",
        "config": {
            "database": "test_db",
            "host": "host_domain",
            "username": "user_name",
            "password": "password",
            "port": 5432,
            "ssl_mode": {"mode": "disable"},
            "schemas": ["schema1", "schema2"],
            "tunnel_method": {
                "tunnel_method": "NO_TUNNEL",
            },
        },
    },
]
