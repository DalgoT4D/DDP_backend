# DDP_backend

Django application for the DDP platform's management backend. Exposes API endpoints for the management frontend to communicate with, for the purposes of

- Onboarding an NGO client
- Adding users from the client-organization
- Creating a client's workspace in our Airbyte installation
- Configuring that workspace i.e. setting up sources, destinations and connections
- Configuring data ingest jobs in our Prefect setup
- Connecting to the client's dbt GitHub repository
- Configuring dbt run jobs in our Prefect setup

## Setup instructions
pip install -r requirements.txt

create .env from .env.template

