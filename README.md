## DDP_backend

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Code coverage badge](https://img.shields.io/codecov/c/github/DalgoT4D/DDP_backend/main.svg)](https://codecov.io/gh/dalgot4d/DDP_backend/branch/main)
[![DeepSource](https://app.deepsource.com/gh/dalgot4d/DDP_backend.svg/?label=active+issues&show_trend=true&token=H-ilF26v7GEjUlQa3hLfMhPy)](https://app.deepsource.com/gh/dalgot4d/DDP_backend/?ref=repository-badge)

Django application for the DDP platform's management backend. Exposes API endpoints for the management frontend to communicate with, for the purposes of

-   Onboarding an NGO client
-   Adding users from the client-organization
-   Creating a client's workspace in our Airbyte installation
-   Configuring that workspace i.e. setting up sources, destinations and connections
-   Configuring data ingest jobs in our Prefect setup
-   Connecting to the client's dbt GitHub repository
-   Configuring dbt run jobs in our Prefect setup

## Development conventions

### Api end points naming

-   REST conventions are being followed.
-   CRUD end points for a User resource would look like:
    -   GET <mark>/api/users/</mark>
    -   GET <mark>/api/users/user_id</mark>
    -   POST <mark>/api/users/</mark>
    -   PUT <mark>/api/users/:user_id</mark>
    -   DELETE <mark>/api/users/:user_id</mark>
-   Route parameteres should be named in snake_case as shown above.

### Ninja api docs

-   [Dashboard](https://api.dalgo.in/api/dashboard/docs)
-   [Airbyte](https://api.dalgo.in/api/airbyte/docs)
-   [Flows](https://api.dalgo.in/api/prefect/docs)
-   [Dbt](https://api.dalgo.in/api/dbt/docs)
-   [Celery tasks](https://api.dalgo.in/api/tasks/docs)
-   [User & Org](https://api.dalgo.in/api/docs)

### Code style

-   `Pep8` has been used to standardized variable names, classes, module names etc.
-   `Pylint` is the linting tool used to analyze the code as per Pep8 style.
-   `Black` is used as the code formatter.

### Setting up your vscode env

-   Recommended IDE is VsCode.
-   Install the pylint extension in vscode and enable it.
-   Set the default format provider in vscode as `black`
-   Update the vscode settings.json as follows<br>
    `    {
"editor.defaultFormatter": null,
"python.linting.enabled": true,
"python.formatting.provider": "black",
"editor.formatOnSave": true
}`

### Running pylint

-   In your virtual environment run `pylint ddpui/`

## Setup instructions

-   `pyenv local 3.10`

-   `pyenv exec python -m venv venv`

-   `source venv/bin/activate`

-   `pip install --upgrade pip`

-   `pip install -r requirements.txt`

-   create .env from .env.template

-   `python manage.py migrate`

-   `python manage.py runserver`
