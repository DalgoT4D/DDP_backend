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

- All api docs are at `http://localhost:8002/api/docs`

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

### Setup pre-commit and run hooks

-  Run "pre-commit install" after activating your virtual env
- Run "pre-commit run --all-files" to run the formatter

### Running celery

- In your virtual environment run:<br>
 `celery -A ddpui worker -n ddpui`
- For windows run:<br>
`celery -A ddpui worker -n ddpui -P solo`
- To start celery beat run:<br>
`celery -A ddpui beat`

## Setup instructions

### Step 1: Create a Python Virtual Environment

-   `pyenv local 3.10`

-   `pyenv exec python -m venv venv`

-   `source venv/bin/activate`

-   `pip install --upgrade pip`

-   `pip install -r requirements.txt`

### Step 2: Create the .env file

-   create `.env` from `.env.template`

### Step 3: Create SQL Database

-   create a SQL database and populate its credentials into `.env`

-   You can use a postgresql docker image for local development

``` 
docker run --name postgres-db -e POSTGRES_PASSWORD=<password> -p 5432:5432 -d <db name>

```

- Add the environment variable to .env

```
DBNAME=<db name>
DBHOST=localhost
DBPORT=5432
DBUSER=postgres
DBPASSWORD=<password>
DBADMINUSER=postgres
DBADMINPASSWORD=<password>

```

### Step 4: Install Airbyte
-   Open a new terminal
-   Download [run-ab-platform.sh](https://raw.githubusercontent.com/airbytehq/airbyte/v0.58.0/run-ab-platform.sh) for Airbyte 0.58.0
-   Run `./run-ab-platform.sh` to start Airbyte. This is a self-contained application which includes the configuration database
-   Populate Airbyte connection credentials in the `.env` from Step 2:

```
AIRBYTE_SERVER_HOST=localhost
AIRBYTE_SERVER_PORT=8000
AIRBYTE_SERVER_APIVER=v1
AIRBYTE_API_TOKEN= <token> # base64 encryption of username:password. Default username and password is airbyte:password and token will be YWlyYnl0ZTpwYXNzd29yZA==
AIRBYTE_DESTINATION_TYPES="Postgres,BigQuery"
```

### Step 5: Install Prefect and Start Prefect Proxy

-   [Start Prefect Proxy](https://github.com/DalgoT4D/prefect-proxy) and populate connection info in `.env`

```
PREFECT_PROXY_API_URL=
```

### Step 6: Create secrets directory
-   Set `DEV_SECRETS_DIR` in `.env` unless you want to use Amazon's Secrets Manager

### Step 7: Install DBT
-   Open a new terminal

-   Create a local `venv`, install `dbt` and put its location into `DBT_VENV` in `.env`

```
pyenv local 3.10

pyenv exec python -m venv <env-name>

source <env-name>/bin/activate

python -m pip install \
  dbt-core \
  dbt-postgres \
  dbt-bigquery

```

-   Create empty directories for `CLIENTDBT_ROOT`

```
CLIENTDBT_ROOT=
DBT_VENV=<env-name>/bin/activate
```

### Step 8: Add SIGNUPCODE and FRONTEND_URL

-   The `SIGNUPCODE` in `.env` is for signing up using the frontend. If you are running the frontend, set its URL in `FRONTEND_URL`

### Step 9: Start Backend

```
DJANGOSECRET=
```
-   Create logs folder in `ddpui`

-   create `whitelist.py` from `.whitelist.template.py` in ddpui > assets folder

-   Run DB migrations `python manage.py migrate`

-   Seed the DB `python manage.py loaddata seed/*.json`

-   Create the system user `python manage.py create-system-orguser`

-   Start the server `uvicorn ddpui.asgi:application --port <PORT_TO_LISTEN_ON>`

### Step 10: Create first org and user
-   Run `python manage.py createorganduser <Org Name> <Email address> --role super-admin`
-   The above command creates a user with super admin role. If we don't provide any role, the default role is of account manager.

## Using Docker
Follow the steps below:

### Step 1: Install Docker and Docker Compose

-  Install [docker](https://docs.docker.com/engine/install/)
-  Install [docker-compose](https://docs.docker.com/compose/install/)

### Step 2: Create .env file

-  create `.env.docker` from `.env.template` inside the Docker folder

### Step 3: Create `whitelist.py` file

-  Copy the file in ddpui/assets/ to Docker/mount

### Step 4: Build the image
If using M1-based MacBook  run this before building image 
  `export DOCKER_DEFAULT_PLATFORM=linux/amd64`


-  `docker build -f Docker/Dockerfile.main --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') -t dalgo_backend_main_image:0.1 .` This will create the main image
- `docker build -f Docker/Dockerfile.dev.deploy --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') -t dalgo_backend:0.1 .` 

### Step 5: Start the other applications

-  Follow [Step 4](#step-4-install-airbyte) and [Step 5](#step-5-install-prefect-and-start-prefect-proxy) in the Setup Instructions

### Step 5: Start/stop Backend

-  `docker compose -p dalgo_backend -f Docker/docker-compose.yml --env-file Docker/.env.docker up -d`
-  `docker compose -p dalgo_backend -f Docker/docker-compose.yml --env-file Docker/.env.docker down`
