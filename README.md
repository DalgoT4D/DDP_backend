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
-   [Start Airbyte](https://docs.airbyte.com/deploying-airbyte/local-deployment) and populate connection info in `.env`

```
AIRBYTE_SERVER_HOST=
AIRBYTE_SERVER_PORT=
AIRBYTE_SERVER_APIVER=
AIRBYTE_API_TOKEN= <token> # base64 encryption of username:password. Default username and password is airbyte:password and token will be YWlyYnl0ZTpwYXNzd29yZA==
AIRBYTE_DESTINATION_TYPES=
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

-   Start the server `python manage.py runserver`

### Step 10: Create first org and user

-   Run `python manage.py createorganduser <Org Name> <Email address>`

## Using Docker
Follow the steps below:

### Step 1: Install Docker and Docker Compose

-  Install [docker](https://docs.docker.com/engine/install/)
-  Install [docker-compose](https://docs.docker.com/compose/install/)

### Step 2: Create .env file

-  create `.env` from `.env.template` inside the Docker folcer

### Step 3: Create `whitelist.py` file

-  Copy the file in ddpui/assets/ to Docker/mount

### Step 4: Build the image

-  `docker build -f Docker/Dockerfile --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') -t dalgo_backend:0.1 .`

### Step 5: Start the other applications

-  Follow [Step 4](#step-4-install-airbyte) and [Step 5](#step-5-install-prefect-and-start-prefect-proxy) in the Setup Instructions

### Step 5: Start Backend

-  `docker-compose -f Docker/docker-compose.dev.yml up`
