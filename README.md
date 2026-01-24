## DDP_backend

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Code coverage badge](https://img.shields.io/codecov/c/github/DalgoT4D/DDP_backend/main.svg)](https://codecov.io/gh/dalgot4d/DDP_backend/branch/main)
[![DeepSource](https://app.deepsource.com/gh/dalgot4d/DDP_backend.svg/?label=active+issues&show_trend=true&token=H-ilF26v7GEjUlQa3hLfMhPy)](https://app.deepsource.com/gh/dalgot4d/DDP_backend/?ref=repository-badge)

Django application for the DDP platform's management backend. The Dalgo Data Platform (DDP) is an open-source initiative by Project Tech4Dev designed to empower social sector organizations with robust data management and analytics capabilities. Exposes API endpoints for the management frontend to communicate with, for the purposes of - 

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

### UV package manager

The project uses `uv` as its package manager. `uv` is chosen for its speed and efficiency in managing Python dependencies. You will need to install it on your machine

UV can be installed system-wide using cURL on macOS and Linux:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sudo sh
```

And with Powershell on Windows (make sure you run Powershell with administrator privileges):

```sh
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

UV is available via Homebrew as well:

```sh
brew install uv
```

## Setup instructions

### Step 1: Create a Python Virtual Environment

```sh
$ uv sync
```

## Setup pre-commit and run hooks

- Run "pre-commit install" after activating your virtual env created in above step
- Run "pre-commit run --all-files" to run the formatter

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

### Step11: Running celery

We use two separate Celery workers for better task isolation:

#### Default Worker (for general tasks)
- In your virtual environment run:<br>
 `celery -A ddpui worker -Q default -n ddpui`
- For windows run:<br>
`celery -A ddpui worker -Q default -n ddpui -P solo`

#### Canvas DBT Worker (for canvas DBT operations)
- In another terminal with the same virtual environment:<br>
 `celery -A ddpui worker -Q canvas_dbt -n canvas_dbt --concurrency=2`
- For windows run:<br>
`celery -A ddpui worker -Q canvas_dbt -n canvas_dbt --concurrency=2 -P solo`

#### Celery Beat (for periodic tasks)
- To start celery beat run:<br>
`celery -A ddpui beat`

**Note**: The Canvas DBT worker handles resource-intensive DBT operations from the canvas, while the default worker handles other background tasks like notifications and maintenance.

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

## Feature Flags

The platform supports feature flags to control the availability of features at both global and organization-specific levels. Organization-specific flags override global flags.

### Available Feature Flags

- `DATA_QUALITY` - Elementary data quality reports
- `USAGE_DASHBOARD` - Superset usage dashboard for org  
- `EMBED_SUPERSET` - Embed superset dashboards
- `LOG_SUMMARIZATION` - Summarize logs using AI
- `AI_DATA_ANALYSIS` - Enable data analysis using AI
- `DATA_STATISTICS` - Enable detailed data statistics in explore

### Managing Feature Flags

#### 1. Creating Global Flags
Global flags apply to all organizations by default:

```python
from ddpui.utils.feature_flags import enable_feature_flag, disable_feature_flag

# Enable a global flag
enable_feature_flag("DATA_QUALITY")  # org=None for global

# Disable a global flag  
disable_feature_flag("DATA_QUALITY")
```

#### 2. Creating Organization-Specific Flags
Organization-specific flags override global flags for that particular org:

```python
from ddpui.models.org import Org
from ddpui.utils.feature_flags import enable_feature_flag, disable_feature_flag

# Get the organization
org = Org.objects.get(slug="org-slug")

# Enable for specific org (overrides global setting)
enable_feature_flag("DATA_QUALITY", org=org)

# Disable for specific org
disable_feature_flag("DATA_QUALITY", org=org)
```

#### 3. Management Command
Use the Django management command to manage flags via CLI:

```bash
# Enable all available global feature flags
python manage.py manage_feature_flags --enable-all-global

# Enable a specific flag globally
python manage.py manage_feature_flags --enable DATA_QUALITY

# Disable a specific flag globally  
python manage.py manage_feature_flags --disable DATA_QUALITY

# Enable for a specific organization
python manage.py manage_feature_flags --enable DATA_QUALITY --org-slug org-slug

# Disable for a specific organization
python manage.py manage_feature_flags --disable DATA_QUALITY --org-slug org-slug
```

#### 4. API Endpoint
Frontend applications can fetch the feature flags for the current organization:

```
GET /api/organizations/flags
```

This endpoint returns a JSON object with all feature flags and their status for the authenticated user's organization. The response includes both global flags and any organization-specific overrides.
