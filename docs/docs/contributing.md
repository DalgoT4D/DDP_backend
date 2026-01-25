# Contributing to Dalgo

## Service Overview

The Dalgo platform consists of multiple interconnected services. Here's a one-line description of each:

| Service | Repository | Description |
|---------|------------|-------------|
| **Dalgo Backend** | [DalgoT4D/DDP_backend](https://github.com/DalgoT4D/DDP_backend) | Django REST API backend with user management, pipeline orchestration, and data services |
| **Webapp v2** | [DalgoT4D/webapp_v2](https://github.com/DalgoT4D/webapp_v2) | Modern Next.js 15 frontend with React 19, dashboard visualization, and real-time features |
| **Webapp (Legacy)** | [DalgoT4D/webapp](https://github.com/DalgoT4D/webapp) | Original Next.js 14 frontend for organization management and pipeline configuration |
| **Prefect Proxy** | [DalgoT4D/prefect-proxy](https://github.com/DalgoT4D/prefect-proxy) | FastAPI service bridging Django with Prefect for async workflow orchestration |
| **AI/LLM Service** | [DalgoT4D/ai-llm-service](https://github.com/DalgoT4D/ai-llm-service) | FastAPI service for AI and LLM tasks with Celery workers and OpenAI integration |
| **Airbyte** | External service | Open-source data integration platform for connecting 100+ data sources |
| **Prefect** | External service | Workflow orchestration engine for managing data pipeline execution |

## Development Environment Setup

### Prerequisites

- **Python 3.10+** with [uv](https://docs.astral.sh/uv/) package manager
- **Node.js 18+** with npm/yarn
- **Docker & Docker Compose** for external services
- **PostgreSQL** for development database
- **Redis** for caching and message broker

### 1. Core Services Setup

#### Dalgo Backend

**Prerequisites: PostgreSQL, Redis & dbt Setup**

```bash
# 1. PostgreSQL & Redis Setup
# Option 1: Using Docker (Recommended for development)
docker run --name postgres-dalgo -e POSTGRES_PASSWORD=dalgo_password -e POSTGRES_DB=dalgo_dev -e POSTGRES_USER=dalgo -p 5432:5432 -d postgres:15
docker run --name redis-dalgo -p 6379:6379 -d redis:7-alpine

# Option 2: Local installation
# macOS
brew install postgresql redis
brew services start postgresql
brew services start redis

# Ubuntu/Debian
sudo apt install postgresql postgresql-contrib redis-server
sudo systemctl start postgresql redis
sudo -u postgres createuser dalgo
sudo -u postgres createdb dalgo_dev -O dalgo

# 2. Setup dbt environment
# Create a separate virtual environment for dbt
python3 -m venv venv
source venv/bin/activate
pip install -r requirements_dbt.txt
deactivate

# Note the absolute path to the directory containing the venv folder
# You'll need this for DBT_VENV in .env (not the activate script path, just the folder path)

# 3. Create client dbt root directory
# Create a directory to store organization-specific dbt projects
mkdir -p /path/to/client-dbt-projects
# You'll need this absolute path for CLIENTDBT_ROOT in .env
```

**Required Environment Configuration (.env):**

The Dalgo Backend requires extensive configuration. Here are all the environment variables:

```bash
# === CORE DJANGO SETTINGS ===
DJANGOSECRET=your-super-secret-django-key-here
DEBUG=True
ENVIRONMENT=development  # options: development, staging, production

# === DATABASE CONFIGURATION ===
DBNAME=dalgo_dev
DBHOST=localhost
DBUSER=dalgo
DBPASSWORD=dalgo_password
DBPORT=5432

# === REDIS CONFIGURATION ===
REDIS_HOST=localhost
REDIS_PORT=6379

# === JWT TOKEN SETTINGS ===
JWT_SECRET_KEY=your-jwt-secret-key
JWT_ACCESS_TOKEN_EXPIRY_HOURS=12
JWT_REFRESH_TOKEN_EXPIRY_DAYS=30

# === FRONTEND URLS ===
FRONTEND_URL=http://localhost:3000
FRONTEND_URL_V2=http://localhost:3001

# === AIRBYTE INTEGRATION ===
AIRBYTE_SERVER_HOST=localhost
AIRBYTE_SERVER_PORT=8000
AIRBYTE_USERNAME=airbyte
AIRBYTE_PASSWORD=password
AIRBYTE_SOURCE_BLACKLIST=file,postgres  # comma-separated list

# === PREFECT INTEGRATION ===
PREFECT_PROXY_API_URL=http://localhost:8085
PREFECT_HTTP_TIMEOUT=5
PREFECT_WORKER_POOL_NAME="dev_dalgo_work_pool"
PREFECT_RETRY_CRASHED_FLOW_RUNS=False
PREFECT_AIRBYTE_TASKS_TIMEOUT_SECS=15

# === AI/LLM SERVICE ===
AI_LLM_SERVICE_URL=http://localhost:7001

# === SENTRY ERROR TRACKING (OPTIONAL) ===
SENTRY_DSN=your-sentry-dsn-url
SENTRY_TSR=1.0  # traces sample rate
SENTRY_PSR=1.0  # profiles sample rate
SENTRY_ENABLE_LOGS=True
SENTRY_SEND_DEFAULT_PII=True

# === EMAIL CONFIGURATION (OPTIONAL) ===
EMAIL_BACKEND=django.core.mail.backends.smtp.EmailBackend
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=True
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password

# === AWS CONFIGURATION (FOR PRODUCTION) ===
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET_NAME=your-s3-bucket

# === GOOGLE CLOUD CONFIGURATION (FOR BIGQUERY) ===
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# === DBT CONFIGURATION ===
DBT_VENV=/path/to/dbt-folder
CLIENTDBT_ROOT=/path/to/client-dbt-projects
```

**Key Configuration Notes:**

1. **DJANGOSECRET**: Generate with `python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"`

2. **Database**: Must be PostgreSQL (SQLite not supported for production features)

3. **Redis**: Required for Celery background tasks and WebSocket support

4. **AIRBYTE_***: Only needed if you plan to use data ingestion features

5. **PREFECT_PROXY_API_URL**: Required for orchestration features

6. **AI_LLM_SERVICE_URL**: Required for AI-powered features

7. **Frontend URLs**: Must match your frontend application URLs for CORS

8. **DBT_VENV**: Absolute path to the folder containing the dbt venv (the system will append `/venv/bin/activate`)

9. **CLIENTDBT_ROOT**: Absolute path to directory where organization dbt projects are stored

**Main Setup**

```bash
# Clone and setup
git clone https://github.com/DalgoT4D/DDP_backend.git
cd DDP_backend

# Install dependencies
uv sync

# Create environment file
cp .env.example .env
# Edit .env with your configuration (see above)

# Setup PostgreSQL database
createdb dalgo_dev  # or use your preferred method

# Run database migrations
uv run python manage.py migrate

# Load initial data
uv run python manage.py loaddata seed/*.json

# Create system user (required for internal operations)
uv run python manage.py create-system-orguser

# Create your first organization and admin user
uv run python manage.py createorganduser "Your Organization" "admin@example.com" --role super-admin

# Optional: Load geographic data for maps
uv run python manage.py seed_map_data

# Start Django ASGI server
uv run uvicorn ddpui.asgi:application --workers 4 --host 0.0.0.0 --port 8002 --reload

# Start Default Celery worker (in another terminal)
uv run celery -A ddpui worker -Q default -n ddpui -l info

# Start Canvas DBT Celery worker (in another terminal)
uv run celery -A ddpui worker -Q canvas_dbt -n canvas_dbt --concurrency=2 -l info

# Start Celery beat scheduler (in another terminal)
uv run celery -A ddpui beat -l info
```

#### Prefect Proxy

**Required Environment Configuration (.env):**

```bash
# === CORE SETTINGS ===
DEBUG=True
ENVIRONMENT=development

# === PREFECT CONFIGURATION ===
PREFECT_API_URL=http://localhost:4200/api
PREFECT_SERVER_HOST=localhost
PREFECT_SERVER_PORT=4200

# === GOOGLE CLOUD (FOR BIGQUERY OPERATIONS) ===
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# === LOGGING ===
LOG_LEVEL=INFO
```

**Setup**

```bash
# Clone repository
git clone https://github.com/DalgoT4D/prefect-proxy.git
cd prefect-proxy

# Install dependencies
uv sync

# Create environment file
cp .env.template .env
# Edit .env with your configuration (see above)

# Start Prefect server (in one terminal)
uv run prefect server start

# Start Prefect workers (in separate terminals)
uv run prefect worker start -q ddp --pool dev_dalgo_work_pool --limit 1
uv run prefect worker start -q manual-dbt --pool dev_dalgo_work_pool --limit 1

# Start proxy service (in another terminal)
uv run uvicorn proxy.main:app --reload --port 8085
```

#### Frontend Applications

**Webapp v2 (Modern Dashboard):**
```bash
# Clone repository
git clone https://github.com/DalgoT4D/webapp_v2.git
cd webapp_v2

# Install dependencies
npm install

# Start development server
npm run dev
# Available at http://localhost:3001
```

**Webapp (Legacy Management Interface):**
```bash
# Clone repository
git clone https://github.com/DalgoT4D/webapp.git
cd webapp

# Install dependencies
yarn install

# Start development server
yarn dev
# Available at http://localhost:3000
```

### 2. External Services Setup

#### Airbyte (Data Integration)

```bash
# Using Docker Compose
git clone https://github.com/airbytehq/airbyte.git
cd airbyte

# Start Airbyte services
./run-ab-platform.sh
# Available at http://localhost:8000
```

#### Prefect Server (if not using prefect-proxy setup)

```bash
# Install Prefect
pip install prefect

# Start Prefect server
prefect server start
# Available at http://localhost:4200
```

#### PostgreSQL & Redis

**Option 1: Docker Compose**
```yaml
# docker-compose.dev.yml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: dalgo_dev
      POSTGRES_USER: dalgo
      POSTGRES_PASSWORD: dalgo
    ports:
      - "5432:5432"
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

```bash
docker-compose -f docker-compose.dev.yml up -d
```

**Option 2: Local Installation**
```bash
# macOS
brew install postgresql redis
brew services start postgresql
brew services start redis

# Ubuntu/Debian
sudo apt install postgresql postgresql-contrib redis-server
sudo systemctl start postgresql redis
```

## Development Workflow

### Starting All Services

1. **Start external services:**
   ```bash
   docker-compose up -d postgres redis
   ./run-ab-platform.sh  # For Airbyte
   ```

2. **Start backend services:**
   ```bash
   cd DDP_backend && pm2 start dalgo_dev.config.js
   cd ../prefect-proxy && uv run uvicorn proxy.main:app --reload --port 8085
   ```

3. **Start frontend services:**
   ```bash
   cd webapp_v2 && npm run dev &
   cd ../webapp && yarn dev &
   ```

### Port Reference

| Service | Port | URL |
|---------|------|-----|
| Dalgo Backend API | 8002 | http://localhost:8002 |
| Prefect Proxy | 8085 | http://localhost:8085 |
| Webapp v2 | 3001 | http://localhost:3001 |
| Webapp (Legacy) | 3000 | http://localhost:3000 |
| Airbyte | 8000 | http://localhost:8000 |
| Prefect Server | 4200 | http://localhost:4200 |
| PostgreSQL | 5432 | - |
| Redis | 6379 | - |

### Testing

```bash
# Backend tests
cd DDP_backend
pytest ddpui/tests

# Frontend tests
cd webapp_v2
npm test

cd ../webapp
yarn test

# Integration tests
cd DDP_backend
pytest ddpui/tests/integration_tests/
```

### Making Changes

1. **Create feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make changes and test:**
   ```bash
   # Run relevant tests
   pytest ddpui/tests/
   npm test
   
   # Check code quality
   black .
   pylint ddpui/
   npm run lint
   ```

3. **Submit pull request:**
   - Ensure all tests pass
   - Follow commit message conventions
   - Include documentation updates if needed

## Common Issues

### Database Connection Issues
```bash
# Reset database
python manage.py migrate --fake-initial
python manage.py loaddata seed/*.json
```

### Service Communication Issues
```bash
# Check service status
pm2 list
pm2 logs

# Restart specific service
pm2 restart django-backend-asgi
```

### Frontend Build Issues
```bash
# Clear cache and reinstall
rm -rf node_modules package-lock.json
npm install

# For Next.js issues
rm -rf .next
npm run build
```

## Getting Help

- **Documentation**: Check service-specific README files
- **Issues**: Create GitHub issues in respective repositories
- **Architecture**: Refer to `docs/architecture/` for system design
- **API Reference**: Visit http://localhost:8002/docs for API documentation

## Code Quality Standards

- **Python**: Follow PEP 8, use Black for formatting, maintain >90% test coverage
- **TypeScript**: Use ESLint + Prettier, follow React best practices
- **Git**: Use conventional commit messages, keep PRs focused and small
- **Documentation**: Update docs for any API or architecture changes