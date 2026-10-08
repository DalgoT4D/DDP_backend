#!/usr/bin/env bash
# Generates a .env for local development from .env.template.
# Auto-fills generated secrets and sensible dev defaults.
# Leave production-only vars (AWS, Airbyte tokens, Sentry, etc.) for manual entry.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/.env.template"
ENVFILE="$SCRIPT_DIR/.env"

if [ ! -f "$TEMPLATE" ]; then
  echo "Error: .env.template not found at $TEMPLATE"
  exit 1
fi

if [ -f "$ENVFILE" ]; then
  read -r -p ".env already exists. Overwrite? [y/N] " response
  if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
  fi
fi

cp "$TEMPLATE" "$ENVFILE"

# Cross-platform sed in-place
sedi() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "$@"
  else
    sed -i "$@"
  fi
}

# Replace KEY=<anything> with KEY=<value> (handles existing values + inline comments)
set_var() {
  local key="$1"
  local value="$2"
  sedi "s|^${key}=.*|${key}=${value}|" "$ENVFILE"
}

# Generate random secrets
DJANGO_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(50))")
JWT_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(50))")
SIGNUPCODE=$(python3 -c "import secrets; print(secrets.token_hex(8))")
RENDER_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
WEBHOOK_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

# ── Core ──────────────────────────────────────────────────────────────────────
set_var ENVIRONMENT        development
set_var DEBUG              True
set_var DJANGOSECRET       "$DJANGO_SECRET"
set_var ALLOWED_HOSTS      "localhost,127.0.0.1"
set_var CORS_ALLOWED_ORIGINS "http://localhost:3000,http://localhost:3001"

# ── JWT ───────────────────────────────────────────────────────────────────────
set_var JWT_SECRET_KEY                "$JWT_SECRET"
set_var JWT_ACCESS_TOKEN_EXPIRY_MINUTES  720
set_var JWT_REFRESH_TOKEN_EXPIRY_DAYS    30

# ── Auth / misc ───────────────────────────────────────────────────────────────
set_var ROLE_PERMISSIONS_REDIS_KEY  dalgo_role_permissions
set_var SIGNUPCODE                  "$SIGNUPCODE"
set_var RENDER_SECRET               "$RENDER_SECRET"
set_var FRONTEND_URL                "http://localhost:3000"
set_var FRONTEND_URL_V2             "http://localhost:3001"

# ── Database ──────────────────────────────────────────────────────────────────
set_var DBNAME          ddpui_dev
set_var DBHOST          localhost
set_var DBPORT          5432
set_var DBUSER          ddp
set_var DBPASSWORD      ddp
set_var DBADMINUSER     ddp
set_var DBADMINPASSWORD ddp

# ── Redis ─────────────────────────────────────────────────────────────────────
set_var REDIS_HOST  localhost
set_var REDIS_PORT  6379

# ── Prefect ───────────────────────────────────────────────────────────────────
set_var PREFECT_PROXY_API_URL              "http://localhost:8085"
set_var PREFECT_NOTIFICATIONS_WEBHOOK_KEY  "$WEBHOOK_KEY"

# ── Airbyte ───────────────────────────────────────────────────────────────────
set_var AIRBYTE_SERVER_HOST    localhost
set_var AIRBYTE_SERVER_PORT    8000
set_var AIRBYTE_SERVER_APIVER  v1

# ── LLM service ───────────────────────────────────────────────────────────────
set_var LLM_SERVICE_API_URL  "http://127.0.0.1:7001"
set_var LLM_SERVICE_API_KEY  "local-dev-key"
set_var LLM_SERVICE_API_VER  ""

echo ""
echo ".env created at $ENVFILE"
echo ""
echo "Generated secrets:"
echo "  DJANGOSECRET, JWT_SECRET_KEY, SIGNUPCODE, RENDER_SECRET, PREFECT_NOTIFICATIONS_WEBHOOK_KEY"
echo ""
echo "Still needs manual config (leave blank to skip the feature):"
echo "  AWS_*                   S3, SES, Secrets Manager"
echo "  AIRBYTE_API_TOKEN       from your Airbyte instance"
echo "  SENTRY_DSN              error tracking (optional)"
echo "  ADMIN_EMAIL / ADMIN_DISCORD_WEBHOOK"
echo "  DALGO_GITHUB_ORG / DALGO_ORG_ADMIN_PAT"
echo "  TRIALS_RDS_*            only if running the trial flow"
