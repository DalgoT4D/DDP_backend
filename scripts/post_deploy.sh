#!/usr/bin/env bash
# Usage: ./scripts/post_deploy.sh [--with-default-org [orgname] [email] [password]]
# --with-default-org creates org + admin user (skipped by default).
# Omitted args fall back to ORG_NAME / ADMIN_EMAIL / PASSWORD env vars,
# then prompt interactively.
set -euo pipefail

SKIP_ORG=true
if [[ "${1:-}" == "--with-default-org" ]]; then
  SKIP_ORG=false
  shift
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> migrate"
uv run python manage.py migrate

echo "==> loaddata seeds"
uv run python manage.py loaddata seed/*.json

echo "==> create-system-orguser"
uv run python manage.py create-system-orguser

echo "==> clear redis permission key"
uv run python manage.py clear_role_permissions

# ---- org + admin user ----
if [[ "$SKIP_ORG" == true ]]; then
  echo "==> skipping org + admin user (use --with-default-org to create)"
  echo "Done."
  exit 0
fi

ORG_NAME="${1:-${ORG_NAME:-}}"
ADMIN_EMAIL="${2:-${ADMIN_EMAIL:-}}"
ADMIN_PASSWORD="${3:-${PASSWORD:-}}"

if [[ -z "$ORG_NAME" ]]; then
  read -rp "Org name: " ORG_NAME
fi
if [[ -z "$ADMIN_EMAIL" ]]; then
  read -rp "Admin email: " ADMIN_EMAIL
fi
# password left empty → createorganduser prompts via getpass

echo "==> create org '${ORG_NAME}' and admin user '${ADMIN_EMAIL}'"
if [[ -n "$ADMIN_PASSWORD" ]]; then
  PASSWORD="$ADMIN_PASSWORD" uv run python manage.py createorganduser "$ORG_NAME" "$ADMIN_EMAIL"
else
  uv run python manage.py createorganduser "$ORG_NAME" "$ADMIN_EMAIL"
fi

echo "Done."
