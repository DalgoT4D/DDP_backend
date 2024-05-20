#!/bin/bash
set -e

# Function to add any cleanup actions
function cleanup() {
	echo "Cleanup."
}
trap cleanup EXIT

# Initialize gcloud


function get_secret_value() {
    local secret_name="$1" key="$2" form="$3" region="$4"
  	: "${json_secret:=$(gcloud secrets versions access latest --secret="$secret_name")}"
    local value=$(echo "$json_secret")
    if [ "$form" == "base64" ]; then
        echo "$value" | tr '_-' '/+' | base64 -d
    else
        echo "$value"
    fi
    

}

echo "Fetching variables from Secret Manager..."

# Fetch secret from Secret Manager at runtime
export DJANGOSECRET=$(get_secret_value "DJANGOSECRET" "payload" "" "us-central1")

export DEBUG=$(get_secret_value "DEBUG" "payload" "" "us-central1")
export PRODUCTION=$(get_secret_value "PRODUCTION" "payload" "" "us-central1")
export DEV_SECRETS_DIR=$(get_secret_value "DEV_SECRETS_DIR" "payload" "" "us-central1")
export USE_AWS_SECRETS_MANAGER=$(get_secret_value "USE_AWS_SECRETS_MANAGER" "payload" "" "us-central1")

export DBNAME=$(get_secret_value "DBNAME" "payload" "" "us-central1")
export DBUSER=$(get_secret_value "DBUSER" "payload" "" "us-central1")
export DBPASSWORD=$(get_secret_value "DBPASSWORD" "payload" "" "us-central1")
export DBHOST=$(get_secret_value "DBHOST" "payload" "" "us-central1")
export DBPORT=$(get_secret_value "DBPORT" "payload" "" "us-central1")
export DBADMINUSER=$(get_secret_value "DBADMINUSER" "payload" "" "us-central1")
export DBADMINPASSWORD=$(get_secret_value "DBADMINPASSWORD" "payload" "" "us-central1")

export AWS_ACCESS_KEY_ID=$(get_secret_value "AWS_ACCESS_KEY_ID" "payload" "" "us-central1")
export AWS_SECRET_ACCESS_KEY=$(get_secret_value "AWS_SECRET_ACCESS" "payload" "" "us-central1")
export AWS_DEFAULT_REGION=$(get_secret_value "AWS_DEFAULT_REGION" "payload" "" "us-central1")

export SES_ACCESS_KEY_ID=$(get_secret_value "SES_ACCESS_KEY_ID" "payload" "" "us-central1")
export SES_SECRET_ACCESS_KEY=$(get_secret_value "SES_SECRET_ACCESS_KEY" "payload" "" "us-central1")
export SES_SENDER_EMAIL=$(get_secret_value "SES_SENDER_EMAIL" "payload" "" "us-central1")

export AIRBYTE_SERVER_HOST=$(get_secret_value "AIRBYTE_SERVER_HOST" "payload" "" "us-central1")
export AIRBYTE_SERVER_PORT=$(get_secret_value "AIRBYTE_SERVER_PORT" "payload" "" "us-central1")
export AIRBYTE_SERVER_APIVER=$(get_secret_value "AIRBYTE_SERVER_APIVER" "payload" "" "us-central1")
export AIRBYTE_API_TOKEN=$(get_secret_value "AIRBYTE_API_TOKEN" "payload" "" "us-central1")
export AIRBYTE_DESTINATION_TYPES=$(get_secret_value "AIRBYTE_DESTINATION_TYPES" "payload" "" "us-central1")

export PREFECT_PROXY_API_URL=$(get_secret_value "PREFECT_PROXY_API_URL" "payload" "" "us-central1")
export PREFECT_HTTP_TIMEOUT=$(get_secret_value "PREFECT_HTTP_TIMEOUT" "payload" "" "us-central1")

export CLIENTDBT_ROOT=$(get_secret_value "CLIENTDBT_ROOT" "payload" "" "us-central1")
export DBT_VENV=$(get_secret_value "DBT_VENV" "payload" "" "us-central1")

export SIGNUPCODE=$(get_secret_value "SIGNUPCODE" "payload" "" "us-central1")
export CREATEORG_CODE=$(get_secret_value "CREATEORG_CODE" "payload" "" "us-central1")
export FRONTEND_URL=$(get_secret_value "FRONTEND_URL" "payload" "" "us-central1")

export SENDGRID_APIKEY=$(get_secret_value "SENDGRID_APIKEY" "payload" "" "us-central1")
export SENDGRID_SENDER=$(get_secret_value "SENDGRID_SENDER" "payload" "" "us-central1")
export SENDGRID_RESET_PASSWORD_TEMPLATE=$(get_secret_value "SENDGRID_RESET_PASSWORD_TEMPLATE" "payload" "" "us-central1")
export SENDGRID_SIGNUP_TEMPLATE=$(get_secret_value "SENDGRID_SIGNUP_TEMPLATE" "payload" "" "us-central1")
export SENDGRID_INVITE_USER_TEMPLATE=$(get_secret_value "SENDGRID_INVITE_USER_TEMPLATE" "payload" "" "us-central1")
export SENDGRID_YOUVE_BEEN_ADDED_TEMPLATE=$(get_secret_value "SENDGRID_YOUVE_BEEN_ADDED_TEMPLATE" "payload" "" "us-central1")

export PREFECT_NOTIFICATIONS_WEBHOOK_KEY=$(get_secret_value "PREFECT_NOTIFICATIONS_WEBHOOK_KEY" "payload" "" "us-central1")

export SUPERSET_USAGE_DASHBOARD_API_URL=$(get_secret_value "SUPERSET_USAGE_DASHBOARD_API_URL" "payload" "" "us-central1")
export SUPERSET_USAGE_CREDS_SECRET_ID=$(get_secret_value "SUPERSET_USAGE_CRED_SECRET_ID" "payload" "" "us-central1")

# First Org and User
export FIRST_ORG_NAME=$(get_secret_value "FIRST_ORG_NAME" "payload" "" "us-central1")
export ADMIN_USER_EMAIL=$(get_secret_value "ADMIN_USER_EMAIL" "payload" "" "us-central1")
export ADMIN_PASSWORD=$(get_secret_value "ADMIN_PASSWORD" "payload" "" "us-central1")



# DEMO ACCOUNTS
export DEMO_SIGNUPCODE=$(get_secret_value "DEMO_SIGNUPCODE" "payload" "" "us-central1")
export DEMO_AIRBYTE_SOURCE_TYPES=$(get_secret_value "DEMO_AIRBYTE_SOURCE_TYPES" "payload" "" "us-central1")
export DEMO_SENDGRID_SIGNUP_TEMPLATE=$(get_secret_value "DEMO_SENDGRID_SIGNUP_TEMPLATE" "payload" "" "us-central1")
export DEMO_SUPERSET_USERNAME=$(get_secret_value "DEMO_SUPERSET_USERNAME" "payload" "" "us-central1")
export DEMO_SUPERSET_PASSWORD=$(get_secret_value "DEMO_SUPERSET_PASSWORD" "payload" "" "us-central1")

# Execute the command provided as CMD in Dockerfile (e.g., start your server)
exec "$@"