# This script automates the deployment steps for the DDP_backend application.
# It assumes it's being run on the target EC2 server itself, or via an SSH connection.
# It mirrors the logic currently found in .github/workflows/dalgo-cd.yml

echo "Starting DDP_backend deployment script..."

# Ensure script exits immediately if any command fails
set -e

# --- Configuration Variables (will need to be set in target environment) ---
# These paths are based on the GitHub Actions workflow.
# Adjust if your target server's setup is different.
DDP_BACKEND_DIR="/home/ddp/DDP_backend"
NVM_SH_PATH="/home/ddp/.nvm/nvm.sh" # Path to nvm.sh if Node/pm2 are used
UV_RUN_PATH="/home/ddp/.local/bin/uv" # Path to uv executable
PM2_PATH="/home/ddp/.yarn/bin/pm2" # Path to pm2 executable

# --- Check if essential commands exist ---
if [ ! -f "$NVM_SH_PATH" ]; then
    echo "Error: NVM script not found at $NVM_SH_PATH. Please ensure Node.js/NVM is installed."
    exit 1
fi
if [ ! -f "$UV_RUN_PATH" ]; then
    echo "Error: 'uv' executable not found at $UV_RUN_PATH. Please ensure 'uv' is installed."
    exit 1
fi
if [ ! -f "$PM2_PATH" ]; then
    echo "Error: 'pm2' executable not found at $PM2_PATH. Please ensure 'pm2' is installed."
    exit 1
fi

# --- Source NVM (if used for pm2) ---
echo "Sourcing NVM..."
source "$NVM_SH_PATH"

# --- Navigate to the application directory ---
echo "Changing to application directory: $DDP_BACKEND_DIR"
cd "$DDP_BACKEND_DIR"

# --- Git Pull Latest Changes ---
echo "Checking current Git branch..."
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "main" ]; then
    echo "Error: Not on the 'main' branch. Current branch is '$current_branch'. Please switch to main."
    exit 1
fi

echo "Pulling latest changes from Git..."
git pull origin main # Explicitly pull from origin main
echo "Git pull completed."

# --- Run Django Migrations ---
echo "Running Django database migrations..."
"$UV_RUN_PATH" run python manage.py migrate
echo "Migrations completed."

# --- Load Seed Data (if applicable) ---
echo "Loading seed data..."
"$UV_RUN_PATH" run python manage.py loaddata seed/*.json
echo "Seed data loaded."

# --- Restart Application Processes using PM2 ---
echo "Restarting Django processes using PM2..."
"$PM2_PATH" restart django-celery-worker django-backend-asgi
echo "Application processes restarted."

echo "DDP_backend deployment script finished successfully!"