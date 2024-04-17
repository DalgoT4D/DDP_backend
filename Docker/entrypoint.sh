#!/bin/bash

# Apply database migrations
echo "Apply database migrations"
python manage.py migrate

# Start server
echo "Starting server"


python3 -m gunicorn -b 0.0.0.0:8002 ddpui.wsgi \
    --capture-output \
    --log-config /usr/src/backend/gunicorn-log.conf