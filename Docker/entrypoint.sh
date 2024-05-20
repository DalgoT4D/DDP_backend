#!/bin/bash


# Apply database migrations
echo "Apply database migrations"
python manage.py migrate

echo "Seed database"
python manage.py loaddata seed/*.json

echo "Create first user ${FIRST_USER_EMAIL} in organization ${FIRST_ORG_NAME}"
python manage.py createadminuser --email ${FIRST_USER_EMAIL} --password "password"
python manage.py createorganduser ${FIRST_ORG_NAME} ${FIRST_USER_EMAIL}

# Start server
echo "Starting server"


python3 -m gunicorn -b 0.0.0.0:8002 ddpui.wsgi \
    --capture-output \
    --log-config /usr/src/backend/gunicorn-log.conf \
    --timeout 120
