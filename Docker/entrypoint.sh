#!/bin/bash

# Apply database migrations

# create case of different containers
case "$1" in
    worker)
        echo "Starting celery worker"
        celery -A ddpui worker -n ddpui
        ;;
    beat)
        echo "Starting celery beat"
        celery -A ddpui beat --schedule=/data/celerybeat-schedule --loglevel=error --max-interval 60
        ;;
    backend)
        echo "Starting backend"

        # Start server
        echo "Starting server"
        python3 -m gunicorn -b 0.0.0.0:8002 ddpui.wsgi \
            --capture-output \
            --log-config /usr/src/backend/gunicorn-log.conf \
            --timeout 120
        ;;
    initdb)
        echo "Apply database migrations"
        python manage.py migrate

        echo "Seed database"
        python manage.py loaddata seed/*.json

        echo "Create first user ${FIRST_USER_EMAIL} in organization ${FIRST_ORG_NAME}"
        python manage.py createorganduser ${FIRST_ORG_NAME} ${FIRST_USER_EMAIL} ${FIRST_USER_PASSWORD} --role ${FIRST_USER_ROLE}

        echo "Create system user if it does not exist"
        python manage.py create-system-orguser
        ;;
    *)
        exec "$@"
esac