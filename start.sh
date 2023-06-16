#!/bin/sh

# /home/ddp/DDP_backend/venv/bin/gunicorn -b localhost:8002 ddpui.wsgi \
#     --capture-output \
#     --error-logfile /home/ddp/DDP_backend/ddpui/logs/gunicorn-error.log \
#     --access-logfile /home/ddp/DDP_backend/ddpui/logs/gunicorn-access.log

/home/ddp/DDP_backend/venv/bin/gunicorn -b localhost:8002 ddpui.wsgi \
    --capture-output \
    --log-config /home/ddp/DDP_backend/gunicorn-log.conf
