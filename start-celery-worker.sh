#!/bin/sh

/home/ddp/DDP_backend/venv/bin/celery -A ddpui worker -n ddpui --pidfile celeryworker.pid