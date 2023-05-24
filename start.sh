#!/bin/sh

/home/ddp/DDP_backend/venv/bin/gunicorn -b localhost:8002 ddpui.wsgi