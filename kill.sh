#!/bin/sh

ps aux | grep "ddpui.wsgi" | grep -v grep | awk '{print $2}' | xargs kill
ps aux | grep "celery -A ddpui worker" | grep -v grep | awk '{print $2}' | xargs kill -9

