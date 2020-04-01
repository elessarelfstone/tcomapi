#!/usr/bin/env bash
cd /home/dataeng/tcomapi/
source ./venv/bin/activate
exec luigi --module "$@"
