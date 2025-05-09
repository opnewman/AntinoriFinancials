#!/bin/bash
# Start gunicorn with extended timeout
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload --timeout 300 main:app