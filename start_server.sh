#!/bin/bash

# Start the Gunicorn server with optimized settings for large file uploads
gunicorn --bind 0.0.0.0:5000 \
         --timeout 300 \
         --workers 2 \
         --threads 4 \
         --reuse-port \
         --reload \
         main:app