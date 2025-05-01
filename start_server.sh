#!/bin/bash
# Start the FastAPI server using Uvicorn directly
python -m uvicorn main:app --host 0.0.0.0 --port 5000