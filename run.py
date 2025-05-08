#!/usr/bin/env python
"""
Server startup script for the nori Financial Portfolio Reporting API.
This script correctly starts a uvicorn server suitable for FastAPI applications.
"""
import os
import uvicorn

if __name__ == "__main__":
    # Get the port from environment variable or use default
    port = int(os.environ.get("PORT", 5000))
    
    # Run the server using uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    )