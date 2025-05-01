#!/usr/bin/env python
"""
Service script for ANTINORI Financial Portfolio Reporting System.
This script can run either the Flask app (for testing) or integrate with FastAPI.
"""
import os
import logging
from flask_app import app

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# The Flask app is imported and ready to use with gunicorn

if __name__ == "__main__":
    # Get port from environment or use default
    port = int(os.environ.get("PORT", 5000))
    
    # Run the Flask app for development
    app.run(host="0.0.0.0", port=port, debug=True)