#!/usr/bin/env python3
"""
Test script for Egnyte API integration.
This script tests the ability to download the security risk stats file from Egnyte.
"""

import os
import sys
import logging
import requests
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Egnyte API configuration
EGNYTE_DOMAIN = os.environ.get("EGNYTE_DOMAIN", "procapitalpartners.egnyte.com")
EGNYTE_ACCESS_TOKEN = os.environ.get("EGNYTE_ACCESS_TOKEN")
FILE_PATH = os.environ.get(
    "EGNYTE_RISK_STATS_PATH", 
    "/Shared/Internal Documents/Proficio Capital Partners/Asset Allocation/Portfolio Management/New Portfolio Sheets/Security Risk Stats.xlsx"
)


def test_api_connection():
    """Test basic connection to Egnyte API."""
    if not EGNYTE_ACCESS_TOKEN:
        logger.error("EGNYTE_ACCESS_TOKEN not found in environment variables")
        return False
        
    url = f"https://{EGNYTE_DOMAIN}/pubapi/v1/userinfo"
    headers = {"Authorization": f"Bearer {EGNYTE_ACCESS_TOKEN}"}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info("Successfully connected to Egnyte API")
            logger.info(f"User info: {response.json()}")
            return True
        else:
            logger.error(f"Failed to connect to Egnyte API: HTTP {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
    except Exception as e:
        logger.exception(f"Error connecting to Egnyte API: {str(e)}")
        return False


def test_file_download():
    """Test downloading the risk stats file from Egnyte."""
    if not EGNYTE_ACCESS_TOKEN:
        logger.error("EGNYTE_ACCESS_TOKEN not found in environment variables")
        return False
        
    url = f"https://{EGNYTE_DOMAIN}/pubapi/v1/fs-content{FILE_PATH}"
    headers = {"Authorization": f"Bearer {EGNYTE_ACCESS_TOKEN}"}
    
    try:
        logger.info(f"Attempting to download file from: {url}")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            # Create a temporary file to store the downloaded content
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            
            # Check file size
            file_size = os.path.getsize(temp_file_path)
            logger.info(f"Successfully downloaded file to {temp_file_path}")
            logger.info(f"File size: {file_size} bytes")
            
            # Clean up
            os.unlink(temp_file_path)
            return True
        else:
            logger.error(f"Failed to download file: HTTP {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
    except Exception as e:
        logger.exception(f"Error downloading file: {str(e)}")
        return False


def main():
    """Main test function."""
    logger.info("Starting Egnyte API tests")
    
    # Test API connection
    logger.info("Testing API connection...")
    connection_success = test_api_connection()
    
    if connection_success:
        # Test file download
        logger.info("Testing file download...")
        download_success = test_file_download()
        
        if download_success:
            logger.info("All tests passed!")
            return 0
        else:
            logger.error("File download test failed!")
            return 1
    else:
        logger.error("API connection test failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())