#!/usr/bin/env python3
"""
Wrapper script to run the data upload in a separate process
"""
import sys
import os
import subprocess
import argparse
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("run_data_upload.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Run data dump processing")
    parser.add_argument("file_path", help="Path to the Excel file to process")
    args = parser.parse_args()
    
    # Check if file exists
    if not os.path.exists(args.file_path):
        logger.error(f"File not found: {args.file_path}")
        sys.exit(1)
    
    # Run the data dump processor in a detached process
    logger.info(f"Starting data dump processing for file: {args.file_path}")
    
    # Using nohup to keep the process running even if parent terminates
    cmd = [
        "nohup", 
        "python", 
        "upload_data_dump.py", 
        args.file_path,
        "&"
    ]
    
    # Join the command into a string for shell execution
    cmd_str = " ".join(cmd)
    
    try:
        # Execute the command
        os.system(cmd_str)
        logger.info(f"Data dump processing started in background")
        
        # Create a status file to indicate process has started
        status_dir = os.path.dirname(args.file_path)
        status_file = os.path.join(status_dir, "data_dump_started.txt")
        
        with open(status_file, "w") as f:
            f.write(f"Processing started at: {os.path.basename(args.file_path)}\n")
        
        logger.info(f"Status file created: {status_file}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error starting data dump process: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()