#!/usr/bin/env python3
"""
Schedule script for risk statistics updates.
This script will schedule a daily risk statistics update job to run at 8 AM.
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("risk_stats_scheduler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import the service
from src.database import get_db_connection
from src.services.egnyte_service import fetch_and_process_risk_stats


def update_risk_stats_job():
    """Job to update risk statistics from Egnyte."""
    logger.info("Starting scheduled risk statistics update...")
    try:
        # Default to conservative values for batch processing
        batch_size = 50
        max_retries = 3
        
        logger.info(f"Using batch size: {batch_size}, max retries: {max_retries}")
        
        with get_db_connection() as db:
            result = fetch_and_process_risk_stats(db, batch_size=batch_size, max_retries=max_retries)
            if result.get('success'):
                logger.info(f"Risk statistics update completed successfully. Imported {result.get('stats', {}).get('total_records', 0)} records.")
            else:
                logger.error(f"Risk statistics update failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        logger.exception(f"Error in scheduled risk statistics update: {str(e)}")


def main():
    """Main function to set up and run the scheduler."""
    logger.info("Starting risk statistics update scheduler")
    
    # Create scheduler
    scheduler = BackgroundScheduler()
    
    # Add job to run at 8:00 AM every day
    scheduler.add_job(
        update_risk_stats_job,
        trigger=CronTrigger(hour=8, minute=0),
        id='risk_stats_update',
        name='Daily risk statistics update',
        replace_existing=True
    )
    
    # Log the next run time
    # We need to start the scheduler before accessing next_run_time
    scheduler.start()
    logger.info("Scheduler started successfully")
    
    # Now we can safely access next_run_time
    job = scheduler.get_job('risk_stats_update')
    if job and hasattr(job, 'next_run_time'):
        next_run = job.next_run_time
        logger.info(f"Next scheduled run: {next_run}")
    else:
        logger.info("Job scheduled, but next run time is not available yet")
    
    try:
        # Scheduler is already started above
        
        # Run the job immediately for the first time
        logger.info("Running initial risk statistics update...")
        update_risk_stats_job()
        
        # Keep the script running
        while True:
            # Sleep for a day to avoid excessive CPU usage
            time.sleep(86400)  # 24 hours
            
    except (KeyboardInterrupt, SystemExit):
        # Shut down the scheduler gracefully
        scheduler.shutdown()
        logger.info("Scheduler shut down")
    except Exception as e:
        logger.exception(f"Error in scheduler: {str(e)}")
        scheduler.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()