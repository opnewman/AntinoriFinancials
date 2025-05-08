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
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

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
        with get_db_connection() as db:
            result = fetch_and_process_risk_stats(db)
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
    job = scheduler.get_job('risk_stats_update')
    if job:
        next_run = job.next_run_time
        logger.info(f"Next scheduled run: {next_run}")
    
    try:
        # Start the scheduler
        scheduler.start()
        logger.info("Scheduler started successfully")
        
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