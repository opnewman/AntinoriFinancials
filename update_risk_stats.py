#!/usr/bin/env python3
"""
Standalone script for downloading and processing risk statistics from Egnyte.
This script runs independently to avoid web server timeouts.

Usage:
    python update_risk_stats.py [--test] [--debug] [--batch-size=100] [--max-retries=3] [--output=output.log]

Options:
    --test         Use test file instead of downloading from Egnyte
    --debug        Enable debug logging
    --batch-size   Size of batches for database operations (default: 100)
    --max-retries  Maximum number of retry attempts for database operations (default: 3)
    --output       Output file for logs (default: risk_stats_update.log)
"""

import argparse
import logging
import os
import sys
import traceback
from datetime import datetime

# Add the current directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

from src.database import get_db
from src.services.egnyte_service import fetch_and_process_risk_stats
from sqlalchemy import text
from src.models.models import EgnyteRiskStat

def setup_logging(output_file=None, debug=False):
    """Setup logging configuration"""
    log_level = logging.DEBUG if debug else logging.INFO
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if requested
    if output_file:
        file_handler = logging.FileHandler(output_file)
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Set specific logger levels
    logging.getLogger('src').setLevel(log_level)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    
    return root_logger

def clean_database(db, date=None):
    """
    Clean out any existing risk statistics records for a specific date.
    If no date is provided, uses today's date.
    """
    logger = logging.getLogger(__name__)
    
    if date is None:
        date = datetime.now().date()
    
    try:
        # Count records by asset class
        equity_count = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == date,
            EgnyteRiskStat.asset_class == 'Equity'
        ).count()
        
        fixed_income_count = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == date,
            EgnyteRiskStat.asset_class == 'Fixed Income'
        ).count()
        
        alternatives_count = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == date,
            EgnyteRiskStat.asset_class == 'Alternatives'
        ).count()
        
        total_count = equity_count + fixed_income_count + alternatives_count
        
        logger.info(f"Found {total_count} existing records for date {date}")
        logger.info(f"By asset class - Equity: {equity_count}, Fixed Income: {fixed_income_count}, Alternatives: {alternatives_count}")
        
        if total_count > 0:
            logger.info(f"Cleaning up {total_count} existing records for date {date}")
            
            # Delete records using raw SQL for better performance
            for asset_class in ['Equity', 'Fixed Income', 'Alternatives']:
                sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :date AND asset_class = :asset_class")
                result = db.execute(sql, {"date": date, "asset_class": asset_class})
                logger.info(f"Deleted {result.rowcount} {asset_class} records")
                db.commit()
            
            # Verify deletion
            remaining = db.query(EgnyteRiskStat).filter(EgnyteRiskStat.import_date == date).count()
            if remaining > 0:
                logger.warning(f"Still found {remaining} records after cleanup - performing final delete")
                sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :date")
                result = db.execute(sql, {"date": date})
                logger.info(f"Deleted {result.rowcount} remaining records")
                db.commit()
            
            logger.info("Database cleanup completed successfully")
        else:
            logger.info("No existing records to clean up")
        
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Error cleaning database: {e}")
        return False

def main():
    """Main entry point for the script"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Update risk statistics from Egnyte')
    parser.add_argument('--test', action='store_true', help='Use test file instead of downloading from Egnyte')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--batch-size', type=int, default=100, help='Size of batches for database operations')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retry attempts for database operations')
    parser.add_argument('--output', type=str, default='risk_stats_update.log', help='Output file for logs')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(output_file=args.output, debug=args.debug)
    
    logger.info("Starting risk statistics update script")
    logger.info(f"Parameters: test={args.test}, debug={args.debug}, batch_size={args.batch_size}, max_retries={args.max_retries}")
    
    # Get database session
    db = get_db()
    
    try:
        # Clean out existing records
        logger.info("Cleaning existing records")
        if not clean_database(db):
            logger.error("Failed to clean database - aborting")
            return 1
        
        # Use the Egnyte service to update risk stats
        logger.info("Fetching and processing risk statistics")
        result = fetch_and_process_risk_stats(
            db, 
            use_test_file=args.test,
            batch_size=args.batch_size,
            max_retries=args.max_retries
        )
        
        if result.get('success', False):
            logger.info("Risk statistics update completed successfully")
            logger.info(f"Result: {result}")
            return 0
        else:
            logger.error(f"Risk statistics update failed: {result.get('error', 'Unknown error')}")
            return 1
    except Exception as e:
        logger.error(f"Error updating risk statistics: {str(e)}")
        logger.error(traceback.format_exc())
        return 1
    finally:
        # Close the session (no explicit close needed with context manager)
        if db:
            db.close()

if __name__ == '__main__':
    sys.exit(main())