#!/usr/bin/env python3
"""
Test script for Egnyte service integration.
This script tests the ability to download and process the security risk stats file from Egnyte.
"""

import os
import sys
import logging
from datetime import date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set the environment variable for the Egnyte API token
os.environ["EGNYTE_ACCESS_TOKEN"] = "pwnvw3mejvy3ptye3p3d4cqt"

# Import the database module after setting environment variables
from src.database import init_db, get_db_connection
from src.services.egnyte_service import download_risk_stats_file, process_excel_file
from src.models.models import EgnyteRiskStat


def main():
    """Main test function."""
    logger.info("Starting Egnyte service tests")
    
    # Initialize database
    init_db()
    
    try:
        # Test downloading the file
        logger.info("Downloading risk stats file...")
        file_path = download_risk_stats_file()
        
        if file_path:
            logger.info(f"File downloaded to {file_path}")
            
            # Process the file
            logger.info("Processing risk stats file...")
            with get_db_connection() as db:
                # Get current count of risk stats
                before_count = db.query(EgnyteRiskStat).count()
                logger.info(f"Current risk stats count: {before_count}")
                
                # Process the file
                stats = process_excel_file(file_path, db)
                
                # Get new count of risk stats
                after_count = db.query(EgnyteRiskStat).count()
                logger.info(f"New risk stats count: {after_count}")
                
                # Print processing stats
                logger.info(f"Processing stats: {stats}")
                logger.info(f"Records added: {after_count - before_count}")
                
                # Check latest import date
                latest_date = db.query(EgnyteRiskStat.import_date).order_by(
                    EgnyteRiskStat.import_date.desc()
                ).first()
                
                if latest_date:
                    logger.info(f"Latest import date: {latest_date[0]}")
                
                # Get count by asset class
                equity_count = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.asset_class == 'Equity'
                ).count()
                
                fixed_income_count = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.asset_class == 'Fixed Income'
                ).count()
                
                alternatives_count = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.asset_class == 'Alternatives'
                ).count()
                
                logger.info(f"Equity records: {equity_count}")
                logger.info(f"Fixed Income records: {fixed_income_count}")
                logger.info(f"Alternatives records: {alternatives_count}")
                
                # Sample records from each asset class
                logger.info("Sample Equity record:")
                equity_sample = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.asset_class == 'Equity'
                ).first()
                if equity_sample:
                    logger.info(f"  Position: {equity_sample.position}")
                    logger.info(f"  Ticker: {equity_sample.ticker_symbol}")
                    logger.info(f"  Beta: {equity_sample.beta}")
                    logger.info(f"  Volatility: {equity_sample.volatility}")
                
                logger.info("Sample Fixed Income record:")
                fi_sample = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.asset_class == 'Fixed Income'
                ).first()
                if fi_sample:
                    logger.info(f"  Position: {fi_sample.position}")
                    logger.info(f"  Duration: {fi_sample.duration}")
                
                logger.info("Sample Alternatives record:")
                alt_sample = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.asset_class == 'Alternatives'
                ).first()
                if alt_sample:
                    logger.info(f"  Position: {alt_sample.position}")
                    logger.info(f"  Beta: {alt_sample.beta}")
            
            # Clean up the temporary file
            try:
                os.unlink(file_path)
                logger.info(f"Temporary file {file_path} removed")
            except Exception as e:
                logger.warning(f"Failed to remove temporary file {file_path}: {e}")
            
            logger.info("Test completed successfully!")
            return 0
        else:
            logger.error("Failed to download risk stats file")
            return 1
    
    except Exception as e:
        logger.exception(f"Error in Egnyte service test: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())