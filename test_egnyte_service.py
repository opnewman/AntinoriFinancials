#!/usr/bin/env python3
"""
Test script for Egnyte service integration.
This script tests the ability to download and process the security risk stats file from Egnyte.
"""

import os
import sys
import logging
import pandas as pd
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
from src.services.egnyte_service import process_equity_sheet, process_fixed_income_sheet, process_alternatives_sheet
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
            
            # Examine the file structure first
            logger.info("Examining Excel file structure...")
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            logger.info(f"Found sheets: {sheet_names}")
            
            # Process a small sample of each sheet
            import_date = date.today()
            with get_db_connection() as db:
                # Get current count of risk stats
                before_count = db.query(EgnyteRiskStat).count()
                logger.info(f"Current risk stats count: {before_count}")
                
                equity_count = 0
                fixed_income_count = 0
                alternatives_count = 0
                
                # Process a few rows from each sheet to test functionality
                for sheet_name in sheet_names:
                    if isinstance(sheet_name, str) and sheet_name.lower() == 'equity':
                        logger.info(f"Processing sample from {sheet_name} sheet...")
                        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=10)
                        logger.info(f"Sample columns: {df.columns.tolist()}")
                        logger.info(f"Sample data: {df.iloc[0].to_dict()}")
                        equity_count = process_equity_sheet(file_path, sheet_name, import_date, db)
                        logger.info(f"Processed {equity_count} equity records")
                    
                    elif isinstance(sheet_name, str) and sheet_name.lower() == 'fixed income':
                        logger.info(f"Processing sample from {sheet_name} sheet...")
                        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=10)
                        logger.info(f"Sample columns: {df.columns.tolist()}")
                        logger.info(f"Sample data: {df.iloc[0].to_dict()}")
                        fixed_income_count = process_fixed_income_sheet(file_path, sheet_name, import_date, db)
                        logger.info(f"Processed {fixed_income_count} fixed income records")
                    
                    elif isinstance(sheet_name, str) and sheet_name.lower() == 'alternatives':
                        logger.info(f"Processing sample from {sheet_name} sheet...")
                        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=10)
                        logger.info(f"Sample columns: {df.columns.tolist()}")
                        logger.info(f"Sample data: {df.iloc[0].to_dict()}")
                        alternatives_count = process_alternatives_sheet(file_path, sheet_name, import_date, db)
                        logger.info(f"Processed {alternatives_count} alternatives records")
                
                total_count = equity_count + fixed_income_count + alternatives_count
                logger.info(f"Processed {total_count} total records")
                
                # Get new count of risk stats
                after_count = db.query(EgnyteRiskStat).count()
                logger.info(f"New risk stats count: {after_count}")
                logger.info(f"Records added: {after_count - before_count}")
                
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