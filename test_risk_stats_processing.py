#!/usr/bin/env python3
"""
Test script for risk statistics processing with improved batch insertion.
This script tests the optimized process for handling large Excel files with risk statistics.
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('risk_stats_test.log')
    ]
)

logger = logging.getLogger(__name__)

# Add current directory to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database import get_db, close_db
from src.services.egnyte_service import download_risk_stats_file
from src.models.models import EgnyteRiskStat
from src.services.upsert_helper import batch_upsert_risk_stats, clean_risk_stats_date

def test_risk_stats_processing(use_test_file=True, batch_size=50, max_retries=3):
    """
    Test the risk statistics processing with improved batch insertion.
    
    Args:
        use_test_file (bool): Whether to use a test file instead of downloading from Egnyte
        batch_size (int): Size of batches for processing
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        dict: Summary of the test results
    """
    start_time = time.time()
    logger.info(f"Starting risk stats processing test (batch_size={batch_size}, max_retries={max_retries})")
    
    # Get database session
    db = get_db()
    
    try:
        # Initialize counters
        equity_count = 0
        fi_count = 0
        alt_count = 0
        error_count = 0
        
        # Set the import date
        import_date = datetime.now().date()
        
        # 1. Clean up any existing records for today
        logger.info(f"Cleaning up any existing records for {import_date}")
        if not clean_risk_stats_date(db, import_date):
            logger.error("Failed to clean up existing records - aborting test")
            return {
                "success": False,
                "error": "Database cleanup failed"
            }
        
        # 2. Download the risk stats file
        logger.info("Downloading risk stats file")
        file_path = download_risk_stats_file(use_test_file=use_test_file)
        
        if not file_path or not os.path.exists(file_path):
            logger.error("Failed to download risk stats file")
            return {
                "success": False,
                "error": "File download failed"
            }
        
        file_size = os.path.getsize(file_path)
        logger.info(f"Successfully downloaded file to {file_path} ({file_size/1024/1024:.2f} MB)")
        
        # 3. Process the Excel file
        try:
            import pandas as pd
            from src.services.egnyte_service import process_excel_file
            
            logger.info("Analyzing Excel file structure")
            
            # Read the file to identify sheets
            with pd.ExcelFile(file_path) as xls:
                sheet_names = xls.sheet_names
                logger.info(f"Found sheets: {sheet_names}")
                
                # Process each sheet
                equity_sheet = None
                fi_sheet = None
                alt_sheet = None
                
                # Try to identify sheets by name
                for sheet in sheet_names:
                    if sheet.lower() in ['equity', 'equities', 'eq']:
                        equity_sheet = sheet
                    elif sheet.lower() in ['fixed income', 'fixed_income', 'fi', 'fixed inc', 'fixed inc.']:
                        fi_sheet = sheet
                    elif sheet.lower() in ['alts', 'alternatives', 'alt', 'alternative']:
                        alt_sheet = sheet
                
                logger.info(f"Identified sheets - Equity: {equity_sheet}, Fixed Income: {fi_sheet}, Alternatives: {alt_sheet}")
                
                # Process Equity sheet
                if equity_sheet:
                    logger.info(f"Processing Equity sheet: {equity_sheet}")
                    equity_records = []
                    
                    # Read sheet in chunks to avoid memory issues
                    chunk_size = 1000
                    sheet_obj = xls.book.sheet_by_name(equity_sheet)
                    total_rows = sheet_obj.nrows - 1  # Subtract header row
                    
                    # Get column headers
                    columns = pd.read_excel(xls, sheet_name=equity_sheet, nrows=1).columns.tolist()
                    logger.info(f"Equity sheet has {total_rows} rows and columns: {columns}")
                    
                    # Process in chunks
                    for chunk_start in range(1, total_rows + 1, chunk_size):
                        chunk_end = min(chunk_start + chunk_size - 1, total_rows + 1)
                        logger.info(f"Reading equity rows {chunk_start}-{chunk_end}")
                        
                        chunk = pd.read_excel(
                            xls, 
                            sheet_name=equity_sheet,
                            skiprows=range(1, chunk_start),
                            nrows=(chunk_end - chunk_start + 1)
                        )
                        
                        for idx, row in chunk.iterrows():
                            try:
                                if pd.isna(row.get('Position')):
                                    continue
                                
                                position = str(row.get('Position')).strip()
                                ticker = str(row.get('Ticker Symbol')).strip() if not pd.isna(row.get('Ticker Symbol')) else None
                                
                                # Create record
                                record = EgnyteRiskStat(
                                    import_date=import_date,
                                    position=position,
                                    ticker_symbol=ticker,
                                    asset_class='Equity',
                                    second_level=str(row.get('Second Level')).strip() if not pd.isna(row.get('Second Level')) else None,
                                    volatility=float(row.get('Vol')) if not pd.isna(row.get('Vol')) else None,
                                    beta=float(row.get('BETA')) if not pd.isna(row.get('BETA')) else None,
                                    notes=str(row.get('Notes')).strip() if not pd.isna(row.get('Notes')) else None,
                                    amended_id=str(row.get('Amended ID')).strip() if not pd.isna(row.get('Amended ID')) else None,
                                    source_file=os.path.basename(file_path),
                                    source_tab=equity_sheet,
                                    source_row=idx + chunk_start
                                )
                                equity_records.append(record)
                            except Exception as e:
                                logger.error(f"Error processing equity row {idx + chunk_start}: {e}")
                                error_count += 1
                    
                    # Process equity records with batch upsert
                    if equity_records:
                        logger.info(f"Upserting {len(equity_records)} equity records")
                        success, errors = batch_upsert_risk_stats(db, equity_records, batch_size, max_retries)
                        equity_count = success
                        error_count += errors
                        logger.info(f"Equity processing complete: {success} successful, {errors} errors")
                
                # Process Fixed Income sheet (similar to Equity)
                # ... (similar code for Fixed Income sheet)
                
                # Process Alternatives sheet (similar to Equity)
                # ... (similar code for Alternatives sheet)
                
            logger.info(f"Processing complete - Equity: {equity_count}, Fixed Income: {fi_count}, Alternatives: {alt_count}, Errors: {error_count}")
            
            # Clean up the file
            if os.path.exists(file_path):
                os.unlink(file_path)
                logger.info(f"Removed temporary file {file_path}")
            
            end_time = time.time()
            duration = end_time - start_time
            
            return {
                "success": True,
                "stats": {
                    "equity": equity_count,
                    "fixed_income": fi_count,
                    "alternatives": alt_count,
                    "errors": error_count,
                    "total": equity_count + fi_count + alt_count,
                    "duration_seconds": duration,
                    "records_per_second": (equity_count + fi_count + alt_count) / duration if duration > 0 else 0
                }
            }
            
        except Exception as e:
            logger.exception(f"Error processing Excel file: {e}")
            return {
                "success": False,
                "error": f"Excel processing error: {str(e)}"
            }
        
    except Exception as e:
        logger.exception(f"Test failed with error: {e}")
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        close_db()

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Test risk statistics processing')
    parser.add_argument('--test-file', action='store_true', help='Use test file instead of downloading from Egnyte')
    parser.add_argument('--batch-size', type=int, default=50, help='Size of batches for processing')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retry attempts')
    
    args = parser.parse_args()
    
    result = test_risk_stats_processing(
        use_test_file=args.test_file,
        batch_size=args.batch_size,
        max_retries=args.max_retries
    )
    
    if result.get('success'):
        logger.info("Test completed successfully")
        logger.info(f"Stats: {result.get('stats')}")
        return 0
    else:
        logger.error(f"Test failed: {result.get('error')}")
        return 1

if __name__ == '__main__':
    sys.exit(main())