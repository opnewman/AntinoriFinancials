"""
Direct service for uploading and processing risk statistics data from Excel files.
This uses a more efficient approach with separate tables for each asset class.
"""

import os
import sys
import time
import logging
import traceback
import tempfile
from datetime import date, datetime
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.models.models import RiskStatisticEquity, RiskStatisticFixedIncome, RiskStatisticAlternatives
from src.services.egnyte_service import download_risk_stats_file

logger = logging.getLogger(__name__)

def process_risk_stats_direct(
    db: Session, 
    use_test_file: bool = False, 
    batch_size: int = 500,
    debug: bool = False
) -> Dict[str, Any]:
    """
    Direct implementation for processing risk statistics from Egnyte.
    
    This implementation uses separate tables for each asset class and simplified
    database operations for maximum stability and performance.
    
    Args:
        db: Database session
        use_test_file: Whether to use a test file instead of downloading
        batch_size: Size of batches for database operations
        debug: Enable detailed debug logging
        
    Returns:
        Dict with processing results including timing and record counts
    """
    # 1. Set up tracing and performance metrics
    logger.info("Starting direct risk stats processing")
    start_time = time.time()
    import_date = date.today()
    
    # Initialize result structure
    results = {
        "success": True,
        "start_time": str(datetime.now()),
        "total_records": 0,
        "equity_records": 0,
        "fixed_income_records": 0,
        "alternatives_records": 0,
        "processing_time_seconds": 0,
        "error": None
    }
    
    # 2. Download the file from Egnyte
    file_path = None
    try:
        # Add extensive debug info
        logger.info("DEBUG: ========== STARTING RISK STATS FILE DOWNLOAD ==========")
        logger.info(f"DEBUG: Parameters - use_test_file={use_test_file}, domain=None")
        
        # Check environment variables
        egnyte_token = os.environ.get('EGNYTE_ACCESS_TOKEN')
        egnyte_domain = os.environ.get('EGNYTE_DOMAIN')
        egnyte_path = os.environ.get('EGNYTE_RISK_STATS_PATH') 
        local_test_file = os.environ.get('LOCAL_RISK_STATS_FILE')
        
        # Log for debugging
        logger.info(f"DEBUG: Environment variables: EGNYTE_ACCESS_TOKEN=EXISTS: {egnyte_token is not None}")
        logger.info(f"DEBUG: Environment variables: EGNYTE_DOMAIN=EXISTS: {egnyte_domain is not None}")
        logger.info(f"DEBUG: Environment variables: EGNYTE_RISK_STATS_PATH=EXISTS: {egnyte_path is not None}")  
        logger.info(f"DEBUG: Environment variables: LOCAL_RISK_STATS_FILE=EXISTS: {local_test_file is not None}")
        
        if use_test_file and local_test_file and os.path.exists(local_test_file):
            logger.info(f"Using local test file: {local_test_file}")
            file_path = local_test_file
        else:
            file_path = download_risk_stats_file(use_test_file=use_test_file)
            logger.info(f"Risk statistics file downloaded to: {file_path} ({os.path.getsize(file_path)} bytes)")
        
        # 3. Process the file
        logger.info(f"Successfully downloaded file ({os.path.getsize(file_path) / 1024 / 1024:.2f} MB)")
        
        # Recreate tables completely to ensure schema consistency
        try:
            # First, drop the tables if they exist
            from sqlalchemy.ext.declarative import declarative_base
            Base = declarative_base()
            
            logger.info("Recreating risk statistics tables to ensure schema consistency")
            db.execute(text("DROP TABLE IF EXISTS risk_statistic_alternatives CASCADE"))
            db.execute(text("DROP TABLE IF EXISTS risk_statistic_fixed_income CASCADE"))
            db.execute(text("DROP TABLE IF EXISTS risk_statistic_equity CASCADE"))
            db.commit()
            
            # Import the Base class with our models
            from src.models.models import Base
            
            # Force SQLAlchemy to create tables based on our models
            Base.metadata.create_all(bind=db.get_bind(), tables=[
                RiskStatisticEquity.__table__,
                RiskStatisticFixedIncome.__table__,
                RiskStatisticAlternatives.__table__
            ])
            db.commit()
            
            logger.info("Risk statistics tables recreated successfully")
        except Exception as recreate_error:
            logger.error(f"Error recreating tables: {recreate_error}")
            logger.error(traceback.format_exc())
            db.rollback()
            results["success"] = False
            results["error"] = f"Schema initialization error: {str(recreate_error)}"
            return results
        
        # Read the Excel file
        logger.info("Reading Excel file")
        xls = pd.ExcelFile(file_path)
        sheets = xls.sheet_names
        logger.info(f"Found sheets: {sheets}")
        
        # Identify the tabs we need
        sheets_dict = {}
        for sheet in sheets:
            # Handle case variations in sheet names
            if 'equity' in sheet.lower():
                sheets_dict['Equity'] = sheet
            elif 'fixed' in sheet.lower() and 'income' in sheet.lower():
                sheets_dict['Fixed Income'] = sheet
            elif 'alt' in sheet.lower():
                sheets_dict['Alternatives'] = sheet
                
        logger.info(f"Identified - Equity: {sheets_dict.get('Equity', 'None')}, "
                  f"Fixed Income: {sheets_dict.get('Fixed Income', 'None')}, "
                  f"Alternatives: {sheets_dict.get('Alternatives', 'None')}")
        
        # Process each sheet separately
        equity_records = []
        fixed_income_records = []
        alternatives_records = []
        
        # Process Equity
        if 'Equity' in sheets_dict:
            try:
                logger.info(f"Processing Equity sheet: {sheets_dict['Equity']}")
                equity_df = pd.read_excel(file_path, sheet_name=sheets_dict['Equity'])
                
                # Clean column names
                equity_df.columns = [str(col).strip().lower() for col in equity_df.columns]
                
                # Log column mapping for debugging
                logger.info(f"Column mapping for Equity: {list(equity_df.columns)}")
                
                # Identify critical columns with flexible matching
                position_col = next((col for col in equity_df.columns if 'position' in col.lower()), None)
                ticker_col = next((col for col in equity_df.columns if 'ticker' in col.lower()), None)
                cusip_col = next((col for col in equity_df.columns if any(x in col.lower() for x in ['cusip', 'amended id', 'amended_id'])), None)
                vol_col = next((col for col in equity_df.columns if any(x in col.lower() for x in ['vol', 'volatility', 'std dev'])), None)
                beta_col = next((col for col in equity_df.columns if 'beta' in col.lower()), None)
                
                # Process each row
                for _, row in equity_df.iterrows():
                    try:
                        # Skip rows without position
                        if position_col and pd.isna(row[position_col]):
                            continue
                            
                        # Clean and format values
                        position = str(row[position_col]).strip() if position_col and not pd.isna(row[position_col]) else None
                        ticker = str(row[ticker_col]).strip() if ticker_col and not pd.isna(row[ticker_col]) else None
                        cusip = str(row[cusip_col]).strip() if cusip_col and not pd.isna(row[cusip_col]) else None
                        
                        # Clean numeric values
                        vol = 0.0
                        beta = 0.0
                        
                        # Process volatility
                        if vol_col and not pd.isna(row[vol_col]):
                            vol_value = row[vol_col]
                            try:
                                if isinstance(vol_value, str) and any(err in vol_value.lower() for err in ['#n/a', '#value']):
                                    vol = 0.0
                                else:
                                    vol = float(vol_value)
                            except (ValueError, TypeError):
                                vol = 0.0
                        
                        # Process beta
                        if beta_col and not pd.isna(row[beta_col]):
                            beta_value = row[beta_col]
                            try:
                                if isinstance(beta_value, str) and any(err in beta_value.lower() for err in ['#n/a', '#value']):
                                    beta = 0.0
                                else:
                                    beta = float(beta_value)
                            except (ValueError, TypeError):
                                beta = 0.0
                                
                        # Add to records list
                        if position or ticker or cusip:
                            equity_records.append({
                                'upload_date': import_date,
                                'position': position,
                                'ticker_symbol': ticker,
                                'cusip': cusip,
                                'vol': vol,
                                'beta': beta
                            })
                    except Exception as row_error:
                        logger.error(f"Error processing equity row: {row_error}")
                        
                logger.info(f"Processed {len(equity_records)} equity records")
                results["equity_records"] = len(equity_records)
                
                # Bulk insert in batches
                for i in range(0, len(equity_records), batch_size):
                    batch = equity_records[i:i+batch_size]
                    try:
                        # Create ORM objects
                        orm_objects = [RiskStatisticEquity(**record) for record in batch]
                        db.bulk_save_objects(orm_objects)
                        db.commit()
                        logger.info(f"Inserted equity batch {i//batch_size + 1}/{(len(equity_records)-1)//batch_size + 1}")
                    except Exception as batch_error:
                        logger.error(f"Error inserting equity batch {i//batch_size + 1}: {batch_error}")
                        logger.error(f"Error details: {traceback.format_exc()}")
                        db.rollback()
                        
            except Exception as equity_error:
                logger.error(f"Error processing Equity sheet: {equity_error}")
                logger.error(f"Error details: {traceback.format_exc()}")
        
        # Process Fixed Income
        if 'Fixed Income' in sheets_dict:
            try:
                logger.info(f"Processing Fixed Income sheet: {sheets_dict['Fixed Income']}")
                fi_df = pd.read_excel(file_path, sheet_name=sheets_dict['Fixed Income'])
                
                # Clean column names
                fi_df.columns = [str(col).strip().lower() for col in fi_df.columns]
                
                # Log column mapping for debugging
                logger.info(f"Column mapping for Fixed Income: {list(fi_df.columns)}")
                
                # Identify critical columns with flexible matching
                position_col = next((col for col in fi_df.columns if 'position' in col.lower()), None)
                ticker_col = next((col for col in fi_df.columns if 'ticker' in col.lower()), None)
                cusip_col = next((col for col in fi_df.columns if any(x in col.lower() for x in ['cusip', 'amended id', 'amended_id'])), None)
                duration_col = next((col for col in fi_df.columns if any(x in col.lower() for x in ['duration', 'vol or duration'])), None)
                
                # Process each row
                for _, row in fi_df.iterrows():
                    try:
                        # Skip rows without position
                        if position_col and pd.isna(row[position_col]):
                            continue
                            
                        # Clean and format values
                        position = str(row[position_col]).strip() if position_col and not pd.isna(row[position_col]) else None
                        ticker = str(row[ticker_col]).strip() if ticker_col and not pd.isna(row[ticker_col]) else None
                        cusip = str(row[cusip_col]).strip() if cusip_col and not pd.isna(row[cusip_col]) else None
                        
                        # Clean numeric values
                        duration = 0.0
                        
                        # Process duration
                        if duration_col and not pd.isna(row[duration_col]):
                            duration_value = row[duration_col]
                            try:
                                if isinstance(duration_value, str) and any(err in duration_value.lower() for err in ['#n/a', '#value']):
                                    duration = 0.0
                                else:
                                    duration = float(duration_value)
                            except (ValueError, TypeError):
                                duration = 0.0
                                
                        # Add to records list
                        if position or ticker or cusip:
                            fixed_income_records.append({
                                'upload_date': import_date,
                                'position': position,
                                'ticker_symbol': ticker,
                                'cusip': cusip,
                                'duration': duration
                            })
                    except Exception as row_error:
                        logger.error(f"Error processing fixed income row: {row_error}")
                        
                logger.info(f"Processed {len(fixed_income_records)} fixed income records")
                results["fixed_income_records"] = len(fixed_income_records)
                
                # Bulk insert in batches
                for i in range(0, len(fixed_income_records), batch_size):
                    batch = fixed_income_records[i:i+batch_size]
                    try:
                        # Create ORM objects - only include non-None fields for cleaner inserts
                        fixed_income_objects = []
                        for record in batch:
                            # Create object with required fields
                            obj = RiskStatisticFixedIncome(
                                upload_date=record['upload_date'],
                                position=record['position'],
                                ticker_symbol=record['ticker_symbol'],
                                cusip=record['cusip'],
                                duration=record['duration']
                            )
                            fixed_income_objects.append(obj)
                            
                        db.bulk_save_objects(fixed_income_objects)
                        db.commit()
                        logger.info(f"Inserted fixed income batch {i//batch_size + 1}/{(len(fixed_income_records)-1)//batch_size + 1}")
                    except Exception as batch_error:
                        logger.error(f"Error inserting fixed income batch {i//batch_size + 1}: {batch_error}")
                        logger.error(f"Error details: {traceback.format_exc()}")
                        db.rollback()
                        
            except Exception as fi_error:
                logger.error(f"Error processing Fixed Income sheet: {fi_error}")
                logger.error(f"Error details: {traceback.format_exc()}")
                
        # Process Alternatives
        if 'Alternatives' in sheets_dict:
            try:
                logger.info(f"Processing Alternatives sheet: {sheets_dict['Alternatives']}")
                alt_df = pd.read_excel(file_path, sheet_name=sheets_dict['Alternatives'])
                
                # Clean column names
                alt_df.columns = [str(col).strip().lower() for col in alt_df.columns]
                
                # Log column mapping for debugging
                logger.info(f"Column mapping for Alternatives: {list(alt_df.columns)}")
                
                # Identify critical columns with flexible matching
                position_col = next((col for col in alt_df.columns if 'position' in col.lower()), None)
                ticker_col = next((col for col in alt_df.columns if 'ticker' in col.lower()), None)
                cusip_col = next((col for col in alt_df.columns if any(x in col.lower() for x in ['cusip', 'amended id', 'amended_id'])), None)
                beta_col = next((col for col in alt_df.columns if 'beta' in col.lower()), None)
                
                # Process each row
                for _, row in alt_df.iterrows():
                    try:
                        # Skip rows without position
                        if position_col and pd.isna(row[position_col]):
                            continue
                            
                        # Clean and format values
                        position = str(row[position_col]).strip() if position_col and not pd.isna(row[position_col]) else None
                        ticker = str(row[ticker_col]).strip() if ticker_col and not pd.isna(row[ticker_col]) else None
                        cusip = str(row[cusip_col]).strip() if cusip_col and not pd.isna(row[cusip_col]) else None
                        
                        # Clean numeric values
                        beta = 0.0
                        
                        # Process beta
                        if beta_col and not pd.isna(row[beta_col]):
                            beta_value = row[beta_col]
                            try:
                                if isinstance(beta_value, str) and any(err in beta_value.lower() for err in ['#n/a', '#value']):
                                    beta = 0.0
                                else:
                                    beta = float(beta_value)
                            except (ValueError, TypeError):
                                beta = 0.0
                                
                        # Add to records list
                        if position or ticker or cusip:
                            alternatives_records.append({
                                'upload_date': import_date,
                                'position': position,
                                'ticker_symbol': ticker,
                                'cusip': cusip,
                                'beta': beta
                            })
                    except Exception as row_error:
                        logger.error(f"Error processing alternatives row: {row_error}")
                        
                logger.info(f"Processed {len(alternatives_records)} alternatives records")
                results["alternatives_records"] = len(alternatives_records)
                
                # Bulk insert in batches
                for i in range(0, len(alternatives_records), batch_size):
                    batch = alternatives_records[i:i+batch_size]
                    try:
                        # Create ORM objects - only include non-None fields for cleaner inserts
                        alternatives_objects = []
                        for record in batch:
                            # Create object with required fields
                            obj = RiskStatisticAlternatives(
                                upload_date=record['upload_date'],
                                position=record['position'],
                                ticker_symbol=record['ticker_symbol'],
                                cusip=record['cusip'],
                                beta=record['beta']
                            )
                            alternatives_objects.append(obj)
                            
                        db.bulk_save_objects(alternatives_objects)
                        db.commit()
                        logger.info(f"Inserted alternatives batch {i//batch_size + 1}/{(len(alternatives_records)-1)//batch_size + 1}")
                    except Exception as batch_error:
                        logger.error(f"Error inserting alternatives batch {i//batch_size + 1}: {batch_error}")
                        logger.error(f"Error details: {traceback.format_exc()}")
                        db.rollback()
                        
            except Exception as alt_error:
                logger.error(f"Error processing Alternatives sheet: {alt_error}")
                logger.error(f"Error details: {traceback.format_exc()}")
        
        # Update total records
        total_records = len(equity_records) + len(fixed_income_records) + len(alternatives_records)
        results["total_records"] = total_records
        logger.info(f"Total records processed: {total_records}")
        
    except Exception as e:
        logger.error(f"Error processing risk statistics: {e}")
        logger.error(f"Error details: {traceback.format_exc()}")
        results["success"] = False
        results["error"] = str(e)
        return results
    finally:
        # Delete temporary file if it's not a test file
        if file_path and os.path.exists(file_path) and not use_test_file:
            try:
                os.unlink(file_path)
                logger.info(f"Deleted temporary file: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting temporary file: {e}")
    
    # Record processing time
    end_time = time.time()
    processing_time = end_time - start_time
    results["processing_time_seconds"] = processing_time
    results["end_time"] = str(datetime.now())
    
    logger.info(f"Risk statistics processing completed in {processing_time:.2f} seconds")
    logger.info(f"Total: {results['total_records']} records - "
              f"Equity: {results['equity_records']}, "
              f"Fixed Income: {results['fixed_income_records']}, "
              f"Alternatives: {results['alternatives_records']}")
    
    return results