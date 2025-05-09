"""
Service for managing risk statistics data.
This uses an optimized approach for handling large Excel files and efficient database operations.
"""

import logging
import os
import time
from datetime import date
import pandas as pd
from sqlalchemy.orm import Session
from src.models.models import EgnyteRiskStat
from src.services.egnyte_service import download_risk_stats_file
from src.services.upsert_helper import batch_upsert_risk_stats, clean_risk_stats_date

logger = logging.getLogger(__name__)

def process_risk_stats(db: Session, use_test_file=False, batch_size=50, max_retries=3):
    """
    Optimized function to download and process risk statistics from Egnyte.
    Uses chunked processing and optimized database operations for large files.
    
    Args:
        db (Session): Database session
        use_test_file (bool): Whether to use a test file instead of downloading from Egnyte
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts for database operations
        
    Returns:
        dict: Summary of the processing results
    """
    start_time = time.time()
    logger.info(f"Starting optimized risk stats processing (batch_size={batch_size}, max_retries={max_retries})")
    
    try:
        # Initialize counters and state
        equity_count = 0
        fi_count = 0
        alt_count = 0
        error_count = 0
        import_date = date.today()
        file_path = None
        
        # 1. Download the risk stats file
        try:
            logger.info(f"Downloading risk stats file (use_test_file={use_test_file})")
            file_path = download_risk_stats_file(use_test_file=use_test_file)
            
            if not file_path or not os.path.exists(file_path):
                return {
                    "success": False,
                    "error": "Failed to download risk statistics file"
                }
                
            file_size = os.path.getsize(file_path)
            logger.info(f"Successfully downloaded file to {file_path} ({file_size/1024/1024:.2f} MB)")
        except Exception as download_error:
            error_msg = str(download_error)
            logger.error(f"Failed to download risk stats file: {error_msg}")
            
            # Provide more specific error messages based on the exception type
            if "EGNYTE_ACCESS_TOKEN" in error_msg:
                return {
                    "success": False,
                    "error": "Egnyte API token is missing or invalid. Please set the EGNYTE_ACCESS_TOKEN environment variable.",
                    "detail": error_msg
                }
            else:
                return {
                    "success": False,
                    "error": f"Download error: {error_msg}",
                    "detail": error_msg
                }
        
        # 2. Clean up any existing records for today
        logger.info(f"Cleaning up existing records for {import_date}")
        if not clean_risk_stats_date(db, import_date):
            logger.error("Failed to clean up existing records - cannot proceed with import")
            return {
                "success": False,
                "error": "Database cleanup failed - cannot proceed with import"
            }
        
        # 3. Process the Excel file with optimized chunked reading
        try:
            logger.info("Analyzing Excel file structure")
            
            # Read the file to identify sheets
            with pd.ExcelFile(file_path) as xls:
                sheet_names = xls.sheet_names
                logger.info(f"Found sheets: {sheet_names}")
                
                # Process each sheet
                equity_sheet = None
                fi_sheet = None
                alt_sheet = None
                fi_durations_sheet = None
                
                # Try to identify sheets by name
                logger.info("Identifying sheet types based on sheet names")
                for sheet in sheet_names:
                    sheet_lower = sheet.lower()
                    logger.info(f"Examining sheet: '{sheet}' (lowercase: '{sheet_lower}')")
                    
                    if 'equity' in sheet_lower:
                        logger.info(f"  - Identified as EQUITY sheet: '{sheet}'")
                        equity_sheet = sheet
                    elif 'fixed income' in sheet_lower and 'duration' not in sheet_lower:
                        logger.info(f"  - Identified as FIXED INCOME sheet: '{sheet}'")
                        fi_sheet = sheet
                    elif 'duration' in sheet_lower:
                        logger.info(f"  - Identified as FIXED INCOME DURATIONS sheet: '{sheet}'")
                        fi_durations_sheet = sheet
                    elif any(alt in sheet_lower for alt in ['alt', 'alternatives']):
                        logger.info(f"  - Identified as ALTERNATIVES sheet: '{sheet}'")
                        alt_sheet = sheet
                    else:
                        logger.info(f"  - Unrecognized sheet type: '{sheet}'")
                
                # Log detailed Excel file structure for debugging
                logger.info("DEBUG: Excel File Structure Details:")
                logger.info(f"  - File path: {file_path}")
                logger.info(f"  - File size: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
                logger.info(f"  - Sheet count: {len(sheet_names)}")
                logger.info(f"  - Excel library: pandas {pd.__version__}")
                
                logger.info(f"Identified sheets - Equity: {equity_sheet}, Fixed Income: {fi_sheet}, "
                           f"Fixed Income Durations: {fi_durations_sheet}, Alternatives: {alt_sheet}")
                
                # Process Equity sheet
                if equity_sheet:
                    logger.info(f"Processing Equity sheet: {equity_sheet}")
                    equity_records = []
                    
                    # Read sheet in chunks to avoid memory issues
                    chunk_size = 1000
                    
                    # Use pandas to get sheet info instead of xlrd methods
                    # Get total rows by reading the sheet
                    logger.info(f"Reading sheet for row count: {equity_sheet}")
                    df_preview = pd.read_excel(xls, sheet_name=equity_sheet)
                    total_rows = len(df_preview) - 1  # Subtract header row
                    
                    # Get column headers
                    columns = df_preview.columns.tolist()
                    logger.info(f"Equity sheet has {total_rows} rows and columns: {columns}")
                    
                    # Define column mappings based on what we find
                    position_col = 'Position' if 'Position' in columns else None
                    ticker_col = 'Ticker Symbol' if 'Ticker Symbol' in columns else None
                    cusip_col = 'CUSIP' if 'CUSIP' in columns else None
                    vol_col = 'Vol' if 'Vol' in columns else None
                    beta_col = 'BETA' if 'BETA' in columns else None
                    second_level_col = 'Second Level' if 'Second Level' in columns else None
                    amended_id_col = 'Amended ID' if 'Amended ID' in columns else None
                    notes_col = 'Notes' if 'Notes' in columns else None
                    
                    if not position_col:
                        logger.error("Position column not found in Equity sheet - cannot proceed")
                        # Skip processing this sheet
                        equity_records = []
                    else:
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
                            
                            # Check for duplicate positions in this chunk
                            if position_col and len(chunk) > 0:
                                duplicates = chunk[chunk.duplicated(position_col, keep='first')][position_col]
                                if len(duplicates) > 0:
                                    logger.warning(f"Found {len(duplicates)} duplicate positions in Equity chunk {chunk_start}-{chunk_end}")
                            
                            for idx, row in chunk.iterrows():
                                try:
                                    if pd.isna(row.get(position_col)):
                                        continue
                                    
                                    position = str(row.get(position_col)).strip()
                                    ticker = str(row.get(ticker_col)).strip() if ticker_col and not pd.isna(row.get(ticker_col)) else None
                                    cusip = str(row.get(cusip_col)).strip() if cusip_col and not pd.isna(row.get(cusip_col)) else None
                                    
                                    # Create record
                                    record = EgnyteRiskStat(
                                        import_date=import_date,
                                        position=position,
                                        ticker_symbol=ticker,
                                        cusip=cusip,
                                        asset_class='Equity',
                                        second_level=str(row.get(second_level_col)).strip() if second_level_col and not pd.isna(row.get(second_level_col)) else None,
                                        volatility=float(row.get(vol_col)) if vol_col and not pd.isna(row.get(vol_col)) else None,
                                        beta=float(row.get(beta_col)) if beta_col and not pd.isna(row.get(beta_col)) else None,
                                        notes=str(row.get(notes_col)).strip() if notes_col and not pd.isna(row.get(notes_col)) else None,
                                        amended_id=str(row.get(amended_id_col)).strip() if amended_id_col and not pd.isna(row.get(amended_id_col)) else None,
                                        source_file=os.path.basename(file_path),
                                        source_tab=equity_sheet,
                                        source_row=idx + chunk_start
                                    )
                                    equity_records.append(record)
                                except Exception as row_error:
                                    logger.error(f"Error processing equity row {idx + chunk_start}: {row_error}")
                                    error_count += 1
                        
                        # Process equity records with batch upsert
                        if equity_records:
                            logger.info(f"Upserting {len(equity_records)} equity records")
                            success, errors = batch_upsert_risk_stats(db, equity_records, batch_size, max_retries)
                            equity_count = success
                            error_count += errors
                            logger.info(f"Equity processing complete: {success} successful, {errors} errors")
                
                # Process Fixed Income sheet
                if fi_sheet:
                    logger.info(f"Processing Fixed Income sheet: {fi_sheet}")
                    fi_records = []
                    
                    # Read durations data from the fixed income durations sheet
                    durations_data = {}
                    if fi_durations_sheet:
                        logger.info(f"Reading durations from sheet: {fi_durations_sheet}")
                        try:
                            # Read the durations sheet into a lookup dictionary
                            durations_df = pd.read_excel(xls, sheet_name=fi_durations_sheet)
                            if 'TICKER' in durations_df.columns and 'LEVERAGE ADJ DURATION' in durations_df.columns:
                                for _, row in durations_df.iterrows():
                                    if not pd.isna(row['TICKER']) and not pd.isna(row['LEVERAGE ADJ DURATION']):
                                        durations_data[row['TICKER']] = row['LEVERAGE ADJ DURATION']
                                logger.info(f"Loaded {len(durations_data)} durations from durations sheet")
                        except Exception as dur_error:
                            logger.error(f"Error loading durations sheet: {dur_error}")
                    
                    # Process the Fixed Income sheet
                    # Read sheet in chunks to avoid memory issues
                    chunk_size = 1000
                    
                    # Use pandas to get sheet info instead of xlrd methods
                    # Get total rows by reading the sheet
                    logger.info(f"Reading sheet for row count: {fi_sheet}")
                    df_preview = pd.read_excel(xls, sheet_name=fi_sheet)
                    total_rows = len(df_preview) - 1  # Subtract header row
                    
                    # Get column headers
                    columns = df_preview.columns.tolist()
                    logger.info(f"Fixed Income sheet has {total_rows} rows and columns: {columns}")
                    
                    # Define column mappings based on what we find
                    position_col = 'Position' if 'Position' in columns else None
                    ticker_col = 'Ticker Symbol' if 'Ticker Symbol' in columns else None
                    cusip_col = 'CUSIP' if 'CUSIP' in columns else None
                    duration_col = 'Duration' if 'Duration' in columns else None
                    second_level_col = 'Second Level' if 'Second Level' in columns else None
                    amended_id_col = 'Amended ID' if 'Amended ID' in columns else None
                    notes_col = 'Notes' if 'Notes' in columns else None
                    
                    if not position_col:
                        logger.error("Position column not found in Fixed Income sheet - cannot proceed")
                        # Skip processing this sheet
                        fi_records = []
                    else:
                        # Process in chunks
                        for chunk_start in range(1, total_rows + 1, chunk_size):
                            chunk_end = min(chunk_start + chunk_size - 1, total_rows + 1)
                            logger.info(f"Reading fixed income rows {chunk_start}-{chunk_end}")
                            
                            chunk = pd.read_excel(
                                xls, 
                                sheet_name=fi_sheet,
                                skiprows=range(1, chunk_start),
                                nrows=(chunk_end - chunk_start + 1)
                            )
                            
                            # Check for duplicate positions in this chunk
                            if position_col and len(chunk) > 0:
                                duplicates = chunk[chunk.duplicated(position_col, keep='first')][position_col]
                                if len(duplicates) > 0:
                                    logger.warning(f"Found {len(duplicates)} duplicate positions in Fixed Income chunk {chunk_start}-{chunk_end}")
                            
                            for idx, row in chunk.iterrows():
                                try:
                                    if pd.isna(row.get(position_col)):
                                        continue
                                    
                                    position = str(row.get(position_col)).strip()
                                    ticker = str(row.get(ticker_col)).strip() if ticker_col and not pd.isna(row.get(ticker_col)) else None
                                    cusip = str(row.get(cusip_col)).strip() if cusip_col and not pd.isna(row.get(cusip_col)) else None
                                    
                                    # Get duration from either the row or the durations lookup
                                    duration = None
                                    if duration_col and not pd.isna(row.get(duration_col)):
                                        duration = float(row.get(duration_col))
                                    elif ticker and ticker in durations_data:
                                        duration = float(durations_data[ticker])
                                    
                                    # Create record
                                    record = EgnyteRiskStat(
                                        import_date=import_date,
                                        position=position,
                                        ticker_symbol=ticker,
                                        cusip=cusip,
                                        asset_class='Fixed Income',
                                        second_level=str(row.get(second_level_col)).strip() if second_level_col and not pd.isna(row.get(second_level_col)) else None,
                                        duration=duration,
                                        notes=str(row.get(notes_col)).strip() if notes_col and not pd.isna(row.get(notes_col)) else None,
                                        amended_id=str(row.get(amended_id_col)).strip() if amended_id_col and not pd.isna(row.get(amended_id_col)) else None,
                                        source_file=os.path.basename(file_path),
                                        source_tab=fi_sheet,
                                        source_row=idx + chunk_start
                                    )
                                    fi_records.append(record)
                                except Exception as row_error:
                                    logger.error(f"Error processing fixed income row {idx + chunk_start}: {row_error}")
                                    error_count += 1
                        
                        # Process fixed income records with batch upsert
                        if fi_records:
                            logger.info(f"Upserting {len(fi_records)} fixed income records")
                            success, errors = batch_upsert_risk_stats(db, fi_records, batch_size, max_retries)
                            fi_count = success
                            error_count += errors
                            logger.info(f"Fixed Income processing complete: {success} successful, {errors} errors")
                
                # Process Alternatives sheet
                if alt_sheet:
                    logger.info(f"Processing Alternatives sheet: {alt_sheet}")
                    alt_records = []
                    
                    # Read sheet in chunks to avoid memory issues
                    chunk_size = 1000

                    # Use pandas to get sheet info instead of xlrd methods
                    # Get total rows by reading the sheet
                    logger.info(f"Reading sheet for row count: {alt_sheet}")
                    df_preview = pd.read_excel(xls, sheet_name=alt_sheet)
                    total_rows = len(df_preview) - 1  # Subtract header row
                    
                    # Get column headers
                    columns = df_preview.columns.tolist()
                    logger.info(f"Alternatives sheet has {total_rows} rows and columns: {columns}")
                    
                    # Define column mappings based on what we find
                    position_col = 'Position' if 'Position' in columns else None
                    ticker_col = 'Ticker Symbol' if 'Ticker Symbol' in columns else None
                    cusip_col = 'CUSIP' if 'CUSIP' in columns else None
                    vol_col = 'Vol' if 'Vol' in columns else None
                    second_level_col = 'Second Level' if 'Second Level' in columns else None
                    amended_id_col = 'Amended ID' if 'Amended ID' in columns else None
                    notes_col = 'Notes' if 'Notes' in columns else None
                    
                    if not position_col:
                        logger.error("Position column not found in Alternatives sheet - cannot proceed")
                        # Skip processing this sheet
                        alt_records = []
                    else:
                        # Process in chunks
                        for chunk_start in range(1, total_rows + 1, chunk_size):
                            chunk_end = min(chunk_start + chunk_size - 1, total_rows + 1)
                            logger.info(f"Reading alternatives rows {chunk_start}-{chunk_end}")
                            
                            chunk = pd.read_excel(
                                xls, 
                                sheet_name=alt_sheet,
                                skiprows=range(1, chunk_start),
                                nrows=(chunk_end - chunk_start + 1)
                            )
                            
                            # Check for duplicate positions in this chunk
                            if position_col and len(chunk) > 0:
                                duplicates = chunk[chunk.duplicated(position_col, keep='first')][position_col]
                                if len(duplicates) > 0:
                                    logger.warning(f"Found {len(duplicates)} duplicate positions in Alternatives chunk {chunk_start}-{chunk_end}")
                            
                            for idx, row in chunk.iterrows():
                                try:
                                    if pd.isna(row.get(position_col)):
                                        continue
                                    
                                    position = str(row.get(position_col)).strip()
                                    ticker = str(row.get(ticker_col)).strip() if ticker_col and not pd.isna(row.get(ticker_col)) else None
                                    cusip = str(row.get(cusip_col)).strip() if cusip_col and not pd.isna(row.get(cusip_col)) else None
                                    
                                    # Create record
                                    record = EgnyteRiskStat(
                                        import_date=import_date,
                                        position=position,
                                        ticker_symbol=ticker,
                                        cusip=cusip,
                                        asset_class='Alternatives',
                                        second_level=str(row.get(second_level_col)).strip() if second_level_col and not pd.isna(row.get(second_level_col)) else None,
                                        volatility=float(row.get(vol_col)) if vol_col and not pd.isna(row.get(vol_col)) else None,
                                        notes=str(row.get(notes_col)).strip() if notes_col and not pd.isna(row.get(notes_col)) else None,
                                        amended_id=str(row.get(amended_id_col)).strip() if amended_id_col and not pd.isna(row.get(amended_id_col)) else None,
                                        source_file=os.path.basename(file_path),
                                        source_tab=alt_sheet,
                                        source_row=idx + chunk_start
                                    )
                                    alt_records.append(record)
                                except Exception as row_error:
                                    logger.error(f"Error processing alternatives row {idx + chunk_start}: {row_error}")
                                    error_count += 1
                        
                        # Process alternatives records with batch upsert
                        if alt_records:
                            logger.info(f"Upserting {len(alt_records)} alternatives records")
                            success, errors = batch_upsert_risk_stats(db, alt_records, batch_size, max_retries)
                            alt_count = success
                            error_count += errors
                            logger.info(f"Alternatives processing complete: {success} successful, {errors} errors")
                
            # Calculate overall statistics and timing
            end_time = time.time()
            duration = end_time - start_time
            total_records = equity_count + fi_count + alt_count
            
            logger.info(f"Processing complete - Total: {total_records} records in {duration:.2f} seconds "
                       f"({total_records/duration:.2f} records/sec)")
            logger.info(f"Breakdown - Equity: {equity_count}, Fixed Income: {fi_count}, "
                       f"Alternatives: {alt_count}, Errors: {error_count}")
            
            # Clean up the file
            if os.path.exists(file_path):
                os.unlink(file_path)
                logger.info(f"Removed temporary file {file_path}")
            
            return {
                "success": True,
                "stats": {
                    "equity": equity_count,
                    "fixed_income": fi_count,
                    "alternatives": alt_count,
                    "errors": error_count,
                    "total": total_records,
                    "duration_seconds": duration,
                    "records_per_second": total_records / duration if duration > 0 else 0
                },
                "message": "Risk statistics update completed successfully"
            }
            
        except Exception as excel_error:
            logger.exception(f"Error processing Excel file: {excel_error}")
            
            # Clean up the file on error
            if file_path and os.path.exists(file_path):
                try:
                    os.unlink(file_path)
                    logger.info(f"Removed temporary file {file_path}")
                except:
                    pass
                
            return {
                "success": False,
                "error": f"Excel processing error: {str(excel_error)}"
            }
        
    except Exception as e:
        logger.exception(f"Unexpected error during risk stats processing: {e}")
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}"
        }

def get_risk_stats_summary(db: Session):
    """
    Get a summary of risk statistics data in the database.
    
    Args:
        db (Session): Database session
        
    Returns:
        dict: Summary of risk statistics data
    """
    try:
        from sqlalchemy import func
        
        # Get total count
        total_count = db.query(EgnyteRiskStat).count()
        
        # Count by asset class
        asset_counts = db.query(
            EgnyteRiskStat.asset_class,
            func.count(EgnyteRiskStat.id).label('count')
        ).group_by(EgnyteRiskStat.asset_class).all()
        
        asset_count_dict = {item[0]: item[1] for item in asset_counts}
        
        # Get latest import date
        latest_date = db.query(func.max(EgnyteRiskStat.import_date)).scalar()
        
        # Get oldest import date
        oldest_date = db.query(func.min(EgnyteRiskStat.import_date)).scalar()
        
        # Get count of distinct positions
        distinct_positions = db.query(func.count(func.distinct(EgnyteRiskStat.position))).scalar()
        
        return {
            "success": True,
            "summary": {
                "total_records": total_count,
                "equity_records": asset_count_dict.get('Equity', 0),
                "fixed_income_records": asset_count_dict.get('Fixed Income', 0),
                "alternatives_records": asset_count_dict.get('Alternatives', 0),
                "latest_date": latest_date.isoformat() if latest_date else None,
                "oldest_date": oldest_date.isoformat() if oldest_date else None,
                "distinct_positions": distinct_positions
            }
        }
    except Exception as e:
        logger.error(f"Error getting risk stats summary: {e}")
        return {
            "success": False,
            "error": f"Error getting summary: {str(e)}"
        }