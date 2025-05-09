"""
Service for interacting with the Egnyte API to fetch risk statistics.
"""
import os
import re
import logging
import tempfile
from datetime import datetime, date
import pandas as pd
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.models.models import EgnyteRiskStat


logger = logging.getLogger(__name__)


def get_egnyte_token():
    """Get the Egnyte API token from environment variables."""
    token = os.environ.get('EGNYTE_ACCESS_TOKEN')
    if not token:
        logger.error("EGNYTE_ACCESS_TOKEN not found in environment variables.")
        raise ValueError("EGNYTE_ACCESS_TOKEN not found in environment variables.")
    return token


def download_risk_stats_file(token=None, domain=None, file_path=None, use_test_file=False):
    """
    Download the Excel file containing risk statistics from Egnyte.
    
    Args:
        token (str, optional): Egnyte API token. If not provided, will be retrieved from env var.
        domain (str, optional): Egnyte domain. Defaults to "procapitalpartners.egnyte.com".
        file_path (str, optional): Path to the file in Egnyte. Defaults to shared risk stats file.
        use_test_file (bool, optional): If True, use a local test file when available.
        
    Returns:
        str: Path to the downloaded temporary file
    """
    # Check for any existing downloaded files first to help with debuging  
    local_test_file = os.environ.get('LOCAL_RISK_STATS_FILE')
    if use_test_file and local_test_file and os.path.exists(local_test_file):
        logger.info(f"Using local test file: {local_test_file}")
        # Create a copy to avoid modifying the original
        temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        with open(local_test_file, 'rb') as f:
            temp_file.write(f.read())
        temp_file.close()
        return temp_file.name
    
    # Use provided parameters or defaults
    token = token or get_egnyte_token()
    domain = domain or os.environ.get('EGNYTE_DOMAIN', "procapitalpartners.egnyte.com")
    egnyte_path = os.environ.get('EGNYTE_RISK_STATS_PATH')
    file_path = file_path or egnyte_path or "/Shared/Internal Documents/Proficio Capital Partners/Asset Allocation/Portfolio Management/New Portfolio Sheets/Security Risk Stats.xlsx"
    
    logger.info(f"Downloading risk statistics file from Egnyte: {file_path}")
    logger.info(f"Using domain: {domain}")
    
    url = f"https://{domain}/pubapi/v1/fs-content{file_path}"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        logger.info(f"Sending request to: {url}")
        response = requests.get(url, headers=headers, timeout=30)  # Add timeout
        
        if response.status_code != 200:
            logger.error(f"Failed to download file from Egnyte: HTTP {response.status_code}")
            logger.error(f"Response: {response.text}")
            
            # Try fallback to sample data if available and we're in test/debug mode
            if use_test_file and os.path.exists('data/sample_risk_stats.xlsx'):
                logger.warning("Using sample risk stats file for development/testing")
                return 'data/sample_risk_stats.xlsx'
                
            raise Exception(f"Failed to download file from Egnyte: HTTP {response.status_code}. Response: {response.text}")
        
        # Check if we actually received an Excel file
        content_type = response.headers.get('Content-Type', '')
        if 'excel' not in content_type.lower() and 'spreadsheet' not in content_type.lower() and '.xlsx' not in content_type.lower():
            logger.warning(f"Response may not be an Excel file. Content-Type: {content_type}")
            # Still try to process it, but log a warning
        
        # Create a temporary file to store the downloaded file
        temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        temp_file.write(response.content)
        temp_file.close()
        
        # Verify the file exists and has content
        if os.path.getsize(temp_file.name) == 0:
            logger.error("Downloaded file is empty (0 bytes)")
            raise Exception("Downloaded file is empty. Check Egnyte permissions and file existence.")
            
        logger.info(f"Risk statistics file downloaded to {temp_file.name} ({os.path.getsize(temp_file.name)} bytes)")
        return temp_file.name
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error when downloading from Egnyte: {str(e)}")
        
        # For development/testing - use a sample file if available
        if use_test_file and os.path.exists('data/sample_risk_stats.xlsx'):
            logger.warning("Using sample risk stats file for development/testing due to connection error")
            return 'data/sample_risk_stats.xlsx'
        
        raise Exception(f"Connection error when downloading from Egnyte: {str(e)}")
    except Exception as e:
        logger.error(f"Error downloading risk stats file: {str(e)}")
        raise


def process_excel_file(file_path, db, batch_size=50, max_retries=3):
    """
    Process the downloaded Excel file and insert the data into the database.
    
    Args:
        file_path (str): Path to the Excel file
        db (Session): Database session
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts for database operations
        
    Returns:
        dict: Summary statistics of the import
    """
    logger.info(f"Processing risk statistics file: {file_path}")
    
    # Read the Excel file with optimized settings for large files
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    logger.info(f"Excel file size: {file_size_mb:.2f} MB")
    
    try:
        # For large files, use optimized reading options
        if file_size_mb > 50:  # If over 50 MB, use optimized settings
            logger.info("Large file detected: Using optimized Excel reader settings")
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
        else:
            excel_file = pd.ExcelFile(file_path)
        
        sheet_names = excel_file.sheet_names
        logger.info(f"Excel file contains sheets: {sheet_names}")
    except Exception as e:
        logger.error(f"Failed to read Excel file with primary engine: {e}")
        try:
            # Try with an alternative engine
            excel_file = pd.ExcelFile(file_path, engine='xlrd')
            sheet_names = excel_file.sheet_names
            logger.info("Successfully read Excel file using alternative engine")
        except Exception as inner_e:
            logger.error(f"Failed to read Excel file with alternative engine: {inner_e}")
            raise ValueError(f"Could not read Excel file: {inner_e}")
        
    # Examine only the first row of each sheet to reduce memory usage
    for sheet in sheet_names:
        try:
            # Read just a single row to analyze structure
            df = pd.read_excel(file_path, sheet_name=sheet, nrows=1)
            logger.info(f"Sheet: {sheet}, Columns: {df.columns.tolist()}")
        except Exception as e:
            logger.warning(f"Could not read sheet {sheet}: {e}")
    
    import_date = date.today()
    stats = {
        "total_records": 0,
        "equity_records": 0,
        "fixed_income_records": 0,
        "alternatives_records": 0
    }
    
    # Find the appropriate sheets using more flexible matching
    equity_sheet = None
    fixed_income_sheet = None
    alternatives_sheet = None
    
    # Find the sheet names based on more flexible matching patterns
    for sheet in sheet_names:
        if not isinstance(sheet, str):
            continue
            
        lower_sheet = sheet.lower()
        if "equity" in lower_sheet:
            equity_sheet = sheet
            logger.info(f"Identified '{sheet}' as the Equity sheet")
        elif any(term in lower_sheet for term in ["fixed", "fixed income", "fi ", "fixed inc", "duration"]):
            fixed_income_sheet = sheet
            logger.info(f"Identified '{sheet}' as the Fixed Income sheet")
        elif any(term in lower_sheet for term in ["alternative", "alt", "alts"]):
            alternatives_sheet = sheet
            logger.info(f"Identified '{sheet}' as the Alternatives sheet")
    
    # Process the Equity sheet if found
    if equity_sheet:
        logger.info(f"Found Equity sheet: {equity_sheet}")
        try:
            stats["equity_records"] = process_equity_sheet(file_path, equity_sheet, import_date, db, 
                                                          batch_size=batch_size, max_retries=max_retries)
            logger.info(f"Successfully processed Equity sheet with {stats['equity_records']} records")
        except Exception as e:
            logger.error(f"Error processing Equity sheet: {e}")
            # Don't fail the entire process - continue to other sheets
            stats["equity_records"] = 0
    else:
        logger.warning("No Equity sheet found in the Excel file")
    
    # Process the Fixed Income sheet if found
    if fixed_income_sheet:
        logger.info(f"Found Fixed Income sheet: {fixed_income_sheet}")
        try:
            stats["fixed_income_records"] = process_fixed_income_sheet(file_path, fixed_income_sheet, import_date, db,
                                                                      batch_size=batch_size, max_retries=max_retries)
            logger.info(f"Successfully processed Fixed Income sheet with {stats['fixed_income_records']} records")
        except Exception as e:
            logger.error(f"Error processing Fixed Income sheet: {e}")
            # Don't fail the entire process - continue to other sheets
            stats["fixed_income_records"] = 0
    else:
        logger.warning("No Fixed Income sheet found in the Excel file")
    
    # Process the Alternatives sheet if found
    if alternatives_sheet:
        logger.info(f"Found Alternatives sheet: {alternatives_sheet}")
        try:
            stats["alternatives_records"] = process_alternatives_sheet(file_path, alternatives_sheet, import_date, db,
                                                                      batch_size=batch_size, max_retries=max_retries)
            logger.info(f"Successfully processed Alternatives sheet with {stats['alternatives_records']} records")
        except Exception as e:
            logger.error(f"Error processing Alternatives sheet: {e}")
            # Don't fail the entire process - continue to other sheets
            stats["alternatives_records"] = 0
    else:
        logger.warning("No Alternatives sheet found in the Excel file")
    
    # Calculate total records
    stats["total_records"] = (
        stats["equity_records"] + 
        stats["fixed_income_records"] + 
        stats["alternatives_records"]
    )
    
    # Verify with a count from database for double-checking
    try:
        latest_date = date.today()
        db_count = db.query(EgnyteRiskStat).filter(EgnyteRiskStat.import_date == latest_date).count()
        
        # Record counts by asset class for validation
        equity_db_count = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == latest_date,
            EgnyteRiskStat.asset_class == 'Equity'
        ).count()
        
        fi_db_count = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == latest_date,
            EgnyteRiskStat.asset_class == 'Fixed Income'
        ).count()
        
        alt_db_count = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == latest_date,
            EgnyteRiskStat.asset_class == 'Alternatives'
        ).count()
        
        # Log counts for comparison
        logger.info(f"Database counts - Total: {db_count}, Equity: {equity_db_count}, "
                    f"Fixed Income: {fi_db_count}, Alternatives: {alt_db_count}")
        
        # Add database counts to stats
        stats["db_total"] = db_count
        stats["db_equity"] = equity_db_count
        stats["db_fixed_income"] = fi_db_count
        stats["db_alternatives"] = alt_db_count
        
        # Check for discrepancies
        if stats["total_records"] != db_count:
            logger.warning(f"Discrepancy between calculated total ({stats['total_records']}) "
                           f"and database count ({db_count})")
                           
        if stats["equity_records"] != equity_db_count:
            logger.warning(f"Equity records discrepancy: calculated={stats['equity_records']}, database={equity_db_count}")
            
        if stats["fixed_income_records"] != fi_db_count:
            logger.warning(f"Fixed Income records discrepancy: calculated={stats['fixed_income_records']}, database={fi_db_count}")
            
        if stats["alternatives_records"] != alt_db_count:
            logger.warning(f"Alternatives records discrepancy: calculated={stats['alternatives_records']}, database={alt_db_count}")
            
    except Exception as count_error:
        logger.error(f"Error verifying database record counts: {count_error}")
        stats["db_count_error"] = str(count_error)
    
    logger.info(f"Imported {stats['total_records']} risk statistics records")
    
    # Log some sample records to help with debugging
    try:
        logger.info("Getting sample records from each asset class for verification")
        
        # Get sample equity records
        equity_samples = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == latest_date,
            EgnyteRiskStat.asset_class == 'Equity'
        ).limit(3).all()
        
        if equity_samples:
            for i, sample in enumerate(equity_samples):
                logger.info(f"Equity sample {i+1}: Position={sample.position}, "
                           f"Ticker={sample.ticker_symbol}, Beta={sample.beta}")
        else:
            logger.warning("No equity samples found in database")
            
        # Get sample fixed income records
        fi_samples = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.import_date == latest_date,
            EgnyteRiskStat.asset_class == 'Fixed Income'
        ).limit(3).all()
        
        if fi_samples:
            for i, sample in enumerate(fi_samples):
                logger.info(f"Fixed Income sample {i+1}: Position={sample.position}, "
                           f"Duration={sample.duration}")
        else:
            logger.warning("No fixed income samples found in database")
    except Exception as sample_error:
        logger.error(f"Error retrieving sample records: {sample_error}")
        
    return stats


def process_equity_sheet(file_path, sheet_name, import_date, db, batch_size=50, max_retries=3):
    """
    Process the Equity sheet from the Excel file.
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet to process
        import_date (date): Date of the import
        db (Session): Database session
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts for database operations
        
    Returns:
        int: Number of records successfully processed
    """
    logger.info(f"Processing Equity sheet")
    
    # For large files, we need to handle them efficiently
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    
    # Check if the file is large (over 100 MB)
    if file_size_mb > 100:
        logger.info(f"Large file detected ({file_size_mb:.2f} MB): Using optimized reading for Equity sheet")
        
        # Excel reader object
        xl = pd.ExcelFile(file_path)
        
        # Get information about the sheet
        sheet_data = xl.book.sheet_by_name(sheet_name)
        total_rows = sheet_data.nrows
        
        # Process in manageable chunks (not using chunksize as pandas doesn't support it for Excel)
        chunk_size = 1000
        chunks_processed = 0
        df = pd.DataFrame()
        
        for start_row in range(0, total_rows, chunk_size):
            chunks_processed += 1
            end_row = min(start_row + chunk_size, total_rows)
            
            # Read a specific range of rows
            # First row (usually row 0) contains headers
            if start_row == 0:
                # First chunk includes headers
                temp_df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=end_row)
            else:
                # Subsequent chunks need headers for proper column names and then skip the header row
                temp_df = pd.read_excel(file_path, sheet_name=sheet_name, 
                                       skiprows=start_row-1, nrows=end_row-start_row+1)
                
                # Drop the header row which is now the first row
                if len(temp_df) > 0:
                    temp_df = temp_df.iloc[1:]
            
            # Clean up column names
            temp_df.columns = [col.strip() if isinstance(col, str) else col for col in temp_df.columns]
            
            # Drop rows with no Position
            temp_df = temp_df.dropna(subset=['Position'], how='all')
            
            # Append to our main dataframe
            if not temp_df.empty:
                df = pd.concat([df, temp_df])
            
            logger.info(f"Processed Equity chunk {chunks_processed} ({start_row}-{end_row}), got {len(temp_df)} valid rows, total rows: {len(df)}")
            
            # If we've read enough rows to handle a large dataset, stop
            if len(df) > 30000:  # Limit to 30,000 rows as a safety valve
                logger.warning(f"Reached maximum row limit of 30,000 for Equity sheet, stopping read process")
                break
        
        # Close the Excel file
        xl.close()
    else:
        # For smaller files, read the entire sheet at once
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        # Clean up column names and drop empty rows
        df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
        df = df.dropna(subset=['Position'], how='all')
    
    # Keep track of records processed
    records_processed = 0
    records_succeeded = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    # batch_size parameter is already provided from function call
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Equity sheet columns: {df.columns.tolist()}")
    logger.info(f"Equity sheet has {len(df)} rows")
    
    # Check for duplicate positions in the input file
    try:
        # Use Series.duplicated() to find positions that appear more than once
        duplicate_mask = df['Position'].duplicated(keep='first')
        duplicate_positions = df.loc[duplicate_mask, 'Position'].tolist()
        
        if duplicate_positions:
            # Take up to 5 examples to log
            sample_duplicates = duplicate_positions[:5]
            logger.warning(f"Found {len(duplicate_positions)} duplicate position names in the Equity sheet: {', '.join(str(d) for d in sample_duplicates)}")
    except Exception as e:
        logger.warning(f"Could not check for duplicates in Equity sheet: {e}")
    
    # Map expected columns to our model fields
    for start_idx in range(0, len(df), batch_size):
        batch_count += 1
        end_idx = min(start_idx + batch_size, len(df))
        logger.info(f"Processing equity batch {batch_count} (rows {start_idx}-{end_idx})")
        
        batch_records = []
        
        for index, row in df.iloc[start_idx:end_idx].iterrows():
            try:
                position = str(row.get('Position', '')).strip()
                if not position:
                    continue
                    
                ticker_symbol = str(row.get('Ticker Symbol', '')).strip() if 'Ticker Symbol' in row and not pd.isna(row.get('Ticker Symbol')) else None
                cusip = str(row.get('CUSIP', '')).strip() if 'CUSIP' in row and not pd.isna(row.get('CUSIP')) else None
                bloomberg_id = str(row.get('Bloomberg ID', '')).strip() if 'Bloomberg ID' in row and not pd.isna(row.get('Bloomberg ID')) else None
                second_level = str(row.get('Second Level', '')).strip() if 'Second Level' in row and not pd.isna(row.get('Second Level')) else None
                amended_id = str(row.get('Amended ID', '')).strip() if 'Amended ID' in row and not pd.isna(row.get('Amended ID')) else None
                notes = str(row.get('Notes', '')).strip() if 'Notes' in row and not pd.isna(row.get('Notes')) else None
                
                # Get volatility and beta, handling potential missing or non-numeric values
                volatility = row.get('Vol', None)
                if pd.isna(volatility):
                    volatility = None
                else:
                    try:
                        volatility = float(volatility)
                    except (ValueError, TypeError):
                        volatility = None
                
                beta = row.get('BETA', None)
                if pd.isna(beta):
                    beta = None
                else:
                    try:
                        beta = float(beta)
                    except (ValueError, TypeError):
                        beta = None
                
                # Create a new risk stat record
                risk_stat = EgnyteRiskStat(
                    import_date=import_date,
                    position=position,
                    ticker_symbol=ticker_symbol,
                    cusip=cusip,
                    asset_class='Equity',
                    second_level=second_level,
                    bloomberg_id=bloomberg_id,
                    volatility=volatility,
                    beta=beta,
                    notes=notes,
                    amended_id=amended_id,
                    source_file=os.path.basename(file_path),
                    source_tab=sheet_name,
                    source_row=index + 2  # +2 for header row and 0-indexing
                )
                
                batch_records.append(risk_stat)
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing equity row {index}: {e}")
        
        # Insert the batch of records
        try:
            # Process records individually using merge to handle duplicates gracefully (upsert)
            success_count = 0
            for risk_stat in batch_records:
                try:
                    # Use merge strategy to handle duplicates
                    db.merge(risk_stat)
                    # Commit after each record to ensure partial progress
                    db.commit()
                    success_count += 1
                except Exception as inner_e:
                    db.rollback()
                    inner_msg = str(inner_e)
                    logger.error(f"Error adding individual equity record for position {risk_stat.position}: {inner_e}")
            
            logger.info(f"Successfully processed equity batch {batch_count}: {success_count}/{len(batch_records)} records merged")
            records_succeeded += success_count
        except Exception as e:
            # Roll back on error
            db.rollback()
            error_msg = str(e)
            logger.error(f"Error processing equity batch {batch_count}: {error_msg}")
            
            # Check for specific error conditions
            if "duplicate key value violates unique constraint" in error_msg:
                logger.warning("Duplicate key constraint violation detected in equity batch")
                # Extract the name of the duplicated position if possible
                duplicate_match = re.search(r"Key \(import_date, position, asset_class\)=\([^,]+, ([^,]+), [^)]+\)", error_msg)
                duplicate_position = duplicate_match.group(1) if duplicate_match else "unknown"
                logger.warning(f"Duplicate position: {duplicate_position}")
                
                # Try inserting records one by one but skip the problematic ones
                success_count = 0
                for risk_stat in batch_records:
                    try:
                        # Sanitize strings to prevent encoding issues
                        safe_position = risk_stat.position[:255] if risk_stat.position else None
                        safe_ticker = risk_stat.ticker_symbol[:50] if risk_stat.ticker_symbol else None
                        safe_cusip = risk_stat.cusip[:50] if risk_stat.cusip else None
                        safe_asset_class = risk_stat.asset_class[:50] if risk_stat.asset_class else None
                        safe_second_level = risk_stat.second_level[:100] if risk_stat.second_level else None
                        safe_bloomberg_id = risk_stat.bloomberg_id[:100] if risk_stat.bloomberg_id else None
                        safe_notes = risk_stat.notes[:500] if risk_stat.notes else None
                        safe_amended_id = risk_stat.amended_id[:100] if risk_stat.amended_id else None
                        
                        # Create a new clean instance
                        clean_risk_stat = EgnyteRiskStat(
                            import_date=risk_stat.import_date,
                            position=safe_position,
                            ticker_symbol=safe_ticker,
                            cusip=safe_cusip,
                            asset_class=safe_asset_class,
                            second_level=safe_second_level,
                            bloomberg_id=safe_bloomberg_id,
                            volatility=risk_stat.volatility,
                            beta=risk_stat.beta,
                            notes=safe_notes,
                            amended_id=safe_amended_id,
                            source_file=os.path.basename(file_path),
                            source_tab=sheet_name,
                            source_row=risk_stat.source_row
                        )
                        
                        # Try simple add with merge strategy
                        db.merge(clean_risk_stat)
                        db.commit()
                        success_count += 1
                    except Exception as inner_e:
                        db.rollback()
                        logger.error(f"Error adding individual equity record for position {risk_stat.position}: {inner_e}")
                
                records_succeeded += success_count
                logger.info(f"Successfully added {success_count} out of {len(batch_records)} equity records individually")
            else:
                # Handle other types of errors with individual inserts
                success_count = 0
                for risk_stat in batch_records:
                    try:
                        db.add(risk_stat)
                        db.commit()
                        success_count += 1
                    except Exception as inner_e:
                        db.rollback()
                        logger.error(f"Error adding individual equity record for position {risk_stat.position}: {inner_e}")
                
                records_succeeded += success_count
                logger.info(f"Successfully added {success_count} out of {len(batch_records)} equity records individually")
    
    logger.info(f"Processed {records_processed} equity risk statistics, successfully imported {records_succeeded}")
    return records_succeeded


def process_fixed_income_sheet(file_path, sheet_name, import_date, db, batch_size=50, max_retries=3):
    """
    Process the Fixed Income sheet from the Excel file.
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet to process
        import_date (date): Date of the import
        db (Session): Database session
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts for database operations
        
    Returns:
        int: Number of records successfully processed
    """
    logger.info(f"Processing Fixed Income sheet")
    
    # For large files, we need to handle them efficiently
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    
    # Check if the file is large (over 100 MB)
    if file_size_mb > 100:
        logger.info(f"Large file detected ({file_size_mb:.2f} MB): Using optimized reading for Fixed Income sheet")
        
        try:
            # Excel reader object
            xl = pd.ExcelFile(file_path)
            
            # Get information about the sheet
            logger.info(f"Attempting to access sheet '{sheet_name}'")
            sheet_data = xl.book.sheet_by_name(sheet_name)
            total_rows = sheet_data.nrows
            logger.info(f"Fixed Income sheet has {total_rows} total rows in Excel file")
            
            # Process in manageable chunks (not using chunksize as pandas doesn't support it for Excel)
            chunk_size = 1000
            chunks_processed = 0
            df = pd.DataFrame()
            
            for start_row in range(0, total_rows, chunk_size):
                chunks_processed += 1
                end_row = min(start_row + chunk_size, total_rows)
                
                logger.info(f"Reading Fixed Income rows {start_row}-{end_row} (chunk {chunks_processed})")
                
                # Read a specific range of rows
                # First row (usually row 0) contains headers
                if start_row == 0:
                    # First chunk includes headers
                    temp_df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=end_row)
                    logger.info(f"Read first chunk with headers. Shape: {temp_df.shape}")
                else:
                    # Subsequent chunks need headers for proper column names and then skip the header row
                    temp_df = pd.read_excel(file_path, sheet_name=sheet_name, 
                                           skiprows=start_row-1, nrows=end_row-start_row+1)
                    logger.info(f"Read subsequent chunk. Shape before header drop: {temp_df.shape}")
                    
                    # Drop the header row which is now the first row
                    if len(temp_df) > 0:
                        temp_df = temp_df.iloc[1:]
                        logger.info(f"Shape after header drop: {temp_df.shape}")
                
                # Clean up column names
                temp_df.columns = [col.strip() if isinstance(col, str) else col for col in temp_df.columns]
                
                # Check if Position column exists
                if 'Position' not in temp_df.columns:
                    available_columns = temp_df.columns.tolist()
                    logger.error(f"Position column not found in Fixed Income sheet. Available columns: {available_columns}")
                    # Try to find an alternative column if possible
                    position_like_columns = [col for col in available_columns if 'name' in str(col).lower() or 'security' in str(col).lower()]
                    if position_like_columns:
                        position_col = position_like_columns[0]
                        logger.info(f"Using alternative column for Position: {position_col}")
                        temp_df['Position'] = temp_df[position_col]
                    else:
                        logger.error("No suitable alternative for Position column found. Skipping chunk.")
                        continue
                
                # Drop rows with no Position
                original_count = len(temp_df)
                temp_df = temp_df.dropna(subset=['Position'], how='all')
                logger.info(f"Dropped {original_count - len(temp_df)} rows with missing Position values")
                
                # Append to our main dataframe
                if not temp_df.empty:
                    df = pd.concat([df, temp_df])
                    logger.info(f"Appended chunk data. Total rows so far: {len(df)}")
                else:
                    logger.warning(f"Chunk {chunks_processed} contains no valid data after filtering")
                
                # If we've read enough rows to handle a large dataset, stop
                if len(df) > 30000:  # Limit to 30,000 rows as a safety valve
                    logger.warning(f"Reached maximum row limit of 30,000 for Fixed Income sheet, stopping read process")
                    break
            
            # Close the Excel file
            xl.close()
            logger.info(f"Finished reading Fixed Income sheet. Total valid rows: {len(df)}")
        except Exception as read_error:
            logger.error(f"Error reading Fixed Income sheet: {read_error}")
            # Create an empty DataFrame with proper columns to avoid further errors
            df = pd.DataFrame(columns=['Position', 'Ticker Symbol', 'CUSIP', 'Bloomberg ID', 'Second Level', 'Duration', 'Amended ID', 'Notes'])
            logger.info("Created empty DataFrame for Fixed Income due to read error")
    else:
        # For smaller files, read the entire sheet at once
        try:
            logger.info(f"Reading entire Fixed Income sheet at once (file size: {file_size_mb:.2f} MB)")
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            logger.info(f"Read Fixed Income sheet successfully. Shape: {df.shape}")
            
            # Clean up column names and drop empty rows
            df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
            
            # Check if Position column exists
            if 'Position' not in df.columns:
                available_columns = df.columns.tolist()
                logger.error(f"Position column not found in Fixed Income sheet. Available columns: {available_columns}")
                # Try to find an alternative column if possible
                position_like_columns = [col for col in available_columns if 'name' in str(col).lower() or 'security' in str(col).lower()]
                if position_like_columns:
                    position_col = position_like_columns[0]
                    logger.info(f"Using alternative column for Position: {position_col}")
                    df['Position'] = df[position_col]
                else:
                    logger.error("No suitable alternative for Position column found. Cannot process Fixed Income sheet.")
                    return 0
            
            original_count = len(df)
            df = df.dropna(subset=['Position'], how='all')
            logger.info(f"Dropped {original_count - len(df)} rows with missing Position values")
        except Exception as read_error:
            logger.error(f"Error reading Fixed Income sheet: {read_error}")
            # Create an empty DataFrame with proper columns to avoid further errors
            df = pd.DataFrame(columns=['Position', 'Ticker Symbol', 'CUSIP', 'Bloomberg ID', 'Second Level', 'Duration', 'Amended ID', 'Notes'])
            logger.info("Created empty DataFrame for Fixed Income due to read error")
    
    # Keep track of records processed
    records_processed = 0
    records_succeeded = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    # batch_size parameter is already provided from function call
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Fixed Income sheet columns: {df.columns.tolist()}")
    logger.info(f"Fixed Income sheet has {len(df)} rows ready for processing")
    
    if len(df) == 0:
        logger.warning("No valid rows found in Fixed Income sheet after filtering. Skipping processing.")
        return 0
    
    # Check for duplicate positions in the input file
    try:
        # Use Series.duplicated() to find positions that appear more than once
        duplicate_mask = df['Position'].duplicated(keep='first')
        duplicate_positions = df.loc[duplicate_mask, 'Position'].tolist()
        
        if duplicate_positions:
            # Take up to 5 examples to log
            sample_duplicates = duplicate_positions[:5]
            logger.warning(f"Found {len(duplicate_positions)} duplicate position names in the Fixed Income sheet: {', '.join(str(d) for d in sample_duplicates)}")
    except Exception as e:
        logger.warning(f"Could not check for duplicates in Fixed Income sheet: {e}")
    
    # Map expected columns to our model fields
    for start_idx in range(0, len(df), batch_size):
        batch_count += 1
        end_idx = min(start_idx + batch_size, len(df))
        logger.info(f"Processing fixed income batch {batch_count} (rows {start_idx}-{end_idx})")
        
        batch_records = []
        
        for index, row in df.iloc[start_idx:end_idx].iterrows():
            try:
                position = str(row.get('Position', '')).strip()
                if not position:
                    logger.debug(f"Skipping row {index} with empty Position value")
                    continue
                
                # Extract and log a sample of the row data for debugging
                if records_processed < 5 or records_processed % 500 == 0:
                    sample_data = {}
                    for col in ['Position', 'Ticker Symbol', 'CUSIP', 'Bloomberg ID', 'Second Level', 'Duration']:
                        if col in row:
                            sample_data[col] = row[col]
                    logger.info(f"Sample row {index} data: {sample_data}")
                    
                ticker_symbol = str(row.get('Ticker Symbol', '')).strip() if 'Ticker Symbol' in row and not pd.isna(row.get('Ticker Symbol')) else None
                cusip = str(row.get('CUSIP', '')).strip() if 'CUSIP' in row and not pd.isna(row.get('CUSIP')) else None
                bloomberg_id = str(row.get('Bloomberg ID', '')).strip() if 'Bloomberg ID' in row and not pd.isna(row.get('Bloomberg ID')) else None
                second_level = str(row.get('Second Level', '')).strip() if 'Second Level' in row and not pd.isna(row.get('Second Level')) else None
                amended_id = str(row.get('Amended ID', '')).strip() if 'Amended ID' in row and not pd.isna(row.get('Amended ID')) else None
                notes = str(row.get('Notes', '')).strip() if 'Notes' in row and not pd.isna(row.get('Notes')) else None
                
                # Get duration, handling potential missing or non-numeric values
                duration = row.get('Duration', None)
                if pd.isna(duration):
                    duration = None
                else:
                    try:
                        duration = float(duration)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not convert Duration value '{duration}' to float for position '{position}': {e}")
                        duration = None
                
                # Create a new risk stat record
                risk_stat = EgnyteRiskStat(
                    import_date=import_date,
                    position=position,
                    ticker_symbol=ticker_symbol,
                    cusip=cusip,
                    asset_class='Fixed Income',
                    second_level=second_level,
                    bloomberg_id=bloomberg_id,
                    duration=duration,
                    notes=notes,
                    amended_id=amended_id,
                    source_file=os.path.basename(file_path),
                    source_tab=sheet_name,
                    source_row=index + 2  # +2 for header row and 0-indexing
                )
                
                batch_records.append(risk_stat)
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing fixed income row {index}: {e}")
                # If we're getting a lot of errors, log more details about the problematic row
                try:
                    if 'Position' in row:
                        pos_val = row['Position']
                        logger.error(f"Problem row had Position value: {pos_val}")
                    logger.error(f"Row data keys: {list(row.keys())}")
                    logger.error(f"Row data (first few items): {dict(list(row.items())[:5])}")
                except Exception as inner_e:
                    logger.error(f"Could not extract detailed error info: {inner_e}")
        
        # Insert the batch of records
        try:
            # Process records individually using merge to handle duplicates gracefully (upsert)
            success_count = 0
            for risk_stat in batch_records:
                try:
                    # Use merge strategy to handle duplicates
                    db.merge(risk_stat)
                    # Commit after each record to ensure partial progress
                    db.commit()
                    success_count += 1
                except Exception as inner_e:
                    db.rollback()
                    inner_msg = str(inner_e)
                    logger.error(f"Error adding individual fixed income record for position {risk_stat.position}: {inner_e}")
            
            logger.info(f"Successfully processed fixed income batch {batch_count}: {success_count}/{len(batch_records)} records merged")
            records_succeeded += success_count
        except Exception as e:
            # Roll back on error
            db.rollback()
            error_msg = str(e)
            logger.error(f"Error processing fixed income batch {batch_count}: {error_msg}")
            
            # Check for specific error conditions
            if "duplicate key value violates unique constraint" in error_msg:
                logger.warning("Duplicate key constraint violation detected in fixed income batch")
                # Extract the name of the duplicated position if possible
                duplicate_match = re.search(r"Key \(import_date, position, asset_class\)=\([^,]+, ([^,]+), [^)]+\)", error_msg)
                duplicate_position = duplicate_match.group(1) if duplicate_match else "unknown"
                logger.warning(f"Duplicate position: {duplicate_position}")
                
                # Try inserting records one by one but skip the problematic ones
                success_count = 0
                for risk_stat in batch_records:
                    try:
                        # Sanitize strings to prevent encoding issues
                        safe_position = risk_stat.position[:255] if risk_stat.position else None
                        safe_ticker = risk_stat.ticker_symbol[:50] if risk_stat.ticker_symbol else None
                        safe_cusip = risk_stat.cusip[:50] if risk_stat.cusip else None
                        safe_asset_class = risk_stat.asset_class[:50] if risk_stat.asset_class else None
                        safe_second_level = risk_stat.second_level[:100] if risk_stat.second_level else None
                        safe_bloomberg_id = risk_stat.bloomberg_id[:100] if risk_stat.bloomberg_id else None
                        safe_notes = risk_stat.notes[:500] if risk_stat.notes else None
                        safe_amended_id = risk_stat.amended_id[:100] if risk_stat.amended_id else None
                        
                        # Create a new clean instance
                        clean_risk_stat = EgnyteRiskStat(
                            import_date=risk_stat.import_date,
                            position=safe_position,
                            ticker_symbol=safe_ticker,
                            cusip=safe_cusip,
                            asset_class=safe_asset_class,
                            second_level=safe_second_level,
                            bloomberg_id=safe_bloomberg_id,
                            volatility=None,  # Not used for fixed income
                            beta=None,  # Not used for fixed income
                            duration=risk_stat.duration,
                            notes=safe_notes,
                            amended_id=safe_amended_id,
                            source_file=os.path.basename(file_path),
                            source_tab=sheet_name,
                            source_row=risk_stat.source_row
                        )
                        
                        # Try simple add with merge strategy
                        db.merge(clean_risk_stat)
                        db.commit()
                        success_count += 1
                    except Exception as inner_e:
                        db.rollback()
                        logger.error(f"Error adding individual fixed income record for position {risk_stat.position}: {inner_e}")
                
                records_succeeded += success_count
                logger.info(f"Successfully added {success_count} out of {len(batch_records)} fixed income records individually")
            else:
                # Handle other types of errors with individual inserts
                success_count = 0
                for risk_stat in batch_records:
                    try:
                        db.add(risk_stat)
                        db.commit()
                        success_count += 1
                    except Exception as inner_e:
                        db.rollback()
                        logger.error(f"Error adding individual fixed income record for position {risk_stat.position}: {inner_e}")
                
                records_succeeded += success_count
                logger.info(f"Successfully added {success_count} out of {len(batch_records)} fixed income records individually")
    
    logger.info(f"Processed {records_processed} fixed income risk statistics, successfully imported {records_succeeded}")
    return records_succeeded


def process_alternatives_sheet(file_path, sheet_name, import_date, db, batch_size=50, max_retries=3):
    """
    Process the Alternatives sheet from the Excel file.
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet to process
        import_date (date): Date of the import
        db (Session): Database session
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts for database operations
        
    Returns:
        int: Number of records successfully processed
    """
    logger.info(f"Processing Alternatives sheet")
    
    # For large files, we need to handle them efficiently
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    
    # Check if the file is large (over 100 MB)
    if file_size_mb > 100:
        logger.info(f"Large file detected ({file_size_mb:.2f} MB): Using optimized reading for Alternatives sheet")
        
        try:
            # Excel reader object
            xl = pd.ExcelFile(file_path)
            
            # Get information about the sheet
            logger.info(f"Attempting to access sheet '{sheet_name}'")
            sheet_data = xl.book.sheet_by_name(sheet_name)
            total_rows = sheet_data.nrows
            logger.info(f"Alternatives sheet has {total_rows} total rows in Excel file")
            
            # Process in manageable chunks (not using chunksize as pandas doesn't support it for Excel)
            chunk_size = 1000
            chunks_processed = 0
            df = pd.DataFrame()
            
            for start_row in range(0, total_rows, chunk_size):
                chunks_processed += 1
                end_row = min(start_row + chunk_size, total_rows)
                
                logger.info(f"Reading Alternatives rows {start_row}-{end_row} (chunk {chunks_processed})")
                
                # Read a specific range of rows
                # First row (usually row 0) contains headers
                if start_row == 0:
                    # First chunk includes headers
                    temp_df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=end_row)
                    logger.info(f"Read first chunk with headers. Shape: {temp_df.shape}")
                else:
                    # Subsequent chunks need headers for proper column names and then skip the header row
                    temp_df = pd.read_excel(file_path, sheet_name=sheet_name, 
                                           skiprows=start_row-1, nrows=end_row-start_row+1)
                    logger.info(f"Read subsequent chunk. Shape before header drop: {temp_df.shape}")
                    
                    # Drop the header row which is now the first row
                    if len(temp_df) > 0:
                        temp_df = temp_df.iloc[1:]
                        logger.info(f"Shape after header drop: {temp_df.shape}")
                
                # Clean up column names
                temp_df.columns = [col.strip() if isinstance(col, str) else col for col in temp_df.columns]
                
                # Check if Position column exists
                if 'Position' not in temp_df.columns:
                    available_columns = temp_df.columns.tolist()
                    logger.error(f"Position column not found in Alternatives sheet. Available columns: {available_columns}")
                    # Try to find an alternative column if possible
                    position_like_columns = [col for col in available_columns if 'name' in str(col).lower() or 'security' in str(col).lower()]
                    if position_like_columns:
                        position_col = position_like_columns[0]
                        logger.info(f"Using alternative column for Position: {position_col}")
                        temp_df['Position'] = temp_df[position_col]
                    else:
                        logger.error("No suitable alternative for Position column found. Skipping chunk.")
                        continue
                
                # Drop rows with no Position
                original_count = len(temp_df)
                temp_df = temp_df.dropna(subset=['Position'], how='all')
                logger.info(f"Dropped {original_count - len(temp_df)} rows with missing Position values")
                
                # Append to our main dataframe
                if not temp_df.empty:
                    df = pd.concat([df, temp_df])
                    logger.info(f"Appended chunk data. Total rows so far: {len(df)}")
                else:
                    logger.warning(f"Chunk {chunks_processed} contains no valid data after filtering")
                
                # If we've read enough rows to handle a large dataset, stop
                if len(df) > 30000:  # Limit to 30,000 rows as a safety valve
                    logger.warning(f"Reached maximum row limit of 30,000 for Alternatives sheet, stopping read process")
                    break
            
            # Close the Excel file
            xl.close()
            logger.info(f"Finished reading Alternatives sheet. Total valid rows: {len(df)}")
        except Exception as read_error:
            logger.error(f"Error reading Alternatives sheet: {read_error}")
            # Create an empty DataFrame with proper columns to avoid further errors
            df = pd.DataFrame(columns=['Position', 'Ticker Symbol', 'CUSIP', 'Bloomberg ID', 'Second Level', 'BETA', 'Amended ID', 'Notes'])
            logger.info("Created empty DataFrame for Alternatives due to read error")
    else:
        # For smaller files, read the entire sheet at once
        try:
            logger.info(f"Reading entire Alternatives sheet at once (file size: {file_size_mb:.2f} MB)")
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            logger.info(f"Read Alternatives sheet successfully. Shape: {df.shape}")
            
            # Clean up column names and drop empty rows
            df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
            
            # Check if Position column exists
            if 'Position' not in df.columns:
                available_columns = df.columns.tolist()
                logger.error(f"Position column not found in Alternatives sheet. Available columns: {available_columns}")
                # Try to find an alternative column if possible
                position_like_columns = [col for col in available_columns if 'name' in str(col).lower() or 'security' in str(col).lower()]
                if position_like_columns:
                    position_col = position_like_columns[0]
                    logger.info(f"Using alternative column for Position: {position_col}")
                    df['Position'] = df[position_col]
                else:
                    logger.error("No suitable alternative for Position column found. Cannot process Alternatives sheet.")
                    return 0
            
            original_count = len(df)
            df = df.dropna(subset=['Position'], how='all')
            logger.info(f"Dropped {original_count - len(df)} rows with missing Position values")
        except Exception as read_error:
            logger.error(f"Error reading Alternatives sheet: {read_error}")
            # Create an empty DataFrame with proper columns to avoid further errors
            df = pd.DataFrame(columns=['Position', 'Ticker Symbol', 'CUSIP', 'Bloomberg ID', 'Second Level', 'BETA', 'Amended ID', 'Notes'])
            logger.info("Created empty DataFrame for Alternatives due to read error")
    
    # Keep track of records processed
    records_processed = 0
    records_succeeded = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    # batch_size parameter is already provided from function call
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Alternatives sheet columns: {df.columns.tolist()}")
    logger.info(f"Alternatives sheet has {len(df)} rows ready for processing")
    
    if len(df) == 0:
        logger.warning("No valid rows found in Alternatives sheet after filtering. Skipping processing.")
        return 0
    
    # Check for duplicate positions in the input file
    try:
        # Use Series.duplicated() to find positions that appear more than once
        duplicate_mask = df['Position'].duplicated(keep='first')
        duplicate_positions = df.loc[duplicate_mask, 'Position'].tolist()
        
        if duplicate_positions:
            # Take up to 5 examples to log
            sample_duplicates = duplicate_positions[:5]
            logger.warning(f"Found {len(duplicate_positions)} duplicate position names in the Alternatives sheet: {', '.join(str(d) for d in sample_duplicates)}")
    except Exception as e:
        logger.warning(f"Could not check for duplicates in Alternatives sheet: {e}")
    
    # Map expected columns to our model fields
    for start_idx in range(0, len(df), batch_size):
        batch_count += 1
        end_idx = min(start_idx + batch_size, len(df))
        logger.info(f"Processing alternatives batch {batch_count} (rows {start_idx}-{end_idx})")
        
        batch_records = []
        
        for index, row in df.iloc[start_idx:end_idx].iterrows():
            try:
                position = str(row.get('Position', '')).strip()
                if not position:
                    logger.debug(f"Skipping row {index} with empty Position value")
                    continue
                
                # Extract and log a sample of the row data for debugging
                if records_processed < 5 or records_processed % 500 == 0:
                    sample_data = {}
                    for col in ['Position', 'Ticker Symbol', 'CUSIP', 'Bloomberg ID', 'Second Level', 'BETA']:
                        if col in row:
                            sample_data[col] = row[col]
                    logger.info(f"Sample row {index} data: {sample_data}")
                    
                ticker_symbol = str(row.get('Ticker Symbol', '')).strip() if 'Ticker Symbol' in row and not pd.isna(row.get('Ticker Symbol')) else None
                cusip = str(row.get('CUSIP', '')).strip() if 'CUSIP' in row and not pd.isna(row.get('CUSIP')) else None
                bloomberg_id = str(row.get('Bloomberg ID', '')).strip() if 'Bloomberg ID' in row and not pd.isna(row.get('Bloomberg ID')) else None
                second_level = str(row.get('Second Level', '')).strip() if 'Second Level' in row and not pd.isna(row.get('Second Level')) else None
                amended_id = str(row.get('Amended ID', '')).strip() if 'Amended ID' in row and not pd.isna(row.get('Amended ID')) else None
                notes = str(row.get('Notes', '')).strip() if 'Notes' in row and not pd.isna(row.get('Notes')) else None
                
                # Check for 'BETA' column and possible alternatives
                beta = None
                beta_candidates = ['BETA', 'Beta', 'beta']
                
                for beta_col in beta_candidates:
                    if beta_col in row:
                        beta = row.get(beta_col, None)
                        logger.debug(f"Found beta value {beta} in column {beta_col}")
                        break
                
                if pd.isna(beta):
                    beta = None
                else:
                    try:
                        beta = float(beta)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not convert Beta value '{beta}' to float for position '{position}': {e}")
                        beta = None
                
                # Create a new risk stat record
                risk_stat = EgnyteRiskStat(
                    import_date=import_date,
                    position=position,
                    ticker_symbol=ticker_symbol,
                    cusip=cusip,
                    asset_class='Alternatives',
                    second_level=second_level,
                    bloomberg_id=bloomberg_id,
                    beta=beta,
                    notes=notes,
                    amended_id=amended_id,
                    source_file=os.path.basename(file_path),
                    source_tab=sheet_name,
                    source_row=index + 2  # +2 for header row and 0-indexing
                )
                
                batch_records.append(risk_stat)
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing alternatives row {index}: {e}")
                # If we're getting a lot of errors, log more details about the problematic row
                try:
                    if 'Position' in row:
                        pos_val = row['Position']
                        logger.error(f"Problem row had Position value: {pos_val}")
                    logger.error(f"Row data keys: {list(row.keys())}")
                    logger.error(f"Row data (first few items): {dict(list(row.items())[:5])}")
                except Exception as inner_e:
                    logger.error(f"Could not extract detailed error info: {inner_e}")
        
        # Insert the batch of records
        try:
            # Process records individually using merge to handle duplicates gracefully (upsert)
            success_count = 0
            for risk_stat in batch_records:
                try:
                    # Use merge strategy to handle duplicates
                    db.merge(risk_stat)
                    # Commit after each record to ensure partial progress
                    db.commit()
                    success_count += 1
                except Exception as inner_e:
                    db.rollback()
                    inner_msg = str(inner_e)
                    logger.error(f"Error adding individual alternatives record for position {risk_stat.position}: {inner_e}")
            
            logger.info(f"Successfully processed alternatives batch {batch_count}: {success_count}/{len(batch_records)} records merged")
            records_succeeded += success_count
        except Exception as e:
            # Roll back on error
            db.rollback()
            error_msg = str(e)
            logger.error(f"Error processing alternatives batch {batch_count}: {error_msg}")
            
            # Check for specific error conditions
            if "duplicate key value violates unique constraint" in error_msg:
                logger.warning("Duplicate key constraint violation detected in alternatives batch")
                # Extract the name of the duplicated position if possible
                duplicate_match = re.search(r"Key \(import_date, position, asset_class\)=\([^,]+, ([^,]+), [^)]+\)", error_msg)
                duplicate_position = duplicate_match.group(1) if duplicate_match else "unknown"
                logger.warning(f"Duplicate position: {duplicate_position}")
                
                # Try inserting records one by one but skip the problematic ones
                success_count = 0
                for risk_stat in batch_records:
                    try:
                        # Sanitize strings to prevent encoding issues
                        safe_position = risk_stat.position[:255] if risk_stat.position else None
                        safe_ticker = risk_stat.ticker_symbol[:50] if risk_stat.ticker_symbol else None
                        safe_cusip = risk_stat.cusip[:50] if risk_stat.cusip else None
                        safe_asset_class = risk_stat.asset_class[:50] if risk_stat.asset_class else None
                        safe_second_level = risk_stat.second_level[:100] if risk_stat.second_level else None
                        safe_bloomberg_id = risk_stat.bloomberg_id[:100] if risk_stat.bloomberg_id else None
                        safe_notes = risk_stat.notes[:500] if risk_stat.notes else None
                        safe_amended_id = risk_stat.amended_id[:100] if risk_stat.amended_id else None
                        
                        # Create a new clean instance
                        clean_risk_stat = EgnyteRiskStat(
                            import_date=risk_stat.import_date,
                            position=safe_position,
                            ticker_symbol=safe_ticker,
                            cusip=safe_cusip,
                            asset_class=safe_asset_class,
                            second_level=safe_second_level,
                            bloomberg_id=safe_bloomberg_id,
                            volatility=None,  # Not used for alternatives
                            beta=risk_stat.beta,
                            duration=None,  # Not used for alternatives
                            notes=safe_notes,
                            amended_id=safe_amended_id,
                            source_file=os.path.basename(file_path),
                            source_tab=sheet_name,
                            source_row=risk_stat.source_row
                        )
                        
                        # Try simple add with merge strategy
                        db.merge(clean_risk_stat)
                        db.commit()
                        success_count += 1
                    except Exception as inner_e:
                        db.rollback()
                        logger.error(f"Error adding individual alternatives record for position {risk_stat.position}: {inner_e}")
                
                records_succeeded += success_count
                logger.info(f"Successfully added {success_count} out of {len(batch_records)} alternatives records individually")
            else:
                # Handle other types of errors with individual inserts
                success_count = 0
                for risk_stat in batch_records:
                    try:
                        db.add(risk_stat)
                        db.commit()
                        success_count += 1
                    except Exception as inner_e:
                        db.rollback()
                        logger.error(f"Error adding individual alternatives record for position {risk_stat.position}: {inner_e}")
                
                records_succeeded += success_count
                logger.info(f"Successfully added {success_count} out of {len(batch_records)} alternatives records individually")
    
    logger.info(f"Processed {records_processed} alternatives risk statistics, successfully imported {records_succeeded}")
    return records_succeeded


def fetch_and_process_risk_stats(db: Session, use_test_file=False, batch_size=50, max_retries=3):
    """
    Main function to fetch and process risk statistics from Egnyte.
    
    Args:
        db (Session): Database session
        use_test_file (bool): Whether to use a test file instead of downloading from Egnyte
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts for database operations
        
    Returns:
        dict: Summary of the import process
    """
    try:
        logger.info("Starting risk statistics fetch from Egnyte")
        
        # Set the import date
        import_date = date.today()
        file_path = None
        
        # Download the file first to avoid cleaning records if there's no file to process
        try:
            logger.info(f"Downloading risk stats file (use_test_file={use_test_file})")
            file_path = download_risk_stats_file(use_test_file=use_test_file)
            
            if not file_path:
                return {
                    "success": False,
                    "error": "Failed to download risk statistics: No file path returned"
                }
                
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            logger.info(f"Successfully downloaded risk stats file to {file_path} ({file_size} bytes)")
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
            elif "connection" in error_msg.lower():
                return {
                    "success": False,
                    "error": "Network error connecting to Egnyte. Please check your internet connection.",
                    "detail": error_msg
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to download risk statistics: {error_msg}",
                    "detail": error_msg
                }
        
        # Now delete any existing records for today's date to prevent unique constraint violations
        try:
            # Get count for logging by asset class
            equity_count = db.query(EgnyteRiskStat).filter(
                EgnyteRiskStat.import_date == import_date,
                EgnyteRiskStat.asset_class == 'Equity'
            ).count()
            
            fixed_income_count = db.query(EgnyteRiskStat).filter(
                EgnyteRiskStat.import_date == import_date,
                EgnyteRiskStat.asset_class == 'Fixed Income'
            ).count()
            
            alternatives_count = db.query(EgnyteRiskStat).filter(
                EgnyteRiskStat.import_date == import_date,
                EgnyteRiskStat.asset_class == 'Alternatives'
            ).count()
            
            total_count = equity_count + fixed_income_count + alternatives_count
            
            # Always do the delete even if count is 0, just to be safe
            logger.info(f"Cleaning up {total_count} existing records for import date {import_date}")
            logger.info(f"By asset class - Equity: {equity_count}, Fixed Income: {fixed_income_count}, Alternatives: {alternatives_count}")
            
            # Use raw SQL for better performance on large deletes
            # This is especially important when dealing with potential conflicts
            sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :import_date")
            result = db.execute(sql, {"import_date": import_date})
            
            # Log the actual number of rows deleted
            logger.info(f"Deleted {result.rowcount} records from egnyte_risk_stats table")
            
            # Commit immediately to ensure clean state
            db.commit()
            
            # Verify the deletion worked
            verify_count = db.query(EgnyteRiskStat).filter(
                EgnyteRiskStat.import_date == import_date
            ).count()
            
            if verify_count > 0:
                logger.warning(f"Found {verify_count} records still remaining after deletion - attempting additional cleanup")
                # Try an alternative deletion approach
                try:
                    # Delete records by asset class one by one
                    for asset_class in ['Equity', 'Fixed Income', 'Alternatives']:
                        asset_sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :import_date AND asset_class = :asset_class")
                        asset_result = db.execute(asset_sql, {"import_date": import_date, "asset_class": asset_class})
                        logger.info(f"Deleted {asset_result.rowcount} {asset_class} records in secondary cleanup")
                        db.commit()
                except Exception as sec_error:
                    db.rollback()
                    logger.error(f"Error in secondary cleanup: {sec_error}")
            
            logger.info("Database cleanup completed successfully")
            
        except Exception as db_error:
            # Roll back on error
            db.rollback()
            logger.error(f"Failed to clean up existing records: {db_error}")
            # In case of database errors, better not to proceed
            if file_path and os.path.exists(file_path):
                try:
                    os.unlink(file_path)  # Clean up file since we're not using it
                except:
                    pass
            return {
                "success": False,
                "error": f"Database error during preparation: {str(db_error)}"
            }
        
        # Now process the file and insert data into the database
        try:
            stats = process_excel_file(file_path, db, batch_size=batch_size, max_retries=max_retries)
            logger.info(f"Successfully processed Excel file with stats: {stats}")
            
            # We don't need to commit here as each processing function manages its own transactions
            logger.info("Successfully completed risk stats data import")
            
            # Add a success check - if any records were actually imported
            if stats["total_records"] == 0:
                logger.warning("No records were imported from the risk stats file")
                return {
                    "success": True,
                    "warning": "No records were found in the risk stats file",
                    "stats": stats
                }
        except Exception as process_error:
            db.rollback()  # Roll back any partial changes
            logger.error(f"Error processing Excel file: {process_error}")
            
            # Provide more specific error information
            error_msg = str(process_error)
            if "duplicate key value violates unique constraint" in error_msg:
                return {
                    "success": False,
                    "error": "Duplicate records found in the risk stats file. Please check your data and try again.",
                    "detail": error_msg
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to process risk statistics file: {error_msg}"
                }
        finally:
            # Always try to clean up the temporary file
            if file_path and os.path.exists(file_path):
                try:
                    os.unlink(file_path)
                    logger.info(f"Temporary file {file_path} removed")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to remove temporary file {file_path}: {cleanup_error}")
        
        return {
            "success": True,
            "import_date": import_date.isoformat(),
            "stats": stats,
            "message": "Risk statistics updated successfully"
        }
    
    except Exception as e:
        logger.exception(f"Error in risk statistics processing: {str(e)}")
        
        # Check for specific error types to provide better feedback
        error_msg = str(e)
        if "duplicate key value violates unique constraint" in error_msg:
            return {
                "success": False,
                "error": "Database conflict occurred. The system already has risk statistics for this date.",
                "detail": error_msg
            }
        elif "EGNYTE_ACCESS_TOKEN" in error_msg:
            return {
                "success": False,
                "error": "Egnyte API token is missing or invalid. Please check your configuration.",
                "detail": error_msg
            }
        else:
            return {
                "success": False,
                "error": f"Unexpected error processing risk statistics: {error_msg}"
            }


def get_latest_risk_stats(db: Session, asset_class=None):
    """
    Get the latest risk statistics from the database.
    
    Args:
        db (Session): Database session
        asset_class (str, optional): Filter to specific asset class. Defaults to None.
        
    Returns:
        list: List of risk statistics records
    """
    query = db.query(EgnyteRiskStat)
    
    # Filter by asset class if specified
    if asset_class:
        query = query.filter(EgnyteRiskStat.asset_class == asset_class)
    
    # Get the latest import date
    latest_date_query = db.query(
        text("MAX(import_date) as latest_date")
    ).select_from(EgnyteRiskStat)
    
    latest_date_result = latest_date_query.one()
    if not latest_date_result or not latest_date_result[0]:
        return []
    
    latest_date = latest_date_result[0]
    
    # Get all records from the latest import date
    query = query.filter(EgnyteRiskStat.import_date == latest_date)
    
    return query.all()