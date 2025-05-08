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


def download_risk_stats_file(token=None, domain=None, file_path=None):
    """
    Download the Excel file containing risk statistics from Egnyte.
    
    Args:
        token (str, optional): Egnyte API token. If not provided, will be retrieved from env var.
        domain (str, optional): Egnyte domain. Defaults to "procapitalpartners.egnyte.com".
        file_path (str, optional): Path to the file in Egnyte. Defaults to shared risk stats file.
        
    Returns:
        str: Path to the downloaded temporary file
    """
    # Use provided parameters or defaults
    token = token or get_egnyte_token()
    domain = domain or "procapitalpartners.egnyte.com"
    file_path = file_path or "/Shared/Internal Documents/Proficio Capital Partners/Asset Allocation/Portfolio Management/New Portfolio Sheets/Security Risk Stats.xlsx"
    
    logger.info(f"Downloading risk statistics file from Egnyte: {file_path}")
    
    url = f"https://{domain}/pubapi/v1/fs-content{file_path}"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        logger.error(f"Failed to download file from Egnyte: HTTP {response.status_code}")
        logger.error(f"Response: {response.text}")
        raise Exception(f"Failed to download file from Egnyte: HTTP {response.status_code}")
    
    # Create a temporary file to store the downloaded file
    temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    temp_file.write(response.content)
    temp_file.close()
    
    logger.info(f"Risk statistics file downloaded to {temp_file.name}")
    return temp_file.name


def process_excel_file(file_path, db):
    """
    Process the downloaded Excel file and insert the data into the database.
    
    Args:
        file_path (str): Path to the Excel file
        db (Session): Database session
        
    Returns:
        dict: Summary statistics of the import
    """
    logger.info(f"Processing risk statistics file: {file_path}")
    
    # Read the Excel file with multiple sheets
    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names
    logger.info(f"Excel file contains sheets: {sheet_names}")
    
    # Examine the structure of each sheet for better understanding
    for sheet in sheet_names:
        try:
            # Read just a few rows to analyze structure
            df = pd.read_excel(file_path, sheet_name=sheet, nrows=5)
            logger.info(f"Sheet: {sheet}, Columns: {df.columns.tolist()}")
            if len(df) > 0:
                sample_row = df.iloc[0].to_dict()
                # Convert any non-serializable types to strings for logging
                sample_row = {k: str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v 
                              for k, v in sample_row.items()}
                logger.info(f"Sample data from {sheet}: {sample_row}")
            else:
                logger.info(f"No data in sheet {sheet}")
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
        elif any(term in lower_sheet for term in ["fixed", "fixed income", "fi ", "fixed inc", "duration"]):
            fixed_income_sheet = sheet
        elif any(term in lower_sheet for term in ["alternative", "alt", "alts"]):
            alternatives_sheet = sheet
    
    # Process the Equity sheet if found
    if equity_sheet:
        logger.info(f"Found Equity sheet: {equity_sheet}")
        stats["equity_records"] = process_equity_sheet(file_path, equity_sheet, import_date, db)
    else:
        logger.warning("No Equity sheet found in the Excel file")
    
    # Process the Fixed Income sheet if found
    if fixed_income_sheet:
        logger.info(f"Found Fixed Income sheet: {fixed_income_sheet}")
        stats["fixed_income_records"] = process_fixed_income_sheet(file_path, fixed_income_sheet, import_date, db)
    else:
        logger.warning("No Fixed Income sheet found in the Excel file")
    
    # Process the Alternatives sheet if found
    if alternatives_sheet:
        logger.info(f"Found Alternatives sheet: {alternatives_sheet}")
        stats["alternatives_records"] = process_alternatives_sheet(file_path, alternatives_sheet, import_date, db)
    else:
        logger.warning("No Alternatives sheet found in the Excel file")
    
    # Calculate total records
    stats["total_records"] = (
        stats["equity_records"] + 
        stats["fixed_income_records"] + 
        stats["alternatives_records"]
    )
    
    logger.info(f"Imported {stats['total_records']} risk statistics records")
    return stats


def process_equity_sheet(file_path, sheet_name, import_date, db):
    """Process the Equity sheet from the Excel file."""
    logger.info(f"Processing Equity sheet")
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Clean up column names and drop empty rows
    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
    df = df.dropna(subset=['Position'], how='all')
    
    # Keep track of records processed
    records_processed = 0
    records_succeeded = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    batch_size = 100
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Equity sheet columns: {df.columns.tolist()}")
    logger.info(f"Equity sheet has {len(df)} rows")
    
    # Check for duplicate positions in the input file
    position_counts = df['Position'].value_counts()
    duplicates = position_counts[position_counts > 1].index.tolist()
    if duplicates:
        logger.warning(f"Found {len(duplicates)} duplicate position names in the Equity sheet: {', '.join(duplicates[:5])}")
    
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
            # Add all records in the batch
            db.add_all(batch_records)
            # Commit the batch
            db.commit()
            logger.info(f"Committed equity batch {batch_count} with {len(batch_records)} records")
            records_succeeded += len(batch_records)
        except Exception as e:
            # Roll back on error
            db.rollback()
            error_msg = str(e)
            logger.error(f"Error committing equity batch {batch_count}: {error_msg}")
            
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
                        # Use a raw SQL INSERT with ON CONFLICT DO NOTHING to handle duplicates gracefully
                        sql = text("""
                            INSERT INTO egnyte_risk_stats 
                            (import_date, position, ticker_symbol, cusip, asset_class, second_level, bloomberg_id, 
                             volatility, beta, duration, notes, amended_id, source_file, source_tab, source_row)
                            VALUES 
                            (:import_date, :position, :ticker_symbol, :cusip, :asset_class, :second_level, :bloomberg_id,
                             :volatility, :beta, :duration, :notes, :amended_id, :source_file, :source_tab, :source_row)
                            ON CONFLICT (import_date, position, asset_class) DO NOTHING
                        """)
                        
                        db.execute(sql, {
                            "import_date": risk_stat.import_date,
                            "position": risk_stat.position,
                            "ticker_symbol": risk_stat.ticker_symbol,
                            "cusip": risk_stat.cusip,
                            "asset_class": risk_stat.asset_class,
                            "second_level": risk_stat.second_level,
                            "bloomberg_id": risk_stat.bloomberg_id,
                            "volatility": risk_stat.volatility,
                            "beta": risk_stat.beta,
                            "duration": None,  # Not used for equity
                            "notes": risk_stat.notes,
                            "amended_id": risk_stat.amended_id,
                            "source_file": risk_stat.source_file,
                            "source_tab": risk_stat.source_tab,
                            "source_row": risk_stat.source_row
                        })
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


def process_fixed_income_sheet(file_path, sheet_name, import_date, db):
    """Process the Fixed Income sheet from the Excel file."""
    logger.info(f"Processing Fixed Income sheet")
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Clean up column names and drop empty rows
    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
    df = df.dropna(subset=['Position'], how='all')
    
    # Keep track of records processed
    records_processed = 0
    records_succeeded = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    batch_size = 100
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Fixed Income sheet columns: {df.columns.tolist()}")
    logger.info(f"Fixed Income sheet has {len(df)} rows")
    
    # Check for duplicate positions in the input file
    position_counts = df['Position'].value_counts()
    duplicates = position_counts[position_counts > 1].index.tolist()
    if duplicates:
        logger.warning(f"Found {len(duplicates)} duplicate position names in the Fixed Income sheet: {', '.join(duplicates[:5])}")
    
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
                    continue
                    
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
                    except (ValueError, TypeError):
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
        
        # Insert the batch of records
        try:
            # Add all records in the batch
            db.add_all(batch_records)
            # Commit the batch
            db.commit()
            logger.info(f"Committed fixed income batch {batch_count} with {len(batch_records)} records")
            records_succeeded += len(batch_records)
        except Exception as e:
            # Roll back on error
            db.rollback()
            error_msg = str(e)
            logger.error(f"Error committing fixed income batch {batch_count}: {error_msg}")
            
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
                        # Use a raw SQL INSERT with ON CONFLICT DO NOTHING to handle duplicates gracefully
                        sql = text("""
                            INSERT INTO egnyte_risk_stats 
                            (import_date, position, ticker_symbol, cusip, asset_class, second_level, bloomberg_id, 
                             volatility, beta, duration, notes, amended_id, source_file, source_tab, source_row)
                            VALUES 
                            (:import_date, :position, :ticker_symbol, :cusip, :asset_class, :second_level, :bloomberg_id,
                             :volatility, :beta, :duration, :notes, :amended_id, :source_file, :source_tab, :source_row)
                            ON CONFLICT (import_date, position, asset_class) DO NOTHING
                        """)
                        
                        db.execute(sql, {
                            "import_date": risk_stat.import_date,
                            "position": risk_stat.position,
                            "ticker_symbol": risk_stat.ticker_symbol,
                            "cusip": risk_stat.cusip,
                            "asset_class": risk_stat.asset_class,
                            "second_level": risk_stat.second_level,
                            "bloomberg_id": risk_stat.bloomberg_id,
                            "volatility": None,  # Not used for fixed income
                            "beta": None,  # Not used for fixed income
                            "duration": risk_stat.duration,
                            "notes": risk_stat.notes,
                            "amended_id": risk_stat.amended_id,
                            "source_file": risk_stat.source_file,
                            "source_tab": risk_stat.source_tab,
                            "source_row": risk_stat.source_row
                        })
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


def process_alternatives_sheet(file_path, sheet_name, import_date, db):
    """Process the Alternatives sheet from the Excel file."""
    logger.info(f"Processing Alternatives sheet")
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Clean up column names and drop empty rows
    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
    df = df.dropna(subset=['Position'], how='all')
    
    # Keep track of records processed
    records_processed = 0
    records_succeeded = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    batch_size = 100
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Alternatives sheet columns: {df.columns.tolist()}")
    logger.info(f"Alternatives sheet has {len(df)} rows")
    
    # Check for duplicate positions in the input file
    position_counts = df['Position'].value_counts()
    duplicates = position_counts[position_counts > 1].index.tolist()
    if duplicates:
        logger.warning(f"Found {len(duplicates)} duplicate position names in the Alternatives sheet: {', '.join(duplicates[:5])}")
    
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
                    continue
                    
                ticker_symbol = str(row.get('Ticker Symbol', '')).strip() if 'Ticker Symbol' in row and not pd.isna(row.get('Ticker Symbol')) else None
                cusip = str(row.get('CUSIP', '')).strip() if 'CUSIP' in row and not pd.isna(row.get('CUSIP')) else None
                bloomberg_id = str(row.get('Bloomberg ID', '')).strip() if 'Bloomberg ID' in row and not pd.isna(row.get('Bloomberg ID')) else None
                second_level = str(row.get('Second Level', '')).strip() if 'Second Level' in row and not pd.isna(row.get('Second Level')) else None
                amended_id = str(row.get('Amended ID', '')).strip() if 'Amended ID' in row and not pd.isna(row.get('Amended ID')) else None
                notes = str(row.get('Notes', '')).strip() if 'Notes' in row and not pd.isna(row.get('Notes')) else None
                
                # Alternatives typically only have beta
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
        
        # Insert the batch of records
        try:
            # Add all records in the batch
            db.add_all(batch_records)
            # Commit the batch
            db.commit()
            logger.info(f"Committed alternatives batch {batch_count} with {len(batch_records)} records")
            records_succeeded += len(batch_records)
        except Exception as e:
            # Roll back on error
            db.rollback()
            error_msg = str(e)
            logger.error(f"Error committing alternatives batch {batch_count}: {error_msg}")
            
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
                        # Use a raw SQL INSERT with ON CONFLICT DO NOTHING to handle duplicates gracefully
                        sql = text("""
                            INSERT INTO egnyte_risk_stats 
                            (import_date, position, ticker_symbol, cusip, asset_class, second_level, bloomberg_id, 
                             volatility, beta, duration, notes, amended_id, source_file, source_tab, source_row)
                            VALUES 
                            (:import_date, :position, :ticker_symbol, :cusip, :asset_class, :second_level, :bloomberg_id,
                             :volatility, :beta, :duration, :notes, :amended_id, :source_file, :source_tab, :source_row)
                            ON CONFLICT (import_date, position, asset_class) DO NOTHING
                        """)
                        
                        db.execute(sql, {
                            "import_date": risk_stat.import_date,
                            "position": risk_stat.position,
                            "ticker_symbol": risk_stat.ticker_symbol,
                            "cusip": risk_stat.cusip,
                            "asset_class": risk_stat.asset_class,
                            "second_level": risk_stat.second_level,
                            "bloomberg_id": risk_stat.bloomberg_id,
                            "volatility": None,  # Not used for alternatives
                            "beta": risk_stat.beta,
                            "duration": None,  # Not used for alternatives
                            "notes": risk_stat.notes,
                            "amended_id": risk_stat.amended_id,
                            "source_file": risk_stat.source_file,
                            "source_tab": risk_stat.source_tab,
                            "source_row": risk_stat.source_row
                        })
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


def fetch_and_process_risk_stats(db: Session):
    """
    Main function to fetch and process risk statistics from Egnyte.
    
    Args:
        db (Session): Database session
        
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
            file_path = download_risk_stats_file()
            logger.info(f"Successfully downloaded risk stats file to {file_path}")
        except Exception as download_error:
            logger.error(f"Failed to download risk stats file: {download_error}")
            return {
                "success": False,
                "error": f"Failed to download risk statistics: {str(download_error)}"
            }
        
        # Now delete any existing records for today's date to prevent unique constraint violations
        try:
            # Get count for logging
            existing_count = db.query(EgnyteRiskStat).filter(
                EgnyteRiskStat.import_date == import_date
            ).count()
            
            # Always do the delete even if count is 0, just to be safe
            logger.info(f"Cleaning up {existing_count} existing records for import date {import_date}")
            
            # Use raw SQL for better performance on large deletes
            # This is especially important when dealing with potential conflicts
            sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :import_date")
            db.execute(sql, {"import_date": import_date})
            
            # Commit immediately to ensure clean state
            db.commit()
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
            stats = process_excel_file(file_path, db)
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