"""
Service for interacting with the Egnyte API to fetch risk statistics.
"""
import os
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
    
    import_date = date.today()
    stats = {
        "total_records": 0,
        "equity_records": 0,
        "fixed_income_records": 0,
        "alternatives_records": 0
    }
    
    # Process each sheet based on its name
    for sheet_name in sheet_names:
        if isinstance(sheet_name, str) and sheet_name.lower() == 'equity':
            stats["equity_records"] = process_equity_sheet(file_path, sheet_name, import_date, db)
        elif isinstance(sheet_name, str) and sheet_name.lower() == 'fixed income':
            stats["fixed_income_records"] = process_fixed_income_sheet(file_path, sheet_name, import_date, db)
        elif isinstance(sheet_name, str) and sheet_name.lower() == 'alternatives':
            stats["alternatives_records"] = process_alternatives_sheet(file_path, sheet_name, import_date, db)
    
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
    
    # Process in smaller batches to avoid excessive parameter lists
    batch_size = 100
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Equity sheet columns: {df.columns.tolist()}")
    logger.info(f"Equity sheet has {len(df)} rows")
    
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
            logger.info(f"Committed batch {batch_count} with {len(batch_records)} records")
        except Exception as e:
            # Roll back on error
            db.rollback()
            logger.error(f"Error committing batch {batch_count}: {e}")
            # Try inserting records one by one to identify problematic records
            for risk_stat in batch_records:
                try:
                    db.add(risk_stat)
                    db.commit()
                    logger.info(f"Individually added record for position: {risk_stat.position}")
                except Exception as inner_e:
                    db.rollback()
                    logger.error(f"Error adding individual record for position {risk_stat.position}: {inner_e}")
    
    logger.info(f"Processed {records_processed} equity risk statistics")
    return records_processed


def process_fixed_income_sheet(file_path, sheet_name, import_date, db):
    """Process the Fixed Income sheet from the Excel file."""
    logger.info(f"Processing Fixed Income sheet")
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Clean up column names and drop empty rows
    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
    df = df.dropna(subset=['Position'], how='all')
    
    # Keep track of records processed
    records_processed = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    batch_size = 100
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Fixed Income sheet columns: {df.columns.tolist()}")
    logger.info(f"Fixed Income sheet has {len(df)} rows")
    
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
            logger.info(f"Committed batch {batch_count} with {len(batch_records)} records")
        except Exception as e:
            # Roll back on error
            db.rollback()
            logger.error(f"Error committing batch {batch_count}: {e}")
            # Try inserting records one by one to identify problematic records
            for risk_stat in batch_records:
                try:
                    db.add(risk_stat)
                    db.commit()
                    logger.info(f"Individually added record for position: {risk_stat.position}")
                except Exception as inner_e:
                    db.rollback()
                    logger.error(f"Error adding individual record for position {risk_stat.position}: {inner_e}")
    
    logger.info(f"Processed {records_processed} fixed income risk statistics")
    return records_processed


def process_alternatives_sheet(file_path, sheet_name, import_date, db):
    """Process the Alternatives sheet from the Excel file."""
    logger.info(f"Processing Alternatives sheet")
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Clean up column names and drop empty rows
    df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
    df = df.dropna(subset=['Position'], how='all')
    
    # Keep track of records processed
    records_processed = 0
    
    # Process in smaller batches to avoid excessive parameter lists
    batch_size = 100
    batch_count = 0
    
    # Log the sheet structure
    logger.info(f"Alternatives sheet columns: {df.columns.tolist()}")
    logger.info(f"Alternatives sheet has {len(df)} rows")
    
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
            logger.info(f"Committed batch {batch_count} with {len(batch_records)} records")
        except Exception as e:
            # Roll back on error
            db.rollback()
            logger.error(f"Error committing batch {batch_count}: {e}")
            # Try inserting records one by one to identify problematic records
            for risk_stat in batch_records:
                try:
                    db.add(risk_stat)
                    db.commit()
                    logger.info(f"Individually added record for position: {risk_stat.position}")
                except Exception as inner_e:
                    db.rollback()
                    logger.error(f"Error adding individual record for position {risk_stat.position}: {inner_e}")
    
    logger.info(f"Processed {records_processed} alternatives risk statistics")
    return records_processed


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
        
        # Delete any existing records for today's date to prevent unique constraint violations
        try:
            existing_count = db.query(EgnyteRiskStat).filter(
                EgnyteRiskStat.import_date == import_date
            ).count()
            
            if existing_count > 0:
                logger.info(f"Deleting {existing_count} existing records for import date {import_date}")
                db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.import_date == import_date
                ).delete()
                db.commit()
        except Exception as e:
            logger.warning(f"Error deleting existing records: {str(e)}")
            # Continue with the process regardless
        
        # Download the file from Egnyte
        file_path = download_risk_stats_file()
        
        # Process the file and insert data into the database
        stats = process_excel_file(file_path, db)
        
        # Clean up the temporary file
        try:
            os.unlink(file_path)
            logger.info(f"Temporary file {file_path} removed")
        except Exception as e:
            logger.warning(f"Failed to remove temporary file {file_path}: {e}")
        
        return {
            "success": True,
            "import_date": import_date.isoformat(),
            "stats": stats
        }
    
    except Exception as e:
        logger.exception(f"Error in risk statistics processing: {str(e)}")
        return {
            "success": False,
            "error": str(e)
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