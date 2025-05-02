#!/usr/bin/env python3
"""
Standalone script for processing data dump files
This avoids the web server timeout issues by running as a separate process
"""
import os
import sys
import pandas as pd
import datetime
import argparse
import time
import logging
import re
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_dump_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Connect to database using environment variable
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable not set")
    sys.exit(1)

# Create engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

def clean_numeric_value(value):
    """Clean and convert a value to float"""
    if pd.isna(value) or value is None:
        return 0.0
            
    if isinstance(value, (int, float)):
        return float(value)
            
    # Remove non-numeric characters except decimal point and minus sign
    clean_value = str(value).replace('$', '').replace(',', '').strip()
    
    # Handle empty strings
    if not clean_value:
        return 0.0
            
    try:
        return float(clean_value)
    except ValueError:
        # For values that can't be converted to float
        return 0.0

def simple_encrypt(value):
    """
    Simple encryption replacement to avoid issues with special characters
    This is just to demonstrate the process - in production use proper encryption
    """
    if value is None:
        return None
    
    # Convert to string and handle encoding
    try:
        if isinstance(value, float):
            value_str = f"{value:.2f}"
        else:
            value_str = str(value)
        
        # For now, just store as a string with a simple prefix
        # In production, use proper encryption
        return f"ENC:{value_str}"
    except:
        logger.error(f"Error encrypting value: {type(value)}")
        return "ENC:0.00"

def process_excel_file(file_path):
    """Process an Excel file with financial position data"""
    logger.info(f"Processing Excel file: {file_path}")
    
    report_date = datetime.date.today()
    view_name = "DATA DUMP"
    
    try:
        # Extract metadata from first 3 rows
        metadata_df = pd.read_excel(file_path, nrows=3, header=None, engine='openpyxl')
        
        # Extract view name
        if len(metadata_df) > 0 and len(metadata_df.columns) > 1 and not pd.isna(metadata_df.iloc[0, 1]):
            view_name = str(metadata_df.iloc[0, 1])
        
        # Parse date range
        if len(metadata_df) > 1 and len(metadata_df.columns) > 1 and not pd.isna(metadata_df.iloc[1, 1]):
            date_range_str = str(metadata_df.iloc[1, 1])
            # Try multiple date formats and patterns
            date_patterns = [
                r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})',  # MM-DD-YYYY to MM-DD-YYYY
                r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD to YYYY-MM-DD
                r'(\w+ \d{1,2}, \d{4})\s+to\s+(\w+ \d{1,2}, \d{4})'  # Month DD, YYYY to Month DD, YYYY
            ]
            
            # Try each pattern
            for pattern in date_patterns:
                match = re.search(pattern, date_range_str)
                if match:
                    try:
                        # We use the end date for reporting
                        if pattern == r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})':
                            _, end_date_str = match.groups()
                            report_date = datetime.datetime.strptime(end_date_str, '%m-%d-%Y').date()
                            break
                        elif pattern == r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})':
                            _, end_date_str = match.groups()
                            report_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
                            break
                        elif pattern == r'(\w+ \d{1,2}, \d{4})\s+to\s+(\w+ \d{1,2}, \d{4})':
                            _, end_date_str = match.groups()
                            report_date = datetime.datetime.strptime(end_date_str, '%B %d, %Y').date()
                            break
                    except ValueError:
                        continue  # Try next pattern if this one fails
        
        # Read actual data (skip metadata rows)
        logger.info("Reading data rows from Excel file")
        df = pd.read_excel(
            file_path, 
            header=3,  # Header is in row 4 (0-indexed)
            engine='openpyxl',
            dtype=str  # Use string for all columns initially
        )
        
        logger.info(f"Excel file parsed, rows: {len(df)}")
        
        # Initialize counters
        rows_processed = 0
        rows_inserted = 0
        errors = []
        
        # Clean up the database
        db = SessionLocal()
        
        try:
            # Delete existing positions for this report date
            logger.info(f"Deleting existing positions for date: {report_date}")
            db.execute(text(
                "DELETE FROM financial_positions WHERE date = :report_date"
            ), {"report_date": report_date})
            db.commit()
        except Exception as e:
            logger.error(f"Error deleting existing positions: {str(e)}")
            db.rollback()
        
        # Process the rows in batches
        total_rows = len(df)
        BATCH_SIZE = 2000
        
        # Start batch processing
        for i in range(0, total_rows, BATCH_SIZE):
            batch_df = df.iloc[i:i+BATCH_SIZE]
            batch_processed = 0
            batch_inserted = 0
            batch_errors = []
            
            # Prepare SQL for batch insert
            sql_values = []
            sql_params = {}
            
            for _, row in batch_df.iterrows():
                try:
                    batch_processed += 1
                    
                    # Map column names properly - handle different formats
                    position = row.get('Position', row.get('position', ''))
                    top_level_client = row.get('Top Level Client', row.get('top_level_client', ''))
                    holding_account = row.get('Holding Account', row.get('holding_account', ''))
                    holding_account_number = row.get('Holding Account Number', row.get('holding_account_number', ''))
                    portfolio = row.get('Portfolio', row.get('portfolio', ''))
                    cusip = row.get('CUSIP', row.get('cusip', ''))
                    ticker_symbol = row.get('Ticker Symbol', row.get('ticker_symbol', ''))
                    asset_class = row.get('Asset Class', row.get('asset_class', ''))
                    second_level = row.get('New Second Level', row.get('second_level', ''))
                    third_level = row.get('New Third Level', row.get('third_level', ''))
                    adv_classification = row.get('ADV Classification', row.get('adv_classification', ''))
                    liquid_vs_illiquid = row.get('Liquid vs. Illiquid Asset', row.get('liquid_vs_illiquid', ''))
                    adjusted_value = row.get('Adjusted Value (USD)', row.get('adjusted_value', 0))
                    
                    # Handle required fields
                    if not position or not top_level_client or not holding_account:
                        raise ValueError("Missing required fields: position, top_level_client, or holding_account")
                    
                    # Clean and encrypt the adjusted value
                    adjusted_value_clean = clean_numeric_value(adjusted_value)
                    encrypted_value = simple_encrypt(adjusted_value_clean)
                    
                    # Add a unique suffix for parameter names to avoid conflicts
                    suffix = f"_{batch_processed + i}"
                    
                    # Add values to parameters dict
                    sql_params[f"position{suffix}"] = position
                    sql_params[f"top_level_client{suffix}"] = top_level_client
                    sql_params[f"holding_account{suffix}"] = holding_account
                    sql_params[f"holding_account_number{suffix}"] = str(holding_account_number) if holding_account_number else "-"
                    sql_params[f"portfolio{suffix}"] = str(portfolio) if portfolio else "-"
                    sql_params[f"cusip{suffix}"] = str(cusip) if cusip else ""
                    sql_params[f"ticker_symbol{suffix}"] = str(ticker_symbol) if ticker_symbol else ""
                    sql_params[f"asset_class{suffix}"] = str(asset_class) if asset_class else "Other"
                    sql_params[f"second_level{suffix}"] = str(second_level) if second_level else ""
                    sql_params[f"third_level{suffix}"] = str(third_level) if third_level else ""
                    sql_params[f"adv_classification{suffix}"] = str(adv_classification) if adv_classification else ""
                    sql_params[f"liquid_vs_illiquid{suffix}"] = str(liquid_vs_illiquid) if liquid_vs_illiquid else "Liquid"
                    sql_params[f"adjusted_value{suffix}"] = encrypted_value
                    sql_params[f"date{suffix}"] = report_date
                    
                    # Add value tuple to SQL values list
                    sql_values.append(
                        f"(:position{suffix}, :top_level_client{suffix}, :holding_account{suffix}, "
                        f":holding_account_number{suffix}, :portfolio{suffix}, :cusip{suffix}, "
                        f":ticker_symbol{suffix}, :asset_class{suffix}, :second_level{suffix}, "
                        f":third_level{suffix}, :adv_classification{suffix}, :liquid_vs_illiquid{suffix}, "
                        f":adjusted_value{suffix}, :date{suffix}, NOW())"
                    )
                    
                    # Count successful row
                    batch_inserted += 1
                
                except Exception as e:
                    error_msg = f"Error processing row {batch_processed + i}: {str(e)}"
                    batch_errors.append(error_msg)
                    logger.error(error_msg)
            
            # Execute the batch insert
            if sql_values:
                try:
                    sql = f"""
                    INSERT INTO financial_positions 
                    (position, top_level_client, holding_account, holding_account_number, 
                     portfolio, cusip, ticker_symbol, asset_class, second_level, third_level, 
                     adv_classification, liquid_vs_illiquid, adjusted_value, date, upload_date)
                    VALUES {', '.join(sql_values)}
                    """
                    db.execute(text(sql), sql_params)
                    db.commit()
                    
                    # Update counters
                    rows_processed += batch_processed
                    rows_inserted += batch_inserted
                    errors.extend(batch_errors)
                    
                    logger.info(f"Batch processed: {i}-{i+len(batch_df)} of {total_rows}, inserted: {batch_inserted}")
                except Exception as e:
                    db.rollback()
                    error_msg = f"Error in batch {i}-{i+len(batch_df)}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    logger.error(traceback.format_exc())
                    
                    # Try to insert individually as fallback
                    logger.info("Attempting individual inserts as fallback")
                    for j, row in batch_df.iterrows():
                        try:
                            position = row.get('Position', row.get('position', ''))
                            top_level_client = row.get('Top Level Client', row.get('top_level_client', ''))
                            holding_account = row.get('Holding Account', row.get('holding_account', ''))
                            holding_account_number = row.get('Holding Account Number', row.get('holding_account_number', ''))
                            portfolio = row.get('Portfolio', row.get('portfolio', ''))
                            cusip = row.get('CUSIP', row.get('cusip', ''))
                            ticker_symbol = row.get('Ticker Symbol', row.get('ticker_symbol', ''))
                            asset_class = row.get('Asset Class', row.get('asset_class', ''))
                            second_level = row.get('New Second Level', row.get('second_level', ''))
                            third_level = row.get('New Third Level', row.get('third_level', ''))
                            adv_classification = row.get('ADV Classification', row.get('adv_classification', ''))
                            liquid_vs_illiquid = row.get('Liquid vs. Illiquid Asset', row.get('liquid_vs_illiquid', ''))
                            adjusted_value = row.get('Adjusted Value (USD)', row.get('adjusted_value', 0))
                            
                            # Skip rows with missing required fields
                            if not position or not top_level_client or not holding_account:
                                continue
                                
                            # Clean and encrypt the adjusted value
                            adjusted_value_clean = clean_numeric_value(adjusted_value)
                            encrypted_value = simple_encrypt(adjusted_value_clean)
                            
                            # Use direct SQL for more control
                            sql = """
                            INSERT INTO financial_positions 
                            (position, top_level_client, holding_account, holding_account_number, 
                             portfolio, cusip, ticker_symbol, asset_class, second_level, third_level, 
                             adv_classification, liquid_vs_illiquid, adjusted_value, date, upload_date)
                            VALUES (:position, :top_level_client, :holding_account, :holding_account_number,
                                    :portfolio, :cusip, :ticker_symbol, :asset_class, :second_level, :third_level,
                                    :adv_classification, :liquid_vs_illiquid, :adjusted_value, :date, NOW())
                            """
                            
                            db.execute(text(sql), {
                                "position": position,
                                "top_level_client": top_level_client,
                                "holding_account": holding_account,
                                "holding_account_number": str(holding_account_number) if holding_account_number else "-",
                                "portfolio": str(portfolio) if portfolio else "-",
                                "cusip": str(cusip) if cusip else "",
                                "ticker_symbol": str(ticker_symbol) if ticker_symbol else "",
                                "asset_class": str(asset_class) if asset_class else "Other",
                                "second_level": str(second_level) if second_level else "",
                                "third_level": str(third_level) if third_level else "",
                                "adv_classification": str(adv_classification) if adv_classification else "",
                                "liquid_vs_illiquid": str(liquid_vs_illiquid) if liquid_vs_illiquid else "Liquid",
                                "adjusted_value": encrypted_value,
                                "date": report_date
                            })
                            db.commit()
                            rows_inserted += 1
                            
                        except Exception as e:
                            db.rollback()
                            error_msg = f"Error in individual insert row {i + j}: {str(e)}"
                            errors.append(error_msg)
                            logger.error(error_msg)
        
        # Generate financial summary
        try:
            logger.info("Generating financial summary")
            
            # This would normally call generate_financial_summary, but for now just log
            logger.info("Financial summary generation - placeholder")
            
            # Mark processing as complete
            status_file = os.path.join(os.path.dirname(file_path), "processing_complete.txt")
            with open(status_file, "w") as f:
                f.write(f"Processing completed at {datetime.datetime.now()}\n")
                f.write(f"Rows processed: {rows_processed}\n")
                f.write(f"Rows inserted: {rows_inserted}\n")
                f.write(f"Errors: {len(errors)}\n")
                
            return {
                "success": True,
                "message": f"Successfully processed {rows_inserted} positions for {report_date}",
                "rows_processed": rows_processed,
                "rows_inserted": rows_inserted,
                "errors": errors[:10] if errors else []
            }
                
        except Exception as e:
            error_msg = f"Error generating financial summary: {str(e)}"
            errors.append(error_msg)
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            
            return {
                "success": True,
                "message": f"Data imported but summary generation failed: {str(e)}",
                "rows_processed": rows_processed,
                "rows_inserted": rows_inserted,
                "errors": errors[:10] if errors else []
            }
            
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "message": f"Error processing file: {str(e)}",
            "rows_processed": 0,
            "rows_inserted": 0,
            "errors": [str(e)]
        }

def main():
    parser = argparse.ArgumentParser(description="Process a data dump file")
    parser.add_argument("file_path", help="Path to the Excel file to process")
    args = parser.parse_args()
    
    if not os.path.exists(args.file_path):
        logger.error(f"File not found: {args.file_path}")
        return 1
    
    start_time = time.time()
    result = process_excel_file(args.file_path)
    end_time = time.time()
    
    processing_time = end_time - start_time
    
    if result["success"]:
        logger.info(f"Processing completed in {processing_time:.2f} seconds")
        logger.info(f"Rows processed: {result['rows_processed']}")
        logger.info(f"Rows inserted: {result['rows_inserted']}")
        if result["errors"]:
            logger.warning(f"Errors occurred: {len(result['errors'])}")
            for error in result["errors"][:10]:
                logger.warning(f"  {error}")
        return 0
    else:
        logger.error(f"Processing failed: {result['message']}")
        return 1

if __name__ == "__main__":
    sys.exit(main())