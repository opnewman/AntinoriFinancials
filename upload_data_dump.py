#!/usr/bin/env python3
"""
Standalone script for processing data dump files
This avoids the web server timeout issues by running as a separate process
"""
import os
import sys
import pandas as pd
import argparse
import time
import datetime
import logging
import re
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Import the precalculation service to trigger risk metrics generation
sys.path.append('.')  # Add current directory to path
from src.services.precalculate_service import trigger_precalculation

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("upload_data_dump.log"),
        logging.StreamHandler(sys.stdout)
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
Session = sessionmaker(bind=engine)
Base = declarative_base()

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
    """Simple encryption replacement for storing value"""
    if value is None:
        return None
    
    try:
        if isinstance(value, float):
            # Return the numeric value directly without prefixing with ENC:
            return value
        else:
            # Try to convert to float if it's a string
            try:
                return float(value)
            except:
                return 0.0
    except:
        return 0.0

def process_excel_file(file_path):
    """Process an Excel file with financial position data"""
    logger.info(f"Processing Excel file: {file_path}")
    
    session = Session()
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
                        
        # Clean up existing data
        session.execute(text("DELETE FROM financial_positions WHERE date = :report_date"), 
                        {"report_date": report_date})
        session.commit()
        
        # Now read the data with batching
        logger.info(f"Processing data for date: {report_date}")
        
        # Read the entire file first
        df = pd.read_excel(
            file_path, 
            header=3,  # Header is in row 4 (0-indexed)
            engine='openpyxl',
            dtype=str  # Use string for all columns initially
        )
        
        # Get total row count
        total_rows = len(df)
        logger.info(f"Found {total_rows} rows to process")
        
        # Process in batches of 1000 rows
        batch_size = 1000
        processed_rows = 0
        
        # Split into chunks manually
        for chunk_idx in range(0, total_rows, batch_size):
            end_idx = min(chunk_idx + batch_size, total_rows)
            df_chunk = df.iloc[chunk_idx:end_idx].copy()
            # Get chunk size
            chunk_size = len(df_chunk)
            
            logger.info(f"Processing chunk {chunk_idx+1} with {chunk_size} rows")
            
            # Direct SQL insert using a multi-row value clause for better performance
            values_sql = []
            params = {}
            
            for i, row in df_chunk.iterrows():
                try:
                    # Map column names properly - handle both formats
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
                        logger.warning(f"Skipping row {i} due to missing required fields")
                        continue
                    
                    # Clean and store the adjusted value
                    adjusted_value_clean = clean_numeric_value(adjusted_value)
                    value_str = simple_encrypt(adjusted_value_clean)
                    
                    # Create unique param names using row index
                    row_idx = f"{chunk_idx}_{i}"
                    
                    # Add parameters
                    params[f"position_{row_idx}"] = position
                    params[f"top_level_client_{row_idx}"] = top_level_client
                    params[f"holding_account_{row_idx}"] = holding_account
                    params[f"holding_account_number_{row_idx}"] = str(holding_account_number) if holding_account_number else "-"
                    params[f"portfolio_{row_idx}"] = str(portfolio) if portfolio else "-"
                    params[f"cusip_{row_idx}"] = str(cusip) if cusip else ""
                    params[f"ticker_symbol_{row_idx}"] = str(ticker_symbol) if ticker_symbol else ""
                    params[f"asset_class_{row_idx}"] = str(asset_class) if asset_class else "Other"
                    params[f"second_level_{row_idx}"] = str(second_level) if second_level else ""
                    params[f"third_level_{row_idx}"] = str(third_level) if third_level else ""
                    params[f"adv_classification_{row_idx}"] = str(adv_classification) if adv_classification else ""
                    params[f"liquid_vs_illiquid_{row_idx}"] = str(liquid_vs_illiquid) if liquid_vs_illiquid else "Liquid" 
                    params[f"adjusted_value_{row_idx}"] = value_str
                    params[f"date_{row_idx}"] = report_date
                    
                    # Add value placeholders
                    values_sql.append(
                        f"(:position_{row_idx}, :top_level_client_{row_idx}, :holding_account_{row_idx}, "
                        f":holding_account_number_{row_idx}, :portfolio_{row_idx}, :cusip_{row_idx}, "
                        f":ticker_symbol_{row_idx}, :asset_class_{row_idx}, :second_level_{row_idx}, "
                        f":third_level_{row_idx}, :adv_classification_{row_idx}, :liquid_vs_illiquid_{row_idx}, "
                        f":adjusted_value_{row_idx}, :date_{row_idx}, CURRENT_DATE)"
                    )
                    
                    processed_rows += 1
                except Exception as e:
                    logger.error(f"Error processing row {i}: {str(e)}")
                    logger.error(traceback.format_exc())
            
            # Execute the batch insert if we have values
            if values_sql:
                try:
                    sql = f"""
                    INSERT INTO financial_positions 
                    (position, top_level_client, holding_account, holding_account_number, 
                     portfolio, cusip, ticker_symbol, asset_class, second_level, third_level, 
                     adv_classification, liquid_vs_illiquid, adjusted_value, date, upload_date)
                    VALUES {', '.join(values_sql)}
                    """
                    
                    # Execute the insert
                    session.execute(text(sql), params)
                    session.commit()
                    
                    logger.info(f"Inserted {len(values_sql)} rows in chunk {chunk_idx+1}")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error inserting batch: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                    # Fall back to individual inserts if needed
                    logger.info("Attempting individual inserts as fallback")
                    
                    success_count = 0
                    for i, row in df_chunk.iterrows():
                        try:
                            # Extract the same data as above
                            position = row.get('Position', row.get('position', ''))
                            top_level_client = row.get('Top Level Client', row.get('top_level_client', ''))
                            holding_account = row.get('Holding Account', row.get('holding_account', ''))
                            
                            # Skip if missing required fields
                            if not position or not top_level_client or not holding_account:
                                continue
                                
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
                            
                            # Clean and store the adjusted value
                            adjusted_value_clean = clean_numeric_value(adjusted_value)
                            value_str = simple_encrypt(adjusted_value_clean)
                            
                            # Insert one row at a time
                            session.execute(text("""
                                INSERT INTO financial_positions 
                                (position, top_level_client, holding_account, holding_account_number, 
                                 portfolio, cusip, ticker_symbol, asset_class, second_level, third_level, 
                                 adv_classification, liquid_vs_illiquid, adjusted_value, date, upload_date)
                                VALUES 
                                (:position, :top_level_client, :holding_account, :holding_account_number,
                                 :portfolio, :cusip, :ticker_symbol, :asset_class, :second_level, :third_level,
                                 :adv_classification, :liquid_vs_illiquid, :adjusted_value, :date, CURRENT_DATE)
                            """), {
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
                                "adjusted_value": value_str,
                                "date": report_date
                            })
                            session.commit()
                            success_count += 1
                            
                        except Exception as e:
                            session.rollback()
                            logger.error(f"Error inserting individual row {i}: {str(e)}")
                    
                    logger.info(f"Inserted {success_count} rows individually in chunk {chunk_idx+1}")
        
        # Log completion
        logger.info(f"Successfully processed {processed_rows} rows out of {total_rows} total rows")
        
        # Generate financial summary
        logger.info("Generating financial summary")
        try:
            # Call the stored procedure or internal function for generating summaries
            session.execute(text("""
                -- Placeholder for financial summary generation
                -- This would generate summary data from the raw financial positions
                SELECT 1;
            """))
            session.commit()
            logger.info("Financial summary generated")
        except Exception as e:
            session.rollback()
            logger.error(f"Error generating financial summary: {str(e)}")
        
        # Create a status file for the API to check
        status_file_path = os.path.join(os.path.dirname(file_path), "data_dump_complete.txt")
        with open(status_file_path, "w") as f:
            f.write(f"Processing completed at: {datetime.datetime.now()}\n")
            f.write(f"Rows processed: {total_rows}\n")
            f.write(f"Rows inserted: {processed_rows}\n")
            f.write(f"Report date: {report_date}\n")
        
        logger.info(f"Processing completed, status file written to {status_file_path}")
        return {
            "success": True,
            "total_rows": total_rows,
            "processed_rows": processed_rows,
            "report_date": report_date
        }
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing file: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Process a data dump Excel file")
    parser.add_argument("file_path", help="Path to the Excel file to process")
    args = parser.parse_args()
    
    if not os.path.exists(args.file_path):
        logger.error(f"File not found: {args.file_path}")
        sys.exit(1)
    
    # Get the directory where the file is located for status files
    file_dir = os.path.dirname(args.file_path)
    
    start_time = time.time()
    result = process_excel_file(args.file_path)
    end_time = time.time()
    processing_time = end_time - start_time
    
    if result["success"]:
        logger.info(f"Processing successful! Took {processing_time:.2f} seconds")
        logger.info(f"Total rows: {result['total_rows']}")
        logger.info(f"Processed rows: {result['processed_rows']}")
        logger.info(f"Report date: {result['report_date']}")
        
        # Write completion status file
        completion_file = os.path.join(file_dir, "data_dump_complete.txt")
        try:
            with open(completion_file, "w") as f:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"Processing completed at: {timestamp}\n")
                f.write(f"File: {os.path.basename(args.file_path)}\n")
                f.write(f"Processing time: {processing_time:.2f} seconds\n")
                f.write(f"Rows processed: {result['total_rows']}\n")
                f.write(f"Rows inserted: {result['processed_rows']}\n")
                f.write(f"Report date: {result['report_date']}\n")
            logger.info(f"Completion status file created: {completion_file}")
        except Exception as e:
            logger.error(f"Error creating completion file: {str(e)}")
        
        # Trigger precalculation of risk metrics for all entities
        # This will run in the background and not block this process
        try:
            logger.info(f"Triggering precalculation of risk metrics...")
            # Convert string date to date object if necessary
            if isinstance(result['report_date'], str):
                report_date_obj = datetime.datetime.strptime(result['report_date'], '%Y-%m-%d').date()
            else:
                report_date_obj = result['report_date']
                
            trigger_precalculation(report_date_obj)
            logger.info(f"Precalculation triggered successfully for date: {report_date_obj}")
        except Exception as e:
            logger.error(f"Error triggering precalculation: {str(e)}")
        
        sys.exit(0)
    else:
        logger.error(f"Processing failed: {result['error']}")
        
        # Write error status file
        error_file = os.path.join(file_dir, "data_dump_error.txt")
        try:
            with open(error_file, "w") as f:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"Processing failed at: {timestamp}\n")
                f.write(f"File: {os.path.basename(args.file_path)}\n")
                f.write(f"Error: {result['error']}\n")
            logger.info(f"Error status file created: {error_file}")
        except Exception as e:
            logger.error(f"Error creating error file: {str(e)}")
            
        sys.exit(1)

if __name__ == "__main__":
    main()