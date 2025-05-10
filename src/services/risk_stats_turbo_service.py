"""
High-performance service for processing risk statistics from Excel files.
Designed to achieve the 2-3 second target for processing ~20,000 records.

Key optimizations:
1. Parallel processing of asset classes
2. PostgreSQL COPY command for bulk data loading
3. Memory-optimized dataframe processing
4. Direct SQL operations instead of ORM for critical paths
"""

import os
import sys
import time
import logging
import traceback
import tempfile
import io
from datetime import date, datetime
from typing import Dict, Any, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session

from src.models.models import RiskStatisticEquity, RiskStatisticFixedIncome, RiskStatisticAlternatives
from src.services.egnyte_service import download_risk_stats_file

logger = logging.getLogger(__name__)

def process_risk_stats_turbo(
    db: Session, 
    use_test_file: bool = False, 
    batch_size: int = 1000,
    max_workers: int = 3,
    debug: bool = False
) -> Dict[str, Any]:
    """
    High-performance implementation for processing risk statistics.
    
    This implementation uses:
    - Parallel processing of asset classes
    - PostgreSQL COPY command for bulk inserts
    - Optimized memory usage
    - Direct SQL operations for critical paths
    
    Args:
        db: Database session
        use_test_file: Whether to use a test file instead of downloading
        batch_size: Size of batches for database operations
        max_workers: Number of parallel worker threads
        debug: Enable detailed debug logging
        
    Returns:
        Dict with processing results including timing and record counts
    """
    # 1. Set up tracing and performance metrics
    logger.info("Starting turbo risk stats processing")
    start_time = time.time()
    import_date = date.today()
    
    # Results object to track progress and counts
    results = {
        "success": False,
        "processing_time_seconds": 0,
        "total_records": 0,
        "equity_records": 0,
        "fixed_income_records": 0,
        "alternatives_records": 0,
        "error": None
    }
    
    try:
        # 2. Get the Excel file - either download or use test file
        excel_file = None
        file_path = None
        
        # Track file acquisition time
        file_start_time = time.time()
        
        if use_test_file:
            # Look for test file in standard locations
            test_locations = [
                "risk_stats_test.xlsx",
                "data/risk_stats_test.xlsx",
                "test_data/risk_stats_test.xlsx"
            ]
            
            for loc in test_locations:
                if os.path.exists(loc):
                    file_path = loc
                    logger.info(f"Using test file: {file_path}")
                    break
                    
            if not file_path:
                logger.warning("No test file found in standard locations")
                return {
                    "success": False,
                    "error": "Test file not found in standard locations"
                }
        else:
            # Download file from Egnyte
            try:
                egnyte_result = download_risk_stats_file()
                
                if not egnyte_result.get("success", False):
                    logger.error(f"Failed to download file from Egnyte: {egnyte_result.get('error')}")
                    return {
                        "success": False,
                        "error": f"Egnyte download failed: {egnyte_result.get('error')}"
                    }
                    
                file_path = egnyte_result.get("file_path")
                logger.info(f"Downloaded file from Egnyte: {file_path}")
            except Exception as download_error:
                logger.exception(f"Error downloading file: {download_error}")
                return {
                    "success": False,
                    "error": f"Download error: {str(download_error)}"
                }
                
        file_time = time.time() - file_start_time
        logger.info(f"File acquisition completed in {file_time:.2f} seconds")
        
        # 3. Read the Excel file into memory
        try:
            # Track Excel reading time
            excel_start_time = time.time()
            
            # Use low_memory mode for pandas to reduce memory usage
            xls = pd.ExcelFile(file_path, engine='openpyxl')
            
            # Check for required sheets
            required_sheets = ['Equity', 'Fixed Income', 'Alternatives']
            missing_sheets = [sheet for sheet in required_sheets if sheet not in xls.sheet_names]
            if missing_sheets:
                logger.error(f"Missing required sheets: {missing_sheets}")
                return {
                    "success": False,
                    "error": f"Missing required sheets: {', '.join(missing_sheets)}"
                }
                
            # Read each sheet with optimized settings
            equity_df = pd.read_excel(xls, 'Equity', skiprows=4)
            fixed_income_df = pd.read_excel(xls, 'Fixed Income', skiprows=4)
            alternatives_df = pd.read_excel(xls, 'Alternatives', skiprows=4)
            
            excel_time = time.time() - excel_start_time
            logger.info(f"Excel reading completed in {excel_time:.2f} seconds")
            
            # Close the ExcelFile to free memory
            xls.close()
        except Exception as excel_error:
            logger.exception(f"Error reading Excel file: {excel_error}")
            return {
                "success": False,
                "error": f"Excel processing error: {str(excel_error)}"
            }
            
        # 4. Process data in parallel using ThreadPoolExecutor
        processing_start_time = time.time()
        
        # Define processing functions for each asset class
        def process_equity():
            """Process equity sheet and return results"""
            try:
                record_count = 0
                # Clear existing data for this date
                clear_stmt = text(f"""
                DELETE FROM risk_statistic_equity 
                WHERE upload_date = :import_date
                """)
                db.execute(clear_stmt, {"import_date": import_date})
                db.commit()
                logger.info(f"Cleared existing equity records for {import_date}")
                
                # Map column names
                # Find the actual column names in the dataframe
                position_col = next((col for col in equity_df.columns if 'position' in str(col).lower()), None)
                ticker_col = next((col for col in equity_df.columns if any(term in str(col).lower() for term in ['ticker', 'symbol'])), None)
                cusip_col = next((col for col in equity_df.columns if 'cusip' in str(col).lower()), None)
                vol_col = next((col for col in equity_df.columns if any(term == str(col).lower() for term in ['vol', 'volatility'])), None)
                beta_col = next((col for col in equity_df.columns if 'beta' in str(col).lower()), None)
                
                if not position_col:
                    logger.error("Position column not found in Equity sheet")
                    return 0
                
                # Create a temporary CSV file for COPY operation
                with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as temp_file:
                    try:
                        # Write CSV data with proper formatting
                        for _, row in equity_df.iterrows():
                            # Skip rows without position
                            if position_col and pd.isna(row[position_col]):
                                continue
                                
                            # Clean and format values
                            position = str(row[position_col]).strip() if position_col and not pd.isna(row[position_col]) else ""
                            ticker = str(row[ticker_col]).strip() if ticker_col and not pd.isna(row[ticker_col]) else ""
                            cusip = str(row[cusip_col]).strip() if cusip_col and not pd.isna(row[cusip_col]) else ""
                            
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
                                    
                            # Write CSV line
                            if position or ticker or cusip:
                                # Handle escaping of special characters for CSV
                                position = position.replace('"', '""')
                                ticker = ticker.replace('"', '""')
                                cusip = cusip.replace('"', '""')
                                
                                # Use tabs as separators to avoid comma issues in text fields
                                temp_file.write(f"{import_date}\\t\"{position}\"\\t\"{ticker}\"\\t\"{cusip}\"\\t{vol}\\t{beta}\\n")
                                record_count += 1
                    
                        temp_file.flush()
                        
                        # Use PostgreSQL COPY command for efficient bulk loading
                        copy_stmt = text(f"""
                        COPY risk_statistic_equity (upload_date, position, ticker_symbol, cusip, vol, beta) 
                        FROM '{temp_file.name}' 
                        WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '"', ESCAPE '"');
                        """)
                        
                        # Execute the COPY command
                        db.execute(copy_stmt)
                        db.commit()
                        
                        return record_count
                            
                    except Exception as proc_error:
                        logger.exception(f"Error processing equity data: {proc_error}")
                        raise
                    finally:
                        # Clean up
                        if os.path.exists(temp_file.name):
                            try:
                                os.unlink(temp_file.name)
                            except:
                                pass
            except Exception as equity_error:
                logger.exception(f"Equity processing error: {equity_error}")
                return 0
                
        def process_fixed_income():
            """Process fixed income sheet and return results"""
            try:
                record_count = 0
                # Clear existing data for this date
                clear_stmt = text(f"""
                DELETE FROM risk_statistic_fixed_income 
                WHERE upload_date = :import_date
                """)
                db.execute(clear_stmt, {"import_date": import_date})
                db.commit()
                logger.info(f"Cleared existing fixed income records for {import_date}")
                
                # Map column names
                position_col = next((col for col in fixed_income_df.columns if 'position' in str(col).lower()), None)
                ticker_col = next((col for col in fixed_income_df.columns if any(term in str(col).lower() for term in ['ticker', 'symbol'])), None)
                cusip_col = next((col for col in fixed_income_df.columns if 'cusip' in str(col).lower()), None)
                duration_col = next((col for col in fixed_income_df.columns if 'duration' in str(col).lower()), None)
                
                if not position_col:
                    logger.error("Position column not found in Fixed Income sheet")
                    return 0
                
                # Create a temporary CSV file for COPY operation
                with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as temp_file:
                    try:
                        # Write CSV data with proper formatting
                        for _, row in fixed_income_df.iterrows():
                            # Skip rows without position
                            if position_col and pd.isna(row[position_col]):
                                continue
                                
                            # Clean and format values
                            position = str(row[position_col]).strip() if position_col and not pd.isna(row[position_col]) else ""
                            ticker = str(row[ticker_col]).strip() if ticker_col and not pd.isna(row[ticker_col]) else ""
                            cusip = str(row[cusip_col]).strip() if cusip_col and not pd.isna(row[cusip_col]) else ""
                            
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
                                    
                            # Write CSV line
                            if position or ticker or cusip:
                                # Handle escaping of special characters for CSV
                                position = position.replace('"', '""')
                                ticker = ticker.replace('"', '""')
                                cusip = cusip.replace('"', '""')
                                
                                # Use tabs as separators to avoid comma issues in text fields
                                temp_file.write(f"{import_date}\\t\"{position}\"\\t\"{ticker}\"\\t\"{cusip}\"\\t{duration}\\n")
                                record_count += 1
                    
                        temp_file.flush()
                        
                        # Use PostgreSQL COPY command for efficient bulk loading
                        copy_stmt = text(f"""
                        COPY risk_statistic_fixed_income (upload_date, position, ticker_symbol, cusip, duration) 
                        FROM '{temp_file.name}' 
                        WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '"', ESCAPE '"');
                        """)
                        
                        # Execute the COPY command
                        db.execute(copy_stmt)
                        db.commit()
                        
                        return record_count
                            
                    except Exception as proc_error:
                        logger.exception(f"Error processing fixed income data: {proc_error}")
                        raise
                    finally:
                        # Clean up
                        if os.path.exists(temp_file.name):
                            try:
                                os.unlink(temp_file.name)
                            except:
                                pass
            except Exception as fi_error:
                logger.exception(f"Fixed income processing error: {fi_error}")
                return 0
                
        def process_alternatives():
            """Process alternatives sheet and return results"""
            try:
                record_count = 0
                # Clear existing data for this date
                clear_stmt = text(f"""
                DELETE FROM risk_statistic_alternatives 
                WHERE upload_date = :import_date
                """)
                db.execute(clear_stmt, {"import_date": import_date})
                db.commit()
                logger.info(f"Cleared existing alternatives records for {import_date}")
                
                # Map column names
                position_col = next((col for col in alternatives_df.columns if 'position' in str(col).lower()), None)
                ticker_col = next((col for col in alternatives_df.columns if any(term in str(col).lower() for term in ['ticker', 'symbol'])), None)
                cusip_col = next((col for col in alternatives_df.columns if 'cusip' in str(col).lower()), None)
                beta_col = next((col for col in alternatives_df.columns if 'beta' in str(col).lower()), None)
                vol_col = next((col for col in alternatives_df.columns if any(term == str(col).lower() for term in ['vol', 'volatility'])), None)
                
                if not position_col:
                    logger.error("Position column not found in Alternatives sheet")
                    return 0
                
                # Create a temporary CSV file for COPY operation
                with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as temp_file:
                    try:
                        # Write CSV data with proper formatting
                        for _, row in alternatives_df.iterrows():
                            # Skip rows without position
                            if position_col and pd.isna(row[position_col]):
                                continue
                                
                            # Clean and format values
                            position = str(row[position_col]).strip() if position_col and not pd.isna(row[position_col]) else ""
                            ticker = str(row[ticker_col]).strip() if ticker_col and not pd.isna(row[ticker_col]) else ""
                            cusip = str(row[cusip_col]).strip() if cusip_col and not pd.isna(row[cusip_col]) else ""
                            
                            # Clean numeric values
                            beta = 0.0
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
                                    
                            # Write CSV line
                            if position or ticker or cusip:
                                # Handle escaping of special characters for CSV
                                position = position.replace('"', '""')
                                ticker = ticker.replace('"', '""')
                                cusip = cusip.replace('"', '""')
                                
                                # Use tabs as separators to avoid comma issues in text fields
                                temp_file.write(f"{import_date}\\t\"{position}\"\\t\"{ticker}\"\\t\"{cusip}\"\\t{beta}\\t{vol}\\n")
                                record_count += 1
                    
                        temp_file.flush()
                        
                        # Use PostgreSQL COPY command for efficient bulk loading
                        copy_stmt = text(f"""
                        COPY risk_statistic_alternatives (upload_date, position, ticker_symbol, cusip, beta, vol) 
                        FROM '{temp_file.name}' 
                        WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '"', ESCAPE '"');
                        """)
                        
                        # Execute the COPY command
                        db.execute(copy_stmt)
                        db.commit()
                        
                        return record_count
                            
                    except Exception as proc_error:
                        logger.exception(f"Error processing alternatives data: {proc_error}")
                        raise
                    finally:
                        # Clean up
                        if os.path.exists(temp_file.name):
                            try:
                                os.unlink(temp_file.name)
                            except:
                                pass
            except Exception as alt_error:
                logger.exception(f"Alternatives processing error: {alt_error}")
                return 0
                
        # Execute processing functions in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            equity_future = executor.submit(process_equity)
            fixed_income_future = executor.submit(process_fixed_income)
            alternatives_future = executor.submit(process_alternatives)
            
            # Collect results
            equity_count = equity_future.result()
            fixed_income_count = fixed_income_future.result()
            alternatives_count = alternatives_future.result()
            
            results["equity_records"] = equity_count
            results["fixed_income_records"] = fixed_income_count
            results["alternatives_records"] = alternatives_count
            results["total_records"] = equity_count + fixed_income_count + alternatives_count
            
        processing_time = time.time() - processing_start_time
        logger.info(f"Data processing completed in {processing_time:.2f} seconds")
        
        # Update materialized views or compute additional statistics if needed
        if results["total_records"] > 0:
            # Setup indexing - this can help with lookups
            index_start_time = time.time()
            
            # Create optimized indexes for better query performance
            index_statements = [
                """
                CREATE INDEX IF NOT EXISTS idx_risk_statistic_equity_ticker_date
                ON risk_statistic_equity (ticker_symbol, upload_date);
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_risk_statistic_equity_cusip_date
                ON risk_statistic_equity (cusip, upload_date);
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_risk_statistic_fixed_income_ticker_date
                ON risk_statistic_fixed_income (ticker_symbol, upload_date);
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_risk_statistic_fixed_income_cusip_date
                ON risk_statistic_fixed_income (cusip, upload_date);
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_risk_statistic_alternatives_ticker_date
                ON risk_statistic_alternatives (ticker_symbol, upload_date);
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_risk_statistic_alternatives_cusip_date
                ON risk_statistic_alternatives (cusip, upload_date);
                """
            ]
            
            for stmt in index_statements:
                try:
                    db.execute(text(stmt))
                except Exception as idx_error:
                    logger.warning(f"Error creating index: {idx_error}")
            
            db.commit()
            index_time = time.time() - index_start_time
            logger.info(f"Index optimization completed in {index_time:.2f} seconds")
            
        # Update results and timing
        total_time = time.time() - start_time
        results["processing_time_seconds"] = total_time
        results["success"] = True
        
        logger.info(f"Turbo risk stats processing completed in {total_time:.2f} seconds")
        logger.info(f"Processed {results['total_records']} records total")
        logger.info(f"  Equity: {results['equity_records']}")
        logger.info(f"  Fixed Income: {results['fixed_income_records']}")
        logger.info(f"  Alternatives: {results['alternatives_records']}")
        
        return results
        
    except Exception as e:
        logger.exception(f"Error during turbo risk stats processing: {e}")
        error_time = time.time() - start_time
        
        return {
            "success": False,
            "error": str(e),
            "processing_time_seconds": error_time,
            "traceback": traceback.format_exc()
        }
    finally:
        # Clean up any temporary files
        if file_path and use_test_file == False and os.path.exists(file_path):
            try:
                os.unlink(file_path)
                logger.info(f"Removed temporary file: {file_path}")
            except Exception as del_error:
                logger.warning(f"Could not delete temporary file {file_path}: {del_error}")