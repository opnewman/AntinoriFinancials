"""
Optimized service for processing risk statistics data from Excel files.
Designed for high-performance batch operations with minimal database interactions.
"""

import os
import gc
import sys
import time
import logging
import tempfile
import traceback
import concurrent.futures
from datetime import date
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.models.models import EgnyteRiskStat
from src.services.egnyte_service import download_risk_stats_file


logger = logging.getLogger(__name__)


def process_risk_stats_optimized(
    db: Session, 
    use_test_file: bool = False, 
    batch_size: int = 1000,
    max_workers: int = 3
) -> Dict[str, Any]:
    """
    Highly optimized function to process risk statistics from Egnyte.
    
    Key optimizations:
    1. Parallel processing of Excel sheets
    2. Direct bulk UPSERT operations
    3. Memory-efficient chunked processing
    4. Minimal database roundtrips
    
    Args:
        db: Database session
        use_test_file: Whether to use a test file instead of downloading
        batch_size: Size of batches for database operations
        max_workers: Maximum number of parallel workers
        
    Returns:
        Dict with processing results including timing and record counts
    """
    start_time = time.time()
    logger.info(f"Starting high-performance risk stats processing with batch_size={batch_size}, max_workers={max_workers}")
    
    results = {
        "success": True,
        "equity_records": 0,
        "fixed_income_records": 0,
        "alternatives_records": 0,
        "total_records": 0,
        "processing_time_seconds": 0,
        "error": None
    }
    
    try:
        # 1. Download the risk stats file (or use test file)
        try:
            file_path = download_risk_stats_file(use_test_file=use_test_file)
            if not file_path or not os.path.exists(file_path):
                return {
                    "success": False,
                    "error": "Failed to download risk statistics file",
                    "processing_time_seconds": time.time() - start_time
                }
                
            file_size = os.path.getsize(file_path)
            logger.info(f"Successfully downloaded file ({file_size/1024/1024:.2f} MB)")
        except Exception as download_error:
            error_msg = str(download_error)
            logger.error(f"Download error: {error_msg}")
            return {
                "success": False,
                "error": f"Download error: {error_msg}",
                "processing_time_seconds": time.time() - start_time
            }
        
        # 2. Clean existing records in a single efficient transaction
        import_date = date.today()
        try:
            # Use a direct SQL DELETE with a single commit
            sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :date")
            result = db.execute(sql, {"date": import_date})
            db.commit()
            logger.info(f"Cleared existing risk stat records for {import_date}")
        except Exception as cleanup_error:
            logger.error(f"Error cleaning up existing records: {cleanup_error}")
            db.rollback()
            return {
                "success": False,
                "error": f"Database cleanup failed: {cleanup_error}",
                "processing_time_seconds": time.time() - start_time
            }
        
        # 3. Analyze Excel file and identify relevant sheets
        try:
            with pd.ExcelFile(file_path) as xl:
                sheet_names = xl.sheet_names
                logger.info(f"Found sheets: {sheet_names}")
                
                # Identify target sheets using flexible matching
                equity_sheet = next((s for s in sheet_names if "equity" in s.lower()), None)
                fixed_income_sheet = next((s for s in sheet_names if ("fixed" in s.lower() or "fi" in s.lower()) and "duration" not in s.lower()), None)
                alternatives_sheet = next((s for s in sheet_names if "alt" in s.lower()), None)
                
                logger.info(f"Identified - Equity: {equity_sheet}, Fixed Income: {fixed_income_sheet}, Alternatives: {alternatives_sheet}")
                
                # 4. Process each sheet in parallel for maximum performance
                processing_results = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    
                    # Submit all sheets for processing
                    if equity_sheet:
                        futures.append(
                            executor.submit(
                                parallel_process_sheet, 
                                file_path=file_path,
                                sheet_name=equity_sheet,
                                asset_class="Equity",
                                import_date=import_date,
                                batch_size=batch_size
                            )
                        )
                    
                    if fixed_income_sheet:
                        futures.append(
                            executor.submit(
                                parallel_process_sheet, 
                                file_path=file_path,
                                sheet_name=fixed_income_sheet,
                                asset_class="Fixed Income",
                                import_date=import_date,
                                batch_size=batch_size
                            )
                        )
                    
                    if alternatives_sheet:
                        futures.append(
                            executor.submit(
                                parallel_process_sheet, 
                                file_path=file_path,
                                sheet_name=alternatives_sheet,
                                asset_class="Alternatives",
                                import_date=import_date,
                                batch_size=batch_size
                            )
                        )
                    
                    # Collect results as they complete
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            result = future.result()
                            processing_results.append(result)
                            logger.info(f"Completed processing {result['asset_class']} sheet with {result['record_count']} records")
                        except Exception as future_error:
                            logger.error(f"Error processing sheet: {future_error}")
                
                # 5. Bulk insert all records in a single optimized transaction
                try:
                    all_records = []
                    for result in processing_results:
                        if result['success'] and result['records']:
                            all_records.extend(result['records'])
                            
                            # Update result metrics
                            asset_class = result['asset_class']
                            if asset_class == "Equity":
                                results["equity_records"] = result['record_count']
                            elif asset_class == "Fixed Income":
                                results["fixed_income_records"] = result['record_count']
                            elif asset_class == "Alternatives":
                                results["alternatives_records"] = result['record_count']
                    
                    # Execute the bulk insert with UPSERT logic
                    if all_records:
                        # First, deduplicate records to avoid the CardinalityViolation error
                        # Create a dictionary with (import_date, position, asset_class) as key
                        unique_records = {}
                        for record in all_records:
                            # Create a unique key for each record
                            key = (record['import_date'], record['position'], record['asset_class'])
                            # If duplicate found, keep the latest one
                            unique_records[key] = record
                        
                        # Convert back to list
                        deduplicated_records = list(unique_records.values())
                        record_count = len(deduplicated_records)
                        
                        logger.info(f"Preparing to insert {record_count} unique records (removed {len(all_records) - record_count} duplicates)")
                        
                        # Process in appropriately sized batches
                        for i in range(0, record_count, batch_size):
                            batch = deduplicated_records[i:i+batch_size]
                            batch_size_mb = sum(sys.getsizeof(r) for r in batch) / (1024 * 1024)
                            logger.info(f"Inserting batch {i//batch_size + 1} with {len(batch)} records (~{batch_size_mb:.2f} MB)")
                            
                            # Using PostgreSQL-specific UPSERT for maximum efficiency
                            stmt = insert(EgnyteRiskStat.__table__)
                            # ON CONFLICT DO UPDATE clause for handling duplicates
                            stmt = stmt.on_conflict_do_update(
                                index_elements=['import_date', 'position', 'asset_class'],
                                set_={
                                    'ticker_symbol': stmt.excluded.ticker_symbol,
                                    'cusip': stmt.excluded.cusip,
                                    'second_level': stmt.excluded.second_level,
                                    'volatility': stmt.excluded.volatility,
                                    'beta': stmt.excluded.beta,
                                    'duration': stmt.excluded.duration,
                                    'notes': stmt.excluded.notes,
                                    'amended_id': stmt.excluded.amended_id,
                                    'updated_at': text('NOW()')
                                }
                            )
                            
                            # Execute with a single transaction
                            db.execute(stmt, batch)
                        
                        # Commit once after all batches
                        db.commit()
                        
                        results["total_records"] = record_count
                        logger.info(f"Successfully inserted all {record_count} records")
                    else:
                        logger.warning("No valid records found to insert")
                        
                except Exception as insert_error:
                    logger.error(f"Error during bulk insert: {insert_error}")
                    db.rollback()
                    results["success"] = False
                    results["error"] = f"Database insert failed: {str(insert_error)}"
        
        except Exception as process_error:
            logger.error(f"Error during processing: {process_error}")
            results["success"] = False
            results["error"] = f"Processing failed: {str(process_error)}"
        
        # 6. Final cleanup
        try:
            # Clean up temporary file
            if file_path and os.path.exists(file_path) and not use_test_file:
                os.unlink(file_path)
                logger.info(f"Removed temporary file: {file_path}")
                
            # Force garbage collection
            gc.collect()
        except Exception as cleanup_error:
            logger.warning(f"Cleanup error (non-fatal): {cleanup_error}")
        
        # 7. Calculate final timing
        end_time = time.time()
        processing_time = end_time - start_time
        results["processing_time_seconds"] = processing_time
        
        logger.info(f"Completed risk stats processing in {processing_time:.2f} seconds")
        logger.info(f"Processed {results['total_records']} records: "
                 f"Equity={results['equity_records']}, "
                 f"Fixed Income={results['fixed_income_records']}, "
                 f"Alternatives={results['alternatives_records']}")
        
        return results
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "traceback": traceback.format_exc(),
            "processing_time_seconds": time.time() - start_time
        }


def parallel_process_sheet(
    file_path: str,
    sheet_name: str,
    asset_class: str,
    import_date: date,
    batch_size: int = 1000
) -> Dict[str, Any]:
    """
    Process a single sheet from the Excel file in a parallel worker.
    This function is optimized for memory usage and performance.
    
    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to process
        asset_class: Asset class to assign to records ('Equity', 'Fixed Income', 'Alternatives')
        import_date: Date to assign to records
        batch_size: Size of batches for processing
        
    Returns:
        Dict with processing results and records
    """
    logger.info(f"Processing {asset_class} sheet: {sheet_name}")
    records = []
    
    try:
        # 1. First identify the columns we need
        df_header = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)
        columns = df_header.columns.tolist()
        
        # Find our columns using flexible matching
        position_col = next((col for col in columns if any(x in str(col).lower() for x in ["position", "security", "name"])), None)
        
        if not position_col:
            logger.error(f"Required 'Position' column not found in {asset_class} sheet")
            return {
                "asset_class": asset_class,
                "success": False,
                "record_count": 0,
                "records": [],
                "error": "Missing required Position column"
            }
        
        # Asset class specific column mapping
        ticker_col = next((col for col in columns if "ticker" in str(col).lower()), None)
        cusip_col = next((col for col in columns if "cusip" in str(col).lower()), None)
        second_level_col = next((col for col in columns if "second" in str(col).lower() and "level" in str(col).lower()), None)
        amended_id_col = next((col for col in columns if "amended" in str(col).lower() and "id" in str(col).lower()), None)
        notes_col = next((col for col in columns if "note" in str(col).lower()), None)
        
        # Metric columns - different by asset class
        volatility_col = None
        beta_col = None
        duration_col = None
        
        if asset_class == "Equity":
            volatility_col = next((col for col in columns if any(x in str(col).lower() for x in ["vol", "volatility", "std dev"])), None)
            beta_col = next((col for col in columns if "beta" in str(col).lower()), None)
        elif asset_class == "Fixed Income":
            duration_col = next((col for col in columns if "duration" in str(col).lower()), None)
            # Some fixed income sheets may have volatility as well
            volatility_col = next((col for col in columns if any(x in str(col).lower() for x in ["vol", "volatility", "std dev"])), None)
        elif asset_class == "Alternatives":
            # Alternatives might have various metrics
            volatility_col = next((col for col in columns if any(x in str(col).lower() for x in ["vol", "volatility", "std dev"])), None)
            beta_col = next((col for col in columns if "beta" in str(col).lower()), None)
            
        logger.info(f"Column mapping for {asset_class}: Position={position_col}, Ticker={ticker_col}, "
                   f"CUSIP={cusip_col}, Second Level={second_level_col}, "
                   f"Volatility={volatility_col}, Beta={beta_col}, Duration={duration_col}")
        
        # 2. Process in memory-efficient chunks
        chunk_size = 1000  # Adjust based on file size and memory constraints
        record_count = 0
        current_row = 0
        
        # Get row count for progress tracking
        try:
            # Using a smaller sample to estimate total rows to save memory
            df_sample = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5)
            # Use the file size to estimate total rows (rough approximation)
            file_size = os.path.getsize(file_path)
            sample_size = sys.getsizeof(df_sample)
            estimated_rows = min(int(file_size / (sample_size / 5) * 0.8), 100000)  # Cap at 100k as safety
            logger.info(f"Estimated {estimated_rows} rows in {asset_class} sheet")
            del df_sample
            gc.collect()
        except Exception as sample_error:
            logger.warning(f"Error estimating row count: {sample_error}")
            estimated_rows = 10000  # Default assumption
        
        # Process in chunks to manage memory usage
        while True:
            try:
                # Read a chunk efficiently
                df_chunk = pd.read_excel(
                    file_path, 
                    sheet_name=sheet_name,
                    skiprows=range(1, current_row + 1) if current_row > 0 else None,
                    nrows=chunk_size
                )
                
                # Exit if no more data
                if len(df_chunk) == 0:
                    break
                
                # Process each row in the chunk
                for idx, row in df_chunk.iterrows():
                    try:
                        # Skip invalid rows
                        if pd.isna(row.get(position_col, None)):
                            continue
                        
                        # Basic record data
                        position = str(row.get(position_col, "")).strip()
                        
                        # Create record dict with all fields
                        record = {
                            "import_date": import_date,
                            "position": position,
                            "ticker_symbol": str(row.get(ticker_col, "")).strip() if ticker_col and not pd.isna(row.get(ticker_col, None)) else None,
                            "cusip": str(row.get(cusip_col, "")).strip() if cusip_col and not pd.isna(row.get(cusip_col, None)) else None,
                            "asset_class": asset_class,
                            "second_level": str(row.get(second_level_col, "")).strip() if second_level_col and not pd.isna(row.get(second_level_col, None)) else None,
                            "volatility": None,
                            "beta": None,
                            "duration": None,
                            "notes": str(row.get(notes_col, "")).strip() if notes_col and not pd.isna(row.get(notes_col, None)) else None,
                            "amended_id": str(row.get(amended_id_col, "")).strip() if amended_id_col and not pd.isna(row.get(amended_id_col, None)) else None,
                            "source_file": os.path.basename(file_path),
                            "source_tab": sheet_name,
                            "source_row": current_row + idx + 1
                        }
                        
                        # Process metric fields appropriately for the asset class
                        # Volatility handling
                        if volatility_col and not pd.isna(row.get(volatility_col, None)):
                            vol_value = row.get(volatility_col)
                            if isinstance(vol_value, (int, float)) and not pd.isna(vol_value):
                                record["volatility"] = float(vol_value)
                            elif isinstance(vol_value, str):
                                vol_str = vol_value.strip().lower()
                                if vol_str and not any(pattern in vol_str for pattern in ['n/a', 'na', 'nan', '-', '#n/a']):
                                    try:
                                        record["volatility"] = float(vol_str)
                                    except (ValueError, TypeError):
                                        pass
                        
                        # Beta handling
                        if beta_col and not pd.isna(row.get(beta_col, None)):
                            beta_value = row.get(beta_col)
                            if isinstance(beta_value, (int, float)) and not pd.isna(beta_value):
                                record["beta"] = float(beta_value)
                            elif isinstance(beta_value, str):
                                beta_str = beta_value.strip().lower()
                                if beta_str and not any(pattern in beta_str for pattern in ['n/a', 'na', 'nan', '-', '#n/a']):
                                    try:
                                        record["beta"] = float(beta_str)
                                    except (ValueError, TypeError):
                                        pass
                        
                        # Duration handling (mainly for Fixed Income)
                        if duration_col and not pd.isna(row.get(duration_col, None)):
                            duration_value = row.get(duration_col)
                            if isinstance(duration_value, (int, float)) and not pd.isna(duration_value):
                                record["duration"] = float(duration_value)
                            elif isinstance(duration_value, str):
                                duration_str = duration_value.strip().lower()
                                if duration_str and not any(pattern in duration_str for pattern in ['n/a', 'na', 'nan', '-', '#n/a']):
                                    try:
                                        record["duration"] = float(duration_str)
                                    except (ValueError, TypeError):
                                        pass
                        
                        # Add to our records collection
                        records.append(record)
                        
                    except Exception as row_error:
                        logger.warning(f"Error processing {asset_class} row {current_row + idx + 1}: {row_error}")
                
                # Update our position and progress tracking
                chunk_record_count = len(records) - record_count
                record_count = len(records)
                current_row += len(df_chunk)
                
                # Log progress
                progress_pct = min(100, int((current_row / estimated_rows) * 100))
                logger.info(f"{asset_class} progress: {progress_pct}% - Processed {chunk_record_count} records from rows {current_row-len(df_chunk)+1}-{current_row}")
                
                # If we got fewer rows than chunk_size, we're at the end
                if len(df_chunk) < chunk_size:
                    break
                
                # Clear chunk data
                del df_chunk
                gc.collect()
                
            except Exception as chunk_error:
                logger.error(f"Error processing {asset_class} chunk starting at row {current_row+1}: {chunk_error}")
                # Try to continue with the next chunk
                current_row += chunk_size
        
        # Return the results
        return {
            "asset_class": asset_class,
            "success": True,
            "record_count": len(records),
            "records": records
        }
        
    except Exception as sheet_error:
        logger.exception(f"Error processing {asset_class} sheet {sheet_name}: {sheet_error}")
        return {
            "asset_class": asset_class,
            "success": False,
            "record_count": 0,
            "records": [],
            "error": str(sheet_error)
        }


# Make sure to import these if they're not in scope
import sqlalchemy as sa