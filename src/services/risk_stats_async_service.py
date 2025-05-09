"""
Asynchronous service for risk statistics processing.
This service provides a completely redesigned approach to processing risk statistics
with a focus on performance, reliability, and scalability.

Key improvements:
1. Job-based tracking for better monitoring and error handling
2. Optimized batch database operations
3. In-memory caching for faster lookups
4. Improved error handling with detailed error reporting
5. Parallel processing capabilities where possible
"""

import os
import logging
import time
import traceback
from datetime import date, datetime
import multiprocessing
import concurrent.futures
from typing import Dict, List, Optional, Tuple, Any, Union
import gc

import pandas as pd
import numpy as np
from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.models.models import EgnyteRiskStat, RiskStatsJob, JobStatus
from src.services.egnyte_service import download_risk_stats_file
from src.database import get_db_connection

# Configure logging
logger = logging.getLogger(__name__)


class RiskStatsCache:
    """
    In-memory cache for risk statistics data.
    Used to accelerate lookups during portfolio risk metric calculations.
    """
    def __init__(self):
        self._cache = {}
        self._stats = {"hits": 0, "misses": 0}
        self._last_updated = None
    
    def set(self, key: str, value: Any) -> None:
        """Store a value in the cache."""
        self._cache[key] = value
        self._last_updated = datetime.utcnow()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the cache."""
        value = self._cache.get(key, default)
        if value is not default:
            self._stats["hits"] += 1
        else:
            self._stats["misses"] += 1
        return value
    
    def contains(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        return key in self._cache
    
    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()
        self._stats = {"hits": 0, "misses": 0}
        self._last_updated = None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self._cache),
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_ratio": self._stats["hits"] / max(1, self._stats["hits"] + self._stats["misses"]),
            "last_updated": self._last_updated
        }


# Global risk stats cache for reuse across requests
# This should be thread-safe as it's mostly read operations
RISK_STATS_CACHE = RiskStatsCache()


def create_risk_stats_job(
    db: Session, 
    use_test_file: bool = False,
    debug_mode: bool = False,
    batch_size: int = 200,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Create a new risk stats processing job.
    
    Args:
        db (Session): Database session
        use_test_file (bool): Whether to use a test file instead of downloading from Egnyte
        debug_mode (bool): Enable debug mode for detailed logging
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        Dict with job information including the job ID
    """
    try:
        # Create a new job record
        job = RiskStatsJob(
            status=JobStatus.PENDING.value,
            use_test_file=use_test_file,
            debug_mode=debug_mode,
            batch_size=batch_size,
            max_retries=max_retries
        )
        
        db.add(job)
        db.commit()
        db.refresh(job)
        
        logger.info(f"Created risk stats job with ID {job.id}")
        
        return {
            "success": True,
            "job_id": job.id,
            "status": job.status,
            "created_at": job.created_at.isoformat() if job.created_at else None
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating risk stats job: {e}")
        return {
            "success": False,
            "error": f"Failed to create job: {str(e)}"
        }


def get_risk_stats_job(db: Session, job_id: int) -> Dict[str, Any]:
    """
    Get information about a risk stats job.
    
    Args:
        db (Session): Database session
        job_id (int): ID of the job to retrieve
        
    Returns:
        Dict with job information
    """
    try:
        job = db.query(RiskStatsJob).filter(RiskStatsJob.id == job_id).first()
        
        if not job:
            return {
                "success": False,
                "error": f"Job with ID {job_id} not found"
            }
        
        return {
            "success": True,
            "job_id": job.id,
            "status": job.status,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "updated_at": job.updated_at.isoformat() if job.updated_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "duration_seconds": job.duration_seconds,
            "total_records": job.total_records,
            "equity_records": job.equity_records,
            "fixed_income_records": job.fixed_income_records,
            "alternatives_records": job.alternatives_records,
            "error_message": job.error_message
        }
    except Exception as e:
        logger.error(f"Error retrieving risk stats job {job_id}: {e}")
        return {
            "success": False,
            "error": f"Failed to retrieve job: {str(e)}"
        }


def start_risk_stats_job(job_id: int) -> Dict[str, Any]:
    """
    Start processing a risk stats job in the background.
    This function spawns a separate process to handle the job.
    
    Args:
        job_id (int): ID of the job to process
        
    Returns:
        Dict with status information
    """
    try:
        # Create a new process to handle the job
        process = multiprocessing.Process(
            target=process_risk_stats_job,
            args=(job_id,)
        )
        
        # Start the process
        process.start()
        
        logger.info(f"Started risk stats job {job_id} in process {process.pid}")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Job started in background process"
        }
    except Exception as e:
        logger.error(f"Error starting risk stats job {job_id}: {e}")
        
        # Update job status to failed
        with get_db_connection() as db:
            try:
                job = db.query(RiskStatsJob).filter(RiskStatsJob.id == job_id).first()
                if job:
                    job.status = JobStatus.FAILED.value
                    job.error_message = f"Failed to start job: {str(e)}"
                    job.traceback = traceback.format_exc()
                    db.commit()
            except Exception as db_error:
                logger.error(f"Error updating job status: {db_error}")
        
        return {
            "success": False,
            "error": f"Failed to start job: {str(e)}"
        }


def process_risk_stats_job(job_id: int) -> None:
    """
    Process a risk stats job in a separate process.
    This function handles the entire risk stats processing workflow.
    
    Args:
        job_id (int): ID of the job to process
    """
    start_time = time.time()
    memory_usage_start = get_memory_usage()
    
    logger.info(f"Processing risk stats job {job_id}")
    
    with get_db_connection() as db:
        try:
            # Get the job record
            job = db.query(RiskStatsJob).filter(RiskStatsJob.id == job_id).first()
            
            if not job:
                logger.error(f"Job with ID {job_id} not found")
                return
            
            # Update job status to running
            job.status = JobStatus.RUNNING.value
            db.commit()
            
            # Process the job using the parameters from the job record
            result = process_risk_stats(
                db=db,
                use_test_file=job.use_test_file,
                batch_size=job.batch_size,
                max_retries=job.max_retries,
                debug_mode=job.debug_mode
            )
            
            # Update job with results
            end_time = time.time()
            memory_usage_end = get_memory_usage()
            
            job.status = JobStatus.COMPLETED.value if result.get("success", False) else JobStatus.FAILED.value
            job.completed_at = datetime.utcnow()
            job.duration_seconds = end_time - start_time
            job.memory_usage_mb = memory_usage_end - memory_usage_start
            
            # Store results
            job.total_records = result.get("total_records", 0)
            job.equity_records = result.get("equity_records", 0)
            job.fixed_income_records = result.get("fixed_income_records", 0)
            job.alternatives_records = result.get("alternatives_records", 0)
            
            if not result.get("success", False):
                job.error_message = result.get("error", "Unknown error")
            
            # Generate a cache key for this job's results
            import hashlib
            cache_key = hashlib.md5(f"{job_id}:{job.completed_at}:{job.total_records}".encode()).hexdigest()
            job.cache_key = cache_key
            
            db.commit()
            
            # Clear the global cache to ensure fresh data for future queries
            RISK_STATS_CACHE.clear()
            
            logger.info(f"Completed risk stats job {job_id} in {job.duration_seconds:.2f} seconds")
            
        except Exception as e:
            logger.exception(f"Error processing risk stats job {job_id}: {e}")
            
            try:
                # Update job status to failed
                job = db.query(RiskStatsJob).filter(RiskStatsJob.id == job_id).first()
                if job:
                    job.status = JobStatus.FAILED.value
                    job.error_message = f"Error during processing: {str(e)}"
                    job.traceback = traceback.format_exc()
                    job.completed_at = datetime.utcnow()
                    job.duration_seconds = time.time() - start_time
                    db.commit()
            except Exception as update_error:
                logger.error(f"Error updating job status: {update_error}")


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return memory_info.rss / 1024 / 1024  # Convert bytes to MB
    except Exception:
        return 0.0


def process_risk_stats(
    db: Session,
    use_test_file: bool = False,
    batch_size: int = 200,
    max_retries: int = 3,
    debug_mode: bool = False
) -> Dict[str, Any]:
    """
    Completely redesigned function to download and process risk statistics.
    
    This is a high-performance implementation focused on:
    1. Efficient database operations
    2. Optimized memory usage
    3. Reliable error handling
    4. Accurate result tracking
    
    Args:
        db (Session): Database session
        use_test_file (bool): Whether to use a test file instead of downloading from Egnyte
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts
        debug_mode (bool): Enable detailed debug logging
        
    Returns:
        Dict with processing results
    """
    start_time = time.time()
    logger.info(f"Starting optimized risk stats processing (batch_size={batch_size}, max_retries={max_retries})")
    
    try:
        # Get the current date for import tracking
        import_date = date.today()
        logger.info(f"Using import date: {import_date}")
        
        # 1. Download the risk stats file
        try:
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
            
            if "EGNYTE_ACCESS_TOKEN" in error_msg:
                return {
                    "success": False,
                    "error": "Egnyte API token is missing or invalid. Please set the EGNYTE_ACCESS_TOKEN environment variable."
                }
            else:
                return {
                    "success": False,
                    "error": f"Download error: {error_msg}"
                }
        
        # 2. Clean up existing records for today
        try:
            # Use SQL directly for better performance
            for asset_class in ['Equity', 'Fixed Income', 'Alternatives']:
                sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :date AND asset_class = :asset_class")
                result = db.execute(sql, {"date": import_date, "asset_class": asset_class})
                
                # Log deletion count when available
                if hasattr(result, 'rowcount'):
                    logger.info(f"Deleted {result.rowcount} {asset_class} records for {import_date}")
                    
                db.commit()
        except Exception as cleanup_error:
            logger.error(f"Error cleaning up existing records: {cleanup_error}")
            db.rollback()
            return {
                "success": False,
                "error": f"Database cleanup failed: {cleanup_error}"
            }
        
        # 3. Load the Excel file using optimized settings
        try:
            xl = pd.ExcelFile(file_path)
            sheet_names = xl.sheet_names
            logger.info(f"Excel file contains sheets: {sheet_names}")
            
            # Identify the sheets we need
            equity_sheet = None
            fixed_income_sheet = None 
            alternatives_sheet = None
            
            # Use flexible matching for sheet names
            for sheet in sheet_names:
                sheet_lower = sheet.lower() if isinstance(sheet, str) else ""
                
                if "equity" in sheet_lower:
                    equity_sheet = sheet
                    logger.info(f"Identified '{sheet}' as the Equity sheet")
                elif "fixed" in sheet_lower or "fi " in sheet_lower:
                    if "duration" not in sheet_lower:
                        fixed_income_sheet = sheet
                        logger.info(f"Identified '{sheet}' as the Fixed Income sheet")
                elif "alt" in sheet_lower:
                    alternatives_sheet = sheet
                    logger.info(f"Identified '{sheet}' as the Alternatives sheet")
        except Exception as excel_error:
            logger.error(f"Error analyzing Excel file: {excel_error}")
            return {
                "success": False,
                "error": f"Error analyzing Excel file: {excel_error}"
            }
        
        # 4. Process each sheet in parallel using a thread pool
        results = {
            "equity_records": 0,
            "fixed_income_records": 0,
            "alternatives_records": 0,
            "total_records": 0,
            "success": True
        }
        
        # Use a thread pool for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all sheet processing tasks
            futures = []
            
            if equity_sheet:
                futures.append(
                    executor.submit(
                        process_equity_sheet_optimized,
                        file_path=file_path,
                        sheet_name=equity_sheet,
                        import_date=import_date,
                        db=db,
                        batch_size=batch_size,
                        max_retries=max_retries
                    )
                )
            
            if fixed_income_sheet:
                futures.append(
                    executor.submit(
                        process_fixed_income_sheet_optimized,
                        file_path=file_path,
                        sheet_name=fixed_income_sheet,
                        import_date=import_date,
                        db=db,
                        batch_size=batch_size,
                        max_retries=max_retries
                    )
                )
            
            if alternatives_sheet:
                futures.append(
                    executor.submit(
                        process_alternatives_sheet_optimized,
                        file_path=file_path,
                        sheet_name=alternatives_sheet,
                        import_date=import_date,
                        db=db,
                        batch_size=batch_size,
                        max_retries=max_retries
                    )
                )
            
            # Collect results from all futures
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    sheet_type = result.get("sheet_type")
                    record_count = result.get("record_count", 0)
                    
                    if sheet_type == "equity":
                        results["equity_records"] = record_count
                    elif sheet_type == "fixed_income":
                        results["fixed_income_records"] = record_count
                    elif sheet_type == "alternatives":
                        results["alternatives_records"] = record_count
                    
                    logger.info(f"Processed {sheet_type} sheet with {record_count} records")
                except Exception as future_error:
                    logger.error(f"Error processing sheet: {future_error}")
                    # Don't mark the entire job as failed, just log the error
        
        # Calculate the total records processed
        results["total_records"] = (
            results["equity_records"] + 
            results["fixed_income_records"] + 
            results["alternatives_records"]
        )
        
        # Final verification and cleanup
        try:
            # Force garbage collection to free memory
            gc.collect()
            
            # Delete the temporary file
            if os.path.exists(file_path) and not use_test_file:
                os.unlink(file_path)
                logger.info(f"Deleted temporary file: {file_path}")
                
            # Log completion time
            end_time = time.time()
            processing_time = end_time - start_time
            results["processing_time_seconds"] = processing_time
            
            logger.info(f"Completed risk stats processing in {processing_time:.2f} seconds")
            logger.info(f"Processed {results['total_records']} records: "
                      f"Equity={results['equity_records']}, "
                      f"Fixed Income={results['fixed_income_records']}, "
                      f"Alternatives={results['alternatives_records']}")
            
            return results
        except Exception as cleanup_error:
            logger.error(f"Error during final cleanup: {cleanup_error}")
            # Still return the results
            return results
    except Exception as e:
        logger.exception(f"Unexpected error processing risk stats: {e}")
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "traceback": traceback.format_exc()
        }


def process_equity_sheet_optimized(
    file_path: str,
    sheet_name: str,
    import_date: date,
    db: Session,
    batch_size: int = 200,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Optimized function to process the Equity sheet from the Excel file.
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet to process
        import_date (date): Date of the import
        db (Session): Database session
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        Dict with processing results
    """
    logger.info(f"Processing Equity sheet: {sheet_name}")
    
    try:
        # Use optimized pandas settings for large files
        chunk_size = 1000
        record_count = 0
        
        # Get file size for logging
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"Excel file size: {file_size_mb:.2f} MB")
        
        # Check sheet structure first by reading header row
        try:
            df_header = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)
            columns = df_header.columns.tolist()
            logger.info(f"Equity sheet columns: {columns}")
            
            # Check for required columns
            position_col = None
            for col in columns:
                col_lower = str(col).lower()
                if "position" in col_lower or "security" in col_lower:
                    position_col = col
                    break
            
            if not position_col:
                logger.error("Required 'Position' column not found in Equity sheet")
                return {"sheet_type": "equity", "record_count": 0, "error": "Missing required columns"}
                
            # Define column mappings
            # We'll attempt to find each important column using flexible matching
            ticker_col = next((col for col in columns if "ticker" in str(col).lower()), None)
            cusip_col = next((col for col in columns if "cusip" in str(col).lower()), None)
            volatility_col = next((col for col in columns if any(x in str(col).lower() for x in ["vol", "volatility", "std dev"])), None)
            beta_col = next((col for col in columns if "beta" in str(col).lower()), None)
            second_level_col = next((col for col in columns if "second" in str(col).lower() and "level" in str(col).lower()), None)
            amended_id_col = next((col for col in columns if "amended" in str(col).lower() and "id" in str(col).lower()), None)
            notes_col = next((col for col in columns if "note" in str(col).lower()), None)
            
            logger.info(f"Identified columns - Position: {position_col}, Ticker: {ticker_col}, CUSIP: {cusip_col}, "
                       f"Volatility: {volatility_col}, Beta: {beta_col}, Second Level: {second_level_col}")
            
        except Exception as header_error:
            logger.error(f"Error reading Equity sheet header: {header_error}")
            return {"sheet_type": "equity", "record_count": 0, "error": f"Header read error: {str(header_error)}"}
            
        # Get a count of rows in the sheet (for progress tracking)
        try:
            df_preview = pd.read_excel(file_path, sheet_name=sheet_name)
            total_rows = len(df_preview)
            logger.info(f"Equity sheet has {total_rows} rows")
            
            # Release memory from preview read
            del df_preview
            gc.collect()
        except Exception as preview_error:
            logger.warning(f"Error getting row count: {preview_error} - will proceed without progress tracking")
            total_rows = None
        
        # Process in chunks to avoid memory issues
        # Use a list to collect all records before batch insertion
        all_records = []
        
        # Define our chunk reading loop
        current_row = 0
        while True:
            try:
                # Read a chunk
                df_chunk = pd.read_excel(
                    file_path, 
                    sheet_name=sheet_name,
                    skiprows=range(1, current_row + 1) if current_row > 0 else None,
                    nrows=chunk_size
                )
                
                # If we get an empty chunk, we're done
                if len(df_chunk) == 0:
                    break
                    
                # Process this chunk
                chunk_records = []
                for idx, row in df_chunk.iterrows():
                    try:
                        # Skip rows without a position
                        if pd.isna(row.get(position_col, None)):
                            continue
                            
                        position = str(row.get(position_col, "")).strip()
                        
                        # Create a new record
                        record = {
                            "import_date": import_date,
                            "position": position,
                            "ticker_symbol": str(row.get(ticker_col, "")).strip() if ticker_col and not pd.isna(row.get(ticker_col, None)) else None,
                            "cusip": str(row.get(cusip_col, "")).strip() if cusip_col and not pd.isna(row.get(cusip_col, None)) else None,
                            "asset_class": "Equity",
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
                        
                        # Clean and convert volatility if available
                        if volatility_col and not pd.isna(row.get(volatility_col, None)):
                            vol_value = row.get(volatility_col)
                            if isinstance(vol_value, (int, float)) and not pd.isna(vol_value):
                                record["volatility"] = float(vol_value)
                            elif isinstance(vol_value, str):
                                vol_str = vol_value.strip().lower()
                                if vol_str and not any(pattern in vol_str for pattern in ['n/a', 'na', 'nan', '-']):
                                    try:
                                        record["volatility"] = float(vol_str)
                                    except ValueError:
                                        pass
                        
                        # Clean and convert beta if available
                        if beta_col and not pd.isna(row.get(beta_col, None)):
                            beta_value = row.get(beta_col)
                            if isinstance(beta_value, (int, float)) and not pd.isna(beta_value):
                                record["beta"] = float(beta_value)
                            elif isinstance(beta_value, str):
                                beta_str = beta_value.strip().lower()
                                if beta_str and not any(pattern in beta_str for pattern in ['n/a', 'na', 'nan', '-']):
                                    try:
                                        record["beta"] = float(beta_str)
                                    except ValueError:
                                        pass
                        
                        # Add to our collection
                        chunk_records.append(record)
                        
                    except Exception as row_error:
                        logger.warning(f"Error processing row {current_row + idx + 1}: {row_error}")
                
                # Add this chunk's records to our master list
                all_records.extend(chunk_records)
                logger.info(f"Processed {len(chunk_records)} valid records from rows {current_row+1}-{current_row+len(df_chunk)}")
                
                # Update our position for the next chunk
                current_row += len(df_chunk)
                
                # Report progress if we know the total
                if total_rows:
                    progress = min(100, int(current_row / total_rows * 100))
                    logger.info(f"Progress: {progress}% ({current_row}/{total_rows} rows)")
                
                # If we got fewer rows than chunk_size, we're at the end
                if len(df_chunk) < chunk_size:
                    break
                    
                # Clear chunk data to free memory
                del df_chunk
                gc.collect()
                
            except Exception as chunk_error:
                logger.error(f"Error processing chunk starting at row {current_row+1}: {chunk_error}")
                # Try to continue with the next chunk
                current_row += chunk_size
                
                # Break if we're past the known total
                if total_rows and current_row >= total_rows:
                    break
        
        # Log the total records collected
        record_count = len(all_records)
        logger.info(f"Collected {record_count} valid equity records for insertion")
        
        # Insert all records using the bulk insert with SQLAlchemy Core
        if record_count > 0:
            # Use a more efficient bulk insert approach
            from sqlalchemy.dialects.postgresql import insert
            from src.models.models import EgnyteRiskStat
            
            # Process in batches
            for i in range(0, record_count, batch_size):
                batch = all_records[i:i+batch_size]
                batch_number = i // batch_size + 1
                logger.info(f"Inserting batch {batch_number} ({len(batch)} records)")
                
                try:
                    # Use the PostgreSQL-specific insert...on conflict
                    stmt = insert(EgnyteRiskStat.__table__)
                    
                    # Add the ON CONFLICT clause
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
                            'updated_at': sa.func.now()
                        }
                    )
                    
                    # Execute the statement
                    db.execute(stmt, batch)
                    db.commit()
                    
                    logger.info(f"Successfully inserted batch {batch_number}")
                    
                except Exception as insert_error:
                    logger.error(f"Error inserting batch {batch_number}: {insert_error}")
                    db.rollback()
                    
                    # If batch fails, try individual records as a fallback
                    logger.info(f"Falling back to individual inserts for batch {batch_number}")
                    success_count = 0
                    
                    for record in batch:
                        try:
                            # Create the EgnyteRiskStat object
                            risk_stat = EgnyteRiskStat(
                                import_date=record["import_date"],
                                position=record["position"],
                                ticker_symbol=record["ticker_symbol"],
                                cusip=record["cusip"],
                                asset_class=record["asset_class"],
                                second_level=record["second_level"],
                                volatility=record["volatility"],
                                beta=record["beta"],
                                duration=record["duration"],
                                notes=record["notes"],
                                amended_id=record["amended_id"],
                                source_file=record["source_file"],
                                source_tab=record["source_tab"],
                                source_row=record["source_row"]
                            )
                            
                            # Insert with retry logic
                            retry_count = 0
                            while retry_count < max_retries:
                                try:
                                    db.merge(risk_stat)
                                    db.commit()
                                    success_count += 1
                                    break
                                except Exception as retry_error:
                                    retry_count += 1
                                    logger.warning(f"Retry {retry_count}/{max_retries} for {record['position']}: {retry_error}")
                                    db.rollback()
                                    time.sleep(0.1 * retry_count)  # Exponential backoff
                            
                        except Exception as record_error:
                            logger.error(f"Error inserting individual record {record['position']}: {record_error}")
                    
                    logger.info(f"Individual inserts completed: {success_count}/{len(batch)} successful")
        
        # Return the result
        return {
            "sheet_type": "equity",
            "record_count": record_count,
            "success": True
        }
        
    except Exception as e:
        logger.exception(f"Error processing Equity sheet: {e}")
        return {
            "sheet_type": "equity",
            "record_count": 0,
            "success": False,
            "error": str(e)
        }


def process_fixed_income_sheet_optimized(
    file_path: str,
    sheet_name: str,
    import_date: date,
    db: Session,
    batch_size: int = 200,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Optimized function to process the Fixed Income sheet from the Excel file.
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet to process
        import_date (date): Date of the import
        db (Session): Database session
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        Dict with processing results
    """
    logger.info(f"Processing Fixed Income sheet: {sheet_name}")
    
    try:
        # Use optimized pandas settings for large files
        chunk_size = 1000
        record_count = 0
        
        # Get file size for logging
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"Excel file size: {file_size_mb:.2f} MB")
        
        # Check sheet structure first by reading header row
        try:
            df_header = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)
            columns = df_header.columns.tolist()
            logger.info(f"Fixed Income sheet columns: {columns}")
            
            # Check for required columns
            position_col = None
            for col in columns:
                col_lower = str(col).lower()
                if "position" in col_lower or "security" in col_lower:
                    position_col = col
                    break
            
            if not position_col:
                logger.error("Required 'Position' column not found in Fixed Income sheet")
                return {"sheet_type": "fixed_income", "record_count": 0, "error": "Missing required columns"}
                
            # Define column mappings
            # We'll attempt to find each important column using flexible matching
            ticker_col = next((col for col in columns if "ticker" in str(col).lower()), None)
            cusip_col = next((col for col in columns if "cusip" in str(col).lower()), None)
            duration_col = next((col for col in columns if "duration" in str(col).lower()), None)
            second_level_col = next((col for col in columns if "second" in str(col).lower() and "level" in str(col).lower()), None)
            amended_id_col = next((col for col in columns if "amended" in str(col).lower() and "id" in str(col).lower()), None)
            notes_col = next((col for col in columns if "note" in str(col).lower()), None)
            
            logger.info(f"Identified columns - Position: {position_col}, Ticker: {ticker_col}, CUSIP: {cusip_col}, "
                       f"Duration: {duration_col}, Second Level: {second_level_col}")
            
        except Exception as header_error:
            logger.error(f"Error reading Fixed Income sheet header: {header_error}")
            return {"sheet_type": "fixed_income", "record_count": 0, "error": f"Header read error: {str(header_error)}"}
            
        # Get a count of rows in the sheet (for progress tracking)
        try:
            df_preview = pd.read_excel(file_path, sheet_name=sheet_name)
            total_rows = len(df_preview)
            logger.info(f"Fixed Income sheet has {total_rows} rows")
            
            # Release memory from preview read
            del df_preview
            gc.collect()
        except Exception as preview_error:
            logger.warning(f"Error getting row count: {preview_error} - will proceed without progress tracking")
            total_rows = None
        
        # Process in chunks to avoid memory issues
        # Use a list to collect all records before batch insertion
        all_records = []
        
        # Define our chunk reading loop
        current_row = 0
        while True:
            try:
                # Read a chunk
                df_chunk = pd.read_excel(
                    file_path, 
                    sheet_name=sheet_name,
                    skiprows=range(1, current_row + 1) if current_row > 0 else None,
                    nrows=chunk_size
                )
                
                # If we get an empty chunk, we're done
                if len(df_chunk) == 0:
                    break
                    
                # Process this chunk
                chunk_records = []
                for idx, row in df_chunk.iterrows():
                    try:
                        # Skip rows without a position
                        if pd.isna(row.get(position_col, None)):
                            continue
                            
                        position = str(row.get(position_col, "")).strip()
                        
                        # Create a new record
                        record = {
                            "import_date": import_date,
                            "position": position,
                            "ticker_symbol": str(row.get(ticker_col, "")).strip() if ticker_col and not pd.isna(row.get(ticker_col, None)) else None,
                            "cusip": str(row.get(cusip_col, "")).strip() if cusip_col and not pd.isna(row.get(cusip_col, None)) else None,
                            "asset_class": "Fixed Income",
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
                        
                        # Clean and convert duration if available
                        if duration_col and not pd.isna(row.get(duration_col, None)):
                            dur_value = row.get(duration_col)
                            if isinstance(dur_value, (int, float)) and not pd.isna(dur_value):
                                record["duration"] = float(dur_value)
                            elif isinstance(dur_value, str):
                                dur_str = dur_value.strip().lower()
                                if dur_str and not any(pattern in dur_str for pattern in ['n/a', 'na', 'nan', '-']):
                                    try:
                                        record["duration"] = float(dur_str)
                                    except ValueError:
                                        pass
                        
                        # Add to our collection
                        chunk_records.append(record)
                        
                    except Exception as row_error:
                        logger.warning(f"Error processing row {current_row + idx + 1}: {row_error}")
                
                # Add this chunk's records to our master list
                all_records.extend(chunk_records)
                logger.info(f"Processed {len(chunk_records)} valid records from rows {current_row+1}-{current_row+len(df_chunk)}")
                
                # Update our position for the next chunk
                current_row += len(df_chunk)
                
                # Report progress if we know the total
                if total_rows:
                    progress = min(100, int(current_row / total_rows * 100))
                    logger.info(f"Progress: {progress}% ({current_row}/{total_rows} rows)")
                
                # If we got fewer rows than chunk_size, we're at the end
                if len(df_chunk) < chunk_size:
                    break
                    
                # Clear chunk data to free memory
                del df_chunk
                gc.collect()
                
            except Exception as chunk_error:
                logger.error(f"Error processing chunk starting at row {current_row+1}: {chunk_error}")
                # Try to continue with the next chunk
                current_row += chunk_size
                
                # Break if we're past the known total
                if total_rows and current_row >= total_rows:
                    break
        
        # Log the total records collected
        record_count = len(all_records)
        logger.info(f"Collected {record_count} valid fixed income records for insertion")
        
        # Insert all records using the bulk insert with SQLAlchemy Core
        if record_count > 0:
            # Use a more efficient bulk insert approach
            from sqlalchemy.dialects.postgresql import insert
            from src.models.models import EgnyteRiskStat
            
            # Process in batches
            for i in range(0, record_count, batch_size):
                batch = all_records[i:i+batch_size]
                batch_number = i // batch_size + 1
                logger.info(f"Inserting batch {batch_number} ({len(batch)} records)")
                
                try:
                    # Use the PostgreSQL-specific insert...on conflict
                    stmt = insert(EgnyteRiskStat.__table__)
                    
                    # Add the ON CONFLICT clause
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
                            'updated_at': sa.func.now()
                        }
                    )
                    
                    # Execute the statement
                    db.execute(stmt, batch)
                    db.commit()
                    
                    logger.info(f"Successfully inserted batch {batch_number}")
                    
                except Exception as insert_error:
                    logger.error(f"Error inserting batch {batch_number}: {insert_error}")
                    db.rollback()
                    
                    # If batch fails, try individual records as a fallback
                    logger.info(f"Falling back to individual inserts for batch {batch_number}")
                    success_count = 0
                    
                    for record in batch:
                        try:
                            # Create the EgnyteRiskStat object
                            risk_stat = EgnyteRiskStat(
                                import_date=record["import_date"],
                                position=record["position"],
                                ticker_symbol=record["ticker_symbol"],
                                cusip=record["cusip"],
                                asset_class=record["asset_class"],
                                second_level=record["second_level"],
                                volatility=record["volatility"],
                                beta=record["beta"],
                                duration=record["duration"],
                                notes=record["notes"],
                                amended_id=record["amended_id"],
                                source_file=record["source_file"],
                                source_tab=record["source_tab"],
                                source_row=record["source_row"]
                            )
                            
                            # Insert with retry logic
                            retry_count = 0
                            while retry_count < max_retries:
                                try:
                                    db.merge(risk_stat)
                                    db.commit()
                                    success_count += 1
                                    break
                                except Exception as retry_error:
                                    retry_count += 1
                                    logger.warning(f"Retry {retry_count}/{max_retries} for {record['position']}: {retry_error}")
                                    db.rollback()
                                    time.sleep(0.1 * retry_count)  # Exponential backoff
                            
                        except Exception as record_error:
                            logger.error(f"Error inserting individual record {record['position']}: {record_error}")
                    
                    logger.info(f"Individual inserts completed: {success_count}/{len(batch)} successful")
        
        # Return the result
        return {
            "sheet_type": "fixed_income",
            "record_count": record_count,
            "success": True
        }
        
    except Exception as e:
        logger.exception(f"Error processing Fixed Income sheet: {e}")
        return {
            "sheet_type": "fixed_income",
            "record_count": 0,
            "success": False,
            "error": str(e)
        }


def process_alternatives_sheet_optimized(
    file_path: str,
    sheet_name: str,
    import_date: date,
    db: Session,
    batch_size: int = 200,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Optimized function to process the Alternatives sheet from the Excel file.
    
    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet to process
        import_date (date): Date of the import
        db (Session): Database session
        batch_size (int): Size of batches for database operations
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        Dict with processing results
    """
    logger.info(f"Processing Alternatives sheet: {sheet_name}")
    
    try:
        # Use optimized pandas settings for large files
        chunk_size = 1000
        record_count = 0
        
        # Get file size for logging
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"Excel file size: {file_size_mb:.2f} MB")
        
        # Check sheet structure first by reading header row
        try:
            df_header = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)
            columns = df_header.columns.tolist()
            logger.info(f"Alternatives sheet columns: {columns}")
            
            # Check for required columns
            position_col = None
            for col in columns:
                col_lower = str(col).lower()
                if "position" in col_lower or "security" in col_lower or "fund" in col_lower:
                    position_col = col
                    break
            
            if not position_col:
                logger.error("Required 'Position' column not found in Alternatives sheet")
                return {"sheet_type": "alternatives", "record_count": 0, "error": "Missing required columns"}
                
            # Define column mappings
            # We'll attempt to find each important column using flexible matching
            ticker_col = next((col for col in columns if "ticker" in str(col).lower()), None)
            cusip_col = next((col for col in columns if "cusip" in str(col).lower()), None)
            vol_col = next((col for col in columns if any(x in str(col).lower() for x in ["vol", "volatility", "std dev"])), None)
            beta_col = next((col for col in columns if "beta" in str(col).lower()), None)
            second_level_col = next((col for col in columns if "second" in str(col).lower() and "level" in str(col).lower()), None)
            amended_id_col = next((col for col in columns if "amended" in str(col).lower() and "id" in str(col).lower()), None)
            notes_col = next((col for col in columns if "note" in str(col).lower()), None)
            
            logger.info(f"Identified columns - Position: {position_col}, Ticker: {ticker_col}, CUSIP: {cusip_col}, "
                       f"Volatility: {vol_col}, Beta: {beta_col}, Second Level: {second_level_col}")
            
        except Exception as header_error:
            logger.error(f"Error reading Alternatives sheet header: {header_error}")
            return {"sheet_type": "alternatives", "record_count": 0, "error": f"Header read error: {str(header_error)}"}
            
        # Get a count of rows in the sheet (for progress tracking)
        try:
            df_preview = pd.read_excel(file_path, sheet_name=sheet_name)
            total_rows = len(df_preview)
            logger.info(f"Alternatives sheet has {total_rows} rows")
            
            # Release memory from preview read
            del df_preview
            gc.collect()
        except Exception as preview_error:
            logger.warning(f"Error getting row count: {preview_error} - will proceed without progress tracking")
            total_rows = None
        
        # Process in chunks to avoid memory issues
        # Use a list to collect all records before batch insertion
        all_records = []
        
        # Define our chunk reading loop
        current_row = 0
        while True:
            try:
                # Read a chunk
                df_chunk = pd.read_excel(
                    file_path, 
                    sheet_name=sheet_name,
                    skiprows=range(1, current_row + 1) if current_row > 0 else None,
                    nrows=chunk_size
                )
                
                # If we get an empty chunk, we're done
                if len(df_chunk) == 0:
                    break
                    
                # Process this chunk
                chunk_records = []
                for idx, row in df_chunk.iterrows():
                    try:
                        # Skip rows without a position
                        if pd.isna(row.get(position_col, None)):
                            continue
                            
                        position = str(row.get(position_col, "")).strip()
                        
                        # Create a new record
                        record = {
                            "import_date": import_date,
                            "position": position,
                            "ticker_symbol": str(row.get(ticker_col, "")).strip() if ticker_col and not pd.isna(row.get(ticker_col, None)) else None,
                            "cusip": str(row.get(cusip_col, "")).strip() if cusip_col and not pd.isna(row.get(cusip_col, None)) else None,
                            "asset_class": "Alternatives",
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
                        
                        # Clean and convert volatility if available
                        if vol_col and not pd.isna(row.get(vol_col, None)):
                            vol_value = row.get(vol_col)
                            if isinstance(vol_value, (int, float)) and not pd.isna(vol_value):
                                record["volatility"] = float(vol_value)
                            elif isinstance(vol_value, str):
                                vol_str = vol_value.strip().lower()
                                if vol_str and not any(pattern in vol_str for pattern in ['n/a', 'na', 'nan', '-']):
                                    try:
                                        record["volatility"] = float(vol_str)
                                    except ValueError:
                                        pass
                        
                        # Clean and convert beta if available
                        if beta_col and not pd.isna(row.get(beta_col, None)):
                            beta_value = row.get(beta_col)
                            if isinstance(beta_value, (int, float)) and not pd.isna(beta_value):
                                record["beta"] = float(beta_value)
                            elif isinstance(beta_value, str):
                                beta_str = beta_value.strip().lower()
                                if beta_str and not any(pattern in beta_str for pattern in ['n/a', 'na', 'nan', '-']):
                                    try:
                                        record["beta"] = float(beta_str)
                                    except ValueError:
                                        pass
                        
                        # Add to our collection
                        chunk_records.append(record)
                        
                    except Exception as row_error:
                        logger.warning(f"Error processing row {current_row + idx + 1}: {row_error}")
                
                # Add this chunk's records to our master list
                all_records.extend(chunk_records)
                logger.info(f"Processed {len(chunk_records)} valid records from rows {current_row+1}-{current_row+len(df_chunk)}")
                
                # Update our position for the next chunk
                current_row += len(df_chunk)
                
                # Report progress if we know the total
                if total_rows:
                    progress = min(100, int(current_row / total_rows * 100))
                    logger.info(f"Progress: {progress}% ({current_row}/{total_rows} rows)")
                
                # If we got fewer rows than chunk_size, we're at the end
                if len(df_chunk) < chunk_size:
                    break
                    
                # Clear chunk data to free memory
                del df_chunk
                gc.collect()
                
            except Exception as chunk_error:
                logger.error(f"Error processing chunk starting at row {current_row+1}: {chunk_error}")
                # Try to continue with the next chunk
                current_row += chunk_size
                
                # Break if we're past the known total
                if total_rows and current_row >= total_rows:
                    break
        
        # Log the total records collected
        record_count = len(all_records)
        logger.info(f"Collected {record_count} valid alternatives records for insertion")
        
        # Insert all records using the bulk insert with SQLAlchemy Core
        if record_count > 0:
            # Use a more efficient bulk insert approach
            from sqlalchemy.dialects.postgresql import insert
            from src.models.models import EgnyteRiskStat
            
            # Process in batches
            for i in range(0, record_count, batch_size):
                batch = all_records[i:i+batch_size]
                batch_number = i // batch_size + 1
                logger.info(f"Inserting batch {batch_number} ({len(batch)} records)")
                
                try:
                    # Use the PostgreSQL-specific insert...on conflict
                    stmt = insert(EgnyteRiskStat.__table__)
                    
                    # Add the ON CONFLICT clause
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
                            'updated_at': sa.func.now()
                        }
                    )
                    
                    # Execute the statement
                    db.execute(stmt, batch)
                    db.commit()
                    
                    logger.info(f"Successfully inserted batch {batch_number}")
                    
                except Exception as insert_error:
                    logger.error(f"Error inserting batch {batch_number}: {insert_error}")
                    db.rollback()
                    
                    # If batch fails, try individual records as a fallback
                    logger.info(f"Falling back to individual inserts for batch {batch_number}")
                    success_count = 0
                    
                    for record in batch:
                        try:
                            # Create the EgnyteRiskStat object
                            risk_stat = EgnyteRiskStat(
                                import_date=record["import_date"],
                                position=record["position"],
                                ticker_symbol=record["ticker_symbol"],
                                cusip=record["cusip"],
                                asset_class=record["asset_class"],
                                second_level=record["second_level"],
                                volatility=record["volatility"],
                                beta=record["beta"],
                                duration=record["duration"],
                                notes=record["notes"],
                                amended_id=record["amended_id"],
                                source_file=record["source_file"],
                                source_tab=record["source_tab"],
                                source_row=record["source_row"]
                            )
                            
                            # Insert with retry logic
                            retry_count = 0
                            while retry_count < max_retries:
                                try:
                                    db.merge(risk_stat)
                                    db.commit()
                                    success_count += 1
                                    break
                                except Exception as retry_error:
                                    retry_count += 1
                                    logger.warning(f"Retry {retry_count}/{max_retries} for {record['position']}: {retry_error}")
                                    db.rollback()
                                    time.sleep(0.1 * retry_count)  # Exponential backoff
                            
                        except Exception as record_error:
                            logger.error(f"Error inserting individual record {record['position']}: {record_error}")
                    
                    logger.info(f"Individual inserts completed: {success_count}/{len(batch)} successful")
        
        # Return the result
        return {
            "sheet_type": "alternatives",
            "record_count": record_count,
            "success": True
        }
        
    except Exception as e:
        logger.exception(f"Error processing Alternatives sheet: {e}")
        return {
            "sheet_type": "alternatives",
            "record_count": 0,
            "success": False,
            "error": str(e)
        }


def find_risk_stat_by_identifier(
    db: Session,
    position: str = None,
    cusip: str = None,
    ticker: str = None,
    asset_class: str = None,
    cache: RiskStatsCache = None
) -> Optional[Dict[str, Any]]:
    """
    Find a risk stat record by various identifiers, with caching.
    
    Args:
        db (Session): Database session
        position (str): Position name
        cusip (str): CUSIP
        ticker (str): Ticker symbol
        asset_class (str): Asset class
        cache (RiskStatsCache): Optional cache for faster lookups
        
    Returns:
        Optional[Dict[str, Any]]: Risk stat record if found, None otherwise
    """
    if not any([position, cusip, ticker]):
        return None
    
    # Get the latest import date
    latest_date = db.query(func.max(EgnyteRiskStat.import_date)).scalar()
    if not latest_date:
        return None
    
    # Try cache first if provided
    if cache:
        # Try CUSIP first (most reliable)
        if cusip:
            cache_key = f"cusip:{cusip}:{asset_class}"
            cached_result = cache.get(cache_key)
            if cached_result:
                return cached_result
        
        # Try ticker next
        if ticker:
            cache_key = f"ticker:{ticker}:{asset_class}"
            cached_result = cache.get(cache_key)
            if cached_result:
                return cached_result
        
        # Try position as last resort
        if position:
            cache_key = f"position:{position}:{asset_class}"
            cached_result = cache.get(cache_key)
            if cached_result:
                return cached_result
    
    # Build the query
    query = db.query(EgnyteRiskStat).filter(EgnyteRiskStat.import_date == latest_date)
    
    # Apply filters in order of preference
    if cusip:
        # Try CUSIP first
        record = query.filter(
            EgnyteRiskStat.cusip == cusip,
            EgnyteRiskStat.asset_class == asset_class if asset_class else True
        ).first()
        
        if record:
            result = record_to_dict(record)
            # Cache the result if we have a cache
            if cache:
                cache.set(f"cusip:{cusip}:{asset_class}", result)
                cache.set(f"position:{record.position}:{asset_class}", result)
                if record.ticker_symbol:
                    cache.set(f"ticker:{record.ticker_symbol}:{asset_class}", result)
            return result
    
    if ticker:
        # Try ticker next
        record = query.filter(
            EgnyteRiskStat.ticker_symbol == ticker,
            EgnyteRiskStat.asset_class == asset_class if asset_class else True
        ).first()
        
        if record:
            result = record_to_dict(record)
            # Cache the result if we have a cache
            if cache:
                cache.set(f"ticker:{ticker}:{asset_class}", result)
                cache.set(f"position:{record.position}:{asset_class}", result)
                if record.cusip:
                    cache.set(f"cusip:{record.cusip}:{asset_class}", result)
            return result
    
    if position:
        # Try position as last resort
        record = query.filter(
            EgnyteRiskStat.position == position,
            EgnyteRiskStat.asset_class == asset_class if asset_class else True
        ).first()
        
        if record:
            result = record_to_dict(record)
            # Cache the result if we have a cache
            if cache:
                cache.set(f"position:{position}:{asset_class}", result)
                if record.cusip:
                    cache.set(f"cusip:{record.cusip}:{asset_class}", result)
                if record.ticker_symbol:
                    cache.set(f"ticker:{record.ticker_symbol}:{asset_class}", result)
            return result
    
    # No match found
    return None


def record_to_dict(record: EgnyteRiskStat) -> Dict[str, Any]:
    """Convert a database record to a dictionary."""
    return {
        "import_date": record.import_date.isoformat() if record.import_date else None,
        "position": record.position,
        "ticker_symbol": record.ticker_symbol,
        "cusip": record.cusip,
        "asset_class": record.asset_class,
        "second_level": record.second_level,
        "volatility": float(record.volatility) if record.volatility is not None else None,
        "beta": float(record.beta) if record.beta is not None else None,
        "duration": float(record.duration) if record.duration is not None else None,
        "notes": record.notes,
        "amended_id": record.amended_id
    }