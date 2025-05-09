"""
Helper functions for performing efficient upsert operations on database records.
These utilities help avoid unique constraint violations while processing large datasets.
"""

import logging
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

def batch_upsert_risk_stats(db: Session, records, batch_size=100, max_retries=3):
    """
    Efficiently insert or update multiple risk stat records using optimized SQL.
    
    This function uses a PostgreSQL-specific UPSERT operation (INSERT ... ON CONFLICT)
    which is much more efficient than SQLAlchemy's ORM-level merge operations for
    large datasets.
    
    Args:
        db (Session): Database session
        records (list): List of EgnyteRiskStat model instances
        batch_size (int): Size of batches for processing
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        tuple: (success_count, error_count)
    """
    if not records:
        return 0, 0
        
    total_count = len(records)
    success_count = 0
    error_count = 0
    
    # Process in batches to avoid excessive parameter lists
    for start_idx in range(0, total_count, batch_size):
        end_idx = min(start_idx + batch_size, total_count)
        batch = records[start_idx:end_idx]
        batch_num = start_idx // batch_size + 1
        
        logger.info(f"Processing batch {batch_num} (records {start_idx+1}-{end_idx} of {total_count})")
        
        # Try the batch as a single transaction first
        try:
            # More efficient PostgreSQL UPSERT with WHERE clause to avoid unnecessary updates
            # This helps prevent excessive WAL generation and improves performance
            upsert_stmt = text("""
            INSERT INTO egnyte_risk_stats 
            (import_date, position, ticker_symbol, cusip, asset_class, second_level, 
            bloomberg_id, volatility, beta, duration, notes, amended_id, 
            source_file, source_tab, source_row, created_at)
            VALUES 
            (:import_date, :position, :ticker, :cusip, :asset_class, :second_level, 
            :bloomberg_id, :volatility, :beta, :duration, :notes, :amended_id, 
            :source_file, :source_tab, :source_row, NOW())
            ON CONFLICT (import_date, position, asset_class) 
            DO UPDATE SET
              ticker_symbol = EXCLUDED.ticker_symbol,
              cusip = EXCLUDED.cusip,
              second_level = EXCLUDED.second_level,
              bloomberg_id = EXCLUDED.bloomberg_id,
              volatility = EXCLUDED.volatility,
              beta = EXCLUDED.beta,
              duration = EXCLUDED.duration,
              notes = EXCLUDED.notes,
              amended_id = EXCLUDED.amended_id,
              updated_at = NOW()
            WHERE egnyte_risk_stats.ticker_symbol IS DISTINCT FROM EXCLUDED.ticker_symbol
               OR egnyte_risk_stats.cusip IS DISTINCT FROM EXCLUDED.cusip
               OR egnyte_risk_stats.second_level IS DISTINCT FROM EXCLUDED.second_level
               OR egnyte_risk_stats.bloomberg_id IS DISTINCT FROM EXCLUDED.bloomberg_id
               OR egnyte_risk_stats.volatility IS DISTINCT FROM EXCLUDED.volatility
               OR egnyte_risk_stats.beta IS DISTINCT FROM EXCLUDED.beta
               OR egnyte_risk_stats.duration IS DISTINCT FROM EXCLUDED.duration
               OR egnyte_risk_stats.notes IS DISTINCT FROM EXCLUDED.notes
               OR egnyte_risk_stats.amended_id IS DISTINCT FROM EXCLUDED.amended_id
            """)
            
            # Execute for all records in the batch
            # Pre-process all records to handle None/null values consistently
            batch_params = []
            for record in batch:
                # Handle possible None values in a more DB-friendly way
                # Convert numeric values to proper Decimal type and handle None safely
                # This fixes issues with database type compatibility
                batch_params.append({
                    "import_date": record.import_date,
                    "position": record.position,
                    "ticker": record.ticker_symbol,
                    "cusip": record.cusip,
                    "asset_class": record.asset_class,
                    "second_level": record.second_level,
                    "bloomberg_id": record.bloomberg_id,
                    "volatility": float(record.volatility) if record.volatility is not None else None,
                    "beta": float(record.beta) if record.beta is not None else None,
                    "duration": float(record.duration) if record.duration is not None else None,
                    "notes": record.notes,
                    "amended_id": record.amended_id,
                    "source_file": record.source_file,
                    "source_tab": record.source_tab,
                    "source_row": record.source_row
                })
            
            # Execute the batch insert with more efficient executemany 
            db.execute(upsert_stmt, batch_params)
            db.commit()
            
            success_count += len(batch)
            logger.info(f"Successfully processed batch {batch_num} ({len(batch)} records)")
        
        except Exception as batch_error:
            db.rollback()
            logger.error(f"Error processing batch {batch_num}: {batch_error}")
            
            # If batch fails, try with a smaller batch size
            if len(batch) > 10:
                logger.info(f"Retrying batch {batch_num} with smaller batch size")
                smaller_batch_size = len(batch) // 2
                
                # Process the current batch with smaller size recursively
                for sub_start in range(0, len(batch), smaller_batch_size):
                    sub_end = min(sub_start + smaller_batch_size, len(batch))
                    sub_batch = batch[sub_start:sub_end]
                    
                    # Recursive call with smaller batch
                    sub_success, sub_errors = batch_upsert_risk_stats(db, sub_batch, smaller_batch_size, max_retries)
                    success_count += sub_success
                    error_count += sub_errors
            else:
                # If batch is already small, try processing records individually
                logger.info(f"Retrying batch {batch_num} with individual record processing")
                
                batch_success = 0
                for record in batch:
                    # Allow multiple retry attempts for transient errors
                    retry_count = 0
                    success = False
                    
                    while not success and retry_count < max_retries:
                        try:
                            # Use the same upsert statement for individual records
                            # Apply the same numeric handling for individual records
                            params = {
                                "import_date": record.import_date,
                                "position": record.position,
                                "ticker": record.ticker_symbol,
                                "cusip": record.cusip,
                                "asset_class": record.asset_class,
                                "second_level": record.second_level,
                                "bloomberg_id": record.bloomberg_id,
                                "volatility": float(record.volatility) if record.volatility is not None else None,
                                "beta": float(record.beta) if record.beta is not None else None,
                                "duration": float(record.duration) if record.duration is not None else None,
                                "notes": record.notes,
                                "amended_id": record.amended_id,
                                "source_file": record.source_file,
                                "source_tab": record.source_tab,
                                "source_row": record.source_row
                            }
                            
                            # Re-defining the statement here to avoid "upsert_stmt is possibly unbound" error
                            individual_upsert_stmt = text("""
                            INSERT INTO egnyte_risk_stats 
                            (import_date, position, ticker_symbol, cusip, asset_class, second_level, 
                            bloomberg_id, volatility, beta, duration, notes, amended_id, 
                            source_file, source_tab, source_row, created_at)
                            VALUES 
                            (:import_date, :position, :ticker, :cusip, :asset_class, :second_level, 
                            :bloomberg_id, :volatility, :beta, :duration, :notes, :amended_id, 
                            :source_file, :source_tab, :source_row, NOW())
                            ON CONFLICT (import_date, position, asset_class) 
                            DO UPDATE SET
                              ticker_symbol = EXCLUDED.ticker_symbol,
                              cusip = EXCLUDED.cusip,
                              second_level = EXCLUDED.second_level,
                              bloomberg_id = EXCLUDED.bloomberg_id,
                              volatility = EXCLUDED.volatility,
                              beta = EXCLUDED.beta,
                              duration = EXCLUDED.duration,
                              notes = EXCLUDED.notes,
                              amended_id = EXCLUDED.amended_id,
                              updated_at = NOW()
                            WHERE egnyte_risk_stats.ticker_symbol IS DISTINCT FROM EXCLUDED.ticker_symbol
                               OR egnyte_risk_stats.cusip IS DISTINCT FROM EXCLUDED.cusip
                               OR egnyte_risk_stats.second_level IS DISTINCT FROM EXCLUDED.second_level
                               OR egnyte_risk_stats.bloomberg_id IS DISTINCT FROM EXCLUDED.bloomberg_id
                               OR egnyte_risk_stats.volatility IS DISTINCT FROM EXCLUDED.volatility
                               OR egnyte_risk_stats.beta IS DISTINCT FROM EXCLUDED.beta
                               OR egnyte_risk_stats.duration IS DISTINCT FROM EXCLUDED.duration
                               OR egnyte_risk_stats.notes IS DISTINCT FROM EXCLUDED.notes
                               OR egnyte_risk_stats.amended_id IS DISTINCT FROM EXCLUDED.amended_id
                            """)
                            
                            db.execute(individual_upsert_stmt, params)
                            db.commit()
                            
                            batch_success += 1
                            success = True
                        
                        except Exception as record_error:
                            db.rollback()
                            retry_count += 1
                            
                            if retry_count >= max_retries:
                                error_count += 1
                                logger.error(f"Failed to process record after {max_retries} attempts. "
                                            f"Position: {record.position}, Asset class: {record.asset_class}, "
                                            f"Error: {record_error}")
                            else:
                                logger.warning(f"Retry {retry_count}/{max_retries} for position {record.position}")
                
                success_count += batch_success
                logger.info(f"Individual processing of batch {batch_num} completed: {batch_success}/{len(batch)} successful")
    
    return success_count, error_count


def clean_risk_stats_date(db: Session, import_date):
    """
    Efficiently clean all risk statistics for a specific date.
    
    Args:
        db (Session): Database session
        import_date (date): The date to clean records for
        
    Returns:
        bool: True if cleaning was successful, False otherwise
    """
    try:
        # Delete records by asset class for better control
        for asset_class in ['Equity', 'Fixed Income', 'Alternatives']:
            sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :date AND asset_class = :asset_class")
            result = db.execute(sql, {"date": import_date, "asset_class": asset_class})
            # Safe access to rowcount (may not be available in some SQLAlchemy versions)
            deleted_count = getattr(result, 'rowcount', 0)
            logger.info(f"Deleted {deleted_count} {asset_class} records for {import_date}")
            db.commit()
        
        # Final check to make sure all records are gone
        from src.models.models import EgnyteRiskStat
        verify_count = db.query(EgnyteRiskStat).filter(EgnyteRiskStat.import_date == import_date).count()
        
        if verify_count > 0:
            # One more attempt with a direct delete
            sql = text("DELETE FROM egnyte_risk_stats WHERE import_date = :date")
            result = db.execute(sql, {"date": import_date})
            # Safe access to rowcount
            deleted_count = getattr(result, 'rowcount', 0)
            logger.info(f"Final cleanup: deleted {deleted_count} remaining records")
            db.commit()
        
        return True
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error cleaning risk stats for date {import_date}: {e}")
        return False