"""
API endpoints for risk statistics management.
This module provides a completely redesigned API approach for risk statistics
with a focus on performance, reliability, and clear error reporting.
"""

import logging
from datetime import date
from flask import jsonify, request
from sqlalchemy import func

from src.database import get_db_connection
from src.models.models import EgnyteRiskStat, RiskStatsJob, JobStatus
from src.services.risk_stats_async_service import (
    create_risk_stats_job, 
    get_risk_stats_job, 
    start_risk_stats_job,
    find_risk_stat_by_identifier,
    RISK_STATS_CACHE
)

# Set up logging
logger = logging.getLogger(__name__)


def update_risk_stats_async():
    """
    Start an asynchronous risk statistics update job.
    
    This endpoint creates and starts a background job to process 
    risk statistics, avoiding web server timeouts.
    
    Query parameters:
    - debug: If set to 'true', enables extended debugging output
    - use_test_file: If set to 'true', attempts to use a local test file if available
    - batch_size: Size of batches for database operations (default: 200)
    - max_retries: Maximum number of retry attempts for database operations (default: 3)
    
    Returns:
        JSON with the job ID and status
    """
    try:
        # Parse parameters from query string
        debug_mode = request.args.get('debug', 'false').lower() == 'true'
        use_test_file = request.args.get('use_test_file', 'false').lower() == 'true'
        
        # Handle batch size
        batch_size_str = request.args.get('batch_size')
        batch_size = 200  # Default
        if batch_size_str:
            try:
                batch_size = int(batch_size_str)
                if batch_size <= 0:
                    batch_size = 200
            except ValueError:
                pass
            
        # Handle max retries
        max_retries_str = request.args.get('max_retries')
        max_retries = 3  # Default
        if max_retries_str:
            try:
                max_retries = int(max_retries_str)
                if max_retries <= 0:
                    max_retries = 3
            except ValueError:
                pass
                
        # Log the parameters
        logger.info(f"Starting risk stats job with parameters: debug={debug_mode}, "
                   f"use_test_file={use_test_file}, batch_size={batch_size}, max_retries={max_retries}")
        
        # Create a job record
        with get_db_connection() as db:
            result = create_risk_stats_job(
                db=db,
                use_test_file=use_test_file,
                debug_mode=debug_mode,
                batch_size=batch_size,
                max_retries=max_retries
            )
            
            if not result.get("success", False):
                return jsonify(result), 500
                
            job_id = result.get("job_id")
            
            # Start the job
            start_result = start_risk_stats_job(job_id)
            
            if not start_result.get("success", False):
                return jsonify(start_result), 500
                
            # Return the combined result
            return jsonify({
                "success": True,
                "job_id": job_id,
                "status": "pending",
                "message": "Risk statistics update job started successfully."
            })
            
    except Exception as e:
        logger.exception(f"Error starting risk stats job: {e}")
        return jsonify({
            "success": False,
            "error": f"Failed to start risk statistics update: {str(e)}"
        }), 500
        

def get_risk_stats_job_status(job_id):
    """
    Get the status of a risk statistics job.
    
    Path parameters:
    - job_id: ID of the job to check
    
    Returns:
        JSON with the job status and details
    """
    try:
        with get_db_connection() as db:
            result = get_risk_stats_job(db, job_id)
            
            if not result.get("success", False):
                return jsonify(result), 404
                
            return jsonify(result)
            
    except Exception as e:
        logger.exception(f"Error getting job status: {e}")
        return jsonify({
            "success": False,
            "error": f"Failed to get job status: {str(e)}"
        }), 500


def get_risk_stats_status():
    """
    Get the status of risk statistics data.
    
    Returns information about when risk stats were last updated and how many records are available.
    
    Returns:
        JSON with the status of risk statistics data
    """
    try:
        with get_db_connection() as db:
            # Get the latest import date
            latest_date = db.query(func.max(EgnyteRiskStat.import_date)).scalar()
            
            # Get the most recent job
            latest_job = db.query(RiskStatsJob).filter(
                RiskStatsJob.status == JobStatus.COMPLETED.value
            ).order_by(RiskStatsJob.completed_at.desc()).first()
            
            if latest_date:
                # Count records by asset class
                equity_count = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.import_date == latest_date,
                    EgnyteRiskStat.asset_class == 'Equity'
                ).count()
                
                fixed_income_count = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.import_date == latest_date,
                    EgnyteRiskStat.asset_class == 'Fixed Income'
                ).count()
                
                alternatives_count = db.query(EgnyteRiskStat).filter(
                    EgnyteRiskStat.import_date == latest_date,
                    EgnyteRiskStat.asset_class == 'Alternatives'
                ).count()
                
                total_count = equity_count + fixed_income_count + alternatives_count
                
                # Get cache statistics
                cache_stats = RISK_STATS_CACHE.get_stats()
                
                # Include job metrics if available
                job_metrics = {}
                if latest_job:
                    job_metrics = {
                        "job_id": latest_job.id,
                        "job_created_at": latest_job.created_at.isoformat() if latest_job.created_at else None,
                        "job_completed_at": latest_job.completed_at.isoformat() if latest_job.completed_at else None,
                        "job_duration_seconds": latest_job.duration_seconds,
                        "job_memory_usage_mb": latest_job.memory_usage_mb
                    }
                
                return jsonify({
                    "success": True,
                    "has_data": True,
                    "latest_import_date": latest_date.isoformat(),
                    "total_records": total_count,
                    "equity_records": equity_count,
                    "fixed_income_records": fixed_income_count,
                    "alternatives_records": alternatives_count,
                    "cache": {
                        "size": cache_stats.get("size", 0),
                        "hit_ratio": cache_stats.get("hit_ratio", 0),
                        "hits": cache_stats.get("hits", 0),
                        "misses": cache_stats.get("misses", 0)
                    },
                    **job_metrics
                })
            else:
                # No data available yet
                return jsonify({
                    "success": True,
                    "has_data": False,
                    "message": "No risk statistics data available yet"
                })
                
    except Exception as e:
        logger.exception(f"Error getting risk stats status: {e}")
        return jsonify({
            "success": False,
            "error": f"Failed to get risk statistics status: {str(e)}"
        }), 500


def get_risk_stats_data():
    """
    Get risk statistics from the database with optimized query handling.
    
    Query parameters:
    - asset_class: Filter to specific asset class - 'Equity', 'Fixed Income', or 'Alternatives'
    - second_level: Filter to specific second level category
    - position: Filter to specific position/security
    - ticker: Filter to specific ticker symbol
    - cusip: Filter to specific CUSIP
    - limit: Maximum number of records to return (default: 100)
    - offset: Number of records to skip (default: 0)
    
    Returns:
        JSON with risk statistics data
    """
    try:
        # Parse query parameters
        asset_class = request.args.get('asset_class')
        second_level = request.args.get('second_level')
        position = request.args.get('position')
        ticker = request.args.get('ticker')
        cusip = request.args.get('cusip')
        
        # Add pagination support with sensible defaults
        try:
            limit = int(request.args.get('limit', '100'))
            offset = int(request.args.get('offset', '0'))
        except ValueError:
            limit = 100
            offset = 0
        
        # Cap limit to prevent excessive data transfer
        if limit > 1000:
            limit = 1000
        
        with get_db_connection() as db:
            # Get the latest import date
            latest_date = db.query(func.max(EgnyteRiskStat.import_date)).scalar()
            
            if not latest_date:
                return jsonify({
                    "success": True,
                    "records": [],
                    "count": 0,
                    "message": "No risk statistics data available yet"
                })
            
            # Base query - always filter by latest date
            query = db.query(EgnyteRiskStat).filter(EgnyteRiskStat.import_date == latest_date)
            
            # Apply specific identifier filters
            if position:
                query = query.filter(EgnyteRiskStat.position.ilike(f"%{position}%"))
                
            if ticker:
                query = query.filter(EgnyteRiskStat.ticker_symbol.ilike(f"%{ticker}%"))
                
            if cusip:
                query = query.filter(EgnyteRiskStat.cusip.ilike(f"%{cusip}%"))
            
            # Apply categorical filters
            if asset_class:
                query = query.filter(EgnyteRiskStat.asset_class == asset_class)
            
            if second_level:
                query = query.filter(EgnyteRiskStat.second_level == second_level)
            
            # Get total count for pagination
            total_count = query.count()
            
            # Apply pagination
            records = query.order_by(EgnyteRiskStat.position).limit(limit).offset(offset).all()
            
            # Convert to list of dictionaries
            result = []
            for record in records:
                result.append({
                    "id": record.id,
                    "import_date": record.import_date.isoformat(),
                    "position": record.position,
                    "ticker_symbol": record.ticker_symbol,
                    "cusip": record.cusip,
                    "asset_class": record.asset_class,
                    "second_level": record.second_level,
                    "volatility": float(record.volatility) if record.volatility is not None else None,
                    "beta": float(record.beta) if record.beta is not None else None,
                    "duration": float(record.duration) if record.duration is not None else None,
                    "bloomberg_id": record.bloomberg_id,
                    "notes": record.notes,
                    "amended_id": record.amended_id
                })
            
            return jsonify({
                "success": True,
                "records": result,
                "count": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + limit) < total_count
            })
            
    except Exception as e:
        logger.exception(f"Error retrieving risk statistics: {e}")
        return jsonify({
            "success": False,
            "error": f"Failed to retrieve risk statistics: {str(e)}"
        }), 500