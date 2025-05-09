"""
API endpoints for the direct risk statistics processing.
"""

import os
import time
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from src.database import get_db
from src.models.models import RiskStatisticEquity, RiskStatisticFixedIncome, RiskStatisticAlternatives
from src.services.risk_stats_direct_service import process_risk_stats_direct

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/update-direct")
def update_risk_stats_direct(
    debug: bool = Query(False, description="Enable debug mode"),
    use_test_file: bool = Query(False, description="Use test file instead of downloading from Egnyte"),
    batch_size: int = Query(500, description="Batch size for database operations"),
    db: Session = Depends(get_db)
):
    """
    Update risk statistics directly using separate tables for each asset class.
    
    This endpoint uses a completely different approach from the original risk stats update,
    with much better error handling and performance characteristics.
    
    Query parameters:
    - debug: Enable debug mode (default: false)
    - use_test_file: Use test file instead of downloading from Egnyte (default: false)
    - batch_size: Batch size for database operations (default: 500)
    
    Returns:
        JSON with processing results and timing information
    """
    try:
        # Enable debugging
        if debug:
            logger.setLevel(logging.DEBUG)
            
        # Log API call
        logger.info(f"Direct risk stats update called with debug={debug}, use_test_file={use_test_file}, batch_size={batch_size}")
        
        # Check Egnyte token if not using test file
        if not use_test_file and not os.environ.get('EGNYTE_ACCESS_TOKEN'):
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "Egnyte API token not found. Please set the EGNYTE_ACCESS_TOKEN environment variable."
                }
            )
        
        # Track start time
        start_time = time.time()
        
        # Process the risk statistics
        results = process_risk_stats_direct(
            db=db,
            use_test_file=use_test_file,
            batch_size=batch_size,
            debug=debug
        )
        
        # Track total API time
        total_time = time.time() - start_time
        results["total_api_time_seconds"] = total_time
        
        # Return results
        return JSONResponse(
            status_code=200,
            content=results
        )
        
    except Exception as e:
        logger.error(f"Error in risk stats direct API: {e}")
        logger.error(f"Error details: {traceback.format_exc()}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )

@router.get("/status")
def get_risk_stats_status(db: Session = Depends(get_db)):
    """
    Get the status of risk statistics across all asset classes.
    
    Returns:
        JSON with status information including record counts and last update date
    """
    try:
        # Get record counts
        equity_count = db.query(RiskStatisticEquity).count()
        fixed_income_count = db.query(RiskStatisticFixedIncome).count()
        alternatives_count = db.query(RiskStatisticAlternatives).count()
        total_count = equity_count + fixed_income_count + alternatives_count
        
        # Get latest update date
        latest_equity_date = db.query(RiskStatisticEquity.upload_date).order_by(
            RiskStatisticEquity.upload_date.desc()
        ).first()
        
        latest_fixed_income_date = db.query(RiskStatisticFixedIncome.upload_date).order_by(
            RiskStatisticFixedIncome.upload_date.desc()
        ).first()
        
        latest_alternatives_date = db.query(RiskStatisticAlternatives.upload_date).order_by(
            RiskStatisticAlternatives.upload_date.desc()
        ).first()
        
        # Determine the overall latest date
        latest_dates = []
        if latest_equity_date:
            latest_dates.append(latest_equity_date[0])
        if latest_fixed_income_date:
            latest_dates.append(latest_fixed_income_date[0])
        if latest_alternatives_date:
            latest_dates.append(latest_alternatives_date[0])
            
        latest_date = max(latest_dates) if latest_dates else None
        
        # Return status information
        return {
            "success": True,
            "total_records": total_count,
            "equity_records": equity_count,
            "fixed_income_records": fixed_income_count,
            "alternatives_records": alternatives_count,
            "last_updated": latest_date.isoformat() if latest_date else None,
            "has_data": total_count > 0
        }
        
    except Exception as e:
        logger.error(f"Error getting risk stats status: {e}")
        logger.error(f"Error details: {traceback.format_exc()}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )