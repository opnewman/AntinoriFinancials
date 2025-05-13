"""
Service to precalculate portfolio risk metrics and store them for later quick retrieval.

This service is triggered whenever new data is uploaded (new data dump or risk stats)
to precalculate all portfolio reports for all clients, portfolios, and accounts.
"""

import logging
import datetime
import threading
import time
from typing import List, Dict, Any, Optional
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import text
import json

from src.models.models import FinancialPosition, PrecalculatedRiskMetric
from src.database import get_db
from src.services.portfolio_risk_service import calculate_portfolio_risk_metrics
from src.services.portfolio_report_service import generate_portfolio_report

# Configure logging
logger = logging.getLogger(__name__)

def get_all_entities(db: Session, report_date: datetime.date) -> Dict[str, List[str]]:
    """
    Get all clients, portfolios, and accounts for a given date.
    
    Args:
        db: Database session
        report_date: The date to get entities for
        
    Returns:
        Dictionary with entity types as keys and lists of entity IDs as values
    """
    # Ensure report_date is a date object
    if isinstance(report_date, str):
        report_date = datetime.datetime.strptime(report_date, '%Y-%m-%d').date()
    try:
        # Query distinct clients
        client_query = text("""
            SELECT DISTINCT top_level_client 
            FROM financial_positions 
            WHERE date = :date AND top_level_client IS NOT NULL
            ORDER BY top_level_client
        """)
        
        # Query distinct portfolios
        portfolio_query = text("""
            SELECT DISTINCT portfolio 
            FROM financial_positions 
            WHERE date = :date AND portfolio IS NOT NULL
            ORDER BY portfolio
        """)
        
        # Query distinct accounts
        account_query = text("""
            SELECT DISTINCT holding_account_number 
            FROM financial_positions 
            WHERE date = :date AND holding_account_number IS NOT NULL
            ORDER BY holding_account_number
        """)
        
        # Execute queries
        clients = [row[0] for row in db.execute(client_query, {"date": report_date}).fetchall()]
        portfolios = [row[0] for row in db.execute(portfolio_query, {"date": report_date}).fetchall()]
        accounts = [row[0] for row in db.execute(account_query, {"date": report_date}).fetchall()]
        
        # Return all entities
        return {
            "clients": clients,
            "portfolios": portfolios,
            "accounts": accounts
        }
    except Exception as e:
        logger.error(f"Error getting entities: {str(e)}")
        return {"clients": [], "portfolios": [], "accounts": []}

def process_entity(db: Session, level: str, level_key: str, report_date: datetime.date, timeout: int = 30, max_positions: int = 500) -> None:
    """
    Process a single entity (client, portfolio, or account) and store its risk metrics.
    
    Args:
        db: Database session
        level: 'client', 'portfolio', or 'account'
        level_key: The identifier for the specified level
        report_date: The report date
        timeout: Maximum processing time in seconds
        max_positions: Maximum number of positions to process (for performance)
    """
    # Ensure report_date is a date object
    if isinstance(report_date, str):
        report_date = datetime.datetime.strptime(report_date, '%Y-%m-%d').date()
    
    # Skip All Clients to avoid timeout
    if level == "client" and level_key == "All Clients":
        logger.info(f"Skipping 'All Clients' to avoid timeout during precalculation")
        return
        
    # Use longer timeout for larger clients
    if level == "client" and level_key in [" The Linden East II Trust (Abigail Wexner)"]:
        timeout = 60  # Use longer timeout for known large clients
        max_positions = 1000  # Use higher position limit
        
    logger.info(f"Precalculating {level} report for {level_key} (timeout: {timeout}s, max_positions: {max_positions})")
    start_time = time.time()
    
    try:
        # Get the most recent risk metrics entry for this entity
        existing = db.query(PrecalculatedRiskMetric).filter_by(
            level=level,
            level_key=level_key,
            report_date=report_date
        ).first()
        
        # Calculate risk metrics with timeout protection
        result_queue = []
        calculation_thread = threading.Thread(
            target=lambda: result_queue.append(calculate_portfolio_risk_metrics(
                db=db,
                level=level,
                level_key=level_key,
                report_date=report_date,
                max_positions=max_positions
            ))
        )
        
        # Start calculation thread
        calculation_thread.daemon = True
        calculation_thread.start()
        
        # Wait for thread to complete or timeout
        calculation_thread.join(timeout=timeout)
        
        # Check if calculation completed
        if not result_queue:
            logger.warning(f"Calculation for {level} {level_key} timed out after {timeout} seconds")
            return
            
        risk_metrics = result_queue[0]
        if not risk_metrics:
            logger.warning(f"No risk metrics returned for {level} {level_key}")
            return
            
        # Convert Decimal to float for JSON storage
        risk_metrics_json = convert_decimal_to_float(risk_metrics)
        
        # Store or update precalculated metrics
        try:
            if existing:
                existing.risk_metrics = json.dumps(risk_metrics_json)
                existing.last_updated = datetime.datetime.now()
            else:
                new_entry = PrecalculatedRiskMetric(
                    level=level,
                    level_key=level_key,
                    report_date=report_date,
                    risk_metrics=json.dumps(risk_metrics_json),
                    last_updated=datetime.datetime.now()
                )
                db.add(new_entry)
                
            # Commit changes
            db.commit()
            
            end_time = time.time()
            logger.info(f"Precalculated {level} report for {level_key} in {end_time - start_time:.2f} seconds")
        except Exception as e:
            db.rollback()
            logger.error(f"Database error storing risk metrics for {level} {level_key}: {str(e)}")
    except Exception as e:
        db.rollback()
        logger.error(f"Error precalculating {level} report for {level_key}: {str(e)}")

def convert_decimal_to_float(data: Any) -> Any:
    """
    Recursively convert Decimal values to float for JSON serialization.
    
    Args:
        data: The data structure to convert
        
    Returns:
        Data structure with Decimal values converted to float
    """
    if isinstance(data, dict):
        return {k: convert_decimal_to_float(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_to_float(item) for item in data]
    elif isinstance(data, Decimal):
        return float(data)
    else:
        return data

def get_most_recent_date(db: Session) -> Optional[datetime.date]:
    """
    Get the most recent date with financial position data.
    
    Args:
        db: Database session
        
    Returns:
        Most recent date or None if no data found
    """
    result = db.query(FinancialPosition.date).order_by(
        FinancialPosition.date.desc()
    ).first()
    
    if result:
        return result[0]
    else:
        logger.error("No financial position data found")
        return None

def precalculate_all_reports(report_date: Optional[datetime.date] = None) -> None:
    """
    Precalculate reports for all entities as of the given date.
    
    Args:
        report_date: The date to calculate reports for (defaults to most recent date)
    """
    # Get the most recent date if none provided
    if not report_date:
        with next(get_db()) as db:
            most_recent = get_most_recent_date(db)
            if not most_recent:
                return
            report_date = most_recent
    
    logger.info(f"Precalculating all reports for date {report_date}")
    start_time = time.time()
    
    with next(get_db()) as db:
        # Get all entities
        entities = get_all_entities(db, report_date)
        
        total_entities = (
            len(entities["clients"]) + 
            len(entities["portfolios"]) + 
            len(entities["accounts"])
        )
        
        if total_entities == 0:
            logger.warning(f"No entities found for date {report_date}")
            return
            
        logger.info(f"Found {len(entities['clients'])} clients, "
                    f"{len(entities['portfolios'])} portfolios, and "
                    f"{len(entities['accounts'])} accounts")
        
        # Process clients with progress tracking
        client_count = len(entities["clients"])
        for i, client in enumerate(entities["clients"], 1):
            if i % 10 == 0:
                logger.info(f"Processing client {i}/{client_count} ({(i/client_count)*100:.1f}%)")
            process_entity(db, "client", client, report_date)
            
        # Process portfolios with progress tracking
        portfolio_count = len(entities["portfolios"])
        for i, portfolio in enumerate(entities["portfolios"], 1):
            if i % 10 == 0:
                logger.info(f"Processing portfolio {i}/{portfolio_count} ({(i/portfolio_count)*100:.1f}%)")
            process_entity(db, "portfolio", portfolio, report_date)
            
        # Process accounts with progress tracking
        account_count = len(entities["accounts"])
        for i, account in enumerate(entities["accounts"], 1):
            if i % 10 == 0:
                logger.info(f"Processing account {i}/{account_count} ({(i/account_count)*100:.1f}%)")
            process_entity(db, "account", account, report_date)
    
    end_time = time.time()
    logger.info(f"Precalculation completed in {end_time - start_time:.2f} seconds")

def trigger_precalculation(report_date: Optional[datetime.date] = None) -> None:
    """
    Trigger precalculation in a background thread.
    
    Args:
        report_date: The date to calculate reports for (defaults to most recent date)
    """
    logger.info("Starting background precalculation")
    thread = threading.Thread(target=precalculate_all_reports, args=(report_date,))
    thread.daemon = True
    thread.start()
    # No return value needed for this function