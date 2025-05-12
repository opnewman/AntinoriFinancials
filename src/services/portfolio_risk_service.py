"""
Service for calculating portfolio risk metrics using risk statistics from Egnyte.

This service takes securities in a portfolio, matches them with risk statistics,
and calculates weighted risk metrics (beta, volatility, duration) by asset class.
"""
import logging
import re
import signal
import time
from contextlib import contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set
import copy

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.models.models import (
    FinancialPosition, 
    RiskStatisticEquity,
    RiskStatisticFixedIncome,
    RiskStatisticAlternatives
)

logger = logging.getLogger(__name__)

# Keep track of securities that don't have matching risk statistics
# This is used for reporting purposes
UNMATCHED_SECURITIES = {
    "Equity": set(),
    "Fixed Income": set(),
    "Alternatives": set(),
    "Hard Currency": set()
}

class TimeoutException(Exception):
    """Custom exception for query timeout"""
    pass

# Thread-safe alternative to signal-based timeouts
def with_timeout(func, args=None, kwargs=None, timeout_duration=10, default=None):
    """
    Thread-safe function for enforcing timeouts on function calls.
    
    Args:
        func: Function to call
        args: Arguments to pass to the function (list or tuple)
        kwargs: Keyword arguments to pass to the function (dict)
        timeout_duration: Maximum execution time in seconds
        default: Default return value if timeout occurs
        
    Returns:
        Function result or default value if timeout occurs
        
    Raises:
        TimeoutException if default is None and timeout occurs
    """
    import threading
    import time
    
    args = args or []
    kwargs = kwargs or {}
    result = [default]
    exception = [None]
    
    def worker():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=worker)
    thread.daemon = True
    thread.start()
    
    thread.join(timeout_duration)
    if thread.is_alive():
        if default is not None:
            logger.warning(f"Function call timed out after {timeout_duration} seconds, returning default value")
            return default
        else:
            logger.warning(f"Function call timed out after {timeout_duration} seconds, raising TimeoutException")
            raise TimeoutException(f"Function timed out after {timeout_duration} seconds")
    
    if exception[0]:
        raise exception[0]
    
    return result[0]

def convert_position_value_to_decimal(position_value: Any, position_name: Any = "unknown") -> Decimal:
    """
    Convert a position value to Decimal, handling various types and formats.
    
    This function handles:
    - Encrypted values with "ENC:" prefix
    - Various string formats including "$1,234.56"
    - Float/int values
    - None values
    - Edge cases like "nan" or empty strings
    
    Args:
        position_value: The position value (may be string, float, int, or encrypted)
        position_name: The name of the position (for logging purposes)
        
    Returns:
        Decimal value of the position
    """
    # Import the encryption service
    from src.utils.encryption import encryption_service
    
    # Handle None values
    if position_value is None:
        return Decimal('0.0')
    
    # Handle encrypted values with "ENC:" prefix
    if isinstance(position_value, str) and position_value.startswith('ENC:'):
        try:
            # Use the encryption service to decrypt the value
            decrypted_value = encryption_service.decrypt_to_float(position_value)
            # Convert float to string first to ensure Decimal precision
            return Decimal(str(decrypted_value))
        except Exception as e:
            logger.warning(f"Could not decrypt position value {position_value} for {position_name}: {str(e)}")
            return Decimal('0.0')
    
    # Handle non-string types
    if not isinstance(position_value, str):
        try:
            # Always convert to string first to avoid float precision issues
            return Decimal(str(position_value))
        except (ValueError, TypeError, ArithmeticError, InvalidOperation) as e:
            logger.warning(f"Could not convert position value {position_value} for {position_name} to Decimal: {e}")
            return Decimal('0.0')
    
    # Process string values
    try:
        # Remove any non-numeric characters except for a decimal point
        clean_value = position_value.replace('$', '').replace(',', '').strip()
        
        # Handle common edge cases
        if clean_value == '':
            return Decimal('0.0')
        elif 'nan' in clean_value.lower() or 'n/a' in clean_value.lower():
            return Decimal('0.0')
        elif '*' in clean_value:  # Handle encrypted values
            return Decimal('0.0')
        elif '%' in clean_value:  # Handle percentage values
            # Remove the % and convert percentage to decimal
            clean_value = clean_value.replace('%', '')
            percentage = Decimal(clean_value)
            return percentage / Decimal(100)
            
        # Try to convert to Decimal
        return Decimal(clean_value)
    except (ValueError, TypeError, ArithmeticError, InvalidOperation) as e:
        logger.warning(f"Could not convert position value {position_value} for {position_name} to Decimal: {e}")
        return Decimal('0.0')

def calculate_portfolio_risk_metrics(
    db: Session,
    level: str,
    level_key: str,
    report_date: date,
    max_positions: Optional[int] = None  # No limit on positions by default
) -> Dict[str, Any]:
    """
    Calculate risk metrics for a portfolio based on its positions.
    
    Args:
        db (Session): Database session
        level (str): Level for analysis - 'client', 'portfolio', or 'account'
        level_key (str): The identifier for the specified level
        report_date (date): The date for the report
        max_positions (int): Maximum number of positions to process (for performance)
        
    Returns:
        Dict[str, Any]: Risk metrics for the portfolio, organized by asset class
    """
    # Clear the unmatched securities list
    global UNMATCHED_SECURITIES
    UNMATCHED_SECURITIES = {
        "Equity": set(),
        "Fixed Income": set(),
        "Alternatives": set(),
        "Hard Currency": set()
    }
    
    # Initialize result structure
    risk_metrics = {
        "equity": {
            "beta": {
                "weighted_sum": Decimal('0.0'),
                "value": None,
                "coverage_pct": Decimal('0.0')
            },
            "volatility": {
                "weighted_sum": Decimal('0.0'),
                "value": None,
                "coverage_pct": Decimal('0.0')
            }
        },
        "fixed_income": {
            "duration": {
                "weighted_sum": Decimal('0.0'),
                "value": None,
                "coverage_pct": Decimal('0.0'),
                "category": None
            }
        },
        "hard_currency": {
            "beta": {
                "weighted_sum": Decimal('0.0'),
                "value": None,
                "coverage_pct": Decimal('0.0')
            }
        },
        "alternatives": {
            "beta": {
                "weighted_sum": Decimal('0.0'),
                "value": None,
                "coverage_pct": Decimal('0.0')
            }
        }
    }
    
    # Get all positions for this portfolio
    try:
        # Construct the filter based on the level
        filter_params = {}
        if level == 'client':
            filter_params = {'top_level_client': level_key}
        elif level == 'portfolio':
            filter_params = {'portfolio': level_key}
        elif level == 'account':
            filter_params = {'holding_account_number': level_key}
        else:
            raise ValueError(f"Invalid level: {level}")
            
        # Add the date filter
        filter_params['date'] = report_date
        
        # Query for positions
        positions_query = db.query(FinancialPosition).filter_by(**filter_params)
        
        # Apply max_positions limit if specified
        if max_positions:
            positions_query = positions_query.limit(max_positions)
            
        positions = positions_query.all()
        logger.info(f"Found {len(positions)} positions for {level} {level_key} on {report_date}")
    except Exception as e:
        logger.error(f"Error getting positions: {str(e)}")
        positions = []
    
    if not positions:
        logger.warning(f"No positions found for {level} {level_key} on {report_date}")
        return risk_metrics
    
    # Calculate total values by asset class
    totals = {
        "equity": Decimal('0.0'),
        "fixed_income": Decimal('0.0'),
        "hard_currency": Decimal('0.0'),
        "alternatives": Decimal('0.0'),
        "cash": Decimal('0.0'),
        "other": Decimal('0.0')
    }
    
    # Get the most recent risk statistics date
    latest_risk_stats_date = None
    try:
        # Try to find the most recent date across all risk statistic tables
        equity_date = db.query(func.max(RiskStatisticEquity.upload_date)).scalar()
        fixed_income_date = db.query(func.max(RiskStatisticFixedIncome.upload_date)).scalar()
        alternatives_date = db.query(func.max(RiskStatisticAlternatives.upload_date)).scalar()
        
        # Use the most recent date that's available
        dates = [d for d in [equity_date, fixed_income_date, alternatives_date] if d is not None]
        if dates:
            latest_risk_stats_date = max(dates)
        
        if not latest_risk_stats_date:
            logger.warning("No risk statistics available - using current date as fallback")
            latest_risk_stats_date = date.today()
    except Exception as e:
        logger.error(f"Error getting latest risk stats date: {str(e)}")
        latest_risk_stats_date = date.today()
        
    # Calculate totals for each asset class
    for position in positions:
        asset_class = position.asset_class
        value = convert_position_value_to_decimal(position.adjusted_value, position.position)
        
        if not asset_class:
            continue
            
        asset_class_lower = asset_class.lower()
        
        if "equity" in asset_class_lower:
            totals["equity"] += value
        elif "fixed" in asset_class_lower:
            totals["fixed_income"] += value
        elif "hard" in asset_class_lower and "currency" in asset_class_lower:
            totals["hard_currency"] += value
        elif "alternative" in asset_class_lower:
            totals["alternatives"] += value
        elif "cash" in asset_class_lower:
            totals["cash"] += value
        else:
            totals["other"] += value
    
    # Create a risk statistics cache to reduce database queries
    risk_stats_cache = {}
    
    # Set a timeout limit per asset class processing (45 seconds per asset class to handle larger portfolios)
    MAX_PROCESSING_TIME = 45  # seconds
    
    # Process each asset class separately with individual timeouts for better resilience
    # Process equity positions
    logger.info(f"Processing equity risk metrics for {level} {level_key}")
    try:
        # Use the thread-safe timeout function
        with_timeout(
            func=process_equity_risk,
            args=(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache),
            timeout_duration=MAX_PROCESSING_TIME
        )
        logger.info("Equity risk processing completed successfully")
    except TimeoutException:
        logger.warning(f"Equity risk processing timed out after {MAX_PROCESSING_TIME} seconds")
        logger.warning("Using partial equity risk metrics")
    except Exception as e:
        logger.error(f"Error processing equity risk metrics: {str(e)}")
    
    # Process fixed income positions
    logger.info(f"Processing fixed income risk metrics for {level} {level_key}")
    try:
        # Use the thread-safe timeout function
        with_timeout(
            func=process_fixed_income_risk,
            args=(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache),
            timeout_duration=MAX_PROCESSING_TIME
        )
        logger.info("Fixed income risk processing completed successfully")
    except TimeoutException:
        logger.warning(f"Fixed income risk processing timed out after {MAX_PROCESSING_TIME} seconds")
        logger.warning("Using partial fixed income risk metrics")
    except Exception as e:
        logger.error(f"Error processing fixed income risk metrics: {str(e)}")
    
    # Process hard currency positions
    logger.info(f"Processing hard currency risk metrics for {level} {level_key}")
    try:
        # Use the thread-safe timeout function
        with_timeout(
            func=process_hard_currency_risk,
            args=(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache),
            timeout_duration=MAX_PROCESSING_TIME
        )
        logger.info("Hard currency risk processing completed successfully")
    except TimeoutException:
        logger.warning(f"Hard currency risk processing timed out after {MAX_PROCESSING_TIME} seconds")
        logger.warning("Using partial hard currency risk metrics")
    except Exception as e:
        logger.error(f"Error processing hard currency risk metrics: {str(e)}")
    
    # Process alternatives positions
    logger.info(f"Processing alternatives risk metrics for {level} {level_key}")
    try:
        # Use the thread-safe timeout function
        with_timeout(
            func=process_alternatives_risk,
            args=(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache),
            timeout_duration=MAX_PROCESSING_TIME
        )
        logger.info("Alternatives risk processing completed successfully")
    except TimeoutException:
        logger.warning(f"Alternatives risk processing timed out after {MAX_PROCESSING_TIME} seconds")
        logger.warning("Using partial alternatives risk metrics")
    except Exception as e:
        logger.error(f"Error processing alternatives risk metrics: {str(e)}")
    
    # Calculate percentages of each asset class
    total_value = sum(totals.values())
    percentages = {}
    for asset_class, value in totals.items():
        if total_value > Decimal('0.0'):
            percentages[asset_class] = (value / total_value) * 100
        else:
            percentages[asset_class] = Decimal('0.0')
    
    # Finalize risk metrics (convert weighted sums to actual values)
    finalize_risk_metrics(risk_metrics, percentages)
    
    # Add totals to the result
    risk_metrics["totals"] = totals
    risk_metrics["percentages"] = percentages
    
    return risk_metrics

def process_equity_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """
    Process equity positions to calculate weighted beta and volatility.
    
    This optimized implementation:
    1. Pre-filters positions to equity only
    2. Adds better error handling for beta and volatility calculations
    3. Improves logging for debugging 
    """
    if not cache:
        cache = {}
        
    # Pre-filter positions to equity only for better performance
    equity_positions = [
        p for p in positions 
        if p.asset_class and "equity" in p.asset_class.lower()
    ]
    
    # Early return if no equity positions
    if not equity_positions:
        logger.info("No equity positions found for beta/volatility calculations")
        return
        
    logger.info(f"Processing {len(equity_positions)} equity positions for beta and volatility")
    
    # Initialize counters
    matched_value = Decimal('0.0')
    beta_matched = 0
    volatility_matched = 0
    
    # Process each equity position
    for position in equity_positions:
        # Convert position value with proper error handling
        position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
        
        # Skip positions with zero value
        if position_value <= Decimal('0.0'):
            continue
            
        # Find matching risk statistic with optimized matching for equity
        risk_stat = find_matching_risk_stat(
            db, 
            position.position, 
            position.cusip, 
            position.ticker_symbol, 
            "Equity", 
            latest_risk_stats_date,
            cache
        )
        
        if risk_stat is not None:
            # Process beta calculation
            beta_value = risk_stat.get("beta") if isinstance(risk_stat, dict) else None
            if beta_value is not None:
                try:
                    # Ensure we're working with Decimal for all calculations
                    beta = Decimal(str(beta_value))
                    
                    # Calculate weighted beta (safely handle division by zero)
                    if totals["equity"] > Decimal('0.0'):
                        weighted_beta = (beta * position_value) / totals["equity"]
                    else:
                        weighted_beta = Decimal('0.0')
                        
                    # Update weighted sum
                    risk_metrics["equity"]["beta"]["weighted_sum"] += weighted_beta
                    matched_value += position_value
                    beta_matched += 1
                except (ValueError, TypeError) as e:
                    # Handle any conversion errors safely
                    logger.warning(f"Error processing beta for {position.position}: {e}")
                    
            # Process volatility calculation  
            volatility_value = risk_stat.get("volatility") if isinstance(risk_stat, dict) else None
            if volatility_value is not None:
                try:
                    # Ensure we're working with Decimal for all calculations
                    volatility = Decimal(str(volatility_value))
                    
                    # Calculate weighted volatility (safely handle division by zero)
                    if totals["equity"] > Decimal('0.0'):
                        weighted_volatility = (volatility * position_value) / totals["equity"]
                    else:
                        weighted_volatility = Decimal('0.0')
                        
                    # Update weighted sum
                    risk_metrics["equity"]["volatility"]["weighted_sum"] += weighted_volatility
                    volatility_matched += 1
                except (ValueError, TypeError) as e:
                    # Handle any conversion errors safely
                    logger.warning(f"Error processing volatility for {position.position}: {e}")
    
    # Calculate coverage percentages
    coverage = Decimal('0.0')
    if totals["equity"] > Decimal('0.0'):
        coverage = (matched_value / totals["equity"]) * 100
        risk_metrics["equity"]["beta"]["coverage_pct"] = coverage
        risk_metrics["equity"]["volatility"]["coverage_pct"] = coverage
        
    logger.info(f"Equity processing complete with {coverage:.2f}% coverage. Beta matches: {beta_matched}, Volatility matches: {volatility_matched}")

def process_fixed_income_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """
    Process fixed income positions to calculate weighted duration.
    
    This optimized implementation:
    1. Pre-filters positions to only fixed income to reduce iterations
    2. Implements better error handling for duration calculations
    3. Adds extra logging for debugging
    """
    if not cache:
        cache = {}
        
    # Pre-filter positions to fixed income only to reduce iterations
    fixed_income_positions = [
        p for p in positions 
        if p.asset_class and "fixed" in p.asset_class.lower()
    ]
    
    # Early return if no fixed income positions
    if not fixed_income_positions:
        logger.info("No fixed income positions found for duration calculations")
        return
        
    logger.info(f"Processing {len(fixed_income_positions)} fixed income positions for duration")
    
    # Initialize counters
    matched_value = Decimal('0.0')
    
    # Process each fixed income position
    for position in fixed_income_positions:
        # Convert position value with proper error handling
        position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
        
        # Skip positions with zero value
        if position_value <= Decimal('0.0'):
            continue
            
        # Find matching risk statistic with optimized matching for fixed income
        risk_stat = find_matching_risk_stat(
            db, 
            position.position, 
            position.cusip, 
            position.ticker_symbol, 
            "Fixed Income", 
            latest_risk_stats_date,
            cache
        )
        
        if risk_stat is not None:
            # Extract duration with type safety
            duration_value = risk_stat.get("duration") if isinstance(risk_stat, dict) else None
            
            if duration_value is not None:
                try:
                    # Ensure we're working with Decimal for all calculations
                    duration = Decimal(str(duration_value))
                    
                    # Calculate weighted duration (safely handle division by zero)
                    if totals["fixed_income"] > Decimal('0.0'):
                        weighted_duration = (duration * position_value) / totals["fixed_income"]
                    else:
                        weighted_duration = Decimal('0.0')
                        
                    # Update weighted sum and matched value
                    risk_metrics["fixed_income"]["duration"]["weighted_sum"] += weighted_duration
                    matched_value += position_value
                    
                except (ValueError, TypeError) as e:
                    # Handle any conversion errors safely
                    logger.warning(f"Error processing duration for {position.position}: {e}")
    
    # Calculate coverage percentage
    coverage = Decimal('0.0')
    if totals["fixed_income"] > Decimal('0.0'):
        coverage = (matched_value / totals["fixed_income"]) * 100
        risk_metrics["fixed_income"]["duration"]["coverage_pct"] = coverage
        
    logger.info(f"Fixed income duration processing complete with {coverage:.2f}% coverage")

def process_hard_currency_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """Process hard currency positions to calculate weighted beta."""
    matched_value = Decimal('0.0')
    
    for position in positions:
        asset_class = position.asset_class
        if not asset_class or not ("hard" in asset_class.lower() and "currency" in asset_class.lower()):
            continue
            
        position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
        
        # Skip positions with zero value
        if position_value <= Decimal('0.0'):
            continue
            
        # Find matching risk statistic - hard currency risk stats are in the Alternatives table
        risk_stat = find_matching_risk_stat(
            db, 
            position.position, 
            position.cusip, 
            position.ticker_symbol, 
            "Hard Currency", 
            latest_risk_stats_date,
            cache
        )
        
        if risk_stat is not None:
            # We found a match - update weighted sums
            beta_value = risk_stat.get("beta") if isinstance(risk_stat, dict) else None
            if beta_value is not None:
                # Ensure we're working with Decimal for all calculations
                beta = Decimal(str(beta_value))
                
                # Safe division
                if totals["hard_currency"] > Decimal('0.0'):
                    weighted_beta = (beta * position_value) / totals["hard_currency"]
                else:
                    weighted_beta = Decimal('0.0')
                    
                risk_metrics["hard_currency"]["beta"]["weighted_sum"] += weighted_beta
                matched_value += position_value
    
    # Calculate coverage percentages
    if totals["hard_currency"] > Decimal('0.0'):
        coverage = (matched_value / totals["hard_currency"]) * 100
        risk_metrics["hard_currency"]["beta"]["coverage_pct"] = coverage

def process_alternatives_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """Process alternatives positions to calculate weighted beta."""
    matched_value = Decimal('0.0')
    
    for position in positions:
        asset_class = position.asset_class
        if not asset_class or "alternative" not in asset_class.lower():
            continue
            
        position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
        
        # Skip positions with zero value
        if position_value <= Decimal('0.0'):
            continue
            
        # Find matching risk statistic
        risk_stat = find_matching_risk_stat(
            db, 
            position.position, 
            position.cusip, 
            position.ticker_symbol, 
            "Alternatives", 
            latest_risk_stats_date,
            cache
        )
        
        if risk_stat is not None:
            # We found a match - update weighted sums
            beta_value = risk_stat.get("beta") if isinstance(risk_stat, dict) else None
            if beta_value is not None:
                # Ensure we're working with Decimal for all calculations
                beta = Decimal(str(beta_value))
                
                # Safe division
                if totals["alternatives"] > Decimal('0.0'):
                    weighted_beta = (beta * position_value) / totals["alternatives"]
                else:
                    weighted_beta = Decimal('0.0')
                    
                risk_metrics["alternatives"]["beta"]["weighted_sum"] += weighted_beta
                matched_value += position_value
    
    # Calculate coverage percentages
    if totals["alternatives"] > Decimal('0.0'):
        coverage = (matched_value / totals["alternatives"]) * 100
        risk_metrics["alternatives"]["beta"]["coverage_pct"] = coverage

def find_matching_risk_stat(
    db: Session,
    position_name: Any,
    cusip: Optional[Any],
    ticker_symbol: Optional[Any], 
    asset_class: str,
    latest_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> Optional[Any]:
    """
    Optimized asset-class specific matching logic to find risk statistics.
    
    Different asset classes are matched differently:
    - Fixed Income: primarily by CUSIP (most reliable for bonds)
    - Equity: primarily by ticker_symbol, then position name
    - Hard Currency/Alternatives: mix of CUSIP, ticker, and position name
    
    Args:
        db (Session): Database session
        position_name (str): Name of the position/security
        cusip (Optional[str]): CUSIP identifier if available
        ticker_symbol (Optional[str]): Ticker symbol if available
        asset_class (str): Asset class to match ('Equity', 'Fixed Income', 'Alternatives')
        latest_date (date): Latest date of risk stat upload
        cache (Optional[Dict[str, Any]]): Optional cache of risk stats
        
    Returns:
        Optional[Any]: Matching risk statistic or None
    """
    # Safety check for inputs - fail fast
    if not position_name and not cusip and not ticker_symbol:
        return None
        
    if not asset_class:
        return None
    
    # Determine model class with simple keyword matching
    model_class = None
    asset_class_str = str(asset_class).lower() if asset_class else ""
    
    if "equity" in asset_class_str:
        model_class = RiskStatisticEquity
    elif "fixed" in asset_class_str:
        model_class = RiskStatisticFixedIncome
    elif "alternative" in asset_class_str or ("hard" in asset_class_str and "currency" in asset_class_str):
        model_class = RiskStatisticAlternatives
    else:
        return None
    
    # Ultra-safe string sanitization function
    def ultra_sanitize(value):
        """Sanitize a value to prevent encoding errors and DB issues"""
        if value is None:
            return ""
            
        try:
            # Convert to string first
            if not isinstance(value, str):
                value = str(value)
                
            # Encode and decode to handle any encoding issues
            clean_value = value.encode('ascii', errors='ignore').decode('ascii', errors='ignore')
            
            # Remove any characters that might cause problems
            clean_value = re.sub(r'[^\w\s\.\-]', '', clean_value)
            
            # Normalize whitespace and trim
            clean_value = re.sub(r'\s+', ' ', clean_value).strip()
            
            return clean_value
        except Exception as e:
            # If all else fails, return empty string
            logger.warning(f"Sanitization error: {str(e)}")
            return ""
    
    # Sanitize all inputs to prevent any possible encoding issues
    safe_position = ultra_sanitize(position_name).lower()
    safe_cusip = ultra_sanitize(cusip)
    safe_ticker = ultra_sanitize(ticker_symbol).lower()
    
    # Use more specific cache keys to improve hit rates
    cache_prefix = asset_class_str.replace(" ", "_")
    
    # Check cache first - no database access needed
    if cache is not None:
        # Try all identifiers with specific prefixes to avoid collisions
        if safe_cusip and f"{cache_prefix}:cusip:{safe_cusip}" in cache:
            return cache[f"{cache_prefix}:cusip:{safe_cusip}"]
            
        if safe_ticker and f"{cache_prefix}:ticker:{safe_ticker}" in cache:
            return cache[f"{cache_prefix}:ticker:{safe_ticker}"]
            
        if safe_position and f"{cache_prefix}:position:{safe_position}" in cache:
            return cache[f"{cache_prefix}:position:{safe_position}"]
    
    # Create a function for all database queries to reduce code duplication
    def execute_query(condition, identifier_type, identifier_value):
        """Execute a database query with proper connection handling and caching"""
        # Check cache first for faster retrievals
        if cache is not None:
            cache_key = f"{cache_prefix}:{identifier_type}:{identifier_value}"
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
        # Avoid connection issues by using a direct connection instead of session
        try:
            # Import required modules
            from sqlalchemy import create_engine, text
            import os
            
            # Get the database URL from environment
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                logger.error("DATABASE_URL not set")
                return None
            
            # Create a new engine for this specific query with minimal settings
            engine = create_engine(
                database_url,
                echo=False,
                pool_pre_ping=True,
                connect_args={"connect_timeout": 3}
            )
            
            try:
                # Create a custom SQL query based on the table type to handle different column structures
                table_name = model_class.__tablename__
                
                # Determine the appropriate columns based on the asset class
                if 'fixed_income' in table_name:
                    # Fixed income has duration but no beta or volatility
                    columns = ["id", "duration"]
                elif 'equity' in table_name:
                    # Equity has beta and volatility
                    columns = ["id", "beta", "volatility"]
                elif 'alternatives' in table_name or 'hard_currency' in table_name:
                    # Alternatives/hard currency have beta and volatility
                    columns = ["id", "beta", "volatility"]
                else:
                    # Default fallback
                    columns = ["id"]
                
                # Sanitize the identifier value to prevent SQL injection
                if isinstance(identifier_value, str):
                    identifier_value = identifier_value.replace("'", "''")
                
                # Use the correct column for matching based on the table structure
                # Check if the column exists in the table before using it
                valid_columns = ["position", "cusip"]
                
                # Only use ticker_symbol if that's what we're searching by
                if identifier_type == "ticker":
                    identifier_type = "ticker_symbol"
                
                # Make sure we're using a valid column name
                if identifier_type not in valid_columns and identifier_type != "ticker_symbol":
                    identifier_type = "position"  # Default to position if invalid column
                
                # Build appropriate SQL based on match type
                if identifier_type == "position" and "%" in identifier_value:
                    # This is a LIKE query for partial position matching
                    sql = f"""
                        SELECT {', '.join(columns)}
                        FROM {table_name}
                        WHERE LOWER({identifier_type}) LIKE LOWER('{identifier_value}')
                        AND upload_date = '{latest_date}'
                        LIMIT 1
                    """
                else:
                    # This is an exact match query
                    sql = f"""
                        SELECT {', '.join(columns)}
                        FROM {table_name}
                        WHERE LOWER({identifier_type}) = LOWER('{identifier_value}')
                        AND upload_date = '{latest_date}'
                        LIMIT 1
                    """
                
                # Execute the raw SQL directly
                with engine.connect() as connection:
                    # Use the text function to safely execute SQL
                    result = connection.execute(text(sql))
                    risk_stat = result.fetchone()
                
                # Process results if any
                if risk_stat is not None:
                    # Convert to dictionary with only needed fields
                    risk_dict = {}
                    
                    # Always include id if it exists
                    if risk_stat[0] is not None:
                        risk_dict['id'] = risk_stat[0]
                    
                    # Include beta if not None
                    if len(risk_stat) > 1 and risk_stat[1] is not None:
                        risk_dict['beta'] = risk_stat[1]
                    
                    # Include volatility if not None 
                    if len(risk_stat) > 2 and risk_stat[2] is not None:
                        risk_dict['volatility'] = risk_stat[2]
                    
                    # Include duration if not None (for fixed income)
                    if len(risk_stat) > 3 and risk_stat[3] is not None:
                        risk_dict['duration'] = risk_stat[3]
                    
                    # Cache the result if cache is available
                    if cache is not None:
                        cache_key = f"{cache_prefix}:{identifier_type}:{identifier_value}"
                        cache[cache_key] = risk_dict
                    
                    return risk_dict
                    
                return None
                
            except Exception as e:
                # Log and continue to next query method
                logger.warning(f"Query error ({identifier_type}={identifier_value}): {str(e)}")
                return None
            finally:
                # Close engine to avoid connection pooling issues
                engine.dispose()
                
        except Exception as outer_e:
            logger.error(f"Database connection error: {str(outer_e)}")
            return None
    
    # Use different search strategies based on asset class
    table_name = model_class.__tablename__
    
    if 'fixed_income' in table_name:
        # Fixed Income: prioritize CUSIP for bonds, then try position name
        if safe_cusip:
            result = execute_query(True, "cusip", safe_cusip)
            if result:
                return result
                
        if safe_position:
            # Try exact position match
            result = execute_query(True, "position", safe_position)
            if result:
                return result
                
            # For bond names, which are often long and complex, try partial matches
            words = safe_position.split()
            # Try first three words for bonds (typically includes issuer name)
            if len(words) >= 3:
                first_three = f"{words[0]} {words[1]} {words[2]}"
                result = execute_query(True, "position", f"%{first_three}%")
                if result:
                    return result
            
            # Try just first word as last resort
            if words and len(words[0]) >= 3:
                result = execute_query(True, "position", f"%{words[0]}%")
                if result:
                    return result
                    
        if safe_ticker and safe_ticker != '-':
            result = execute_query(True, "ticker_symbol", safe_ticker)
            if result:
                return result
    
    elif 'equity' in table_name:
        # Equity: prioritize ticker symbol, which is most reliable for stocks
        if safe_ticker:
            result = execute_query(True, "ticker_symbol", safe_ticker)
            if result:
                return result
                
        if safe_cusip:
            result = execute_query(True, "cusip", safe_cusip)
            if result:
                return result
                
        if safe_position:
            # Try exact position name
            result = execute_query(True, "position", safe_position)
            if result:
                return result
                
            # For equity names, first word is often the company name
            words = safe_position.split()
            if words and len(words[0]) >= 3:
                result = execute_query(True, "position", f"%{words[0]}%")
                if result:
                    return result
    
    else:  # Alternatives or Hard Currency
        # Try CUSIP first for alternatives
        if safe_cusip:
            result = execute_query(True, "cusip", safe_cusip)
            if result:
                return result
                
        # For alternatives, ticker symbols are also reliable
        if safe_ticker and safe_ticker != '-':
            result = execute_query(True, "ticker_symbol", safe_ticker)
            if result:
                return result
                
        # Then try position name with various strategies
        if safe_position:
            # Try exact position match
            result = execute_query(True, "position", safe_position)
            if result:
                return result
                
            # For alternatives, try first two words (often fund name)
            words = safe_position.split()
            if len(words) >= 2:
                first_two = f"{words[0]} {words[1]}"
                result = execute_query(True, "position", f"%{first_two}%")
                if result:
                    return result
                    
            # Try just first word if it's significant
            if words and len(words[0]) >= 3:
                result = execute_query(True, "position", f"%{words[0]}%")
                if result:
                    return result
    
    # Track unmatched securities for reporting
    if position_name:
        track_unmatched_security(position_name, asset_class)
        
    # No match found
    return None

def get_unmatched_securities():
    """
    Return a dictionary of securities that couldn't be matched with risk statistics.
    This is useful for identifying which securities need risk data.
    
    Returns:
        Dict[str, List[str]]: Dictionary with asset classes as keys and lists of 
                             unmatched security names as values
    """
    global UNMATCHED_SECURITIES
    
    result = {}
    for asset_class, securities in UNMATCHED_SECURITIES.items():
        result[asset_class] = list(securities)
    
    return result

def track_unmatched_security(position_name, asset_class):
    """
    Track securities that don't have matching risk statistics
    
    Args:
        position_name (str): Name of the security
        asset_class (str): Asset class of the security
    """
    global UNMATCHED_SECURITIES
    
    if not position_name or not asset_class:
        return
        
    asset_class_str = str(asset_class).lower()
    
    if "equity" in asset_class_str:
        UNMATCHED_SECURITIES["Equity"].add(str(position_name))
    elif "fixed" in asset_class_str:
        UNMATCHED_SECURITIES["Fixed Income"].add(str(position_name))
    elif "hard" in asset_class_str and "currency" in asset_class_str:
        UNMATCHED_SECURITIES["Hard Currency"].add(str(position_name))
    elif "alternative" in asset_class_str:
        UNMATCHED_SECURITIES["Alternatives"].add(str(position_name))

def finalize_risk_metrics(risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]], percentages: Dict[str, Decimal]) -> None:
    """
    Finalize risk metrics by calculating actual values from weighted sums and coverage.
    Also calculates beta-adjusted values based on asset class percentages.
    
    Args:
        risk_metrics (Dict): Risk metrics dictionary with weighted sums and coverage percentages
        percentages (Dict): Percentages of each asset class in the total portfolio
    """
    # Equity - calculate final beta and volatility
    if "weighted_sum" in risk_metrics["equity"]["beta"] and risk_metrics["equity"]["beta"]["weighted_sum"] > Decimal('0.0'):
        risk_metrics["equity"]["beta"]["value"] = risk_metrics["equity"]["beta"]["weighted_sum"]
    else:
        risk_metrics["equity"]["beta"]["value"] = Decimal('0.0')
        
    if "weighted_sum" in risk_metrics["equity"]["volatility"] and risk_metrics["equity"]["volatility"]["weighted_sum"] > Decimal('0.0'):
        risk_metrics["equity"]["volatility"]["value"] = risk_metrics["equity"]["volatility"]["weighted_sum"]
    else:
        risk_metrics["equity"]["volatility"]["value"] = Decimal('0.0')
        
    # Fixed Income - calculate final duration and categorize it
    if "weighted_sum" in risk_metrics["fixed_income"]["duration"] and risk_metrics["fixed_income"]["duration"]["weighted_sum"] > Decimal('0.0'):
        duration_value = risk_metrics["fixed_income"]["duration"]["weighted_sum"]
        risk_metrics["fixed_income"]["duration"]["value"] = duration_value
        
        # Categorize duration
        if duration_value < Decimal('2.0'):
            risk_metrics["fixed_income"]["duration"]["category"] = Decimal('1.0')  # short_duration
        elif duration_value < Decimal('7.0'):
            risk_metrics["fixed_income"]["duration"]["category"] = Decimal('2.0')  # market_duration
        else:
            risk_metrics["fixed_income"]["duration"]["category"] = Decimal('3.0')  # long_duration
            
    else:
        risk_metrics["fixed_income"]["duration"]["value"] = Decimal('0.0')
        risk_metrics["fixed_income"]["duration"]["category"] = Decimal('0.0')  # unknown
        
    # Hard Currency - calculate final beta
    if "weighted_sum" in risk_metrics["hard_currency"]["beta"] and risk_metrics["hard_currency"]["beta"]["weighted_sum"] > Decimal('0.0'):
        risk_metrics["hard_currency"]["beta"]["value"] = risk_metrics["hard_currency"]["beta"]["weighted_sum"]
    else:
        risk_metrics["hard_currency"]["beta"]["value"] = Decimal('0.0')
        
    # Alternatives - calculate final beta
    if "weighted_sum" in risk_metrics["alternatives"]["beta"] and risk_metrics["alternatives"]["beta"]["weighted_sum"] > Decimal('0.0'):
        risk_metrics["alternatives"]["beta"]["value"] = risk_metrics["alternatives"]["beta"]["weighted_sum"]
    else:
        risk_metrics["alternatives"]["beta"]["value"] = Decimal('0.0')
        
    # Calculate overall portfolio beta
    portfolio_beta = Decimal('0.0')
    
    # Add equity contribution to portfolio beta
    if "value" in risk_metrics["equity"]["beta"] and risk_metrics["equity"]["beta"]["value"] > Decimal('0.0'):
        equity_pct = percentages.get("equity", Decimal('0.0')) / Decimal('100.0') if "equity" in percentages else Decimal('0.0')
        portfolio_beta += risk_metrics["equity"]["beta"]["value"] * equity_pct
        
    # Add hard currency contribution to portfolio beta
    if "value" in risk_metrics["hard_currency"]["beta"] and risk_metrics["hard_currency"]["beta"]["value"] > Decimal('0.0'):
        hard_currency_pct = percentages.get("hard_currency", Decimal('0.0')) / Decimal('100.0') if "hard_currency" in percentages else Decimal('0.0')
        portfolio_beta += risk_metrics["hard_currency"]["beta"]["value"] * hard_currency_pct
        
    # Add alternatives contribution to portfolio beta
    if "value" in risk_metrics["alternatives"]["beta"] and risk_metrics["alternatives"]["beta"]["value"] > Decimal('0.0'):
        alternatives_pct = percentages.get("alternatives", Decimal('0.0')) / Decimal('100.0') if "alternatives" in percentages else Decimal('0.0')
        portfolio_beta += risk_metrics["alternatives"]["beta"]["value"] * alternatives_pct
    
    # Create a new dictionary for portfolio level metrics to avoid type errors
    portfolio_metrics = {"beta": portfolio_beta}
    
    # Store the overall portfolio beta in a new key to avoid type errors
    risk_metrics["portfolio"] = portfolio_metrics