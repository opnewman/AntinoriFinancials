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
from decimal import Decimal
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
    Convert a position value string to Decimal, handling encrypted values 
    and various invalid formats.
    
    Args:
        position_value: The position value as a string (may be encrypted or have formatting)
        position_name: The name of the position (for logging purposes)
        
    Returns:
        Decimal value of the position
    """
    # Import the encryption service
    from src.utils.encryption import encryption_service
    
    if position_value is None:
        return Decimal('0.0')
    
    # Handle encrypted values with "ENC:" prefix
    if isinstance(position_value, str) and position_value.startswith('ENC:'):
        try:
            # Use the encryption service to decrypt the value
            decrypted_value = encryption_service.decrypt_to_float(position_value)
            return Decimal(str(decrypted_value))
        except Exception as e:
            logger.warning(f"Could not decrypt position value {position_value} for {position_name}: {str(e)}")
            return Decimal('0.0')
    
    # Convert to string first if it's not already
    if not isinstance(position_value, str):
        try:
            return Decimal(str(position_value))
        except (ValueError, TypeError, ArithmeticError):
            logger.warning(f"Could not convert position value {position_value} for {position_name} to Decimal")
            return Decimal('0.0')
    
    # Handle different formats and edge cases
    try:
        # Remove any non-numeric characters except for a decimal point
        position_value = position_value.replace('$', '').replace(',', '')
        
        # Handling common problematic patterns found in the data
        if position_value.strip() == '':
            return Decimal('0.0')
        elif 'nan' in position_value.lower():
            return Decimal('0.0')
        elif '*' in position_value:  # Handle encrypted values
            return Decimal('0.0')
            
        # Try to convert to Decimal
        return Decimal(position_value)
    except (ValueError, TypeError, ArithmeticError):
        logger.warning(f"Could not convert position value {position_value} for {position_name} to Decimal")
        return Decimal('0.0')

def calculate_portfolio_risk_metrics(
    db: Session,
    level: str,
    level_key: str,
    report_date: date,
    max_positions: Optional[int] = 100  # Default to 100 positions
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
    
    # Set a timeout limit for the entire risk metric calculation (10 seconds max)
    MAX_PROCESSING_TIME = 10  # seconds
    
    # Use our thread-safe timeout implementation instead of signals
    def process_all_risk_metrics():
        # Process positions by asset class
        logger.info(f"Processing equity risk metrics for {level} {level_key}")
        process_equity_risk(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache)
        
        logger.info(f"Processing fixed income risk metrics for {level} {level_key}")
        process_fixed_income_risk(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache)
        
        logger.info(f"Processing hard currency risk metrics for {level} {level_key}")
        process_hard_currency_risk(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache)
        
        logger.info(f"Processing alternatives risk metrics for {level} {level_key}")
        process_alternatives_risk(db, positions, totals, risk_metrics, latest_risk_stats_date, risk_stats_cache)
    
    # Execute with timeout
    try:
        # Use the thread-safe timeout function
        with_timeout(
            func=process_all_risk_metrics,
            timeout_duration=MAX_PROCESSING_TIME
        )
    except TimeoutException:
        logger.warning(f"Risk metrics calculation timed out after {MAX_PROCESSING_TIME} seconds")
        # Continue with partial results
        logger.warning("Using partial risk metrics calculation results")
    except Exception as e:
        logger.error(f"Error calculating risk metrics: {str(e)}")
        # Keep going with what we have
    
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
    """Process equity positions to calculate weighted beta and volatility."""
    matched_value = Decimal('0.0')
    
    for position in positions:
        asset_class = position.asset_class
        if not asset_class or "equity" not in asset_class.lower():
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
            "Equity", 
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
                if totals["equity"] > Decimal('0.0'):
                    weighted_beta = (beta * position_value) / totals["equity"]
                else:
                    weighted_beta = Decimal('0.0')
                    
                risk_metrics["equity"]["beta"]["weighted_sum"] += weighted_beta
                matched_value += position_value
                
            volatility_value = risk_stat.get("volatility") if isinstance(risk_stat, dict) else None
            if volatility_value is not None:
                # Ensure we're working with Decimal for all calculations
                volatility = Decimal(str(volatility_value))
                
                # Safe division
                if totals["equity"] > Decimal('0.0'):
                    weighted_volatility = (volatility * position_value) / totals["equity"]
                else:
                    weighted_volatility = Decimal('0.0')
                    
                risk_metrics["equity"]["volatility"]["weighted_sum"] += weighted_volatility
    
    # Calculate coverage percentages
    if totals["equity"] > Decimal('0.0'):
        coverage = (matched_value / totals["equity"]) * 100
        risk_metrics["equity"]["beta"]["coverage_pct"] = coverage
        risk_metrics["equity"]["volatility"]["coverage_pct"] = coverage

def process_fixed_income_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """Process fixed income positions to calculate weighted duration."""
    matched_value = Decimal('0.0')
    
    for position in positions:
        asset_class = position.asset_class
        if not asset_class or "fixed" not in asset_class.lower():
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
            "Fixed Income", 
            latest_risk_stats_date,
            cache
        )
        
        if risk_stat is not None:
            # We found a match - update weighted sums
            duration_value = risk_stat.get("duration") if isinstance(risk_stat, dict) else None
            if duration_value is not None:
                # Ensure we're working with Decimal for all calculations
                duration = Decimal(str(duration_value))
                
                # Safe division
                if totals["fixed_income"] > Decimal('0.0'):
                    weighted_duration = (duration * position_value) / totals["fixed_income"]
                else:
                    weighted_duration = Decimal('0.0')
                    
                risk_metrics["fixed_income"]["duration"]["weighted_sum"] += weighted_duration
                matched_value += position_value
    
    # Calculate coverage percentages
    if totals["fixed_income"] > Decimal('0.0'):
        coverage = (matched_value / totals["fixed_income"]) * 100
        risk_metrics["fixed_income"]["duration"]["coverage_pct"] = coverage

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
    Find a matching risk statistic for a position using different identifiers.
    
    Ultra-optimized implementation to avoid memory issues, connection errors,
    and timeout problems. Uses multiple efficient matching strategies with
    proper connection and cursor management.
    
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
                # Create simple SQL query with parameters
                table_name = model_class.__tablename__
                
                # Determine column names
                columns = ["id", "beta", "volatility"]
                if hasattr(model_class, 'duration'):
                    columns.append("duration")
                
                # Sanitize the identifier value even further to prevent SQL injection
                if isinstance(identifier_value, str):
                    identifier_value = identifier_value.replace("'", "''")
                
                # Build a simple SQL query to avoid ORM overhead
                sql = f"""
                    SELECT {', '.join(columns)}
                    FROM {table_name}
                    WHERE {identifier_type} = '{identifier_value}'
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
    
    # Try lookup methods in order of reliability
    
    # 1. Try by CUSIP (most reliable identifier)
    if safe_cusip:
        result = execute_query(model_class.cusip == safe_cusip, "cusip", safe_cusip)
        if result:
            return result
    
    # 2. Try by ticker symbol (next most reliable)
    if safe_ticker:
        result = execute_query(func.lower(model_class.ticker_symbol) == safe_ticker, "ticker", safe_ticker)
        if result:
            return result
    
    # 3. Try by exact position name match
    if safe_position:
        result = execute_query(func.lower(model_class.position) == safe_position, "position", safe_position)
        if result:
            return result
            
        # 4. Try by simplest position name
        # Extract first significant word to improve matching
        simple_words = [w for w in safe_position.split() if len(w) > 3]
        if simple_words:
            simple_word = simple_words[0]
            # Only try if we have a meaningful word
            if len(simple_word) > 3:
                result = execute_query(
                    func.lower(model_class.position).like(f"%{simple_word}%"), 
                    "position_simple", 
                    simple_word
                )
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
        risk_metrics["equity"]["beta"]["value"] = None
        
    if "weighted_sum" in risk_metrics["equity"]["volatility"] and risk_metrics["equity"]["volatility"]["weighted_sum"] > Decimal('0.0'):
        risk_metrics["equity"]["volatility"]["value"] = risk_metrics["equity"]["volatility"]["weighted_sum"]
    else:
        risk_metrics["equity"]["volatility"]["value"] = None
        
    # Fixed Income - calculate final duration and categorize it
    if "weighted_sum" in risk_metrics["fixed_income"]["duration"] and risk_metrics["fixed_income"]["duration"]["weighted_sum"] > Decimal('0.0'):
        duration_value = risk_metrics["fixed_income"]["duration"]["weighted_sum"]
        risk_metrics["fixed_income"]["duration"]["value"] = duration_value
        
        # Categorize duration
        if duration_value < 2:
            risk_metrics["fixed_income"]["duration"]["category"] = "short_duration"
        elif duration_value < 7:
            risk_metrics["fixed_income"]["duration"]["category"] = "market_duration"
        else:
            risk_metrics["fixed_income"]["duration"]["category"] = "long_duration"
            
    else:
        risk_metrics["fixed_income"]["duration"]["value"] = None
        risk_metrics["fixed_income"]["duration"]["category"] = "unknown"
        
    # Hard Currency - calculate final beta
    if "weighted_sum" in risk_metrics["hard_currency"]["beta"] and risk_metrics["hard_currency"]["beta"]["weighted_sum"] > Decimal('0.0'):
        risk_metrics["hard_currency"]["beta"]["value"] = risk_metrics["hard_currency"]["beta"]["weighted_sum"]
    else:
        risk_metrics["hard_currency"]["beta"]["value"] = None
        
    # Alternatives - calculate final beta
    if "weighted_sum" in risk_metrics["alternatives"]["beta"] and risk_metrics["alternatives"]["beta"]["weighted_sum"] > Decimal('0.0'):
        risk_metrics["alternatives"]["beta"]["value"] = risk_metrics["alternatives"]["beta"]["weighted_sum"]
    else:
        risk_metrics["alternatives"]["beta"]["value"] = None
        
    # Calculate overall portfolio beta
    portfolio_beta = Decimal('0.0')
    
    # Add equity contribution to portfolio beta
    if "value" in risk_metrics["equity"]["beta"] and risk_metrics["equity"]["beta"]["value"] is not None:
        equity_pct = percentages.get("equity", Decimal('0.0')) / 100 if "equity" in percentages else Decimal('0.0')
        portfolio_beta += risk_metrics["equity"]["beta"]["value"] * equity_pct
        
    # Add hard currency contribution to portfolio beta
    if "value" in risk_metrics["hard_currency"]["beta"] and risk_metrics["hard_currency"]["beta"]["value"] is not None:
        hard_currency_pct = percentages.get("hard_currency", Decimal('0.0')) / 100 if "hard_currency" in percentages else Decimal('0.0')
        portfolio_beta += risk_metrics["hard_currency"]["beta"]["value"] * hard_currency_pct
        
    # Add alternatives contribution to portfolio beta
    if "value" in risk_metrics["alternatives"]["beta"] and risk_metrics["alternatives"]["beta"]["value"] is not None:
        alternatives_pct = percentages.get("alternatives", Decimal('0.0')) / 100 if "alternatives" in percentages else Decimal('0.0')
        portfolio_beta += risk_metrics["alternatives"]["beta"]["value"] * alternatives_pct
        
    # Store the overall portfolio beta
    risk_metrics["portfolio_beta"] = portfolio_beta