"""
Service for calculating portfolio risk metrics using risk statistics from Egnyte.

This service takes securities in a portfolio, matches them with risk statistics,
and calculates weighted risk metrics (beta, volatility, duration) by asset class.
"""
import logging
import re
import signal
import time
import traceback
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

# Import our optimized risk stat matching implementation
try:
    from optimized_find_matching_risk_stat_implementation import find_matching_risk_stat as optimized_find_matching_risk_stat
    USE_OPTIMIZED_MATCHING = True
    logger.info("Using optimized risk stat matching implementation")
except ImportError:
    USE_OPTIMIZED_MATCHING = False
    logger.warning("Optimized risk stat matching implementation not available, using standard implementation")

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
    
    This function handles all database operations within a try-except block to ensure
    that any errors are properly caught and transactions are cleanly handled.
    
    Args:
        db (Session): Database session
        level (str): Level for analysis - 'client', 'portfolio', or 'account'
        level_key (str): The identifier for the specified level
        report_date (date): The date for the report
        max_positions (int): Maximum number of positions to process (for performance)
        
    Returns:
        Dict[str, Any]: Risk metrics for the portfolio, organized by asset class
    """
    # Initialize the risk metrics structure
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
    
    # Use error handling to manage database operations
    try:
        # Clear the unmatched securities list
        global UNMATCHED_SECURITIES
        UNMATCHED_SECURITIES = {
            "Equity": set(),
            "Fixed Income": set(),
            "Alternatives": set(),
            "Hard Currency": set()
        }
        
        logger.info(f"Processing risk metrics for {level} {level_key}")
        
        # Get all positions for this portfolio
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
    # The cache has the structure: {asset_class: {identifier_type: {identifier_value: risk_stat}}}
    risk_stats_cache = {
        "equity": {"cusip": {}, "ticker_symbol": {}, "position": {}},
        "fixed_income": {"cusip": {}, "ticker_symbol": {}, "position": {}},
        "alternatives": {"cusip": {}, "ticker_symbol": {}, "position": {}},
        "hard_currency": {"cusip": {}, "ticker_symbol": {}, "position": {}}
    }
    
    # Set a timeout limit per asset class processing (60 seconds per asset class to handle larger portfolios)
    MAX_PROCESSING_TIME = 60  # seconds
    
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
    
    # Create properly structured dictionaries for totals and percentages
    asset_totals = {}
    asset_percentages = {}
    
    # Convert flat dictionaries to nested ones to match expected structure
    for asset_class, value in totals.items():
        asset_totals[asset_class] = {"value": value}
    
    for asset_class, pct in percentages.items():
        asset_percentages[asset_class] = {"value": pct}
    
    # Add the structured totals and percentages to the result
    risk_metrics["totals"] = asset_totals
    risk_metrics["percentages"] = asset_percentages
    
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
    
    High-performance implementation that:
    1. Pre-loads all risk stats in a single database query
    2. Uses in-memory lookups instead of individual database queries
    3. Processes positions in a single pass
    4. Provides detailed performance metrics
    """
    import time
    start_time = time.time()
    
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
        
    position_count = len(equity_positions)
    logger.info(f"Processing {position_count} equity positions for beta and volatility")
    
    # Check if we already have equity risk stats in cache
    if 'equity' not in cache or cache.get('equity', {}).get('preloaded') != True:
        # Preload ALL equity risk stats in a single query for maximum performance
        preload_start = time.time()
        logger.info(f"Preloading equity risk stats data from database")
        
        try:
            # Extract all identifiers we need to look up
            tickers = set()
            cusips = set()
            names = set()
            
            # Collect all identifiers for a single query
            for position in equity_positions:
                if position.ticker_symbol:
                    tickers.add(position.ticker_symbol.lower())
                if position.cusip:
                    cusips.add(position.cusip.lower())
                if position.position:
                    names.add(position.position.lower())
            
            # Execute a single optimized query to get all risk stats at once
            from sqlalchemy import or_, func, text
            
            # Build the query with all possible matches
            query = db.query(RiskStatisticEquity).filter(
                RiskStatisticEquity.upload_date == latest_risk_stats_date
            )
            
            # Use chunks to avoid query parameter limits
            ticker_chunks = list(chunk_list(list(tickers), 100))
            cusip_chunks = list(chunk_list(list(cusips), 100))
            
            # Create empty risk stats cache if needed
            if 'equity' not in cache:
                cache['equity'] = {
                    'ticker_symbol': {},
                    'cusip': {},
                    'position': {},
                    'preloaded': True
                }
                
            # Process in chunks to avoid query size limits
            for ticker_chunk in ticker_chunks:
                if ticker_chunk:
                    ticker_query = query.filter(func.lower(RiskStatisticEquity.ticker_symbol).in_(ticker_chunk))
                    ticker_results = ticker_query.all()
                    logger.info(f"Loaded {len(ticker_results)} equity risk stats for {len(ticker_chunk)} tickers")
                    
                    # Store in memory cache
                    for risk_stat in ticker_results:
                        if risk_stat.ticker_symbol:
                            ticker_key = risk_stat.ticker_symbol.lower()
                            # Get volatility from either field (try both column names)
                            volatility_value = None
                            try:
                                if hasattr(risk_stat, 'volatility') and risk_stat.volatility is not None:
                                    volatility_value = risk_stat.volatility
                                elif hasattr(risk_stat, 'vol') and risk_stat.vol is not None:
                                    volatility_value = risk_stat.vol
                                
                                logger.debug(f"Equity volatility value: ticker={ticker_key}, vol={volatility_value}, has_vol_field={hasattr(risk_stat, 'vol')}, has_volatility_field={hasattr(risk_stat, 'volatility')}")
                            except Exception as e:
                                logger.error(f"Error accessing volatility field: {str(e)}")
                            
                            cache['equity']['ticker_symbol'][ticker_key] = {
                                'id': risk_stat.id,
                                'beta': risk_stat.beta if risk_stat.beta is not None else None,
                                'volatility': volatility_value
                            }
            
            for cusip_chunk in cusip_chunks:
                if cusip_chunk:
                    cusip_query = query.filter(func.lower(RiskStatisticEquity.cusip).in_(cusip_chunk))
                    cusip_results = cusip_query.all()
                    logger.info(f"Loaded {len(cusip_results)} equity risk stats for {len(cusip_chunk)} cusips")
                    
                    # Store in memory cache
                    for risk_stat in cusip_results:
                        if risk_stat.cusip:
                            cusip_key = risk_stat.cusip.lower()
                            
                            # Get volatility from either field (try both column names)
                            volatility_value = None
                            try:
                                if hasattr(risk_stat, 'volatility') and risk_stat.volatility is not None:
                                    volatility_value = risk_stat.volatility
                                elif hasattr(risk_stat, 'vol') and risk_stat.vol is not None:
                                    volatility_value = risk_stat.vol
                                
                                logger.debug(f"Equity volatility value: cusip={cusip_key}, vol={volatility_value}")
                            except Exception as e:
                                logger.error(f"Error accessing volatility field: {str(e)}")
                            
                            cache['equity']['cusip'][cusip_key] = {
                                'id': risk_stat.id,
                                'beta': risk_stat.beta if risk_stat.beta is not None else None,
                                'volatility': volatility_value
                            }
            
            # For position names, use a direct SQL query with more efficient LIKE operations
            if names:
                # Only process position names if we didn't find enough matches
                if len(cache['equity']['ticker_symbol']) + len(cache['equity']['cusip']) < len(equity_positions) / 2:
                    logger.info(f"Searching by position names for remaining matches")
                    
                    # Use a more flexible matching approach for position names
                    for position_name in names:
                        if not position_name or len(position_name) < 4:
                            continue
                            
                        # Look for exact and partial matches
                        position_query = query.filter(
                            func.lower(RiskStatisticEquity.position) == position_name
                        )
                        exact_match = position_query.first()
                        
                        if exact_match:
                            # Get volatility from either field (try both column names)
                            volatility_value = None
                            try:
                                if hasattr(exact_match, 'volatility') and exact_match.volatility is not None:
                                    volatility_value = exact_match.volatility
                                elif hasattr(exact_match, 'vol') and exact_match.vol is not None:
                                    volatility_value = exact_match.vol
                                
                                logger.debug(f"Equity volatility value: position={position_name}, vol={volatility_value}")
                            except Exception as e:
                                logger.error(f"Error accessing volatility field: {str(e)}")
                            
                            cache['equity']['position'][position_name] = {
                                'id': exact_match.id,
                                'beta': exact_match.beta if exact_match.beta is not None else None,
                                'volatility': volatility_value
                            }
            
            preload_time = time.time() - preload_start
            logger.info(f"Preloaded {len(cache['equity']['ticker_symbol'])} ticker matches, {len(cache['equity']['cusip'])} CUSIP matches, and {len(cache['equity']['position'])} position name matches in {preload_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error preloading equity risk stats: {str(e)}")
            logger.error(f"TRACEBACK: {traceback.format_exc()}")
    else:
        logger.info("Using existing preloaded equity risk stats cache")
    
    # Initialize counters
    matched_value = Decimal('0.0')
    beta_matched = 0
    volatility_matched = 0
    processed_count = 0
    last_log_time = time.time()
    
    # Process each equity position using in-memory lookups
    for position in equity_positions:
        # Convert position value with proper error handling
        position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
        
        # Skip positions with zero value
        if position_value <= Decimal('0.0'):
            continue
        
        processed_count += 1
        
        # Log progress every 100 positions or 5 seconds
        if processed_count % 100 == 0 or (time.time() - last_log_time) > 5:
            progress_pct = (processed_count / position_count) * 100
            logger.info(f"Processed {processed_count}/{position_count} equity positions ({progress_pct:.1f}%)")
            last_log_time = time.time()
        
        # Find matching risk statistic using in-memory cache
        risk_stat = None
        match_source = None
        
        # Check ticker first (most reliable for equities)
        if position.ticker_symbol and position.ticker_symbol.lower() in cache.get('equity', {}).get('ticker_symbol', {}):
            risk_stat = cache['equity']['ticker_symbol'][position.ticker_symbol.lower()]
            match_source = "ticker"
        
        # Then check CUSIP
        elif position.cusip and position.cusip.lower() in cache.get('equity', {}).get('cusip', {}):
            risk_stat = cache['equity']['cusip'][position.cusip.lower()]
            match_source = "cusip"
        
        # Finally check position name
        elif position.position and position.position.lower() in cache.get('equity', {}).get('position', {}):
            risk_stat = cache['equity']['position'][position.position.lower()]
            match_source = "name"
        
        if risk_stat is not None:
            # Process beta calculation
            beta_value = risk_stat.get("beta")
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
                    
            # Process volatility calculation - try both field names
            volatility_value = risk_stat.get("volatility")
            if volatility_value is None:
                volatility_value = risk_stat.get("vol")  # Try alternative field name
                
            if volatility_value is not None:
                try:
                    # Log the actual volatility value we found
                    logger.debug(f"Found volatility {volatility_value} for {position.position} via {match_source}")
                    
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
            else:
                logger.debug(f"No volatility found for {position.position}")
    
    # Calculate coverage percentages
    coverage = Decimal('0.0')
    if totals["equity"] > Decimal('0.0'):
        coverage = (matched_value / totals["equity"]) * 100
        risk_metrics["equity"]["beta"]["coverage_pct"] = coverage
        risk_metrics["equity"]["volatility"]["coverage_pct"] = coverage
    
    total_time = time.time() - start_time
    logger.info(f"Equity processing complete in {total_time:.2f} seconds with {coverage:.2f}% coverage")
    logger.info(f"Beta matches: {beta_matched}, Volatility matches: {volatility_matched}")
    
# Helper function for chunking lists to handle large queries
def chunk_list(lst, chunk_size):
    """Split a list into chunks of specified size"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def process_fixed_income_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """
    Ultra-optimized fixed income processing for maximum performance.
    
    This implementation uses:
    1. Aggressive pre-caching with simplified queries
    2. Memory-based lookups to eliminate DB calls during processing
    3. Rapid batch operations with automatic failover
    4. Extensive diagnostic logging
    5. Hard timeout limits for every operation
    """
    if not cache:
        cache = {}
    
    # Log start time for overall performance tracking
    start_time = time.time()
    
    # Initialize fixed income risk stats cache directly - simplified structure
    cache_fi = cache.setdefault('fixed_income', {
        'cusip': {},
        'ticker_symbol': {},
        'position': {},
        'preloaded': False
    })
    
    # Pre-filter positions to fixed income only 
    try:
        logger.info("Starting fixed income risk calculation with timeout protection")
        fixed_income_positions = []
        position_count = 0
        
        # Use efficient filtering with early exit on potential timeout
        for p in positions:
            if p.asset_class and "fixed" in p.asset_class.lower():
                fixed_income_positions.append(p)
                position_count += 1
        
        if position_count == 0:
            logger.info("No fixed income positions found for duration calculations")
            return
            
        logger.info(f"Found {position_count} fixed income positions for processing")
        
        # Direct DB query with timeout protection to get all durations at once
        if not cache_fi.get('preloaded'):
            logger.info("Performing one-time preload of all fixed income risk stats")
            
            # We'll use raw SQL for maximum performance
            try:
                from src.models.models import RiskStatisticFixedIncome
                from sqlalchemy.sql import func, text
                
                # Create an empty dictionary to store results by ID
                duration_map = {}
                
                # Use a with_timeout wrapper to enforce a hard deadline
                def load_all_durations():
                    # Run an optimized query to get all durations in a single call
                    # This is significantly faster than multiple small queries
                    query = text("""
                        SELECT 
                            cusip, ticker_symbol, position, duration
                        FROM 
                            risk_statistic_fixed_income
                        WHERE 
                            upload_date <= :latest_date
                        ORDER BY 
                            upload_date DESC
                    """)
                    
                    # Execute the query with a timeout
                    result = db.execute(query, {"latest_date": latest_risk_stats_date})
                    
                    # Process all results at once
                    duration_records = result.fetchall()
                    logger.info(f"Loaded {len(duration_records)} fixed income duration records from database")
                    
                    # Build lookups for each identifier type
                    for record in duration_records:
                        cusip, ticker, position_name, duration = record
                        
                        # Create a simplified record structure
                        record_data = {'duration': duration}
                        
                        # Add to each lookup table
                        if cusip:
                            cache_fi['cusip'][cusip.lower()] = record_data
                        if ticker:
                            cache_fi['ticker_symbol'][ticker.lower()] = record_data
                        if position_name:
                            cache_fi['position'][position_name.lower()] = record_data
                    
                    # Log completion
                    logger.info(f"Built {len(cache_fi['cusip'])} CUSIP lookups, {len(cache_fi['ticker_symbol'])} ticker lookups, and {len(cache_fi['position'])} position lookups")
                    return True
                
                # Execute with timeout protection
                success = with_timeout(load_all_durations, timeout_duration=5, default=False)
                
                if success:
                    cache_fi['preloaded'] = True
                    logger.info("Successfully preloaded all fixed income risk stats")
                else:
                    logger.warning("Timed out while preloading fixed income risk stats - reverting to on-demand loading")
            
            except Exception as e:
                logger.error(f"Error during fixed income preload: {str(e)}")
                logger.info("Continuing with on-demand lookups")
        else:
            logger.info("Using existing fixed income risk stats cache")
        
        # Initialize metrics
        matched_value = Decimal('0.0')
        duration_matches = 0
        total_positions = len(fixed_income_positions)
        processed = 0
        match_counts = {
            'cusip': 0,
            'ticker': 0,
            'position': 0,
            'none': 0
        }
        
        # Process all positions in a single batch operation
        # This eliminates the overhead of individual position processing
        logger.info(f"Batch processing {len(fixed_income_positions)} fixed income positions")
        
        # Set a hard deadline for position processing
        deadline = time.time() + 10  # Maximum 10 seconds to process all positions
        
        for i, position in enumerate(fixed_income_positions):
            # Check for timeout
            if time.time() > deadline:
                logger.warning(f"Processing timeout reached after {i}/{len(fixed_income_positions)} positions")
                break
                
            try:
                # Convert position value - use optimized path for common case
                try:
                    position_value = Decimal(str(position.adjusted_value))
                except (ValueError, TypeError):
                    position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
                
                # Skip zero-value positions
                if position_value <= Decimal('0.0'):
                    continue
                    
                # Log progress at regular intervals
                processed += 1
                if processed % 10 == 0:
                    logger.debug(f"Fixed income progress: {processed}/{total_positions} ({(processed/total_positions)*100:.1f}%)")
                
                # Find matching risk statistic using ultra-fast cache lookup
                risk_stat = None
                match_source = None
                
                # Check in descending order of reliability for fixed income
                if position.cusip and position.cusip.lower() in cache_fi['cusip']:
                    risk_stat = cache_fi['cusip'][position.cusip.lower()]
                    match_source = 'cusip'
                    match_counts['cusip'] += 1
                elif position.ticker_symbol and position.ticker_symbol.lower() in cache_fi['ticker_symbol']:
                    risk_stat = cache_fi['ticker_symbol'][position.ticker_symbol.lower()]
                    match_source = 'ticker'
                    match_counts['ticker'] += 1
                elif position.position and position.position.lower() in cache_fi['position']:
                    risk_stat = cache_fi['position'][position.position.lower()]
                    match_source = 'position'
                    match_counts['position'] += 1
                else:
                    match_counts['none'] += 1
                
                # Process the duration if found
                if risk_stat and 'duration' in risk_stat:
                    duration_value = risk_stat['duration']
                    
                    # Use try-except only for the conversion, which is the most error-prone part
                    try:
                        duration = Decimal(str(duration_value))
                        
                        # Fast-path weighted duration calculation
                        if totals["fixed_income"] > Decimal('0.0'):
                            weighted_duration = (duration * position_value) / totals["fixed_income"]
                        else:
                            weighted_duration = Decimal('0.0')
                            
                        # Update metrics
                        risk_metrics["fixed_income"]["duration"]["weighted_sum"] += weighted_duration
                        matched_value += position_value
                        duration_matches += 1
                        
                    except (ValueError, TypeError):
                        # Silent handling for conversion errors - just skip this position
                        pass
            except Exception as e:
                # Log and continue on any error
                logger.debug(f"Error processing position {position.position}: {str(e)}")
                
        # Calculate coverage
        coverage = Decimal('0.0')
        if totals["fixed_income"] > Decimal('0.0'):
            coverage = (matched_value / totals["fixed_income"]) * 100
            risk_metrics["fixed_income"]["duration"]["coverage_pct"] = coverage
        
        # Log detailed statistics
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"Fixed income duration processing completed in {total_time:.2f} seconds")
        logger.info(f"Duration matches: {duration_matches}/{total_positions} positions ({coverage:.2f}% coverage)")
        logger.info(f"Match sources: CUSIP={match_counts['cusip']}, Ticker={match_counts['ticker']}, Position={match_counts['position']}, None={match_counts['none']}")
        
    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"Critical error in fixed income processing: {str(e)}")
        # Handle partial results
        if totals["fixed_income"] > Decimal('0.0') and matched_value > Decimal('0.0'):
            coverage = (matched_value / totals["fixed_income"]) * 100
            risk_metrics["fixed_income"]["duration"]["coverage_pct"] = coverage
            logger.info(f"Partial fixed income results: {coverage:.2f}% coverage from recovered data")

def process_hard_currency_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """
    Optimized hard currency processing for maximum performance.
    
    Uses the same approach as the fixed income optimization:
    1. Preloading all risk stats at once
    2. In-memory lookup instead of individual DB queries
    3. Fast batch operations
    4. Timeout protection to ensure completion
    """
    # Log start time for overall performance tracking
    start_time = time.time()
    
    if not cache:
        cache = {}
    
    # Initialize hard currency risk stats cache directly
    cache_hc = cache.setdefault('hard_currency', {
        'cusip': {},
        'ticker_symbol': {},
        'position': {},
        'preloaded': False
    })
    
    # Pre-filter positions to hard currency only
    try:
        logger.info("Starting hard currency risk calculation with timeout protection")
        hard_currency_positions = []
        position_count = 0
        
        # Use efficient filtering with early exit on potential timeout
        for p in positions:
            if p.asset_class and "hard" in p.asset_class.lower() and "currency" in p.asset_class.lower():
                hard_currency_positions.append(p)
                position_count += 1
        
        if position_count == 0:
            logger.info("No hard currency positions found for beta calculations")
            return
            
        logger.info(f"Found {position_count} hard currency positions for processing")
        
        # Direct DB query with timeout protection to get all betas at once
        if not cache_hc.get('preloaded'):
            logger.info("Performing one-time preload of all hard currency risk stats")
            
            try:
                from src.models.models import RiskStatisticEquity
                from sqlalchemy.sql import func, text
                
                # Use a with_timeout wrapper to enforce a hard deadline
                def load_all_betas():
                    # Run an optimized query to get all betas in a single call
                    # Use proper source table (equity or alternatives) based on asset class
                    query = text("""
                        SELECT 
                            cusip, ticker_symbol, position, beta, volatility
                        FROM 
                            risk_statistic_equity
                        WHERE 
                            upload_date <= :latest_date
                        ORDER BY 
                            upload_date DESC
                    """)
                    
                    # Execute the query with a timeout
                    result = db.execute(query, {"latest_date": latest_risk_stats_date})
                    
                    # Process all results at once
                    beta_records = result.fetchall()
                    logger.info(f"Loaded {len(beta_records)} hard currency beta records from database")
                    
                    # Build lookups for each identifier type
                    for record in beta_records:
                        # We're getting beta and volatility from equity table
                        cusip, ticker, position_name, beta, volatility = record
                        
                        # Just use beta directly
                        beta_value = beta
                        
                        # Only create records if we have a beta value
                        if beta_value is not None:
                            # Create a simplified record structure
                            record_data = {'beta': beta_value}
                            
                            # Add to each lookup table
                            if cusip:
                                cache_hc['cusip'][cusip.lower()] = record_data
                            if ticker:
                                cache_hc['ticker_symbol'][ticker.lower()] = record_data
                            if position_name:
                                cache_hc['position'][position_name.lower()] = record_data
                    
                    # Log completion
                    logger.info(f"Built {len(cache_hc['cusip'])} CUSIP lookups, {len(cache_hc['ticker_symbol'])} ticker lookups, and {len(cache_hc['position'])} position lookups")
                    return True
                
                # Execute with timeout protection
                success = with_timeout(load_all_betas, timeout_duration=5, default=False)
                
                if success:
                    cache_hc['preloaded'] = True
                    logger.info("Successfully preloaded all hard currency risk stats")
                else:
                    logger.warning("Timed out while preloading hard currency risk stats - reverting to on-demand loading")
            
            except Exception as e:
                logger.error(f"Error during hard currency preload: {str(e)}")
                logger.info("Continuing with on-demand lookups")
        else:
            logger.info("Using existing hard currency risk stats cache")
        
        # Initialize metrics
        matched_value = Decimal('0.0')
        beta_matches = 0
        total_positions = len(hard_currency_positions)
        processed = 0
        match_counts = {
            'cusip': 0,
            'ticker': 0,
            'position': 0,
            'none': 0
        }
        
        # Process all positions in a single batch operation
        # This eliminates the overhead of individual position processing
        logger.info(f"Batch processing {len(hard_currency_positions)} hard currency positions")
        
        # Set a hard deadline for position processing
        deadline = time.time() + 10  # Maximum 10 seconds to process all positions
        
        for i, position in enumerate(hard_currency_positions):
            # Check for timeout
            if time.time() > deadline:
                logger.warning(f"Processing timeout reached after {i}/{len(hard_currency_positions)} positions")
                break
                
            try:
                # Convert position value - use optimized path for common case
                try:
                    position_value = Decimal(str(position.adjusted_value))
                except (ValueError, TypeError):
                    position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
                
                # Skip zero-value positions
                if position_value <= Decimal('0.0'):
                    continue
                    
                # Log progress at regular intervals
                processed += 1
                if processed % 10 == 0:
                    logger.debug(f"Hard currency progress: {processed}/{total_positions} ({(processed/total_positions)*100:.1f}%)")
                
                # Find matching risk statistic using ultra-fast cache lookup
                risk_stat = None
                match_source = None
                
                # Check in order of reliability for hard currency (similar to fixed income)
                if position.cusip and position.cusip.lower() in cache_hc['cusip']:
                    risk_stat = cache_hc['cusip'][position.cusip.lower()]
                    match_source = 'cusip'
                    match_counts['cusip'] += 1
                elif position.ticker_symbol and position.ticker_symbol.lower() in cache_hc['ticker_symbol']:
                    risk_stat = cache_hc['ticker_symbol'][position.ticker_symbol.lower()]
                    match_source = 'ticker'
                    match_counts['ticker'] += 1
                elif position.position and position.position.lower() in cache_hc['position']:
                    risk_stat = cache_hc['position'][position.position.lower()]
                    match_source = 'position'
                    match_counts['position'] += 1
                else:
                    match_counts['none'] += 1
                    # If not in cache, fall back to direct lookup
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
                        match_source = 'direct'
                    else:
                        # Track unmatched securities
                        track_unmatched_security(position.position, "Hard Currency")
                
                # Process the beta if found
                if risk_stat is not None:
                    # Extract beta with type safety (could be in different fields based on asset class)
                    beta_value = None
                    if isinstance(risk_stat, dict):
                        # Try both common beta field names
                        beta_value = risk_stat.get('beta')
                        if beta_value is None:
                            beta_value = risk_stat.get('average_beta')
                    
                    if beta_value is not None:
                        # Use try-except only for the conversion, which is the most error-prone part
                        try:
                            beta = Decimal(str(beta_value))
                            
                            # Fast-path weighted beta calculation
                            if totals["hard_currency"] > Decimal('0.0'):
                                weighted_beta = (beta * position_value) / totals["hard_currency"]
                            else:
                                weighted_beta = Decimal('0.0')
                                
                            # Update metrics
                            risk_metrics["hard_currency"]["beta"]["weighted_sum"] += weighted_beta
                            matched_value += position_value
                            beta_matches += 1
                            
                        except (ValueError, TypeError) as e:
                            # Silent handling for conversion errors - just skip this position
                            logger.debug(f"Error processing beta for {position.position}: {e}")
            except Exception as e:
                # Log and continue on any error
                logger.debug(f"Error processing position {position.position}: {str(e)}")
        
        # Calculate coverage
        coverage = Decimal('0.0')
        if totals["hard_currency"] > Decimal('0.0'):
            coverage = (matched_value / totals["hard_currency"]) * 100
            risk_metrics["hard_currency"]["beta"]["coverage_pct"] = coverage
        
        # Log detailed statistics
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"Hard currency beta processing completed in {total_time:.2f} seconds")
        logger.info(f"Beta matches: {beta_matches}/{total_positions} positions ({coverage:.2f}% coverage)")
        logger.info(f"Match sources: CUSIP={match_counts['cusip']}, Ticker={match_counts['ticker']}, Position={match_counts['position']}, None={match_counts['none']}")
        
    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"Critical error in hard currency processing: {str(e)}")
        # Handle partial results
        try:
            if totals["hard_currency"] > Decimal('0.0') and 'matched_value' in locals() and matched_value > Decimal('0.0'):
                coverage = (matched_value / totals["hard_currency"]) * 100
                risk_metrics["hard_currency"]["beta"]["coverage_pct"] = coverage
                logger.info(f"Partial hard currency results: {coverage:.2f}% coverage from recovered data")
        except Exception:
            # If even the recovery fails, at least log that we tried
            logger.error("Could not recover partial results for hard currency")

def process_alternatives_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date,
    cache: Optional[Dict[str, Any]] = None
) -> None:
    """
    Optimized alternatives processing for maximum performance.
    
    Uses the same approach as the fixed income optimization:
    1. Preloading all risk stats at once
    2. In-memory lookup instead of individual DB queries
    3. Fast batch operations
    4. Timeout protection to ensure completion
    """
    # Log start time for overall performance tracking
    start_time = time.time()
    
    if not cache:
        cache = {}
    
    # Initialize alternatives risk stats cache directly
    cache_alt = cache.setdefault('alternatives', {
        'cusip': {},
        'ticker_symbol': {},
        'position': {},
        'preloaded': False
    })
    
    # Pre-filter positions to alternatives only
    try:
        logger.info("Starting alternatives risk calculation with timeout protection")
        alt_positions = []
        position_count = 0
        
        # Use efficient filtering with early exit on potential timeout
        for p in positions:
            if p.asset_class and "alternative" in p.asset_class.lower():
                alt_positions.append(p)
                position_count += 1
        
        if position_count == 0:
            logger.info("No alternatives positions found for beta calculations")
            return
            
        logger.info(f"Found {position_count} alternatives positions for processing")
        
        # Direct DB query with timeout protection to get all betas at once
        if not cache_alt.get('preloaded'):
            logger.info("Performing one-time preload of all alternatives risk stats")
            
            try:
                from src.models.models import RiskStatisticAlternatives
                from sqlalchemy.sql import func, text
                
                # Use a with_timeout wrapper to enforce a hard deadline
                def load_all_betas():
                    # Run an optimized query to get all betas in a single call
                    # Note: Alternatives table only has 'beta' column, not 'average_beta'
                    query = text("""
                        SELECT 
                            cusip, ticker_symbol, position, beta
                        FROM 
                            risk_statistic_alternatives
                        WHERE 
                            upload_date <= :latest_date
                        ORDER BY 
                            upload_date DESC
                    """)
                    
                    # Execute the query with a timeout
                    result = db.execute(query, {"latest_date": latest_risk_stats_date})
                    
                    # Process all results at once
                    beta_records = result.fetchall()
                    logger.info(f"Loaded {len(beta_records)} alternatives beta records from database")
                    
                    # Build lookups for each identifier type
                    for record in beta_records:
                        # Note: Only 4 columns now (no avg_beta)
                        cusip, ticker, position_name, beta = record
                        
                        # Just use beta directly, as there's no avg_beta
                        beta_value = beta
                        
                        # Only create records if we have a beta value
                        if beta_value is not None:
                            # Create a simplified record structure
                            record_data = {'beta': beta_value}
                            
                            # Add to each lookup table
                            if cusip:
                                cache_alt['cusip'][cusip.lower()] = record_data
                            if ticker:
                                cache_alt['ticker_symbol'][ticker.lower()] = record_data
                            if position_name:
                                cache_alt['position'][position_name.lower()] = record_data
                    
                    # Log completion
                    logger.info(f"Built {len(cache_alt['cusip'])} CUSIP lookups, {len(cache_alt['ticker_symbol'])} ticker lookups, and {len(cache_alt['position'])} position lookups")
                    return True
                
                # Execute with timeout protection
                success = with_timeout(load_all_betas, timeout_duration=5, default=False)
                
                if success:
                    cache_alt['preloaded'] = True
                    logger.info("Successfully preloaded all alternatives risk stats")
                else:
                    logger.warning("Timed out while preloading alternatives risk stats - reverting to on-demand loading")
            
            except Exception as e:
                logger.error(f"Error during alternatives preload: {str(e)}")
                logger.info("Continuing with on-demand lookups")
        else:
            logger.info("Using existing alternatives risk stats cache")
        
        # Initialize metrics
        matched_value = Decimal('0.0')
        beta_matches = 0
        total_positions = len(alt_positions)
        processed = 0
        match_counts = {
            'cusip': 0,
            'ticker': 0,
            'position': 0,
            'none': 0
        }
        
        # Process all positions in a single batch operation
        # This eliminates the overhead of individual position processing
        logger.info(f"Batch processing {len(alt_positions)} alternatives positions")
        
        # Set a hard deadline for position processing
        deadline = time.time() + 10  # Maximum 10 seconds to process all positions
        
        for i, position in enumerate(alt_positions):
            # Check for timeout
            if time.time() > deadline:
                logger.warning(f"Processing timeout reached after {i}/{len(alt_positions)} positions")
                break
                
            try:
                # Convert position value - use optimized path for common case
                try:
                    position_value = Decimal(str(position.adjusted_value))
                except (ValueError, TypeError):
                    position_value = convert_position_value_to_decimal(position.adjusted_value, position.position)
                
                # Skip zero-value positions
                if position_value <= Decimal('0.0'):
                    continue
                    
                # Log progress at regular intervals
                processed += 1
                if processed % 10 == 0:
                    logger.debug(f"Alternatives progress: {processed}/{total_positions} ({(processed/total_positions)*100:.1f}%)")
                
                # Find matching risk statistic using ultra-fast cache lookup
                risk_stat = None
                match_source = None
                
                # Check in order of reliability for alternatives (similar to fixed income)
                if position.cusip and position.cusip.lower() in cache_alt['cusip']:
                    risk_stat = cache_alt['cusip'][position.cusip.lower()]
                    match_source = 'cusip'
                    match_counts['cusip'] += 1
                elif position.ticker_symbol and position.ticker_symbol.lower() in cache_alt['ticker_symbol']:
                    risk_stat = cache_alt['ticker_symbol'][position.ticker_symbol.lower()]
                    match_source = 'ticker'
                    match_counts['ticker'] += 1
                elif position.position and position.position.lower() in cache_alt['position']:
                    risk_stat = cache_alt['position'][position.position.lower()]
                    match_source = 'position'
                    match_counts['position'] += 1
                else:
                    match_counts['none'] += 1
                    # If not in cache, fall back to direct lookup
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
                        match_source = 'direct'
                    else:
                        # Track unmatched securities
                        track_unmatched_security(position.position, "Alternatives")
                
                # Process the beta if found
                if risk_stat is not None:
                    # Extract beta with type safety (could be in different fields based on asset class)
                    beta_value = None
                    if isinstance(risk_stat, dict):
                        # Try both common beta field names
                        beta_value = risk_stat.get('beta')
                        if beta_value is None:
                            beta_value = risk_stat.get('average_beta')
                    
                    if beta_value is not None:
                        # Use try-except only for the conversion, which is the most error-prone part
                        try:
                            beta = Decimal(str(beta_value))
                            
                            # Fast-path weighted beta calculation
                            if totals["alternatives"] > Decimal('0.0'):
                                weighted_beta = (beta * position_value) / totals["alternatives"]
                            else:
                                weighted_beta = Decimal('0.0')
                                
                            # Update metrics
                            risk_metrics["alternatives"]["beta"]["weighted_sum"] += weighted_beta
                            matched_value += position_value
                            beta_matches += 1
                            
                        except (ValueError, TypeError) as e:
                            # Silent handling for conversion errors - just skip this position
                            logger.debug(f"Error processing beta for {position.position}: {e}")
            except Exception as e:
                # Log and continue on any error
                logger.debug(f"Error processing position {position.position}: {str(e)}")
        
        # Calculate coverage
        coverage = Decimal('0.0')
        if totals["alternatives"] > Decimal('0.0'):
            coverage = (matched_value / totals["alternatives"]) * 100
            risk_metrics["alternatives"]["beta"]["coverage_pct"] = coverage
        
        # Log detailed statistics
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"Alternatives beta processing completed in {total_time:.2f} seconds")
        logger.info(f"Beta matches: {beta_matches}/{total_positions} positions ({coverage:.2f}% coverage)")
        logger.info(f"Match sources: CUSIP={match_counts['cusip']}, Ticker={match_counts['ticker']}, Position={match_counts['position']}, None={match_counts['none']}")
        
    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"Critical error in alternatives processing: {str(e)}")
        # Handle partial results
        try:
            if totals["alternatives"] > Decimal('0.0') and 'matched_value' in locals() and matched_value > Decimal('0.0'):
                coverage = (matched_value / totals["alternatives"]) * 100
                risk_metrics["alternatives"]["beta"]["coverage_pct"] = coverage
                logger.info(f"Partial alternatives results: {coverage:.2f}% coverage from recovered data")
        except Exception:
            # If even the recovery fails, at least log that we tried
            logger.error("Could not recover partial results for alternatives")

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
    # Use our optimized implementation if available
    if USE_OPTIMIZED_MATCHING:
        try:
            return optimized_find_matching_risk_stat(
                db, position_name, cusip, ticker_symbol, asset_class, latest_date, cache
            )
        except Exception as e:
            logger.warning(f"Error using optimized matching implementation: {e}")
            logger.warning("Falling back to standard implementation")
            # Fall back to standard implementation below
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
    
    # Determine the asset class key for cache lookup
    asset_class_key = None
    if "equity" in asset_class_str:
        asset_class_key = "equity"
    elif "fixed" in asset_class_str:
        asset_class_key = "fixed_income"
    elif "alternative" in asset_class_str:
        asset_class_key = "alternatives"
    elif "hard" in asset_class_str and "currency" in asset_class_str:
        asset_class_key = "hard_currency"
    
    # Check cache first - no database access needed if we have a cached value
    if cache is not None and asset_class_key:
        # Try all identifiers in order of reliability
        if safe_cusip and safe_cusip in cache.get(asset_class_key, {}).get("cusip", {}):
            return cache[asset_class_key]["cusip"][safe_cusip]
            
        if safe_ticker and safe_ticker in cache.get(asset_class_key, {}).get("ticker_symbol", {}):
            return cache[asset_class_key]["ticker_symbol"][safe_ticker]
            
        if safe_position and safe_position in cache.get(asset_class_key, {}).get("position", {}):
            return cache[asset_class_key]["position"][safe_position]
    
    # Create a function for all database queries to reduce code duplication
    def execute_query(condition, identifier_type, identifier_value):
        """Execute a database query with proper connection handling and caching"""
        # Check structured cache first for faster retrievals
        if cache is not None and asset_class_key and identifier_type in cache.get(asset_class_key, {}):
            if identifier_value in cache[asset_class_key][identifier_type]:
                return cache[asset_class_key][identifier_type][identifier_value]
        
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
                    
                    # Cache the result in the structured cache
                    if cache is not None and asset_class_key:
                        try:
                            # Store in the appropriate nested dictionary
                            if identifier_type in cache[asset_class_key]:
                                cache[asset_class_key][identifier_type][identifier_value] = risk_dict
                        except Exception as e:
                            logger.warning(f"Cache storage error: {str(e)}")
                    
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
    
    # Create a properly structured metrics dictionary for portfolio-level metrics
    portfolio_metrics = {
        "beta": {
            "weighted_sum": portfolio_beta,
            "value": portfolio_beta,
            "coverage_pct": Decimal('100.0')  # Using full coverage for calculated values
        }
    }
    
    # Store the overall portfolio beta in a structured dictionary that matches the expected types
    risk_metrics["portfolio"] = portfolio_metrics