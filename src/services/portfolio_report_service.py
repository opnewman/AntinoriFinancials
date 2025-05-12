"""
Portfolio Report Service - Optimized Database Queries

This service uses direct PostgreSQL queries to efficiently generate portfolio reports
according to the nori categorization schema:

- Equity: Uses asset_class='equity' with subcategories from second_level
- Fixed Income: Uses asset_class='fixed income' with subcategories from second_level
- Hard Currency: Uses asset_class='alternatives' AND second_level='hard currency' with subcategories from third_level
- Uncorrelated Alternatives: Uses asset_class='alternatives' BUT NOT second_level='hard currency'
  with custom subcategory logic (crypto, proficio funds, etc.)
- Cash: Uses asset_class='cash & cash equivalent'
- Liquidity: Based on liquid_vs_illiquid field
- Performance: Generated from historical data (to be integrated)
"""

import logging
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy import text, func, distinct

# Import models and services
from src.models.models import EgnyteRiskStat
from src.services.portfolio_risk_service import calculate_portfolio_risk_metrics

# Set up logging
logger = logging.getLogger(__name__)

# SQL Queries for direct database access
SQL_TOTAL_PORTFOLIO_VALUE = """
    SELECT SUM(
        CASE WHEN adjusted_value LIKE 'ENC:%' 
            THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
            ELSE CAST(adjusted_value AS NUMERIC) 
        END
    ) as total_value
    FROM financial_positions
    WHERE date = :date
    AND {level_filter}
"""

SQL_ASSET_CLASS_TOTALS = """
    SELECT
        asset_class,
        SUM(
            CASE WHEN adjusted_value LIKE 'ENC:%' 
                THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
                ELSE CAST(adjusted_value AS NUMERIC) 
            END
        ) as total_value
    FROM financial_positions
    WHERE date = :date
    AND {level_filter}
    GROUP BY asset_class
"""

SQL_EQUITY_SUBCATEGORIES = """
    SELECT
        second_level,
        SUM(
            CASE WHEN adjusted_value LIKE 'ENC:%' 
                THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
                ELSE CAST(adjusted_value AS NUMERIC) 
            END
        ) as total_value
    FROM financial_positions
    WHERE date = :date
    AND asset_class = 'Equity'
    AND {level_filter}
    GROUP BY second_level
"""

SQL_FIXED_INCOME_SUBCATEGORIES = """
    SELECT
        second_level,
        SUM(
            CASE WHEN adjusted_value LIKE 'ENC:%' 
                THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
                ELSE CAST(adjusted_value AS NUMERIC) 
            END
        ) as total_value
    FROM financial_positions
    WHERE date = :date
    AND asset_class = 'Fixed Income'
    AND {level_filter}
    GROUP BY second_level
"""

SQL_HARD_CURRENCY_SUBCATEGORIES = """
    SELECT
        third_level,
        SUM(
            CASE WHEN adjusted_value LIKE 'ENC:%' 
                THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
                ELSE CAST(adjusted_value AS NUMERIC) 
            END
        ) as total_value
    FROM financial_positions
    WHERE date = :date
    AND asset_class = 'Alternatives'
    AND second_level = 'Precious Metals'
    AND {level_filter}
    GROUP BY third_level
"""

SQL_UNCORRELATED_ALTERNATIVES = """
    SELECT
        position,
        second_level,
        third_level,
        CASE WHEN adjusted_value LIKE 'ENC:%' 
            THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
            ELSE CAST(adjusted_value AS NUMERIC) 
        END as adjusted_value
    FROM financial_positions
    WHERE date = :date
    AND asset_class = 'Alternatives'
    AND second_level != 'Precious Metals'
    AND {level_filter}
"""

SQL_LIQUIDITY = """
    SELECT
        liquid_vs_illiquid,
        SUM(
            CASE WHEN adjusted_value LIKE 'ENC:%' 
                THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
                ELSE CAST(adjusted_value AS NUMERIC) 
            END
        ) as total_value
    FROM financial_positions
    WHERE date = :date
    AND {level_filter}
    GROUP BY liquid_vs_illiquid
"""

def get_level_filter(level: str, level_key: str) -> str:
    """
    Generate the appropriate SQL filter based on level and level_key.
    
    Args:
        level: The hierarchy level ('client', 'portfolio', 'group', 'account')
        level_key: The identifier for the specified level
        
    Returns:
        SQL string for filtering
    """
    # Sanitize level_key for SQL injection prevention
    sanitized_key = level_key.replace("'", "''")
    
    if level == 'client':
        return f"top_level_client = '{sanitized_key}'"
    elif level == 'portfolio':
        return f"portfolio = '{sanitized_key}'"
    elif level == 'group':
        # For group level, we'll use portfolio field for now
        return f"portfolio = '{sanitized_key}'"
    elif level == 'account':
        # Use holding_account instead of holding_account_number for more flexible matching
        return f"(holding_account = '{sanitized_key}' OR holding_account_number = '{sanitized_key}')"
    else:
        logger.warning(f"Invalid level: {level}, defaulting to portfolio")
        return f"portfolio = '{sanitized_key}'"


def get_total_adjusted_value(db: Session, report_date: date, level: str, level_key: str) -> float:
    """
    Get the total adjusted value for the portfolio.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        
    Returns:
        Total adjusted value
    """
    level_filter = get_level_filter(level, level_key)
    sql = SQL_TOTAL_PORTFOLIO_VALUE.format(level_filter=level_filter)
    
    result = db.execute(text(sql), {'date': report_date}).first()
    return float(result.total_value) if result and result.total_value else 0.0


def get_asset_class_breakdowns(db: Session, report_date: date, level: str, level_key: str, 
                               total_value: float) -> Dict:
    """
    Get breakdown of asset classes.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        total_value: Total adjusted value for percentage calculations
        
    Returns:
        Dict of asset class breakdowns with percentages
    """
    level_filter = get_level_filter(level, level_key)
    sql = SQL_ASSET_CLASS_TOTALS.format(level_filter=level_filter)
    
    results = db.execute(text(sql), {'date': report_date}).fetchall()
    
    # Initialize default values
    breakdowns = {
        'equities': {'total_pct': 0.0, 'subcategories': {}},
        'fixed_income': {'total_pct': 0.0, 'subcategories': {}},
        'hard_currency': {'total_pct': 0.0, 'subcategories': {}},
        'uncorrelated_alternatives': {'total_pct': 0.0, 'subcategories': {}},
        'cash': {'total_pct': 0.0}
    }
    
    for row in results:
        asset_class = row.asset_class if row.asset_class else 'unknown'
        value = float(row.total_value) if row.total_value else 0.0
        percentage = (value / total_value * 100) if total_value > 0 else 0.0
        
        if asset_class == 'Equity':
            breakdowns['equities']['total_pct'] = percentage
        elif asset_class == 'Fixed Income':
            breakdowns['fixed_income']['total_pct'] = percentage
        elif asset_class == 'Cash & Cash Equivalent':
            breakdowns['cash']['total_pct'] = percentage
    
    # Hard Currency and Uncorrelated Alternatives will be calculated separately
    # since they both come from the 'alternatives' asset class
    
    return breakdowns


def get_equity_breakdown(db: Session, report_date: date, level: str, level_key: str, 
                         total_value: float) -> Dict:
    """
    Get detailed breakdown of equity positions.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        total_value: Total adjusted value for percentage calculations
        
    Returns:
        Dict of equity subcategories with percentages
    """
    level_filter = get_level_filter(level, level_key)
    sql = SQL_EQUITY_SUBCATEGORIES.format(level_filter=level_filter)
    
    results = db.execute(text(sql), {'date': report_date}).fetchall()
    
    # Initialize all equity subcategories with zero
    subcategories = {
        'us_markets': 0.0,
        'global_markets': 0.0,
        'emerging_markets': 0.0,
        'real_estate': 0.0,
        'private_equity': 0.0,
        'venture_capital': 0.0,
        'equity_derivatives': 0.0,
        'commodities': 0.0,
        'high_yield': 0.0,
        'income_notes': 0.0,
        'low_beta_alpha': 0.0
    }
    
    for row in results:
        second_level = row.second_level.lower() if row.second_level else 'unknown'
        value = float(row.total_value) if row.total_value else 0.0
        percentage = (value / total_value * 100) if total_value > 0 else 0.0
        
        # Map the second_level to our expected subcategories
        if second_level == 'us markets':
            subcategories['us_markets'] = percentage
        elif second_level == 'global markets':
            subcategories['global_markets'] = percentage
        elif second_level == 'emerging markets':
            subcategories['emerging_markets'] = percentage
        elif second_level == 'real estate':
            subcategories['real_estate'] = percentage
        elif second_level == 'private equity':
            subcategories['private_equity'] = percentage
        elif second_level == 'venture capital':
            subcategories['venture_capital'] = percentage
        elif second_level == 'equity derivatives':
            subcategories['equity_derivatives'] = percentage
        elif second_level == 'commodities':
            subcategories['commodities'] = percentage
        elif second_level == 'high yield':
            subcategories['high_yield'] = percentage
        elif second_level == 'income notes':
            subcategories['income_notes'] = percentage
        elif second_level == 'low beta alpha':
            subcategories['low_beta_alpha'] = percentage
        # Add any missing mappings as needed
    
    return subcategories


def get_fixed_income_breakdown(db: Session, report_date: date, level: str, level_key: str, 
                               total_value: float) -> Dict:
    """
    Get detailed breakdown of fixed income positions.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        total_value: Total adjusted value for percentage calculations
        
    Returns:
        Dict of fixed income subcategories with percentages
    """
    level_filter = get_level_filter(level, level_key)
    sql = SQL_FIXED_INCOME_SUBCATEGORIES.format(level_filter=level_filter)
    
    results = db.execute(text(sql), {'date': report_date}).fetchall()
    
    # Initialize fixed income subcategories with default structures
    subcategories = {
        'municipal_bonds': {'long_duration': 0.0, 'market_duration': 0.0, 'short_duration': 0.0, 'total_pct': 0.0},
        'government_bonds': {'long_duration': 0.0, 'market_duration': 0.0, 'short_duration': 0.0, 'total_pct': 0.0},
        'investment_grade': {'long_duration': 0.0, 'market_duration': 0.0, 'short_duration': 0.0, 'total_pct': 0.0},
        'fixed_income_derivatives': {'total_pct': 0.0}
    }
    
    for row in results:
        second_level = row.second_level.lower() if row.second_level else 'unknown'
        value = float(row.total_value) if row.total_value else 0.0
        percentage = (value / total_value * 100) if total_value > 0 else 0.0
        
        # Map the second_level to our expected subcategories
        if second_level == 'municipal bonds':
            subcategories['municipal_bonds']['total_pct'] = percentage
        elif second_level == 'government bonds':
            subcategories['government_bonds']['total_pct'] = percentage
        elif second_level == 'investment grade':
            subcategories['investment_grade']['total_pct'] = percentage
        elif second_level == 'corporate bonds':  # Map corporate bonds to investment grade for now
            subcategories['investment_grade']['total_pct'] += percentage
        elif second_level == 'fixed income derivatives':
            subcategories['fixed_income_derivatives']['total_pct'] = percentage
        # Add any missing mappings as needed
    
    # Duration breakdowns will be populated later when risk stats are integrated
    
    return subcategories


def get_hard_currency_breakdown(db: Session, report_date: date, level: str, level_key: str, 
                                total_value: float) -> Tuple[float, Dict]:
    """
    Get detailed breakdown of hard currency positions.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        total_value: Total adjusted value for percentage calculations
        
    Returns:
        Tuple of (total hard currency percentage, dict of hard currency subcategories with percentages)
    """
    level_filter = get_level_filter(level, level_key)
    sql = SQL_HARD_CURRENCY_SUBCATEGORIES.format(level_filter=level_filter)
    
    results = db.execute(text(sql), {'date': report_date}).fetchall()
    
    # Initialize hard currency subcategories
    subcategories = {
        'gold': 0.0,
        'gold_miners': 0.0,
        'silver': 0.0,
        'silver_miners': 0.0,
        'industrial_metals': 0.0,
        'precious_metals_derivatives': 0.0,
        'hard_currency_physical_investment': 0.0
    }
    
    total_hard_currency_value = 0.0
    
    for row in results:
        third_level = row.third_level.lower() if row.third_level else 'unknown'
        value = float(row.total_value) if row.total_value else 0.0
        percentage = (value / total_value * 100) if total_value > 0 else 0.0
        
        total_hard_currency_value += value
        
        # Map the third_level to our expected subcategories
        if third_level == 'gold':
            subcategories['gold'] = percentage
        elif third_level == 'gold miners':
            subcategories['gold_miners'] = percentage
        elif third_level == 'silver':
            subcategories['silver'] = percentage
        elif third_level == 'silver miners':
            subcategories['silver_miners'] = percentage
        elif third_level == 'industrial metals':
            subcategories['industrial_metals'] = percentage
        elif third_level == 'precious metals derivatives':
            subcategories['precious_metals_derivatives'] = percentage
        elif third_level == 'hard currency physical investment':
            subcategories['hard_currency_physical_investment'] = percentage
        # Add any missing mappings as needed
    
    total_hard_currency_pct = (total_hard_currency_value / total_value * 100) if total_value > 0 else 0.0
    
    return total_hard_currency_pct, subcategories


def get_uncorrelated_alternatives_breakdown(db: Session, report_date: date, level: str, level_key: str, 
                                            total_value: float) -> Tuple[float, Dict]:
    """
    Get detailed breakdown of uncorrelated alternatives positions.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        total_value: Total adjusted value for percentage calculations
        
    Returns:
        Tuple of (total alternatives percentage, dict of alternatives subcategories with percentages)
    """
    level_filter = get_level_filter(level, level_key)
    sql = SQL_UNCORRELATED_ALTERNATIVES.format(level_filter=level_filter)
    
    results = db.execute(text(sql), {'date': report_date}).fetchall()
    
    # Initialize alternatives subcategories
    subcategories = {
        'crypto': 0.0,
        'proficio_short_term': 0.0,
        'proficio_long_term': 0.0,
        'other': 0.0
    }
    
    total_alternatives_value = 0.0
    
    for row in results:
        position = row.position.lower() if row.position else ''
        second_level = row.second_level.lower() if row.second_level else ''
        third_level = row.third_level.lower() if row.third_level else ''
        value = float(row.adjusted_value) if row.adjusted_value else 0.0
        percentage = (value / total_value * 100) if total_value > 0 else 0.0
        
        total_alternatives_value += value
        
        # Categorize based on the rules
        # First check if it's crypto
        if 'crypto' in third_level or 'crypto' in position:
            subcategories['crypto'] += percentage
        # Then check if it's Proficio short or long term
        elif 'proficio short' in position or 'short term alts' in position:
            subcategories['proficio_short_term'] += percentage
        elif 'proficio long' in position or 'long term alts' in position:
            subcategories['proficio_long_term'] += percentage
        # Everything else goes to 'other'
        else:
            subcategories['other'] += percentage
    
    total_alternatives_pct = (total_alternatives_value / total_value * 100) if total_value > 0 else 0.0
    
    return total_alternatives_pct, subcategories


def get_liquidity_breakdown(db: Session, report_date: date, level: str, level_key: str) -> Dict:
    """
    Get liquidity breakdown.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        
    Returns:
        Dict with liquid and illiquid percentages
    """
    level_filter = get_level_filter(level, level_key)
    sql = SQL_LIQUIDITY.format(level_filter=level_filter)
    
    results = db.execute(text(sql), {'date': report_date}).fetchall()
    
    total_value = 0.0
    liquid_value = 0.0
    illiquid_value = 0.0
    
    for row in results:
        liquidity = row.liquid_vs_illiquid.lower() if row.liquid_vs_illiquid else 'unknown'
        value = float(row.total_value) if row.total_value else 0.0
        
        total_value += value
        
        if liquidity == 'liquid':
            liquid_value = value
        elif liquidity == 'illiquid':
            illiquid_value = value
    
    # Calculate percentages
    liquid_pct = (liquid_value / total_value * 100) if total_value > 0 else 0.0
    illiquid_pct = (illiquid_value / total_value * 100) if total_value > 0 else 0.0
    
    return {
        'liquid_assets': liquid_pct,
        'illiquid_assets': illiquid_pct
    }


def get_performance_data(db: Session, report_date: date, level: str, level_key: str) -> Dict:
    """
    Get performance data. This is a placeholder until we implement the proper calculation.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        
    Returns:
        Dict with performance percentages for different time periods
    """
    # Placeholder - will be implemented when historical data is available
    return {
        '1D': 0.0,
        'MTD': 0.0,
        'QTD': 0.0,
        'YTD': 0.0
    }


def generate_portfolio_report(db: Session, report_date: date, level: str, level_key: str, display_format: str = 'percent') -> Dict:
    """
    Generate a comprehensive portfolio report.
    
    Args:
        db: Database session
        report_date: The report date
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        display_format: Format to display values ('percent' or 'dollar')
        
    Returns:
        Dict with complete portfolio report data
    """
    import time
    
    # Start a timeout timer to prevent worker crashes
    start_time = time.time()
    max_execution_time = 25  # Maximum seconds to allow for execution before returning partial results
    
    # Initialize a timeout flag
    timeout_occurred = False
    
    # Define timeout checker
    def check_timeout():
        elapsed = time.time() - start_time
        if elapsed > max_execution_time:
            logger.warning(f"Execution time approaching limit ({elapsed:.2f}s). Returning partial results.")
            return True
        return False
    
    logger.info(f"Generating portfolio report for {level}={level_key} on date {report_date}")
    
    # Get total portfolio value
    total_value = get_total_adjusted_value(db, report_date, level, level_key)
    
    # Get main asset class breakdowns
    report_data = get_asset_class_breakdowns(db, report_date, level, level_key, total_value)
    
    # Get equity subcategories
    report_data['equities']['subcategories'] = get_equity_breakdown(
        db, report_date, level, level_key, total_value
    )
    
    # Get fixed income subcategories
    report_data['fixed_income']['subcategories'] = get_fixed_income_breakdown(
        db, report_date, level, level_key, total_value
    )
    
    # Get hard currency data
    hard_currency_pct, hard_currency_subcategories = get_hard_currency_breakdown(
        db, report_date, level, level_key, total_value
    )
    report_data['hard_currency']['total_pct'] = hard_currency_pct
    report_data['hard_currency']['subcategories'] = hard_currency_subcategories
    
    # Get uncorrelated alternatives data
    alternatives_pct, alternatives_subcategories = get_uncorrelated_alternatives_breakdown(
        db, report_date, level, level_key, total_value
    )
    report_data['uncorrelated_alternatives']['total_pct'] = alternatives_pct
    report_data['uncorrelated_alternatives']['subcategories'] = alternatives_subcategories
    
    # Get liquidity breakdown
    report_data['liquidity'] = get_liquidity_breakdown(db, report_date, level, level_key)
    
    # Get performance data
    report_data['performance'] = get_performance_data(db, report_date, level, level_key)
    
    # Add metadata
    report_data['level'] = level
    report_data['level_key'] = level_key
    report_data['report_date'] = report_date.strftime('%Y-%m-%d')
    report_data['total_adjusted_value'] = total_value
    report_data['display_format'] = display_format
    
    # Convert percentages to dollar values if requested
    if display_format == 'dollar':
        # Convert main asset class percentages to dollar values
        for asset_class in ['equities', 'fixed_income', 'hard_currency', 'uncorrelated_alternatives', 'cash']:
            if asset_class in report_data and 'total_pct' in report_data[asset_class]:
                pct = report_data[asset_class]['total_pct']
                report_data[asset_class]['total_value'] = (pct / 100.0) * total_value
                
                # Convert subcategories if they exist
                if 'subcategories' in report_data[asset_class]:
                    subcats = report_data[asset_class]['subcategories']
                    # Handle nested structure (like fixed income)
                    for subcat, value in subcats.items():
                        if isinstance(value, dict):
                            # For nested structures like fixed income
                            for inner_key, inner_value in value.items():
                                if inner_key != 'total_value':  # Avoid duplicating
                                    value[inner_key + '_value'] = (inner_value / 100.0) * total_value
                        else:
                            # For flat structures
                            subcats[subcat + '_value'] = (value / 100.0) * total_value
        
        # Convert liquidity percentages to dollar values
        if 'liquidity' in report_data:
            for liquidity_type in report_data['liquidity']:
                pct = report_data['liquidity'][liquidity_type]
                report_data['liquidity'][liquidity_type + '_value'] = (pct / 100.0) * total_value
    
    # Map level_key to portfolio name for display
    if level == 'portfolio':
        report_data['portfolio'] = level_key
    elif level == 'client':
        report_data['client'] = level_key
    elif level == 'account':
        report_data['account'] = level_key
    
    # Calculate and integrate risk metrics with timeout handling
    try:
        # Set a default empty structure in case calculation fails
        report_data['risk_metrics'] = {
            'equity': {'beta': None, 'volatility': None, 'coverage_pct': None},
            'fixed_income': {'duration': None, 'short_duration_pct': None, 'market_duration_pct': None, 'long_duration_pct': None},
            'hard_currency': {'beta': None, 'coverage_pct': None}
        }
        
        # Check for timeout before major calculation
        if check_timeout():
            logger.warning("Timeout occurred before risk metrics calculation. Returning report with empty risk metrics.")
            report_data["timeout_occurred"] = True
            return report_data
            
        try:
            # Attempt to calculate risk metrics with error protection and timeout limit
            import threading
            import queue
            
            # Use a queue to get the result from the thread
            result_queue = queue.Queue()
            
            def calculate_metrics_with_timeout():
                try:
                    # Calculate risk metrics
                    result = calculate_portfolio_risk_metrics(db, level, level_key, report_date)
                    # Put the result in the queue
                    result_queue.put(result)
                except Exception as e:
                    logger.error(f"Error in risk metrics calculation thread: {str(e)}")
                    # Put an error result in the queue
                    result_queue.put({"success": False, "error": str(e)})
            
            # Start the calculation in a separate thread
            calculation_thread = threading.Thread(target=calculate_metrics_with_timeout)
            calculation_thread.start()
            
            # Wait for the thread to complete or timeout
            calculation_thread.join(timeout=60)  # Increased to 60 second timeout for larger portfolios
            
            # Check if thread is still alive (timeout occurred)
            if calculation_thread.is_alive():
                logger.warning("Risk metrics calculation timed out after 60 seconds")
                report_data["timeout_occurred"] = True
                # Use default risk metrics
                risk_metrics_result = {
                    "success": False, 
                    "error": "Calculation timed out",
                    "risk_metrics": report_data['risk_metrics']
                }
            else:
                # Get the result from the queue
                try:
                    risk_metrics_result = result_queue.get(block=False)
                except queue.Empty:
                    logger.error("Risk metrics calculation thread didn't return a result")
                    risk_metrics_result = {"success": False, "error": "No result from calculation"}
        except Exception as e:
            logger.error(f"Critical error in risk metrics calculation setup: {str(e)}")
            risk_metrics_result = {"success": False, "error": str(e)}
        
        # Only process if calculation was successful
        if risk_metrics_result.get('success', False):
            # Process equity risk metrics
            if 'equity' in risk_metrics_result.get('risk_metrics', {}) and risk_metrics_result['risk_metrics']['equity'] is not None:
                try:
                    equity_metrics = risk_metrics_result['risk_metrics']['equity']
                    
                    # Safely extract values
                    beta_value = equity_metrics.get('beta', {}).get('value')
                    volatility_value = equity_metrics.get('volatility', {}).get('value')
                    coverage_pct = equity_metrics.get('beta', {}).get('coverage_pct')
                    
                    # Create the risk metrics structure with safe conversion
                    report_data['risk_metrics']['equity'] = {
                        'beta': float(beta_value) if beta_value is not None else None,
                        'volatility': float(volatility_value) if volatility_value is not None else None,
                        'coverage_pct': float(coverage_pct) if coverage_pct is not None else None
                    }
                    
                    # Calculate beta adjusted value with safety checks
                    if report_data['risk_metrics']['equity']['beta'] is not None and report_data['equities'].get('total_pct') is not None:
                        try:
                            # Convert Decimal to float before multiplication to avoid type errors
                            equity_total_pct = float(report_data['equities']['total_pct']) if hasattr(report_data['equities']['total_pct'], 'to_float') else float(report_data['equities']['total_pct'])
                            equity_beta = float(report_data['risk_metrics']['equity']['beta']) if hasattr(report_data['risk_metrics']['equity']['beta'], 'to_float') else float(report_data['risk_metrics']['equity']['beta']) 
                            
                            # Beta adjusted = Equity % × Portfolio's Equity Beta
                            report_data['equities']['beta_adjusted'] = (equity_total_pct * equity_beta) / 100.0
                        except (TypeError, ValueError) as e:
                            logger.warning(f"Error calculating beta adjusted value: {str(e)}")
                            report_data['equities']['beta_adjusted'] = None
                    else:
                        report_data['equities']['beta_adjusted'] = None
                        
                except Exception as e:
                    logger.warning(f"Error processing equity risk metrics: {str(e)}")
                    # Keep default values if processing fails
        
        # Process fixed income risk metrics
        if 'fixed_income' in risk_metrics_result.get('risk_metrics', {}) and risk_metrics_result['risk_metrics']['fixed_income'] is not None:
            try:
                fi_metrics = risk_metrics_result['risk_metrics']['fixed_income']
                
                # Initialize with safe default values
                duration_value = None
                coverage_pct = None
                short_duration_pct = None
                market_duration_pct = None
                long_duration_pct = None
                
                # Safely extract values with additional null checks
                if 'duration' in fi_metrics and isinstance(fi_metrics['duration'], dict):
                    duration_value = fi_metrics['duration'].get('value')
                    coverage_pct = fi_metrics['duration'].get('coverage_pct')
                
                # Safely extract percentage values with null checks
                if 'short_duration_pct' in fi_metrics:
                    short_duration_pct = fi_metrics['short_duration_pct']
                if 'market_duration_pct' in fi_metrics:
                    market_duration_pct = fi_metrics['market_duration_pct']
                if 'long_duration_pct' in fi_metrics:
                    long_duration_pct = fi_metrics['long_duration_pct']
                
                # Create the risk metrics structure with safe conversion
                report_data['risk_metrics']['fixed_income'] = {
                    'duration': float(duration_value) if duration_value is not None else None,
                    'coverage_pct': float(coverage_pct) if coverage_pct is not None else None,
                    'short_duration_pct': float(short_duration_pct) if short_duration_pct is not None else None,
                    'market_duration_pct': float(market_duration_pct) if market_duration_pct is not None else None,
                    'long_duration_pct': float(long_duration_pct) if long_duration_pct is not None else None
                }
            except Exception as e:
                logger.warning(f"Error processing fixed income risk metrics: {str(e)}")
                # Keep default values if processing fails
                report_data['risk_metrics']['fixed_income'] = {
                    'duration': None,
                    'coverage_pct': None,
                    'short_duration_pct': None,
                    'market_duration_pct': None,
                    'long_duration_pct': None
                }
                
            # Categorize fixed income durations based on duration values
            # Process durations for municipal bonds, government bonds, and investment grade
            # Update according to our defined duration categories: 
            # - Low/short duration: < 2 years
            # - Market duration: 2-7 years
            # - Long duration: > 7 years
            
            # Get all fixed income positions to categorize by duration
            level_filter = get_level_filter(level, level_key)
            duration_sql = f"""
                SELECT
                    second_level,
                    position,
                    cusip,
                    ticker_symbol,
                    CASE WHEN adjusted_value LIKE 'ENC:%' 
                        THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
                        ELSE CAST(adjusted_value AS NUMERIC) 
                    END as value
                FROM financial_positions
                WHERE date = :date
                AND asset_class = 'Fixed Income'
                AND {level_filter}
            """
            
            fi_positions = db.execute(text(duration_sql), {'date': report_date}).fetchall()
            
            # Get latest risk stats
            latest_risk_stats_date = db.query(func.max(EgnyteRiskStat.import_date)).scalar()
            
            if latest_risk_stats_date:
                # Create dictionaries to track totals by category and duration
                for category in ['municipal_bonds', 'government_bonds', 'investment_grade']:
                    report_data['fixed_income']['subcategories'][category]['short_duration'] = 0.0
                    report_data['fixed_income']['subcategories'][category]['market_duration'] = 0.0
                    report_data['fixed_income']['subcategories'][category]['long_duration'] = 0.0
                
                # Map second level to category
                second_level_to_category = {
                    'municipal bonds': 'municipal_bonds',
                    'government bonds': 'government_bonds',
                    'investment grade': 'investment_grade',
                    'corporate bonds': 'investment_grade'
                }
                
                # Process each position
                for position in fi_positions:
                    second_level = position.second_level.lower() if position.second_level else 'unknown'
                    category = second_level_to_category.get(second_level)
                    
                    if not category:
                        continue
                    
                    # Try to find risk stats for this position
                    risk_stat = db.query(EgnyteRiskStat).filter(
                        EgnyteRiskStat.import_date == latest_risk_stats_date,
                        EgnyteRiskStat.asset_class == 'Fixed Income'
                    )
                    
                    # Try matching by CUSIP
                    if position.cusip:
                        risk_stat = risk_stat.filter(
                            func.lower(EgnyteRiskStat.cusip) == func.lower(position.cusip)
                        ).first()
                    
                    # If not found, try by ticker
                    if not risk_stat and position.ticker_symbol:
                        risk_stat = db.query(EgnyteRiskStat).filter(
                            EgnyteRiskStat.import_date == latest_risk_stats_date,
                            EgnyteRiskStat.asset_class == 'Fixed Income',
                            func.lower(EgnyteRiskStat.ticker_symbol) == func.lower(position.ticker_symbol)
                        ).first()
                    
                    # If still not found, try by position name
                    if not risk_stat:
                        risk_stat = db.query(EgnyteRiskStat).filter(
                            EgnyteRiskStat.import_date == latest_risk_stats_date,
                            EgnyteRiskStat.asset_class == 'Fixed Income',
                            func.lower(EgnyteRiskStat.position).contains(func.lower(position.position))
                        ).first()
                    
                    position_pct = (position.value / total_value * 100) if total_value > 0 else 0.0
                    
                    # Categorize based on duration
                    if risk_stat and risk_stat.duration is not None:
                        duration = float(risk_stat.duration)
                        if duration < 2.0:
                            report_data['fixed_income']['subcategories'][category]['short_duration'] += position_pct
                        elif duration <= 7.0:
                            report_data['fixed_income']['subcategories'][category]['market_duration'] += position_pct
                        else:
                            report_data['fixed_income']['subcategories'][category]['long_duration'] += position_pct
                    else:
                        # Default to market duration if no duration data
                        # This ensures the numbers balance - we put unmatched positions into market duration
                        report_data['fixed_income']['subcategories'][category]['market_duration'] += position_pct
        
        # Process hard currency risk metrics
        if 'hard_currency' in risk_metrics_result.get('risk_metrics', {}) and risk_metrics_result['risk_metrics']['hard_currency'] is not None:
            try:
                hc_metrics = risk_metrics_result['risk_metrics']['hard_currency']
                
                # Safely extract values
                beta_value = hc_metrics.get('beta', {}).get('value')
                coverage_pct = hc_metrics.get('beta', {}).get('coverage_pct')
                
                # Create the risk metrics structure with safe conversion
                report_data['risk_metrics']['hard_currency'] = {
                    'beta': float(beta_value) if beta_value is not None else None,
                    'coverage_pct': float(coverage_pct) if coverage_pct is not None else None
                }
                
                # Calculate beta adjusted value for hard currency with safety checks
                if report_data['risk_metrics']['hard_currency']['beta'] is not None and 'hard_currency' in report_data and 'total_pct' in report_data['hard_currency']:
                    try:
                        # Convert Decimal to float before multiplication to avoid type errors
                        hc_total_pct = float(report_data['hard_currency']['total_pct']) if hasattr(report_data['hard_currency']['total_pct'], 'to_float') else float(report_data['hard_currency']['total_pct'])
                        hc_beta = float(report_data['risk_metrics']['hard_currency']['beta']) if hasattr(report_data['risk_metrics']['hard_currency']['beta'], 'to_float') else float(report_data['risk_metrics']['hard_currency']['beta'])
                        
                        # Beta adjusted = Hard Currency % × Portfolio's Hard Currency Beta
                        report_data['risk_metrics']['hard_currency']['beta_adjusted'] = (hc_total_pct * hc_beta) / 100.0
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Error calculating hard currency beta adjusted value: {str(e)}")
                        report_data['risk_metrics']['hard_currency']['beta_adjusted'] = None
                else:
                    report_data['risk_metrics']['hard_currency']['beta_adjusted'] = None
            except Exception as e:
                logger.warning(f"Error processing hard currency risk metrics: {str(e)}")
                # Keep default values if processing fails
                
    except Exception as e:
        # Log critical error but continue with report generation
        logger.error(f"Error calculating risk metrics for portfolio report: {str(e)}")
    
    return report_data