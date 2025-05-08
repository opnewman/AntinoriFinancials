"""
Portfolio Report Service - Optimized Database Queries

This service uses direct PostgreSQL queries to efficiently generate portfolio reports
according to the ANTINORI categorization schema:

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

from sqlalchemy.orm import Session
from sqlalchemy import text, func, distinct

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
    AND asset_class = 'equity'
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
    AND asset_class = 'fixed income'
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
    AND asset_class = 'alternatives'
    AND second_level = 'hard currency'
    AND {level_filter}
    GROUP BY third_level
"""

SQL_UNCORRELATED_ALTERNATIVES = """
    SELECT
        position,
        third_level,
        CASE WHEN adjusted_value LIKE 'ENC:%' 
            THEN CAST(SUBSTRING(adjusted_value, 5) AS NUMERIC) 
            ELSE CAST(adjusted_value AS NUMERIC) 
        END as adjusted_value
    FROM financial_positions
    WHERE date = :date
    AND asset_class = 'alternatives'
    AND second_level != 'hard currency'
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
        level: The hierarchy level ('client', 'portfolio', 'account')
        level_key: The identifier for the specified level
        
    Returns:
        SQL string for filtering
    """
    if level == 'client':
        return f"top_level_client = '{level_key}'"
    elif level == 'portfolio':
        return f"portfolio = '{level_key}'"
    elif level == 'account':
        return f"holding_account_number = '{level_key}'"
    else:
        raise ValueError(f"Invalid level: {level}")


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
        asset_class = row.asset_class.lower() if row.asset_class else 'unknown'
        value = float(row.total_value) if row.total_value else 0.0
        percentage = (value / total_value * 100) if total_value > 0 else 0.0
        
        if asset_class == 'equity':
            breakdowns['equities']['total_pct'] = percentage
        elif asset_class == 'fixed income':
            breakdowns['fixed_income']['total_pct'] = percentage
        elif asset_class == 'cash & cash equivalent':
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
        third_level = row.third_level.lower() if row.third_level else ''
        value = float(row.adjusted_value) if row.adjusted_value else 0.0
        percentage = (value / total_value * 100) if total_value > 0 else 0.0
        
        total_alternatives_value += value
        
        # Categorize based on the rules
        if third_level == 'crypto':
            subcategories['crypto'] += percentage
        elif 'proficio short term alts fund' in position:
            subcategories['proficio_short_term'] += percentage
        elif 'proficio long term alts fund' in position:
            subcategories['proficio_long_term'] += percentage
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
                    for subcat in report_data[asset_class]['subcategories']:
                        pct = report_data[asset_class]['subcategories'][subcat]
                        report_data[asset_class]['subcategories'][subcat + '_value'] = (pct / 100.0) * total_value
        
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
    
    return report_data