"""
Service for calculating portfolio risk metrics using risk statistics from Egnyte.

This service takes securities in a portfolio, matches them with risk statistics,
and calculates weighted risk metrics (beta, volatility, duration) by asset class.
"""

import logging
from datetime import date
from typing import Dict, List, Optional, Tuple, Any, Union
from decimal import Decimal

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.models.models import FinancialPosition, EgnyteRiskStat, FinancialSummary

logger = logging.getLogger(__name__)

def calculate_portfolio_risk_metrics(
    db: Session,
    level: str,
    level_key: str,
    report_date: date
) -> Dict[str, Any]:
    """
    Calculate risk metrics for a portfolio based on its positions.
    
    Args:
        db (Session): Database session
        level (str): Level for analysis - 'client', 'portfolio', or 'account'
        level_key (str): The identifier for the specified level
        report_date (date): The date for the report
        
    Returns:
        Dict[str, Any]: Risk metrics for the portfolio, organized by asset class
    """
    logger.info(f"Calculating risk metrics for {level}={level_key}, date={report_date}")
    
    # Get all positions for this portfolio/client/account
    positions_query = db.query(FinancialPosition).filter(
        FinancialPosition.report_date == report_date
    )
    
    # Filter by level
    if level == 'client':
        positions_query = positions_query.filter(FinancialPosition.top_level_client == level_key)
    elif level == 'portfolio':
        positions_query = positions_query.filter(FinancialPosition.portfolio == level_key)
    elif level == 'account':
        positions_query = positions_query.filter(FinancialPosition.holding_account_number == level_key)
    else:
        raise ValueError(f"Invalid level: {level}. Must be 'client', 'portfolio', or 'account'")
    
    positions = positions_query.all()
    
    if not positions:
        logger.warning(f"No positions found for {level}={level_key}, date={report_date}")
        return {
            "success": False,
            "error": f"No positions found for {level}={level_key}, date={report_date}"
        }
    
    # Calculate totals by asset class
    totals = {
        "equity": Decimal('0.0'),
        "fixed_income": Decimal('0.0'),
        "alternatives": Decimal('0.0'),
        "hard_currency": Decimal('0.0'),
        "cash": Decimal('0.0'),
        "total": Decimal('0.0')
    }
    
    # Map asset classes to standardized categories
    asset_class_map = {
        "equity": "equity",
        "equities": "equity",
        "fixed income": "fixed_income",
        "fixed-income": "fixed_income",
        "bond": "fixed_income",
        "bonds": "fixed_income",
        "alternative": "alternatives",
        "alternatives": "alternatives",
        "alternative investment": "alternatives", 
        "hard currency": "hard_currency",
        "precious metal": "hard_currency",
        "precious metals": "hard_currency",
        "gold": "hard_currency",
        "silver": "hard_currency",
        "cash": "cash",
        "cash equivalent": "cash",
        "cash equivalents": "cash"
    }
    
    # Calculate totals by asset class
    for position in positions:
        asset_class = position.asset_class.lower() if position.asset_class else ""
        standardized_class = asset_class_map.get(asset_class, "other")
        
        if standardized_class in totals:
            totals[standardized_class] += position.adjusted_value
        totals["total"] += position.adjusted_value
    
    # Get the latest risk stats import date
    latest_risk_stats_date = db.query(func.max(EgnyteRiskStat.import_date)).scalar()
    
    if not latest_risk_stats_date:
        logger.warning("No risk statistics available")
        return {
            "success": True,
            "message": "Risk metrics calculation skipped - no risk statistics available",
            "totals": totals,
            "risk_metrics": {}
        }
    
    # Initialize risk metrics
    risk_metrics = {
        "equity": {
            "beta": {
                "weighted_sum": Decimal('0.0'),
                "coverage_pct": Decimal('0.0')
            },
            "volatility": {
                "weighted_sum": Decimal('0.0'),
                "coverage_pct": Decimal('0.0')
            }
        },
        "fixed_income": {
            "duration": {
                "weighted_sum": Decimal('0.0'),
                "coverage_pct": Decimal('0.0')
            }
        },
        "hard_currency": {
            "beta": {
                "weighted_sum": Decimal('0.0'),
                "coverage_pct": Decimal('0.0')
            }
        },
        "alternatives": {
            "beta": {
                "weighted_sum": Decimal('0.0'),
                "coverage_pct": Decimal('0.0')
            }
        }
    }
    
    # Process positions by asset class to calculate weighted risk metrics
    process_equity_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
    process_fixed_income_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
    process_hard_currency_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
    process_alternatives_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
    
    # Calculate final metrics by dividing weighted sums by coverage percentage
    finalize_risk_metrics(risk_metrics)
    
    return {
        "success": True,
        "totals": totals,
        "risk_metrics": risk_metrics,
        "latest_risk_stats_date": latest_risk_stats_date.isoformat(),
        "report_date": report_date.isoformat()
    }

def process_equity_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date
) -> None:
    """Process equity positions to calculate weighted beta and volatility."""
    # Skip if no equity positions
    if totals["equity"] == Decimal('0.0'):
        return
    
    equity_positions = [p for p in positions if 
                       p.asset_class and p.asset_class.lower() in ['equity', 'equities']]
    
    matched_value = Decimal('0.0')
    
    for position in equity_positions:
        # Try to find the risk stats by first using position name
        risk_stat = find_matching_risk_stat(
            db, position.position, position.cusip, position.ticker_symbol, 
            'Equity', latest_risk_stats_date
        )
        
        if risk_stat:
            position_weight = position.adjusted_value / totals["equity"]
            
            # Beta calculation
            if risk_stat.beta is not None:
                weighted_beta = position_weight * risk_stat.beta
                risk_metrics["equity"]["beta"]["weighted_sum"] += Decimal(str(weighted_beta))
            
            # Volatility calculation
            if risk_stat.volatility is not None:
                weighted_vol = position_weight * risk_stat.volatility
                risk_metrics["equity"]["volatility"]["weighted_sum"] += Decimal(str(weighted_vol))
            
            matched_value += position.adjusted_value
    
    # Calculate coverage
    if totals["equity"] > Decimal('0.0'):
        coverage = (matched_value / totals["equity"]) * 100
        risk_metrics["equity"]["beta"]["coverage_pct"] = coverage
        risk_metrics["equity"]["volatility"]["coverage_pct"] = coverage

def process_fixed_income_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date
) -> None:
    """Process fixed income positions to calculate weighted duration."""
    # Skip if no fixed income positions
    if totals["fixed_income"] == Decimal('0.0'):
        return
    
    fi_positions = [p for p in positions if 
                   p.asset_class and p.asset_class.lower() in ['fixed income', 'fixed-income', 'bond', 'bonds']]
    
    matched_value = Decimal('0.0')
    
    for position in fi_positions:
        # Try to find the risk stats
        risk_stat = find_matching_risk_stat(
            db, position.position, position.cusip, position.ticker_symbol, 
            'Fixed Income', latest_risk_stats_date
        )
        
        if risk_stat:
            position_weight = position.adjusted_value / totals["fixed_income"]
            
            # Duration calculation
            if risk_stat.duration is not None:
                weighted_duration = position_weight * risk_stat.duration
                risk_metrics["fixed_income"]["duration"]["weighted_sum"] += Decimal(str(weighted_duration))
            
            matched_value += position.adjusted_value
    
    # Calculate coverage
    if totals["fixed_income"] > Decimal('0.0'):
        coverage = (matched_value / totals["fixed_income"]) * 100
        risk_metrics["fixed_income"]["duration"]["coverage_pct"] = coverage

def process_hard_currency_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date
) -> None:
    """Process hard currency positions to calculate weighted beta."""
    # Skip if no hard currency positions
    if totals["hard_currency"] == Decimal('0.0'):
        return
    
    hc_positions = [p for p in positions if 
                   p.asset_class and p.asset_class.lower() in 
                   ['hard currency', 'precious metal', 'precious metals', 'gold', 'silver']]
    
    matched_value = Decimal('0.0')
    
    for position in hc_positions:
        # Try to find the risk stats (may be in Alternatives tab for gold-linked assets)
        risk_stat = find_matching_risk_stat(
            db, position.position, position.cusip, position.ticker_symbol, 
            'Alternatives', latest_risk_stats_date
        )
        
        if not risk_stat:
            # Try in Equity tab as fallback
            risk_stat = find_matching_risk_stat(
                db, position.position, position.cusip, position.ticker_symbol, 
                'Equity', latest_risk_stats_date
            )
        
        if risk_stat:
            position_weight = position.adjusted_value / totals["hard_currency"]
            
            # Beta calculation
            if risk_stat.beta is not None:
                weighted_beta = position_weight * risk_stat.beta
                risk_metrics["hard_currency"]["beta"]["weighted_sum"] += Decimal(str(weighted_beta))
            
            matched_value += position.adjusted_value
    
    # Calculate coverage
    if totals["hard_currency"] > Decimal('0.0'):
        coverage = (matched_value / totals["hard_currency"]) * 100
        risk_metrics["hard_currency"]["beta"]["coverage_pct"] = coverage

def process_alternatives_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date
) -> None:
    """Process alternatives positions to calculate weighted beta."""
    # Skip if no alternatives positions
    if totals["alternatives"] == Decimal('0.0'):
        return
    
    alt_positions = [p for p in positions if 
                    p.asset_class and p.asset_class.lower() in 
                    ['alternative', 'alternatives', 'alternative investment']]
    
    matched_value = Decimal('0.0')
    
    for position in alt_positions:
        # Try to find the risk stats
        risk_stat = find_matching_risk_stat(
            db, position.position, position.cusip, position.ticker_symbol, 
            'Alternatives', latest_risk_stats_date
        )
        
        if risk_stat:
            position_weight = position.adjusted_value / totals["alternatives"]
            
            # Beta calculation
            if risk_stat.beta is not None:
                weighted_beta = position_weight * risk_stat.beta
                risk_metrics["alternatives"]["beta"]["weighted_sum"] += Decimal(str(weighted_beta))
            
            matched_value += position.adjusted_value
    
    # Calculate coverage
    if totals["alternatives"] > Decimal('0.0'):
        coverage = (matched_value / totals["alternatives"]) * 100
        risk_metrics["alternatives"]["beta"]["coverage_pct"] = coverage

def find_matching_risk_stat(
    db: Session,
    position_name: str,
    cusip: Optional[str],
    ticker_symbol: Optional[str],
    asset_class: str,
    latest_date: date
) -> Optional[EgnyteRiskStat]:
    """
    Find a matching risk statistic for a position using different identifiers.
    
    Try matching in this order of priority:
    1. CUSIP (if provided)
    2. Ticker symbol (if provided)
    3. Position name
    
    Args:
        db (Session): Database session
        position_name (str): Name of the position/security
        cusip (Optional[str]): CUSIP identifier if available
        ticker_symbol (Optional[str]): Ticker symbol if available
        asset_class (str): Asset class to match ('Equity', 'Fixed Income', 'Alternatives')
        latest_date (date): Latest date of risk stat import
        
    Returns:
        Optional[EgnyteRiskStat]: Matching risk statistic or None
    """
    # Try matching by CUSIP first (most reliable)
    if cusip:
        risk_stat = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.cusip == cusip,
            EgnyteRiskStat.asset_class == asset_class,
            EgnyteRiskStat.import_date == latest_date
        ).first()
        
        if risk_stat:
            return risk_stat
    
    # Try matching by ticker symbol
    if ticker_symbol:
        risk_stat = db.query(EgnyteRiskStat).filter(
            EgnyteRiskStat.ticker_symbol == ticker_symbol,
            EgnyteRiskStat.asset_class == asset_class,
            EgnyteRiskStat.import_date == latest_date
        ).first()
        
        if risk_stat:
            return risk_stat
    
    # Finally, try matching by position name
    risk_stat = db.query(EgnyteRiskStat).filter(
        EgnyteRiskStat.position == position_name,
        EgnyteRiskStat.asset_class == asset_class,
        EgnyteRiskStat.import_date == latest_date
    ).first()
    
    return risk_stat

def finalize_risk_metrics(risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]]) -> None:
    """
    Finalize risk metrics by calculating actual values from weighted sums and coverage.
    
    Args:
        risk_metrics (Dict): Risk metrics dictionary with weighted sums and coverage percentages
    """
    # Process equity metrics
    if risk_metrics["equity"]["beta"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["equity"]["beta"]["value"] = risk_metrics["equity"]["beta"]["weighted_sum"]
    else:
        risk_metrics["equity"]["beta"]["value"] = None
        
    if risk_metrics["equity"]["volatility"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["equity"]["volatility"]["value"] = risk_metrics["equity"]["volatility"]["weighted_sum"]
    else:
        risk_metrics["equity"]["volatility"]["value"] = None
    
    # Process fixed income metrics
    if risk_metrics["fixed_income"]["duration"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["fixed_income"]["duration"]["value"] = risk_metrics["fixed_income"]["duration"]["weighted_sum"]
    else:
        risk_metrics["fixed_income"]["duration"]["value"] = None
    
    # Process hard currency metrics
    if risk_metrics["hard_currency"]["beta"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["hard_currency"]["beta"]["value"] = risk_metrics["hard_currency"]["beta"]["weighted_sum"]
    else:
        risk_metrics["hard_currency"]["beta"]["value"] = None
    
    # Process alternatives metrics
    if risk_metrics["alternatives"]["beta"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["alternatives"]["beta"]["value"] = risk_metrics["alternatives"]["beta"]["weighted_sum"]
    else:
        risk_metrics["alternatives"]["beta"]["value"] = None