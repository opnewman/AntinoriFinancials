"""
Service for calculating portfolio risk metrics using risk statistics from Egnyte.

This service takes securities in a portfolio, matches them with risk statistics,
and calculates weighted risk metrics (beta, volatility, duration) by asset class.
"""

import logging
from datetime import date
from typing import Dict, List, Optional, Tuple, Any, Union
from decimal import Decimal, InvalidOperation

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.models.models import (
    FinancialPosition, EgnyteRiskStat, FinancialSummary,
    RiskStatisticEquity, RiskStatisticFixedIncome, RiskStatisticAlternatives
)
from src.utils.encryption import encryption_service

logger = logging.getLogger(__name__)

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
    if not position_value:
        logger.debug(f"Empty position value for {position_name}. Using 0.")
        return Decimal('0.0')
    
    # Handle SQLAlchemy Column objects
    if hasattr(position_value, 'key') and hasattr(position_value, 'type'):
        try:
            # This is a SQLAlchemy Column object, extract the string value
            logger.debug(f"Converting SQLAlchemy Column to string for {position_name}")
            position_value = str(position_value)
        except Exception as e:
            logger.warning(f"Failed to convert SQLAlchemy Column to string for {position_name}: {str(e)}. Using 0.")
            return Decimal('0.0')
            
    # Handle SQLAlchemy Column objects in position_name as well
    if hasattr(position_name, 'key') and hasattr(position_name, 'type'):
        try:
            position_name = str(position_name)
        except Exception:
            position_name = "unknown"
    
    # Handle non-string inputs (safety check)
    if not isinstance(position_value, str):
        try:
            # Try direct conversion for numeric types
            return Decimal(str(position_value))
        except (ValueError, TypeError, InvalidOperation) as e:
            logger.warning(f"Non-string position value '{position_value}' for {position_name} couldn't be converted. Error: {str(e)}. Using 0.")
            return Decimal('0.0')
    
    # Special value handling
    special_values = ['N/A', '#N/A', 'None', 'NULL', '#N/A Invalid Security']
    if any(val.lower() in position_value.lower() for val in special_values):
        logger.debug(f"Special value '{position_value}' for {position_name}. Using 0.")
        return Decimal('0.0')
        
    try:
        # Check if the value is encrypted (starts with "ENC:")
        if position_value.startswith('ENC:'):
            # Extract the encrypted part (remove "ENC:" prefix)
            encrypted_part = position_value[4:]
            # Decrypt the value and convert to Decimal
            decrypted_value = encryption_service.decrypt(encrypted_part)
            if decrypted_value:
                # Remove any currency symbols, commas, parentheses (negative values), and whitespace
                cleaned_value = decrypted_value.replace(',', '').replace('$', '').replace('(', '-').replace(')', '').strip()
                return Decimal(cleaned_value)
            else:
                logger.warning(f"Decryption failed for position {position_name}. Using 0.")
                return Decimal('0.0')
        else:
            # Regular non-encrypted value
            # Remove any currency symbols, commas, parentheses (negative values), and whitespace
            cleaned_value = position_value.replace(',', '').replace('$', '').replace('(', '-').replace(')', '').strip()
            
            # Handle percentage values (convert to decimal)
            if cleaned_value.endswith('%'):
                return Decimal(cleaned_value.rstrip('%')) / 100
                
            return Decimal(cleaned_value)
    except (ValueError, TypeError, InvalidOperation) as e:
        logger.warning(f"Could not convert value '{position_value}' to Decimal for position {position_name}. Error: {str(e)}. Using 0.")
        return Decimal('0.0')

def calculate_portfolio_risk_metrics(
    db: Session,
    level: str,
    level_key: str,
    report_date: date,
    max_positions: Optional[int] = None
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
    
    try:
        # Get all positions for this portfolio/client/account
        positions_query = db.query(FinancialPosition).filter(
            FinancialPosition.date == report_date
        )
        
        # Filter by level
        if level == 'client':
            # Special handling for "All Clients" - don't filter by client
            if level_key != "All Clients":
                positions_query = positions_query.filter(FinancialPosition.top_level_client == level_key)
        elif level == 'portfolio':
            positions_query = positions_query.filter(FinancialPosition.portfolio == level_key)
        elif level == 'account':
            positions_query = positions_query.filter(FinancialPosition.holding_account_number == level_key)
        else:
            raise ValueError(f"Invalid level: {level}. Must be 'client', 'portfolio', or 'account'")
        
        # Check if we have any positions before loading them all
        position_count = positions_query.count()
        
        if not position_count:
            logger.warning(f"No positions found for {level}={level_key}, date={report_date}")
            return {
                "success": False,
                "error": f"No positions found for {level}={level_key}, date={report_date}"
            }
            
        logger.info(f"Found {position_count} positions for {level}={level_key}, date={report_date}")
            
        # For large portfolios (>1000 positions), use a more efficient approach with caching
        is_large_portfolio = position_count > 1000
        cache = {} if is_large_portfolio else None
        
        if is_large_portfolio:
            logger.info(f"Using optimized approach with caching for large portfolio with {position_count} positions")
        
        # Apply sampling for large position sets if max_positions is specified
        if max_positions and position_count > max_positions:
            logger.info(f"Using sampling: selecting {max_positions} positions out of {position_count}")
            # Use random sampling for statistical representation
            positions = positions_query.order_by(func.random()).limit(max_positions).all()
            # Flag that we're using a sample
            is_sample = True
            sample_size = max_positions
            total_positions = position_count
        else:
            # Load all positions (no sampling)
            positions = positions_query.all()
            is_sample = False
            sample_size = None
            total_positions = position_count
    except Exception as e:
        logger.exception(f"Error querying positions: {str(e)}")
        return {
            "success": False,
            "error": f"Error querying positions: {str(e)}"
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
        
        # Convert adjusted_value from string to Decimal using the utility function
        adjusted_value_decimal = convert_position_value_to_decimal(position.adjusted_value, position.position)
            
        if standardized_class in totals:
            totals[standardized_class] += adjusted_value_decimal
        totals["total"] += adjusted_value_decimal
    
    # Calculate percentages of each asset class in the total portfolio
    percentages = {}
    for asset_class, value in totals.items():
        if asset_class != "total" and totals["total"] > Decimal('0.0'):
            percentages[asset_class] = (value / totals["total"]) * 100
        else:
            percentages[asset_class] = Decimal('0.0')
    
    # Get the latest risk stats upload date from new tables - always use the most recent available stats
    # instead of trying to match the report date exactly
    latest_equity_date = db.query(func.max(RiskStatisticEquity.upload_date)).scalar()
    latest_fixed_income_date = db.query(func.max(RiskStatisticFixedIncome.upload_date)).scalar()
    latest_alternatives_date = db.query(func.max(RiskStatisticAlternatives.upload_date)).scalar()
    
    # Determine the overall latest date from all tables
    latest_dates = []
    if latest_equity_date:
        latest_dates.append(latest_equity_date)
    if latest_fixed_income_date:
        latest_dates.append(latest_fixed_income_date)
    if latest_alternatives_date:
        latest_dates.append(latest_alternatives_date)
    
    latest_risk_stats_date = max(latest_dates) if latest_dates else None
    
    if not latest_risk_stats_date:
        logger.warning("No risk statistics available in the database")
        return {
            "success": True,
            "message": "Risk metrics calculation skipped - no risk statistics available",
            "totals": totals,
            "percentages": percentages,
            "risk_metrics": {}
        }
        
    # Log the date difference if there is one
    if latest_risk_stats_date != report_date:
        logger.info(f"Using risk stats from {latest_risk_stats_date} for report date {report_date}")
        
    # Get counts of available risk stats by asset class for this date
    equity_count = db.query(RiskStatisticEquity).filter(
        RiskStatisticEquity.upload_date == latest_risk_stats_date
    ).count()
    
    fixed_income_count = db.query(RiskStatisticFixedIncome).filter(
        RiskStatisticFixedIncome.upload_date == latest_risk_stats_date
    ).count()
    
    alternatives_count = db.query(RiskStatisticAlternatives).filter(
        RiskStatisticAlternatives.upload_date == latest_risk_stats_date
    ).count()
    
    total_risk_stats_count = equity_count + fixed_income_count + alternatives_count
    
    logger.info(f"Found {total_risk_stats_count} total risk statistics for date {latest_risk_stats_date}")
    logger.info(f"Equity: {equity_count}, Fixed Income: {fixed_income_count}, Alternatives: {alternatives_count}")
    
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
            },
            "volatility": {
                "weighted_sum": Decimal('0.0'),
                "coverage_pct": Decimal('0.0')
            }
        },
        "portfolio": {
            "beta_adjusted": Decimal('0.0'),
            "total_beta": Decimal('0.0')
        }
    }
    
    # Process positions by asset class to calculate weighted risk metrics
    if is_large_portfolio:
        logger.info("Using cache-optimized risk stat lookup for large portfolio")
        # Add time tracking for debugging
        import time
        start_time = time.time()
        processed_count = 0
        
        # Check if a sample size was specified by the user, or if this is a large portfolio
        if max_positions or position_count > 10000:
            sample_size = max_positions if max_positions else 2000  # Use specified sample size or default
            logger.warning(f"Extremely large portfolio with {position_count} positions. Using sampling technique with sample size {sample_size}.")
            sampled_positions = []
            
            # Sample positions by asset class to ensure good coverage
            equity_positions = [p for p in positions if p.asset_class and "equity" in p.asset_class.lower()]
            fixed_income_positions = [p for p in positions if p.asset_class and "fixed income" in p.asset_class.lower()]
            hard_currency_positions = [p for p in positions if p.asset_class and "hard currency" in p.asset_class.lower()]
            alternative_positions = [p for p in positions if p.asset_class and "alternative" in p.asset_class.lower()]
            
            # Calculate proportions based on total positions
            equity_count = len(equity_positions)
            fixed_income_count = len(fixed_income_positions)
            hard_currency_count = len(hard_currency_positions)
            alternative_count = len(alternative_positions)
            
            # Calculate sample sizes proportionally
            total_asset_count = equity_count + fixed_income_count + hard_currency_count + alternative_count
            if total_asset_count > 0:
                equity_sample = min(equity_count, int((equity_count / total_asset_count) * sample_size))
                fixed_income_sample = min(fixed_income_count, int((fixed_income_count / total_asset_count) * sample_size))
                hard_currency_sample = min(hard_currency_count, int((hard_currency_count / total_asset_count) * sample_size))
                alternative_sample = min(alternative_count, int((alternative_count / total_asset_count) * sample_size))
                
                # Ensure we take at least some from each category if available
                equity_sample = max(equity_sample, min(equity_count, 100))
                fixed_income_sample = max(fixed_income_sample, min(fixed_income_count, 100))
                hard_currency_sample = max(hard_currency_sample, min(hard_currency_count, 100))
                alternative_sample = max(alternative_sample, min(alternative_count, 100))
                
                # Take samples
                import random
                if equity_count > 0:
                    sampled_positions.extend(random.sample(equity_positions, equity_sample))
                if fixed_income_count > 0:
                    sampled_positions.extend(random.sample(fixed_income_positions, fixed_income_sample))
                if hard_currency_count > 0:
                    sampled_positions.extend(random.sample(hard_currency_positions, hard_currency_sample))
                if alternative_count > 0:
                    sampled_positions.extend(random.sample(alternative_positions, alternative_sample))
                
                logger.info(f"Sampled {len(sampled_positions)} positions out of {position_count} total positions")
                # Use the sampled positions instead of all positions
                positions = sampled_positions
            
        # Pass the cache for optimized lookups
        position_count = len(positions)
        batch_size = 1000
        for i in range(0, position_count, batch_size):
            batch = positions[i:i+batch_size]
            batch_start_time = time.time()
            logger.info(f"Processing batch {i}-{i+len(batch)} of {position_count}")
            
            for position in batch:
                processed_count += 1
                # Try to find the risk stats with cache
                asset_class = position.asset_class.lower() if position.asset_class else ""
                standardized_class = asset_class_map.get(asset_class, "other")
                
                # Log progress every 5000 positions
                if processed_count % 5000 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Processed {processed_count}/{position_count} positions ({processed_count/position_count*100:.1f}%) in {elapsed:.1f} seconds")
            
            # Get the full asset class name for risk stat lookup
            asset_class_name = None
            if standardized_class == "equity":
                asset_class_name = "Equity"
                
                risk_stat = find_matching_risk_stat(
                    db, position.position, position.cusip, position.ticker_symbol, 
                    asset_class_name, latest_risk_stats_date, cache
                )
                
                if risk_stat and totals["equity"] > Decimal('0.0'):
                    try:
                        # Convert adjusted_value from string to Decimal
                        adjusted_value_decimal = convert_position_value_to_decimal(position.adjusted_value, position.position)
                        position_weight = adjusted_value_decimal / totals["equity"]
                        
                        # Beta calculation
                        if risk_stat.beta is not None:
                            try:
                                # Safe conversion to ensure we have a valid Decimal
                                beta_value = Decimal(str(risk_stat.beta))
                                weighted_beta = position_weight * beta_value
                                risk_metrics["equity"]["beta"]["weighted_sum"] += weighted_beta
                                
                                # Track matched value for coverage
                                if "matched_value" not in risk_metrics["equity"]:
                                    risk_metrics["equity"]["matched_value"] = Decimal('0.0')
                                risk_metrics["equity"]["matched_value"] += adjusted_value_decimal
                            except (ValueError, TypeError, InvalidOperation) as e:
                                logger.warning(f"Invalid beta value for {position.position}: {risk_stat.beta}. Error: {str(e)}")
                        
                        # Volatility calculation
                        if risk_stat.vol is not None:
                            try:
                                # Safe conversion to ensure we have a valid Decimal
                                volatility_value = Decimal(str(risk_stat.vol))
                                weighted_vol = position_weight * volatility_value
                                risk_metrics["equity"]["volatility"]["weighted_sum"] += weighted_vol
                            except (ValueError, TypeError, InvalidOperation) as e:
                                logger.warning(f"Invalid volatility value for {position.position}: {risk_stat.vol}. Error: {str(e)}")
                    except Exception as e:
                        logger.warning(f"Error processing equity position {position.position}: {str(e)}")
                            
            elif standardized_class == "fixed_income":
                asset_class_name = "Fixed Income"
                
                risk_stat = find_matching_risk_stat(
                    db, position.position, position.cusip, position.ticker_symbol, 
                    asset_class_name, latest_risk_stats_date, cache
                )
                
                if risk_stat and totals["fixed_income"] > Decimal('0.0'):
                    try:
                        # Convert adjusted_value from string to Decimal
                        adjusted_value_decimal = convert_position_value_to_decimal(position.adjusted_value, position.position)
                        position_weight = adjusted_value_decimal / totals["fixed_income"]
                        
                        # Duration calculation
                        if risk_stat.duration is not None:
                            try:
                                # Safe conversion to ensure we have a valid Decimal
                                duration_value = Decimal(str(risk_stat.duration))
                                weighted_duration = position_weight * duration_value
                                risk_metrics["fixed_income"]["duration"]["weighted_sum"] += weighted_duration
                                
                                # Track matched value for coverage
                                if "matched_value" not in risk_metrics["fixed_income"]:
                                    risk_metrics["fixed_income"]["matched_value"] = Decimal('0.0')
                                risk_metrics["fixed_income"]["matched_value"] += adjusted_value_decimal
                            except (ValueError, TypeError, InvalidOperation) as e:
                                logger.warning(f"Invalid duration value for {position.position}: {risk_stat.duration}. Error: {str(e)}")
                    except Exception as e:
                        logger.warning(f"Error processing fixed income position {position.position}: {str(e)}")
                        
            elif standardized_class == "hard_currency":
                asset_class_name = "Hard Currency"
                
                risk_stat = find_matching_risk_stat(
                    db, position.position, position.cusip, position.ticker_symbol, 
                    asset_class_name, latest_risk_stats_date, cache
                )
                
                if risk_stat and totals["hard_currency"] > Decimal('0.0'):
                    try:
                        # Convert adjusted_value from string to Decimal
                        adjusted_value_decimal = convert_position_value_to_decimal(position.adjusted_value, position.position)
                        position_weight = adjusted_value_decimal / totals["hard_currency"]
                        
                        # Beta calculation
                        if risk_stat.beta is not None:
                            try:
                                # Safe conversion to ensure we have a valid Decimal
                                beta_value = Decimal(str(risk_stat.beta))
                                weighted_beta = position_weight * beta_value
                                risk_metrics["hard_currency"]["beta"]["weighted_sum"] += weighted_beta
                                
                                # Track matched value for coverage
                                if "matched_value" not in risk_metrics["hard_currency"]:
                                    risk_metrics["hard_currency"]["matched_value"] = Decimal('0.0')
                                risk_metrics["hard_currency"]["matched_value"] += adjusted_value_decimal
                            except (ValueError, TypeError, InvalidOperation) as e:
                                logger.warning(f"Invalid beta value for {position.position}: {risk_stat.beta}. Error: {str(e)}")
                    except Exception as e:
                        logger.warning(f"Error processing hard currency position {position.position}: {str(e)}")
                
            elif standardized_class == "alternatives":
                asset_class_name = "Alternative"
                
                risk_stat = find_matching_risk_stat(
                    db, position.position, position.cusip, position.ticker_symbol, 
                    asset_class_name, latest_risk_stats_date, cache
                )
                
                if not risk_stat:
                    # Try with "Alternatives" if "Alternative" didn't match
                    risk_stat = find_matching_risk_stat(
                        db, position.position, position.cusip, position.ticker_symbol, 
                        "Alternatives", latest_risk_stats_date, cache
                    )
                
                if risk_stat and totals["alternatives"] > Decimal('0.0'):
                    try:
                        # Convert adjusted_value from string to Decimal
                        adjusted_value_decimal = convert_position_value_to_decimal(position.adjusted_value, position.position)
                        position_weight = adjusted_value_decimal / totals["alternatives"]
                        
                        # Beta calculation
                        if risk_stat.beta is not None:
                            try:
                                # Safe conversion to ensure we have a valid Decimal
                                beta_value = Decimal(str(risk_stat.beta))
                                weighted_beta = position_weight * beta_value
                                risk_metrics["alternatives"]["beta"]["weighted_sum"] += weighted_beta
                                
                                # Track matched value for coverage
                                if "matched_value" not in risk_metrics["alternatives"]:
                                    risk_metrics["alternatives"]["matched_value"] = Decimal('0.0')
                                risk_metrics["alternatives"]["matched_value"] += adjusted_value_decimal
                            except (ValueError, TypeError, InvalidOperation) as e:
                                logger.warning(f"Invalid beta value for {position.position}: {risk_stat.beta}. Error: {str(e)}")
                    except Exception as e:
                        logger.warning(f"Error processing alternatives position {position.position}: {str(e)}")
        
        # Calculate coverage percentages
        if "matched_value" in risk_metrics["equity"] and totals["equity"] > Decimal('0.0'):
            coverage = (risk_metrics["equity"]["matched_value"] / totals["equity"]) * 100
            risk_metrics["equity"]["beta"]["coverage_pct"] = coverage
            risk_metrics["equity"]["volatility"]["coverage_pct"] = coverage
            
        if "matched_value" in risk_metrics["fixed_income"] and totals["fixed_income"] > Decimal('0.0'):
            coverage = (risk_metrics["fixed_income"]["matched_value"] / totals["fixed_income"]) * 100
            risk_metrics["fixed_income"]["duration"]["coverage_pct"] = coverage
            
        if "matched_value" in risk_metrics["hard_currency"] and totals["hard_currency"] > Decimal('0.0'):
            coverage = (risk_metrics["hard_currency"]["matched_value"] / totals["hard_currency"]) * 100
            risk_metrics["hard_currency"]["beta"]["coverage_pct"] = coverage
            
        if "matched_value" in risk_metrics["alternatives"] and totals["alternatives"] > Decimal('0.0'):
            coverage = (risk_metrics["alternatives"]["matched_value"] / totals["alternatives"]) * 100
            risk_metrics["alternatives"]["beta"]["coverage_pct"] = coverage
            if "volatility" in risk_metrics["alternatives"]:
                risk_metrics["alternatives"]["volatility"]["coverage_pct"] = coverage
    else:
        # For smaller portfolios, use the original approach without caching
        process_equity_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
        process_fixed_income_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
        process_hard_currency_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
        process_alternatives_risk(db, positions, totals, risk_metrics, latest_risk_stats_date)
    
    # Calculate final metrics by dividing weighted sums by coverage percentage
    # and compute beta-adjusted values based on asset class percentages
    finalize_risk_metrics(risk_metrics, percentages)
    
    # Create response with sampling information if applicable
    response = {
        "success": True,
        "totals": totals,
        "percentages": percentages,
        "risk_metrics": risk_metrics,
        "latest_risk_stats_date": latest_risk_stats_date.isoformat(),
        "report_date": report_date.isoformat()
    }
    
    # Add sampling information if sampling was used
    if is_sample:
        response["is_sampled"] = True
        response["sample_size"] = sample_size
        response["total_positions"] = total_positions
        response["performance_notes"] = f"Results calculated using a sample of {sample_size} positions out of {total_positions} total positions for improved performance."
    else:
        response["is_sampled"] = False
    
    # Add entity name for display in UI
    if level == 'client':
        response["entity_name"] = level_key
    elif level == 'portfolio':
        response["entity_name"] = f"Portfolio: {level_key}"
    elif level == 'account':
        response["entity_name"] = f"Account: {level_key}"
    
    return response

def process_equity_risk(
    db: Session,
    positions: List[FinancialPosition],
    totals: Dict[str, Decimal],
    risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]],
    latest_risk_stats_date: date
) -> None:
    """Process equity positions to calculate weighted beta and volatility."""
    # Enhanced logging for debugging
    logger.info("Starting equity risk processing")
    
    # Skip if no equity positions
    if totals["equity"] == Decimal('0.0'):
        logger.info("No equity positions to process")
        return
    
    # Safely extract asset class string values, handling SQLAlchemy Column objects
    equity_positions = []
    for p in positions:
        try:
            # Get the asset class, handle if it's a SQLAlchemy Column object
            asset_class_val = getattr(p, 'asset_class', None)
            if hasattr(asset_class_val, 'key') and hasattr(asset_class_val, 'type'):
                asset_class_val = str(asset_class_val)
                
            # Check if it's an equity position
            if asset_class_val and isinstance(asset_class_val, str) and asset_class_val.lower() in ['equity', 'equities']:
                equity_positions.append(p)
        except Exception as e:
            logger.warning(f"Error processing position asset class: {str(e)}")
    logger.info(f"Found {len(equity_positions)} equity positions to process")
    
    matched_value = Decimal('0.0')
    
    for position in equity_positions:
        try:
            # Safely extract position attributes
            position_name = getattr(position, 'position', None)
            cusip = getattr(position, 'cusip', None)
            ticker_symbol = getattr(position, 'ticker_symbol', None)
            
            # Convert SQLAlchemy Column objects to strings if needed
            if hasattr(position_name, 'key') and hasattr(position_name, 'type'):
                position_name = str(position_name)
            if hasattr(cusip, 'key') and hasattr(cusip, 'type'):
                cusip = str(cusip)
            if hasattr(ticker_symbol, 'key') and hasattr(ticker_symbol, 'type'):
                ticker_symbol = str(ticker_symbol)
                
            logger.debug(f"Finding risk stat for {position_name} (CUSIP: {cusip}, Ticker: {ticker_symbol}) in Equity asset class")
            
            # Try to find the risk stats by first using position name
            risk_stat = find_matching_risk_stat(
                db, position_name, cusip, ticker_symbol, 
                'Equity', latest_risk_stats_date
            )
        except Exception as e:
            logger.error(f"Error processing equity position: {str(e)}")
            continue
        
        if risk_stat:
            try:
                # Safely extract adjusted_value attribute
                adjusted_value = getattr(position, 'adjusted_value', None)
                pos_name_for_log = getattr(position, 'position', "unknown")
                
                # Convert SQLAlchemy Column objects to strings if needed
                if hasattr(adjusted_value, 'key') and hasattr(adjusted_value, 'type'):
                    adjusted_value = str(adjusted_value)
                if hasattr(pos_name_for_log, 'key') and hasattr(pos_name_for_log, 'type'):
                    pos_name_for_log = str(pos_name_for_log)
                
                # Convert adjusted_value from string to Decimal using the utility function
                adjusted_value_decimal = convert_position_value_to_decimal(adjusted_value, pos_name_for_log)
                
                position_weight = adjusted_value_decimal / totals["equity"]
                
                # Beta calculation
                if risk_stat.beta is not None:
                    try:
                        # Safe conversion to ensure we have a valid Decimal
                        beta_value = Decimal(str(risk_stat.beta))
                        weighted_beta = position_weight * beta_value
                        risk_metrics["equity"]["beta"]["weighted_sum"] += weighted_beta
                    except (ValueError, TypeError, InvalidOperation) as e:
                        logger.warning(f"Invalid beta value for {pos_name_for_log}: {risk_stat.beta}. Error: {str(e)}")
                
                # Volatility calculation - try both field names (vol and volatility)
                volatility_value = None
                
                # First check if 'volatility' attribute exists and has a value
                if hasattr(risk_stat, 'volatility') and risk_stat.volatility is not None:
                    try:
                        volatility_value = Decimal(str(risk_stat.volatility))
                    except (ValueError, TypeError, InvalidOperation) as e:
                        logger.warning(f"Invalid volatility value for {pos_name_for_log}: {risk_stat.volatility}. Error: {str(e)}")
                
                # If not found, try 'vol' attribute as a fallback
                if volatility_value is None and hasattr(risk_stat, 'vol') and risk_stat.vol is not None:
                    try:
                        volatility_value = Decimal(str(risk_stat.vol))
                    except (ValueError, TypeError, InvalidOperation) as e:
                        logger.warning(f"Invalid vol value for {pos_name_for_log}: {risk_stat.vol}. Error: {str(e)}")
                
                # If we found a valid volatility value from either field, use it
                if volatility_value is not None:
                    weighted_vol = position_weight * volatility_value
                    risk_metrics["equity"]["volatility"]["weighted_sum"] += weighted_vol
                
                # Track the total matched value for coverage calculation
                matched_value += adjusted_value_decimal
            except Exception as e:
                logger.error(f"Error calculating position metrics: {str(e)}")
                continue
    
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
    # Enhanced logging for debugging
    logger.info("Starting fixed income risk processing")
    
    # Skip if no fixed income positions
    if totals["fixed_income"] == Decimal('0.0'):
        logger.info("No fixed income positions to process")
        return
    
    # Safely extract asset class string values, handling SQLAlchemy Column objects
    fi_positions = []
    for p in positions:
        try:
            # Get the asset class, handle if it's a SQLAlchemy Column object
            asset_class_val = getattr(p, 'asset_class', None)
            if hasattr(asset_class_val, 'key') and hasattr(asset_class_val, 'type'):
                asset_class_val = str(asset_class_val)
                
            # Check if it's a fixed income position
            if asset_class_val and isinstance(asset_class_val, str) and asset_class_val.lower() in ['fixed income', 'fixed-income', 'bond', 'bonds']:
                fi_positions.append(p)
        except Exception as e:
            logger.warning(f"Error processing fixed income position asset class: {str(e)}")
    logger.info(f"Found {len(fi_positions)} fixed income positions to process")
    
    matched_value = Decimal('0.0')
    
    for position in fi_positions:
        try:
            # Safely extract position attributes
            position_name = getattr(position, 'position', None)
            cusip = getattr(position, 'cusip', None)
            ticker_symbol = getattr(position, 'ticker_symbol', None)
            
            # Convert SQLAlchemy Column objects to strings if needed
            if hasattr(position_name, 'key') and hasattr(position_name, 'type'):
                position_name = str(position_name)
            if hasattr(cusip, 'key') and hasattr(cusip, 'type'):
                cusip = str(cusip)
            if hasattr(ticker_symbol, 'key') and hasattr(ticker_symbol, 'type'):
                ticker_symbol = str(ticker_symbol)
                
            logger.debug(f"Finding risk stat for {position_name} (CUSIP: {cusip}, Ticker: {ticker_symbol}) in Fixed Income asset class")
            
            # Try to find the risk stats
            risk_stat = find_matching_risk_stat(
                db, position_name, cusip, ticker_symbol, 
                'Fixed Income', latest_risk_stats_date
            )
        except Exception as e:
            logger.error(f"Error processing fixed income position: {str(e)}")
            continue
        
        if risk_stat:
            try:
                # Safely extract adjusted_value attribute
                adjusted_value = getattr(position, 'adjusted_value', None)
                pos_name_for_log = getattr(position, 'position', "unknown")
                
                # Convert SQLAlchemy Column objects to strings if needed
                if hasattr(adjusted_value, 'key') and hasattr(adjusted_value, 'type'):
                    adjusted_value = str(adjusted_value)
                if hasattr(pos_name_for_log, 'key') and hasattr(pos_name_for_log, 'type'):
                    pos_name_for_log = str(pos_name_for_log)
                
                # Convert adjusted_value from string to Decimal using the utility function
                adjusted_value_decimal = convert_position_value_to_decimal(adjusted_value, pos_name_for_log)
                
                position_weight = adjusted_value_decimal / totals["fixed_income"]
                
                # Duration calculation
                if risk_stat.duration is not None:
                    try:
                        # Safe conversion to ensure we have a valid Decimal
                        duration_value = Decimal(str(risk_stat.duration))
                        weighted_duration = position_weight * duration_value
                        risk_metrics["fixed_income"]["duration"]["weighted_sum"] += weighted_duration
                    except (ValueError, TypeError, InvalidOperation) as e:
                        logger.warning(f"Invalid duration value for {pos_name_for_log}: {risk_stat.duration}. Error: {str(e)}")
                
                # Track the total matched value for coverage calculation
                matched_value += adjusted_value_decimal
            except Exception as e:
                logger.error(f"Error calculating fixed income position metrics: {str(e)}")
                continue
    
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
    # Enhanced logging for debugging
    logger.info("Starting hard currency risk processing")
    
    # Skip if no hard currency positions
    if totals["hard_currency"] == Decimal('0.0'):
        logger.info("No hard currency positions to process")
        return
    
    # Safely extract asset class string values, handling SQLAlchemy Column objects
    hc_positions = []
    for p in positions:
        try:
            # Get the asset class, handle if it's a SQLAlchemy Column object
            asset_class_val = getattr(p, 'asset_class', None)
            if hasattr(asset_class_val, 'key') and hasattr(asset_class_val, 'type'):
                asset_class_val = str(asset_class_val)
                
            # Check if it's a hard currency position
            if asset_class_val and isinstance(asset_class_val, str) and asset_class_val.lower() in ['hard currency', 'precious metal', 'precious metals', 'gold', 'silver']:
                hc_positions.append(p)
        except Exception as e:
            logger.warning(f"Error processing hard currency position asset class: {str(e)}")
    logger.info(f"Found {len(hc_positions)} hard currency positions to process")
    
    matched_value = Decimal('0.0')
    
    for position in hc_positions:
        try:
            # Safely extract position attributes
            position_name = getattr(position, 'position', None)
            cusip = getattr(position, 'cusip', None)
            ticker_symbol = getattr(position, 'ticker_symbol', None)
            
            # Convert SQLAlchemy Column objects to strings if needed
            if hasattr(position_name, 'key') and hasattr(position_name, 'type'):
                position_name = str(position_name)
            if hasattr(cusip, 'key') and hasattr(cusip, 'type'):
                cusip = str(cusip)
            if hasattr(ticker_symbol, 'key') and hasattr(ticker_symbol, 'type'):
                ticker_symbol = str(ticker_symbol)
                
            logger.debug(f"Finding risk stat for {position_name} (CUSIP: {cusip}, Ticker: {ticker_symbol}) in Hard Currency asset class")
            
            # Try to find the risk stats (may be in Alternatives tab for gold-linked assets)
            risk_stat = find_matching_risk_stat(
                db, position_name, cusip, ticker_symbol, 
                'Alternatives', latest_risk_stats_date
            )
            
            if not risk_stat:
                # Try in Equity tab as fallback
                risk_stat = find_matching_risk_stat(
                    db, position_name, cusip, ticker_symbol, 
                    'Equity', latest_risk_stats_date
                )
                
            if risk_stat:
                try:
                    # Safely extract adjusted_value attribute
                    adjusted_value = getattr(position, 'adjusted_value', None)
                    pos_name_for_log = getattr(position, 'position', "unknown")
                    
                    # Convert SQLAlchemy Column objects to strings if needed
                    if hasattr(adjusted_value, 'key') and hasattr(adjusted_value, 'type'):
                        adjusted_value = str(adjusted_value)
                    if hasattr(pos_name_for_log, 'key') and hasattr(pos_name_for_log, 'type'):
                        pos_name_for_log = str(pos_name_for_log)
                    
                    # Convert adjusted_value from string to Decimal using the utility function
                    adjusted_value_decimal = convert_position_value_to_decimal(adjusted_value, pos_name_for_log)
                    
                    position_weight = adjusted_value_decimal / totals["hard_currency"]
                    
                    # Beta calculation
                    if risk_stat.beta is not None:
                        try:
                            # Safe conversion to ensure we have a valid Decimal
                            beta_value = Decimal(str(risk_stat.beta))
                            weighted_beta = position_weight * beta_value
                            risk_metrics["hard_currency"]["beta"]["weighted_sum"] += weighted_beta
                        except (ValueError, TypeError, InvalidOperation) as e:
                            logger.warning(f"Invalid beta value for hard currency {pos_name_for_log}: {risk_stat.beta}. Error: {str(e)}")
                    
                    # Track the total matched value for coverage calculation
                    matched_value += adjusted_value_decimal
                except Exception as e:
                    logger.error(f"Error calculating hard currency position metrics: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Error processing hard currency position: {str(e)}")
            continue
    
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
    # Enhanced logging for debugging
    logger.info("Starting alternatives risk processing")
    
    # Skip if no alternatives positions
    if totals["alternatives"] == Decimal('0.0'):
        logger.info("No alternatives positions to process")
        return
    
    # Safely extract asset class string values, handling SQLAlchemy Column objects
    alt_positions = []
    for p in positions:
        try:
            # Get the asset class, handle if it's a SQLAlchemy Column object
            asset_class_val = getattr(p, 'asset_class', None)
            if hasattr(asset_class_val, 'key') and hasattr(asset_class_val, 'type'):
                asset_class_val = str(asset_class_val)
                
            # Check if it's an alternatives position
            if asset_class_val and isinstance(asset_class_val, str) and asset_class_val.lower() in ['alternative', 'alternatives', 'alternative investment']:
                alt_positions.append(p)
        except Exception as e:
            logger.warning(f"Error processing alternatives position asset class: {str(e)}")
    logger.info(f"Found {len(alt_positions)} alternatives positions to process")
    
    matched_value = Decimal('0.0')
    
    for position in alt_positions:
        try:
            # Safely extract position attributes
            position_name = getattr(position, 'position', None)
            cusip = getattr(position, 'cusip', None)
            ticker_symbol = getattr(position, 'ticker_symbol', None)
            
            # Convert SQLAlchemy Column objects to strings if needed
            if hasattr(position_name, 'key') and hasattr(position_name, 'type'):
                position_name = str(position_name)
            if hasattr(cusip, 'key') and hasattr(cusip, 'type'):
                cusip = str(cusip)
            if hasattr(ticker_symbol, 'key') and hasattr(ticker_symbol, 'type'):
                ticker_symbol = str(ticker_symbol)
                
            logger.debug(f"Finding risk stat for {position_name} (CUSIP: {cusip}, Ticker: {ticker_symbol}) in Alternatives asset class")
            
            # Try to find the risk stats
            risk_stat = find_matching_risk_stat(
                db, position_name, cusip, ticker_symbol, 
                'Alternatives', latest_risk_stats_date
            )
        except Exception as e:
            logger.error(f"Error processing alternatives position: {str(e)}")
            continue
        
        if risk_stat:
            try:
                # Safely extract adjusted_value attribute
                adjusted_value = getattr(position, 'adjusted_value', None)
                pos_name_for_log = getattr(position, 'position', "unknown")
                
                # Convert SQLAlchemy Column objects to strings if needed
                if hasattr(adjusted_value, 'key') and hasattr(adjusted_value, 'type'):
                    adjusted_value = str(adjusted_value)
                if hasattr(pos_name_for_log, 'key') and hasattr(pos_name_for_log, 'type'):
                    pos_name_for_log = str(pos_name_for_log)
                
                # Convert adjusted_value from string to Decimal using the utility function
                adjusted_value_decimal = convert_position_value_to_decimal(adjusted_value, pos_name_for_log)
                
                position_weight = adjusted_value_decimal / totals["alternatives"]
                
                # Beta calculation
                if risk_stat.beta is not None:
                    try:
                        # Safe conversion to ensure we have a valid Decimal
                        beta_value = Decimal(str(risk_stat.beta))
                        weighted_beta = position_weight * beta_value
                        risk_metrics["alternatives"]["beta"]["weighted_sum"] += weighted_beta
                    except (ValueError, TypeError, InvalidOperation) as e:
                        logger.warning(f"Invalid beta value for alternative {pos_name_for_log}: {risk_stat.beta}. Error: {str(e)}")
                        
                # Volatility calculation - try both field names (vol and volatility) if we're tracking volatility for alternatives
                if "volatility" in risk_metrics["alternatives"]:
                    volatility_value = None
                    
                    # First check if 'volatility' attribute exists and has a value
                    if hasattr(risk_stat, 'volatility') and risk_stat.volatility is not None:
                        try:
                            volatility_value = Decimal(str(risk_stat.volatility))
                        except (ValueError, TypeError, InvalidOperation) as e:
                            logger.warning(f"Invalid volatility value for alternative {pos_name_for_log}: {risk_stat.volatility}. Error: {str(e)}")
                    
                    # If not found, try 'vol' attribute as a fallback
                    if volatility_value is None and hasattr(risk_stat, 'vol') and risk_stat.vol is not None:
                        try:
                            volatility_value = Decimal(str(risk_stat.vol))
                        except (ValueError, TypeError, InvalidOperation) as e:
                            logger.warning(f"Invalid vol value for alternative {pos_name_for_log}: {risk_stat.vol}. Error: {str(e)}")
                    
                    # If we found a valid volatility value from either field, use it
                    if volatility_value is not None:
                        weighted_vol = position_weight * volatility_value
                        risk_metrics["alternatives"]["volatility"]["weighted_sum"] += weighted_vol
                
                # Track the total matched value for coverage calculation
                matched_value += adjusted_value_decimal
            except Exception as e:
                logger.error(f"Error calculating alternatives position metrics: {str(e)}")
                continue
    
    # Calculate coverage
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
    
    Uses the new table structure with separate tables for each asset class.
    Simplified and optimized implementation to avoid timeout errors.
    
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
    # Safety check for inputs
    if not position_name and not cusip and not ticker_symbol:
        return None
        
    if not asset_class:
        return None
        
    # Determine model class without complex logic
    model_class = None
    asset_class_str = str(asset_class).lower()
    
    if "equity" in asset_class_str:
        model_class = RiskStatisticEquity
    elif "fixed" in asset_class_str:
        model_class = RiskStatisticFixedIncome
    elif "alternative" in asset_class_str:
        model_class = RiskStatisticAlternatives
    elif "hard" in asset_class_str and "currency" in asset_class_str:
        model_class = RiskStatisticAlternatives
    else:
        return None
        
    # Convert strings safely
    try:
        safe_position = str(position_name).strip() if position_name else ""
        safe_cusip = str(cusip).strip() if cusip else ""
        safe_ticker = str(ticker_symbol).strip() if ticker_symbol else ""
    except:
        # If conversion fails, use empty strings
        safe_position = ""
        safe_cusip = ""
        safe_ticker = ""
        
    # Sanitize inputs to prevent database errors
    # Remove non-ASCII characters that cause encoding errors
    def sanitize(text):
        if not text:
            return ""
        # Keep only ASCII printable characters
        return ''.join(c for c in text if ord(c) < 128 and c.isprintable())
        
    safe_position = sanitize(safe_position)
    safe_cusip = sanitize(safe_cusip)  
    safe_ticker = sanitize(safe_ticker)
    
    # Check cache first (no database query)
    if cache is not None:
        cache_prefix = asset_class_str.replace(" ", "_")
        
        if safe_cusip and f"{cache_prefix}:cusip:{safe_cusip}" in cache:
            return cache[f"{cache_prefix}:cusip:{safe_cusip}"]
            
        if safe_ticker and f"{cache_prefix}:ticker:{safe_ticker}" in cache:
            return cache[f"{cache_prefix}:ticker:{safe_ticker}"]
            
        if safe_position and f"{cache_prefix}:position:{safe_position}" in cache:
            return cache[f"{cache_prefix}:position:{safe_position}"]
    
    # Find match in database with minimal risk of errors
    try:
        # Try by CUSIP first (most reliable)
        if safe_cusip:
            try:
                query = f"""
                    SELECT id, upload_date, position, ticker_symbol, cusip,
                           beta, volatility, vol, duration
                    FROM {model_class.__tablename__}
                    WHERE cusip = '{safe_cusip}'
                    AND upload_date = '{latest_date}'
                    LIMIT 1
                """
                
                result = db.execute(text(query)).first()
                if result:
                    # Convert to dictionary
                    columns = ['id', 'upload_date', 'position', 'ticker_symbol', 'cusip', 
                               'beta', 'volatility', 'vol', 'duration']
                    risk_stat = {col: result[i] for i, col in enumerate(columns) if i < len(result)}
                    return risk_stat
            except Exception as e:
                logger.warning(f"CUSIP lookup error: {str(e)}")
        
        # Try by ticker
        if safe_ticker:
            try:
                query = f"""
                    SELECT id, upload_date, position, ticker_symbol, cusip,
                           beta, volatility, vol, duration
                    FROM {model_class.__tablename__}
                    WHERE ticker_symbol = '{safe_ticker}'
                    AND upload_date = '{latest_date}'
                    LIMIT 1
                """
                
                result = db.execute(text(query)).first()
                if result:
                    # Convert to dictionary
                    columns = ['id', 'upload_date', 'position', 'ticker_symbol', 'cusip', 
                               'beta', 'volatility', 'vol', 'duration']
                    risk_stat = {col: result[i] for i, col in enumerate(columns) if i < len(result)}
                    return risk_stat
            except Exception as e:
                logger.warning(f"Ticker lookup error: {str(e)}")
        
        # Try by position exactly (most efficient)
        if safe_position:
            try:
                query = f"""
                    SELECT id, upload_date, position, ticker_symbol, cusip,
                           beta, volatility, vol, duration
                    FROM {model_class.__tablename__}
                    WHERE position = '{safe_position}'
                    AND upload_date = '{latest_date}'
                    LIMIT 1
                """
                
                result = db.execute(text(query)).first()
                if result:
                    # Convert to dictionary
                    columns = ['id', 'upload_date', 'position', 'ticker_symbol', 'cusip', 
                               'beta', 'volatility', 'vol', 'duration']
                    risk_stat = {col: result[i] for i, col in enumerate(columns) if i < len(result)}
                    return risk_stat
            except Exception as e:
                logger.warning(f"Position lookup error: {str(e)}")
                
        # No matches found
        return None
        
    except Exception as e:
        logger.error(f"Critical database error in risk stat lookup: {str(e)}")
        return None

def finalize_risk_metrics(risk_metrics: Dict[str, Dict[str, Dict[str, Decimal]]], percentages: Dict[str, Decimal]) -> None:
    """
    Finalize risk metrics by calculating actual values from weighted sums and coverage.
    Also calculates beta-adjusted values based on asset class percentages.
    
    Args:
        risk_metrics (Dict): Risk metrics dictionary with weighted sums and coverage percentages
        percentages (Dict): Percentages of each asset class in the total portfolio
    """
    # Process equity metrics
    if risk_metrics["equity"]["beta"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["equity"]["beta"]["value"] = risk_metrics["equity"]["beta"]["weighted_sum"]
        # Calculate equity beta-adjusted value (equity % * equity beta)
        equity_beta_adjusted = (percentages["equity"] / 100) * risk_metrics["equity"]["beta"]["weighted_sum"]
        risk_metrics["equity"]["beta"]["beta_adjusted"] = equity_beta_adjusted
    else:
        risk_metrics["equity"]["beta"]["value"] = None
        risk_metrics["equity"]["beta"]["beta_adjusted"] = Decimal('0.0')
        
    if risk_metrics["equity"]["volatility"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["equity"]["volatility"]["value"] = risk_metrics["equity"]["volatility"]["weighted_sum"]
    else:
        risk_metrics["equity"]["volatility"]["value"] = None
    
    # Process fixed income metrics
    if risk_metrics["fixed_income"]["duration"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["fixed_income"]["duration"]["value"] = risk_metrics["fixed_income"]["duration"]["weighted_sum"]
        
        # Categorize duration into buckets for report
        duration_value = risk_metrics["fixed_income"]["duration"]["weighted_sum"]
        if duration_value < 2:
            risk_metrics["fixed_income"]["duration"]["category"] = "short_duration"
        elif duration_value < 7:
            risk_metrics["fixed_income"]["duration"]["category"] = "market_duration"
        else:
            risk_metrics["fixed_income"]["duration"]["category"] = "long_duration"
            
    else:
        risk_metrics["fixed_income"]["duration"]["value"] = None
        risk_metrics["fixed_income"]["duration"]["category"] = "unknown"
    
    # Process hard currency metrics
    if risk_metrics["hard_currency"]["beta"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["hard_currency"]["beta"]["value"] = risk_metrics["hard_currency"]["beta"]["weighted_sum"]
        
        # Calculate hard currency beta-adjusted value (hard currency % * hard currency beta)
        hc_beta_adjusted = (percentages["hard_currency"] / 100) * risk_metrics["hard_currency"]["beta"]["weighted_sum"]
        risk_metrics["hard_currency"]["beta"]["beta_adjusted"] = hc_beta_adjusted
    else:
        risk_metrics["hard_currency"]["beta"]["value"] = None
        risk_metrics["hard_currency"]["beta"]["beta_adjusted"] = Decimal('0.0')
    
    # Process alternatives metrics
    # First handle beta
    if risk_metrics["alternatives"]["beta"]["coverage_pct"] > Decimal('0.0'):
        risk_metrics["alternatives"]["beta"]["value"] = risk_metrics["alternatives"]["beta"]["weighted_sum"]
        
        # Calculate alternatives beta-adjusted value (alternatives % * alternatives beta)
        alt_beta_adjusted = (percentages["alternatives"] / 100) * risk_metrics["alternatives"]["beta"]["weighted_sum"]
        risk_metrics["alternatives"]["beta"]["beta_adjusted"] = alt_beta_adjusted
    else:
        risk_metrics["alternatives"]["beta"]["value"] = None
        risk_metrics["alternatives"]["beta"]["beta_adjusted"] = Decimal('0.0')
        
    # Then handle volatility
    if "volatility" in risk_metrics["alternatives"]:
        if risk_metrics["alternatives"]["volatility"]["coverage_pct"] > Decimal('0.0'):
            risk_metrics["alternatives"]["volatility"]["value"] = risk_metrics["alternatives"]["volatility"]["weighted_sum"]
        else:
            risk_metrics["alternatives"]["volatility"]["value"] = None
    
    # Calculate total portfolio beta-adjusted metrics
    # Sum up all the beta-adjusted values
    total_beta_adjusted = Decimal('0.0')
    
    # Add equity beta-adjusted if available
    if "beta_adjusted" in risk_metrics["equity"]["beta"]:
        total_beta_adjusted += risk_metrics["equity"]["beta"]["beta_adjusted"]
    
    # Add hard currency beta-adjusted if available
    if "beta_adjusted" in risk_metrics["hard_currency"]["beta"]:
        total_beta_adjusted += risk_metrics["hard_currency"]["beta"]["beta_adjusted"]
    
    # Add alternatives beta-adjusted if available
    if "beta_adjusted" in risk_metrics["alternatives"]["beta"]:
        total_beta_adjusted += risk_metrics["alternatives"]["beta"]["beta_adjusted"]
    
    # Store the total beta-adjusted value in the portfolio section
    risk_metrics["portfolio"]["beta_adjusted"] = total_beta_adjusted
    
    # Log the results for debugging
    logger.info(f"Portfolio beta-adjusted value: {total_beta_adjusted}")