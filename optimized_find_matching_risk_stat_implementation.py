"""
Optimized version of the find_matching_risk_stat function
"""
import re
import logging
import string
from datetime import date
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.models.models import (
    RiskStatisticEquity,
    RiskStatisticFixedIncome,
    RiskStatisticAlternatives
)

logger = logging.getLogger(__name__)

# Dictionary to track securities with no matching risk statistics
UNMATCHED_SECURITIES = {
    "Equity": set(),
    "Fixed Income": set(),
    "Alternatives": set(),
    "Hard Currency": set()
}

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
        asset_class_key = "equity"
    elif "fixed" in asset_class_str:
        model_class = RiskStatisticFixedIncome
        asset_class_key = "fixed_income"
    elif "alternative" in asset_class_str:
        model_class = RiskStatisticAlternatives
        asset_class_key = "alternatives"
    elif "hard" in asset_class_str and "currency" in asset_class_str:
        model_class = RiskStatisticAlternatives
        asset_class_key = "hard_currency"
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
    safe_ticker = ultra_sanitize(ticker_symbol).lower() if ticker_symbol else ""
    
    # Check cache first - no database access needed if we have a cached value
    if cache is not None:
        # Try all identifiers in order of reliability
        if safe_cusip and safe_cusip in cache.get(asset_class_key, {}).get("cusip", {}):
            return cache[asset_class_key]["cusip"][safe_cusip]
            
        if safe_ticker and safe_ticker in cache.get(asset_class_key, {}).get("ticker_symbol", {}):
            return cache[asset_class_key]["ticker_symbol"][safe_ticker]
            
        if safe_position and safe_position in cache.get(asset_class_key, {}).get("position", {}):
            return cache[asset_class_key]["position"][safe_position]
    
    # Use the session provided instead of creating a new connection for each query
    # This significantly improves performance for large portfolios
    try:
        # Get the appropriate query based on asset class
        matched_value = None
        if asset_class_key == "fixed_income":
            # Try CUSIP first for fixed income (most reliable for bonds)
            if safe_cusip:
                query = db.query(RiskStatisticFixedIncome).filter(
                    func.lower(RiskStatisticFixedIncome.cusip) == safe_cusip.lower(),
                    RiskStatisticFixedIncome.upload_date == latest_date
                ).first()
                
                if query:
                    matched_value = {"id": query.id, "duration": query.duration}
                    # Cache the result
                    if cache is not None and "cusip" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["cusip"][safe_cusip] = matched_value
            
            # Try ticker symbol next if no match yet
            if not matched_value and safe_ticker and safe_ticker != '-':
                # First try exact match
                query = db.query(RiskStatisticFixedIncome).filter(
                    func.lower(RiskStatisticFixedIncome.ticker_symbol) == safe_ticker.lower(),
                    RiskStatisticFixedIncome.upload_date == latest_date
                ).first()
                
                # If no match, try fuzzy match with partial ticker
                if not query and len(safe_ticker) > 3:
                    query = db.query(RiskStatisticFixedIncome).filter(
                        RiskStatisticFixedIncome.ticker_symbol.ilike(f"%{safe_ticker}%"),
                        RiskStatisticFixedIncome.upload_date == latest_date
                    ).first()
                
                if query:
                    matched_value = {"id": query.id, "duration": query.duration}
                    # Cache the result
                    if cache is not None and "ticker_symbol" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["ticker_symbol"][safe_ticker] = matched_value
            
            # Try position name last if still no match
            if not matched_value and safe_position:
                query = db.query(RiskStatisticFixedIncome).filter(
                    func.lower(RiskStatisticFixedIncome.position) == safe_position.lower(),
                    RiskStatisticFixedIncome.upload_date == latest_date
                ).first()
                
                if query:
                    matched_value = {"id": query.id, "duration": query.duration}
                    # Cache the result
                    if cache is not None and "position" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["position"][safe_position] = matched_value
                
                # For bond names, try various partial matching strategies
                if not matched_value:
                    words = safe_position.split()
                    
                    # Strategy 1: Match with first three words
                    if len(words) >= 3:
                        first_three = f"{words[0]} {words[1]} {words[2]}"
                        query = db.query(RiskStatisticFixedIncome).filter(
                            RiskStatisticFixedIncome.position.ilike(f"%{first_three}%"),
                            RiskStatisticFixedIncome.upload_date == latest_date
                        ).first()
                        
                        if query:
                            matched_value = {"id": query.id, "duration": query.duration}
                            # Cache the result
                            if cache is not None and "position" in cache.get(asset_class_key, {}):
                                cache[asset_class_key]["position"][safe_position] = matched_value
                    
                    # Strategy 2: Match with first two words
                    if not matched_value and len(words) >= 2:
                        first_two = f"{words[0]} {words[1]}"
                        if len(first_two) >= 5:  # Only use if it's substantial enough
                            query = db.query(RiskStatisticFixedIncome).filter(
                                RiskStatisticFixedIncome.position.ilike(f"%{first_two}%"),
                                RiskStatisticFixedIncome.upload_date == latest_date
                            ).first()
                            
                            if query:
                                matched_value = {"id": query.id, "duration": query.duration}
                                # Cache the result
                                if cache is not None and "position" in cache.get(asset_class_key, {}):
                                    cache[asset_class_key]["position"][safe_position] = matched_value
                    
                    # Strategy 3: Try matching bond maturity patterns like "5.5% 2025"
                    if not query:
                        # Look for patterns like "X.X% 20XX" which are common in bond names
                        try:
                            maturity_pattern = re.search(r'(\d+\.?\d*)\%.*?20(\d{2})', safe_position)
                            if maturity_pattern:
                                rate = maturity_pattern.group(1)
                                year = maturity_pattern.group(2)
                                pattern = f"{rate}%{' '}20{year}"
                                query = db.query(RiskStatisticFixedIncome).filter(
                                    RiskStatisticFixedIncome.position.ilike(f"%{pattern}%"),
                                    RiskStatisticFixedIncome.upload_date == latest_date
                                ).first()
                        except Exception as e:
                            logger.warning(f"Error matching bond pattern: {e}")
                            
                            # Fallback approach: extract percentage and year through string parsing
                            try:
                                # Try to find a pattern like "5.625%" or "5.625 %"
                                parts = safe_position.split('%')
                                if len(parts) > 1:
                                    # Get the part before the % sign, which should be the rate
                                    rate_part = parts[0].split()[-1].strip()
                                    
                                    # Now look for a year after the % sign
                                    after_percent = parts[1]
                                    year_match = None
                                    
                                    # Look for patterns like "2033" or "20XX"
                                    for word in after_percent.split():
                                        word = word.strip('.,;:()')
                                        if word.isdigit() and len(word) == 4 and word.startswith('20'):
                                            year_match = word[2:]  # Just get the last 2 digits
                                            break
                                    
                                    if rate_part and year_match:
                                        pattern = f"{rate_part}%{' '}20{year_match}"
                                        query = db.query(RiskStatisticFixedIncome).filter(
                                            RiskStatisticFixedIncome.position.ilike(f"%{pattern}%"),
                                            RiskStatisticFixedIncome.upload_date == latest_date
                                        ).first()
                            except Exception as e:
                                logger.warning(f"Error in fallback bond pattern matching: {e}")
                        
                        if query:
                            matched_value = {"id": query.id, "duration": query.duration}
                            # Cache the result
                            if cache is not None and "position" in cache.get(asset_class_key, {}):
                                cache[asset_class_key]["position"][safe_position] = matched_value
        
        elif asset_class_key == "equity":
            # Try ticker symbol first for equities (most reliable)
            if safe_ticker and safe_ticker != '-':
                query = db.query(RiskStatisticEquity).filter(
                    func.lower(RiskStatisticEquity.ticker_symbol) == safe_ticker.lower(),
                    RiskStatisticEquity.upload_date == latest_date
                ).first()
                
                if query:
                    # Get beta and volatility values correctly, checking both fields
                    beta = query.beta if hasattr(query, 'beta') else None
                    volatility = None
                    
                    # First try direct volatility field
                    if hasattr(query, 'volatility') and query.volatility is not None:
                        volatility = query.volatility
                    # Then try the vol field
                    elif hasattr(query, 'vol') and query.vol is not None:
                        volatility = query.vol
                    
                    matched_value = {"id": query.id, "beta": beta, "volatility": volatility}
                    # Cache the result
                    if cache is not None and "ticker_symbol" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["ticker_symbol"][safe_ticker] = matched_value
            
            # Try CUSIP for derivatives if no match yet
            if not matched_value and safe_cusip:
                query = db.query(RiskStatisticEquity).filter(
                    func.lower(RiskStatisticEquity.cusip) == safe_cusip.lower(),
                    RiskStatisticEquity.upload_date == latest_date
                ).first()
                
                if query:
                    # Get beta and volatility values correctly, checking both fields
                    beta = query.beta if hasattr(query, 'beta') else None
                    volatility = None
                    
                    # First try direct volatility field
                    if hasattr(query, 'volatility') and query.volatility is not None:
                        volatility = query.volatility
                    # Then try the vol field
                    elif hasattr(query, 'vol') and query.vol is not None:
                        volatility = query.vol
                    
                    matched_value = {"id": query.id, "beta": beta, "volatility": volatility}
                    # Cache the result
                    if cache is not None and "cusip" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["cusip"][safe_cusip] = matched_value
            
            # Try position name last if still no match
            if not matched_value and safe_position:
                query = db.query(RiskStatisticEquity).filter(
                    func.lower(RiskStatisticEquity.position) == safe_position.lower(),
                    RiskStatisticEquity.upload_date == latest_date
                ).first()
                
                if query:
                    # Get beta and volatility values correctly, checking both fields
                    beta = query.beta if hasattr(query, 'beta') else None
                    volatility = None
                    
                    # First try direct volatility field
                    if hasattr(query, 'volatility') and query.volatility is not None:
                        volatility = query.volatility
                    # Then try the vol field
                    elif hasattr(query, 'vol') and query.vol is not None:
                        volatility = query.vol
                    
                    matched_value = {"id": query.id, "beta": beta, "volatility": volatility}
                    # Cache the result
                    if cache is not None and "position" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["position"][safe_position] = matched_value
        
        else:  # Alternatives or Hard Currency
            # Try all identifiers for alternatives/hard currency
            if safe_cusip:
                query = db.query(RiskStatisticAlternatives).filter(
                    func.lower(RiskStatisticAlternatives.cusip) == safe_cusip.lower(),
                    RiskStatisticAlternatives.upload_date == latest_date
                ).first()
                
                if query:
                    # Get beta and volatility values correctly, checking both fields
                    beta = query.beta if hasattr(query, 'beta') else None
                    volatility = None
                    
                    # First try direct volatility field
                    if hasattr(query, 'volatility') and query.volatility is not None:
                        volatility = query.volatility
                    # Then try the vol field
                    elif hasattr(query, 'vol') and query.vol is not None:
                        volatility = query.vol
                    
                    matched_value = {"id": query.id, "beta": beta, "volatility": volatility}
                    # Cache the result
                    if cache is not None and "cusip" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["cusip"][safe_cusip] = matched_value
            
            # Try ticker symbol if no match yet
            if not matched_value and safe_ticker and safe_ticker != '-':
                query = db.query(RiskStatisticAlternatives).filter(
                    func.lower(RiskStatisticAlternatives.ticker_symbol) == safe_ticker.lower(),
                    RiskStatisticAlternatives.upload_date == latest_date
                ).first()
                
                if query:
                    # Get beta and volatility values correctly, checking both fields
                    beta = query.beta if hasattr(query, 'beta') else None
                    volatility = None
                    
                    # First try direct volatility field
                    if hasattr(query, 'volatility') and query.volatility is not None:
                        volatility = query.volatility
                    # Then try the vol field
                    elif hasattr(query, 'vol') and query.vol is not None:
                        volatility = query.vol
                    
                    matched_value = {"id": query.id, "beta": beta, "volatility": volatility}
                    # Cache the result
                    if cache is not None and "ticker_symbol" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["ticker_symbol"][safe_ticker] = matched_value
            
            # Try position name last if still no match
            if not matched_value and safe_position:
                query = db.query(RiskStatisticAlternatives).filter(
                    func.lower(RiskStatisticAlternatives.position) == safe_position.lower(),
                    RiskStatisticAlternatives.upload_date == latest_date
                ).first()
                
                if query:
                    # Get beta and volatility values correctly, checking both fields
                    beta = query.beta if hasattr(query, 'beta') else None
                    volatility = None
                    
                    # First try direct volatility field
                    if hasattr(query, 'volatility') and query.volatility is not None:
                        volatility = query.volatility
                    # Then try the vol field
                    elif hasattr(query, 'vol') and query.vol is not None:
                        volatility = query.vol
                    
                    matched_value = {"id": query.id, "beta": beta, "volatility": volatility}
                    # Cache the result
                    if cache is not None and "position" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["position"][safe_position] = matched_value
        
        if matched_value:
            return matched_value
            
    except Exception as e:
        logger.warning(f"Database query error for {asset_class_key} ({position_name}): {str(e)}")
    
    # Track this unmatched security for reporting
    track_unmatched_security(position_name, asset_class)
    
    # Return None if no match found
    return None

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
        
    # Map variations to standard asset class names
    asset_class_str = str(asset_class).lower()
    if "equity" in asset_class_str:
        UNMATCHED_SECURITIES["Equity"].add(str(position_name))
    elif "fixed" in asset_class_str:
        UNMATCHED_SECURITIES["Fixed Income"].add(str(position_name))
    elif "alternative" in asset_class_str:
        UNMATCHED_SECURITIES["Alternatives"].add(str(position_name))
    elif "hard" in asset_class_str and "currency" in asset_class_str:
        UNMATCHED_SECURITIES["Hard Currency"].add(str(position_name))

def get_unmatched_securities():
    """
    Return a dictionary of securities that couldn't be matched with risk statistics.
    This is useful for identifying which securities need risk data.
    
    Returns:
        Dict[str, List[str]]: Dictionary with asset classes as keys and lists of 
                             unmatched security names as values
    """
    global UNMATCHED_SECURITIES
    
    # Convert sets to sorted lists for better display
    result = {}
    for asset_class, securities in UNMATCHED_SECURITIES.items():
        result[asset_class] = sorted(list(securities))
    
    return result