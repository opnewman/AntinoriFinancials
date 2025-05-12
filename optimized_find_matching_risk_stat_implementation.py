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
    if asset_class_key == "fixed_income":
        # Try CUSIP first for fixed income (most reliable for bonds)
        if safe_cusip:
            query = db.query(RiskStatisticFixedIncome).filter(
                func.lower(RiskStatisticFixedIncome.cusip) == safe_cusip.lower(),
                RiskStatisticFixedIncome.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "duration": query.duration}
                # Cache the result
                if cache is not None and "cusip" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["cusip"][safe_cusip] = result
                return result
        
        # Try ticker symbol next
        if safe_ticker and safe_ticker != '-':
            query = db.query(RiskStatisticFixedIncome).filter(
                func.lower(RiskStatisticFixedIncome.ticker_symbol) == safe_ticker.lower(),
                RiskStatisticFixedIncome.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "duration": query.duration}
                # Cache the result
                if cache is not None and "ticker_symbol" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["ticker_symbol"][safe_ticker] = result
                return result
        
        # Try position name last
        if safe_position:
            query = db.query(RiskStatisticFixedIncome).filter(
                func.lower(RiskStatisticFixedIncome.position) == safe_position.lower(),
                RiskStatisticFixedIncome.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "duration": query.duration}
                # Cache the result
                if cache is not None and "position" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["position"][safe_position] = result
                return result
            
            # For bond names, try partial match with first few words
            words = safe_position.split()
            if len(words) >= 3:
                first_three = f"{words[0]} {words[1]} {words[2]}"
                query = db.query(RiskStatisticFixedIncome).filter(
                    RiskStatisticFixedIncome.position.ilike(f"%{first_three}%"),
                    RiskStatisticFixedIncome.upload_date == latest_date
                ).first()
                
                if query:
                    result = {"id": query.id, "duration": query.duration}
                    # Cache the result
                    if cache is not None and "position" in cache.get(asset_class_key, {}):
                        cache[asset_class_key]["position"][safe_position] = result
                    return result
    
    elif asset_class_key == "equity":
        # Try ticker symbol first for equities (most reliable)
        if safe_ticker and safe_ticker != '-':
            query = db.query(RiskStatisticEquity).filter(
                func.lower(RiskStatisticEquity.ticker_symbol) == safe_ticker.lower(),
                RiskStatisticEquity.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "beta": query.beta, "volatility": query.volatility}
                # Cache the result
                if cache is not None and "ticker_symbol" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["ticker_symbol"][safe_ticker] = result
                return result
        
        # Try CUSIP for derivatives
        if safe_cusip:
            query = db.query(RiskStatisticEquity).filter(
                func.lower(RiskStatisticEquity.cusip) == safe_cusip.lower(),
                RiskStatisticEquity.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "beta": query.beta, "volatility": query.volatility}
                # Cache the result
                if cache is not None and "cusip" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["cusip"][safe_cusip] = result
                return result
        
        # Try position name last
        if safe_position:
            query = db.query(RiskStatisticEquity).filter(
                func.lower(RiskStatisticEquity.position) == safe_position.lower(),
                RiskStatisticEquity.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "beta": query.beta, "volatility": query.volatility}
                # Cache the result
                if cache is not None and "position" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["position"][safe_position] = result
                return result
    
    else:  # Alternatives or Hard Currency
        # Try all identifiers for alternatives/hard currency
        if safe_cusip:
            query = db.query(RiskStatisticAlternatives).filter(
                func.lower(RiskStatisticAlternatives.cusip) == safe_cusip.lower(),
                RiskStatisticAlternatives.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "beta": query.beta, "volatility": query.volatility}
                # Cache the result
                if cache is not None and "cusip" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["cusip"][safe_cusip] = result
                return result
        
        if safe_ticker and safe_ticker != '-':
            query = db.query(RiskStatisticAlternatives).filter(
                func.lower(RiskStatisticAlternatives.ticker_symbol) == safe_ticker.lower(),
                RiskStatisticAlternatives.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "beta": query.beta, "volatility": query.volatility}
                # Cache the result
                if cache is not None and "ticker_symbol" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["ticker_symbol"][safe_ticker] = result
                return result
        
        if safe_position:
            query = db.query(RiskStatisticAlternatives).filter(
                func.lower(RiskStatisticAlternatives.position) == safe_position.lower(),
                RiskStatisticAlternatives.upload_date == latest_date
            ).first()
            
            if query:
                result = {"id": query.id, "beta": query.beta, "volatility": query.volatility}
                # Cache the result
                if cache is not None and "position" in cache.get(asset_class_key, {}):
                    cache[asset_class_key]["position"][safe_position] = result
                return result
    
except Exception as e:
    logger.warning(f"Database query error for {asset_class_key} ({position_name}): {str(e)}")

# Track this unmatched security for reporting
track_unmatched_security(position_name, asset_class)

# Return None if no match found
return None