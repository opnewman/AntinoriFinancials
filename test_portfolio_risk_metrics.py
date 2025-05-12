"""
Portfolio Risk Metrics Test Suite

This script tests the portfolio risk metrics API with various client sizes
and configurations to ensure proper functionality and performance.
"""

import requests
import json
import time
from datetime import datetime

def test_client(level_key, expected_asset_classes=None, max_time=45):
    """
    Test a specific client's risk metrics
    
    Args:
        level_key: The client name
        expected_asset_classes: Asset classes expected to be present
        max_time: Maximum acceptable time in seconds
    """
    # URL with parameters
    base_url = "http://localhost:5000/api/portfolio/risk-metrics"
    params = {
        "level": "client",
        "level_key": level_key,
        "date": "2025-05-01"
    }
    
    # Make the request
    print(f"\nTesting client: '{level_key}'")
    print(f"Making request to {base_url} with params {params}")
    
    start_time = time.time()
    
    try:
        response = requests.get(base_url, params=params, timeout=60)
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Print response information
        print(f"Status code: {response.status_code}")
        print(f"Response time: {elapsed:.2f} seconds")
        
        # Try to parse JSON only if we get a successful response
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Check structure of response
                asset_classes = [
                    key for key in data.keys() 
                    if key not in ['totals', 'percentages', 'portfolio']
                ]
                print(f"Asset classes present: {', '.join(asset_classes)}")
                
                # If we have equity data, print beta info
                if 'equity' in data and data['equity'].get('beta'):
                    beta_value = data['equity']['beta'].get('value')
                    coverage = data['equity']['beta'].get('coverage_pct')
                    print(f"Equity beta: {beta_value} (coverage: {coverage}%)")
                
                # If we have portfolio data, print total beta
                if 'portfolio' in data and data['portfolio'].get('beta'):
                    portfolio_beta = data['portfolio']['beta'].get('value')
                    print(f"Portfolio beta: {portfolio_beta}")
                
                # Verify expected asset classes if provided
                if expected_asset_classes:
                    missing = [ac for ac in expected_asset_classes if ac not in asset_classes]
                    if missing:
                        print(f"WARNING: Missing expected asset classes: {', '.join(missing)}")
                    else:
                        print("All expected asset classes present")
                
                # Check if time exceeds maximum
                if elapsed > max_time:
                    print(f"WARNING: Response time of {elapsed:.2f}s exceeds maximum of {max_time}s")
                
            except json.JSONDecodeError:
                print(f"Response body (not JSON):\n{response.text[:500]}")
        else:
            print(f"Response body (error):\n{response.text[:500]}")
            
    except requests.RequestException as e:
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"Request error after {elapsed:.2f}s: {e}")

def main():
    """Test risk metrics for clients of various sizes"""
    print(f"Starting portfolio risk metrics test at {datetime.now()}")
    
    # Small client
    test_client("18 Sole LLC", expected_asset_classes=["equity"], max_time=10)
    
    # Medium client (with leading space in name)
    test_client(" The Linden East II Trust (Abigail Wexner)", 
               expected_asset_classes=["equity", "fixed_income"], 
               max_time=45)
    
    print("\nTests completed")

if __name__ == "__main__":
    main()