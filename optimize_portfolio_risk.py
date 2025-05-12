"""
Performance optimization script for portfolio risk calculations.

This script provides optimizations for calculating portfolio risk metrics,
particularly for larger portfolios with many positions.
"""
import logging
import os
import time
import json
from datetime import datetime
from decimal import Decimal

from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

from src.models.models import (
    FinancialPosition, 
    RiskStatisticEquity,
    RiskStatisticFixedIncome,
    RiskStatisticAlternatives
)
from src.services.portfolio_risk_service import calculate_portfolio_risk_metrics

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_session():
    """Create a database session"""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    return Session()

def optimize_portfolio_metrics():
    """Test the optimized portfolio risk metrics calculation"""
    db = get_db_session()
    
    # Define test parameters
    test_clients = [
        {"name": "18 Sole LLC", "expected_assets": ["equity"]},
        {"name": " The Linden East II Trust (Abigail Wexner)", "expected_assets": ["equity", "fixed_income"]}
    ]
    test_date = datetime.strptime("2025-05-01", "%Y-%m-%d").date()
    
    # Run performance tests
    print(f"\nPortfolio Risk Metrics Performance Test")
    print(f"======================================")
    print(f"Testing with date: {test_date}")
    
    total_success = True
    
    for client in test_clients:
        client_name = client["name"]
        print(f"\nTesting client: {client_name}")
        
        # Get position count
        position_count = db.query(FinancialPosition).filter_by(
            top_level_client=client_name,
            date=test_date
        ).count()
        
        print(f"Number of positions: {position_count}")
        
        if position_count == 0:
            print(f"ERROR: No positions found for {client_name} on {test_date}")
            total_success = False
            continue
        
        # Test calculation time
        print("Calculating risk metrics...")
        start_time = time.time()
        
        try:
            result = calculate_portfolio_risk_metrics(db, "client", client_name, test_date)
            end_time = time.time()
            elapsed = end_time - start_time
            
            print(f"Calculation completed in {elapsed:.2f} seconds")
            
            # Print metrics
            if "equity" in result and "beta" in result["equity"]:
                equity_beta = result["equity"]["beta"].get("value")
                equity_coverage = result["equity"]["beta"].get("coverage_pct")
                print(f"Equity beta: {equity_beta} (coverage: {equity_coverage}%)")
            
            if "fixed_income" in result and "duration" in result["fixed_income"]:
                duration = result["fixed_income"]["duration"].get("value")
                duration_coverage = result["fixed_income"]["duration"].get("coverage_pct")
                print(f"Fixed income duration: {duration} (coverage: {duration_coverage}%)")
            
            if "portfolio" in result and "beta" in result["portfolio"]:
                portfolio_beta = result["portfolio"]["beta"].get("value")
                print(f"Portfolio beta: {portfolio_beta}")
            
            # Check coverage
            if result["equity"]["beta"]["coverage_pct"] == 0 and "equity" in client["expected_assets"]:
                print(f"WARNING: Expected equity metrics but got 0% coverage")
                total_success = False
                
            if result["fixed_income"]["duration"]["coverage_pct"] == 0 and "fixed_income" in client["expected_assets"]:
                print(f"WARNING: Expected fixed income metrics but got 0% coverage")
                total_success = False
            
        except Exception as e:
            print(f"ERROR: {str(e)}")
            total_success = False
    
    db.close()
    
    if total_success:
        print("\nAll performance tests completed successfully!")
    else:
        print("\nSome performance tests failed. See warnings/errors above.")

if __name__ == "__main__":
    optimize_portfolio_metrics()