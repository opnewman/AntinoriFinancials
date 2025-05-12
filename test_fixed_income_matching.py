"""
Test script to verify the enhanced fixed income risk statistic matching.

This script tests our new optimized implementation with a set of 
challenging bond names that previously had low match rates.
"""
import logging
import datetime
from sqlalchemy.orm import Session

from src.database import get_db
from src.models.models import (
    RiskStatisticFixedIncome,
    FinancialPosition
)
from optimized_find_matching_risk_stat_implementation import find_matching_risk_stat

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test bonds with different naming patterns
TEST_BONDS = [
    # Format: (position_name, cusip, ticker_symbol)
    ("US TREASURY 1.5% 2025", "912828ZX1", ""),
    ("VERIZON COMMUNICATIONS 5.5% 2047", "92343VFS8", "VZ"),
    ("FEDERAL HOME LOAN BANK 3.375% 2024 BOND", "3130A3GE8", ""),
    ("FNMA 30YR 3.0", "3138EXVN8", ""),
    ("FHLMC 30YR UMBS SUPER", "31317YA32", ""),
    ("AMAZON.COM INC 3.15% 01MAY2024", "023135CE5", "AMZN"),
    ("WAL-MART STORES INC. 2.55% 04/11/2023", "931142DP5", "WMT"),
    ("APPLE 3.85% 05/04/2043", "037833AL4", "AAPL"),
    ("MICROSOFT CORP", "594918BW3", "MSFT")
]

def test_fixed_income_matching():
    """Test the fixed income matching logic with different bond name patterns."""
    # Get database session
    db = next(get_db())
    
    # Find the latest available risk statistics date
    latest_date_record = db.query(RiskStatisticFixedIncome.upload_date).order_by(
        RiskStatisticFixedIncome.upload_date.desc()
    ).first()
    
    if not latest_date_record:
        logger.error("No fixed income risk statistics found in the database")
        # Create a dummy date for testing
        latest_date = datetime.date(2025, 5, 1)
        
        # Create some test data to demonstrate the matching
        logger.info("Creating sample risk statistics for testing...")
        
        # Create slightly altered variants of the test bonds to test our matching logic
        
        # Creating variants with different formatting but same essential info
        variants = []
        for bond_name, cusip, ticker in TEST_BONDS:
            # Original exact copy
            variants.append((bond_name, cusip, ticker))
            
            # CUSIP without dashes
            if cusip and "-" in cusip:
                clean_cusip = cusip.replace("-", "")
                variants.append((bond_name, clean_cusip, ticker))
            
            # Variant with different formatting of the % sign
            if "%" in bond_name:
                variant1 = bond_name.replace("%", " %")
                variant2 = bond_name.replace("%", "% ")
                variant3 = bond_name.replace("%", " percent ")
                variants.append((variant1, cusip, ticker))
                variants.append((variant2, cusip, ticker))
                variants.append((variant3, cusip, ticker))
            
            # Variant with different date formatting
            if "/" in bond_name:
                variant = bond_name.replace("/", "-")
                variants.append((variant, cusip, ticker))
                
            # Variant with abbreviated name
            words = bond_name.split()
            if len(words) > 3:
                short_name = " ".join(words[:3])
                variants.append((short_name, cusip, ticker))
        
        # Create risk statistics for all variants
        for bond_name, cusip, ticker in variants:
            test_stat = RiskStatisticFixedIncome(
                position=bond_name,
                cusip=cusip,
                ticker=ticker,
                duration=5.0,  # Example duration
                upload_date=latest_date
            )
            db.add(test_stat)
        
        # Add test statistics that mimic real samples
        real_samples = [
            # Position name, CUSIP, Ticker
            ("AMERICAN EXPRESS CO 1.65% 11/04/2026", "025816CU2", "AXP"),
            ("Apple Inc 3.85% 05/04/2043", "037833AL4", "AAPL"),
            ("FHLMC 30YR UMBS SUPER TBA", "31317YA32", ""),
            ("US TREAS 1.50% 02/15/2025", "912828ZX1", ""),
            ("VERIZON COMMUNICATIONS 5.5% 03/16/2047", "92343VFS8", "VZ"),
            ("AMAZON.COM INC 3.15% 08/22/2027", "023135CE5", "AMZN"),
            ("WALMART INC 2.55% 04/11/2023", "931142DP5", "WMT"),
            ("MICROSOFT CORP 2.4% 08/08/2026", "594918BW3", "MSFT"),
            ("Federal Home Loan Bank 3.375% 06/12/2024", "3130A3GE8", ""),
            ("FNMA 30YR 3.0 TBA", "3138EXVN8", "")
        ]
        
        for bond_name, cusip, ticker in real_samples:
            test_stat = RiskStatisticFixedIncome(
                position=bond_name,
                cusip=cusip,
                ticker=ticker,
                duration=5.0,  # Example duration
                upload_date=latest_date
            )
            db.add(test_stat)
        
        db.commit()
        logger.info(f"Test data created with {len(variants) + len(real_samples)} variants")
    else:
        latest_date = latest_date_record[0]
        logger.info(f"Using latest available risk statistics date: {latest_date}")
    
    # Verify we have risk statistics for the test date
    risk_stats_count = db.query(RiskStatisticFixedIncome).filter(
        RiskStatisticFixedIncome.upload_date == latest_date
    ).count()
    
    if risk_stats_count == 0:
        logger.error(f"No fixed income risk statistics found for date {latest_date}")
        return
    
    logger.info(f"Found {risk_stats_count} fixed income risk statistics for date {latest_date}")
    
    # Test each bond
    matches = 0
    match_details = []
    
    for bond_name, cusip, ticker in TEST_BONDS:
        match = find_matching_risk_stat(
            db=db,
            position_name=bond_name,
            cusip=cusip,
            ticker_symbol=ticker,
            asset_class="Fixed Income",
            latest_date=latest_date
        )
        
        if match:
            matches += 1
            match_details.append((bond_name, "MATCHED", "CUSIP" if cusip else "Name pattern"))
        else:
            match_details.append((bond_name, "NOT MATCHED", ""))
    
    # Print results
    logger.info(f"Fixed Income Match Rate: {matches}/{len(TEST_BONDS)} ({matches/len(TEST_BONDS)*100:.2f}%)")
    logger.info("Detailed results:")
    for name, result, method in match_details:
        logger.info(f"{name}: {result} {f'via {method}' if result == 'MATCHED' else ''}")

    # Add similar test for a real position from the database
    sample_positions = db.query(FinancialPosition).filter(
        FinancialPosition.asset_class == "Fixed Income"
    ).limit(5).all()
    
    if sample_positions:
        logger.info("\nTesting with real database positions:")
        real_matches = 0
        
        for position in sample_positions:
            match = find_matching_risk_stat(
                db=db,
                position_name=position.position,
                cusip=position.cusip,
                ticker_symbol=position.ticker_symbol,
                asset_class="Fixed Income",
                latest_date=latest_date
            )
            
            if match:
                real_matches += 1
                logger.info(f"MATCHED: {position.position}")
            else:
                logger.info(f"NOT MATCHED: {position.position}")
        
        logger.info(f"Real position match rate: {real_matches}/{len(sample_positions)}")

if __name__ == "__main__":
    test_fixed_income_matching()