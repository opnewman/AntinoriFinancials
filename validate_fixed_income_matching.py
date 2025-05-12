"""
Script to validate the fixed income risk statistic matching.

This script uses a clean approach to create a self-contained test environment
with a controlled set of test data to verify our risk matching algorithm.
"""
import logging
import datetime
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, func, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create in-memory database for testing
engine = create_engine("sqlite:///:memory:")
Base = declarative_base()

# Create the model classes for our test
class FinancialPosition(Base):
    __tablename__ = "financial_position"
    
    id = Column(Integer, primary_key=True)
    position = Column(String, nullable=False)
    cusip = Column(String)
    ticker_symbol = Column(String)
    asset_class = Column(String, nullable=False)
    market_value = Column(Float, default=0.0)
    
    def __repr__(self):
        return f"<FinancialPosition(position='{self.position}')>"

class RiskStatisticFixedIncome(Base):
    __tablename__ = "risk_statistic_fixed_income"
    
    id = Column(Integer, primary_key=True)
    position = Column(String)
    cusip = Column(String)
    ticker_symbol = Column(String)  # Match the column name expected by find_matching_risk_stat
    upload_date = Column(Date, nullable=False)
    duration = Column(Float)
    # Add additional columns that might be referenced by the query
    modified_duration = Column(Float)
    convexity = Column(Float)
    yield_to_maturity = Column(Float)
    yield_to_worst = Column(Float)
    option_adjusted_spread = Column(Float)
    credit_rating = Column(String)
    meta = Column(String)
    created_at = Column(Date)
    updated_at = Column(Date)
    
    def __repr__(self):
        return f"<RiskStatisticFixedIncome(position='{self.position}', duration={self.duration})>"

# Import the function we're testing
import sys
import os
sys.path.append(os.path.abspath("."))
from optimized_find_matching_risk_stat_implementation import find_matching_risk_stat

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

def validate_fixed_income_matching():
    """Test the fixed income matching logic with different bond name patterns."""
    # Create all tables
    Base.metadata.create_all(engine)
    
    # Create a session
    Session = sessionmaker(bind=engine)
    db = Session()
    
    # Set up test data
    latest_date = datetime.date(2025, 5, 1)
    
    # Create the risk statistics with similar but not identical data
    risk_stats = [
        # Using different formats and variations of bond names
        RiskStatisticFixedIncome(
            position="US TREASURY 1.50% 02/15/2025", 
            cusip="912828ZX1", 
            ticker_symbol="", 
            upload_date=latest_date,
            duration=4.5
        ),
        RiskStatisticFixedIncome(
            position="VERIZON COMMUNICATIONS 5.50% 03/16/2047", 
            cusip="92343VFS8", 
            ticker_symbol="VZ", 
            upload_date=latest_date,
            duration=17.8
        ),
        RiskStatisticFixedIncome(
            position="FEDERAL HOME LOAN BANK 3.375% 06/12/2024", 
            cusip="3130A3GE8", 
            ticker_symbol="", 
            upload_date=latest_date,
            duration=3.7
        ),
        RiskStatisticFixedIncome(
            position="FNMA 30YR 3.0 TBA", 
            cusip="3138EXVN8", 
            ticker_symbol="", 
            upload_date=latest_date,
            duration=7.2
        ),
        RiskStatisticFixedIncome(
            position="FHLMC 30YR UMBS SUPER TBA", 
            cusip="31317YA32", 
            ticker_symbol="", 
            upload_date=latest_date,
            duration=6.9
        ),
        RiskStatisticFixedIncome(
            position="AMAZON.COM INC 3.15% 08/22/2024", 
            cusip="023135CE5", 
            ticker_symbol="AMZN", 
            upload_date=latest_date,
            duration=2.8
        ),
        RiskStatisticFixedIncome(
            position="WALMART INC 2.55% 04/11/2023", 
            cusip="931142DP5", 
            ticker_symbol="WMT", 
            upload_date=latest_date,
            duration=0.9
        ),
        RiskStatisticFixedIncome(
            position="Apple Inc. 3.85% 05/04/2043", 
            cusip="037833AL4", 
            ticker_symbol="AAPL", 
            upload_date=latest_date,
            duration=12.5
        ),
        RiskStatisticFixedIncome(
            position="MICROSOFT CORP 2.4% 08/08/2026", 
            cusip="594918BW3", 
            ticker_symbol="MSFT", 
            upload_date=latest_date,
            duration=5.3
        ),
        # Add some additional variants for more thorough testing
        RiskStatisticFixedIncome(
            position="AMERICAN EXPRESS CO 1.65% 11/04/2026", 
            cusip="025816CU2", 
            ticker_symbol="AXP", 
            upload_date=latest_date,
            duration=4.1
        ),
    ]
    
    # Add all risk statistics
    for stat in risk_stats:
        db.add(stat)
    
    # Add some financial positions
    positions = []
    for bond_name, cusip, ticker in TEST_BONDS:
        position = FinancialPosition(
            position=bond_name,
            cusip=cusip,
            ticker_symbol=ticker,
            asset_class="Fixed Income",
            market_value=1000000.0
        )
        positions.append(position)
        db.add(position)
    
    # Add some additional real-world examples
    additional_positions = [
        FinancialPosition(
            position="American Express CO Note 1.65 % Due Nov 4, 2026",
            cusip="025816CU2",
            ticker_symbol="AXP",
            asset_class="Fixed Income",
            market_value=500000.0
        ),
        FinancialPosition(
            position="MSFT 2.4 08/08/26",  # Abbreviated format
            cusip="594918BW3",
            ticker_symbol="MSFT",
            asset_class="Fixed Income",
            market_value=750000.0
        ),
        FinancialPosition(
            position="U.S. TREASURY NOTE 1.50% 2025",  # Slightly different format
            cusip="912828ZX1",
            ticker_symbol="",
            asset_class="Fixed Income",
            market_value=2000000.0
        )
    ]
    
    for position in additional_positions:
        positions.append(position)
        db.add(position)
    
    db.commit()
    
    # Verify we have the data we expect
    risk_stats_count = db.query(RiskStatisticFixedIncome).filter(
        RiskStatisticFixedIncome.upload_date == latest_date
    ).count()
    
    logger.info(f"Created {risk_stats_count} fixed income risk statistics for date {latest_date}")
    logger.info(f"Created {len(positions)} financial positions for testing")
    
    # Test each position
    matches = 0
    match_details = []
    
    for position in positions:
        match = find_matching_risk_stat(
            db=db,
            position_name=position.position,
            cusip=position.cusip,
            ticker_symbol=position.ticker_symbol,
            asset_class="Fixed Income",
            latest_date=latest_date
        )
        
        if match:
            matches += 1
            match_details.append((
                position.position, 
                "MATCHED", 
                "CUSIP" if position.cusip else "Name pattern"
            ))
        else:
            match_details.append((position.position, "NOT MATCHED", ""))
    
    # Print results
    logger.info(f"Fixed Income Match Rate: {matches}/{len(positions)} ({matches/len(positions)*100:.2f}%)")
    logger.info("Detailed results:")
    for name, result, method in match_details:
        logger.info(f"{name}: {result} {f'via {method}' if result == 'MATCHED' else ''}")
    
    # Close the session
    db.close()

if __name__ == "__main__":
    validate_fixed_income_matching()