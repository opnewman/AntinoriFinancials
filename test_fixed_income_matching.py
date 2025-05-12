"""
Test script to verify the enhanced fixed income risk statistic matching.

This script tests our new optimized implementation with a set of 
challenging bond names that previously had low match rates.
"""
import logging
import datetime
import re
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create in-memory database for testing
engine = create_engine("sqlite:///:memory:")
Base = declarative_base()

class RiskStatisticFixedIncome(Base):
    __tablename__ = "risk_statistic_fixed_income"
    
    id = Column(Integer, primary_key=True)
    position = Column(String)
    cusip = Column(String)
    ticker_symbol = Column(String)
    upload_date = Column(Date, nullable=False)
    duration = Column(Float)
    modified_duration = Column(Float)
    convexity = Column(Float)
    yield_to_maturity = Column(Float)
    yield_to_worst = Column(Float)
    option_adjusted_spread = Column(Float)
    credit_rating = Column(String)
    meta = Column(String)
    created_at = Column(Date)
    updated_at = Column(Date)

# Test set of various bond formats from real-world examples
REAL_WORLD_BONDS = [
    # Standard format bonds
    ("US TREASURY NOTE 1.5% 08/15/2025", "912828ZX1", "1.5", "25"),
    ("VERIZON COMMUNICATIONS 5.5% 03/16/2047", "92343VFS8", "5.5", "47"),
    # Bonds with full years
    ("FEDERAL HOME LOAN BANK 3.375% 06/12/2024 BOND", "3130A3GE8", "3.375", "24"),
    ("FANNIE MAE BOND 2.625% 10/13/2024", "3135G0W66", "2.625", "24"),
    # Mortgage-backed securities
    ("FNMA 30YR 3.0 TBA", "3138EXVN8", "3.0", None),
    ("FHLMC 30YR UMBS SUPER TBA", "31317YA32", None, None),
    # Corporate bonds with different formats
    ("AMAZON.COM INC 3.15% 01MAY2024", "023135CE5", "3.15", "24"),
    ("WAL-MART STORES INC. 2.55% 04/11/2023", "931142DP5", "2.55", "23"),
    ("APPLE 3.85% 05/04/2043", "037833AL4", "3.85", "43"),
    # Abbreviated formats
    ("MSFT 2.4 08/08/26", "594918BW3", "2.4", "26"),
    # Complex note descriptions
    ("American Express CO Note 1.65 % Due Nov 4, 2026", "025816CU2", "1.65", "26"),
    # Municipal bonds
    ("NEW YORK ST URBAN DEV CORP 3.95% 03/15/2028", "650036AS4", "3.95", "28"),
    # International bonds
    ("EUROPEAN INVESTMENT BANK 2.875% 06/13/2025", "298785HM1", "2.875", "25"),
    # Zero coupon bonds
    ("US TREASURY STRIP 0% 08/15/2027", "912821ET0", "0", "27"),
    # Floating rate notes
    ("JPMORGAN CHASE & CO FRN 05/15/2026", "46647PAP1", None, "26"),
    # Callable bonds
    ("AT&T INC 4.75% 05/15/2046 CALLABLE", "00206RDQ2", "4.75", "46"),
    # Convertibles
    ("TESLA INC CONV 2.00% 05/15/2024", "88160RAE7", "2.00", "24"),
    # Bond funds
    ("VANGUARD TOTAL BOND MARKET ETF", "921937835", None, None),
    # Inflation-indexed bonds
    ("TIPS 0.5% 01/15/2028", "912828Y38", "0.5", "28")
]

def test_fixed_income_matching():
    """Test the fixed income matching logic with different bond name patterns."""
    # Import the function here to avoid circular imports
    from optimized_find_matching_risk_stat_implementation import find_matching_risk_stat
    
    # Create tables
    Base.metadata.create_all(engine)
    
    # Create a session
    Session = sessionmaker(bind=engine)
    db = Session()
    
    # Check if any fixed income risk statistics exist
    latest_date_record = db.query(func.max(RiskStatisticFixedIncome.upload_date)).first()
    
    if latest_date_record[0] is None:
        logger.error("No fixed income risk statistics found in the database")
        # Create a dummy date for testing
        latest_date = datetime.date(2025, 5, 1)
        
        # Create some test data to demonstrate the matching
        logger.info("Creating sample risk statistics for testing...")
        
        # Store original bond names to use for querying
        original_bond_names = []
        
        # Add all the different bond types for testing
        for bond_name, cusip, rate, year in REAL_WORLD_BONDS:
            # Store original name
            original_bond_names.append(bond_name)
            
            # Create the main record
            test_stat = RiskStatisticFixedIncome(
                position=bond_name,
                cusip=cusip,
                ticker_symbol="",
                upload_date=latest_date,
                duration=5.0,  # Example duration
                modified_duration=4.8,
                yield_to_maturity=0.03,
                credit_rating="AA+"
            )
            db.add(test_stat)
            
            # Create slightly different variants for testing pattern matching
            if rate and year:
                variant1 = f"Different format {rate}% 20{year}"
                variant2 = f"Abbreviated {rate} 20{year}"
                
                # Add variants
                db.add(RiskStatisticFixedIncome(
                    position=variant1,
                    cusip="",  # No CUSIP to force pattern matching
                    ticker_symbol="",
                    upload_date=latest_date,
                    duration=5.0
                ))
                
                db.add(RiskStatisticFixedIncome(
                    position=variant2,
                    cusip="",  # No CUSIP to force pattern matching
                    ticker_symbol="",
                    upload_date=latest_date,
                    duration=5.0
                ))
        
        db.commit()
        count = db.query(RiskStatisticFixedIncome).filter(
            RiskStatisticFixedIncome.upload_date == latest_date
        ).count()
        logger.info(f"Found {count} fixed income risk statistics for date {latest_date}")
        
        # Now test our bond pattern matching
        successful_matches = 0
        for bond_name, cusip, _, _ in REAL_WORLD_BONDS:
            # Intentionally remove the CUSIP to test pattern matching
            result = find_matching_risk_stat(
                db=db,
                position_name=bond_name,
                cusip=None,  # No CUSIP to force pattern matching
                ticker_symbol=None,
                asset_class="Fixed Income",
                latest_date=latest_date
            )
            
            if result:
                successful_matches += 1
                status = "MATCHED"
            else:
                status = "NOT MATCHED"
                
            logger.info(f"{bond_name}: {status}")
        
        match_rate = successful_matches / len(REAL_WORLD_BONDS) * 100 if REAL_WORLD_BONDS else 0
        logger.info(f"Fixed Income Match Rate: {successful_matches}/{len(REAL_WORLD_BONDS)} ({match_rate:.2f}%)")
        
        # Test with some real database positions (if database exists)
        logger.info("\nTesting with real database positions:")
        real_positions = [
            "American Express CO Note 1.65 % Due Nov 4, 2026",
            "American Express CO Note 1.65 % Due Nov 4, 2026",
            "American General Universal Life Guaranteed $10mm Policy",
            "American High-Income Municipal Bond Fund F3",
            "American Ho C/P  00000  25JL21"
        ]
        
        real_matches = 0
        for pos in real_positions:
            result = find_matching_risk_stat(
                db=db,
                position_name=pos,
                cusip=None,
                ticker_symbol=None,
                asset_class="Fixed Income",
                latest_date=latest_date
            )
            
            if result:
                real_matches += 1
                logger.info(f"MATCHED: {pos}")
            else:
                logger.info(f"NOT MATCHED: {pos}")
        
        logger.info(f"Real position match rate: {real_matches}/{len(real_positions)}")
        
    else:
        latest_date = latest_date_record[0]
        logger.info(f"Using latest available risk statistics date: {latest_date}")
    
    # Close session
    db.close()

if __name__ == "__main__":
    test_fixed_income_matching()