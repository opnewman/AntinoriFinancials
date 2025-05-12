"""
Test script for portfolio risk calculations
"""
import logging
import datetime
from decimal import Decimal

from src.database import get_db
from src.services.portfolio_risk_service import calculate_portfolio_risk_metrics
from optimized_find_matching_risk_stat_implementation import find_matching_risk_stat

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_portfolio_risk():
    """
    Test portfolio risk calculations for a specific portfolio
    """
    # Get database session
    db = next(get_db())
    
    try:
        # Attempt to calculate portfolio risk metrics
        # Use level='client' to test against a client-level risk calculation
        logger.info("Starting test portfolio risk calculation")
        
        # Get most recent available date from financial positions
        from src.models.models import FinancialPosition
        from sqlalchemy import func
        
        most_recent_date_query = db.query(func.max(FinancialPosition.date)).scalar()
        if not most_recent_date_query:
            logger.warning("No financial positions found in database")
            return
            
        logger.info(f"Using most recent financial position date: {most_recent_date_query}")
        
        # Use client with most positions for better testing
        client_query = "D'Angelo Family"
            
        logger.info(f"Testing with client: {client_query}")
        
        # Calculate risk metrics with a balanced subset of positions for testing
        risk_metrics = calculate_portfolio_risk_metrics(
            db=db,
            level="client",
            level_key=client_query,
            report_date=most_recent_date_query,
            max_positions=750  # Balance between coverage and performance
        )
        
        # Print results summary
        logger.info("Risk metrics calculation completed")
        
        # Check if risk metrics were calculated
        if not risk_metrics:
            logger.warning("No risk metrics were calculated")
            return
            
        # Print summary of risk metrics
        for asset_class in ['equity', 'fixed_income', 'hard_currency', 'alternatives']:
            if asset_class in risk_metrics:
                logger.info(f"{asset_class.title()} risk metrics:")
                
                for metric, data in risk_metrics[asset_class].items():
                    if 'value' in data and data['value'] is not None:
                        logger.info(f"  - {metric}: {data['value']:.4f} (coverage: {data['coverage_pct']:.2f}%)")
                    else:
                        logger.info(f"  - {metric}: N/A (coverage: {data['coverage_pct']:.2f}%)")
        
        # Print asset class percentages
        if 'percentages' in risk_metrics:
            logger.info("Asset class percentages:")
            for asset_class, data in risk_metrics['percentages'].items():
                logger.info(f"  - {asset_class}: {data['value']:.2f}%")
                
        return risk_metrics
        
    except Exception as e:
        import traceback
        logger.error(f"Error testing portfolio risk: {str(e)}")
        logger.error(traceback.format_exc())
        return None
        
if __name__ == "__main__":
    test_portfolio_risk()