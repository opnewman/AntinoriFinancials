"""
Script to create a sample model portfolio for testing
"""
import datetime
import sys
import logging
from sqlalchemy import create_engine, text
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_sample_model_portfolio():
    """Create a sample model portfolio in the database"""
    try:
        # Connect to the database
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            logger.error("DATABASE_URL environment variable is not set")
            return False

        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # Create a model portfolio
            portfolio_query = text("""
                INSERT INTO model_portfolios (
                    name, description, is_active, creation_date, update_date
                ) VALUES (
                    'Balanced Growth Model', 
                    'A balanced growth portfolio with moderate risk profile. This model aims for steady growth with controlled volatility.', 
                    TRUE, 
                    CURRENT_DATE, 
                    CURRENT_DATE
                ) RETURNING id
            """)
            
            result = conn.execute(portfolio_query)
            portfolio_id = result.fetchone()[0]
            
            logger.info(f"Created model portfolio with ID: {portfolio_id}")
            
            # Add allocations - Main categories
            allocations = [
                # Equities (total 60%)
                ('Equities', None, 60.0),
                ('Equities', 'US Markets', 30.0),
                ('Equities', 'Global Markets', 15.0),
                ('Equities', 'Emerging Markets', 5.0),
                ('Equities', 'Real Estate', 5.0),
                ('Equities', 'Low Beta Alpha', 5.0),
                
                # Fixed Income (total 25%)
                ('Fixed Income', None, 25.0),
                ('Fixed Income', 'Municipal Bonds', 10.0),
                ('Fixed Income', 'Government Bonds', 5.0),
                ('Fixed Income', 'Investment Grade', 10.0),
                
                # Hard Currency (total 5%)
                ('Hard Currency', None, 5.0),
                ('Hard Currency', 'Gold', 3.0),
                ('Hard Currency', 'Silver', 2.0),
                
                # Uncorrelated Alternatives (total 5%)
                ('Uncorrelated Alternatives', None, 5.0),
                ('Uncorrelated Alternatives', 'Hedge Funds', 5.0),
                
                # Cash (total 5%)
                ('Cash & Cash Equivalent', None, 5.0)
            ]
            
            for category, subcategory, allocation_pct in allocations:
                alloc_query = text("""
                    INSERT INTO model_portfolio_allocations (
                        model_portfolio_id, category, subcategory, allocation_percentage, is_model_weight
                    ) VALUES (
                        :portfolio_id, :category, :subcategory, :allocation_pct, TRUE
                    )
                """)
                
                conn.execute(alloc_query, {
                    "portfolio_id": portfolio_id,
                    "category": category,
                    "subcategory": subcategory,
                    "allocation_pct": allocation_pct
                })
            
            logger.info("Added allocations to model portfolio")
            
            # Add fixed income metrics
            fi_metrics = [
                ('Duration', None, 5.2),
                ('Duration', 'Municipal Bonds', 6.3),
                ('Duration', 'Government Bonds', 7.2),
                ('Duration', 'Investment Grade', 4.8),
                ('Yield', None, 3.5),
                ('Yield', 'Municipal Bonds', 3.8),
                ('Yield', 'Government Bonds', 2.9),
                ('Yield', 'Investment Grade', 4.1)
            ]
            
            for metric_name, subcategory, value in fi_metrics:
                metric_query = text("""
                    INSERT INTO fixed_income_metrics (
                        model_portfolio_id, metric_name, metric_subcategory, metric_value
                    ) VALUES (
                        :portfolio_id, :metric_name, :subcategory, :value
                    )
                """)
                
                conn.execute(metric_query, {
                    "portfolio_id": portfolio_id,
                    "metric_name": metric_name,
                    "subcategory": subcategory,
                    "value": value
                })
            
            logger.info("Added fixed income metrics to model portfolio")
            
            # Add performance metrics
            performance_metrics = [
                ('1D', 0.15),
                ('MTD', 1.8),
                ('QTD', 3.2),
                ('YTD', 7.5)
            ]
            
            for period, value in performance_metrics:
                perf_query = text("""
                    INSERT INTO performance_metrics (
                        model_portfolio_id, period, performance_percentage, as_of_date
                    ) VALUES (
                        :portfolio_id, :period, :value, CURRENT_DATE
                    )
                """)
                
                conn.execute(perf_query, {
                    "portfolio_id": portfolio_id,
                    "period": period,
                    "value": value
                })
            
            logger.info("Added performance metrics to model portfolio")
            
            # Add currency allocations
            currency_allocations = [
                ('USD', 95.0),
                ('EUR', 3.0),
                ('GBP', 2.0)
            ]
            
            for currency, value in currency_allocations:
                curr_query = text("""
                    INSERT INTO currency_allocations (
                        model_portfolio_id, currency_name, allocation_percentage
                    ) VALUES (
                        :portfolio_id, :currency, :value
                    )
                """)
                
                conn.execute(curr_query, {
                    "portfolio_id": portfolio_id,
                    "currency": currency,
                    "value": value
                })
            
            logger.info("Added currency allocations to model portfolio")
            
            # Create a second model portfolio - Conservative Income
            portfolio_query = text("""
                INSERT INTO model_portfolios (
                    name, description, is_active, creation_date, update_date
                ) VALUES (
                    'Conservative Income Model', 
                    'A conservative income portfolio focused on capital preservation and steady income generation.', 
                    TRUE, 
                    CURRENT_DATE, 
                    CURRENT_DATE
                ) RETURNING id
            """)
            
            result = conn.execute(portfolio_query)
            portfolio_id_2 = result.fetchone()[0]
            
            logger.info(f"Created second model portfolio with ID: {portfolio_id_2}")
            
            # Add allocations for conservative income model
            conservative_allocations = [
                # Equities (total 30%)
                ('Equities', None, 30.0),
                ('Equities', 'US Markets', 20.0),
                ('Equities', 'Global Markets', 10.0),
                
                # Fixed Income (total 55%)
                ('Fixed Income', None, 55.0),
                ('Fixed Income', 'Municipal Bonds', 25.0),
                ('Fixed Income', 'Government Bonds', 20.0),
                ('Fixed Income', 'Investment Grade', 10.0),
                
                # Hard Currency (total 5%)
                ('Hard Currency', None, 5.0),
                ('Hard Currency', 'Gold', 5.0),
                
                # Uncorrelated Alternatives (total 0%)
                ('Uncorrelated Alternatives', None, 0.0),
                
                # Cash (total 10%)
                ('Cash & Cash Equivalent', None, 10.0)
            ]
            
            for category, subcategory, allocation_pct in conservative_allocations:
                alloc_query = text("""
                    INSERT INTO model_portfolio_allocations (
                        model_portfolio_id, category, subcategory, allocation_percentage, is_model_weight
                    ) VALUES (
                        :portfolio_id, :category, :subcategory, :allocation_pct, TRUE
                    )
                """)
                
                conn.execute(alloc_query, {
                    "portfolio_id": portfolio_id_2,
                    "category": category,
                    "subcategory": subcategory,
                    "allocation_pct": allocation_pct
                })
            
            logger.info("Added allocations to conservative model portfolio")
            
            # Add performance metrics for conservative model
            conservative_performance = [
                ('1D', 0.08),
                ('MTD', 1.1),
                ('QTD', 2.4),
                ('YTD', 5.2)
            ]
            
            for period, value in conservative_performance:
                perf_query = text("""
                    INSERT INTO performance_metrics (
                        model_portfolio_id, period, performance_percentage, as_of_date
                    ) VALUES (
                        :portfolio_id, :period, :value, CURRENT_DATE
                    )
                """)
                
                conn.execute(perf_query, {
                    "portfolio_id": portfolio_id_2,
                    "period": period,
                    "value": value
                })
            
            logger.info("Added performance metrics to conservative model portfolio")
            
            conn.commit()
            logger.info("Successfully created sample model portfolios")
            return True
    
    except Exception as e:
        logger.error(f"Error creating sample model portfolio: {e}")
        return False

if __name__ == "__main__":
    success = create_sample_model_portfolio()
    sys.exit(0 if success else 1)