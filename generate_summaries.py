"""
Script to generate financial summaries from existing data.
This is a one-time process to populate the financial_summary table
from the financial_positions data we already have.
"""
import os
import sys
import datetime
import logging
from sqlalchemy import create_engine, text, Column, Integer, String, Date, Float, MetaData, Table
from sqlalchemy.orm import sessionmaker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get database connection from environment
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable not set")
    sys.exit(1)

def main():
    """Generate financial summaries from existing data"""
    logger.info("Starting financial summary generation")
    
    # Connect to the database
    try:
        engine = create_engine(DATABASE_URL)
        conn = engine.connect()
        logger.info("✅ Database connection successful")
        
        # First, get the most recent date of the financial_positions
        date_query = text("""
            SELECT DISTINCT date FROM financial_positions ORDER BY date DESC LIMIT 1
        """)
        
        report_date = conn.execute(date_query).scalar()
        if not report_date:
            logger.error("No data found in financial_positions table")
            conn.close()
            return
        
        logger.info(f"Generating financial summaries for date: {report_date}")
        
        # Delete existing summary data for the report date
        delete_query = text("""
            DELETE FROM financial_summary WHERE report_date = :report_date
        """)
        conn.execute(delete_query, {"report_date": report_date})
        
        # Run a big SQL query to generate the summaries
        logger.info("Running aggregate queries to generate financial summaries")
        
        # Generate client level summary
        client_query = text("""
        INSERT INTO financial_summary (level, level_key, total_adjusted_value, report_date, upload_date)
        SELECT 
            'client' as level,
            top_level_client as level_key,
            SUM(
                CASE 
                    WHEN adjusted_value LIKE 'ENC:%' THEN CAST(SUBSTRING(adjusted_value, 5) AS DECIMAL)
                    ELSE CAST(adjusted_value AS DECIMAL) 
                END
            ) as total_adjusted_value,
            date as report_date,
            CURRENT_DATE as upload_date
        FROM 
            financial_positions
        WHERE 
            date = :report_date AND top_level_client IS NOT NULL
        GROUP BY 
            top_level_client, date
        """)
        
        result = conn.execute(client_query, {"report_date": report_date})
        logger.info(f"Generated client summaries: {result.rowcount} rows")
        
        # Generate portfolio level summary
        portfolio_query = text("""
        INSERT INTO financial_summary (level, level_key, total_adjusted_value, report_date, upload_date)
        SELECT 
            'portfolio' as level,
            portfolio as level_key,
            SUM(
                CASE 
                    WHEN adjusted_value LIKE 'ENC:%' THEN CAST(SUBSTRING(adjusted_value, 5) AS DECIMAL)
                    ELSE CAST(adjusted_value AS DECIMAL) 
                END
            ) as total_adjusted_value,
            date as report_date,
            CURRENT_DATE as upload_date
        FROM 
            financial_positions
        WHERE 
            date = :report_date AND portfolio IS NOT NULL AND portfolio != '-'
        GROUP BY 
            portfolio, date
        """)
        
        result = conn.execute(portfolio_query, {"report_date": report_date})
        logger.info(f"Generated portfolio summaries: {result.rowcount} rows")
        
        # Generate account level summary
        account_query = text("""
        INSERT INTO financial_summary (level, level_key, total_adjusted_value, report_date, upload_date)
        SELECT 
            'account' as level,
            holding_account_number as level_key,
            SUM(
                CASE 
                    WHEN adjusted_value LIKE 'ENC:%' THEN CAST(SUBSTRING(adjusted_value, 5) AS DECIMAL)
                    ELSE CAST(adjusted_value AS DECIMAL) 
                END
            ) as total_adjusted_value,
            date as report_date,
            CURRENT_DATE as upload_date
        FROM 
            financial_positions
        WHERE 
            date = :report_date AND holding_account_number IS NOT NULL
        GROUP BY 
            holding_account_number, date
        """)
        
        result = conn.execute(account_query, {"report_date": report_date})
        logger.info(f"Generated account summaries: {result.rowcount} rows")
        
        # Also add "All Clients" entry for easier selection in UI
        all_clients_query = text("""
        INSERT INTO financial_summary (level, level_key, total_adjusted_value, report_date, upload_date)
        SELECT 
            'client' as level,
            'All Clients' as level_key,
            SUM(
                CASE 
                    WHEN adjusted_value LIKE 'ENC:%' THEN CAST(SUBSTRING(adjusted_value, 5) AS DECIMAL)
                    ELSE CAST(adjusted_value AS DECIMAL) 
                END
            ) as total_adjusted_value,
            date as report_date,
            CURRENT_DATE as upload_date
        FROM 
            financial_positions
        WHERE 
            date = :report_date
        GROUP BY 
            date
        """)
        
        result = conn.execute(all_clients_query, {"report_date": report_date})
        logger.info(f"Generated 'All Clients' summary: {result.rowcount} rows")
        
        # Commit the transaction
        conn.commit()
        
        # Verify we have data now
        count_query = text("SELECT COUNT(*) FROM financial_summary")
        count = conn.execute(count_query).scalar()
        logger.info(f"Total financial summary entries created: {count}")
        
        conn.close()
        logger.info("✅ Financial summary generation complete")
        
    except Exception as e:
        logger.error(f"❌ Error generating financial summaries: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()