"""
Diagnostic script to check data loading and verify database contents.
"""
import os
import sys
import datetime
import logging
import json
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

def test_database_connection():
    """Test the database connection and report status"""
    logger.info(f"Testing connection to: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")
    
    try:
        engine = create_engine(DATABASE_URL)
        conn = engine.connect()
        logger.info("✅ Database connection successful")
        conn.close()
        return engine
    except Exception as e:
        logger.error(f"❌ Database connection failed: {str(e)}")
        sys.exit(1)

def count_rows_by_table(engine):
    """Count rows in each table and report"""
    logger.info("Counting rows in each table...")
    
    try:
        with engine.connect() as conn:
            # Get list of tables
            tables_query = text("""
                SELECT tablename FROM pg_catalog.pg_tables 
                WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'
            """)
            tables = [row[0] for row in conn.execute(tables_query)]
            
            logger.info(f"Found {len(tables)} tables in database")
            
            for table in tables:
                count_query = text(f"SELECT COUNT(*) FROM {table}")
                try:
                    result = conn.execute(count_query).scalar()
                    logger.info(f"Table {table}: {result} rows")
                except Exception as e:
                    logger.error(f"Error counting rows in {table}: {str(e)}")
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")

def examine_financial_positions(engine):
    """Examine the financial_positions table structure and sample data"""
    logger.info("Examining financial_positions table...")
    
    with engine.connect() as conn:
        # Check if table exists
        table_exists_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'financial_positions'
            )
        """)
        
        table_exists = conn.execute(table_exists_query).scalar()
        if not table_exists:
            logger.error("❌ financial_positions table does not exist")
            return
        
        # Get column information
        columns_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'financial_positions'
            ORDER BY ordinal_position
        """)
        
        columns = conn.execute(columns_query).fetchall()
        logger.info("Financial positions columns:")
        for col in columns:
            logger.info(f"   {col[0]} ({col[1]})")
        
        # Get sample data
        sample_query = text("""
            SELECT * FROM financial_positions LIMIT 5
        """)
        
        sample_rows = conn.execute(sample_query).fetchall()
        if sample_rows:
            logger.info("Sample data:")
            for i, row in enumerate(sample_rows):
                row_dict = {col[0]: getattr(row, col[0]) for col in columns}
                logger.info(f"Row {i+1}: {json.dumps(row_dict, default=str)}")
        else:
            logger.error("❌ No data found in financial_positions table")
        
        # Check available dates
        dates_query = text("""
            SELECT DISTINCT date FROM financial_positions ORDER BY date
        """)
        
        dates = [row[0] for row in conn.execute(dates_query)]
        logger.info(f"Available dates in database: {dates}")
        
        # Check for "ENC:" values in adjusted_value column
        enc_query = text("""
            SELECT COUNT(*) FROM financial_positions 
            WHERE adjusted_value LIKE 'ENC:%'
        """)
        
        enc_count = conn.execute(enc_query).scalar()
        logger.info(f"Values with 'ENC:' prefix in adjusted_value: {enc_count}")
        
        # Check client counts by date
        client_query = text("""
            SELECT date, COUNT(DISTINCT top_level_client) 
            FROM financial_positions 
            GROUP BY date
            ORDER BY date
        """)
        
        client_counts = conn.execute(client_query).fetchall()
        logger.info("Client counts by date:")
        for date, count in client_counts:
            logger.info(f"   {date}: {count} clients")

def check_entity_options_query(engine):
    """Manually run the entity options query and see results"""
    logger.info("Testing entity options query...")
    
    with engine.connect() as conn:
        # Execute the entity-options query for clients
        query = text("""
        WITH date_records AS (
            SELECT DISTINCT date 
            FROM financial_positions 
            ORDER BY date DESC 
            LIMIT 1
        )
        SELECT DISTINCT top_level_client 
        FROM financial_positions fp
        JOIN date_records dr ON fp.date = dr.date
        ORDER BY top_level_client
        """)
        
        results = conn.execute(query).fetchall()
        
        logger.info(f"Found {len(results)} client options")
        if results:
            sample = [row[0] for row in results[:5]]
            logger.info(f"Sample clients: {sample}")
        
        # Test our original query with direct reference to 2025-05-01
        direct_query = text("""
        SELECT DISTINCT top_level_client 
        FROM financial_positions 
        WHERE date = '2025-05-01'
        ORDER BY top_level_client
        """)
        
        direct_results = conn.execute(direct_query).fetchall()
        logger.info(f"Using direct date reference found {len(direct_results)} client options")
        
        # Test extracting adjusted_value with SUBSTRING
        value_query = text("""
        SELECT 
            MIN(CASE WHEN adjusted_value LIKE 'ENC:%' 
                THEN CAST(SUBSTRING(adjusted_value, 5) AS DECIMAL) 
                ELSE CAST(adjusted_value AS DECIMAL) 
            END) as min_value,
            MAX(CASE WHEN adjusted_value LIKE 'ENC:%' 
                THEN CAST(SUBSTRING(adjusted_value, 5) AS DECIMAL) 
                ELSE CAST(adjusted_value AS DECIMAL) 
            END) as max_value
        FROM financial_positions
        WHERE date = '2025-05-01'
        """)
        
        try:
            value_results = conn.execute(value_query).fetchone()
            logger.info(f"Min/max adjusted values: {value_results.min_value} to {value_results.max_value}")
        except Exception as e:
            logger.error(f"Error extracting adjusted values: {str(e)}")

def test_api_queries(engine):
    """Test API queries directly against the database"""
    logger.info("Testing API queries directly...")
    
    with engine.connect() as conn:
        # Test allocation chart query
        allocation_query = text("""
        SELECT asset_class, SUM(CAST(
            CASE 
                WHEN adjusted_value LIKE 'ENC:%' THEN SUBSTRING(adjusted_value, 5)
                ELSE adjusted_value 
            END AS DECIMAL
        )) as total_value 
        FROM financial_positions 
        WHERE date = '2025-05-01'
        GROUP BY asset_class
        """)
        
        try:
            allocation_results = conn.execute(allocation_query).fetchall()
            logger.info(f"Allocation query returned {len(allocation_results)} rows")
            if allocation_results:
                for asset_class, total in allocation_results:
                    logger.info(f"   {asset_class}: {total}")
        except Exception as e:
            logger.error(f"❌ Allocation query failed: {str(e)}")
        
        # Test liquidity chart query
        liquidity_query = text("""
        SELECT liquid_vs_illiquid, SUM(CAST(
            CASE 
                WHEN adjusted_value LIKE 'ENC:%' THEN SUBSTRING(adjusted_value, 5)
                ELSE adjusted_value 
            END AS DECIMAL
        )) as total_value 
        FROM financial_positions 
        WHERE date = '2025-05-01'
        GROUP BY liquid_vs_illiquid
        """)
        
        try:
            liquidity_results = conn.execute(liquidity_query).fetchall()
            logger.info(f"Liquidity query returned {len(liquidity_results)} rows")
            if liquidity_results:
                for liquidity, total in liquidity_results:
                    logger.info(f"   {liquidity}: {total}")
        except Exception as e:
            logger.error(f"❌ Liquidity query failed: {str(e)}")

def check_frontend_api_calls():
    """Check what API calls the frontend is making"""
    logger.info("Checking frontend API calls in Dashboard.jsx...")
    
    try:
        with open('./frontend/src/components/Dashboard.jsx', 'r') as f:
            content = f.read()
            
            # Extract API calls
            api_calls = []
            import re
            api_pattern = r'api\.[a-zA-Z]+\([^)]*\)'
            matches = re.findall(api_pattern, content)
            logger.info(f"Found {len(matches)} API calls in Dashboard.jsx")
            for match in matches:
                logger.info(f"   {match}")
            
            # Extract date handling
            date_pattern = r'const \[reportDate, setReportDate\] = React\.useState\([^)]*\)'
            date_matches = re.findall(date_pattern, content)
            if date_matches:
                logger.info(f"Date initialization: {date_matches[0]}")
    except Exception as e:
        logger.error(f"Error checking frontend API calls: {str(e)}")

def check_financial_summary(engine):
    """Check for any financial summary records"""
    logger.info("Checking financial_summary table...")
    
    with engine.connect() as conn:
        # Check if table exists
        table_exists_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'financial_summary'
            )
        """)
        
        table_exists = conn.execute(table_exists_query).scalar()
        if not table_exists:
            logger.error("❌ financial_summary table does not exist")
            return
        
        # Get row count
        count_query = text("SELECT COUNT(*) FROM financial_summary")
        count = conn.execute(count_query).scalar()
        logger.info(f"Financial summary table has {count} rows")
        
        # Get column information to see what's available
        columns_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'financial_summary'
            ORDER BY ordinal_position
        """)
        
        columns = conn.execute(columns_query).fetchall()
        logger.info("Financial summary columns:")
        for col in columns:
            logger.info(f"   {col[0]} ({col[1]})")
        
        # Sample data
        sample_query = text("SELECT * FROM financial_summary LIMIT 3")
        sample = conn.execute(sample_query).fetchall()
        if sample:
            logger.info("Sample financial summary data:")
            column_names = [col[0] for col in columns]
            
            for i, row in enumerate(sample):
                row_dict = {col: getattr(row, col) for col in column_names}
                logger.info(f"Row {i+1}: {json.dumps(row_dict, default=str)}")

def main():
    """Main execution function"""
    logger.info("=== ANTINORI Data Load Diagnostic Tool ===")
    
    # Test database connection
    engine = test_database_connection()
    
    # Count rows in each table
    count_rows_by_table(engine)
    
    # Examine financial positions table
    examine_financial_positions(engine)
    
    # Check entity options query
    check_entity_options_query(engine)
    
    # Test API queries
    test_api_queries(engine)
    
    # Check frontend API calls
    check_frontend_api_calls()
    
    # Check financial summary
    check_financial_summary(engine)
    
    logger.info("=== Diagnostic complete ===")

if __name__ == "__main__":
    main()