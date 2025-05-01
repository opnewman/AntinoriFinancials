#!/usr/bin/env python3
"""
Database initialization script for ANTINORI.
This script recreates the database tables with all indexes and optimizations.
"""

import os
import sys
import logging
from time import time

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import init_db
from src.models.models import Base, OwnershipItem, OwnershipMetadata

def main():
    """Initialize the database schema"""
    # Set up logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("Starting database initialization...")
    start_time = time()
    
    # Initialize the database
    try:
        init_db()
        logger.info("Database initialization completed successfully.")
        
        # Print some useful information about the schema
        logger.info(f"Created tables:")
        for table in Base.metadata.tables:
            logger.info(f"  - {table}")
        
        # List indexes for OwnershipItem
        logger.info(f"Indexes for OwnershipItem:")
        for index in OwnershipItem.__table__.indexes:
            logger.info(f"  - {index.name} on {', '.join(index.columns.keys())}")
        
        end_time = time()
        logger.info(f"Database initialization completed in {end_time - start_time:.2f} seconds.")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()