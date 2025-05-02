#!/usr/bin/env python3
"""
Database migration script to add the row_order column and populate it.
This is essential for preserving the original Excel file row ordering.
"""

import os
import sys
import logging
from datetime import date
from typing import Dict, List, Tuple

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Execute the migration"""
    logger.info("Starting row_order migration")
    
    # Step 1: Add the row_order column if it doesn't exist
    with get_db_connection() as db:
        # Check if the column already exists
        result = db.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='ownership_items' AND column_name='row_order'
        """)).fetchone()
        
        if not result:
            logger.info("Adding row_order column to ownership_items table")
            db.execute(text("""
                ALTER TABLE ownership_items 
                ADD COLUMN row_order INTEGER
            """))
            
            # Create index for the new column
            db.execute(text("""
                CREATE INDEX idx_ownership_row_order 
                ON ownership_items (metadata_id, row_order)
            """))
            
            db.commit()
            logger.info("Successfully added row_order column and index")
        else:
            logger.info("row_order column already exists")
    
    # Step 2: Populate the row_order column based on ordering in the database
    # We'll use the existing id column as a proxy for the original ordering
    with get_db_connection() as db:
        # For each metadata_id, order records and set row_order
        metadata_ids = db.execute(text("""
            SELECT DISTINCT metadata_id FROM ownership_items
        """)).fetchall()
        
        for (metadata_id,) in metadata_ids:
            logger.info(f"Processing metadata_id: {metadata_id}")
            
            # Get records in their natural order (by id)
            records = db.execute(text("""
                SELECT id FROM ownership_items 
                WHERE metadata_id = :metadata_id 
                ORDER BY id
            """), {"metadata_id": metadata_id}).fetchall()
            
            # Set row_order for each record
            for idx, (record_id,) in enumerate(records, start=1):
                db.execute(text("""
                    UPDATE ownership_items 
                    SET row_order = :row_order 
                    WHERE id = :id
                """), {"row_order": idx, "id": record_id})
            
            db.commit()
            logger.info(f"Updated row_order for {len(records)} records with metadata_id {metadata_id}")
    
    logger.info("Row order migration completed successfully")

if __name__ == "__main__":
    main()