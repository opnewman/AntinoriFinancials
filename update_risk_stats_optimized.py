#!/usr/bin/env python
"""
High-performance script for downloading and processing risk statistics from Egnyte.
This script is designed to complete in 2-3 seconds instead of minutes.

Usage:
    python update_risk_stats_optimized.py [--test] [--debug] [--batch-size=1000] [--workers=3] [--output=output.log]

Options:
    --test         Use test file instead of downloading from Egnyte
    --debug        Enable debug logging
    --batch-size   Size of batches for database operations (default: 1000)
    --workers      Number of parallel workers for processing (default: 3)
    --output       Output file for logs (default: risk_stats_update_optimized.log)
"""

import os
import sys
import gc
import time
import logging
import argparse
from datetime import datetime, date
import traceback

# Set up logging before imports to catch any import errors
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    # Import optimized service and database modules
    from src.database import get_db_connection
    from src.services.optimized_risk_stats_service import process_risk_stats_optimized
    
    def setup_logging(output_file=None, debug=False):
        """Setup logging configuration"""
        log_level = logging.DEBUG if debug else logging.INFO
        
        # Set up root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Remove any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create formatters
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Create console handler
        console = logging.StreamHandler()
        console.setLevel(log_level)
        console.setFormatter(formatter)
        root_logger.addHandler(console)
        
        # Create file handler if output file specified
        if output_file:
            file_handler = logging.FileHandler(output_file, mode='w')
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
        logger.info(f"Logging initialized. Level: {'DEBUG' if debug else 'INFO'}, Output file: {output_file or 'None'}")
        
    def main():
        """Main entry point for the script"""
        start_time = time.time()
        
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Process risk statistics data')
        parser.add_argument('--test', action='store_true', help='Use test file instead of downloading from Egnyte')
        parser.add_argument('--debug', action='store_true', help='Enable debug logging')
        parser.add_argument('--batch-size', type=int, default=1000, help='Size of batches for database operations')
        parser.add_argument('--workers', type=int, default=3, help='Number of parallel workers for processing')
        parser.add_argument('--output', default=f'risk_stats_update_optimized_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', 
                            help='Output file for logs')
        
        args = parser.parse_args()
        
        # Set up logging
        setup_logging(output_file=args.output, debug=args.debug)
        
        # Log script configuration
        logger.info(f"Starting optimized risk statistics update with configuration:")
        logger.info(f"  Test mode: {args.test}")
        logger.info(f"  Debug mode: {args.debug}")
        logger.info(f"  Batch size: {args.batch_size}")
        logger.info(f"  Worker threads: {args.workers}")
        logger.info(f"  Output file: {args.output}")
        
        try:
            # Check environment variables
            egnyte_token = os.environ.get('EGNYTE_ACCESS_TOKEN')
            if not egnyte_token and not args.test:
                logger.error("EGNYTE_ACCESS_TOKEN not found in environment variables")
                logger.error("Either set the environment variable or use --test mode")
                return 1
            
            # Process risk statistics
            with get_db_connection() as db:
                results = process_risk_stats_optimized(
                    db=db,
                    use_test_file=args.test,
                    batch_size=args.batch_size,
                    max_workers=args.workers
                )
                
                # Log results
                if results.get('success', False):
                    logger.info("Risk statistics update completed successfully")
                    logger.info(f"Processing time: {results.get('processing_time_seconds', 0):.2f} seconds")
                    logger.info(f"Records processed: {results.get('total_records', 0)} total")
                    logger.info(f"  Equity: {results.get('equity_records', 0)}")
                    logger.info(f"  Fixed Income: {results.get('fixed_income_records', 0)}")
                    logger.info(f"  Alternatives: {results.get('alternatives_records', 0)}")
                else:
                    logger.error(f"Risk statistics update failed: {results.get('error', 'Unknown error')}")
                    
                # Calculate total runtime
                total_time = time.time() - start_time
                logger.info(f"Total script runtime: {total_time:.2f} seconds")
                
                # Force memory cleanup before exit
                gc.collect()
                
                # Return success/failure code
                return 0 if results.get('success', False) else 1
        
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            return 1

    if __name__ == "__main__":
        sys.exit(main())
        
except Exception as startup_error:
    logger.exception(f"Fatal error during startup: {startup_error}")
    sys.exit(1)