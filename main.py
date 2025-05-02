import logging
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
import datetime
from sqlalchemy import text, func
import time
from collections import defaultdict, Counter
import json
import re
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database module
from src.database import init_db, get_db, get_db_connection
from src.models.models import OwnershipMetadata, OwnershipItem, FinancialPosition, FinancialSummary
from src.utils.encryption import encryption_service

# Initialize database (create tables)
init_db()

# Initialize Flask app
app = Flask(__name__, static_folder='frontend')

# Enable CORS for all routes
CORS(app)

# Root endpoint - serve frontend
@app.route("/")
def root():
    return send_from_directory('frontend', 'index.html')

# Serve frontend static files
@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('frontend', path)

# Test upload page
@app.route('/test')
def test_page():
    return send_from_directory('frontend', 'upload_test.html')

# Ownership tree visualization page
@app.route('/ownership-tree')
def ownership_tree_page():
    return send_from_directory('frontend', 'ownership-tree.html')

# Upload data page
@app.route('/upload')
def upload_page():
    return send_from_directory('frontend', 'upload.html')

# Ownership relationship explorer page
@app.route('/ownership-explorer')
def ownership_explorer_page():
    return send_from_directory('frontend', 'ownership-explorer.html')

# API root endpoint
@app.route("/api")
def api_root():
    return jsonify({"message": "ANTINORI Financial Portfolio Reporting API"})

# Health check endpoint
@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

def generate_financial_summary(db, report_date):
    """
    Generate financial summary data by aggregating financial positions.
    
    This function calculates and stores summary data at multiple levels:
    - Client level
    - Group level
    - Portfolio level
    - Account level
    
    Args:
        db: Database session
        report_date: The report date
    """
    try:
        logger.info(f"Generating financial summary for {report_date}")
        
        # Delete existing summary data for the report date
        db.execute(text(
            "DELETE FROM financial_summary WHERE report_date = :report_date"
        ), {"report_date": report_date})
        
        # Get all positions for the report date
        positions = db.query(FinancialPosition).filter(
            FinancialPosition.date == report_date
        ).all()
        
        # Prepare summary data at different levels
        summary_data = {}
        
        # First, get latest metadata with proper classifications
        metadata_id = db.execute(text("""
            SELECT id 
            FROM ownership_metadata 
            WHERE id IN (
                SELECT DISTINCT metadata_id FROM ownership_items
                WHERE grouping_attribute_name IN ('Client', 'Group', 'Holding Account')
                GROUP BY metadata_id
                HAVING COUNT(DISTINCT grouping_attribute_name) = 3
            )
            ORDER BY id DESC 
            LIMIT 1
        """)).fetchone()[0]
        
        logger.info(f"Using metadata ID {metadata_id} for ownership relationships")
        
        # Build maps for relationship lookups
        # 1. Map account numbers to groups
        account_to_groups = defaultdict(set)
        # 2. Map account numbers to portfolios
        account_to_portfolio = {}
        # 3. Map portfolios to client names
        portfolio_to_client = {}
        
        # Get all ownership items for efficient lookups
        ownership_items = db.query(
            OwnershipItem.client,
            OwnershipItem.portfolio,
            OwnershipItem.holding_account_number,
            OwnershipItem.grouping_attribute_name,
            OwnershipItem.group_id
        ).filter(
            OwnershipItem.metadata_id == metadata_id
        ).all()
        
        # Build the relationship maps
        for item in ownership_items:
            if item.grouping_attribute_name == 'Client' and item.portfolio:
                portfolio_to_client[item.portfolio] = item.client
                
            elif item.grouping_attribute_name == 'Holding Account' and item.holding_account_number:
                if item.portfolio:
                    account_to_portfolio[item.holding_account_number] = item.portfolio
        
        # Get group relationships
        group_relationships = db.execute(text("""
            SELECT g.group_id, g.client AS group_name, a.holding_account_number
            FROM ownership_items g
            JOIN ownership_items a ON g.portfolio = a.portfolio
            WHERE g.metadata_id = :metadata_id 
              AND g.grouping_attribute_name = 'Group'
              AND a.metadata_id = :metadata_id
              AND a.grouping_attribute_name = 'Holding Account'
              AND a.holding_account_number IS NOT NULL
        """), {"metadata_id": metadata_id}).fetchall()
        
        # Build account to groups map
        for _, group_name, account_number in group_relationships:
            if account_number and group_name:
                account_to_groups[account_number].add(group_name)
        
        # Process all positions
        for position in positions:
            # Decrypt the adjusted value
            adjusted_value = encryption_service.decrypt_to_float(position.adjusted_value)
            
            # Skip positions with zero or negative values if needed
            # if adjusted_value <= 0:
            #     continue
            
            # Aggregate by client
            if position.top_level_client:
                client_key = f"client:{position.top_level_client}"
                if client_key not in summary_data:
                    summary_data[client_key] = 0
                summary_data[client_key] += adjusted_value
            
            # Aggregate by account
            if position.holding_account_number:
                account_key = f"account:{position.holding_account_number}"
                if account_key not in summary_data:
                    summary_data[account_key] = 0
                summary_data[account_key] += adjusted_value
            
            # Aggregate by portfolio
            if position.portfolio and position.portfolio != "-":
                portfolio_key = f"portfolio:{position.portfolio}"
                if portfolio_key not in summary_data:
                    summary_data[portfolio_key] = 0
                summary_data[portfolio_key] += adjusted_value
                
                # Also aggregate to client via portfolio relationship if not already done
                if position.portfolio in portfolio_to_client:
                    client_name = portfolio_to_client[position.portfolio]
                    alt_client_key = f"client:{client_name}"
                    if alt_client_key not in summary_data:
                        summary_data[alt_client_key] = 0
                    summary_data[alt_client_key] += adjusted_value
            
            # Aggregate by groups based on account number
            if position.holding_account_number in account_to_groups:
                for group_name in account_to_groups[position.holding_account_number]:
                    group_key = f"group:{group_name}"
                    if group_key not in summary_data:
                        summary_data[group_key] = 0
                    summary_data[group_key] += adjusted_value
        
        # Create summary records
        summary_records = []
        for key, total_value in summary_data.items():
            level, level_key = key.split(':', 1)
            summary = FinancialSummary(
                level=level,
                level_key=level_key,
                total_adjusted_value=total_value,
                report_date=report_date
            )
            summary_records.append(summary)
        
        # Bulk insert summaries
        db.bulk_save_objects(summary_records)
        db.commit()
        
        logger.info(f"Generated {len(summary_records)} financial summary records")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error generating financial summary: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# API Endpoints
@app.route("/api/upload/data-dump", methods=["POST"])
def upload_data_dump():
    """
    Upload and process data_dump.xlsx file which contains financial position data.
    
    This endpoint processes an Excel file with the following columns:
    - Position (security name)
    - Top Level Client (client name)
    - Holding Account (account name)
    - Holding Account Number
    - Portfolio (portfolio name)
    - CUSIP (unique security identifier)
    - Ticker Symbol
    - Asset Class
    - Second Level (secondary classification)
    - Third Level (tertiary classification)
    - ADV Classification
    - Liquid vs. Illiquid
    - Adjusted Value (in dollars)
    
    The first 3 rows of the file contain metadata:
    - Row 1: View Name (e.g., "DATA DUMP")
    - Row 2: Date Range (e.g., "05-01-2025 to 05-01-2025")
    - Row 3: Portfolio (e.g., "All clients")
    
    The actual column headers are in row 4.
    """
    # Set the response content type to ensure proper JSON response
    response_headers = {"Content-Type": "application/json"}
    
    # Early validation for required fields
    if 'file' not in request.files:
        response = jsonify({
            "success": False, 
            "message": "No file part in the request"
        })
        return response, 400, response_headers
    
    file = request.files['file']
    if file.filename == '':
        response = jsonify({
            "success": False, 
            "message": "No file selected"
        })
        return response, 400, response_headers
    
    # Check file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ['.xlsx', '.xls', '.csv', '.txt']:
        response = jsonify({
            "success": False, 
            "message": "Only Excel files (.xlsx, .xls), CSV or TXT files are supported"
        })
        return response, 400, response_headers
    
    try:
        # Start timing the process
        start_time = time.time()
        logger.info(f"Data dump upload started for file: {file.filename}")
        
        # Save the file to temporary storage
        import tempfile
        import subprocess
        
        # Create a temporary directory to store the file
        temp_dir = tempfile.mkdtemp(prefix="data_dump_")
        file_path = os.path.join(temp_dir, secure_filename(file.filename))
        
        # Save the file
        file.save(file_path)
        file_size = os.path.getsize(file_path)
        logger.info(f"File saved to: {file_path}, size: {file_size} bytes")
        
        # Start the background process to handle the file
        # This avoids web server timeouts by immediately returning and
        # allowing the file to be processed in a separate process
        logger.info("Starting background process for data upload")
        
        # Run as a direct shell command to ensure it continues after this request completes
        os.system(f"python run_data_upload.py {file_path} &")
        
        # Return immediate response to client
        return jsonify({
            "success": True,
            "message": f"File received and processing started in background.",
            "file_size": file_size,
            "status": "processing",
            "status_url": f"/api/upload/status?file={secure_filename(file.filename)}"
        }), 202, response_headers
        
    except Exception as e:
        logger.error(f"Error starting data upload process: {str(e)}")
        logger.error(traceback.format_exc())
        response = jsonify({
            "success": False,
            "message": f"Error processing file: {str(e)}",
            "errors": [str(e)]
        })
        return response, 500, response_headers

# Endpoint to check the status of a background upload job
@app.route("/api/upload/status", methods=["GET"])
def check_upload_status():
    file_name = request.args.get('file')
    if not file_name:
        return jsonify({
            "success": False,
            "message": "Missing file parameter"
        }), 400
    
    logger.info(f"Checking upload status for file: {file_name}")
    
    # Look in all temp directories for the status file
    import glob
    temp_dir_pattern = "/tmp/data_dump_*"
    temp_dirs = glob.glob(temp_dir_pattern)
    logger.info(f"Found {len(temp_dirs)} temp directories: {temp_dirs}")
    
    # First check all directories for completion
    for temp_dir in temp_dirs:
        # Check for completion file
        completed_file = os.path.join(temp_dir, "data_dump_complete.txt")
        
        if os.path.exists(completed_file):
            logger.info(f"Found completion file: {completed_file}")
            try:
                with open(completed_file, "r") as f:
                    status_info = f.read()
                
                # Parse the completion data
                rows_processed = 0
                total_rows = 0
                report_date = None
                
                for line in status_info.split("\n"):
                    if "Rows processed:" in line:
                        total_rows = int(line.split(":", 1)[1].strip())
                    elif "Rows inserted:" in line:
                        rows_processed = int(line.split(":", 1)[1].strip())
                    elif "Report date:" in line:
                        report_date = line.split(":", 1)[1].strip()
                
                return jsonify({
                    "success": True,
                    "status": "completed",
                    "message": f"Processing completed. Inserted {rows_processed} of {total_rows} rows for {report_date}.",
                    "rows_processed": rows_processed,
                    "total_rows": total_rows,
                    "report_date": report_date
                })
            except Exception as e:
                logger.error(f"Error parsing completion file: {str(e)}")
    
    # If no completion found, check for processing
    for temp_dir in temp_dirs:
        # Check for started but not completed file
        started_file = os.path.join(temp_dir, "data_dump_started.txt")
        
        if os.path.exists(started_file):
            logger.info(f"Found started file: {started_file}")
            # Check log file for progress
            log_file = "upload_data_dump.log"
            
            if os.path.exists(log_file):
                try:
                    # Get last few lines of log file
                    with open(log_file, "r") as f:
                        log_lines = f.readlines()[-20:]  # Last 20 lines
                    
                    # Look for chunk progress
                    progress_info = []
                    for line in log_lines:
                        if "Chunk" in line and "rows" in line:
                            progress_info.append(line.strip())
                        # Also check for successful completion in log
                        elif "Processing successful" in line and "seconds" in line:
                            # This means processing completed but status file wasn't found
                            return jsonify({
                                "success": True,
                                "status": "completed",
                                "message": "Processing completed successfully.",
                                "rows_processed": 0,  # Unknown count
                                "total_rows": 0,
                                "details": line.strip()
                            })
                    
                    return jsonify({
                        "success": True,
                        "status": "processing",
                        "message": "File is still being processed.",
                        "progress": progress_info[-3:] if progress_info else []  # Last 3 progress messages
                    })
                except Exception as e:
                    logger.error(f"Error reading log file: {str(e)}")
            
            return jsonify({
                "success": True,
                "status": "processing",
                "message": "File is still being processed. Check back later."
            })
    
    # If we got here, check if we have the expected database rows
    try:
        # Count financial positions for the most recent date
        from sqlalchemy import func, desc
        with get_db_connection() as db:
            # Get the most recent date
            latest_date = db.query(func.max(FinancialPosition.date)).scalar()
            
            if latest_date:
                # Count rows for this date
                count = db.query(func.count(FinancialPosition.id)).filter(
                    FinancialPosition.date == latest_date
                ).scalar()
                
                if count > 0:
                    return jsonify({
                        "success": True,
                        "status": "completed",
                        "message": f"Processing completed. Found {count} rows in database for {latest_date}.",
                        "rows_processed": count,
                        "total_rows": count,
                        "report_date": str(latest_date)
                    })
    except Exception as e:
        logger.error(f"Error checking database: {str(e)}")
    
    # No status files found
    return jsonify({
        "success": False,
        "status": "unknown",
        "message": "No status information found for this file."
    })

@app.route("/api/upload/ownership", methods=["POST"])
def upload_ownership_tree():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file part in the request"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    # Check file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ['.xlsx', '.xls', '.csv', '.txt']:
        return jsonify({
            "success": False, 
            "message": "Only Excel files (.xlsx, .xls), CSV or TXT files are supported"
        }), 400
    
    try:
        import pandas as pd
        from io import BytesIO, StringIO
        
        # Start timing the process
        start_time = time.time()
        logger.info(f"Upload started for file: {file.filename}")
        
        # Read the file content
        file_content = file.read()
        logger.info(f"File size: {len(file_content)} bytes")
        
        # Process the file based on its type
        view_name = "NORI Ownership"
        start_date = end_date = datetime.date.today()
        portfolio_coverage = "All clients"
        
        if file_ext in ['.xlsx', '.xls']:
            # Excel file - use BytesIO with optimized settings
            excel_data = BytesIO(file_content)
            
            # Extract metadata from first 3 rows only
            try:
                metadata_df = pd.read_excel(excel_data, nrows=3, header=None, engine='openpyxl')
                
                # Extract view name, date range, and portfolio coverage efficiently
                if len(metadata_df) > 0 and len(metadata_df.columns) > 1 and not pd.isna(metadata_df.iloc[0, 1]):
                    view_name = str(metadata_df.iloc[0, 1])
                
                # Parse date range
                if len(metadata_df) > 1 and len(metadata_df.columns) > 1 and not pd.isna(metadata_df.iloc[1, 1]):
                    date_range_str = str(metadata_df.iloc[1, 1])
                    # Try multiple date formats and patterns
                    date_patterns = [
                        r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})',  # MM-DD-YYYY to MM-DD-YYYY
                        r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD to YYYY-MM-DD
                        r'(\w+ \d{1,2}, \d{4})\s+to\s+(\w+ \d{1,2}, \d{4})'  # Month DD, YYYY to Month DD, YYYY
                    ]
                    
                    # Try each pattern
                    for pattern in date_patterns:
                        match = re.search(pattern, date_range_str)
                        if match:
                            try:
                                if pattern == r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})':
                                    start_date_str, end_date_str = match.groups()
                                    start_date = datetime.datetime.strptime(start_date_str, '%m-%d-%Y').date()
                                    end_date = datetime.datetime.strptime(end_date_str, '%m-%d-%Y').date()
                                    break
                                elif pattern == r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})':
                                    start_date_str, end_date_str = match.groups()
                                    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
                                    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
                                    break
                                elif pattern == r'(\w+ \d{1,2}, \d{4})\s+to\s+(\w+ \d{1,2}, \d{4})':
                                    start_date_str, end_date_str = match.groups()
                                    start_date = datetime.datetime.strptime(start_date_str, '%B %d, %Y').date()
                                    end_date = datetime.datetime.strptime(end_date_str, '%B %d, %Y').date()
                                    break
                            except ValueError:
                                continue  # Try next pattern if this one fails
                
                # Get portfolio coverage
                if len(metadata_df) > 2 and len(metadata_df.columns) > 1 and not pd.isna(metadata_df.iloc[2, 1]):
                    portfolio_coverage = str(metadata_df.iloc[2, 1])
                
                # Reset file pointer for data rows
                excel_data.seek(0)
                
                # Read data with optimized settings
                df = pd.read_excel(
                    excel_data, 
                    header=3,  # Header is in row 4 (0-indexed)
                    engine='openpyxl',
                    dtype={
                        'Client': str,
                        'Entity ID': str,
                        'Holding Account Number': str,
                        'Portfolio': str,
                        'Group ID': str,
                        'Grouping Attribute Name': str
                    }
                )
                
                logger.info(f"Excel file parsed, rows: {len(df)}")
            
            except Exception as e:
                logger.error(f"Error parsing Excel file: {str(e)}")
                return jsonify({
                    "success": False,
                    "message": f"Error parsing Excel file: {str(e)}",
                    "rows_processed": 0,
                    "rows_inserted": 0,
                    "errors": [str(e)]
                }), 400
                
        elif file_ext in ['.csv', '.txt']:
            # CSV or TXT file - use StringIO with optimized approach
            try:
                # Decode file content
                text_content = file_content.decode('utf-8')
                
                # Split by lines to get metadata
                lines = text_content.splitlines()
                if len(lines) >= 3:
                    # Extract view name (line 1)
                    if ':' in lines[0]:
                        view_name = lines[0].split(':', 1)[1].strip()
                    
                    # Extract date range (line 2)
                    if ':' in lines[1]:
                        date_range_str = lines[1].split(':', 1)[1].strip()
                        # Try multiple date formats and patterns (same as Excel)
                        date_patterns = [
                            r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})',  # MM-DD-YYYY to MM-DD-YYYY
                            r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD to YYYY-MM-DD
                            r'(\w+ \d{1,2}, \d{4})\s+to\s+(\w+ \d{1,2}, \d{4})'  # Month DD, YYYY to Month DD, YYYY
                        ]
                        
                        # Try each pattern
                        for pattern in date_patterns:
                            match = re.search(pattern, date_range_str)
                            if match:
                                try:
                                    if pattern == r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})':
                                        start_date_str, end_date_str = match.groups()
                                        start_date = datetime.datetime.strptime(start_date_str, '%m-%d-%Y').date()
                                        end_date = datetime.datetime.strptime(end_date_str, '%m-%d-%Y').date()
                                        break
                                    elif pattern == r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})':
                                        start_date_str, end_date_str = match.groups()
                                        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
                                        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
                                        break
                                    elif pattern == r'(\w+ \d{1,2}, \d{4})\s+to\s+(\w+ \d{1,2}, \d{4})':
                                        start_date_str, end_date_str = match.groups()
                                        start_date = datetime.datetime.strptime(start_date_str, '%B %d, %Y').date()
                                        end_date = datetime.datetime.strptime(end_date_str, '%B %d, %Y').date()
                                        break
                                except ValueError:
                                    continue  # Try next pattern if this one fails
                    
                    # Extract portfolio coverage (line 3)
                    if ':' in lines[2]:
                        portfolio_coverage = lines[2].split(':', 1)[1].strip()
                
                # Create a new buffer with just the data rows (skip metadata and header)
                data_buffer = StringIO('\n'.join(lines[4:]))  # Skip first 4 lines
                
                # Determine the delimiter
                delimiter = '\t' if file_ext == '.txt' else ','
                
                # Read the data with optimized settings
                df = pd.read_csv(
                    data_buffer,
                    sep=delimiter,
                    dtype={
                        'Client': str,
                        'Entity ID': str,
                        'Holding Account Number': str,
                        'Portfolio': str,
                        'Group ID': str,
                        'Grouping Attribute Name': str
                    }
                )
                
                logger.info(f"{file_ext} file parsed, rows: {len(df)}")
                
            except Exception as e:
                logger.error(f"Error parsing text file: {str(e)}")
                return jsonify({
                    "success": False,
                    "message": f"Error parsing text file: {str(e)}",
                    "rows_processed": 0,
                    "rows_inserted": 0,
                    "errors": [str(e)]
                }), 400
        
        # Standardize column names (convert to lowercase and replace spaces with underscores)
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Handle special case for data_inception_date
        data_inception_col = None
        for col in df.columns:
            if 'inception' in col.lower():
                data_inception_col = col
                break
        
        with get_db_connection() as db:
            # Check if we have all the required columns
            required_cols = ['client', 'grouping_attribute_name']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                return jsonify({
                    "success": False,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "rows_processed": 0,
                    "rows_inserted": 0,
                    "errors": [f"Missing required columns: {', '.join(missing_cols)}"]
                }), 400
            
            # Create new metadata record
            new_metadata = OwnershipMetadata(
                view_name=view_name,
                date_range_start=start_date,
                date_range_end=end_date,
                portfolio_coverage=portfolio_coverage,
                is_current=True  # This is the new current metadata
            )
            
            # Add metadata record and get its ID
            db.add(new_metadata)
            
            # Mark other metadata as not current
            db.execute(text("""
                UPDATE ownership_metadata
                SET is_current = FALSE
                WHERE id != (SELECT MAX(id) FROM ownership_metadata)
            """))
            
            db.commit()
            
            # Get the new metadata ID
            metadata_id = new_metadata.id
            logger.info(f"Created new metadata record with ID: {metadata_id}")
            
            # Process the dataframe in batches for better memory management
            total_rows = len(df)
            BATCH_SIZE = 500
            rows_processed = 0
            rows_inserted = 0
            errors = []
            
            for i in range(0, total_rows, BATCH_SIZE):
                batch_df = df.iloc[i:i+BATCH_SIZE]
                ownership_items = []
                
                # Use row order for ordering in the Excel file
                for idx, row in enumerate(batch_df.itertuples(), start=1):
                    try:
                        rows_processed += 1
                        
                        # Handle missing values
                        client = getattr(row, 'client', '') or ''
                        entity_id = getattr(row, 'entity_id', None)
                        holding_account_number = getattr(row, 'holding_account_number', None)
                        portfolio = getattr(row, 'portfolio', None)
                        group_id = getattr(row, 'group_id', None)
                        grouping_attribute_name = getattr(row, 'grouping_attribute_name', '') or 'Unknown'
                        
                        # Parse data inception date if available
                        data_inception_date = None
                        if data_inception_col and hasattr(row, data_inception_col):
                            inception_value = getattr(row, data_inception_col)
                            if inception_value and not pd.isna(inception_value):
                                try:
                                    if isinstance(inception_value, datetime.datetime):
                                        data_inception_date = inception_value.date()
                                    elif isinstance(inception_value, str):
                                        # Try different date formats
                                        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d-%b-%Y', '%d/%m/%Y']:
                                            try:
                                                data_inception_date = datetime.datetime.strptime(inception_value, fmt).date()
                                                break
                                            except ValueError:
                                                continue
                                except Exception as e:
                                    logger.warning(f"Could not parse inception date '{inception_value}': {str(e)}")
                        
                        # Parse ownership percentage if available
                        ownership_percentage = None
                        if hasattr(row, 'ownership_percentage'):
                            pct_value = getattr(row, 'ownership_percentage')
                            if pct_value and not pd.isna(pct_value):
                                try:
                                    if isinstance(pct_value, (int, float)):
                                        ownership_percentage = float(pct_value)
                                    elif isinstance(pct_value, str):
                                        # Remove % sign and convert to float
                                        ownership_percentage = float(pct_value.replace('%', '')) / 100
                                except Exception as e:
                                    logger.warning(f"Could not parse ownership percentage '{pct_value}': {str(e)}")
                        
                        # Create ownership item
                        item = OwnershipItem(
                            client=client,
                            entity_id=entity_id if entity_id and not pd.isna(entity_id) else None,
                            holding_account_number=holding_account_number if holding_account_number and not pd.isna(holding_account_number) else None,
                            portfolio=portfolio if portfolio and not pd.isna(portfolio) else None,
                            group_id=group_id if group_id and not pd.isna(group_id) else None,
                            data_inception_date=data_inception_date,
                            ownership_percentage=ownership_percentage,
                            grouping_attribute_name=grouping_attribute_name,
                            metadata_id=metadata_id,
                            row_order=i + idx  # Store original Excel row order
                        )
                        
                        ownership_items.append(item)
                        rows_inserted += 1
                        
                    except Exception as e:
                        error_msg = f"Error processing row {rows_processed}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Bulk insert the batch
                db.bulk_save_objects(ownership_items)
                db.commit()
                logger.info(f"Batch processed: {i}-{i+len(batch_df)} of {total_rows}")
            
            # Calculate processing time
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Clear any ownership tree cache
            # The ownership tree endpoint will rebuild the cache on next request
            
            return jsonify({
                "success": True,
                "message": f"Successfully processed {rows_inserted} ownership items",
                "rows_processed": rows_processed,
                "rows_inserted": rows_inserted,
                "processing_time_seconds": round(processing_time, 3),
                "metadata_id": metadata_id,
                "errors": errors[:10]  # Limit number of errors returned
            })
    
    except Exception as e:
        logger.error(f"Error processing ownership file: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "message": f"Error processing file: {str(e)}",
            "rows_processed": 0,
            "rows_inserted": 0,
            "errors": [str(e)]
        }), 500

@app.route("/api/upload/risk-stats", methods=["POST"])
def upload_security_risk_stats():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file part in the request"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    # Placeholder for implementation
    return jsonify({
        "success": True,
        "message": "Risk statistics upload not implemented yet",
        "rows_processed": 0,
        "rows_inserted": 0,
        "errors": []
    })

# Cache for ownership tree to improve performance
ownership_tree_cache = {
    "tree": None,
    "metadata_id": None,
    "timestamp": 0,
    "client_count": 0,
    "total_records": 0
}
CACHE_TTL = 300  # 5 minutes

@app.route("/api/ownership-tree", methods=["GET"])
def get_ownership_tree():
    """
    Get the complete ownership hierarchy tree.
    
    This builds a hierarchical tree structure showing the relationships between:
    - Clients
    - Groups
    - Holding Accounts (financial accounts)
    
    The tree follows the order of the original Excel file.
    """
    try:
        # Get query parameters for filtering
        client_filter = request.args.get('client', '')
        
        # Start timing the process
        start_time = time.time()
        
        # Initialize the tree structure
        tree = {
            "name": "All Clients",
            "children": []
        }
        
        with get_db_connection() as db:
            # Try to find the latest metadata with proper client/group/account classifications
            # Avoid using metadata with all "Unknown" grouping attributes
            latest_metadata = db.execute(text("""
                SELECT m.* 
                FROM ownership_metadata m
                WHERE m.id IN (
                    SELECT DISTINCT metadata_id FROM ownership_items
                    WHERE grouping_attribute_name IN ('Client', 'Group', 'Holding Account')
                    GROUP BY metadata_id
                    HAVING COUNT(DISTINCT grouping_attribute_name) = 3
                )
                ORDER BY m.id DESC
                LIMIT 1
            """)).fetchone()
            
            if not latest_metadata:
                # Fallback to most recent metadata if no good one is found
                latest_metadata = db.execute(text("""
                    SELECT * FROM ownership_metadata 
                    ORDER BY id DESC LIMIT 1
                """)).fetchone()
                
            logger.info(f"Using metadata ID {latest_metadata.id} which has proper Client/Group/Holding Account classifications")
            
            # Check if we have a valid cached tree for this metadata
            use_cache = False
            if (ownership_tree_cache["tree"] and 
                ownership_tree_cache["metadata_id"] == latest_metadata.id and
                time.time() - ownership_tree_cache["timestamp"] < CACHE_TTL):
                
                # Use cached tree if query parameters match
                if client_filter == '':
                    logger.info("Using cached ownership tree")
                    return jsonify({
                        "success": True,
                        "data": ownership_tree_cache["tree"],
                        "client_count": ownership_tree_cache["client_count"],
                        "total_records": ownership_tree_cache["total_records"],
                        "processing_time_seconds": 0.0,
                        "from_cache": True
                    })
            
            # Count total number of records
            total_records = db.execute(text("""
                SELECT COUNT(*) FROM ownership_items 
                WHERE metadata_id = :metadata_id
            """), {"metadata_id": latest_metadata.id}).fetchone()[0]
            
            # Count client entries
            client_count = db.execute(text("""
                SELECT COUNT(*) FROM ownership_items 
                WHERE metadata_id = :metadata_id 
                AND grouping_attribute_name = 'Client'
            """), {"metadata_id": latest_metadata.id}).fetchone()[0]
            
            logger.info(f"Total client count: {client_count}")
            
            # Verify we have client records
            client_count_check = db.execute(text("""
                SELECT COUNT(*) FROM ownership_items 
                WHERE metadata_id = :metadata_id 
                AND grouping_attribute_name = 'Client'
            """), {"metadata_id": latest_metadata.id}).fetchone()[0]
            
            logger.info(f"Found {client_count_check} entries with grouping_attribute_name = 'Client'")
            
            # Get all distinct clients
            clients_query = db.execute(text("""
                SELECT DISTINCT client FROM ownership_items 
                WHERE metadata_id = :metadata_id 
                ORDER BY client
                LIMIT 100
            """), {"metadata_id": latest_metadata.id})
            
            clients = [row[0] for row in clients_query if row[0]]
            
            logger.info(f"Found {len(clients)} distinct clients (showing first 100)")
            
            # Apply client filter if provided
            if client_filter:
                clients = [c for c in clients if client_filter.lower() in c.lower()]
            
            # Determine which entities are accounts
            account_type_check = db.execute(text("""
                SELECT COUNT(*) FROM ownership_items 
                WHERE metadata_id = :metadata_id 
                AND grouping_attribute_name = 'Holding Account'
            """), {"metadata_id": latest_metadata.id}).fetchone()[0]
            
            if account_type_check > 0:
                account_type = 'Holding Account'
                logger.info(f"Using '{account_type}' grouping attribute to identify accounts")
            else:
                # Fallback - look for account numbers
                account_type = 'Unknown'
                logger.warning("No 'Holding Account' entries found, using fallback detection")
            
            # Determine which entities are groups
            group_type_check = db.execute(text("""
                SELECT COUNT(*) FROM ownership_items 
                WHERE metadata_id = :metadata_id 
                AND grouping_attribute_name = 'Group'
            """), {"metadata_id": latest_metadata.id}).fetchone()[0]
            
            if group_type_check > 0:
                group_type = 'Group'
                logger.info(f"Using '{group_type}' grouping attribute to identify groups")
            else:
                # Fallback - look for group_id
                group_type = 'Unknown'
                logger.warning("No 'Group' entries found, using fallback detection")
            
            # Get entities that are likely accounts but may be misclassified
            likely_accounts = set()
            potential_true_clients = set()
            
            # Count word frequency to help identify clients vs accounts
            client_words = Counter()
            
            for client in clients:
                if client:
                    words = client.split()
                    client_words.update(words)
                    
                    # Heuristics to identify true clients
                    if " Trust" in client or " Family" in client:
                        potential_true_clients.add(client)
            
            # Get all entities in their ORIGINAL ROW ORDER from the Excel file
            # This is critical to maintaining the hierarchical structure based on row appearance
            logger.info("Building ownership tree based on Excel file row ordering")
            
            # Get all entities ordered by their row_order field
            all_entities = db.query(
                OwnershipItem.client,
                OwnershipItem.portfolio,
                OwnershipItem.entity_id,
                OwnershipItem.group_id,
                OwnershipItem.holding_account_number,
                OwnershipItem.grouping_attribute_name,
                OwnershipItem.id  # Use ID as a proxy for row order until migration completes
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id
            ).order_by(
                # First try to use row_order if available, fall back to id if not
                text("COALESCE(row_order, id) ASC")
            ).all()
            
            # Create a set of client names and maps for all entity types
            client_names = set()
            client_portfolio_map = {}  # Maps client name to their portfolio
            client_entity_map = {}     # Maps client name to their entity_id
            group_name_to_id = {}      # Maps group name to its ID
            
            # Maps to store parent-child relationships
            client_to_groups = defaultdict(list)    # Maps client name to its groups
            client_to_accounts = defaultdict(list)  # Maps client name to its direct accounts
            group_to_accounts = defaultdict(list)   # Maps group name to its accounts
            
            # Process all entities following the Excel file row order
            current_client = None
            current_group = None
            
            for entity in all_entities:
                # Skip rows with missing essential data
                if not entity.client:
                    continue
                
                # Process different entity types
                if entity.grouping_attribute_name == "Client":
                    # Found a new client - this starts a new section in the hierarchy
                    current_client = entity.client
                    current_group = None  # Reset current group when a new client starts
                    
                    # Store client information
                    client_names.add(current_client)
                    if entity.portfolio:
                        client_portfolio_map[current_client] = entity.portfolio
                    if entity.entity_id:
                        client_entity_map[current_client] = entity.entity_id
                
                elif entity.grouping_attribute_name == "Group" and current_client:
                    # Found a group that belongs to the current client
                    current_group = entity.client
                    
                    # Store group information
                    if entity.group_id:
                        group_name_to_id[current_group] = entity.group_id
                    
                    # Link this group to its parent client
                    client_to_groups[current_client].append({
                        "name": current_group,
                        "id": entity.group_id
                    })
                
                elif entity.grouping_attribute_name == "Holding Account":
                    # This is an account - link it to either current group or client
                    account_data = {
                        "name": entity.client,
                        "entity_id": entity.entity_id,
                        "account_number": entity.holding_account_number,
                        "value": 1  # Fixed value for visualization
                    }
                    
                    if current_group and current_client:
                        # Account belongs to the current group
                        group_to_accounts[current_group].append(account_data)
                    elif current_client:
                        # Direct account owned by the client (no group)
                        client_to_accounts[current_client].append(account_data)
            
            # Build the tree following the Excel file ordering hierarchy
            # Only include clients in our filtered set
            for client_name in clients:
                # Skip if not marked as a Client when we have Client markers
                if client_name not in client_names and client_count_check > 0:
                    continue
                
                # Create the client node
                client_node = {
                    "name": client_name,
                    "children": []
                }
                
                # Add all groups for this client based on Excel row ordering
                group_nodes_added = False
                
                # Add groups with their accounts
                for group_data in client_to_groups.get(client_name, []):
                    group_name = group_data["name"]
                    
                    # Create group node
                    group_node = {
                        "name": group_name,
                        "children": []
                    }
                    
                    # Add all accounts that belong to this group
                    for account in group_to_accounts.get(group_name, []):
                        group_node["children"].append(account)
                    
                    # Only add groups that have accounts or a valid ID
                    if group_node["children"] or group_name in group_name_to_id:
                        client_node["children"].append(group_node)
                        group_nodes_added = True
                
                # Add direct accounts for this client
                direct_accounts = client_to_accounts.get(client_name, [])
                
                if direct_accounts:
                    direct_accounts_node = {
                        "name": "Direct Accounts",
                        "children": []
                    }
                    
                    for account in direct_accounts:
                        direct_accounts_node["children"].append(account)
                    
                    # Only add the direct accounts node if it has accounts
                    if direct_accounts_node["children"]:
                        client_node["children"].append(direct_accounts_node)
                        group_nodes_added = True
                
                # Add client to tree if it has any children or is a verified client
                if group_nodes_added:
                    tree["children"].append(client_node)
                elif client_name in potential_true_clients and client_name not in likely_accounts:
                    # Include empty clients if they're verified as true clients
                    tree["children"].append(client_node)
            
            # Calculate time
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Update cache
            ownership_tree_cache["tree"] = tree
            ownership_tree_cache["metadata_id"] = latest_metadata.id
            ownership_tree_cache["timestamp"] = time.time()
            ownership_tree_cache["client_count"] = client_count
            ownership_tree_cache["total_records"] = total_records
            
            # Return the tree structure with additional metadata
            return jsonify({
                "success": True, 
                "data": tree,
                "client_count": client_count,
                "total_records": total_records,
                "processing_time_seconds": round(processing_time, 3),
                "from_cache": False
            })
    
    except Exception as e:
        logger.error(f"Error generating ownership tree: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Error generating ownership tree: {str(e)}"
        }), 500

@app.route("/api/portfolio-report", methods=["GET"])
def generate_portfolio_report():
    date = request.args.get('date', datetime.date.today().isoformat())
    level = request.args.get('level', 'portfolio')
    level_key = request.args.get('level_key', 'Portfolio 1')
    
    return jsonify({
        "report_date": date,
        "level": level,
        "level_key": level_key,
        "total_adjusted_value": 5000000.00,
        "asset_allocation": {
            "Equities": 45.5,
            "Fixed Income": 30.0,
            "Alternatives": 15.5,
            "Cash": 9.0
        },
        "liquidity": {
            "Daily": 60.0,
            "Weekly": 15.0,
            "Monthly": 10.0,
            "Quarterly": 10.0,
            "Yearly": 5.0
        },
        "performance": [
            {"period": "1D", "value": 50000.00, "percentage": 1.0},
            {"period": "MTD", "value": 150000.00, "percentage": 3.0},
            {"period": "QTD", "value": 250000.00, "percentage": 5.0},
            {"period": "YTD", "value": 450000.00, "percentage": 9.0}
        ],
        "risk_metrics": [
            {"metric": "Volatility", "value": 12.5},
            {"metric": "Sharpe Ratio", "value": 1.8},
            {"metric": "Beta", "value": 0.85}
        ]
    })

@app.route("/api/charts/allocation", methods=["GET"])
def get_allocation_chart_data():
    date = request.args.get('date', datetime.date.today().isoformat())
    level = request.args.get('level', 'client')
    level_key = request.args.get('level_key', 'All Clients')
    
    try:
        with get_db_connection() as db:
            # Create a connection to query the database
            if level == 'client':
                # For 'All Clients' or a specific client
                if level_key == 'All Clients':
                    # Query all asset classes and sum their values
                    query = text("""
                    SELECT asset_class, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                    FROM financial_positions 
                    WHERE report_date = :date
                    GROUP BY asset_class
                    """)
                    result = db.execute(query, {"date": date})
                else:
                    # For a specific client
                    query = text("""
                    SELECT asset_class, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                    FROM financial_positions 
                    WHERE report_date = :date AND top_level_client = :client
                    GROUP BY asset_class
                    """)
                    result = db.execute(query, {"date": date, "client": level_key})
            elif level == 'group':
                # For a specific group
                query = text("""
                SELECT asset_class, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                FROM financial_positions 
                WHERE report_date = :date AND group_name = :group
                GROUP BY asset_class
                """)
                result = db.execute(query, {"date": date, "group": level_key})
            elif level == 'portfolio':
                # For a specific portfolio
                query = text("""
                SELECT asset_class, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                FROM financial_positions 
                WHERE report_date = :date AND portfolio = :portfolio
                GROUP BY asset_class
                """)
                result = db.execute(query, {"date": date, "portfolio": level_key})
            elif level == 'account':
                # For a specific account
                query = text("""
                SELECT asset_class, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                FROM financial_positions 
                WHERE report_date = :date AND holding_account = :account
                GROUP BY asset_class
                """)
                result = db.execute(query, {"date": date, "account": level_key})
            else:
                # Default to all data if level is not recognized
                query = text("""
                SELECT asset_class, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                FROM financial_positions 
                WHERE report_date = :date
                GROUP BY asset_class
                """)
                result = db.execute(query, {"date": date})
            
            # Fetch results
            results = result.fetchall()
            
            if not results:
                # If no data is found, return default values
                logger.warning(f"No data found for allocation chart with date={date}, level={level}, level_key={level_key}")
                return jsonify({
                    "labels": ["Equities", "Fixed Income", "Alternatives", "Cash"],
                    "datasets": [{
                        "data": [45.5, 30.0, 15.5, 9.0],
                        "backgroundColor": ["#4C72B0", "#55A868", "#C44E52", "#8172B3"],
                        "borderWidth": 1
                    }]
                })
            
            # Extract labels and data from query results
            labels = []
            data = []
            
            for row in results:
                if hasattr(row, 'asset_class'):
                    # SQLAlchemy Row object
                    label = row.asset_class if row.asset_class else "Unclassified"
                    value = float(row.total_value)
                else:
                    # Tuple
                    label = row[0] if row[0] else "Unclassified"
                    value = float(row[1])
                
                labels.append(label)
                data.append(value)
            
            # Define a fixed set of colors for consistency
            colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B3", "#CCB974", "#64B5CD", "#E59C59", "#8C8C8C"]
            # Repeat colors if we have more categories than colors
            backgroundColor = [colors[i % len(colors)] for i in range(len(labels))]
            
            return jsonify({
                "labels": labels,
                "datasets": [{
                    "data": data,
                    "backgroundColor": backgroundColor,
                    "borderWidth": 1
                }]
            })
    except Exception as e:
        logger.error(f"Error retrieving allocation chart data: {str(e)}")
        # Return default values on error
        return jsonify({
            "labels": ["Equities", "Fixed Income", "Alternatives", "Cash"],
            "datasets": [{
                "data": [45.5, 30.0, 15.5, 9.0],
                "backgroundColor": ["#4C72B0", "#55A868", "#C44E52", "#8172B3"],
                "borderWidth": 1
            }]
        })

@app.route("/api/charts/liquidity", methods=["GET"])
def get_liquidity_chart_data():
    date = request.args.get('date', datetime.date.today().isoformat())
    level = request.args.get('level', 'client')
    level_key = request.args.get('level_key', 'All Clients')
    
    try:
        with get_db_connection() as db:
            # Create a connection to query the database
            if level == 'client':
                # For 'All Clients' or a specific client
                if level_key == 'All Clients':
                    # Query all liquidity categories and sum their values
                    query = text("""
                    SELECT liquid_vs_illiquid, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                    FROM financial_positions 
                    WHERE report_date = :date
                    GROUP BY liquid_vs_illiquid
                    """)
                    result = db.execute(query, {"date": date})
                else:
                    # For a specific client
                    query = text("""
                    SELECT liquid_vs_illiquid, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                    FROM financial_positions 
                    WHERE report_date = :date AND top_level_client = :client
                    GROUP BY liquid_vs_illiquid
                    """)
                    result = db.execute(query, {"date": date, "client": level_key})
            elif level == 'group':
                # For a specific group
                query = text("""
                SELECT liquid_vs_illiquid, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                FROM financial_positions 
                WHERE report_date = :date AND group_name = :group
                GROUP BY liquid_vs_illiquid
                """)
                result = db.execute(query, {"date": date, "group": level_key})
            elif level == 'portfolio':
                # For a specific portfolio
                query = text("""
                SELECT liquid_vs_illiquid, SUM(CAST(adjusted_value AS DECIMAL)) as total_value 
                FROM financial_positions 
                WHERE report_date = :date AND portfolio = :portfolio
                GROUP BY liquid_vs_illiquid
                """)
                result = db.execute(query, {"date": date, "portfolio": level_key})
            elif level == 'account':
                # For a specific account
                query = text("""
                SELECT liquid_vs_illiquid, SUM(adjusted_value) as total_value 
                FROM financial_positions 
                WHERE report_date = :date AND holding_account = :account
                GROUP BY liquid_vs_illiquid
                """)
                result = db.execute(query, {"date": date, "account": level_key})
            else:
                # Default to all data if level is not recognized
                query = text("""
                SELECT liquid_vs_illiquid, SUM(adjusted_value) as total_value 
                FROM financial_positions 
                WHERE report_date = :date
                GROUP BY liquid_vs_illiquid
                """)
                result = db.execute(query, {"date": date})
            
            # Fetch results
            results = result.fetchall()
            
            if not results:
                # If no data is found, return default values
                logger.warning(f"No data found for liquidity chart with date={date}, level={level}, level_key={level_key}")
                return jsonify({
                    "labels": ["Daily", "Weekly", "Monthly", "Quarterly", "Yearly"],
                    "datasets": [{
                        "data": [60.0, 15.0, 10.0, 10.0, 5.0],
                        "backgroundColor": ["#4C72B0", "#55A868", "#C44E52", "#8172B3", "#CCB974"],
                        "borderWidth": 1
                    }]
                })
            
            # Extract labels and data from query results
            labels = []
            data = []
            
            for row in results:
                if hasattr(row, 'liquid_vs_illiquid'):
                    # SQLAlchemy Row object
                    label = row.liquid_vs_illiquid if row.liquid_vs_illiquid else "Unclassified"
                    value = float(row.total_value)
                else:
                    # Tuple
                    label = row[0] if row[0] else "Unclassified"
                    value = float(row[1])
                
                labels.append(label)
                data.append(value)
            
            # Define a fixed set of colors for consistency
            colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B3", "#CCB974"]
            # Repeat colors if we have more categories than colors
            backgroundColor = [colors[i % len(colors)] for i in range(len(labels))]
            
            return jsonify({
                "labels": labels,
                "datasets": [{
                    "data": data,
                    "backgroundColor": backgroundColor,
                    "borderWidth": 1
                }]
            })
    except Exception as e:
        logger.error(f"Error retrieving liquidity chart data: {str(e)}")
        # Return default values on error
        return jsonify({
            "labels": ["Daily", "Weekly", "Monthly", "Quarterly", "Yearly"],
            "datasets": [{
                "data": [60.0, 15.0, 10.0, 10.0, 5.0],
                "backgroundColor": ["#4C72B0", "#55A868", "#C44E52", "#8172B3", "#CCB974"],
                "borderWidth": 1
            }]
        })

@app.route("/api/charts/performance", methods=["GET"])
def get_performance_chart_data():
    date = request.args.get('date', datetime.date.today().isoformat())
    level = request.args.get('level', 'portfolio')
    level_key = request.args.get('level_key', 'Portfolio 1')
    period = request.args.get('period', 'YTD')
    
    if period == 'YTD':
        labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        data = [1.2, 0.8, -0.5, 1.5, 2.0, 1.0, 1.8, 0.7, -1.0, 2.5, 1.0, 1.0]
    elif period == 'QTD':
        labels = ["Week 1", "Week 2", "Week 3", "Week 4", "Week 5", "Week 6", "Week 7", "Week 8", "Week 9", "Week 10", "Week 11", "Week 12", "Week 13"]
        data = [0.5, 0.3, -0.2, 0.8, 1.0, 0.5, 0.7, 0.3, -0.5, 1.0, 0.5, 0.4, 0.2]
    elif period == 'MTD':
        labels = ["Day 1", "Day 5", "Day 10", "Day 15", "Day 20", "Day 25", "Day 30"]
        data = [0.2, 0.1, -0.1, 0.3, 0.4, 0.2, 0.3]
    else:  # 1D
        labels = ["9:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "13:00", "13:30", "14:00", "14:30", "15:00", "15:30", "16:00"]
        data = [0.1, 0.2, 0.15, -0.1, -0.2, -0.1, 0.0, 0.2, 0.3, 0.25, 0.4, 0.3, 0.5, 0.6]
    
    return jsonify({
        "labels": labels,
        "datasets": [{
            "label": f"{period} Performance",
            "data": data,
            "borderColor": "#4C72B0",
            "backgroundColor": "rgba(76, 114, 176, 0.1)",
            "borderWidth": 2,
            "fill": True
        }]
    })

@app.route("/api/ownership-metadata", methods=["GET"])
def get_metadata_options():
    """
    Get available metadata options for ownership data
    Returns a list of metadata records with flags indicating which ones have proper classifications
    """
    try:
        with get_db_connection() as db:
            # Get all metadata records
            metadata_query = text("""
                SELECT m.id, m.view_name, m.date_range_start, m.date_range_end, m.is_current,
                       (SELECT COUNT(DISTINCT grouping_attribute_name) 
                        FROM ownership_items 
                        WHERE metadata_id = m.id 
                        AND grouping_attribute_name IN ('Client', 'Group', 'Holding Account')) AS classification_count
                FROM ownership_metadata m
                ORDER BY m.date_range_end DESC, m.id DESC
            """)
            
            metadata_records = db.execute(metadata_query).fetchall()
            
            result = []
            for record in metadata_records:
                # Check if this metadata has proper classification structure
                has_proper_classification = record.classification_count == 3
                
                result.append({
                    "id": record.id,
                    "view_name": record.view_name,
                    "date_range_start": record.date_range_start.isoformat() if record.date_range_start else None,
                    "date_range_end": record.date_range_end.isoformat() if record.date_range_end else None,
                    "is_current": record.is_current,
                    "has_proper_classification": has_proper_classification
                })
            
            return jsonify({
                "success": True,
                "data": result
            })
            
    except Exception as e:
        logger.error(f"Error getting metadata options: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Error getting metadata options: {str(e)}"
        }), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)