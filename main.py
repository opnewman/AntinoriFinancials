import logging
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
import datetime
from sqlalchemy import text

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database module
from src.database import init_db, get_db

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

# API Endpoints
@app.route("/api/upload/data-dump", methods=["POST"])
def upload_data_dump():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file part in the request"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "message": "No file selected"}), 400
    
    return jsonify({
        "success": True,
        "message": "File uploaded successfully",
        "rows_processed": 0,
        "rows_inserted": 0,
        "errors": []
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
        import re
        import datetime
        import time
        from sqlalchemy import text, func
        from src.models.models import OwnershipMetadata, OwnershipItem
        from src.database import get_db_connection
        
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
        
        # Process metadata first - record the upload timing
        metadata_time = time.time() - start_time
        logger.info(f"Metadata processed in {metadata_time:.2f} seconds")
        
        # Now process the dataframe efficiently
        total_rows = len(df)
        errors = []
        
        # Normalize columns with a function instead of loops for better performance
        expected_columns = ['Client', 'Entity ID', 'Holding Account Number', 'Portfolio', 
                           'Group ID', 'Data Inception Date', '% Ownership', 'Grouping Attribute Name']
        
        # Map actual column names to expected ones
        column_mapping = {}
        for expected_col in expected_columns:
            if expected_col not in df.columns:
                # Find a case-insensitive match
                for actual_col in df.columns:
                    if actual_col.strip().lower() == expected_col.lower():
                        column_mapping[actual_col] = expected_col
                        break
        
        # Apply the column renaming if needed
        if column_mapping:
            df.rename(columns=column_mapping, inplace=True)
        
        # Pre-filter - remove empty clients and "Total" rows in one step
        client_col = 'Client'
        if client_col in df.columns:
            # Efficient filtering
            df = df[df[client_col].notna() & ~df[client_col].str.contains('total', case=False, na=False)]
        
        # Record valid rows after filtering
        valid_rows = len(df)
        logger.info(f"After filtering: {valid_rows} valid rows")
        
        # Efficiently clean up string columns in a single pass
        string_cols = ['Client', 'Entity ID', 'Holding Account Number', 'Portfolio', 
                      'Group ID', 'Grouping Attribute Name']
        
        # Handle string columns
        for col in string_cols:
            if col in df.columns:
                # Filter out NaN values
                df[col] = df[col].fillna('')
                # For certain columns, replace '-' with empty string
                if col in ['Entity ID', 'Holding Account Number', 'Group ID']:
                    df[col] = df[col].replace('-', '')
        
        # Process dates with optimal conversion
        if 'Data Inception Date' in df.columns:
            # Try direct conversion first (fastest)
            df['parsed_date'] = pd.to_datetime(df['Data Inception Date'], errors='coerce')
        
        # Process ownership percentages with optimal conversion
        if '% Ownership' in df.columns:
            # Fast one-step conversion
            df['ownership_percentage'] = pd.to_numeric(
                df['% Ownership'].astype(str).str.replace('%', '').str.strip(), 
                errors='coerce'
            ) / 100
        
        # Create metadata record
        metadata_id = None
        with get_db_connection() as db:
            try:
                # Mark existing metadata as not current
                db.query(OwnershipMetadata).filter(OwnershipMetadata.is_current == True).update({"is_current": False})
                db.flush()  # Ensure update is processed before the insert
                
                # Create new metadata
                new_metadata = OwnershipMetadata(
                    view_name=view_name,
                    date_range_start=start_date,
                    date_range_end=end_date,
                    portfolio_coverage=portfolio_coverage,
                    is_current=True
                )
                db.add(new_metadata)
                db.commit()
                db.refresh(new_metadata)
                metadata_id = new_metadata.id
                
                logger.info(f"Metadata created, id: {metadata_id}")
            except Exception as e:
                db.rollback()
                logger.error(f"Error creating metadata: {str(e)}")
                return jsonify({
                    "success": False,
                    "message": f"Error creating metadata: {str(e)}",
                    "rows_processed": 0,
                    "rows_inserted": 0,
                    "errors": [str(e)]
                }), 500
        
        # Process data rows with high-performance approach
        rows_inserted = 0
        if metadata_id is not None and valid_rows > 0:
            # Calculate optimal batch size based on data size
            batch_size = min(1000, max(100, valid_rows // 10))
            logger.info(f"Using batch size: {batch_size}")
            
            # Prepare data for insertion using bulk SQL
            records = []
            
            # Create connection for data insertion
            with get_db_connection() as db:
                try:
                    # Process rows efficiently
                    for i, row in enumerate(df.itertuples(index=False)):
                        try:
                            # Extract data safely
                            client = getattr(row, 'Client', '') or ''
                            
                            # Only process rows with valid client names
                            if client and client.lower() != 'total':
                                entity_id = getattr(row, 'Entity ID', None) or None
                                holding_account_number = getattr(row, 'Holding Account Number', None) or None
                                portfolio = getattr(row, 'Portfolio', None) or None
                                group_id = getattr(row, 'Group ID', None) or None
                                
                                # Handle date values more carefully
                                parsed_date = None
                                if hasattr(row, 'parsed_date'):
                                    date_val = getattr(row, 'parsed_date')
                                    if pd.notna(date_val) and date_val is not None:
                                        try:
                                            if isinstance(date_val, datetime.datetime):
                                                parsed_date = date_val.date()
                                            elif isinstance(date_val, str):
                                                parsed_date = datetime.datetime.strptime(date_val, '%Y-%m-%d').date()
                                        except (ValueError, TypeError):
                                            parsed_date = None
                                
                                # Handle ownership percentage with safety checks
                                ownership_pct = None
                                if hasattr(row, 'ownership_percentage'):
                                    pct_val = getattr(row, 'ownership_percentage')
                                    if pd.notna(pct_val) and pct_val is not None:
                                        try:
                                            ownership_pct = float(pct_val)
                                        except (ValueError, TypeError):
                                            ownership_pct = None
                                
                                # Get grouping attribute with fallback
                                try:
                                    grouping_attr = getattr(row, 'Grouping Attribute Name', 'Unknown')
                                    if not grouping_attr or pd.isna(grouping_attr):
                                        grouping_attr = 'Unknown'
                                except:
                                    grouping_attr = 'Unknown'
                                
                                # Clean and validate all fields before insertion
                                # Make sure we don't have any problematic values
                                if grouping_attr and isinstance(grouping_attr, str) and len(grouping_attr) <= 100:
                                    # Create dictionary for bulk insert with explicit type conversion
                                    record = {
                                        'client': str(client)[:100] if client else '',
                                        'entity_id': str(entity_id)[:50] if entity_id else None,
                                        'holding_account_number': str(holding_account_number)[:50] if holding_account_number else None,
                                        'portfolio': str(portfolio)[:100] if portfolio else None,
                                        'group_id': str(group_id)[:50] if group_id else None,
                                        'data_inception_date': parsed_date,
                                        'ownership_percentage': ownership_pct,
                                        'grouping_attribute_name': str(grouping_attr)[:50],
                                        'metadata_id': metadata_id,
                                        'upload_date': datetime.date.today()
                                    }
                                    
                                    records.append(record)
                                    rows_inserted += 1
                                    
                                    # Insert in batches with proper error handling
                                    if len(records) >= batch_size:
                                        try:
                                            # Use raw SQL insert for maximum speed
                                            db.execute(OwnershipItem.__table__.insert(), records)
                                            db.commit()
                                            logger.info(f"Inserted batch, total: {rows_inserted}")
                                            records = []  # Clear the batch
                                        except Exception as e:
                                            db.rollback()
                                            logger.error(f"Error during batch insert: {str(e)}")
                                            # Try inserting one by one to identify problematic records
                                            for idx, rec in enumerate(records):
                                                try:
                                                    db.execute(OwnershipItem.__table__.insert(), [rec])
                                                    db.commit()
                                                except Exception as e2:
                                                    logger.error(f"Problem with record {idx}: {str(e2)}")
                                            records = []  # Clear the batch after recovery attempt
                            
                        except Exception as e:
                            error_msg = f"Error processing row {i+5}: {str(e)}"
                            errors.append(error_msg)
                            logger.error(error_msg)
                    
                    # Insert any remaining records with proper error handling
                    if records:
                        try:
                            db.execute(OwnershipItem.__table__.insert(), records)
                            db.commit()
                            logger.info(f"Inserted final batch, total: {rows_inserted}")
                        except Exception as e:
                            db.rollback()
                            logger.error(f"Error during final batch insert: {str(e)}")
                            # Try inserting one by one to identify problematic records
                            for idx, rec in enumerate(records):
                                try:
                                    db.execute(OwnershipItem.__table__.insert(), [rec])
                                    db.commit()
                                except Exception as e2:
                                    logger.error(f"Problem with record {idx}: {str(e2)}")
                
                except Exception as e:
                    db.rollback()
                    error_msg = f"Error during batch insert: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    
                    # Return a proper JSON response even on error
                    return jsonify({
                        "success": False,
                        "message": f"Error during data insertion: {str(e)}",
                        "rows_processed": total_rows,
                        "rows_inserted": rows_inserted,
                        "errors": errors,
                        "processing_time_seconds": round(time.time() - start_time, 3)
                    }), 500
        
        # Calculate total processing time
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Return success response with timing information
        return jsonify({
            "success": True,
            "message": "Ownership tree uploaded successfully",
            "rows_processed": total_rows,
            "rows_inserted": rows_inserted,
            "processing_time_seconds": round(processing_time, 3),
            "errors": errors
        })
    
    except Exception as e:
        logger.error(f"Error uploading ownership file: {str(e)}")
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
    
    return jsonify({
        "success": True,
        "message": "Risk statistics uploaded successfully",
        "rows_processed": 0,
        "rows_inserted": 0,
        "errors": []
    })

# Cache for ownership tree data
ownership_tree_cache = {
    "tree": None,             # The full tree structure
    "metadata_id": None,      # The metadata ID this tree was built from
    "timestamp": None,        # When the cache was last updated
    "client_count": 0,
    "total_records": 0
}

@app.route("/api/ownership-tree", methods=["GET"])
def get_ownership_tree():
    try:
        from src.models.models import OwnershipMetadata, OwnershipItem
        from src.database import get_db_connection
        from collections import defaultdict, namedtuple
        from sqlalchemy import func, distinct
        import time
        import hashlib
        
        # Performance timing
        start_time = time.time()
        
        # Check if force_refresh is set as a query parameter
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
        
        # Use our improved connection context manager to get metadata and check if we need to refresh cache
        with get_db_connection() as db:
            # CRITICAL FIX: Ensure we have metadata with proper Client/Group/Holding Account classifications
            # First check which metadata has proper classifications
            classification_metadata = db.query(OwnershipMetadata).join(
                OwnershipItem, 
                OwnershipMetadata.id == OwnershipItem.metadata_id
            ).filter(
                OwnershipItem.grouping_attribute_name.in_(["Client", "Group", "Holding Account"])
            ).order_by(
                OwnershipMetadata.id.desc()
            ).first()
            
            if classification_metadata:
                logger.info(f"Using metadata ID {classification_metadata.id} which has proper Client/Group/Holding Account classifications")
                latest_metadata = classification_metadata
            else:
                # Fall back to the current metadata if no classification metadata is found
                latest_metadata = db.query(OwnershipMetadata).filter(OwnershipMetadata.is_current == True).first()
                logger.warning(f"No metadata with proper classifications found, using current metadata (ID: {latest_metadata.id if latest_metadata else None})")
            
            if not latest_metadata:
                return jsonify({
                    "success": False,
                    "message": "No ownership data available. Please upload ownership data first."
                }), 404
                
            # Check if we can use the cached tree
            cache_valid = (
                not force_refresh and
                ownership_tree_cache["tree"] is not None and
                ownership_tree_cache["metadata_id"] == latest_metadata.id
            )
            
            # If cache is valid, return it immediately
            if cache_valid:
                # Calculate time for cache hit
                end_time = time.time()
                processing_time = end_time - start_time
                
                # Return the cached tree with updated timestamps
                return jsonify({
                    "success": True, 
                    "data": ownership_tree_cache["tree"],
                    "client_count": ownership_tree_cache["client_count"],
                    "total_records": ownership_tree_cache["total_records"],
                    "processing_time_seconds": round(processing_time, 3),
                    "from_cache": True
                })
            
            # If we get here, we need to build the tree from scratch
            # Get metadata information
            metadata_info = {
                "view_name": latest_metadata.view_name,
                "date_range_start": latest_metadata.date_range_start.isoformat() if latest_metadata.date_range_start else None,
                "date_range_end": latest_metadata.date_range_end.isoformat() if latest_metadata.date_range_end else None,
                "portfolio_coverage": latest_metadata.portfolio_coverage,
                "upload_date": latest_metadata.upload_date.isoformat() if latest_metadata.upload_date else None
            }
            
            # Use database-level aggregation for faster data extraction
            # First, get the count of clients and total records for statistics
            # Check if we have any "Client" labeled entries
            client_with_label_count = db.query(func.count(distinct(OwnershipItem.client))).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Client"
            ).scalar()
            
            # If we found "Client" entries, use that count
            if client_with_label_count > 0:
                client_count = client_with_label_count
            # Otherwise, count all distinct clients
            else:
                client_count = db.query(func.count(distinct(OwnershipItem.client))).filter(
                    OwnershipItem.metadata_id == latest_metadata.id
                ).scalar()
                
            logger.info(f"Total client count: {client_count}")
            
            total_records = db.query(func.count(OwnershipItem.id)).filter(
                OwnershipItem.metadata_id == latest_metadata.id
            ).scalar()
            
            # Check if we have any entries with grouping_attribute_name = "Client"
            client_count_check = db.query(func.count()).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Client"
            ).scalar()
            
            # Initialize sets to store client identification info
            # These will be used later for filtering
            potential_true_clients = set()
            likely_accounts = set()
            
            # Create a namedtuple to match the format of our client query results
            ClientRow = namedtuple('ClientRow', ['client'])
            
            # If we have "Client" entries, use them - this is the primary method since
            # the grouping_attribute_name directly identifies clients
            if client_count_check > 0:
                logger.info(f"Found {client_count_check} entries with grouping_attribute_name = 'Client'")
                client_rows = db.query(
                    OwnershipItem.client,
                    OwnershipItem.portfolio
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.grouping_attribute_name == "Client"
                ).distinct().all()
                
                # Build a map of client names to portfolio names (which may also be client names)
                client_portfolio_map = {}
                
                # Add these to our potential_true_clients set
                for row in client_rows:
                    if row.client:
                        potential_true_clients.add(row.client)
                        # Store the portfolio associated with this client if available
                        if row.portfolio:
                            client_portfolio_map[row.client] = row.portfolio
                
                # Convert to our client row format
                client_rows = [ClientRow(client=client) for client in potential_true_clients]
            # If no "Client" entries, we need an advanced approach to identify true clients
            else:
                logger.info("No 'Client' grouping attribute found, using advanced identification logic")
                
                # STEP 1: Build a full relationship map of the data
                # This will help us identify the true hierarchy
                
                # Get all unique portfolios first
                all_portfolios = db.query(
                    distinct(OwnershipItem.portfolio)
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.portfolio != None,
                    OwnershipItem.portfolio != ''
                ).all()
                
                # Map to track clients that appear as portfolio names
                portfolio_to_clients = defaultdict(set)
                client_to_portfolios = defaultdict(set)
                
                # Get mappings between clients and portfolios
                client_portfolio_mapping = db.query(
                    OwnershipItem.client,
                    OwnershipItem.portfolio
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.portfolio != None,
                    OwnershipItem.portfolio != ''
                ).all()
                
                for mapping in client_portfolio_mapping:
                    if mapping.client and mapping.portfolio:
                        portfolio_to_clients[mapping.portfolio].add(mapping.client)
                        client_to_portfolios[mapping.client].add(mapping.portfolio)
                
                # STEP 2: Identify leaf nodes (likely accounts)
                # Accounts typically have holding_account_number, entity_id, and appear as leaf nodes
                likely_accounts = set()
                
                account_candidates = db.query(
                    OwnershipItem.client
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.holding_account_number != None,
                    OwnershipItem.holding_account_number != ''
                ).distinct().all()
                
                for candidate in account_candidates:
                    if candidate.client:
                        likely_accounts.add(candidate.client)
                
                # STEP 3: Identify portfolio-level entities
                # These are entries that have the same name as a portfolio, indicating they're parent-level
                portfolio_names = [p[0] for p in all_portfolios if p[0]]
                
                # STEP 4: Identify potential true clients
                # A true client is any entity that:
                # - Has entity_id but no holding_account_number, OR
                # - Is referenced as a portfolio by other entities, OR
                # - Has entries with group_id referencing it
                
                potential_true_clients = set()
                
                # Entities with entity_id but no holding_account_number
                entity_id_clients = db.query(
                    OwnershipItem.client
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.entity_id != None,
                    OwnershipItem.entity_id != '',
                    (OwnershipItem.holding_account_number == None) | (OwnershipItem.holding_account_number == '')
                ).distinct().all()
                
                for client in entity_id_clients:
                    if client.client:
                        potential_true_clients.add(client.client)
                
                # Add clients that match portfolio names or have group_id references
                for portfolio in portfolio_names:
                    if portfolio in client_to_portfolios:
                        potential_true_clients.add(portfolio)
                
                # Get group references
                group_references = db.query(
                    OwnershipItem.client
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.group_id != None,
                    OwnershipItem.group_id != ''
                ).distinct().all()
                
                for ref in group_references:
                    if ref.client:
                        potential_true_clients.add(ref.client)
                
                # STEP 5: Apply hierarchical filtering
                # Remove likely_accounts from potential_true_clients if they're in both sets
                true_clients = potential_true_clients - likely_accounts
                
                if true_clients:
                    logger.info(f"Identified {len(true_clients)} true clients using advanced hierarchical logic")
                    # Create client rows from the identified true clients
                    client_rows = [ClientRow(client=client) for client in true_clients]
                else:
                    # As a fallback, get clients that don't have holding account numbers
                    logger.info("No clear clients identified, falling back to non-account entities")
                    client_rows = db.query(
                        OwnershipItem.client
                    ).filter(
                        OwnershipItem.metadata_id == latest_metadata.id,
                        (OwnershipItem.holding_account_number == None) | (OwnershipItem.holding_account_number == '')
                    ).distinct().all()
                    
                # If we still don't have any clients, take a small subset as a last resort
                if len(client_rows) == 0:
                    logger.info("No clients identified with advanced logic, falling back to top 20 distinct clients")
                    client_rows = db.query(
                        OwnershipItem.client
                    ).filter(
                        OwnershipItem.metadata_id == latest_metadata.id
                    ).distinct().limit(20).all()
            
            # Sort and limit to first 100 clients for initial testing 
            # (to avoid overwhelming the browser)
            clients = sorted([client.client for client in client_rows if client.client])[:100]
            logger.info(f"Found {len(clients)} distinct clients (showing first 100)")
            
            # Use a more optimized query with joins to get all the data at once
            # First, get client to group relationships
            client_group_query = db.query(
                OwnershipItem.client,
                OwnershipItem.group_id
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.group_id != None,  # Only get items with a group_id
                OwnershipItem.group_id != ''     # Ensure group_id is not empty
            ).distinct().all()
            
            # Build a mapping of clients to their groups
            client_to_groups = defaultdict(set)
            for row in client_group_query:
                if row.client and row.group_id:
                    client_to_groups[row.client].add(row.group_id)
            
            # Get group to portfolio relationships
            group_portfolio_query = db.query(
                OwnershipItem.group_id,
                OwnershipItem.portfolio
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.group_id != None,  # Only get items with a group_id
                OwnershipItem.group_id != '',    # Ensure group_id is not empty
                OwnershipItem.portfolio != None, # Only get items with a portfolio
                OwnershipItem.portfolio != ''    # Ensure portfolio is not empty
            ).distinct().all()
            
            # Build a mapping of groups to their portfolios
            group_to_portfolios = defaultdict(set)
            for row in group_portfolio_query:
                if row.group_id and row.portfolio:
                    group_to_portfolios[row.group_id].add(row.portfolio)
            
            # Check if we have any "Holding Account" entries
            has_holding_accounts = db.query(func.count()).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Holding Account"
            ).scalar() > 0
            
            # Get accounts for each portfolio - this is more complex
            if has_holding_accounts:
                logger.info("Using 'Holding Account' grouping attribute to identify accounts")
                portfolio_accounts_query = db.query(
                    OwnershipItem.portfolio,
                    OwnershipItem.group_id,
                    OwnershipItem.client,
                    OwnershipItem.entity_id,
                    OwnershipItem.holding_account_number
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.grouping_attribute_name == "Holding Account",
                    OwnershipItem.portfolio != None,
                    OwnershipItem.portfolio != ''
                ).all()
            else:
                # If no "Holding Account" entries, try to use holding_account_number to identify accounts
                logger.info("No 'Holding Account' entries found, using holding_account_number to identify accounts")
                portfolio_accounts_query = db.query(
                    OwnershipItem.portfolio,
                    OwnershipItem.group_id,
                    OwnershipItem.client,
                    OwnershipItem.entity_id,
                    OwnershipItem.holding_account_number
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.holding_account_number.isnot(None),
                    OwnershipItem.holding_account_number != '',
                    OwnershipItem.portfolio != None,
                    OwnershipItem.portfolio != ''
                ).all()
            
            # Build mappings for portfolio to accounts
            portfolio_to_accounts = defaultdict(list)
            for row in portfolio_accounts_query:
                if row.portfolio:
                    # Create an account object
                    account = {
                        "name": row.client,
                        "value": 1,
                        "entity_id": row.entity_id,
                        "account_number": row.holding_account_number
                    }
                    # Key includes both portfolio and group_id (or 'direct')
                    key = (row.portfolio, row.group_id or "direct")
                    portfolio_to_accounts[key].append(account)
            
            # Check if we have any "Group" labeled entries
            has_group_labels = db.query(func.count()).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Group"
            ).scalar() > 0
            
            # Get group names - groups are represented as clients with group_id
            if has_group_labels:
                logger.info("Using 'Group' grouping attribute to identify groups")
                group_names_query = db.query(
                    OwnershipItem.group_id,
                    OwnershipItem.client
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.grouping_attribute_name == "Group",
                    OwnershipItem.group_id != None,
                    OwnershipItem.group_id != ''
                ).all()
            else:
                # If no "Group" entries, use any entry with a group_id
                logger.info("No 'Group' entries found, using any entries with group_id")
                group_names_query = db.query(
                    OwnershipItem.group_id,
                    OwnershipItem.client
                ).filter(
                    OwnershipItem.metadata_id == latest_metadata.id,
                    OwnershipItem.group_id != None,
                    OwnershipItem.group_id != ''
                ).distinct().all()
            
            # Map group IDs to names
            group_id_to_name = {}
            for row in group_names_query:
                if row.group_id:
                    group_id_to_name[row.group_id] = row.client
                    
            # Get direct portfolios (not in groups)
            direct_portfolios_query = db.query(
                OwnershipItem.client,
                OwnershipItem.portfolio
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                (OwnershipItem.group_id == None) | (OwnershipItem.group_id == ''),
                OwnershipItem.portfolio != None,
                OwnershipItem.portfolio != ''
            ).distinct().all()
            
            # Map clients to their direct portfolios
            client_to_direct_portfolios = defaultdict(set)
            for row in direct_portfolios_query:
                if row.client and row.portfolio:
                    client_to_direct_portfolios[row.client].add(row.portfolio)
        
        # Get additional client-portfolio relationships based on account data
        # In many cases, the portfolio field contains the true client name
        # This is essential for building a proper hierarchical structure
        portfolio_client_mapping = defaultdict(set)
        account_portfolio_mapping = defaultdict(str)
        
        # Query all portfolio-account relationships
        portfolio_account_rel = db.query(
            OwnershipItem.portfolio,
            OwnershipItem.client,
            OwnershipItem.holding_account_number
        ).filter(
            OwnershipItem.metadata_id == latest_metadata.id,
            OwnershipItem.portfolio != None,
            OwnershipItem.portfolio != '',
            OwnershipItem.holding_account_number != None,
            OwnershipItem.holding_account_number != ''
        ).all()
        
        # Build mappings - many "client" entries with account numbers are actually accounts
        # belonging to portfolios, which are the true clients
        for rel in portfolio_account_rel:
            if rel.portfolio and rel.client and rel.holding_account_number:
                portfolio_client_mapping[rel.portfolio].add(rel.client)
                account_portfolio_mapping[rel.client] = rel.portfolio
                
        # Outside the database connection, we have all our optimized data
        # Now build the tree very efficiently
        try:
            # Build the tree structure
            tree = {
                "name": "ANTINORI Family Office",
                "children": [],
                "metadata": metadata_info
            }
            
            # CRITICAL: Follow the exact grouping_attribute_name structure
            # This is the backbone of how the reports are built
            # Hierarchy: Client -> Group -> Holding Account
            
            # Step 1: Get all entities in their ORIGINAL ROW ORDER from the Excel file
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
                # This ensures we maintain the Excel file's original sequence
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
            
            # Step 2: Build the tree following the Excel file ordering hierarchy
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
            logger.error(f"Error building ownership tree: {str(e)}")
            return jsonify({
                "success": False,
                "message": f"Error building ownership tree: {str(e)}"
            }), 500
                
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
    level = request.args.get('level', 'portfolio')
    level_key = request.args.get('level_key', 'Portfolio 1')
    
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
    level = request.args.get('level', 'portfolio')
    level_key = request.args.get('level_key', 'Portfolio 1')
    
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
        data = [0.1, 0.2, 0.1, -0.1, -0.2, 0.0, 0.1, 0.2, 0.3, 0.1, 0.0, 0.2, 0.4, 0.5]
    
    return jsonify({
        "labels": labels,
        "datasets": [{
            "label": "Performance (%)",
            "data": data,
            "borderColor": "#4C72B0",
            "backgroundColor": "rgba(76, 114, 176, 0.1)",
            "fill": True,
            "tension": 0.4
        }]
    })
    
# Endpoints for new ownership relationship explorer

@app.route("/api/metadata-options", methods=["GET"])
def get_metadata_options():
    """
    Get available metadata options for ownership data
    Returns a list of metadata records with flags indicating which ones have proper classifications
    """
    try:
        from src.models.models import OwnershipMetadata, OwnershipItem
        from src.database import get_db_connection
        from sqlalchemy import func, distinct
        
        with get_db_connection() as db:
            # Get all metadata records
            metadata_records = db.query(OwnershipMetadata).order_by(
                OwnershipMetadata.id.desc()
            ).all()
            
            # Prepare the result
            options = []
            
            for metadata in metadata_records:
                # Check if this metadata has proper classifications
                has_classifications = db.query(func.count()).filter(
                    OwnershipItem.metadata_id == metadata.id,
                    OwnershipItem.grouping_attribute_name.in_(["Client", "Group", "Holding Account"])
                ).scalar() > 0
                
                options.append({
                    "id": metadata.id,
                    "view_name": metadata.view_name,
                    "date_range_start": metadata.date_range_start.isoformat() if metadata.date_range_start else None,
                    "date_range_end": metadata.date_range_end.isoformat() if metadata.date_range_end else None,
                    "portfolio_coverage": metadata.portfolio_coverage,
                    "upload_date": metadata.upload_date.isoformat() if metadata.upload_date else None,
                    "is_current": metadata.is_current,
                    "properClassification": has_classifications
                })
            
            return jsonify({
                "success": True,
                "options": options
            })
            
    except Exception as e:
        logger.error(f"Error fetching metadata options: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Error fetching metadata options: {str(e)}"
        }), 500

@app.route("/api/ownership-network", methods=["GET"])
def get_ownership_network():
    """
    Get ownership relationship network data for visualization
    This creates a network graph of clients, groups, and accounts
    """
    try:
        from src.models.models import OwnershipMetadata, OwnershipItem
        from src.database import get_db_connection
        from sqlalchemy import func, distinct
        import time
        
        # Start timing
        start_time = time.time()
        
        # Get filter parameters
        metadata_id = request.args.get('metadata_id')
        client_filter = request.args.get('client')
        
        # Entity type filters
        hide_clients = request.args.get('hide_clients', 'false').lower() == 'true'
        hide_groups = request.args.get('hide_groups', 'false').lower() == 'true'
        hide_accounts = request.args.get('hide_accounts', 'false').lower() == 'true'
        
        with get_db_connection() as db:
            # If no metadata_id specified, find one with proper classifications
            if not metadata_id:
                metadata_with_classifications = db.query(OwnershipMetadata).join(
                    OwnershipItem, 
                    OwnershipMetadata.id == OwnershipItem.metadata_id
                ).filter(
                    OwnershipItem.grouping_attribute_name.in_(["Client", "Group", "Holding Account"])
                ).order_by(
                    OwnershipMetadata.id.desc()
                ).first()
                
                if metadata_with_classifications:
                    logger.info(f"Using metadata ID {metadata_with_classifications.id} with proper classifications")
                    metadata_id = metadata_with_classifications.id
                else:
                    # Fall back to the latest metadata
                    latest_metadata = db.query(OwnershipMetadata).order_by(
                        OwnershipMetadata.id.desc()
                    ).first()
                    
                    if latest_metadata:
                        metadata_id = latest_metadata.id
                    else:
                        return jsonify({
                            "success": False,
                            "message": "No ownership data found"
                        }), 404
            
            # Convert to int if we got a string
            metadata_id = int(metadata_id)
            
            # Build query filters
            filters = [OwnershipItem.metadata_id == metadata_id]
            
            # Add entity type filters
            entity_type_filters = []
            if not hide_clients:
                entity_type_filters.append(OwnershipItem.grouping_attribute_name == "Client")
            if not hide_groups:
                entity_type_filters.append(OwnershipItem.grouping_attribute_name == "Group")
            if not hide_accounts:
                entity_type_filters.append(OwnershipItem.grouping_attribute_name == "Holding Account")
            
            # Add entity types to filters if any were selected
            if entity_type_filters:
                filters.append(OwnershipItem.grouping_attribute_name.in_(["Client", "Group", "Holding Account"]))
            
            # Add client filter if specified
            client_items = []
            if client_filter:
                # If specific client requested, get that client and its related entities
                client_items = db.query(OwnershipItem).filter(
                    OwnershipItem.metadata_id == metadata_id,
                    OwnershipItem.client == client_filter
                ).all()
                
                # Get portfolios associated with this client
                client_portfolios = set([item.portfolio for item in client_items if item.portfolio])
                
                # Add to filters to get related entities
                if client_portfolios:
                    filters.append(OwnershipItem.portfolio.in_(client_portfolios))
            
            # Get all relevant ownership items based on filters
            ownership_items = db.query(OwnershipItem).filter(*filters).all()
            
            if not ownership_items:
                return jsonify({
                    "success": True,
                    "data": {
                        "nodes": [],
                        "links": []
                    },
                    "processing_time_seconds": round(time.time() - start_time, 3)
                })
            
            # Build network data
            nodes = []
            links = []
            node_ids = set()  # To avoid duplicates
            
            # Helper function to add node if not already added
            def add_node(id, name, type, portfolio=None, entity_id=None, account_number=None):
                if id not in node_ids:
                    node_ids.add(id)
                    node = {
                        "id": id,
                        "name": name,
                        "type": type
                    }
                    if portfolio:
                        node["portfolio"] = portfolio
                    if entity_id:
                        node["entity_id"] = entity_id
                    if account_number:
                        node["account_number"] = account_number
                    nodes.append(node)
                    return True
                return False
            
            # First, add all entities as nodes
            for item in ownership_items:
                # Node ID is combination of name and type to avoid collisions
                node_id = f"{item.client}_{item.grouping_attribute_name}"
                
                # Only add node if it passes the filters
                should_add = True
                if item.grouping_attribute_name == "Client" and hide_clients:
                    should_add = False
                elif item.grouping_attribute_name == "Group" and hide_groups:
                    should_add = False
                elif item.grouping_attribute_name == "Holding Account" and hide_accounts:
                    should_add = False
                
                if should_add:
                    add_node(
                        id=node_id,
                        name=item.client,
                        type=item.grouping_attribute_name,
                        portfolio=item.portfolio,
                        entity_id=item.entity_id,
                        account_number=item.holding_account_number
                    )
            
            # Now add links based on portfolio relationships
            portfolio_to_nodes = {}
            
            # First, collect nodes by portfolio
            for item in ownership_items:
                if item.portfolio:
                    if item.portfolio not in portfolio_to_nodes:
                        portfolio_to_nodes[item.portfolio] = []
                    
                    node_id = f"{item.client}_{item.grouping_attribute_name}"
                    if node_id in node_ids:  # Only add if node exists
                        portfolio_to_nodes[item.portfolio].append({
                            "id": node_id,
                            "type": item.grouping_attribute_name
                        })
            
            # Then create links between nodes in the same portfolio
            # Hierarchy: Client -> Group -> Account
            for portfolio, portfolio_nodes in portfolio_to_nodes.items():
                # Get clients, groups, and accounts in this portfolio
                clients = [n for n in portfolio_nodes if n["type"] == "Client"]
                groups = [n for n in portfolio_nodes if n["type"] == "Group"]
                accounts = [n for n in portfolio_nodes if n["type"] == "Holding Account"]
                
                # Link clients to groups
                for client in clients:
                    for group in groups:
                        links.append({
                            "source": client["id"],
                            "target": group["id"],
                            "value": 1
                        })
                
                # Link groups to accounts
                for group in groups:
                    for account in accounts:
                        links.append({
                            "source": group["id"],
                            "target": account["id"],
                            "value": 1
                        })
                
                # If no groups, link clients directly to accounts
                if not groups and clients:
                    for client in clients:
                        for account in accounts:
                            links.append({
                                "source": client["id"],
                                "target": account["id"],
                                "value": 1
                            })
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            return jsonify({
                "success": True,
                "data": {
                    "nodes": nodes,
                    "links": links
                },
                "processing_time_seconds": round(processing_time, 3)
            })
    
    except Exception as e:
        logger.error(f"Error generating ownership network: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Error generating ownership network: {str(e)}"
        }), 500

# Run the server
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
