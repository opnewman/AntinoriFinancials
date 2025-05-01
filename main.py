import logging
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
import datetime

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
        from sqlalchemy import func
        from src.models.models import OwnershipMetadata, OwnershipItem
        from src.database import get_db_connection
        
        # Read the file content
        file_content = file.read()
        
        # Process the file based on its type
        if file_ext in ['.xlsx', '.xls']:
            # Excel file - use BytesIO
            excel_data = BytesIO(file_content)
            
            # Extract metadata from first 3 rows
            try:
                metadata_df = pd.read_excel(excel_data, nrows=3, header=None)
                
                # Extract view name, date range, and portfolio coverage
                view_name = metadata_df.iloc[0, 1] if len(metadata_df) > 0 and not pd.isna(metadata_df.iloc[0, 1]) else "NORI Ownership"
                
                # Parse date range
                date_range_str = metadata_df.iloc[1, 1] if len(metadata_df) > 1 and not pd.isna(metadata_df.iloc[1, 1]) else ""
                date_range_match = re.search(r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})', date_range_str)
                
                if date_range_match:
                    start_date_str, end_date_str = date_range_match.groups()
                    start_date = datetime.datetime.strptime(start_date_str, '%m-%d-%Y').date()
                    end_date = datetime.datetime.strptime(end_date_str, '%m-%d-%Y').date()
                else:
                    # Default to today if date range can't be parsed
                    start_date = end_date = datetime.date.today()
                
                portfolio_coverage = metadata_df.iloc[2, 1] if len(metadata_df) > 2 and not pd.isna(metadata_df.iloc[2, 1]) else "All clients"
                
                # Reset file pointer and read the data rows (from row 5 onwards)
                excel_data.seek(0)
                df = pd.read_excel(excel_data, header=3, dtype=str)  # Header is in row 4 (0-indexed), read all as strings
            
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
            # CSV or TXT file - use StringIO
            text_data = StringIO(file_content.decode('utf-8'))
            
            # Read first three lines for metadata
            header_lines = []
            for _ in range(3):
                if text_data.tell() < len(file_content):
                    header_lines.append(text_data.readline().strip())
            
            # Extract view name, date range, and portfolio coverage
            view_name = header_lines[0].split(':', 1)[1].strip() if len(header_lines) > 0 and ':' in header_lines[0] else "NORI Ownership"
            
            # Parse date range
            date_range_str = header_lines[1].split(':', 1)[1].strip() if len(header_lines) > 1 and ':' in header_lines[1] else ""
            date_range_match = re.search(r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})', date_range_str)
            
            if date_range_match:
                start_date_str, end_date_str = date_range_match.groups()
                start_date = datetime.datetime.strptime(start_date_str, '%m-%d-%Y').date()
                end_date = datetime.datetime.strptime(end_date_str, '%m-%d-%Y').date()
            else:
                # Default to today if date range can't be parsed
                start_date = end_date = datetime.date.today()
            
            portfolio_coverage = header_lines[2].split(':', 1)[1].strip() if len(header_lines) > 2 and ':' in header_lines[2] else "All clients"
            
            # Reset file pointer and read the data
            text_data.seek(0)
            
            # Skip the first 4 lines (3 metadata + 1 header)
            for _ in range(4):
                text_data.readline()
            
            # Read the rest of the file
            try:
                if file_ext == '.csv':
                    df = pd.read_csv(text_data, dtype=str)
                else:  # .txt, assuming tab-delimited
                    df = pd.read_csv(text_data, sep='\t', dtype=str)
            except Exception as e:
                logger.error(f"Error parsing text file: {str(e)}")
                return jsonify({
                    "success": False,
                    "message": f"Error parsing text file: {str(e)}",
                    "rows_processed": 0,
                    "rows_inserted": 0,
                    "errors": [str(e)]
                }), 400
        
        # Create, update, and retrieve metadata record
        new_metadata = None
        
        # Use our connection context manager
        with get_db_connection() as db:
            # Check if there are any existing metadata records
            metadata_count = db.query(OwnershipMetadata).count()
            if metadata_count > 0:
                # Set all existing metadata records to is_current=False
                db.query(OwnershipMetadata).update({"is_current": False})
            
            # Create new metadata record
            new_metadata = OwnershipMetadata(
                view_name=view_name,
                date_range_start=start_date,
                date_range_end=end_date,
                portfolio_coverage=portfolio_coverage,
                is_current=True
            )
            db.add(new_metadata)
            db.commit()
            
            # Get back the ID of the inserted record
            db.refresh(new_metadata)
        
        # Process all rows at once using pandas for better performance
        rows_inserted = 0
        errors = []
        total_rows = len(df)
        
        # Get the expected column names
        expected_columns = ['Client', 'Entity ID', 'Holding Account Number', 'Portfolio', 
                           'Group ID', 'Data Inception Date', '% Ownership', 'Grouping Attribute Name']
        
        # Verify and normalize column names if needed
        for col in expected_columns:
            if col not in df.columns:
                # Try to find a match by stripping spaces and lowercasing
                for actual_col in df.columns:
                    if actual_col.strip().lower() == col.lower():
                        df.rename(columns={actual_col: col}, inplace=True)
                        break
        
        # Filter out empty clients and total rows
        df = df[~df['Client'].isna() & ~df['Client'].str.lower().str.contains('total', na=False)]
        valid_rows = len(df)
        
        # Prepare all data at once using vectorized operations
        # Handle NaN values for string columns
        for col in ['Client', 'Entity ID', 'Holding Account Number', 'Portfolio', 'Group ID', 'Grouping Attribute Name']:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
                # Replace '-' with empty string only for certain columns
                if col in ['Entity ID', 'Holding Account Number', 'Group ID']:
                    df[col] = df[col].replace('-', '')
        
        # Parse dates more efficiently
        if 'Data Inception Date' in df.columns:
            # Convert to datetime with a common format or None
            df['parsed_date'] = None
            date_mask = ~df['Data Inception Date'].isna() & (df['Data Inception Date'] != '-') & (df['Data Inception Date'] != '')
            
            # Try different date formats in sequence
            date_formats = ['%b %d, %Y', '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
            
            for date_format in date_formats:
                # For rows where date is still None but has a value
                null_dates_mask = (df['parsed_date'].isna()) & date_mask
                if null_dates_mask.any():
                    try:
                        parsed_series = pd.to_datetime(
                            df.loc[null_dates_mask, 'Data Inception Date'], 
                            format=date_format, 
                            errors='coerce'
                        )
                        df.loc[null_dates_mask, 'parsed_date'] = parsed_series
                    except:
                        pass
        
        # Parse ownership percentages
        if '% Ownership' in df.columns:
            df['ownership_percentage'] = None
            pct_mask = ~df['% Ownership'].isna() & (df['% Ownership'] != '-') & (df['% Ownership'] != '')
            
            if pct_mask.any():
                # Remove % sign and convert to float
                df.loc[pct_mask, 'ownership_percentage'] = (
                    df.loc[pct_mask, '% Ownership']
                    .astype(str)
                    .str.replace('%', '')
                    .str.strip()
                    .astype(float) / 100
                )
        
        # Use a single connection and bulk insert for much better performance
        with get_db_connection() as db:
            try:
                # Create list of ownership items in one go
                ownership_items = []
                
                for _, row in df.iterrows():
                    try:
                        # Create ownership item object
                        ownership_item = OwnershipItem(
                            client=row['Client'],
                            entity_id=row['Entity ID'] if row['Entity ID'] else None,
                            holding_account_number=row['Holding Account Number'] if row['Holding Account Number'] else None,
                            portfolio=row['Portfolio'] if row['Portfolio'] else None,
                            group_id=row['Group ID'] if row['Group ID'] else None,
                            data_inception_date=row['parsed_date'] if 'parsed_date' in row and not pd.isna(row['parsed_date']) else None,
                            ownership_percentage=row['ownership_percentage'] if 'ownership_percentage' in row and not pd.isna(row['ownership_percentage']) else None,
                            grouping_attribute_name=row['Grouping Attribute Name'],
                            metadata_id=new_metadata.id
                        )
                        ownership_items.append(ownership_item)
                        rows_inserted += 1
                        
                    except Exception as e:
                        error_msg = f"Error processing row: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Bulk insert in a single operation
                if ownership_items:
                    db.bulk_save_objects(ownership_items)
                    db.commit()
                    
            except Exception as e:
                db.rollback()
                error_msg = f"Error during bulk insert: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
                raise
        
        return jsonify({
            "success": True,
            "message": "Ownership tree uploaded successfully",
            "rows_processed": total_rows,
            "rows_inserted": rows_inserted,
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
        from collections import defaultdict
        from sqlalchemy import func, distinct
        import time
        import hashlib
        
        # Performance timing
        start_time = time.time()
        
        # Check if force_refresh is set as a query parameter
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
        
        # Use our improved connection context manager to get metadata and check if we need to refresh cache
        with get_db_connection() as db:
            # Get the most recent metadata
            latest_metadata = db.query(OwnershipMetadata).filter(OwnershipMetadata.is_current == True).first()
            
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
            client_count = db.query(func.count(distinct(OwnershipItem.client))).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Client"
            ).scalar()
            
            total_records = db.query(func.count(OwnershipItem.id)).filter(
                OwnershipItem.metadata_id == latest_metadata.id
            ).scalar()
            
            # Get clients (top level) with a single query
            client_rows = db.query(
                OwnershipItem.client
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Client"
            ).distinct().all()
            
            clients = sorted([client.client for client in client_rows if client.client])
            
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
            
            # Get accounts for each portfolio - this is more complex and requires grouping_attribute_name
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
            
            # Get group names - groups are represented as clients with group_id
            group_names_query = db.query(
                OwnershipItem.group_id,
                OwnershipItem.client
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Group",
                OwnershipItem.group_id != None,
                OwnershipItem.group_id != ''
            ).all()
            
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
        
        # Outside the database connection, we have all our optimized data
        # Now build the tree very efficiently
        try:
            # Build the tree structure
            tree = {
                "name": "ANTINORI Family Office",
                "children": [],
                "metadata": metadata_info
            }
            
            # Process each client using our optimized data structures
            for client_name in clients:
                client_node = {
                    "name": client_name,
                    "children": []
                }
                
                # First add groups and their portfolios/accounts
                for group_id in client_to_groups.get(client_name, set()):
                    # Get the group name
                    group_name = group_id_to_name.get(group_id, f"Group {group_id}")
                    
                    group_node = {
                        "name": group_name,
                        "children": []
                    }
                    
                    # Add all portfolios for this group
                    for portfolio_name in group_to_portfolios.get(group_id, set()):
                        portfolio_node = {
                            "name": portfolio_name,
                            "children": []
                        }
                        
                        # Add all accounts for this portfolio in this group
                        account_key = (portfolio_name, group_id)
                        accounts = portfolio_to_accounts.get(account_key, [])
                        
                        # Add each account to the portfolio
                        for account in accounts:
                            portfolio_node["children"].append(account)
                        
                        # Only add portfolio if it has accounts
                        if portfolio_node["children"]:
                            group_node["children"].append(portfolio_node)
                    
                    # Only add group if it has portfolios
                    if group_node["children"]:
                        client_node["children"].append(group_node)
                
                # Next add direct portfolios (not in groups)
                for portfolio_name in client_to_direct_portfolios.get(client_name, set()):
                    portfolio_node = {
                        "name": portfolio_name,
                        "children": []
                    }
                    
                    # Add all accounts for this direct portfolio
                    account_key = (portfolio_name, "direct")
                    accounts = portfolio_to_accounts.get(account_key, [])
                    
                    # Add each account to the portfolio
                    for account in accounts:
                        portfolio_node["children"].append(account)
                    
                    # Only add portfolio if it has accounts
                    if portfolio_node["children"]:
                        client_node["children"].append(portfolio_node)
                
                # Only add client if it has children
                if client_node["children"]:
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

# Run the server
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
