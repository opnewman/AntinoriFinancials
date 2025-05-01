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
        
        # Process each row in batches
        rows_inserted = 0
        errors = []
        batch_size = 100  # Process 100 rows at a time
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
        
        # Now process in batches
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = df.iloc[batch_start:batch_end]
            
            with get_db_connection() as db:
                for index, row in batch_df.iterrows():
                    try:
                        # Skip rows where Client is empty or "Total"
                        if pd.isna(row.get('Client', '')) or 'total' in str(row.get('Client', '')).lower():
                            continue
                            
                        # Convert to native Python types and handle NaN values
                        client = str(row.get('Client', '')) if not pd.isna(row.get('Client', '')) else ""
                        entity_id = str(row.get('Entity ID', '')) if not pd.isna(row.get('Entity ID', '')) else None
                        holding_account_number = str(row.get('Holding Account Number', '')) if not pd.isna(row.get('Holding Account Number', '')) else None
                        portfolio = str(row.get('Portfolio', '')) if not pd.isna(row.get('Portfolio', '')) else None
                        group_id = str(row.get('Group ID', '')) if not pd.isna(row.get('Group ID', '')) else None
                        
                        # Handle date format conversion
                        data_inception_date = None
                        date_value = row.get('Data Inception Date', '')
                        if not pd.isna(date_value) and date_value not in ['-', '']:
                            try:
                                if isinstance(date_value, datetime.datetime):
                                    data_inception_date = date_value.date()
                                elif isinstance(date_value, str):
                                    # Try different date formats
                                    date_formats = ['%b %d, %Y', '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
                                    for date_format in date_formats:
                                        try:
                                            data_inception_date = datetime.datetime.strptime(date_value, date_format).date()
                                            break
                                        except ValueError:
                                            continue
                            except Exception as e:
                                logger.warning(f"Could not parse date: {date_value} - {str(e)}")
                        
                        # Parse ownership percentage
                        ownership_percentage = None
                        pct_value = row.get('% Ownership', '')
                        if not pd.isna(pct_value) and pct_value not in ['-', '']:
                            try:
                                # Remove % sign if present and convert to float
                                ownership_str = str(pct_value).replace('%', '').strip()
                                ownership_percentage = float(ownership_str) / 100 if ownership_str else None
                            except:
                                ownership_percentage = None
                        
                        grouping_attribute = str(row.get('Grouping Attribute Name', '')) if not pd.isna(row.get('Grouping Attribute Name', '')) else "Unknown"
                        
                        # Create and add the ownership item
                        ownership_item = OwnershipItem(
                            client=client,
                            entity_id=entity_id,
                            holding_account_number=holding_account_number,
                            portfolio=portfolio,
                            group_id=group_id,
                            data_inception_date=data_inception_date,
                            ownership_percentage=ownership_percentage,
                            grouping_attribute_name=grouping_attribute,
                            metadata_id=new_metadata.id
                        )
                        db.add(ownership_item)
                        rows_inserted += 1
                        
                    except Exception as e:
                        error_msg = f"Error processing row {batch_start + index + 5}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Commit the batch
                db.commit()
        
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

@app.route("/api/ownership-tree", methods=["GET"])
def get_ownership_tree():
    try:
        from src.models.models import OwnershipMetadata, OwnershipItem
        from src.database import get_db_connection
        from collections import defaultdict
        from sqlalchemy import func, distinct
        import time
        
        # Performance timing
        start_time = time.time()
        
        # Use our improved connection context manager
        with get_db_connection() as db:
            # Get the most recent metadata
            latest_metadata = db.query(OwnershipMetadata).filter(OwnershipMetadata.is_current == True).first()
            
            if not latest_metadata:
                return jsonify({
                    "success": False,
                    "message": "No ownership data available. Please upload ownership data first."
                }), 404
            
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
            
            # Get clients (top level)
            clients = db.query(
                OwnershipItem.client
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id,
                OwnershipItem.grouping_attribute_name == "Client"
            ).distinct().all()
            
            clients = [client.client for client in clients if client.client]
            
            # Now build the tree structure efficiently - using a two-phase approach
            # Phase 1: Fetch all the data we need in structured format
            
            # Get all ownership items for the latest metadata - using a more targeted query
            # Only select the fields we need for the tree visualization and organize by level
            items_by_level = {
                "client_level": [],
                "group_level": [],
                "portfolio_level": [],
                "account_level": []
            }
            
            # Use a single query and sort the results in memory
            all_items = db.query(
                OwnershipItem.id, 
                OwnershipItem.client, 
                OwnershipItem.entity_id,
                OwnershipItem.holding_account_number,
                OwnershipItem.portfolio,
                OwnershipItem.group_id,
                OwnershipItem.grouping_attribute_name
            ).filter(
                OwnershipItem.metadata_id == latest_metadata.id
            ).all()
            
            # Organize items by levels for faster access
            for item in all_items:
                # Convert SQLAlchemy object to dict
                item_dict = {
                    "id": item.id,
                    "client": item.client,
                    "entity_id": item.entity_id,
                    "holding_account_number": item.holding_account_number,
                    "portfolio": item.portfolio,
                    "group_id": item.group_id,
                    "grouping_attribute_name": item.grouping_attribute_name
                }
                
                # Categorize by level
                if item.grouping_attribute_name == "Client":
                    items_by_level["client_level"].append(item_dict)
                elif item.grouping_attribute_name == "Group":
                    items_by_level["group_level"].append(item_dict)
                elif item.grouping_attribute_name == "Holding Account":
                    items_by_level["account_level"].append(item_dict)
                else:
                    # For any other categorization
                    items_by_level["portfolio_level"].append(item_dict)
        
        # We're now outside the database connection, but have all our data
        # Phase 2: Build the tree structure using the optimized data organization
        try:
            # Create lookup dictionaries for faster access
            # Map client names to their items
            client_items_map = defaultdict(list)
            for item in items_by_level["client_level"]:
                if item["client"]:
                    client_items_map[item["client"].strip()].append(item)
            
            # Map group IDs to their items
            group_items_map = defaultdict(list)
            for item in items_by_level["group_level"]:
                if item["group_id"]:
                    group_items_map[item["group_id"]].append(item)
            
            # Map portfolio names to account items
            portfolio_account_map = defaultdict(list)
            for item in items_by_level["account_level"]:
                if item["portfolio"]:
                    key = (item["portfolio"], item["group_id"] or "direct")
                    portfolio_account_map[key].append(item)
            
            # Build the tree
            tree = {
                "name": "ANTINORI Family Office",
                "children": [],
                "metadata": metadata_info
            }
            
            # Sort clients for consistent output
            clients.sort()
            
            # Process each client
            for client_name in clients:
                client_node = {
                    "name": client_name,
                    "children": []
                }
                
                # Find all items for this client (groups, portfolios, accounts)
                client_related_items = [item for item in all_items 
                                       if item.client and item.client.strip() == client_name.strip()]
                
                # Extract unique group IDs for this client
                groups = set()
                for item in client_related_items:
                    if item.grouping_attribute_name == "Group" and item.group_id:
                        groups.add(item.group_id)
                
                # Process each group
                for group_id in groups:
                    # Find group name
                    group_items = [item for item in client_related_items 
                                  if item.group_id == group_id]
                    
                    group_name = next((item.client for item in group_items 
                                     if item.grouping_attribute_name == "Group"), f"Group {group_id}")
                    
                    group_node = {
                        "name": group_name,
                        "children": []
                    }
                    
                    # Extract unique portfolios in this group
                    portfolios = set()
                    for item in group_items:
                        if item.portfolio:
                            portfolios.add(item.portfolio)
                    
                    # Process each portfolio in the group
                    for portfolio_name in portfolios:
                        portfolio_node = {
                            "name": portfolio_name,
                            "children": []
                        }
                        
                        # Find account items for this portfolio in this group
                        account_items = [item for item in group_items
                                        if item.portfolio == portfolio_name
                                        and item.grouping_attribute_name == "Holding Account"]
                        
                        # Add account nodes
                        for account in account_items:
                            account_node = {
                                "name": account.client,
                                "value": 1,
                                "entity_id": account.entity_id,
                                "account_number": account.holding_account_number
                            }
                            portfolio_node["children"].append(account_node)
                        
                        # Only add portfolio if it has accounts
                        if portfolio_node["children"]:
                            group_node["children"].append(portfolio_node)
                    
                    # Only add group if it has portfolios
                    if group_node["children"]:
                        client_node["children"].append(group_node)
                
                # Handle direct portfolios (not in groups)
                direct_items = [item for item in client_related_items if not item.group_id]
                direct_portfolios = set()
                for item in direct_items:
                    if item.portfolio:
                        direct_portfolios.add(item.portfolio)
                
                # Process each direct portfolio
                for portfolio_name in direct_portfolios:
                    portfolio_node = {
                        "name": portfolio_name,
                        "children": []
                    }
                    
                    # Find account items for this direct portfolio
                    direct_account_items = [item for item in direct_items
                                          if item.portfolio == portfolio_name
                                          and item.grouping_attribute_name == "Holding Account"]
                    
                    # Add account nodes
                    for account in direct_account_items:
                        account_node = {
                            "name": account.client,
                            "value": 1,
                            "entity_id": account.entity_id,
                            "account_number": account.holding_account_number
                        }
                        portfolio_node["children"].append(account_node)
                    
                    # Only add portfolio if it has accounts
                    if portfolio_node["children"]:
                        client_node["children"].append(portfolio_node)
                
                # Only add client if it has children
                if client_node["children"]:
                    tree["children"].append(client_node)
            
            # Calculate time
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Return the tree structure with additional metadata
            return jsonify({
                "success": True, 
                "data": tree,
                "client_count": client_count,
                "total_records": total_records,
                "processing_time_seconds": round(processing_time, 3)
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
