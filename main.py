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
    if not file.filename.endswith('.xlsx'):
        return jsonify({"success": False, "message": "Only Excel files (.xlsx) are supported"}), 400
    
    try:
        import pandas as pd
        from io import BytesIO
        import re
        import datetime
        from sqlalchemy import func
        from src.models.models import OwnershipMetadata, OwnershipItem
        from src.database import SessionLocal
        
        # Create database session
        db = SessionLocal()
        
        # Read the Excel file
        excel_data = BytesIO(file.read())
        
        # Extract metadata from first 3 rows
        metadata_df = pd.read_excel(excel_data, nrows=3, header=None)
        
        # Extract view name, date range, and portfolio coverage
        view_name = metadata_df.iloc[0, 1] if not pd.isna(metadata_df.iloc[0, 1]) else "NORI Ownership"
        
        # Parse date range
        date_range_str = metadata_df.iloc[1, 1] if not pd.isna(metadata_df.iloc[1, 1]) else ""
        date_range_match = re.search(r'(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})', date_range_str)
        
        if date_range_match:
            start_date_str, end_date_str = date_range_match.groups()
            start_date = datetime.datetime.strptime(start_date_str, '%m-%d-%Y').date()
            end_date = datetime.datetime.strptime(end_date_str, '%m-%d-%Y').date()
        else:
            # Default to today if date range can't be parsed
            start_date = end_date = datetime.date.today()
        
        portfolio_coverage = metadata_df.iloc[2, 1] if not pd.isna(metadata_df.iloc[2, 1]) else "All clients"
        
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
        db.flush()  # Flush to get the ID
        
        # Reset file pointer and read the data rows (from row 5 onwards)
        excel_data.seek(0)
        df = pd.read_excel(excel_data, header=3)  # Header is in row 4 (0-indexed)
        
        # Process each row
        rows_inserted = 0
        errors = []
        
        for index, row in df.iterrows():
            try:
                # Convert to native Python types and handle NaN values
                client = str(row['Client']) if not pd.isna(row['Client']) else ""
                entity_id = str(row['Entity ID']) if not pd.isna(row['Entity ID']) else None
                holding_account_number = str(row['Holding Account Number']) if not pd.isna(row['Holding Account Number']) else None
                portfolio = str(row['Portfolio']) if not pd.isna(row['Portfolio']) else None
                group_id = str(row['Group ID']) if not pd.isna(row['Group ID']) else None
                
                # Handle date format conversion
                data_inception_date = None
                if not pd.isna(row['Data Inception Date']):
                    try:
                        if isinstance(row['Data Inception Date'], datetime.datetime):
                            data_inception_date = row['Data Inception Date'].date()
                        elif isinstance(row['Data Inception Date'], str):
                            # Try different date formats
                            date_formats = ['%b %d, %Y', '%Y-%m-%d', '%m/%d/%Y']
                            for date_format in date_formats:
                                try:
                                    data_inception_date = datetime.datetime.strptime(row['Data Inception Date'], date_format).date()
                                    break
                                except ValueError:
                                    continue
                    except Exception as e:
                        logger.warning(f"Could not parse date: {row['Data Inception Date']} - {str(e)}")
                
                # Parse ownership percentage
                ownership_percentage = None
                if not pd.isna(row['% Ownership']):
                    try:
                        # Remove % sign if present and convert to float
                        ownership_str = str(row['% Ownership']).replace('%', '')
                        ownership_percentage = float(ownership_str) / 100 if ownership_str else None
                    except:
                        ownership_percentage = None
                
                grouping_attribute = str(row['Grouping Attribute Name']) if not pd.isna(row['Grouping Attribute Name']) else "Unknown"
                
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
                error_msg = f"Error processing row {index + 5}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        # Commit the transaction
        db.commit()
        
        return jsonify({
            "success": True,
            "message": "Ownership tree uploaded successfully",
            "rows_processed": len(df),
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
        from src.database import SessionLocal
        from collections import defaultdict
        
        # Create database session
        db = SessionLocal()
        
        # Get the most recent metadata
        latest_metadata = db.query(OwnershipMetadata).filter(OwnershipMetadata.is_current == True).first()
        
        if not latest_metadata:
            return jsonify({
                "success": False,
                "message": "No ownership data available. Please upload ownership data first."
            }), 404
        
        # Get all ownership items for the latest metadata
        items = db.query(OwnershipItem).filter(OwnershipItem.metadata_id == latest_metadata.id).all()
        
        if not items:
            return jsonify({
                "success": False,
                "message": "No ownership items found for the latest upload."
            }), 404
        
        # Build the tree structure
        # First, identify all unique clients
        clients = set()
        for item in items:
            if item.grouping_attribute_name == "Client":
                clients.add(item.client)
        
        # Now construct the tree
        tree = {
            "name": "ANTINORI Family Office",
            "children": []
        }
        
        # Process client by client
        for client_name in clients:
            client_node = {
                "name": client_name,
                "children": []
            }
            
            # Find all groups for this client
            client_items = [item for item in items if item.client.strip() == client_name.strip()]
            groups = set()
            for item in client_items:
                if item.grouping_attribute_name == "Group" and item.group_id:
                    groups.add(item.group_id)
            
            # Process each group
            for group_id in groups:
                group_name = next((item.client for item in client_items if item.group_id == group_id), f"Group {group_id}")
                group_node = {
                    "name": group_name,
                    "children": []
                }
                
                # Find all portfolios in this group
                group_items = [item for item in client_items if item.group_id == group_id]
                portfolios = set()
                for item in group_items:
                    if item.portfolio:
                        portfolios.add(item.portfolio)
                
                # Process each portfolio
                for portfolio_name in portfolios:
                    portfolio_node = {
                        "name": portfolio_name,
                        "children": []
                    }
                    
                    # Find all accounts in this portfolio
                    portfolio_items = [item for item in group_items 
                                      if item.portfolio == portfolio_name 
                                      and item.grouping_attribute_name == "Holding Account"]
                    
                    # Add each account
                    for account in portfolio_items:
                        # We don't have actual values here, so we'll use 1 as a placeholder
                        account_node = {
                            "name": account.client,
                            "value": 1
                        }
                        portfolio_node["children"].append(account_node)
                    
                    # Only add portfolio if it has accounts
                    if portfolio_node["children"]:
                        group_node["children"].append(portfolio_node)
                
                # Only add group if it has portfolios
                if group_node["children"]:
                    client_node["children"].append(group_node)
            
            # Find direct portfolios (not in groups)
            direct_portfolios = set()
            direct_items = [item for item in client_items if not item.group_id]
            for item in direct_items:
                if item.portfolio:
                    direct_portfolios.add(item.portfolio)
            
            # Process each direct portfolio
            for portfolio_name in direct_portfolios:
                portfolio_node = {
                    "name": portfolio_name,
                    "children": []
                }
                
                # Find all accounts in this portfolio
                portfolio_items = [item for item in direct_items 
                                  if item.portfolio == portfolio_name 
                                  and item.grouping_attribute_name == "Holding Account"]
                
                # Add each account
                for account in portfolio_items:
                    account_node = {
                        "name": account.client,
                        "value": 1
                    }
                    portfolio_node["children"].append(account_node)
                
                # Only add portfolio if it has accounts
                if portfolio_node["children"]:
                    client_node["children"].append(portfolio_node)
            
            # Only add client if it has children
            if client_node["children"]:
                tree["children"].append(client_node)
        
        return jsonify(tree)
        
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
