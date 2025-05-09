@app.route("/api/risk-stats/update", methods=["GET", "POST"])
def update_risk_stats():
    """
    Update risk statistics from Egnyte.
    
    This endpoint launches a background process to fetch the risk statistics file 
    from Egnyte and process it, avoiding web server timeouts.
    
    It requires an Egnyte API token to be set in the environment variables.
    
    Query parameters:
    - debug: If set to 'true', enables extended debugging output
    - use_test_file: If set to 'true', attempts to use a local test file if available
    - batch_size: Size of batches for database operations (default: 100)
    
    Returns:
        JSON with the status and job information
    """
    try:
        # Check query parameters
        debug_mode = request.args.get('debug', 'false').lower() == 'true'
        use_test_file = request.args.get('use_test_file', 'false').lower() == 'true'
        
        # Process batch size parameter
        try:
            batch_size = int(request.args.get('batch_size', '100'))
            if batch_size < 10 or batch_size > 1000:
                batch_size = 100  # Reset to default if out of reasonable range
        except ValueError:
            batch_size = 100
            
        # Log the request mode
        if debug_mode:
            logger.info("Running risk stats update in DEBUG mode with extended logging")
        if use_test_file:
            logger.info("Attempting to use local test file if available")
        
        # Check for required environment variables
        egnyte_token = os.environ.get('EGNYTE_ACCESS_TOKEN')
        egnyte_domain = os.environ.get('EGNYTE_DOMAIN')
        egnyte_path = os.environ.get('EGNYTE_RISK_STATS_PATH')
        
        # Log configuration details in debug mode
        if debug_mode:
            logger.info(f"Egnyte configuration: Domain={egnyte_domain or 'default'}, Path={egnyte_path or 'default'}")
            logger.info(f"Token available: {'Yes' if egnyte_token else 'No'}")
        
        if not egnyte_token and not use_test_file:
            logger.error("EGNYTE_ACCESS_TOKEN not found in environment variables")
            return jsonify({
                "success": False,
                "error": "Egnyte API token not configured. Please set the EGNYTE_ACCESS_TOKEN environment variable.",
                "debug_info": "Missing required environment variable EGNYTE_ACCESS_TOKEN"
            }), 400
        
        # Construct command for the background process
        cmd_args = ['python', 'update_risk_stats.py']
        if debug_mode:
            cmd_args.append('--debug')
        if use_test_file:
            cmd_args.append('--test')
        cmd_args.append(f'--batch-size={batch_size}')
        cmd_args.append(f'--output=risk_stats_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        # Log the command
        logger.info(f"Launching background process: {' '.join(cmd_args)}")
        
        # Start the background process
        from subprocess import Popen
        process = Popen(cmd_args)
        
        # Return success response immediately, process will continue in background
        logger.info(f"Background process started with PID {process.pid}")
        
        # Check current database state for the response
        with get_db_connection() as db:
            try:
                # Count current records in the database
                from src.models.models import EgnyteRiskStat
                stats_count = db.query(EgnyteRiskStat).count()
                
                # Count by asset class
                equity_count = db.query(EgnyteRiskStat).filter_by(asset_class='Equity').count()
                fi_count = db.query(EgnyteRiskStat).filter_by(asset_class='Fixed Income').count()
                alt_count = db.query(EgnyteRiskStat).filter_by(asset_class='Alternatives').count()
                
                # Get the most recent import date
                from sqlalchemy import func
                latest_date_query = db.query(func.max(EgnyteRiskStat.import_date))
                latest_date = latest_date_query.scalar()
                
                current_stats = {
                    "total": stats_count,
                    "equity": equity_count,
                    "fixed_income": fi_count,
                    "alternatives": alt_count,
                    "latest_date": latest_date.isoformat() if latest_date else None
                }
            except Exception as db_error:
                logger.warning(f"Could not retrieve current database stats: {db_error}")
                current_stats = {"error": str(db_error)}
        
        return jsonify({
            "success": True,
            "message": "Risk statistics update started in background process",
            "process_id": process.pid,
            "current_stats": current_stats,
            "status_endpoint": "/api/risk-stats/status"
        })
    
    except Exception as e:
        # Log the full exception for server-side debugging
        logger.exception("Error starting risk statistics update process:")
        
        # Return a user-friendly error message
        return jsonify({
            "success": False,
            "error": "An unexpected error occurred when starting risk statistics update.",
            "debug_info": str(e)
        }), 500