import os
import io
import logging
import pandas as pd
import openpyxl
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any
from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Depends, Form, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, case, cast, Float, and_, or_, select, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, validator
import traceback

# Import local modules
from src.database import get_db
from src.models.models import (
    FinancialPosition, OwnershipHierarchy, FinancialSummary,
    RiskStatisticEquity, RiskStatisticFixedIncome, RiskStatisticAlternatives
)
from src.utils.encryption import encryption_service

# Set up logging
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()

# Batch size for database operations
BATCH_SIZE = 10000

# ---- Models for request/response ----

class UploadResponse(BaseModel):
    success: bool
    message: str
    rows_processed: int = 0
    rows_inserted: int = 0
    errors: List[str] = []

class ChartData(BaseModel):
    labels: List[str]
    datasets: List[Dict[str, Any]]

class PerformanceData(BaseModel):
    period: str
    value: float
    percentage: float

class RiskData(BaseModel):
    metric: str
    value: float

class PortfolioReportResponse(BaseModel):
    report_date: str
    level: str
    level_key: str
    total_adjusted_value: float
    asset_allocation: Dict[str, float]
    liquidity: Dict[str, float]
    performance: List[PerformanceData]
    risk_metrics: List[RiskData]

# ---- Utility Functions ----

def clean_numeric_value(value):
    """Clean and convert a value to float"""
    if pd.isna(value) or value is None:
        return 0.0
    
    if isinstance(value, (int, float)):
        return float(value)
    
    # Remove currency symbols, commas, and spaces
    clean_value = str(value).replace('$', '').replace(',', '').replace(' ', '')
    try:
        return float(clean_value)
    except ValueError:
        return 0.0

def calculate_performance_change(current_value, previous_value):
    """Calculate percentage change between two values"""
    if previous_value == 0:
        return 0.0
    
    change = ((current_value - previous_value) / previous_value) * 100
    return round(change, 2)

def get_previous_date(current_date, period='1D'):
    """Get the previous date based on the period"""
    current_date = datetime.strptime(current_date, '%Y-%m-%d').date() if isinstance(current_date, str) else current_date
    
    if period == '1D':
        return current_date - timedelta(days=1)
    elif period == 'MTD':
        return date(current_date.year, current_date.month, 1)
    elif period == 'QTD':
        quarter_month = ((current_date.month - 1) // 3) * 3 + 1
        return date(current_date.year, quarter_month, 1)
    elif period == 'YTD':
        return date(current_date.year, 1, 1)
    else:
        return current_date - timedelta(days=1)

# ---- API Endpoints ----

@router.post("/upload/data_dump", response_model=UploadResponse)
async def upload_data_dump(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and process data_dump.xlsx file
    """
    try:
        if not file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="Only Excel (.xlsx) files are accepted")
        
        # Read the file content
        contents = await file.read()
        excel_data = io.BytesIO(contents)
        
        # Read Excel file - skip header rows (start from row 5)
        df = pd.read_excel(excel_data, engine='openpyxl', skiprows=4)
        
        # Validate required columns
        required_columns = [
            'position', 'top_level_client', 'holding_account', 'holding_account_number', 
            'portfolio', 'asset_class', 'liquid_vs_illiquid', 'adjusted_value'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400, 
                detail=f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        # Fill NaN values
        df = df.fillna('')
        
        # Prepare to track stats
        rows_processed = len(df)
        rows_inserted = 0
        errors = []
        
        # Get current date for report
        report_date = date.today()
        
        # Process data in batches
        for i in range(0, rows_processed, BATCH_SIZE):
            batch_df = df.iloc[i:i+BATCH_SIZE]
            positions = []
            
            for _, row in batch_df.iterrows():
                try:
                    # Clean the adjusted value
                    adjusted_value_clean = clean_numeric_value(row['adjusted_value'])
                    
                    # Encrypt the adjusted value
                    encrypted_value = encryption_service.encrypt(adjusted_value_clean)
                    
                    # Create financial position object
                    position = FinancialPosition(
                        position=row['position'],
                        top_level_client=row['top_level_client'],
                        holding_account=row['holding_account'],
                        holding_account_number=row['holding_account_number'],
                        portfolio=row['portfolio'],
                        cusip=row.get('cusip', ''),
                        ticker_symbol=row.get('ticker_symbol', ''),
                        asset_class=row['asset_class'],
                        second_level=row.get('second_level', ''),
                        third_level=row.get('third_level', ''),
                        adv_classification=row.get('adv_classification', ''),
                        liquid_vs_illiquid=row['liquid_vs_illiquid'],
                        adjusted_value=encrypted_value,
                        date=report_date
                    )
                    positions.append(position)
                    rows_inserted += 1
                    
                except Exception as e:
                    error_msg = f"Error processing row {i}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            # Bulk insert the batch
            db.bulk_save_objects(positions)
            db.commit()
        
        # Generate financial summary data
        await generate_financial_summary(db, report_date)
        
        return UploadResponse(
            success=True,
            message=f"Successfully processed data_dump.xlsx",
            rows_processed=rows_processed,
            rows_inserted=rows_inserted,
            errors=errors[:10]  # Limit number of errors returned
        )
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing data_dump.xlsx: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

async def generate_financial_summary(db: Session, report_date: date):
    """
    Generate financial summary data by aggregating financial positions
    """
    try:
        # Delete existing summary data for the report date
        db.query(FinancialSummary).filter(FinancialSummary.report_date == report_date).delete()
        
        # Get all positions for the report date
        positions = db.query(FinancialPosition).filter(FinancialPosition.date == report_date).all()
        
        # Prepare summary data at different levels
        summary_data = {}
        
        for position in positions:
            # Decrypt the adjusted value
            adjusted_value = encryption_service.decrypt_to_float(position.adjusted_value)
            
            # Aggregate by client
            client_key = f"client:{position.top_level_client}"
            if client_key not in summary_data:
                summary_data[client_key] = 0
            summary_data[client_key] += adjusted_value
            
            # Aggregate by portfolio
            portfolio_key = f"portfolio:{position.portfolio}"
            if portfolio_key not in summary_data:
                summary_data[portfolio_key] = 0
            summary_data[portfolio_key] += adjusted_value
            
            # Aggregate by account
            account_key = f"account:{position.holding_account_number}"
            if account_key not in summary_data:
                summary_data[account_key] = 0
            summary_data[account_key] += adjusted_value
            
            # Get ownership data to aggregate by group
            ownership = db.query(OwnershipHierarchy).filter(
                OwnershipHierarchy.holding_account_number == position.holding_account_number
            ).first()
            
            if ownership and ownership.groups:
                groups = ownership.groups.split(',')
                for group in groups:
                    group = group.strip()
                    if group:
                        group_key = f"group:{group}"
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

@router.post("/upload/ownership_tree", response_model=UploadResponse)
async def upload_ownership_tree(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and process ownership.xlsx file
    """
    try:
        if not file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="Only Excel (.xlsx) files are accepted")
        
        # Read the file content
        contents = await file.read()
        excel_data = io.BytesIO(contents)
        
        # Read Excel file - skip header rows (start from row 5)
        df = pd.read_excel(excel_data, engine='openpyxl', skiprows=4)
        
        # Validate required columns
        required_columns = [
            'holding_account', 'holding_account_number', 'top_level_client', 
            'entity_id', 'portfolio'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400, 
                detail=f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        # Fill NaN values
        df = df.fillna('')
        
        # Prepare to track stats
        rows_processed = len(df)
        rows_inserted = 0
        errors = []
        
        # Process data in batches
        for i in range(0, rows_processed, BATCH_SIZE):
            batch_df = df.iloc[i:i+BATCH_SIZE]
            ownerships = []
            
            for _, row in batch_df.iterrows():
                try:
                    # Create ownership hierarchy object
                    ownership = OwnershipHierarchy(
                        holding_account=row['holding_account'],
                        holding_account_number=row['holding_account_number'],
                        top_level_client=row['top_level_client'],
                        entity_id=row['entity_id'],
                        portfolio=row['portfolio'],
                        groups=row.get('groups', ''),
                        last_updated=date.today()
                    )
                    ownerships.append(ownership)
                    rows_inserted += 1
                    
                except Exception as e:
                    error_msg = f"Error processing row {i}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            # Bulk insert the batch
            db.bulk_save_objects(ownerships)
            db.commit()
        
        return UploadResponse(
            success=True,
            message=f"Successfully processed ownership.xlsx",
            rows_processed=rows_processed,
            rows_inserted=rows_inserted,
            errors=errors[:10]  # Limit number of errors returned
        )
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing ownership.xlsx: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@router.post("/upload/security_risk_stats", response_model=UploadResponse)
async def upload_security_risk_stats(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and process risk_stats.xlsx file
    """
    try:
        if not file.filename.endswith('.xlsx'):
            raise HTTPException(status_code=400, detail="Only Excel (.xlsx) files are accepted")
        
        # Read the file content
        contents = await file.read()
        excel_data = io.BytesIO(contents)
        
        # Read Excel file - need to read multiple sheets
        xls = pd.ExcelFile(excel_data, engine='openpyxl')
        
        # Check if required sheets exist
        required_sheets = ['Equity', 'Fixed Income', 'Alternatives']
        missing_sheets = [sheet for sheet in required_sheets if sheet not in xls.sheet_names]
        if missing_sheets:
            raise HTTPException(
                status_code=400, 
                detail=f"Missing required sheets: {', '.join(missing_sheets)}"
            )
        
        # Initialize counters
        total_rows_processed = 0
        total_rows_inserted = 0
        errors = []
        
        # Process Equity sheet
        equity_df = pd.read_excel(xls, 'Equity', skiprows=4)
        equity_required_cols = ['position', 'ticker_symbol', 'vol', 'beta']
        missing_cols = [col for col in equity_required_cols if col not in equity_df.columns]
        if missing_cols:
            errors.append(f"Equity sheet missing columns: {', '.join(missing_cols)}")
        else:
            equity_df = equity_df.fillna(0)
            equity_rows = len(equity_df)
            total_rows_processed += equity_rows
            
            # Process in batches
            for i in range(0, equity_rows, BATCH_SIZE):
                batch_df = equity_df.iloc[i:i+BATCH_SIZE]
                equity_stats = []
                
                for _, row in batch_df.iterrows():
                    try:
                        # Validate data exists in financial_positions
                        position_exists = db.query(FinancialPosition).filter(
                            or_(
                                FinancialPosition.position == row['position'],
                                FinancialPosition.ticker_symbol == row['ticker_symbol']
                            )
                        ).first()
                        
                        if position_exists:
                            equity_stat = RiskStatisticEquity(
                                position=row['position'],
                                ticker_symbol=row['ticker_symbol'],
                                vol=clean_numeric_value(row['vol']),
                                beta=clean_numeric_value(row['beta'])
                            )
                            equity_stats.append(equity_stat)
                            total_rows_inserted += 1
                        else:
                            errors.append(f"Position not found: {row['position']} / {row['ticker_symbol']}")
                            
                    except Exception as e:
                        error_msg = f"Error processing Equity row {i}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Bulk insert the batch
                db.bulk_save_objects(equity_stats)
                db.commit()
        
        # Process Fixed Income sheet
        fixed_income_df = pd.read_excel(xls, 'Fixed Income', skiprows=4)
        fi_required_cols = ['position', 'ticker_symbol', 'vol', 'duration']
        missing_cols = [col for col in fi_required_cols if col not in fixed_income_df.columns]
        if missing_cols:
            errors.append(f"Fixed Income sheet missing columns: {', '.join(missing_cols)}")
        else:
            fixed_income_df = fixed_income_df.fillna(0)
            fi_rows = len(fixed_income_df)
            total_rows_processed += fi_rows
            
            # Process in batches
            for i in range(0, fi_rows, BATCH_SIZE):
                batch_df = fixed_income_df.iloc[i:i+BATCH_SIZE]
                fi_stats = []
                
                for _, row in batch_df.iterrows():
                    try:
                        # Validate data exists in financial_positions
                        position_exists = db.query(FinancialPosition).filter(
                            or_(
                                FinancialPosition.position == row['position'],
                                FinancialPosition.ticker_symbol == row['ticker_symbol']
                            )
                        ).first()
                        
                        if position_exists:
                            fi_stat = RiskStatisticFixedIncome(
                                position=row['position'],
                                ticker_symbol=row['ticker_symbol'],
                                vol=clean_numeric_value(row['vol']),
                                duration=clean_numeric_value(row['duration'])
                            )
                            fi_stats.append(fi_stat)
                            total_rows_inserted += 1
                        else:
                            errors.append(f"Position not found: {row['position']} / {row['ticker_symbol']}")
                            
                    except Exception as e:
                        error_msg = f"Error processing Fixed Income row {i}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Bulk insert the batch
                db.bulk_save_objects(fi_stats)
                db.commit()
        
        # Process Alternatives sheet
        alt_df = pd.read_excel(xls, 'Alternatives', skiprows=4)
        alt_required_cols = ['position', 'ticker_symbol', 'vol', 'beta_to_gold']
        missing_cols = [col for col in alt_required_cols if col not in alt_df.columns]
        if missing_cols:
            errors.append(f"Alternatives sheet missing columns: {', '.join(missing_cols)}")
        else:
            alt_df = alt_df.fillna(0)
            alt_rows = len(alt_df)
            total_rows_processed += alt_rows
            
            # Process in batches
            for i in range(0, alt_rows, BATCH_SIZE):
                batch_df = alt_df.iloc[i:i+BATCH_SIZE]
                alt_stats = []
                
                for _, row in batch_df.iterrows():
                    try:
                        # Validate data exists in financial_positions
                        position_exists = db.query(FinancialPosition).filter(
                            or_(
                                FinancialPosition.position == row['position'],
                                FinancialPosition.ticker_symbol == row['ticker_symbol']
                            )
                        ).first()
                        
                        if position_exists:
                            alt_stat = RiskStatisticAlternatives(
                                position=row['position'],
                                ticker_symbol=row['ticker_symbol'],
                                vol=clean_numeric_value(row['vol']),
                                beta_to_gold=clean_numeric_value(row['beta_to_gold'])
                            )
                            alt_stats.append(alt_stat)
                            total_rows_inserted += 1
                        else:
                            errors.append(f"Position not found: {row['position']} / {row['ticker_symbol']}")
                            
                    except Exception as e:
                        error_msg = f"Error processing Alternatives row {i}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Bulk insert the batch
                db.bulk_save_objects(alt_stats)
                db.commit()
        
        return UploadResponse(
            success=True,
            message=f"Successfully processed risk_stats.xlsx",
            rows_processed=total_rows_processed,
            rows_inserted=total_rows_inserted,
            errors=errors[:10]  # Limit number of errors returned
        )
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing risk_stats.xlsx: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@router.get("/generate_portfolio_report", response_model=PortfolioReportResponse)
async def generate_portfolio_report(
    date: str = Query(..., description="Report date in YYYY-MM-DD format"),
    level: str = Query(..., description="Report level (client, group, portfolio, account, custom)"),
    level_key: str = Query(..., description="The key for the chosen level (e.g., client name, portfolio name)"),
    db: Session = Depends(get_db)
):
    """
    Generate a portfolio report for the specified date, level, and level key
    """
    try:
        # Parse date
        report_date = datetime.strptime(date, '%Y-%m-%d').date()
        
        # Validate level
        valid_levels = ['client', 'group', 'portfolio', 'account', 'custom']
        if level not in valid_levels:
            raise HTTPException(status_code=400, detail=f"Invalid level. Must be one of: {', '.join(valid_levels)}")
        
        # Get financial summary for the level and date
        summary = db.query(FinancialSummary).filter(
            FinancialSummary.level == level,
            FinancialSummary.level_key == level_key,
            FinancialSummary.report_date == report_date
        ).first()
        
        if not summary:
            raise HTTPException(status_code=404, detail=f"No data found for level '{level}', key '{level_key}' on date '{date}'")
        
        # Get total adjusted value
        total_adjusted_value = summary.total_adjusted_value
        
        # Get positions based on level and key
        if level == 'client':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.top_level_client == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'portfolio':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.portfolio == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'account':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.holding_account_number == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'group':
            # For group level, need to join with ownership_hierarchy
            account_numbers = db.query(OwnershipHierarchy.holding_account_number).filter(
                OwnershipHierarchy.groups.like(f"%{level_key}%")
            ).all()
            account_numbers = [acc[0] for acc in account_numbers]
            
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.holding_account_number.in_(account_numbers),
                FinancialPosition.date == report_date
            ).all()
        else:  # custom - assume custom is a comma-separated list of portfolio names
            custom_portfolios = level_key.split(',')
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.portfolio.in_(custom_portfolios),
                FinancialPosition.date == report_date
            ).all()
        
        if not positions:
            raise HTTPException(status_code=404, detail=f"No positions found for level '{level}', key '{level_key}' on date '{date}'")
        
        # Calculate asset allocation
        asset_allocation = {}
        for position in positions:
            adj_value = encryption_service.decrypt_to_float(position.adjusted_value)
            asset_class = position.asset_class
            
            if asset_class not in asset_allocation:
                asset_allocation[asset_class] = 0
            
            asset_allocation[asset_class] += adj_value
        
        # Convert to percentages
        for asset_class in asset_allocation:
            asset_allocation[asset_class] = round((asset_allocation[asset_class] / total_adjusted_value) * 100, 2)
        
        # Calculate liquidity
        liquidity = {'Liquid': 0, 'Illiquid': 0}
        for position in positions:
            adj_value = encryption_service.decrypt_to_float(position.adjusted_value)
            liquidity_type = position.liquid_vs_illiquid
            
            if liquidity_type in liquidity:
                liquidity[liquidity_type] += adj_value
            else:
                # Default to illiquid if not specified
                liquidity['Illiquid'] += adj_value
        
        # Convert to percentages
        for liq_type in liquidity:
            liquidity[liq_type] = round((liquidity[liq_type] / total_adjusted_value) * 100, 2)
        
        # Calculate performance metrics
        performance = []
        performance_periods = ['1D', 'MTD', 'QTD', 'YTD']
        
        for period in performance_periods:
            previous_date = get_previous_date(report_date, period)
            
            # Get previous summary
            previous_summary = db.query(FinancialSummary).filter(
                FinancialSummary.level == level,
                FinancialSummary.level_key == level_key,
                FinancialSummary.report_date == previous_date
            ).first()
            
            if previous_summary:
                previous_value = previous_summary.total_adjusted_value
                absolute_change = total_adjusted_value - previous_value
                percentage_change = calculate_performance_change(total_adjusted_value, previous_value)
                
                performance.append(PerformanceData(
                    period=period,
                    value=round(absolute_change, 2),
                    percentage=percentage_change
                ))
            else:
                # No previous data, assume new investment (100% growth)
                performance.append(PerformanceData(
                    period=period,
                    value=total_adjusted_value,
                    percentage=100.0
                ))
        
        # Calculate risk metrics
        risk_metrics = []
        
        # Weighted average volatility
        weighted_vol = 0
        weighted_beta = 0
        weighted_duration = 0
        weighted_beta_to_gold = 0
        
        equity_total = 0
        fixed_income_total = 0
        alternatives_total = 0
        
        for position in positions:
            adj_value = encryption_service.decrypt_to_float(position.adjusted_value)
            
            # Get risk statistics based on asset class
            if position.asset_class == 'Equity':
                equity_total += adj_value
                risk_equity = db.query(RiskStatisticEquity).filter(
                    RiskStatisticEquity.position == position.position
                ).first()
                
                if risk_equity:
                    weighted_vol += risk_equity.vol * adj_value
                    weighted_beta += risk_equity.beta * adj_value
            
            elif position.asset_class == 'Fixed Income':
                fixed_income_total += adj_value
                risk_fi = db.query(RiskStatisticFixedIncome).filter(
                    RiskStatisticFixedIncome.position == position.position
                ).first()
                
                if risk_fi:
                    weighted_vol += risk_fi.vol * adj_value
                    weighted_duration += risk_fi.duration * adj_value
            
            elif 'Alternative' in position.asset_class:
                alternatives_total += adj_value
                risk_alt = db.query(RiskStatisticAlternatives).filter(
                    RiskStatisticAlternatives.position == position.position
                ).first()
                
                if risk_alt:
                    weighted_vol += risk_alt.vol * adj_value
                    weighted_beta_to_gold += risk_alt.beta_to_gold * adj_value
        
        # Calculate final weighted metrics
        portfolio_vol = weighted_vol / total_adjusted_value if total_adjusted_value > 0 else 0
        risk_metrics.append(RiskData(metric="Portfolio Volatility", value=round(portfolio_vol, 4)))
        
        if equity_total > 0:
            equity_beta = weighted_beta / equity_total
            risk_metrics.append(RiskData(metric="Equity Beta", value=round(equity_beta, 4)))
        
        if fixed_income_total > 0:
            fi_duration = weighted_duration / fixed_income_total
            risk_metrics.append(RiskData(metric="Fixed Income Duration", value=round(fi_duration, 4)))
        
        if alternatives_total > 0:
            alt_beta_to_gold = weighted_beta_to_gold / alternatives_total
            risk_metrics.append(RiskData(metric="Alternatives Beta to Gold", value=round(alt_beta_to_gold, 4)))
        
        # Format the response
        return PortfolioReportResponse(
            report_date=date,
            level=level,
            level_key=level_key,
            total_adjusted_value=round(total_adjusted_value, 2),
            asset_allocation=asset_allocation,
            liquidity=liquidity,
            performance=performance,
            risk_metrics=risk_metrics
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating portfolio report: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error generating portfolio report: {str(e)}")

@router.get("/portfolio_report/chart/allocations", response_model=ChartData)
async def get_allocation_chart_data(
    date: str = Query(..., description="Report date in YYYY-MM-DD format"),
    level: str = Query(..., description="Report level (client, group, portfolio, account, custom)"),
    level_key: str = Query(..., description="The key for the chosen level (e.g., client name, portfolio name)"),
    db: Session = Depends(get_db)
):
    """
    Get asset allocation chart data
    """
    try:
        # Parse date
        report_date = datetime.strptime(date, '%Y-%m-%d').date()
        
        # Get positions based on level and key
        if level == 'client':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.top_level_client == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'portfolio':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.portfolio == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'account':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.holding_account_number == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'group':
            # For group level, need to join with ownership_hierarchy
            account_numbers = db.query(OwnershipHierarchy.holding_account_number).filter(
                OwnershipHierarchy.groups.like(f"%{level_key}%")
            ).all()
            account_numbers = [acc[0] for acc in account_numbers]
            
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.holding_account_number.in_(account_numbers),
                FinancialPosition.date == report_date
            ).all()
        else:  # custom - assume custom is a comma-separated list of portfolio names
            custom_portfolios = level_key.split(',')
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.portfolio.in_(custom_portfolios),
                FinancialPosition.date == report_date
            ).all()
        
        if not positions:
            raise HTTPException(status_code=404, detail=f"No positions found for level '{level}', key '{level_key}' on date '{date}'")
        
        # Calculate asset allocation
        asset_allocation = {}
        total_value = 0
        
        for position in positions:
            adj_value = encryption_service.decrypt_to_float(position.adjusted_value)
            total_value += adj_value
            
            asset_class = position.asset_class
            if asset_class not in asset_allocation:
                asset_allocation[asset_class] = 0
            
            asset_allocation[asset_class] += adj_value
        
        # Prepare chart data
        labels = list(asset_allocation.keys())
        values = [round((value / total_value) * 100, 2) for value in asset_allocation.values()]
        
        # Define colors for chart
        colors = [
            '#14532D',  # Deep green
            '#22C55E',  # Success green
            '#0F172A',  # Navy
            '#1E293B',  # Slate
            '#475569',  # Gray
            '#94A3B8',  # Light gray
            '#EF4444',  # Red
            '#FB923C',  # Orange
            '#FACC15',  # Yellow
            '#4ADE80',  # Green
        ]
        
        # Repeat colors if we have more asset classes than colors
        if len(labels) > len(colors):
            colors = colors * (len(labels) // len(colors) + 1)
        
        # Prepare dataset
        dataset = {
            'data': values,
            'backgroundColor': colors[:len(labels)],
            'borderColor': 'rgba(255, 255, 255, 0.7)',
            'borderWidth': 1
        }
        
        return ChartData(
            labels=labels,
            datasets=[dataset]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating allocation chart data: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error generating allocation chart data: {str(e)}")

@router.get("/portfolio_report/chart/performance", response_model=ChartData)
async def get_performance_chart_data(
    date: str = Query(..., description="Report date in YYYY-MM-DD format"),
    level: str = Query(..., description="Report level (client, group, portfolio, account, custom)"),
    level_key: str = Query(..., description="The key for the chosen level (e.g., client name, portfolio name)"),
    period: str = Query("YTD", description="Performance period (1D, MTD, QTD, YTD)"),
    db: Session = Depends(get_db)
):
    """
    Get performance chart data
    """
    try:
        # Parse date
        end_date = datetime.strptime(date, '%Y-%m-%d').date()
        
        # Determine start date based on period
        if period == "1D":
            start_date = end_date - timedelta(days=1)
            date_interval = "day"
        elif period == "MTD":
            start_date = date(end_date.year, end_date.month, 1)
            date_interval = "day"
        elif period == "QTD":
            quarter_month = ((end_date.month - 1) // 3) * 3 + 1
            start_date = date(end_date.year, quarter_month, 1)
            date_interval = "week"
        elif period == "YTD":
            start_date = date(end_date.year, 1, 1)
            date_interval = "month"
        else:
            # Default to 1 month
            start_date = end_date - timedelta(days=30)
            date_interval = "day"
        
        # Get all summaries for the date range
        summaries = db.query(FinancialSummary).filter(
            FinancialSummary.level == level,
            FinancialSummary.level_key == level_key,
            FinancialSummary.report_date >= start_date,
            FinancialSummary.report_date <= end_date
        ).order_by(FinancialSummary.report_date).all()
        
        if not summaries:
            raise HTTPException(status_code=404, detail=f"No data found for level '{level}', key '{level_key}' in the selected date range")
        
        # Prepare chart data
        labels = [summary.report_date.strftime('%Y-%m-%d') for summary in summaries]
        values = [summary.total_adjusted_value for summary in summaries]
        
        # If first summary is not at start date, add a zero point
        if summaries[0].report_date > start_date:
            labels.insert(0, start_date.strftime('%Y-%m-%d'))
            values.insert(0, 0)
        
        # Calculate percentage change compared to first value
        first_value = values[0] if values[0] > 0 else 1  # Avoid division by zero
        percentages = [round(((value - first_value) / first_value) * 100, 2) for value in values]
        
        # Prepare datasets
        datasets = [
            {
                'label': 'Total Value',
                'data': values,
                'borderColor': '#14532D',  # Deep green
                'backgroundColor': 'rgba(20, 83, 45, 0.1)',
                'borderWidth': 2,
                'fill': True,
                'tension': 0.4,
                'yAxisID': 'y'
            },
            {
                'label': 'Percentage Change',
                'data': percentages,
                'borderColor': '#22C55E',  # Success green
                'backgroundColor': 'rgba(34, 197, 94, 0.1)',
                'borderWidth': 2,
                'borderDash': [5, 5],
                'fill': False,
                'tension': 0.4,
                'yAxisID': 'y1'
            }
        ]
        
        return ChartData(
            labels=labels,
            datasets=datasets
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating performance chart data: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error generating performance chart data: {str(e)}")

@router.get("/portfolio_report/chart/liquidity", response_model=ChartData)
async def get_liquidity_chart_data(
    date: str = Query(..., description="Report date in YYYY-MM-DD format"),
    level: str = Query(..., description="Report level (client, group, portfolio, account, custom)"),
    level_key: str = Query(..., description="The key for the chosen level (e.g., client name, portfolio name)"),
    db: Session = Depends(get_db)
):
    """
    Get liquidity chart data
    """
    try:
        # Parse date
        report_date = datetime.strptime(date, '%Y-%m-%d').date()
        
        # Get positions based on level and key
        if level == 'client':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.top_level_client == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'portfolio':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.portfolio == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'account':
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.holding_account_number == level_key,
                FinancialPosition.date == report_date
            ).all()
        elif level == 'group':
            # For group level, need to join with ownership_hierarchy
            account_numbers = db.query(OwnershipHierarchy.holding_account_number).filter(
                OwnershipHierarchy.groups.like(f"%{level_key}%")
            ).all()
            account_numbers = [acc[0] for acc in account_numbers]
            
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.holding_account_number.in_(account_numbers),
                FinancialPosition.date == report_date
            ).all()
        else:  # custom - assume custom is a comma-separated list of portfolio names
            custom_portfolios = level_key.split(',')
            positions = db.query(FinancialPosition).filter(
                FinancialPosition.portfolio.in_(custom_portfolios),
                FinancialPosition.date == report_date
            ).all()
        
        if not positions:
            raise HTTPException(status_code=404, detail=f"No positions found for level '{level}', key '{level_key}' on date '{date}'")
        
        # Calculate liquidity
        liquidity = {'Liquid': 0, 'Illiquid': 0}
        total_value = 0
        
        for position in positions:
            adj_value = encryption_service.decrypt_to_float(position.adjusted_value)
            total_value += adj_value
            
            liquidity_type = position.liquid_vs_illiquid
            if liquidity_type in liquidity:
                liquidity[liquidity_type] += adj_value
            else:
                # Default to illiquid if not specified
                liquidity['Illiquid'] += adj_value
        
        # Prepare chart data
        labels = list(liquidity.keys())
        values = [round((value / total_value) * 100, 2) for value in liquidity.values()]
        
        # Define colors for chart
        colors = [
            '#22C55E',  # Success green (Liquid)
            '#EF4444',  # Red (Illiquid)
        ]
        
        # Prepare dataset
        dataset = {
            'data': values,
            'backgroundColor': colors[:len(labels)],
            'borderColor': 'rgba(255, 255, 255, 0.7)',
            'borderWidth': 1
        }
        
        return ChartData(
            labels=labels,
            datasets=[dataset]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating liquidity chart data: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error generating liquidity chart data: {str(e)}")

@router.get("/ownership_tree")
async def get_ownership_tree(db: Session = Depends(get_db)):
    """
    Get the complete ownership hierarchy
    """
    try:
        ownerships = db.query(OwnershipHierarchy).all()
        
        if not ownerships:
            raise HTTPException(status_code=404, detail="No ownership data found")
        
        # Transform data into a hierarchical structure
        hierarchy = {}
        
        # Group by top-level client
        for ownership in ownerships:
            client = ownership.top_level_client
            
            if client not in hierarchy:
                hierarchy[client] = {
                    'id': client,
                    'name': client,
                    'type': 'client',
                    'children': {}
                }
            
            # Add portfolios
            portfolio = ownership.portfolio
            portfolio_key = f"{client}_{portfolio}"
            
            if portfolio_key not in hierarchy[client]['children']:
                hierarchy[client]['children'][portfolio_key] = {
                    'id': portfolio_key,
                    'name': portfolio,
                    'type': 'portfolio',
                    'parent': client,
                    'children': {}
                }
            
            # Add accounts
            account = ownership.holding_account
            account_key = f"{portfolio_key}_{ownership.holding_account_number}"
            
            if account_key not in hierarchy[client]['children'][portfolio_key]['children']:
                hierarchy[client]['children'][portfolio_key]['children'][account_key] = {
                    'id': account_key,
                    'name': account,
                    'type': 'account',
                    'account_number': ownership.holding_account_number,
                    'parent': portfolio_key,
                    'children': {}
                }
            
            # Add groups if present
            if ownership.groups:
                groups = ownership.groups.split(',')
                for group in groups:
                    group = group.strip()
                    if group:
                        group_key = f"{client}_{group}"
                        
                        # Add group to client if not exists
                        if group_key not in hierarchy[client]['children']:
                            hierarchy[client]['children'][group_key] = {
                                'id': group_key,
                                'name': group,
                                'type': 'group',
                                'parent': client,
                                'children': {}
                            }
                        
                        # Add account to group
                        hierarchy[client]['children'][group_key]['children'][account_key] = {
                            'id': account_key,
                            'name': account,
                            'type': 'account',
                            'account_number': ownership.holding_account_number,
                            'parent': group_key,
                            'children': {}
                        }
        
        # Convert dictionaries to lists for the final response
        result = []
        
        for client_key, client in hierarchy.items():
            client_node = {
                'id': client['id'],
                'name': client['name'],
                'type': client['type'],
                'children': []
            }
            
            for portfolio_key, portfolio in client['children'].items():
                portfolio_node = {
                    'id': portfolio['id'],
                    'name': portfolio['name'],
                    'type': portfolio['type'],
                    'parent': portfolio.get('parent'),
                    'children': []
                }
                
                for account_key, account in portfolio['children'].items():
                    account_node = {
                        'id': account['id'],
                        'name': account['name'],
                        'type': account['type'],
                        'account_number': account.get('account_number'),
                        'parent': account.get('parent')
                    }
                    
                    portfolio_node['children'].append(account_node)
                
                client_node['children'].append(portfolio_node)
            
            result.append(client_node)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving ownership tree: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error retrieving ownership tree: {str(e)}")
