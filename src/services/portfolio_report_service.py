"""
Portfolio Report Service

This service handles the calculations and data formatting for portfolio reports.
It processes financial position data to generate reports in the exact format
required by the Excel template.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text, func

from src.models.models import FinancialPosition, ModelPortfolio
from src.database import get_db_connection

logger = logging.getLogger(__name__)

class PortfolioReportService:
    """Service for generating portfolio reports"""
    
    def __init__(self):
        self.report_date = None
        self.level = None
        self.level_key = None
        
    def generate_report(self, report_date: date, level: str, level_key: str) -> Dict[str, Any]:
        """
        Generate a complete portfolio report for the specified date, level, and level key.
        
        Args:
            report_date: The date for the report
            level: The level (client, portfolio, account)
            level_key: The key for the specified level
            
        Returns:
            Dictionary containing the complete portfolio report data
        """
        self.report_date = report_date
        self.level = level
        self.level_key = level_key
        
        with get_db_connection() as db:
            try:
                # Get all positions for this portfolio/client/account
                positions = self._get_positions(db)
                
                if not positions or positions.empty:
                    logger.warning(f"No positions found for {level}={level_key} on {report_date}")
                    return self._generate_empty_report()
                
                # Calculate total adjusted value
                total_adjusted_value = self._calculate_total_value(positions)
                
                # Generate each section of the report
                equities_data = self._calculate_equities(positions, total_adjusted_value)
                fixed_income_data = self._calculate_fixed_income(positions, total_adjusted_value)
                hard_currency_data = self._calculate_hard_currency(positions, total_adjusted_value)
                uncorrelated_alts_data = self._calculate_uncorrelated_alternatives(positions, total_adjusted_value)
                cash_data = self._calculate_cash(positions, total_adjusted_value)
                liquidity_data = self._calculate_liquidity(positions)
                performance_data = self._calculate_performance(level, level_key)
                
                # Combine all data into the final report
                report_data = {
                    "report_date": report_date.strftime("%m/%d/%Y"),
                    "portfolio": level_key,
                    "level": level,
                    "total_adjusted_value": total_adjusted_value,
                    "equities": equities_data,
                    "fixed_income": fixed_income_data,
                    "hard_currency": hard_currency_data,
                    "uncorrelated_alternatives": uncorrelated_alts_data,
                    "cash": cash_data,
                    "liquidity": liquidity_data,
                    "performance": performance_data
                }
                
                return report_data
                
            except Exception as e:
                logger.error(f"Error generating portfolio report: {str(e)}")
                raise
    
    def _get_positions(self, db: Session) -> pd.DataFrame:
        """
        Get all positions for the specified level and level key
        
        Args:
            db: Database session
            
        Returns:
            DataFrame with all relevant positions
        """
        try:
            query_params = {"date": self.report_date}
            
            if self.level == 'client':
                where_clause = "top_level_client = :level_key"
                query_params["level_key"] = self.level_key
            elif self.level == 'portfolio':
                where_clause = "portfolio = :level_key"
                query_params["level_key"] = self.level_key
            elif self.level == 'account':
                where_clause = "holding_account = :level_key"
                query_params["level_key"] = self.level_key
            else:
                raise ValueError(f"Invalid level: {self.level}")
            
            query = text(f"""
            SELECT 
                position,
                top_level_client,
                holding_account,
                holding_account_number,
                portfolio,
                cusip,
                ticker_symbol,
                asset_class,
                second_level,
                third_level,
                adv_classification,
                liquid_vs_illiquid,
                CASE 
                    WHEN adjusted_value LIKE 'ENC:%' THEN SUBSTRING(adjusted_value, 5)
                    ELSE adjusted_value 
                END AS adjusted_value,
                date
            FROM financial_positions
            WHERE date = :date AND {where_clause}
            """)
            
            result = db.execute(query, query_params)
            columns = result.keys()
            positions_data = [dict(zip(columns, row)) for row in result.fetchall()]
            
            if not positions_data:
                return pd.DataFrame()
            
            # Convert to DataFrame for easier analysis
            df = pd.DataFrame(positions_data)
            
            # Convert adjusted_value to float for calculations
            df['adjusted_value'] = df['adjusted_value'].astype(float)
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting positions: {str(e)}")
            raise
    
    def _calculate_total_value(self, positions: pd.DataFrame) -> float:
        """
        Calculate the total adjusted value of all positions
        
        Args:
            positions: DataFrame containing all positions
            
        Returns:
            Total adjusted value as a float
        """
        return positions['adjusted_value'].sum()
    
    def _calculate_equities(self, positions: pd.DataFrame, total_value: float) -> Dict[str, Any]:
        """
        Calculate equity allocations and metrics
        
        Args:
            positions: DataFrame containing all positions
            total_value: Total adjusted value of the portfolio
            
        Returns:
            Dictionary with equity data structured for the report
        """
        # Filter for equity positions
        equity_positions = positions[positions['asset_class'].str.lower() == 'equity']
        
        # Calculate total equity value
        equity_value = equity_positions['adjusted_value'].sum()
        equity_pct = (equity_value / total_value) * 100 if total_value > 0 else 0
        
        # Initialize subcategories
        subcategories = [
            "US Markets", "Global Markets", "Emerging Markets", "Commodities", 
            "Real Estate", "Private Equity", "High Yield", "Venture Capital", 
            "Low Beta Alpha", "Equity Derivatives", "Income Notes"
        ]
        
        # Calculate subcategory allocations
        subcategory_data = {}
        for subcategory in subcategories:
            subcategory_positions = equity_positions[
                equity_positions['second_level'].str.lower() == subcategory.lower()
            ] if not equity_positions.empty else pd.DataFrame()
            
            subcategory_value = subcategory_positions['adjusted_value'].sum() if not subcategory_positions.empty else 0
            subcategory_pct = (subcategory_value / total_value) * 100 if total_value > 0 else 0
            subcategory_data[subcategory.replace(" ", "_").lower()] = subcategory_pct
        
        # Get vol and beta from risk stats (if available)
        # For now, we'll leave these blank as mentioned
        vol = None
        beta = None
        beta_adjusted = None
        
        return {
            "total_pct": equity_pct,
            "vol": vol,
            "beta": beta,
            "beta_adjusted": beta_adjusted,
            "subcategories": subcategory_data
        }
    
    def _calculate_fixed_income(self, positions: pd.DataFrame, total_value: float) -> Dict[str, Any]:
        """
        Calculate fixed income allocations and metrics
        
        Args:
            positions: DataFrame containing all positions
            total_value: Total adjusted value of the portfolio
            
        Returns:
            Dictionary with fixed income data structured for the report
        """
        # Filter for fixed income positions
        fixed_income_positions = positions[
            positions['asset_class'].str.lower().str.contains('fixed income')
        ]
        
        # Calculate total fixed income value
        fixed_income_value = fixed_income_positions['adjusted_value'].sum()
        fixed_income_pct = (fixed_income_value / total_value) * 100 if total_value > 0 else 0
        
        # Fixed income subcategories
        subcategories = {
            "municipal_bonds": "Municipal Bonds",
            "investment_grade": "Investment Grade",
            "government_bonds": "Government Bonds",
            "fixed_income_derivatives": "Fixed Income Derivatives"
        }
        
        # Calculate subcategory allocations
        subcategory_data = {}
        for key, subcategory in subcategories.items():
            subcategory_positions = fixed_income_positions[
                fixed_income_positions['second_level'].str.lower() == subcategory.lower()
            ] if not fixed_income_positions.empty else pd.DataFrame()
            
            subcategory_value = subcategory_positions['adjusted_value'].sum() if not subcategory_positions.empty else 0
            subcategory_pct = (subcategory_value / total_value) * 100 if total_value > 0 else 0
            
            subcategory_data[key] = {
                "total_pct": subcategory_pct,
            }
            
            # For each subcategory, add the duration breakdowns
            if key in ["municipal_bonds", "investment_grade", "government_bonds"]:
                duration_types = ["Short Duration", "Market Duration", "Long Duration"]
                for duration_type in duration_types:
                    duration_positions = subcategory_positions[
                        subcategory_positions['third_level'].str.lower() == duration_type.lower()
                    ] if not subcategory_positions.empty else pd.DataFrame()
                    
                    duration_value = duration_positions['adjusted_value'].sum() if not duration_positions.empty else 0
                    duration_pct = (duration_value / total_value) * 100 if total_value > 0 else 0
                    
                    # Convert the duration type key to snake_case
                    duration_key = duration_type.lower().replace(" ", "_")
                    subcategory_data[key][duration_key] = duration_pct
        
        # Get average duration (if available)
        # For now, we'll leave this blank as mentioned
        duration = None
        
        return {
            "total_pct": fixed_income_pct,
            "duration": duration,
            "subcategories": subcategory_data
        }
    
    def _calculate_hard_currency(self, positions: pd.DataFrame, total_value: float) -> Dict[str, Any]:
        """
        Calculate hard currency allocations
        
        Args:
            positions: DataFrame containing all positions
            total_value: Total adjusted value of the portfolio
            
        Returns:
            Dictionary with hard currency data structured for the report
        """
        # Filter for alternative positions with "Precious Metals" as second level
        hard_currency_positions = positions[
            (positions['asset_class'].str.lower() == 'alternative') & 
            (positions['second_level'].str.lower() == 'precious metals')
        ]
        
        # Calculate total hard currency value
        hard_currency_value = hard_currency_positions['adjusted_value'].sum()
        hard_currency_pct = (hard_currency_value / total_value) * 100 if total_value > 0 else 0
        
        # Hard currency subcategories
        subcategories = ["Gold", "Silver", "Gold Miners", "Silver Miners", "Industrial Metals", 
                         "Hard Currency Physical Investment", "Precious Metals Derivatives"]
        
        subcategory_data = {}
        for subcategory in subcategories:
            subcategory_positions = hard_currency_positions[
                hard_currency_positions['third_level'].str.lower() == subcategory.lower()
            ] if not hard_currency_positions.empty else pd.DataFrame()
            
            subcategory_value = subcategory_positions['adjusted_value'].sum() if not subcategory_positions.empty else 0
            subcategory_pct = (subcategory_value / total_value) * 100 if total_value > 0 else 0
            
            subcategory_key = subcategory.lower().replace(" ", "_")
            subcategory_data[subcategory_key] = subcategory_pct
        
        return {
            "total_pct": hard_currency_pct,
            "subcategories": subcategory_data
        }
    
    def _calculate_uncorrelated_alternatives(self, positions: pd.DataFrame, total_value: float) -> Dict[str, Any]:
        """
        Calculate uncorrelated alternatives allocations
        
        Args:
            positions: DataFrame containing all positions
            total_value: Total adjusted value of the portfolio
            
        Returns:
            Dictionary with uncorrelated alternatives data structured for the report
        """
        # Filter for alternative positions that are NOT "Precious Metals"
        uncorrelated_alt_positions = positions[
            (positions['asset_class'].str.lower() == 'alternative') & 
            ((positions['second_level'].str.lower() != 'precious metals') | 
             (positions['second_level'].isnull()))
        ]
        
        # Calculate total uncorrelated alternatives value
        uncorrelated_alt_value = uncorrelated_alt_positions['adjusted_value'].sum()
        uncorrelated_alt_pct = (uncorrelated_alt_value / total_value) * 100 if total_value > 0 else 0
        
        # Uncorrelated alternatives subcategories
        subcategories = ["Crypto", "CTAs", "Periodic Short Term Alt Fund", "Periodic Long Term Alt Fund"]
        
        subcategory_data = {}
        for subcategory in subcategories:
            subcategory_positions = uncorrelated_alt_positions[
                uncorrelated_alt_positions['third_level'].str.lower() == subcategory.lower()
            ] if not uncorrelated_alt_positions.empty else pd.DataFrame()
            
            subcategory_value = subcategory_positions['adjusted_value'].sum() if not subcategory_positions.empty else 0
            subcategory_pct = (subcategory_value / total_value) * 100 if total_value > 0 else 0
            
            subcategory_key = subcategory.lower().replace(" ", "_")
            subcategory_data[subcategory_key] = subcategory_pct
        
        return {
            "total_pct": uncorrelated_alt_pct,
            "subcategories": subcategory_data
        }
    
    def _calculate_cash(self, positions: pd.DataFrame, total_value: float) -> Dict[str, Any]:
        """
        Calculate cash allocations
        
        Args:
            positions: DataFrame containing all positions
            total_value: Total adjusted value of the portfolio
            
        Returns:
            Dictionary with cash data structured for the report
        """
        # Filter for cash positions
        cash_positions = positions[
            positions['asset_class'].str.lower().str.contains('cash')
        ]
        
        # Calculate total cash value
        cash_value = cash_positions['adjusted_value'].sum()
        cash_pct = (cash_value / total_value) * 100 if total_value > 0 else 0
        
        return {
            "total_pct": cash_pct
        }
    
    def _calculate_liquidity(self, positions: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate liquidity metrics
        
        Args:
            positions: DataFrame containing all positions
            
        Returns:
            Dictionary with liquidity data
        """
        if positions.empty:
            return {
                "liquid_assets": 0,
                "illiquid_assets": 0
            }
        
        # Get total value
        total_value = positions['adjusted_value'].sum()
        
        # Calculate liquid vs illiquid
        liquid_positions = positions[
            positions['liquid_vs_illiquid'].str.lower() == 'liquid'
        ]
        illiquid_positions = positions[
            positions['liquid_vs_illiquid'].str.lower() == 'illiquid'
        ]
        
        liquid_value = liquid_positions['adjusted_value'].sum() if not liquid_positions.empty else 0
        illiquid_value = illiquid_positions['adjusted_value'].sum() if not illiquid_positions.empty else 0
        
        liquid_pct = (liquid_value / total_value) * 100 if total_value > 0 else 0
        illiquid_pct = (illiquid_value / total_value) * 100 if total_value > 0 else 0
        
        return {
            "liquid_assets": liquid_pct,
            "illiquid_assets": illiquid_pct
        }
    
    def _calculate_performance(self, level: str, level_key: str) -> Dict[str, float]:
        """
        Calculate performance metrics
        
        Args:
            level: The level (client, portfolio, account)
            level_key: The key for the specified level
            
        Returns:
            Dictionary with performance data
        """
        # In a real implementation, this would pull from historical data
        # For now, we'll return placeholder data
        return {
            "1D": 0.0,
            "MTD": 0.0,
            "QTD": 0.0,
            "YTD": 0.0
        }
    
    def _generate_empty_report(self) -> Dict[str, Any]:
        """
        Generate an empty report when no data is available
        
        Returns:
            Empty report structure with zeros
        """
        equity_subcategories = [
            "us_markets", "global_markets", "emerging_markets", "commodities", 
            "real_estate", "private_equity", "high_yield", "venture_capital", 
            "low_beta_alpha", "equity_derivatives", "income_notes"
        ]
        
        fixed_income_subcategories = ["municipal_bonds", "investment_grade", "government_bonds", "fixed_income_derivatives"]
        fixed_income_duration_types = ["short_duration", "market_duration", "long_duration"]
        
        hard_currency_subcategories = [
            "gold", "silver", "gold_miners", "silver_miners", "industrial_metals", 
            "hard_currency_physical_investment", "precious_metals_derivatives"
        ]
        
        uncorrelated_alt_subcategories = [
            "crypto", "ctas", "periodic_short_term_alt_fund", "periodic_long_term_alt_fund"
        ]
        
        return {
            "report_date": self.report_date.strftime("%m/%d/%Y") if self.report_date else None,
            "portfolio": self.level_key,
            "level": self.level,
            "total_adjusted_value": 0,
            "equities": {
                "total_pct": 0,
                "vol": None,
                "beta": None,
                "beta_adjusted": None,
                "subcategories": {subcategory: 0 for subcategory in equity_subcategories}
            },
            "fixed_income": {
                "total_pct": 0,
                "duration": None,
                "subcategories": {
                    subcategory: {
                        "total_pct": 0,
                        **{duration_type: 0 for duration_type in fixed_income_duration_types}
                    } if subcategory != "fixed_income_derivatives" else {"total_pct": 0}
                    for subcategory in fixed_income_subcategories
                }
            },
            "hard_currency": {
                "total_pct": 0,
                "subcategories": {subcategory: 0 for subcategory in hard_currency_subcategories}
            },
            "uncorrelated_alternatives": {
                "total_pct": 0,
                "subcategories": {subcategory: 0 for subcategory in uncorrelated_alt_subcategories}
            },
            "cash": {
                "total_pct": 0
            },
            "liquidity": {
                "liquid_assets": 0,
                "illiquid_assets": 0
            },
            "performance": {
                "1D": 0.0,
                "MTD": 0.0,
                "QTD": 0.0,
                "YTD": 0.0
            }
        }