"""
Database models for the nori Financial Portfolio Reporting System.
"""

import sqlalchemy as sa
from sqlalchemy.orm import relationship
from datetime import date

from src.database import Base


class OwnershipMetadata(Base):
    """
    Metadata for ownership tree uploads.
    """
    __tablename__ = 'ownership_metadata'
    
    id = sa.Column(sa.Integer, primary_key=True)
    filename = sa.Column(sa.String, nullable=False)
    upload_date = sa.Column(sa.DateTime, server_default=sa.func.now())
    row_count = sa.Column(sa.Integer)
    has_classifications = sa.Column(sa.Boolean, default=False)
    
    # Relationships
    items = relationship("OwnershipItem", back_populates="meta_info", cascade="all, delete-orphan")


class OwnershipItem(Base):
    """
    Individual item in the ownership tree.
    """
    __tablename__ = 'ownership_items'
    
    id = sa.Column(sa.Integer, primary_key=True)
    metadata_id = sa.Column(sa.Integer, sa.ForeignKey('ownership_metadata.id'), nullable=False)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey('ownership_items.id'), nullable=True)
    name = sa.Column(sa.String, nullable=False)
    type = sa.Column(sa.String)  # 'client', 'group', 'account'
    account_number = sa.Column(sa.String)
    row_order = sa.Column(sa.Integer)
    
    # Relationships
    meta_info = relationship("OwnershipMetadata", back_populates="items")
    children = relationship("OwnershipItem", 
                           backref=sa.orm.backref('parent', remote_side=[id]),
                           cascade="all, delete-orphan")


class FinancialPosition(Base):
    """
    Represents a financial position from the data dump.
    Contains all the raw data from the file upload.
    """
    __tablename__ = 'financial_positions'
    
    id = sa.Column(sa.Integer, primary_key=True)
    date = sa.Column(sa.Date, nullable=False, index=True)  # The actual date column in the database
    position = sa.Column(sa.String, nullable=False)
    top_level_client = sa.Column(sa.String, nullable=False, index=True)
    holding_account = sa.Column(sa.String, nullable=False)
    holding_account_number = sa.Column(sa.String, nullable=False, index=True)
    portfolio = sa.Column(sa.String, nullable=False, index=True)
    cusip = sa.Column(sa.String)
    ticker_symbol = sa.Column(sa.String)
    asset_class = sa.Column(sa.String, index=True)
    second_level = sa.Column(sa.String, index=True)
    third_level = sa.Column(sa.String, index=True)
    adv_classification = sa.Column(sa.String)
    liquid_vs_illiquid = sa.Column(sa.String, index=True)
    adjusted_value = sa.Column(sa.Numeric(20, 2), nullable=False)
    row_order = sa.Column(sa.Integer, index=True)  # To preserve original order
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    # For compatibility with code that expects 'report_date'
    @property
    def report_date(self):
        return self.date


class FinancialSummary(Base):
    """
    Pre-calculated summary table for optimized reporting.
    Stores aggregate data at various levels (client, portfolio, account).
    """
    __tablename__ = 'financial_summary'
    
    id = sa.Column(sa.Integer, primary_key=True)
    report_date = sa.Column(sa.Date, nullable=False, index=True)
    level = sa.Column(sa.String, nullable=False, index=True)  # 'client', 'portfolio', 'account'
    level_key = sa.Column(sa.String, nullable=False, index=True)
    
    # Total values
    total_adjusted_value = sa.Column(sa.Numeric(20, 2), nullable=False)
    
    # Asset class totals
    equities_pct = sa.Column(sa.Numeric(10, 4))
    fixed_income_pct = sa.Column(sa.Numeric(10, 4))
    alternatives_pct = sa.Column(sa.Numeric(10, 4))
    hard_currency_pct = sa.Column(sa.Numeric(10, 4))
    uncorrelated_alternatives_pct = sa.Column(sa.Numeric(10, 4))
    cash_pct = sa.Column(sa.Numeric(10, 4))
    
    # Liquidity
    liquid_assets_pct = sa.Column(sa.Numeric(10, 4))
    illiquid_assets_pct = sa.Column(sa.Numeric(10, 4))
    
    # JSON fields for detailed breakdowns
    equities_detail = sa.Column(sa.JSON)
    fixed_income_detail = sa.Column(sa.JSON)
    hard_currency_detail = sa.Column(sa.JSON)
    uncorrelated_alternatives_detail = sa.Column(sa.JSON)
    
    # Timestamps
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    __table_args__ = (
        sa.UniqueConstraint('report_date', 'level', 'level_key', name='uix_summary_date_level_key'),
    )


class OwnershipNode(Base):
    """
    Represents a node in the ownership hierarchy tree.
    Used to build the ownership structure visualization.
    """
    __tablename__ = 'ownership_nodes'
    
    id = sa.Column(sa.Integer, primary_key=True)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey('ownership_nodes.id'), nullable=True)
    name = sa.Column(sa.String, nullable=False)
    type = sa.Column(sa.String, nullable=False)  # 'client', 'group', 'account'
    level = sa.Column(sa.Integer, nullable=False)
    meta = sa.Column(sa.JSON)
    row_order = sa.Column(sa.Integer)  # To preserve original order
    
    # Relationships
    children = relationship("OwnershipNode", 
                          backref=sa.orm.backref('parent', remote_side=[id]),
                          cascade="all, delete-orphan")
    
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())


class UploadStatus(Base):
    """
    Tracks the status of background file uploads and processing.
    """
    __tablename__ = 'upload_status'
    
    id = sa.Column(sa.Integer, primary_key=True)
    task_id = sa.Column(sa.String, nullable=False, unique=True)
    filename = sa.Column(sa.String, nullable=False)
    status = sa.Column(sa.String, nullable=False)  # 'pending', 'processing', 'completed', 'failed'
    progress = sa.Column(sa.Integer, default=0)  # 0-100 percentage
    result = sa.Column(sa.JSON)
    error = sa.Column(sa.String)
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())


class SecurityRiskStats(Base):
    """
    Risk statistics for securities, imported from a separate file.
    Will be used for enhanced risk metrics and duration calculations.
    """
    __tablename__ = 'security_risk_stats'
    
    id = sa.Column(sa.Integer, primary_key=True)
    report_date = sa.Column(sa.Date, nullable=False, index=True)
    cusip = sa.Column(sa.String, nullable=False, index=True)
    ticker_symbol = sa.Column(sa.String, index=True)
    security_name = sa.Column(sa.String)
    duration = sa.Column(sa.Numeric(10, 4))
    yield_to_maturity = sa.Column(sa.Numeric(10, 4))
    beta = sa.Column(sa.Numeric(10, 4))
    volatility = sa.Column(sa.Numeric(10, 4))
    sharpe_ratio = sa.Column(sa.Numeric(10, 4))
    meta = sa.Column(sa.JSON)  # For any additional fields
    
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    __table_args__ = (
        sa.UniqueConstraint('report_date', 'cusip', name='uix_risk_stats_date_cusip'),
    )


class RiskStatisticEquity(Base):
    """
    Risk statistics specific to equity securities.
    """
    __tablename__ = 'risk_statistic_equity'
    
    id = sa.Column(sa.Integer, primary_key=True)
    report_date = sa.Column(sa.Date, nullable=False, index=True)
    security_id = sa.Column(sa.String, nullable=False, index=True)  # CUSIP or internal ID
    beta = sa.Column(sa.Numeric(10, 4))
    volatility = sa.Column(sa.Numeric(10, 4))
    alpha = sa.Column(sa.Numeric(10, 4))
    sharpe_ratio = sa.Column(sa.Numeric(10, 4))
    information_ratio = sa.Column(sa.Numeric(10, 4))
    tracking_error = sa.Column(sa.Numeric(10, 4))
    max_drawdown = sa.Column(sa.Numeric(10, 4))
    meta = sa.Column(sa.JSON)  # For any additional fields
    
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    __table_args__ = (
        sa.UniqueConstraint('report_date', 'security_id', name='uix_equity_risk_date_security'),
    )


class RiskStatisticFixedIncome(Base):
    """
    Risk statistics specific to fixed income securities.
    """
    __tablename__ = 'risk_statistic_fixed_income'
    
    id = sa.Column(sa.Integer, primary_key=True)
    report_date = sa.Column(sa.Date, nullable=False, index=True)
    security_id = sa.Column(sa.String, nullable=False, index=True)  # CUSIP or internal ID
    duration = sa.Column(sa.Numeric(10, 4))
    modified_duration = sa.Column(sa.Numeric(10, 4))
    convexity = sa.Column(sa.Numeric(10, 4))
    yield_to_maturity = sa.Column(sa.Numeric(10, 4))
    yield_to_worst = sa.Column(sa.Numeric(10, 4))
    option_adjusted_spread = sa.Column(sa.Numeric(10, 4))
    credit_rating = sa.Column(sa.String)
    meta = sa.Column(sa.JSON)  # For any additional fields
    
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    __table_args__ = (
        sa.UniqueConstraint('report_date', 'security_id', name='uix_fixed_income_risk_date_security'),
    )


class RiskStatisticAlternatives(Base):
    """
    Risk statistics specific to alternative investments.
    """
    __tablename__ = 'risk_statistic_alternatives'
    
    id = sa.Column(sa.Integer, primary_key=True)
    report_date = sa.Column(sa.Date, nullable=False, index=True)
    security_id = sa.Column(sa.String, nullable=False, index=True)  # CUSIP or internal ID
    correlation_equity = sa.Column(sa.Numeric(10, 4))
    correlation_fixed_income = sa.Column(sa.Numeric(10, 4))
    beta = sa.Column(sa.Numeric(10, 4))
    volatility = sa.Column(sa.Numeric(10, 4))
    max_drawdown = sa.Column(sa.Numeric(10, 4))
    illiquidity_premium = sa.Column(sa.Numeric(10, 4))
    meta = sa.Column(sa.JSON)  # For any additional fields
    
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    __table_args__ = (
        sa.UniqueConstraint('report_date', 'security_id', name='uix_alternatives_risk_date_security'),
    )


class ModelPortfolio(Base):
    """
    Represents a model portfolio with target allocations.
    """
    __tablename__ = 'model_portfolios'
    
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False, unique=True)
    description = sa.Column(sa.Text)
    
    # Target allocations
    equity_target = sa.Column(sa.Numeric(10, 4))
    fixed_income_target = sa.Column(sa.Numeric(10, 4))
    hard_currency_target = sa.Column(sa.Numeric(10, 4))
    alternatives_target = sa.Column(sa.Numeric(10, 4))
    cash_target = sa.Column(sa.Numeric(10, 4))
    
    # Detailed allocations stored as JSON
    equity_detail = sa.Column(sa.JSON)
    fixed_income_detail = sa.Column(sa.JSON)
    hard_currency_detail = sa.Column(sa.JSON)
    alternatives_detail = sa.Column(sa.JSON)
    
    is_active = sa.Column(sa.Boolean, default=True)
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())


class PerformanceData(Base):
    """
    Historical performance data for portfolios, accounts, and clients.
    Used to calculate performance metrics (1D, MTD, QTD, YTD).
    """
    __tablename__ = 'performance_data'
    
    id = sa.Column(sa.Integer, primary_key=True)
    report_date = sa.Column(sa.Date, nullable=False, index=True)
    level = sa.Column(sa.String, nullable=False, index=True)  # 'client', 'portfolio', 'account'
    level_key = sa.Column(sa.String, nullable=False, index=True)
    
    # Daily performance values
    daily_return = sa.Column(sa.Numeric(10, 6))
    mtd_return = sa.Column(sa.Numeric(10, 6))
    qtd_return = sa.Column(sa.Numeric(10, 6))
    ytd_return = sa.Column(sa.Numeric(10, 6))
    
    total_value = sa.Column(sa.Numeric(20, 2), nullable=False)
    previous_day_value = sa.Column(sa.Numeric(20, 2))
    
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    __table_args__ = (
        sa.UniqueConstraint('report_date', 'level', 'level_key', name='uix_performance_date_level_key'),
    )


class EgnyteRiskStat(Base):
    """
    Risk statistics for securities fetched from Egnyte API.
    Combines data from Equity, Fixed Income, and Alternatives tabs in the risk stats Excel file.
    """
    __tablename__ = 'egnyte_risk_stats'
    
    id = sa.Column(sa.Integer, primary_key=True)
    import_date = sa.Column(sa.Date, nullable=False, index=True)  # Date the data was imported
    position = sa.Column(sa.String, nullable=False, index=True)   # Security/position name
    ticker_symbol = sa.Column(sa.String, index=True)              # Ticker symbol if available
    cusip = sa.Column(sa.String, index=True)                      # CUSIP if available
    asset_class = sa.Column(sa.String, nullable=False, index=True)  # 'Equity', 'Fixed Income', 'Alternatives'
    second_level = sa.Column(sa.String, index=True)               # Secondary classification
    bloomberg_id = sa.Column(sa.String, index=True)               # Bloomberg identifier
    
    # Risk metrics
    volatility = sa.Column(sa.Numeric(10, 6))                     # Volatility (standard deviation)
    beta = sa.Column(sa.Numeric(10, 6))                           # Beta (compared to benchmark)
    duration = sa.Column(sa.Numeric(10, 6))                       # Duration (for fixed income)
    
    # Additional metadata
    notes = sa.Column(sa.Text)                                    # Notes about proxy usage etc.
    amended_id = sa.Column(sa.String, index=True)                 # Alternative identifier
    
    # Source tracking
    source_file = sa.Column(sa.String)                            # Original file path/name
    source_tab = sa.Column(sa.String)                             # Tab in Excel file
    source_row = sa.Column(sa.Integer)                            # Row in the Excel file
    
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
    
    __table_args__ = (
        sa.UniqueConstraint('import_date', 'position', 'asset_class', name='uix_egnyte_risk_date_position_asset'),
    )