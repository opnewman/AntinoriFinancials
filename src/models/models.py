import datetime
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Boolean, func, Index
from sqlalchemy.orm import relationship

# Import Base from database module
from src.database import Base

class FinancialPosition(Base):
    """
    Stores financial position data from data_dump.xlsx
    """
    __tablename__ = "financial_positions"
    
    id = Column(Integer, primary_key=True, index=True)
    position = Column(String, nullable=False)
    top_level_client = Column(String, nullable=False)
    holding_account = Column(String, nullable=False)
    holding_account_number = Column(String, nullable=False)
    portfolio = Column(String, nullable=False)
    cusip = Column(String)
    ticker_symbol = Column(String)
    asset_class = Column(String, nullable=False)
    second_level = Column(String)
    third_level = Column(String)
    adv_classification = Column(String)
    liquid_vs_illiquid = Column(String, nullable=False)
    adjusted_value = Column(String, nullable=False)  # Encrypted value stored as string
    date = Column(Date, nullable=False)
    upload_date = Column(Date, default=datetime.date.today, nullable=False)
    
    # Relationships
    risk_equity = relationship("RiskStatisticEquity", 
                              primaryjoin="and_(FinancialPosition.position==RiskStatisticEquity.position, "
                                         "FinancialPosition.ticker_symbol==RiskStatisticEquity.ticker_symbol)",
                              foreign_keys="[RiskStatisticEquity.position, RiskStatisticEquity.ticker_symbol]", 
                              uselist=False, viewonly=True)
    risk_fixed_income = relationship("RiskStatisticFixedIncome", 
                                    primaryjoin="and_(FinancialPosition.position==RiskStatisticFixedIncome.position, "
                                               "FinancialPosition.ticker_symbol==RiskStatisticFixedIncome.ticker_symbol)",
                                    foreign_keys="[RiskStatisticFixedIncome.position, RiskStatisticFixedIncome.ticker_symbol]", 
                                    uselist=False, viewonly=True)
    risk_alternatives = relationship("RiskStatisticAlternatives", 
                                    primaryjoin="and_(FinancialPosition.position==RiskStatisticAlternatives.position, "
                                               "FinancialPosition.ticker_symbol==RiskStatisticAlternatives.ticker_symbol)",
                                    foreign_keys="[RiskStatisticAlternatives.position, RiskStatisticAlternatives.ticker_symbol]", 
                                    uselist=False, viewonly=True)

class OwnershipMetadata(Base):
    """
    Stores metadata about the ownership file (first 3 rows)
    """
    __tablename__ = "ownership_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    view_name = Column(String, nullable=False)
    date_range_start = Column(Date, nullable=False)
    date_range_end = Column(Date, nullable=False)
    portfolio_coverage = Column(String, nullable=False)
    upload_date = Column(Date, default=datetime.date.today, nullable=False)
    is_current = Column(Boolean, default=True, nullable=False)  # Flag to mark the most recent upload

class OwnershipItem(Base):
    """
    Stores ownership hierarchy data from ownership.xlsx (row 4 onwards)
    """
    __tablename__ = "ownership_items"
    
    id = Column(Integer, primary_key=True, index=True)
    client = Column(String, nullable=False, index=True)
    entity_id = Column(String, index=True)
    holding_account_number = Column(String, index=True)
    portfolio = Column(String, index=True)
    group_id = Column(String, index=True)
    data_inception_date = Column(Date)
    ownership_percentage = Column(Float)
    grouping_attribute_name = Column(String, nullable=False, index=True)  # Client, Group, or Holding Account
    upload_date = Column(Date, default=datetime.date.today, nullable=False)
    metadata_id = Column(Integer, ForeignKey("ownership_metadata.id"), nullable=False, index=True)
    row_order = Column(Integer, index=True)  # Store original Excel row order for proper hierarchy construction
    
    # Define additional indexes for common queries
    __table_args__ = (
        # Composite index for faster filtering by metadata_id and grouping_attribute_name
        Index('idx_ownership_metadata_grouping', 'metadata_id', 'grouping_attribute_name'),
        # Composite index for client/portfolio lookups
        Index('idx_ownership_client_portfolio', 'client', 'portfolio'),
        # Composite index for group lookups
        Index('idx_ownership_portfolio_group', 'portfolio', 'group_id'),
        # Index for row ordering (critical for rebuilding the hierarchy)
        Index('idx_ownership_row_order', 'metadata_id', 'row_order'),
    )
    
    # Relationship to metadata
    ownership_metadata = relationship("OwnershipMetadata")

class FinancialSummary(Base):
    """
    Stores aggregated financial summary data
    """
    __tablename__ = "financial_summary"
    
    id = Column(Integer, primary_key=True, index=True)
    level = Column(String, nullable=False, index=True)  # client, group, portfolio, account
    level_key = Column(String, nullable=False, index=True)  # The name/identifier of the level
    total_adjusted_value = Column(Float, nullable=False)
    upload_date = Column(Date, default=datetime.date.today, nullable=False)
    report_date = Column(Date, nullable=False, index=True)

class RiskStatisticEquity(Base):
    """
    Stores risk statistics for equity positions
    """
    __tablename__ = "risk_statistic_equity"
    
    id = Column(Integer, primary_key=True, index=True)
    position = Column(String, nullable=False, index=True)
    ticker_symbol = Column(String, nullable=False, index=True)
    vol = Column(Float)
    beta = Column(Float)

class RiskStatisticFixedIncome(Base):
    """
    Stores risk statistics for fixed income positions
    """
    __tablename__ = "risk_statistic_fixed_income"
    
    id = Column(Integer, primary_key=True, index=True)
    position = Column(String, nullable=False, index=True)
    ticker_symbol = Column(String, nullable=False, index=True)
    vol = Column(Float)
    duration = Column(Float)

class RiskStatisticAlternatives(Base):
    """
    Stores risk statistics for alternative investment positions
    """
    __tablename__ = "risk_statistic_alternatives"
    
    id = Column(Integer, primary_key=True, index=True)
    position = Column(String, nullable=False, index=True)
    ticker_symbol = Column(String, nullable=False, index=True)
    vol = Column(Float)
    beta_to_gold = Column(Float)

class ModelPortfolio(Base):
    """
    Stores model portfolio information (like those in your screenshot)
    """
    __tablename__ = "model_portfolios"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    description = Column(String)
    is_active = Column(Boolean, default=True)
    creation_date = Column(Date, default=datetime.date.today, nullable=False)
    update_date = Column(Date, default=datetime.date.today, onupdate=datetime.date.today, nullable=False)
    
    # Relationships
    allocations = relationship("ModelPortfolioAllocation", back_populates="model_portfolio", cascade="all, delete-orphan")

class ModelPortfolioAllocation(Base):
    """
    Stores allocation percentages for model portfolios
    """
    __tablename__ = "model_portfolio_allocations"
    
    id = Column(Integer, primary_key=True, index=True)
    model_portfolio_id = Column(Integer, ForeignKey("model_portfolios.id"), nullable=False, index=True)
    category = Column(String, nullable=False, index=True)  # e.g., "Equities", "Fixed Income", etc.
    subcategory = Column(String, index=True)  # e.g., "US Equities", "EM Markets", etc.
    allocation_percentage = Column(Float, nullable=False)
    is_model_weight = Column(Boolean, default=True)  # Flag for model vs actual weight
    
    # Relationship back to model portfolio
    model_portfolio = relationship("ModelPortfolio", back_populates="allocations")
    
    # Additional constraints
    __table_args__ = (
        # Composite index for faster filtering
        Index('idx_model_category', 'model_portfolio_id', 'category', 'subcategory'),
    )

class FixedIncomeMetrics(Base):
    """
    Stores fixed income metrics for model portfolios (duration, yield, etc.)
    """
    __tablename__ = "fixed_income_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    model_portfolio_id = Column(Integer, ForeignKey("model_portfolios.id"), nullable=False, index=True)
    metric_name = Column(String, nullable=False, index=True)  # e.g., "Duration", "Yield", etc.
    metric_subcategory = Column(String, index=True)  # e.g., "Municipal Bonds", "Long Duration", etc.
    metric_value = Column(Float, nullable=False)
    
    # Relationship to model portfolio
    model_portfolio = relationship("ModelPortfolio")

class CurrencyAllocation(Base):
    """
    Stores hard currency allocations for model portfolios
    """
    __tablename__ = "currency_allocations"
    
    id = Column(Integer, primary_key=True, index=True)
    model_portfolio_id = Column(Integer, ForeignKey("model_portfolios.id"), nullable=False, index=True)
    currency_name = Column(String, nullable=False, index=True)  # e.g., "USD", "EUR", etc.
    allocation_percentage = Column(Float, nullable=False)
    
    # Relationship to model portfolio
    model_portfolio = relationship("ModelPortfolio")

class PerformanceMetric(Base):
    """
    Stores performance metrics for model portfolios
    """
    __tablename__ = "performance_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    model_portfolio_id = Column(Integer, ForeignKey("model_portfolios.id"), nullable=False, index=True)
    period = Column(String, nullable=False, index=True)  # e.g., "1D", "MTD", "QTD", "YTD"
    performance_percentage = Column(Float, nullable=False)
    as_of_date = Column(Date, nullable=False, index=True)
    
    # Relationship to model portfolio
    model_portfolio = relationship("ModelPortfolio")
