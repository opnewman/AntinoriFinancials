import datetime
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Boolean, func
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
    grouping_attribute_name = Column(String, nullable=False)  # Client, Group, or Holding Account
    upload_date = Column(Date, default=datetime.date.today, nullable=False)
    metadata_id = Column(Integer, ForeignKey("ownership_metadata.id"), nullable=False)
    
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
