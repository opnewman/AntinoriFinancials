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

class OwnershipHierarchy(Base):
    """
    Stores ownership hierarchy data from ownership.xlsx
    """
    __tablename__ = "ownership_hierarchy"
    
    id = Column(Integer, primary_key=True, index=True)
    holding_account = Column(String, nullable=False)
    holding_account_number = Column(String, nullable=False, index=True)
    top_level_client = Column(String, nullable=False, index=True)
    entity_id = Column(String, nullable=False)
    portfolio = Column(String, nullable=False, index=True)
    groups = Column(String)
    last_updated = Column(Date, default=datetime.date.today, nullable=False)

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
