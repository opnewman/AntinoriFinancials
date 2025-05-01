import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

# Set up logging
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/antinori")

# Create database engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Session dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize database
def init_db():
    # Import models to register them with SQLAlchemy
    from src.models.models import FinancialPosition, FinancialSummary
    from src.models.models import RiskStatisticEquity, RiskStatisticFixedIncome, RiskStatisticAlternatives
    from src.models.models import OwnershipMetadata, OwnershipItem
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created")