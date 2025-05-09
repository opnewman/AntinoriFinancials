import os
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Set up logging
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/nori")

# Adjust the connection parameters to address SSL connection issues
connection_args = {
    "pool_recycle": 60,  # Recycle connections every 60 seconds
    "pool_timeout": 30,  # 30 second timeout on pool checkout
    "pool_size": 5,      # Connection pool size
    "max_overflow": 10,  # Max overflow connections
    "pool_pre_ping": True,  # Test connections before using them
    "connect_args": {
        "connect_timeout": 10,  # 10 second connection timeout
        "keepalives": 1,        # Enable keepalives
        "keepalives_idle": 30,  # Idle time before sending keepalive
        "keepalives_interval": 10,  # Interval between keepalives
        "keepalives_count": 5,  # Max number of keepalives
        "sslmode": os.environ.get("PGSSLMODE", "prefer")  # SSL mode (prefer over require)
    }
}

# Create database engine and session
engine = create_engine(DATABASE_URL, **connection_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Session dependency for FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Session context manager for Flask
@contextmanager
def get_db_connection():
    """
    Context manager for database session to ensure proper cleanup.
    Usage:
        with get_db_connection() as db:
            # use db for database operations
    """
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        db.close()

# Initialize database
def init_db():
    # Import models to register them with SQLAlchemy
    from src.models.models import FinancialPosition, FinancialSummary
    from src.models.models import RiskStatisticEquity, RiskStatisticFixedIncome, RiskStatisticAlternatives
    from src.models.models import OwnershipMetadata, OwnershipItem
    from src.models.models import RiskStatsJob, EgnyteRiskStat
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created")