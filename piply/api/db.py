"""
Database session management.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from .database import Base
import os

# Use SQLite for simplicity (can be configured via environment)
DATABASE_URL = os.getenv("PIPLY_DATABASE_URL", "sqlite:///./piply.db")

# For SQLite, need connect_args
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith(
    "sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Session:
    """
    Dependency for FastAPI to get database session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables."""
    Base.metadata.create_all(bind=engine)


def drop_all_tables():
    """Drop all tables (for testing)."""
    Base.metadata.drop_all(bind=engine)
