"""
Database models for tracking pipeline executions.
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, Boolean, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
import enum

Base = declarative_base()


class RunStatus(enum.Enum):
    """Status of a pipeline or task run."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class PipelineRun(Base):
    """Tracks a complete pipeline execution."""
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_name = Column(String, nullable=False, index=True)
    tenant = Column(String, nullable=True, index=True)
    status = Column(SQLEnum(RunStatus),
                    default=RunStatus.PENDING, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    trigger_type = Column(String, default="manual")  # manual, schedule, api
    triggered_by = Column(String, nullable=True)
    retry_of = Column(Integer, ForeignKey("pipeline_runs.id"), nullable=True)
    config = Column(JSON, default=dict)  # Pipeline config snapshot
    metadata_ = Column("metadata", JSON, default=dict)  # Additional metadata

    # Relationships
    task_runs = relationship(
        "TaskRun", back_populates="pipeline_run", cascade="all, delete-orphan")
    retry = relationship("PipelineRun", remote_side=[id])


class TaskRun(Base):
    """Tracks individual task/step execution."""
    __tablename__ = "task_runs"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_run_id = Column(Integer, ForeignKey(
        "pipeline_runs.id"), nullable=False, index=True)
    task_name = Column(String, nullable=False)
    status = Column(SQLEnum(RunStatus),
                    default=RunStatus.PENDING, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    attempt = Column(Integer, default=1)
    error_message = Column(Text, nullable=True)
    result = Column(JSON, nullable=True)  # Task result data
    logs = Column(Text, nullable=True)  # Captured logs

    # Relationships
    pipeline_run = relationship("PipelineRun", back_populates="task_runs")


class LogEntry(Base):
    """Structured log entries for debugging."""
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow,
                       nullable=False, index=True)
    level = Column(String, nullable=False)  # DEBUG, INFO, WARNING, ERROR
    logger = Column(String, nullable=True)
    message = Column(Text, nullable=False)
    pipeline_run_id = Column(Integer, ForeignKey(
        "pipeline_runs.id"), nullable=True, index=True)
    task_run_id = Column(Integer, ForeignKey(
        "task_runs.id"), nullable=True, index=True)
    extra = Column(JSON, default=dict)  # Additional context
