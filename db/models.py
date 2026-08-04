import enum
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class EventStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    RETRYING = "RETRYING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    aggregate_type = Column(String(64), nullable=False)
    aggregate_id = Column(String(64), nullable=False)
    event_type = Column(String(64), nullable=False)
    target_url = Column(Text, nullable=False)
    payload = Column(JSON, nullable=False)

    status = Column(Enum(EventStatus), nullable=False, default=EventStatus.PENDING)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=5)

    next_retry_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by = Column(String(64), nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    delivered_at = Column(DateTime(timezone=True), nullable=True)

    attempts = relationship(
        "DeliveryAttempt", back_populates="event", cascade="all, delete-orphan"
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    event_id = Column(
        String(36), ForeignKey("outbox_events.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number = Column(Integer, nullable=False)

    response_status_code = Column(Integer, nullable=True)
    execution_duration_ms = Column(Integer, nullable=False)
    error_message = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    event = relationship("OutboxEvent", back_populates="attempts")


Index(
    "idx_outbox_processing",
    OutboxEvent.status,
    OutboxEvent.next_retry_at,
)


def init_db(db_url: str = "sqlite:///outbox_engine.db"):
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


if __name__ == "__main__":
    SessionLocal = init_db()
    print("Database tables created successfully.")