import uuid
import logging
from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import or_, and_
from sqlalchemy.orm import Session, sessionmaker

from db.models import OutboxEvent, EventStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WorkerPoller")


class WorkerPoller:
    def __init__(self, session_factory: sessionmaker, worker_id: Optional[str] = None):
        self.session_factory = session_factory
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"

    def fetch_and_lock_batch(self, batch_size: int = 10) -> List[OutboxEvent]:
        session: Session = self.session_factory()
        now = datetime.now(timezone.utc)

        try:
            query = (
                session.query(OutboxEvent)
                .filter(
                    or_(
                        OutboxEvent.status == EventStatus.PENDING,
                        OutboxEvent.status == EventStatus.RETRYING,
                    ),
                    OutboxEvent.next_retry_at <= now,
                    OutboxEvent.locked_at.is_(None),
                )
                .order_by(OutboxEvent.next_retry_at.asc())
                .limit(batch_size)
            )

            try:
                query = query.with_for_update(skip_locked=True)
            except Exception:
                pass

            candidate_events = query.all()

            if not candidate_events:
                session.close()
                return []

            locked_event_ids = []
            for event in candidate_events:
                event.status = EventStatus.PROCESSING
                event.locked_at = now
                event.locked_by = self.worker_id
                locked_event_ids.append(event.id)

            session.commit()
            logger.info(
                f"[{self.worker_id}] Successfully locked {len(locked_event_ids)} event(s) for delivery."
            )

            locked_events = (
                session.query(OutboxEvent)
                .filter(OutboxEvent.id.in_(locked_event_ids))
                .all()
            )
            session.close()
            return locked_events

        except Exception as e:
            session.rollback()
            session.close()
            logger.error(f"[{self.worker_id}] Error during batch polling: {str(e)}")
            return []


if __name__ == "__main__":
    from models import init_db, OutboxEvent, EventStatus
    import json

    SessionLocal = init_db("sqlite:///outbox_engine.db")

    session = SessionLocal()
    sample_event = OutboxEvent(
        aggregate_type="order",
        aggregate_id="ord_9901",
        event_type="order.created",
        target_url="https://api.example.com/webhooks",
        payload={"order_id": "ord_9901", "amount": 149.99},
        status=EventStatus.PENDING,
    )
    session.add(sample_event)
    session.commit()
    session.close()

    poller = WorkerPoller(session_factory=SessionLocal, worker_id="worker-node-1")
    locked_batch = poller.fetch_and_lock_batch(batch_size=5)

    print(f"\nPolled batch size: {len(locked_batch)}")
    for ev in locked_batch:
        print(f"ID: {ev.id} | Status: {ev.status} | Locked By: {ev.locked_by}")