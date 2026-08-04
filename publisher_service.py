import logging
from typing import Any, Dict, Optional
from sqlalchemy.orm import Session, sessionmaker

from db.models import OutboxEvent, EventStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PublisherService")


class PublisherService:
    def __init__(self, session_factory: sessionmaker):
        self.session_factory = session_factory

    def publish_event(
        self,
        session: Session,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        target_url: str,
        payload: Dict[str, Any],
        max_retries: int = 5,
    ) -> OutboxEvent:
        outbox_event = OutboxEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            target_url=target_url,
            payload=payload,
            status=EventStatus.PENDING,
            max_retries=max_retries,
        )
        session.add(outbox_event)
        logger.info(
            f"Staged outbox event '{event_type}' for aggregate '{aggregate_type}:{aggregate_id}'."
        )
        return outbox_event

    def execute_transactional_action(
        self,
        domain_action_func,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        target_url: str,
        payload: Dict[str, Any],
    ) -> Any:
        session: Session = self.session_factory()
        try:
            result = domain_action_func(session)

            self.publish_event(
                session=session,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                event_type=event_type,
                target_url=target_url,
                payload=payload,
            )

            session.commit()
            logger.info("Transaction committed successfully.")
            return result

        except Exception as e:
            session.rollback()
            logger.error(f"Transaction failed and rolled back: {str(e)}")
            raise e
        finally:
            session.close()


if __name__ == "__main__":
    from models import init_db

    SessionLocal = init_db("sqlite:///outbox_engine.db")
    publisher = PublisherService(session_factory=SessionLocal)

    def create_order_logic(session: Session):
        print("Executing domain logic: Creating order entity in DB...")
        return {"order_id": "ord_1001", "status": "created"}

    result = publisher.execute_transactional_action(
        domain_action_func=create_order_logic,
        aggregate_type="order",
        aggregate_id="ord_1001",
        event_type="order.created",
        target_url="https://api.partner.com/webhooks",
        payload={"order_id": "ord_1001", "total": 99.99},
    )

    print("Execution Result:", result)