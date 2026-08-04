import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging
import time
from sqlalchemy.orm import Session

from db.models import init_db, OutboxEvent, DeliveryAttempt, EventStatus
from circuit_breaker import CircuitBreaker
from delivery_worker import DeliveryWorker
from publisher_service import PublisherService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TestRunner")


def run_mock_integration_test():
    logger.info("Initializing SQLite database...")
    session_factory = init_db("sqlite:///outbox_engine.db")

    circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout_seconds=5.0)
    publisher = PublisherService(session_factory=session_factory)
    worker = DeliveryWorker(
        session_factory=session_factory,
        circuit_breaker=circuit_breaker,
        worker_id="test-worker-1",
        base_backoff_seconds=1,
        request_timeout_seconds=3.0,
    )

    logger.info("Staging test events targeting local mock receiver endpoints...")

    def dummy_action(session: Session):
        return True

    publisher.execute_transactional_action(
        domain_action_func=dummy_action,
        aggregate_type="order",
        aggregate_id="ord_101",
        event_type="order.created",
        target_url="http://127.0.0.1:8080/webhook/success",
        payload={"order_id": "ord_101", "status": "COMPLETED"},
    )

    publisher.execute_transactional_action(
        domain_action_func=dummy_action,
        aggregate_type="order",
        aggregate_id="ord_102",
        event_type="order.bad_request",
        target_url="http://127.0.0.1:8080/webhook/error-400",
        payload={"invalid_field": None},
    )

    publisher.execute_transactional_action(
        domain_action_func=dummy_action,
        aggregate_type="order",
        aggregate_id="ord_103",
        event_type="order.server_error",
        target_url="http://127.0.0.1:8080/webhook/error-500",
        payload={"order_id": "ord_103"},
    )

    publisher.execute_transactional_action(
        domain_action_func=dummy_action,
        aggregate_type="order",
        aggregate_id="ord_104",
        event_type="order.timeout",
        target_url="http://127.0.0.1:8080/webhook/timeout",
        payload={"order_id": "ord_104"},
    )

    logger.info("Running worker batch execution...")
    processed_count = worker.run_once(batch_size=10)
    logger.info(f"Worker finished processing {processed_count} event(s).\n")

    session = session_factory()
    logger.info("=== OUTBOX EVENTS SUMMARY ===")
    events = session.query(OutboxEvent).all()
    for ev in events:
        logger.info(
            f"ID: {ev.id[:8]} | Type: {ev.event_type:<20} | Status: {ev.status.value:<10} | Retries: {ev.retry_count}"
        )

    logger.info("\n=== DELIVERY ATTEMPTS LOG ===")
    attempts = session.query(DeliveryAttempt).all()
    for att in attempts:
        logger.info(
            f"Attempt ID: {att.id[:8]} | Event: {att.event_id[:8]} | Status Code: {att.response_status_code} | Duration: {att.execution_duration_ms}ms | Error: {att.error_message}"
        )
    session.close()


if __name__ == "__main__":
    run_mock_integration_test()