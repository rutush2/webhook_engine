import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
import urllib.request
import urllib.error
import json

from sqlalchemy.orm import Session, sessionmaker

from db.models import OutboxEvent, DeliveryAttempt, EventStatus
from publisher.poller import WorkerPoller
from circuit_breaker import CircuitBreaker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeliveryWorker")


class DeliveryWorker:
    def __init__(
        self,
        session_factory: sessionmaker,
        circuit_breaker: CircuitBreaker,
        worker_id: Optional[str] = None,
        base_backoff_seconds: int = 2,
        request_timeout_seconds: float = 5.0,
    ):
        self.session_factory = session_factory
        self.circuit_breaker = circuit_breaker
        self.poller = WorkerPoller(session_factory, worker_id)
        self.worker_id = self.poller.worker_id
        self.base_backoff_seconds = base_backoff_seconds
        self.request_timeout_seconds = request_timeout_seconds

    def calculate_next_retry(self, retry_count: int) -> datetime:
        delay_seconds = self.base_backoff_seconds * (2 ** retry_count)
        return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

    def send_http_request(self, target_url: str, payload: dict) -> tuple[Optional[int], int, Optional[str]]:
        start_time = time.time()
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            target_url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "WebhookEngineWorker/1.0"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout_seconds) as response:
                duration_ms = int((time.time() - start_time) * 1000)
                return response.status, duration_ms, None
        except urllib.error.HTTPError as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return e.code, duration_ms, f"HTTP Error {e.code}: {e.reason}"
        except urllib.error.URLError as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return None, duration_ms, f"Network Error: {e.reason}"
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return None, duration_ms, f"Unexpected Error: {str(e)}"

    def process_event(self, event_id: str) -> None:
        session: Session = self.session_factory()
        event = session.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()

        if not event:
            session.close()
            return

        if not self.circuit_breaker.can_execute(event.target_url):
            logger.warning(
                f"[{self.worker_id}] Circuit OPEN for {event.target_url}. Releasing lock and skipping event {event.id}."
            )
            event.status = EventStatus.RETRYING
            event.locked_at = None
            event.locked_by = None
            session.commit()
            session.close()
            return

        attempt_number = event.retry_count + 1
        status_code, duration_ms, error_msg = self.send_http_request(event.target_url, event.payload)

        attempt_record = DeliveryAttempt(
            event_id=event.id,
            attempt_number=attempt_number,
            response_status_code=status_code,
            execution_duration_ms=duration_ms,
            error_message=error_msg,
        )
        session.add(attempt_record)

        now = datetime.now(timezone.utc)

        if status_code is not None and 200 <= status_code < 300:
            event.status = EventStatus.DELIVERED
            event.delivered_at = now
            event.locked_at = None
            event.locked_by = None
            self.circuit_breaker.record_success(event.target_url)
            logger.info(f"[{self.worker_id}] Event {event.id} successfully DELIVERED to {event.target_url}.")

        elif status_code is not None and 400 <= status_code < 500 and status_code != 429:
            event.status = EventStatus.FAILED
            event.locked_at = None
            event.locked_by = None
            self.circuit_breaker.record_failure(event.target_url)
            logger.error(
                f"[{self.worker_id}] Event {event.id} FAILED with client error {status_code}. No further retries."
            )

        else:
            event.retry_count += 1
            self.circuit_breaker.record_failure(event.target_url)

            if event.retry_count >= event.max_retries:
                event.status = EventStatus.FAILED
                event.locked_at = None
                event.locked_by = None
                logger.error(
                    f"[{self.worker_id}] Event {event.id} reached MAX RETRIES ({event.max_retries}). Marked FAILED."
                )
            else:
                event.status = EventStatus.RETRYING
                event.next_retry_at = self.calculate_next_retry(event.retry_count)
                event.locked_at = None
                event.locked_by = None
                logger.warning(
                    f"[{self.worker_id}] Event {event.id} failed attempt {event.retry_count}. Scheduled retry at {event.next_retry_at}."
                )

        session.commit()
        session.close()

    def run_once(self, batch_size: int = 10) -> int:
        batch = self.poller.fetch_and_lock_batch(batch_size=batch_size)
        if not batch:
            return 0

        for event in batch:
            self.process_event(event.id)

        return len(batch)