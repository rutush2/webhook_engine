import concurrent.futures
import logging
import signal
import sys
import time
from typing import List

from circuit_breaker import CircuitBreaker
from db.models import init_db
from delivery_worker import DeliveryWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Orchestrator")


class EngineOrchestrator:
    def __init__(
        self,
        db_url: str = "sqlite:///outbox_engine.db",
        num_workers: int = 2,
        poll_interval_seconds: float = 2.0,
        batch_size_per_worker: int = 5,
    ):
        self.db_url = db_url
        self.num_workers = num_workers
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size_per_worker = batch_size_per_worker

        self.running = True
        self.session_factory = init_db(self.db_url)
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout_seconds=10.0)

        self.workers: List[DeliveryWorker] = [
            DeliveryWorker(
                session_factory=self.session_factory,
                circuit_breaker=self.circuit_breaker,
                worker_id=f"worker-node-{i+1}",
            )
            for i in range(self.num_workers)
        ]

        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        signal_name = signal.Signals(signum).name
        logger.warning(
            f"Received signal {signal_name} ({signum}). Initiating shutdown..."
        )
        self.running = False

    def _run_worker_cycle(self, worker: DeliveryWorker) -> int:
        try:
            return worker.run_once(batch_size=self.batch_size_per_worker)
        except Exception as e:
            logger.error(f"Uncaught error in worker [{worker.worker_id}]: {str(e)}")
            return 0

    def start(self):
        logger.info(
            f"Starting Engine Orchestrator with {self.num_workers} worker thread(s)..."
        )
        logger.info("Press Ctrl+C to shutdown.\n")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_workers, thread_name_prefix="DeliveryWorker"
        ) as executor:
            while self.running:
                futures = [
                    executor.submit(self._run_worker_cycle, worker)
                    for worker in self.workers
                ]

                results = [
                    f.result() for f in concurrent.futures.as_completed(futures)
                ]
                total_processed = sum(results)

                if total_processed > 0:
                    logger.info(
                        f"Processed {total_processed} event(s) across {self.num_workers} worker(s)."
                    )

                time.sleep(self.poll_interval_seconds)

        logger.info("Orchestrator stopped cleanly. All worker threads shut down.")


if __name__ == "__main__":
    orchestrator = EngineOrchestrator(
        db_url="sqlite:///outbox_engine.db",
        num_workers=2,
        poll_interval_seconds=1.0,
        batch_size_per_worker=5,
    )
    orchestrator.start()