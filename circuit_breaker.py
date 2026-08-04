import logging
import time
from enum import Enum
from typing import Dict
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CircuitBreaker")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:


    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds

        self._state: Dict[str, CircuitState] = {}
        self._failure_counts: Dict[str, int] = {}
        self._last_state_change: Dict[str, float] = {}

    def extract_host(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def can_execute(self, url: str) -> bool:
        host = self.extract_host(url)
        current_state = self._state.get(host, CircuitState.CLOSED)

        if current_state == CircuitState.CLOSED:
            return True

        now = time.time()
        last_change = self._last_state_change.get(host, 0.0)

        if current_state == CircuitState.OPEN:
            if now - last_change >= self.recovery_timeout_seconds:
                self._state[host] = CircuitState.HALF_OPEN
                self._last_state_change[host] = now
                logger.info(f"[{host}] Circuit switched to HALF_OPEN. Allowing trial request.")
                return True
            return False

        if current_state == CircuitState.HALF_OPEN:
            return True

        return True

    def record_success(self, url: str) -> None:
        host = self.extract_host(url)
        previous_state = self._state.get(host, CircuitState.CLOSED)

        self._failure_counts[host] = 0
        self._state[host] = CircuitState.CLOSED
        self._last_state_change[host] = time.time()

        if previous_state != CircuitState.CLOSED:
            logger.info(f"[{host}] Host recovered. Circuit reset to CLOSED.")

    def record_failure(self, url: str) -> None:
        host = self.extract_host(url)
        current_state = self._state.get(host, CircuitState.CLOSED)

        failures = self._failure_counts.get(host, 0) + 1
        self._failure_counts[host] = failures

        if current_state == CircuitState.HALF_OPEN:
            self._state[host] = CircuitState.OPEN
            self._last_state_change[host] = time.time()
            logger.warning(f"[{host}] Trial request failed in HALF_OPEN state. Circuit set to OPEN.")
            return

        if failures >= self.failure_threshold:
            self._state[host] = CircuitState.OPEN
            self._last_state_change[host] = time.time()
            logger.warning(
                f"[{host}] Failure threshold ({failures}/{self.failure_threshold}) reached. Circuit TRIPPED to OPEN."
            )

    def get_status(self, url: str) -> Dict[str, object]:
        host = self.extract_host(url)
        return {
            "host": host,
            "state": self._state.get(host, CircuitState.CLOSED),
            "failures": self._failure_counts.get(host, 0),
        }


if __name__ == "__main__":
    target = "https://api.failing-client.com/webhooks"
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout_seconds=2.0)

    print("Initial check:", cb.can_execute(target))  

    cb.record_failure(target)
    cb.record_failure(target)
    cb.record_failure(target)

    print("After failures check:", cb.can_execute(target))

    time.sleep(2.1)
    print("After recovery timeout check:", cb.can_execute(target))

    cb.record_success(target)
    print("Final status:", cb.get_status(target))