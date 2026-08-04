import time
from enum import Enum
from typing import Dict


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class DomainState:
    def __init__(self):
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0


class CircuitBreaker:
    def __init__(
        self, failure_threshold: int = 3, recovery_timeout_seconds: float = 10.0
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.domains: Dict[str, DomainState] = {}

    def _get_domain_state(self, domain: str) -> DomainState:
        if domain not in self.domains:
            self.domains[domain] = DomainState()
        return self.domains[domain]

    def can_execute(self, domain: str) -> bool:
        ds = self._get_domain_state(domain)
        now = time.time()

        if ds.state == CircuitState.OPEN:
            if now - ds.last_failure_time >= self.recovery_timeout_seconds:
                ds.state = CircuitState.HALF_OPEN
                return True
            return False

        return True

    def record_success(self, domain: str):
        ds = self._get_domain_state(domain)
        ds.failure_count = 0
        ds.state = CircuitState.CLOSED

    def record_failure(self, domain: str):
        ds = self._get_domain_state(domain)
        ds.failure_count += 1
        ds.last_failure_time = time.time()

        if ds.failure_count >= self.failure_threshold:
            ds.state = CircuitState.OPEN

    def reset(self, domain: str):
        if domain in self.domains:
            ds = self.domains[domain]
            ds.state = CircuitState.CLOSED
            ds.failure_count = 0
            ds.last_failure_time = 0.0