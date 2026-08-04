
---
```markdown
# 🌐 Webhook Outbox Engine

A resilient, event-driven Webhook Engine implementing the **Transactional Outbox Pattern** in Python. Built with SQLAlchemy, FastAPI (Mock Receiver), multi-threaded delivery workers, exponential backoff retries, circuit breaker fault tolerance, and a real-time Streamlit telemetry dashboard.

---

## 🛠️ Key Features

- **Transactional Outbox Pattern:** Guarantees atomicity by staging domain events inside the database within the same database transaction.
- **Resilient Worker & Retry Logic:** Concurrent delivery workers handle event payloads with exponential backoff and jitter.
- **Circuit Breaker Pattern:** Protects downstream endpoints by tracking failure rates and breaking execution cycles to failing domains.
- **Dead-Letter Queue (DLQ):** Captures non-retryable 4xx HTTP responses or max-retried failures for manual auditing.
- **Real-Time Telemetry Dashboard:** Streamlit UI for monitoring outbox event states, reviewing attempt audit logs, manual event ingestion, worker triggering, and DLQ re-queueing.
- **Mock HTTP Receiver:** Fast-API based mock endpoint simulating real-world network edge cases (200, 400, 500, timeouts, rate-limits).

---

## 🏗️ Project Architecture


```text

webhook_engine/
│
├── db/
│   ├── models.py             # SQLAlchemy models (OutboxEvent, DeliveryAttempt)
├── publisher_service.py      # Executes domain operations and stages outbox events
├── delivery_worker.py        # Worker logic for picking up PENDING events and sending HTTP webhooks
├── circuit_breaker.py        # Circuit breaker implementation per domain
├── mock_receiver.py          # FastAPI server simulating endpoint responses
├── dashboard.py               # Streamlit telemetry and management dashboard
└── requirements.txt          # Python dependencies 

```

---

## 🚀 Getting Started

### 1. Prerequisites & Installation

Ensure you have Python 3.10+ installed. Clone the repository and install required packages:

```bash
git clone [https://github.com/your-username/webhook-outbox-engine.git](https://github.com/your-username/webhook-outbox-engine.git)
cd webhook-outbox-engine
pip install -r requirements.txt

```

### 2. Run the Mock HTTP Endpoint Receiver

Start the FastAPI mock server to simulate destination webhook receivers:

```bash
python mock_receiver.py

```

*Server will start listening at `http://127.0.0.1:8080`.*

### 3. Run the Streamlit Management Dashboard

Launch the real-time operational dashboard:

```bash
streamlit run dashboard.py

```

*Access the UI in your browser at `http://localhost:8501`.*

---

## ⚙️ Dashboard Operations

Through the Streamlit UI, you can:

1. **Stage Events:** Dispatch synthetic events to various endpoints (`/webhook/success`, `/webhook/error-500`, `/webhook/error-400`, `/webhook/timeout`).
2. **Execute Worker Cycles:** Click **⚡ Run Worker Batch Now** to process staged events on demand.
3. **Reprocess DLQ Events:** Navigate to the **❌ Dead-Letter Queue (DLQ)** tab to re-queue failed events back to `PENDING` state.
4. **Reset Circuit Breaker:** Reset open circuit breaker states for destination domains directly from the controls panel.

```

---

