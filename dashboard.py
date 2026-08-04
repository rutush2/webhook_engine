import json
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from circuit_breaker import CircuitBreaker, CircuitState
from db.models import DeliveryAttempt, EventStatus, OutboxEvent, init_db
from delivery_worker import DeliveryWorker
from publisher_service import PublisherService

st.set_page_config(
    page_title="Webhook Outbox Engine Monitor",
    page_icon="🌐",
    layout="wide",
)

st.title("🌐 Webhook Outbox Engine Monitor")
st.caption("Real-Time Telemetry & Operational Control Dashboard")

session_factory = init_db("sqlite:///outbox_engine.db")

if "circuit_breaker" not in st.session_state:
    st.session_state.circuit_breaker = CircuitBreaker(
        failure_threshold=3, recovery_timeout_seconds=5.0
    )


def load_data():
    session: Session = session_factory()

    events = session.query(OutboxEvent).order_by(OutboxEvent.created_at.desc()).all()
    attempts = session.query(DeliveryAttempt).order_by(DeliveryAttempt.created_at.desc()).all()

    events_data = []
    for ev in events:
        payload_str = json.dumps(ev.payload) if isinstance(ev.payload, dict) else str(ev.payload)
        events_data.append(
            {
                "ID": ev.id[:8],
                "Full_ID": ev.id,
                "Aggregate": f"{ev.aggregate_type}:{ev.aggregate_id}",
                "Type": ev.event_type,
                "Payload": payload_str,
                "Target URL": ev.target_url,
                "Status": ev.status.value,
                "Retries": f"{ev.retry_count}/{ev.max_retries}",
                "Next Retry": str(ev.next_retry_at),
                "Locked By": ev.locked_by if ev.locked_by else "",
                "Created At": str(ev.created_at),
                "Delivered At": str(ev.delivered_at) if ev.delivered_at else "",
            }
        )

    attempts_data = [
        {
            "Attempt ID": att.id[:8],
            "Event ID": att.event_id[:8],
            "Attempt #": att.attempt_number,
            "Status Code": str(att.response_status_code) if att.response_status_code is not None else "",
            "Duration (ms)": att.execution_duration_ms,
            "Error Message": att.error_message if att.error_message else "",
            "Timestamp": str(att.created_at),
        }
        for att in attempts
    ]

    session.close()
    return pd.DataFrame(events_data), pd.DataFrame(attempts_data)


col_left, col_right = st.columns([2, 1])

with col_left:
    with st.expander("➕ **Dispatch New Event to Outbox Engine**", expanded=False):
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            aggregate_type = st.text_input("Aggregate Type", value="order", key="agg_type_input")
            aggregate_id = st.text_input("Aggregate ID", value="ord_300", key="agg_id_input")

        with col_b:
            event_type = st.selectbox("Event Type", ["order.created", "order.updated", "order.canceled"], key="evt_type_select")
            target_url = st.selectbox(
                "Target Endpoint",
                [
                    "http://127.0.0.1:8080/webhook/success",
                    "http://127.0.0.1:8080/webhook/error-500",
                    "http://127.0.0.1:8080/webhook/error-400",
                    "http://127.0.0.1:8080/webhook/rate-limit",
                    "http://127.0.0.1:8080/webhook/timeout",
                    "http://127.0.0.1:8080/webhook/flaky",
                ],
                key="url_select",
            )

        with col_c:
            payload_raw = st.text_area("Payload (JSON)", value='{"order_id": "test_cmd", "amount": 250.00}', height=90, key="payload_input")

        if st.button("🚀 Stage Event Now", use_container_width=True):
            try:
                payload = json.loads(payload_raw)
                publisher = PublisherService(session_factory=session_factory)

                def dummy_action(session: Session):
                    return True

                publisher.execute_transactional_action(
                    domain_action_func=dummy_action,
                    aggregate_type=aggregate_type,
                    aggregate_id=aggregate_id,
                    event_type=event_type,
                    target_url=target_url,
                    payload=payload,
                )
                st.toast(f"Event '{event_type}' staged successfully!", icon="✅")
                st.rerun()
            except Exception as e:
                st.error(f"Error creating event: {str(e)}")

with col_right:
    with st.expander("⚙️ **Engine Execution Controls**", expanded=True):
        batch_size = st.number_input("Batch Size", min_value=1, max_value=50, value=5)

        if st.button("⚡ Run Worker Batch Now", use_container_width=True):
            worker = DeliveryWorker(
                session_factory=session_factory,
                circuit_breaker=st.session_state.circuit_breaker,
                worker_id="dashboard-worker",
                request_timeout_seconds=3.0,
            )
            processed = worker.run_once(batch_size=batch_size)
            st.toast(f"Worker executed. Processed {processed} event(s).", icon="⚡")
            st.rerun()

        st.divider()

        domain_to_reset = st.text_input("Reset Domain Circuit Breaker", value="127.0.0.1:8080")
        if st.button("🔌 Reset Circuit Breaker", use_container_width=True):
            cb = st.session_state.circuit_breaker
            if hasattr(cb, "reset"):
                cb.reset(domain_to_reset)
            elif hasattr(cb, "domains") and domain_to_reset in cb.domains:
                ds = cb.domains[domain_to_reset]
                ds.state = CircuitState.CLOSED
                ds.failure_count = 0
                ds.last_failure_time = 0.0
            else:
                st.session_state.circuit_breaker = CircuitBreaker(
                    failure_threshold=3, recovery_timeout_seconds=5.0
                )
            st.toast(f"Circuit breaker reset for domain '{domain_to_reset}'", icon="🔌")

st.divider()

events_df, attempts_df = load_data()

st.subheader("System Telemetry Overview")
col1, col2, col3, col4, col5 = st.columns(5)

total_events = len(events_df)
pending_count = len(events_df[events_df["Status"] == EventStatus.PENDING.value]) if not events_df.empty else 0
processing_count = len(events_df[events_df["Status"] == EventStatus.PROCESSING.value]) if not events_df.empty else 0
delivered_count = len(events_df[events_df["Status"] == EventStatus.DELIVERED.value]) if not events_df.empty else 0
failed_count = len(events_df[events_df["Status"] == EventStatus.FAILED.value]) if not events_df.empty else 0

col1.metric("Total Events", total_events)
col2.metric("Pending", pending_count)
col3.metric("Processing", processing_count)
col4.metric("Delivered", delivered_count)
col5.metric("Failed", failed_count)

st.divider()

tab1, tab2, tab3 = st.tabs(["📋 Outbox Events", "❌ Dead-Letter Queue (DLQ)", "📜 Delivery Attempts Audit Log"])

with tab1:
    st.subheader("Outbox Event Records")
    if not events_df.empty:
        status_filter = st.multiselect(
            "Filter by Event Status",
            options=list(events_df["Status"].unique()),
            default=list(events_df["Status"].unique()),
        )
        display_df = events_df[events_df["Status"].isin(status_filter)].drop(columns=["Full_ID"])
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No outbox events in database.")

with tab2:
    st.subheader("Dead-Letter Queue & Failed Events")

    failed_events = events_df[events_df["Status"] == EventStatus.FAILED.value]

    if not failed_events.empty:
        st.dataframe(failed_events.drop(columns=["Full_ID"]), use_container_width=True)

        st.subheader("🔄 Reprocess Failed Event")
        event_options = {row["ID"]: row["Full_ID"] for _, row in failed_events.iterrows()}
        selected_short_id = st.selectbox("Select Event ID to Re-queue", list(event_options.keys()))

        if st.button("♻️ Re-queue Event to PENDING"):
            target_id = event_options[selected_short_id]
            session: Session = session_factory()
            ev = session.query(OutboxEvent).filter_by(id=target_id).first()
            if ev:
                ev.status = EventStatus.PENDING
                ev.retry_count = 0
                ev.locked_at = None
                ev.locked_by = None
                ev.next_retry_at = datetime.now(timezone.utc)
                session.commit()
                session.close()
                st.toast(f"Event '{selected_short_id}' successfully re-queued!", icon="♻️")
                st.rerun()
            else:
                session.close()
                st.error("Event not found.")
    else:
        st.info("No failed events currently in Dead-Letter Queue.")

with tab3:
    st.subheader("Delivery Attempt Logs")
    if not attempts_df.empty:
        st.dataframe(attempts_df, use_container_width=True)
    else:
        st.info("No delivery attempts recorded yet.")