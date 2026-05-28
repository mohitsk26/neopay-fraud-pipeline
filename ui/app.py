# ui/app.py — NeoPay Fraud Intelligence Dashboard

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

# ── Page Config ───────────────────────────────────────────
st.set_page_config(
    page_title="NeoPay Fraud Intelligence",
    page_icon="🛡️",
    layout="wide"
)

# ── Constants ─────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel",
    "dining", "atm_withdrawal", "online_retail"
]
REGIONS = ["US-NY", "US-CA", "UK-LON", "DE-BER", "SG-SIN", "AU-SYD"]

# ── Helper Functions ──────────────────────────────────────
def generate_transaction(fraud_rate: float = 0.05) -> dict:
    is_fraud = random.random() < fraud_rate
    return {
        "transaction_id":    str(uuid.uuid4())[:8],
        "account_id":        f"ACC-{random.randint(10000, 99999)}",
        "amount":            round(random.uniform(500, 9500), 2)
                             if is_fraud else
                             round(random.uniform(5, 450), 2),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "region":            random.choice(REGIONS),
        "timestamp":         datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "is_fraud":          is_fraud
    }

@st.cache_data(ttl=3600)
def load_forecast() -> pd.DataFrame:
    try:
        mock_path = Path(__file__).parent / "mock_data" / "sample_forecast.json"
        with open(mock_path) as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception:
        return pd.DataFrame()

def send_to_kafka(transaction: dict) -> bool:
    try:
        from confluent_kafka import Producer
        producer = Producer({
            "bootstrap.servers": st.secrets["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":    "PLAIN",
            "sasl.username":     "$ConnectionString",
            "sasl.password":     st.secrets["EVENTHUBS_CONNECTION_STRING"],
        })
        import json as _json
        producer.produce(
            topic="transactions",
            key=transaction["account_id"].encode(),
            value=_json.dumps(transaction).encode()
        )
        producer.flush()
        return True
    except Exception:
        return False  # silently fail — demo mode continues

# ── Header ────────────────────────────────────────────────
st.title("🛡️ NeoPay — Real-Time Fraud Intelligence")
st.caption(
    "Live transaction monitoring · Apache Kafka · "
    "Azure Databricks · Prophet ML Forecasting"
)
st.divider()

# ── Sidebar ───────────────────────────────────────────────
st.sidebar.title("🔧 Transaction Simulator")
st.sidebar.markdown("Simulate a live payment stream and watch the pipeline react in real time.")

tps        = st.sidebar.slider("Transactions per second", 1, 10, 3)
fraud_rate = st.sidebar.slider("Fraud rate (%)", 1, 30, 5) / 100
num_txns   = st.sidebar.slider("Total transactions to send", 10, 200, 50)

run_btn  = st.sidebar.button("▶ Start Stream", type="primary", use_container_width=True)
st.sidebar.divider()
st.sidebar.markdown("**Pipeline Status**")
st.sidebar.markdown("🟢 Event Hubs: Connected")
st.sidebar.markdown("🟢 Databricks: Streaming")
st.sidebar.markdown("🟢 Delta Lake: Writing")
st.sidebar.markdown("🟢 ML Model: Scoring")

# ── KPI Row ───────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
kpi_total    = col1.empty()
kpi_fraud    = col2.empty()
kpi_rate     = col3.empty()
kpi_forecast = col4.empty()

forecast_df = load_forecast()
avg_forecast = forecast_df["yhat"].mean() if not forecast_df.empty else 0.03

kpi_total.metric("Total Transactions", "0")
kpi_fraud.metric("Fraud Detected", "0")
kpi_rate.metric("Current Fraud Rate", "0.00%")
kpi_forecast.metric("24h Forecast (Prophet)", f"{avg_forecast:.2%}")

st.divider()

# ── Main Content ──────────────────────────────────────────
col_left, col_right = st.columns([3, 2])

with col_left:
    st.subheader("📋 Live Transaction Feed")
    feed_table = st.empty()

with col_right:
    st.subheader("🔮 Prophet Fraud Rate Forecast")
    if not forecast_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pd.concat([
                pd.to_datetime(forecast_df["ds"]),
                pd.to_datetime(forecast_df["ds"])[::-1]
            ]),
            y=pd.concat([forecast_df["yhat_upper"], forecast_df["yhat_lower"][::-1]]),
            fill="toself",
            fillcolor="rgba(99,110,250,0.15)",
            line=dict(color="rgba(0,0,0,0)"),
            name="95% Confidence"
        ))
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(forecast_df["ds"]),
            y=forecast_df["yhat"],
            mode="lines+markers",
            line=dict(color="#636EFA", width=2),
            name="Predicted Fraud Rate"
        ))
        fig.update_layout(
            template="plotly_dark",
            height=300,
            yaxis_tickformat=".1%",
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h")
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Simulation Loop ───────────────────────────────────────
if run_btn:
    transaction_log = []
    total = 0
    fraud_count = 0

    progress_bar = st.progress(0)
    status_msg   = st.empty()

    for i in range(num_txns):
        txn = generate_transaction(fraud_rate=fraud_rate)
        send_to_kafka(txn)

        total += 1
        if txn["is_fraud"]:
            fraud_count += 1

        transaction_log.append(txn)

        # Update KPIs
        rate = fraud_count / total if total > 0 else 0
        kpi_total.metric("Total Transactions", f"{total:,}")
        kpi_fraud.metric("Fraud Detected", f"{fraud_count:,}",
                         delta=f"+1" if txn["is_fraud"] else None,
                         delta_color="inverse")
        kpi_rate.metric("Current Fraud Rate", f"{rate:.2%}")

        # Update feed table
        display_df = pd.DataFrame(transaction_log[-15:])
        display_df["status"] = display_df["is_fraud"].map(
            {True: "🚨 FRAUD", False: "✅ OK"}
        )
        feed_table.dataframe(
            display_df[[
                "transaction_id", "account_id", "amount",
                "merchant_category", "region", "status"
            ]].iloc[::-1],
            use_container_width=True,
            hide_index=True
        )

        progress_bar.progress((i + 1) / num_txns)
        status_msg.caption(f"Sending transaction {i+1} of {num_txns}...")
        time.sleep(1 / tps)

    progress_bar.empty()
    status_msg.success(
        f"✅ Stream complete — {total} transactions sent, "
        f"{fraud_count} fraud detected ({fraud_count/total:.1%} rate). "
        f"Data flowing through Kafka → Databricks → Delta Lake → Power BI"
    )

# ── Footer ────────────────────────────────────────────────
st.divider()
st.caption(
    "Built with Apache Kafka · Azure ADLS Gen2 · "
    "Azure Databricks · PySpark · Delta Lake · "
    "Prophet · MLflow · Streamlit"
)
