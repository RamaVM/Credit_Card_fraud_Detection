import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
import plotly.express as px

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")

# -------------------------------
# KAFKA CONSUMER
# -------------------------------
consumer = KafkaConsumer(
    "predicted-transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# -------------------------------
# STATE VARIABLES
# -------------------------------
rows = []
chart_df = pd.DataFrame(columns=["timestamp", "Amount", "predicted_class", "fraud_probability"])

fraud_total = 0
safe_total = 0

# -------------------------------
# HEADER
# -------------------------------
st.title("🏦 Real-Time Fraud Detection System (Enterprise Dashboard)")

col1, col2, col3, col4 = st.columns(4)
metrics_fraud = col1.metric("🚨 Fraud Count", 0)
metrics_safe  = col2.metric("✔ Safe Count", 0)
metrics_total = col3.metric("📦 Total Transactions", 0)
metrics_rate  = col4.metric("📉 Fraud %", "0%")

# -------------------------------
# PLACEHOLDERS
# -------------------------------
chart1_placeholder = st.empty()
chart2_placeholder = st.empty()
table_placeholder = st.empty()

# -------------------------------
# TABLE STYLING (BLACK TEXT + BORDERS)
# -------------------------------
def highlight_row(row):
    if row["Prediction"] == "FRAUD":
        return [
            "background-color: #ffb3b3; color: black; font-weight: bold; border-bottom: 1px solid #888;"
            for _ in row
        ]
    else:
        return [
            "background-color: #c6f5c6; color: black; border-bottom: 1px solid #ccc;"
            for _ in row
        ]


# -------------------------------
# STREAM LOOP
# -------------------------------
for msg in consumer:

    record = msg.value

    # Update counters
    if record["predicted_class"] == 1:
        fraud_total += 1
    else:
        safe_total += 1

    total = fraud_total + safe_total
    fraud_rate = (fraud_total / total * 100) if total > 0 else 0

    metrics_fraud.metric("🚨 Fraud Count", fraud_total)
    metrics_safe.metric("✔ Safe Count", safe_total)
    metrics_total.metric("📦 Total Transactions", total)
    metrics_rate.metric("📉 Fraud %", f"{fraud_rate:.2f}%")

    # Append new row to live table
    rows.append({
        "Timestamp": record["timestamp"],
        "Amount": record["Amount"],
        "Prediction": "FRAUD" if record["predicted_class"] == 1 else "SAFE",
        "Probability": round(record["fraud_probability"], 4)
    })

    df = pd.DataFrame(rows)

    styled_df = (
        df.style
        .apply(highlight_row, axis=1)
        .set_properties(**{
            "border": "1px solid #444",
            "color": "black",
            "font-size": "14px"
        })
    )

    table_placeholder.dataframe(styled_df, height=400)

    # --------------------------
    # ADD TO CHART HISTORY
    # --------------------------
    chart_df.loc[len(chart_df)] = [
        record["timestamp"],
        record["Amount"],
        record["predicted_class"],
        record["fraud_probability"]
    ]

    # --------------------------
    # AMOUNT TREND CHART
    # --------------------------
    fig1 = px.line(
        chart_df,
        x="timestamp",
        y="Amount",
        title="📈 Real-Time Transaction Amount Trend",
    )

    fraud_points = chart_df[chart_df["predicted_class"] == 1]
    fig1.add_scatter(
        x=fraud_points["timestamp"],
        y=fraud_points["Amount"],
        mode="markers",
        marker=dict(color="red", size=10),
        name="Fraud"
    )

    chart1_placeholder.plotly_chart(fig1, use_container_width=True)

    # --------------------------
    # PROBABILITY TREND CHART
    # --------------------------
    fig2 = px.scatter(
        chart_df,
        x="timestamp",
        y="fraud_probability",
        color="predicted_class",
        title="🔬 Fraud Probability Trend",
        color_continuous_scale=["green", "red"]
    )

    chart2_placeholder.plotly_chart(fig2, use_container_width=True)
