import json
import pickle
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import os
import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ----------------------------------------------------
# CSV LOGGING SETUP
# ----------------------------------------------------
BASE_DIR = r"D:\data_engineering\fraud_detection\data\fraud_data"

ALL_CSV = os.path.join(BASE_DIR, "all_transactions.csv")
FRAUD_CSV = os.path.join(BASE_DIR, "fraud_transactions.csv")

os.makedirs(BASE_DIR, exist_ok=True)

if not os.path.isfile(ALL_CSV):
    with open(ALL_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "Amount", "predicted_class", "fraud_probability", "raw_record"])

if not os.path.isfile(FRAUD_CSV):
    with open(FRAUD_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "Amount", "fraud_probability", "raw_record"])

# ----------------------------------------------------
# EMAIL ALERT CONFIG
# ----------------------------------------------------
EMAIL_ADDRESS = "YOUR_EMAIL@gmail.com"
EMAIL_PASSWORD = "YOUR_APP_PASSWORD"
TO_EMAIL = "TARGET_EMAIL@gmail.com"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# ----------------------------------------------------
# LOAD MODEL
# ----------------------------------------------------
MODEL_PATH = r"D:\data_engineering\fraud_detection\2_stream_processing\model_training\lightgbm_model.pkl"

with open(MODEL_PATH, "rb") as f:
    artifact = pickle.load(f)

model = artifact["model"]
FEATURES = artifact["features"]

print("✅ Model loaded successfully.")
print(f"Total features loaded: {len(FEATURES)}")

# ----------------------------------------------------
# EMAIL ALERT FUNCTION
# ----------------------------------------------------
def send_email_alert(amount, probability, timestamp, record):
    subject = f"🚨 FRAUD ALERT — Rs {amount}"

    body = f"""
    <h2>🚨 FRAUD ALERT DETECTED</h2>
    <p><b>Time:</b> {timestamp}</p>
    <p><b>Amount:</b> {amount}</p>
    <p><b>Probability:</b> {probability:.4f}</p>
    <br>
    <h3>Full Record:</h3>
    <pre>{json.dumps(record, indent=4)}</pre>
    """

    msg = MIMEMultipart()
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = TO_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.sendmail(EMAIL_ADDRESS, TO_EMAIL, msg.as_string())
        server.quit()
        print("📧 Email alert sent!")
    except Exception as e:
        print("❌ Email sending failed:", e)

# ----------------------------------------------------
# KAFKA SETUP
# ----------------------------------------------------
consumer = KafkaConsumer(
    "cleaned-transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# producer for alerts
alert_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# producer for dashboard
dashboard_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📡 Listening to Kafka topic: cleaned-transactions ...")

# ----------------------------------------------------
# FEATURE EXTRACTOR
# ----------------------------------------------------
def extract_features(record):
    values = []
    for f in FEATURES:
        values.append(float(record.get(f, 0)))
    return np.array(values).reshape(1, -1)

# ----------------------------------------------------
# REAL-TIME PREDICTION LOOP
# ----------------------------------------------------
for msg in consumer:
    data = msg.value
    features = extract_features(data)

    pred = model.predict(features)[0]
    proba = model.predict_proba(features)[0][1]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n===========================")
    print(f"⏱ Timestamp: {now}")
    print(f"💰 Amount: {data.get('Amount', 0)}")
    print(f"🔍 Prediction: {'🚨 FRAUD' if pred == 1 else '✔️ SAFE'}")
    print(f"📊 Probability: {proba:.4f}")
    print("===========================\n")

    # ----------------------------------------------------
    # SAVE ALL TRANSACTIONS
    # ----------------------------------------------------
    with open(ALL_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            now,
            data.get("Amount", 0),
            int(pred),
            round(proba, 4),
            json.dumps(data)
        ])
    print("💾 Saved to ALL transactions CSV")

    # ----------------------------------------------------
    # SAVE FRAUDS + ALERT FLOW
    # ----------------------------------------------------
    if pred == 1:

        # Save CSV
        with open(FRAUD_CSV, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now,
                data.get("Amount", 0),
                round(proba, 4),
                json.dumps(data)
            ])
        print("🚨 Saved FRAUD record to fraud CSV")

        # Kafka notify email service
        alert_event = {
            "timestamp": now,
            "Amount": data.get("Amount", 0),
            "probability": float(proba),
            "record": data
        }
        alert_producer.send("fraud-alerts", alert_event)
        alert_producer.flush()
        print("📢 Pushed fraud alert → fraud-alerts")

        # Email alert
        send_email_alert(
            amount=data.get("Amount", 0),
            probability=proba,
            timestamp=now,
            record=data
        )

    # ----------------------------------------------------
    # SEND EVERY PREDICTION TO DASHBOARD
    # ----------------------------------------------------
    dashboard_event = {
        "timestamp": now,
        "Amount": data.get("Amount", 0),
        "predicted_class": int(pred),
        "fraud_probability": float(proba)
    }

    dashboard_producer.send("predicted-transactions", dashboard_event)
    dashboard_producer.flush()

    print("📊 Sent update to dashboard → predicted-transactions")
