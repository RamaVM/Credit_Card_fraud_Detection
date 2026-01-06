import time
import json
import pandas as pd
import numpy as np
from confluent_kafka import Producer
import yaml
import os
import random

# ----------------------------------------------------
# LOAD CONFIG
# ----------------------------------------------------
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

print("Loaded config:", config)

CSV_PATH = config["data"]["csv_path"]
KAFKA_BOOTSTRAP = config["kafka"]["bootstrap_servers"]
TOPIC = config["kafka"]["topic"]
RATE = config["producer"]["rows_per_second"]

# ----------------------------------------------------
# PRODUCER CONFIG
# ----------------------------------------------------
producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "creditcard-producer"
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

# ----------------------------------------------------
# LOAD CSV
# ----------------------------------------------------
df = pd.read_csv(CSV_PATH)
df_normal = df[df["Class"] == 0].reset_index(drop=True)
df_fraud = df[df["Class"] == 1].reset_index(drop=True)

print(f"Normal rows: {len(df_normal)} Fraud rows: {len(df_fraud)}")
print(f"Streaming to Kafka topic → {TOPIC}")

# ----------------------------------------------------
# SYNTHETIC FRAUD GENERATOR
# ----------------------------------------------------
def generate_synthetic_fraud():
    base = df_fraud.sample(1).iloc[0].to_dict()
    fraud = base.copy()

    for v in [f"V{i}" for i in range(1, 29)]:
        fraud[v] = float(base[v] + np.random.normal(0, 0.4))

    fraud["Amount"] = float(base["Amount"] * random.uniform(0.5, 2.5))
    fraud["Time"] = float(random.uniform(0, max(df["Time"])))
    fraud["Class"] = 1

    return fraud

# ----------------------------------------------------
# STREAM CSV
# ----------------------------------------------------
def stream_csv():
    print(f"🚀 Streaming {len(df_normal)} normal rows...")

    for _, row in df_normal.iterrows():
        event = row.to_dict()

        print(f"📤 Normal event → Amount={event['Amount']}")
        producer.produce(
            topic=TOPIC,
            value=json.dumps(event),
            callback=delivery_report
        )
        producer.poll(0)

        if random.random() < 0.05:
            fraud_event = generate_synthetic_fraud()
            print(f"⚠️ FRAUD injected → Amount={fraud_event['Amount']}")
            producer.produce(
                topic=TOPIC,
                value=json.dumps(fraud_event),
                callback=delivery_report
            )
            producer.poll(0)

        time.sleep(1 / RATE)

    producer.flush()
    print("🎉 Stream Complete!")

if __name__ == "__main__":
    stream_csv()
