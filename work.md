#### **D:\\data\_engineering\\fraud\_detection\\1\_data\_ingestion\\producers\\config.yaml**

data:

&nbsp; csv\_path: "/data/creditcard.csv"



kafka:

&nbsp; bootstrap\_servers: "kafka:9093"



&nbsp; topic: "creditcard-transactions"

producer:

&nbsp; rows\_per\_second: 1









#### **D:\\data\_engineering\\fraud\_detection\\1\_data\_ingestion\\producers\\producer.py**



**this runs when docker is started** 



import time

import json

import pandas as pd

import numpy as np

from confluent\_kafka import Producer

import yaml

import os

import random



\# ----------------------------------------------------

\# LOAD CONFIG

\# ----------------------------------------------------

config\_path = os.path.join(os.path.dirname(\_\_file\_\_), "config.yaml")

with open(config\_path, "r") as f:

&nbsp;   config = yaml.safe\_load(f)

print("Loaded config:", config)



CSV\_PATH = config\["data"]\["csv\_path"]

KAFKA\_BOOTSTRAP = config\["kafka"]\["bootstrap\_servers"]

TOPIC = config\["kafka"]\["topic"]

RATE = config\["producer"]\["rows\_per\_second"]



\# ----------------------------------------------------

\# PRODUCER CONFIG

\# ----------------------------------------------------

producer\_conf = {

&nbsp;   "bootstrap.servers": KAFKA\_BOOTSTRAP,

&nbsp;   "client.id": "creditcard-producer"

}



producer = Producer(producer\_conf)



def delivery\_report(err, msg):

&nbsp;   if err is not None:

&nbsp;       print(f" Delivery failed: {err}")

&nbsp;   else:

&nbsp;       print(f" Delivered to {msg.topic()} \[{msg.partition()}]")





\# ----------------------------------------------------

\# LOAD CSV + SPLIT NORMAL / FRAUD

\# ----------------------------------------------------

df = pd.read\_csv(CSV\_PATH)

df\_normal = df\[df\["Class"] == 0].reset\_index(drop=True)

df\_fraud = df\[df\["Class"] == 1].reset\_index(drop=True)



print(f"Normal rows: {len(df\_normal)}  Fraud rows: {len(df\_fraud)}")

print(f"Streaming to Kafka topic -> {TOPIC}")





\# ----------------------------------------------------

\# SYNTHETIC FRAUD GENERATOR

\# ----------------------------------------------------

def generate\_synthetic\_fraud():

&nbsp;   """Create a realistic synthetic fraud row."""

&nbsp;   

&nbsp;   base = df\_fraud.sample(1).iloc\[0].to\_dict()

&nbsp;   fraud = base.copy()



&nbsp;   # Add slight noise to V1–V28 features

&nbsp;   for v in \[f"V{i}" for i in range(1, 29)]:

&nbsp;       fraud\[v] = float(base\[v] + np.random.normal(0, 0.4))



&nbsp;   # Modify Amount

&nbsp;   fraud\["Amount"] = float(base\["Amount"] \* random.uniform(0.5, 2.5))



&nbsp;   # Randomize time

&nbsp;   fraud\["Time"] = float(random.uniform(0, max(df\["Time"])))



&nbsp;   fraud\["Class"] = 1  # ensure fraud label

&nbsp;   

&nbsp;   return fraud





\# ----------------------------------------------------

\# STREAM CSV + INJECT RANDOM FRAUDS

\# ----------------------------------------------------

def stream\_csv():



&nbsp;   print(f" Streaming {len(df\_normal)} normal rows to Kafka topic -> {TOPIC}")



&nbsp;   for \_, row in df\_normal.iterrows():



&nbsp;       # Send normal event

&nbsp;       producer.produce(

&nbsp;           topic=TOPIC,

&nbsp;           value=json.dumps(row.to\_dict()),

&nbsp;           callback=delivery\_report

&nbsp;       )

&nbsp;       producer.poll(0)



&nbsp;       # --- RANDOM FRAUD INJECTION (1 in ~20 = 5%) ---

&nbsp;       if random.random() < 0.05:

&nbsp;           fraud\_event = generate\_synthetic\_fraud()

&nbsp;           

&nbsp;           producer.produce(

&nbsp;               topic=TOPIC,

&nbsp;               value=json.dumps(fraud\_event),

&nbsp;               callback=delivery\_report

&nbsp;           )

&nbsp;           producer.poll(0)



&nbsp;           print("⚠️  Injected Synthetic FRAUD Event")



&nbsp;       time.sleep(1 / RATE)



&nbsp;   producer.flush()

&nbsp;   print("Stream complete!")





\# ----------------------------------------------------

\# MAIN

\# ----------------------------------------------------

if \_\_name\_\_ == "\_\_main\_\_":

&nbsp;   stream\_csv()









#### **D:\\data\_engineering\\fraud\_detection\\1\_data\_ingestion\\producers\\requirements.txt**

pandas

confluent-kafka

pyyaml

#### 





#### **D:\\data\_engineering\\fraud\_detection\\1\_data\_ingestion\\spark\_jobs\\clean\_stream.py**



**this runs using spark-submit clea\_stream.py and we need venv for this** 



from pyspark.sql import SparkSession

from pyspark.sql.functions import from\_json, col, current\_timestamp

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType



\# ----------------------------------

\# SCHEMA

\# ----------------------------------

schema = StructType(

&nbsp;   \[StructField("Time", DoubleType(), True)] +

&nbsp;   \[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)] +

&nbsp;   \[

&nbsp;       StructField("Amount", DoubleType(), True),

&nbsp;       StructField("Class", IntegerType(), True)

&nbsp;   ]

)



\# ----------------------------------

\# SPARK SESSION (NO spark.jars!)

\# ----------------------------------

spark = (

&nbsp;   SparkSession.builder

&nbsp;   .appName("CreditCardIngestionCleaner")

&nbsp;   .master("local\[\*]")

&nbsp;   .config("spark.driver.host", "127.0.0.1")

&nbsp;   .config("spark.driver.bindAddress", "0.0.0.0")

&nbsp;   .config("spark.jars.packages",

&nbsp;           "org.apache.spark:spark-sql-kafka-0-10\_2.12:3.2.0")

&nbsp;   .getOrCreate()

)



spark.sparkContext.setLogLevel("WARN")



\# ----------------------------------

\# AWS S3 CONFIG — FIXED

\# ----------------------------------

hadoopConf = spark.sparkContext.\_jsc.hadoopConfiguration()



hadoopConf.set("fs.s3a.access.key", "AKIAVFIWIXW5YYSZ2TUQ")

hadoopConf.set("fs.s3a.secret.key", "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")

hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com")

hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")



\# 🔥 FORCE SIMPLE CREDENTIALS (avoids IAM error)

hadoopConf.set("fs.s3a.aws.credentials.provider",

&nbsp;              "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")



\# 🔥 JUST IN CASE spark cached IAM provider

hadoopConf.unset("fs.s3a.session.credentials.provider")



\# ----------------------------------

\# READ RAW KAFKA STREAM

\# ----------------------------------

df\_raw = (

&nbsp;   spark.readStream

&nbsp;   .format("kafka")

&nbsp;   .option("kafka.bootstrap.servers", "localhost:9092")

&nbsp;   .option("subscribe", "creditcard-transactions")

&nbsp;   .option("startingOffsets", "latest")

&nbsp;   .load()

)



df\_json = df\_raw.selectExpr("CAST(value AS STRING) AS json\_str")



df\_parsed = df\_json.select(from\_json(col("json\_str"), schema).alias("data")).select("data.\*")



df\_cleaned = df\_parsed.fillna(0).withColumn("ingest\_ts", current\_timestamp())



\# ----------------------------------

\# TOGGLE: ENABLE/DISABLE S3 WRITE

\# ----------------------------------

ENABLE\_S3 = False   # Set True to enable S3 writes, False to disable



\# ----------------------------------

\# WRITE PARQUET TO S3 (TOGGLE CONTROL)

\# ----------------------------------

\# ----------------------------------

\# WRITE PARQUET TO S3 (TOGGLE CONTROL)

\# ----------------------------------

def write\_to\_s3(batch\_df, batch\_id):

&nbsp;   if ENABLE\_S3:

&nbsp;       print(f"S3 ENABLED - Writing batch {batch\_id} to S3...")

&nbsp;       try:

&nbsp;           batch\_df.write.format("parquet").mode("append") \\

&nbsp;               .save("s3a://credit-card-fraud-detection-1/raw/")

&nbsp;           print(f"Batch {batch\_id} successfully written to S3.")

&nbsp;       except Exception as e:

&nbsp;           print("S3 write error:", e)

&nbsp;   else:

&nbsp;       print(f"S3 DISABLED - Skipping S3 write for batch {batch\_id}")





batch\_query = (

&nbsp;   df\_cleaned.writeStream

&nbsp;   .trigger(processingTime="15 seconds")

&nbsp;   .foreachBatch(write\_to\_s3)

&nbsp;   .outputMode("append")

&nbsp;   .start()

)



\# ----------------------------------

\# REAL-TIME KAFKA OUT STREAM

\# ----------------------------------

df\_realtime = df\_cleaned.selectExpr("to\_json(struct(\*)) AS value")



realtime\_query = (

&nbsp;   df\_realtime.writeStream

&nbsp;   .trigger(processingTime="15 seconds")

&nbsp;   .format("kafka")

&nbsp;   .option("kafka.bootstrap.servers", "localhost:9092")

&nbsp;   .option("topic", "cleaned-transactions")

&nbsp;   .option("checkpointLocation",

&nbsp;           "file:///D:/data\_engineering/fraud\_detection/checkpoints/s3\_test")

&nbsp;   .outputMode("append")

&nbsp;   .start()

)



batch\_query.awaitTermination()



#### **D:\\data\_engineering\\fraud\_detection\\2\_stream\_processing\\alert\_service\\email\_alert\_service.py**



**this runs using python email\_alert\_service.py , it also needs venv** 



import json

import smtplib

from email.mime.text import MIMEText

from email.mime.multipart import MIMEMultipart

from kafka import KafkaConsumer



\# ----------------------------------------------------

\# EMAIL CONFIG

\# ----------------------------------------------------

EMAIL\_ADDRESS = "rama118143@gmail.com"

EMAIL\_PASSWORD = "nxqh oxmz weyl gknh"

TO\_EMAIL = "rama118143@gmail.com"



SMTP\_SERVER = "smtp.gmail.com"

SMTP\_PORT = 587



\# ----------------------------------------------------

\# KAFKA CONSUMER (LISTEN TO FRAUD EVENTS)

\# ----------------------------------------------------

consumer = KafkaConsumer(

&nbsp;   "fraud-alerts",

&nbsp;   bootstrap\_servers="localhost:9092",

&nbsp;   auto\_offset\_reset="latest",

&nbsp;   enable\_auto\_commit=True,

&nbsp;   value\_deserializer=lambda v: json.loads(v.decode("utf-8"))

)



print("📡 Email Alert Service Started — Listening for FRAUD alerts...")





\# ----------------------------------------------------

\# EMAIL SENDER

\# ----------------------------------------------------

def send\_email\_alert(amount, prob, timestamp, record):

&nbsp;   subject = f"🚨 FRAUD ALERT — ${amount}"

&nbsp;   

&nbsp;   body = f"""

&nbsp;   <h2>FRAUD ALERT DETECTED 🚨</h2>

&nbsp;   <p><b>Timestamp:</b> {timestamp}</p>

&nbsp;   <p><b>Amount:</b> ${amount}</p>

&nbsp;   <p><b>Fraud Probability:</b> {prob:.4f}</p>

&nbsp;   <br>

&nbsp;   <h3>Full Record:</h3>

&nbsp;   <pre>{json.dumps(record, indent=4)}</pre>

&nbsp;   """



&nbsp;   msg = MIMEMultipart()

&nbsp;   msg\["From"] = EMAIL\_ADDRESS

&nbsp;   msg\["To"] = TO\_EMAIL

&nbsp;   msg\["Subject"] = subject



&nbsp;   msg.attach(MIMEText(body, "html"))



&nbsp;   try:

&nbsp;       server = smtplib.SMTP(SMTP\_SERVER, SMTP\_PORT)

&nbsp;       server.starttls()

&nbsp;       server.login(EMAIL\_ADDRESS, EMAIL\_PASSWORD)

&nbsp;       server.sendmail(EMAIL\_ADDRESS, TO\_EMAIL, msg.as\_string())

&nbsp;       server.quit()



&nbsp;       print(f"📧 Email sent successfully → {TO\_EMAIL}")



&nbsp;   except Exception as e:

&nbsp;       print("❌ Email sending failed:", e)





\# ----------------------------------------------------

\# MAIN LOOP — LISTEN FOR FRAUD EVENTS

\# ----------------------------------------------------

for msg in consumer:

&nbsp;   alert = msg.value

&nbsp;   print("\\n🚨 Fraud Alert Received:", alert)



&nbsp;   send\_email\_alert(

&nbsp;       amount=alert\["Amount"],

&nbsp;       prob=alert\["probability"],

&nbsp;       timestamp=alert\["timestamp"],

&nbsp;       record=alert\["record"]

&nbsp;   )







#### **D:\\data\_engineering\\fraud\_detection\\2\_stream\_processing\\dashboard\\dashboard.py**



**it runs using streamlit run dashboard.py , inside venv** 



import streamlit as st

import pandas as pd

import json

from kafka import KafkaConsumer

import plotly.express as px



st.set\_page\_config(page\_title="Fraud Detection Dashboard", layout="wide")



\# -------------------------------

\# KAFKA CONSUMER

\# -------------------------------

consumer = KafkaConsumer(

&nbsp;   "predicted-transactions",

&nbsp;   bootstrap\_servers="localhost:9092",

&nbsp;   auto\_offset\_reset="latest",

&nbsp;   enable\_auto\_commit=True,

&nbsp;   value\_deserializer=lambda v: json.loads(v.decode("utf-8"))

)



\# -------------------------------

\# STATE VARIABLES

\# -------------------------------

rows = \[]

chart\_df = pd.DataFrame(columns=\["timestamp", "Amount", "predicted\_class", "fraud\_probability"])



fraud\_total = 0

safe\_total = 0



\# -------------------------------

\# HEADER

\# -------------------------------

st.title("🏦 Real-Time Fraud Detection System (Enterprise Dashboard)")



col1, col2, col3, col4 = st.columns(4)

metrics\_fraud = col1.metric("🚨 Fraud Count", 0)

metrics\_safe  = col2.metric("✔ Safe Count", 0)

metrics\_total = col3.metric("📦 Total Transactions", 0)

metrics\_rate  = col4.metric("📉 Fraud %", "0%")



\# -------------------------------

\# PLACEHOLDERS

\# -------------------------------

chart1\_placeholder = st.empty()

chart2\_placeholder = st.empty()

table\_placeholder = st.empty()



\# -------------------------------

\# TABLE STYLING (BLACK TEXT + BORDERS)

\# -------------------------------

def highlight\_row(row):

&nbsp;   if row\["Prediction"] == "FRAUD":

&nbsp;       return \[

&nbsp;           "background-color: #ffb3b3; color: black; font-weight: bold; border-bottom: 1px solid #888;"

&nbsp;           for \_ in row

&nbsp;       ]

&nbsp;   else:

&nbsp;       return \[

&nbsp;           "background-color: #c6f5c6; color: black; border-bottom: 1px solid #ccc;"

&nbsp;           for \_ in row

&nbsp;       ]





\# -------------------------------

\# STREAM LOOP

\# -------------------------------

for msg in consumer:



&nbsp;   record = msg.value



&nbsp;   # Update counters

&nbsp;   if record\["predicted\_class"] == 1:

&nbsp;       fraud\_total += 1

&nbsp;   else:

&nbsp;       safe\_total += 1



&nbsp;   total = fraud\_total + safe\_total

&nbsp;   fraud\_rate = (fraud\_total / total \* 100) if total > 0 else 0



&nbsp;   metrics\_fraud.metric("🚨 Fraud Count", fraud\_total)

&nbsp;   metrics\_safe.metric("✔ Safe Count", safe\_total)

&nbsp;   metrics\_total.metric("📦 Total Transactions", total)

&nbsp;   metrics\_rate.metric("📉 Fraud %", f"{fraud\_rate:.2f}%")



&nbsp;   # Append new row to live table

&nbsp;   rows.append({

&nbsp;       "Timestamp": record\["timestamp"],

&nbsp;       "Amount": record\["Amount"],

&nbsp;       "Prediction": "FRAUD" if record\["predicted\_class"] == 1 else "SAFE",

&nbsp;       "Probability": round(record\["fraud\_probability"], 4)

&nbsp;   })



&nbsp;   df = pd.DataFrame(rows)



&nbsp;   styled\_df = (

&nbsp;       df.style

&nbsp;       .apply(highlight\_row, axis=1)

&nbsp;       .set\_properties(\*\*{

&nbsp;           "border": "1px solid #444",

&nbsp;           "color": "black",

&nbsp;           "font-size": "14px"

&nbsp;       })

&nbsp;   )



&nbsp;   table\_placeholder.dataframe(styled\_df, height=400)



&nbsp;   # --------------------------

&nbsp;   # ADD TO CHART HISTORY

&nbsp;   # --------------------------

&nbsp;   chart\_df.loc\[len(chart\_df)] = \[

&nbsp;       record\["timestamp"],

&nbsp;       record\["Amount"],

&nbsp;       record\["predicted\_class"],

&nbsp;       record\["fraud\_probability"]

&nbsp;   ]



&nbsp;   # --------------------------

&nbsp;   # AMOUNT TREND CHART

&nbsp;   # --------------------------

&nbsp;   fig1 = px.line(

&nbsp;       chart\_df,

&nbsp;       x="timestamp",

&nbsp;       y="Amount",

&nbsp;       title="📈 Real-Time Transaction Amount Trend",

&nbsp;   )



&nbsp;   fraud\_points = chart\_df\[chart\_df\["predicted\_class"] == 1]

&nbsp;   fig1.add\_scatter(

&nbsp;       x=fraud\_points\["timestamp"],

&nbsp;       y=fraud\_points\["Amount"],

&nbsp;       mode="markers",

&nbsp;       marker=dict(color="red", size=10),

&nbsp;       name="Fraud"

&nbsp;   )



&nbsp;   chart1\_placeholder.plotly\_chart(fig1, use\_container\_width=True)



&nbsp;   # --------------------------

&nbsp;   # PROBABILITY TREND CHART

&nbsp;   # --------------------------

&nbsp;   fig2 = px.scatter(

&nbsp;       chart\_df,

&nbsp;       x="timestamp",

&nbsp;       y="fraud\_probability",

&nbsp;       color="predicted\_class",

&nbsp;       title="🔬 Fraud Probability Trend",

&nbsp;       color\_continuous\_scale=\["green", "red"]

&nbsp;   )



&nbsp;   chart2\_placeholder.plotly\_chart(fig2, use\_container\_width=True)



#### 

#### **D:\\data\_engineering\\fraud\_detection\\2\_stream\_processing\\predict\_step1.py**



**inside venv using python predict\_step1.py**



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



\# ----------------------------------------------------

\# CSV LOGGING SETUP

\# ----------------------------------------------------

BASE\_DIR = r"D:\\data\_engineering\\fraud\_detection\\data\\fraud\_data"



ALL\_CSV = os.path.join(BASE\_DIR, "all\_transactions.csv")

FRAUD\_CSV = os.path.join(BASE\_DIR, "fraud\_transactions.csv")



os.makedirs(BASE\_DIR, exist\_ok=True)



if not os.path.isfile(ALL\_CSV):

&nbsp;   with open(ALL\_CSV, "w", newline="") as f:

&nbsp;       writer = csv.writer(f)

&nbsp;       writer.writerow(\["timestamp", "Amount", "predicted\_class", "fraud\_probability", "raw\_record"])



if not os.path.isfile(FRAUD\_CSV):

&nbsp;   with open(FRAUD\_CSV, "w", newline="") as f:

&nbsp;       writer = csv.writer(f)

&nbsp;       writer.writerow(\["timestamp", "Amount", "fraud\_probability", "raw\_record"])



\# ----------------------------------------------------

\# EMAIL ALERT CONFIG

\# ----------------------------------------------------

EMAIL\_ADDRESS = "YOUR\_EMAIL@gmail.com"

EMAIL\_PASSWORD = "YOUR\_APP\_PASSWORD"

TO\_EMAIL = "TARGET\_EMAIL@gmail.com"



SMTP\_SERVER = "smtp.gmail.com"

SMTP\_PORT = 587



\# ----------------------------------------------------

\# LOAD MODEL

\# ----------------------------------------------------

MODEL\_PATH = r"D:\\data\_engineering\\fraud\_detection\\2\_stream\_processing\\model\_training\\lightgbm\_model.pkl"



with open(MODEL\_PATH, "rb") as f:

&nbsp;   artifact = pickle.load(f)



model = artifact\["model"]

FEATURES = artifact\["features"]



print("✅ Model loaded successfully.")

print(f"Total features loaded: {len(FEATURES)}")



\# ----------------------------------------------------

\# EMAIL ALERT FUNCTION

\# ----------------------------------------------------

def send\_email\_alert(amount, probability, timestamp, record):

&nbsp;   subject = f"🚨 FRAUD ALERT — Rs {amount}"



&nbsp;   body = f"""

&nbsp;   <h2>🚨 FRAUD ALERT DETECTED</h2>

&nbsp;   <p><b>Time:</b> {timestamp}</p>

&nbsp;   <p><b>Amount:</b> {amount}</p>

&nbsp;   <p><b>Probability:</b> {probability:.4f}</p>

&nbsp;   <br>

&nbsp;   <h3>Full Record:</h3>

&nbsp;   <pre>{json.dumps(record, indent=4)}</pre>

&nbsp;   """



&nbsp;   msg = MIMEMultipart()

&nbsp;   msg\["From"] = EMAIL\_ADDRESS

&nbsp;   msg\["To"] = TO\_EMAIL

&nbsp;   msg\["Subject"] = subject

&nbsp;   msg.attach(MIMEText(body, "html"))



&nbsp;   try:

&nbsp;       server = smtplib.SMTP(SMTP\_SERVER, SMTP\_PORT)

&nbsp;       server.starttls()

&nbsp;       server.login(EMAIL\_ADDRESS, EMAIL\_PASSWORD)

&nbsp;       server.sendmail(EMAIL\_ADDRESS, TO\_EMAIL, msg.as\_string())

&nbsp;       server.quit()

&nbsp;       print("📧 Email alert sent!")

&nbsp;   except Exception as e:

&nbsp;       print("❌ Email sending failed:", e)



\# ----------------------------------------------------

\# KAFKA SETUP

\# ----------------------------------------------------

consumer = KafkaConsumer(

&nbsp;   "cleaned-transactions",

&nbsp;   bootstrap\_servers="localhost:9092",

&nbsp;   auto\_offset\_reset="latest",

&nbsp;   enable\_auto\_commit=True,

&nbsp;   value\_deserializer=lambda v: json.loads(v.decode("utf-8"))

)



\# producer for alerts

alert\_producer = KafkaProducer(

&nbsp;   bootstrap\_servers="localhost:9092",

&nbsp;   value\_serializer=lambda v: json.dumps(v).encode("utf-8")

)



\# producer for dashboard

dashboard\_producer = KafkaProducer(

&nbsp;   bootstrap\_servers="localhost:9092",

&nbsp;   value\_serializer=lambda v: json.dumps(v).encode("utf-8")

)



print("📡 Listening to Kafka topic: cleaned-transactions ...")



\# ----------------------------------------------------

\# FEATURE EXTRACTOR

\# ----------------------------------------------------

def extract\_features(record):

&nbsp;   values = \[]

&nbsp;   for f in FEATURES:

&nbsp;       values.append(float(record.get(f, 0)))

&nbsp;   return np.array(values).reshape(1, -1)



\# ----------------------------------------------------

\# REAL-TIME PREDICTION LOOP

\# ----------------------------------------------------

for msg in consumer:

&nbsp;   data = msg.value

&nbsp;   features = extract\_features(data)



&nbsp;   pred = model.predict(features)\[0]

&nbsp;   proba = model.predict\_proba(features)\[0]\[1]



&nbsp;   now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")



&nbsp;   print("\\n===========================")

&nbsp;   print(f"⏱ Timestamp: {now}")

&nbsp;   print(f"💰 Amount: {data.get('Amount', 0)}")

&nbsp;   print(f"🔍 Prediction: {'🚨 FRAUD' if pred == 1 else '✔️ SAFE'}")

&nbsp;   print(f"📊 Probability: {proba:.4f}")

&nbsp;   print("===========================\\n")



&nbsp;   # ----------------------------------------------------

&nbsp;   # SAVE ALL TRANSACTIONS

&nbsp;   # ----------------------------------------------------

&nbsp;   with open(ALL\_CSV, "a", newline="") as f:

&nbsp;       writer = csv.writer(f)

&nbsp;       writer.writerow(\[

&nbsp;           now,

&nbsp;           data.get("Amount", 0),

&nbsp;           int(pred),

&nbsp;           round(proba, 4),

&nbsp;           json.dumps(data)

&nbsp;       ])

&nbsp;   print("💾 Saved to ALL transactions CSV")



&nbsp;   # ----------------------------------------------------

&nbsp;   # SAVE FRAUDS + ALERT FLOW

&nbsp;   # ----------------------------------------------------

&nbsp;   if pred == 1:



&nbsp;       # Save CSV

&nbsp;       with open(FRAUD\_CSV, "a", newline="") as f:

&nbsp;           writer = csv.writer(f)

&nbsp;           writer.writerow(\[

&nbsp;               now,

&nbsp;               data.get("Amount", 0),

&nbsp;               round(proba, 4),

&nbsp;               json.dumps(data)

&nbsp;           ])

&nbsp;       print("🚨 Saved FRAUD record to fraud CSV")



&nbsp;       # Kafka notify email service

&nbsp;       alert\_event = {

&nbsp;           "timestamp": now,

&nbsp;           "Amount": data.get("Amount", 0),

&nbsp;           "probability": float(proba),

&nbsp;           "record": data

&nbsp;       }

&nbsp;       alert\_producer.send("fraud-alerts", alert\_event)

&nbsp;       alert\_producer.flush()

&nbsp;       print("📢 Pushed fraud alert → fraud-alerts")



&nbsp;       # Email alert

&nbsp;       send\_email\_alert(

&nbsp;           amount=data.get("Amount", 0),

&nbsp;           probability=proba,

&nbsp;           timestamp=now,

&nbsp;           record=data

&nbsp;       )



&nbsp;   # ----------------------------------------------------

&nbsp;   # SEND EVERY PREDICTION TO DASHBOARD

&nbsp;   # ----------------------------------------------------

&nbsp;   dashboard\_event = {

&nbsp;       "timestamp": now,

&nbsp;       "Amount": data.get("Amount", 0),

&nbsp;       "predicted\_class": int(pred),

&nbsp;       "fraud\_probability": float(proba)

&nbsp;   }



&nbsp;   dashboard\_producer.send("predicted-transactions", dashboard\_event)

&nbsp;   dashboard\_producer.flush()



&nbsp;   print("📊 Sent update to dashboard → predicted-transactions")





#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\analytics\_high\_risk\_patterns.sql**



**it should run inside snowflake I think** 



SELECT

&nbsp;   V14, V17, V21,

&nbsp;   COUNT(\*) AS frequency,

&nbsp;   SUM(CASE WHEN Class = 1 THEN 1 END) AS fraud\_count

FROM cleaned\_batch\_data

GROUP BY 1,2,3

ORDER BY fraud\_count DESC

LIMIT 20;







#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\copy\_into.sql**

**it should run inside snowflake I think**



COPY INTO cleaned\_transactions

FROM @cleaned\_stage

FILE\_FORMAT = (TYPE=PARQUET);





#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\create\_stage.sql**



**it should run inside snowflake I think**



CREATE OR REPLACE STAGE s3\_cleaned\_stage

URL = 's3://credit-card-fraud-detection-1/raw/'

FILE\_FORMAT = (TYPE = PARQUET);



#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\create\_table.sql**



**it should run inside snowflake I think**



CREATE OR REPLACE TABLE cleaned\_transactions (

&nbsp;   Time FLOAT,

&nbsp;   V1 FLOAT, V2 FLOAT, V3 FLOAT, V4 FLOAT, V5 FLOAT, V6 FLOAT, V7 FLOAT,

&nbsp;   V8 FLOAT, V9 FLOAT, V10 FLOAT, V11 FLOAT, V12 FLOAT, V13 FLOAT, V14 FLOAT,

&nbsp;   V15 FLOAT, V16 FLOAT, V17 FLOAT, V18 FLOAT, V19 FLOAT, V20 FLOAT,

&nbsp;   V21 FLOAT, V22 FLOAT, V23 FLOAT, V24 FLOAT, V25 FLOAT, V26 FLOAT,

&nbsp;   V27 FLOAT, V28 FLOAT,

&nbsp;   Amount FLOAT,

&nbsp;   Class INTEGER,

&nbsp;   ingest\_ts TIMESTAMP

);



#### 

#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\daily\_fraud\_summary.sql**



**it should run inside snowflake I think**



SELECT

&nbsp;   DATE(ingest\_ts) AS day,

&nbsp;   COUNT(\*) AS total\_tx,

&nbsp;   SUM(CASE WHEN Class = 1 THEN 1 ELSE 0 END) AS fraud\_tx,

&nbsp;   ROUND((fraud\_tx / total\_tx) \* 100, 2) AS fraud\_percentage

FROM cleaned\_batch\_data

GROUP BY 1

ORDER BY 1;



#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\fraud\_amount\_analysis.sql**



**it should run inside snowflake I think**



SELECT 

&nbsp;   Class,

&nbsp;   ROUND(AVG(Amount), 2) AS avg\_amount,

&nbsp;   ROUND(MIN(Amount), 2) AS min\_amount,

&nbsp;   ROUND(MAX(Amount), 2) AS max\_amount,

&nbsp;   COUNT(\*) AS count

FROM cleaned\_transactions

GROUP BY Class

ORDER BY Class;



#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\fraud\_hourly\_pattern.sql**



**it should run inside snowflake I think**





SELECT 

&nbsp;   DATE\_PART(hour, ingest\_ts) AS hour,

&nbsp;   COUNT\_IF(Class = 1) AS fraud\_count

FROM cleaned\_transactions

GROUP BY hour

ORDER BY hour;



#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\fraud\_percentage\_report.sql**



**it should run inside snowflake I think**



SELECT 

&nbsp;   COUNT\_IF(Class = 1) AS frauds,

&nbsp;   COUNT(\*) AS total,

&nbsp;   ROUND(frauds / total \* 100, 2) AS fraud\_rate

FROM cleaned\_transactions;



#### **D:\\data\_engineering\\fraud\_detection\\3\_batch\_processing\\snowflake\_sql\\top\_fraud\_transactions.sql**



**it should run inside snowflake I think**



SELECT 

&nbsp;   Amount,

&nbsp;   ingest\_ts,

&nbsp;   Time

FROM cleaned\_transactions

WHERE Class = 1

ORDER BY Amount DESC

LIMIT 20;



#### D:\\data\_engineering\\fraud\_detection\\data\\raw\\creditcard.csv



it has raw csv data 



#### **D:\\data\_engineering\\fraud\_detection\\infrastructure\\docker\\docker-compose.ingestion.yml**



**this is docker which controls ingestion , and producer.py also run when we run this docker** 



version: "3.9"



services:



&nbsp; zookeeper:

&nbsp;   image: confluentinc/cp-zookeeper:7.4.1

&nbsp;   environment:

&nbsp;     ZOOKEEPER\_CLIENT\_PORT: 2181

&nbsp;   ports:

&nbsp;     - "2181:2181"



&nbsp; kafka:

&nbsp;   image: confluentinc/cp-kafka:7.4.1

&nbsp;   depends\_on:

&nbsp;     - zookeeper

&nbsp;   environment:

&nbsp;     KAFKA\_BROKER\_ID: 1

&nbsp;     KAFKA\_ZOOKEEPER\_CONNECT: zookeeper:2181

&nbsp;     KAFKA\_LISTENERS: PLAINTEXT\_INTERNAL://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9092

&nbsp;     KAFKA\_ADVERTISED\_LISTENERS: PLAINTEXT\_INTERNAL://kafka:9093,PLAINTEXT://localhost:9092

&nbsp;     KAFKA\_LISTENER\_SECURITY\_PROTOCOL\_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT\_INTERNAL:PLAINTEXT

&nbsp;     KAFKA\_INTER\_BROKER\_LISTENER\_NAME: PLAINTEXT\_INTERNAL

&nbsp;     KAFKA\_OFFSETS\_TOPIC\_REPLICATION\_FACTOR: 1

&nbsp;   ports:

&nbsp;     - "9092:9092"

&nbsp;     - "9093:9093"



&nbsp; kafka-ui:

&nbsp;   image: provectuslabs/kafka-ui:latest

&nbsp;   depends\_on:

&nbsp;     - kafka

&nbsp;   ports:

&nbsp;     - "8082:8080"

&nbsp;   environment:

&nbsp;     KAFKA\_CLUSTERS\_0\_NAME: local

&nbsp;     KAFKA\_CLUSTERS\_0\_BOOTSTRAPSERVERS: kafka:9093



&nbsp; producer:

&nbsp;   image: python:3.10-slim

&nbsp;   container\_name: producer

&nbsp;   stdin\_open: true

&nbsp;   tty: true

&nbsp;   depends\_on:

&nbsp;     - kafka

&nbsp;   working\_dir: /app

&nbsp;   volumes:

&nbsp;     - ../../1\_data\_ingestion/producers:/app

&nbsp;     - ../../data/raw:/data

&nbsp;   command: >

&nbsp;     bash -c "

&nbsp;       pip install -r requirements.txt \&\&

&nbsp;       python producer.py

&nbsp;     "

#### **D:\\data\_engineering\\fraud\_detection\\infrastructure\\docker\\airflow\\dags\\batch\_dag.py**



from airflow import DAG

from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

import snowflake.connector

import os



PROJECT\_ROOT = "/workspace"

SQL\_DIR = os.path.join(PROJECT\_ROOT, "3\_batch\_processing", "snowflake\_sql")



\# read credentials from env (set via connections.env or Airflow vars)

SNOWFLAKE\_ACCOUNT = os.getenv("SNOWFLAKE\_ACCOUNT")

SNOWFLAKE\_USER = os.getenv("SNOWFLAKE\_USER")

SNOWFLAKE\_PASSWORD = os.getenv("SNOWFLAKE\_PASSWORD")

SNOWFLAKE\_WAREHOUSE = os.getenv("SNOWFLAKE\_WAREHOUSE", "MYWH")

SNOWFLAKE\_DATABASE = os.getenv("SNOWFLAKE\_DATABASE", "MYDB")

SNOWFLAKE\_SCHEMA = os.getenv("SNOWFLAKE\_SCHEMA", "PUBLIC")

SNOWFLAKE\_ROLE = os.getenv("SNOWFLAKE\_ROLE", "ACCOUNTADMIN")



default\_args = {

&nbsp;   "owner": "you",

&nbsp;   "depends\_on\_past": False,

&nbsp;   "retries": 0,

&nbsp;   "retry\_delay": timedelta(minutes=1),

}



def run\_sql\_file(filename):

&nbsp;   path = os.path.join(SQL\_DIR, filename)

&nbsp;   with open(path, 'r') as f:

&nbsp;       sql = f.read()



&nbsp;   ctx = snowflake.connector.connect(

&nbsp;       user=SNOWFLAKE\_USER,

&nbsp;       password=SNOWFLAKE\_PASSWORD,

&nbsp;       account=SNOWFLAKE\_ACCOUNT,

&nbsp;       warehouse=SNOWFLAKE\_WAREHOUSE,

&nbsp;       database=SNOWFLAKE\_DATABASE,

&nbsp;       schema=SNOWFLAKE\_SCHEMA,

&nbsp;       role=SNOWFLAKE\_ROLE

&nbsp;   )

&nbsp;   cs = ctx.cursor()

&nbsp;   try:

&nbsp;       for stmt in sql.strip().split(";"):

&nbsp;           s = stmt.strip()

&nbsp;           if s:

&nbsp;               cs.execute(s)

&nbsp;       ctx.commit()

&nbsp;   finally:

&nbsp;       cs.close()

&nbsp;       ctx.close()



with DAG(

&nbsp;   dag\_id="batch\_dag",

&nbsp;   default\_args=default\_args,

&nbsp;   schedule\_interval="@daily",

&nbsp;   start\_date=datetime(2025, 1, 1),

&nbsp;   catchup=False,

&nbsp;   tags=\["batch"],

) as dag:



&nbsp;   copy\_into = PythonOperator(

&nbsp;       task\_id="run\_copy\_into",

&nbsp;       python\_callable=lambda: run\_sql\_file("copy\_into.sql")

&nbsp;   )



&nbsp;   analytics\_daily = PythonOperator(

&nbsp;       task\_id="analytics\_daily\_fraud",

&nbsp;       python\_callable=lambda: run\_sql\_file("analytics\_daily\_fraud.sql")

&nbsp;   )



&nbsp;   analytics\_amount = PythonOperator(

&nbsp;       task\_id="analytics\_amount\_stats",

&nbsp;       python\_callable=lambda: run\_sql\_file("analytics\_amount\_stats.sql")

&nbsp;   )



&nbsp;   analytics\_patterns = PythonOperator(

&nbsp;       task\_id="analytics\_high\_risk\_patterns",

&nbsp;       python\_callable=lambda: run\_sql\_file("analytics\_high\_risk\_patterns.sql")

&nbsp;   )



&nbsp;   copy\_into >> \[analytics\_daily, analytics\_amount, analytics\_patterns]





#### **D:\\data\_engineering\\fraud\_detection\\infrastructure\\docker\\airflow\\dags\\ingestion\_dag.py**



from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta



default\_args = {

&nbsp;   "owner": "you",

&nbsp;   "depends\_on\_past": False,

&nbsp;   "retries": 0,

&nbsp;   "retry\_delay": timedelta(minutes=1),

}



INGESTION\_COMPOSE\_PATH = "/workspace/infrastructure/docker/docker-compose.ingestion.yml"



with DAG(

&nbsp;   dag\_id="ingestion\_dag",

&nbsp;   default\_args=default\_args,

&nbsp;   schedule\_interval=None,

&nbsp;   start\_date=datetime(2025, 1, 1),

&nbsp;   catchup=False,

&nbsp;   tags=\["ingestion"],

) as dag:



&nbsp;   start\_ingestion = BashOperator(

&nbsp;       task\_id="start\_ingestion\_compose",

&nbsp;       bash\_command=f"docker compose -f {INGESTION\_COMPOSE\_PATH} up -d --build producer kafka zookeeper",

&nbsp;   )



&nbsp;   # optional: give the producer some time to warm up

&nbsp;   wait\_task = BashOperator(

&nbsp;       task\_id="wait\_30s",

&nbsp;       bash\_command="sleep 20"

&nbsp;   )



&nbsp;   start\_ingestion >> wait\_task



#### **D:\\data\_engineering\\fraud\_detection\\infrastructure\\docker\\airflow\\dags\\master\_dag.py**

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.trigger\_dagrun import TriggerDagRunOperator



default\_args = {

&nbsp;   "owner": "you",

&nbsp;   "depends\_on\_past": False,

&nbsp;   "retries": 0,

&nbsp;   "retry\_delay": timedelta(minutes=1),

}



with DAG(

&nbsp;   dag\_id="master\_dag",

&nbsp;   default\_args=default\_args,

&nbsp;   schedule\_interval="@daily",

&nbsp;   start\_date=datetime(2025, 1, 1),

&nbsp;   catchup=False,

&nbsp;   tags=\["master"],

) as dag:



&nbsp;   start\_ingestion = TriggerDagRunOperator(

&nbsp;       task\_id="trigger\_ingestion",

&nbsp;       trigger\_dag\_id="ingestion\_dag",

&nbsp;       wait\_for\_completion=True,

&nbsp;   )



&nbsp;   start\_stream = TriggerDagRunOperator(

&nbsp;       task\_id="trigger\_stream",

&nbsp;       trigger\_dag\_id="stream\_dag",

&nbsp;       wait\_for\_completion=True,

&nbsp;   )



&nbsp;   start\_batch = TriggerDagRunOperator(

&nbsp;       task\_id="trigger\_batch",

&nbsp;       trigger\_dag\_id="batch\_dag",

&nbsp;       wait\_for\_completion=True,

&nbsp;   )



&nbsp;   start\_ingestion >> start\_stream >> start\_batch



#### **D:\\data\_engineering\\fraud\_detection\\infrastructure\\docker\\airflow\\dags\\stream\_dag.py**

from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

import os



PROJECT\_ROOT = "/workspace"



default\_args = {

&nbsp;   "owner": "you",

&nbsp;   "depends\_on\_past": False,

&nbsp;   "retries": 0,

&nbsp;   "retry\_delay": timedelta(minutes=1),

}



with DAG(

&nbsp;   dag\_id="stream\_dag",

&nbsp;   default\_args=default\_args,

&nbsp;   schedule\_interval=None,

&nbsp;   start\_date=datetime(2025, 1, 1),

&nbsp;   catchup=False,

&nbsp;   tags=\["stream"],

) as dag:



&nbsp;   run\_clean\_stream = BashOperator(

&nbsp;       task\_id="run\_clean\_stream",

&nbsp;       bash\_command=f"{PROJECT\_ROOT}/infrastructure/docker/airflow/scripts/run\_spark\_submit.sh {PROJECT\_ROOT}/1\_data\_ingestion/spark\_jobs/clean\_stream.py"

&nbsp;   )



&nbsp;   start\_predictor = BashOperator(

&nbsp;       task\_id="start\_predictor",

&nbsp;       bash\_command=f"{PROJECT\_ROOT}/infrastructure/docker/airflow/scripts/run\_python\_service.sh {PROJECT\_ROOT}/2\_stream\_processing/predict\_step1.py \&",

&nbsp;   )



&nbsp;   start\_alert\_service = BashOperator(

&nbsp;       task\_id="start\_alert\_service",

&nbsp;       bash\_command=f"{PROJECT\_ROOT}/infrastructure/docker/airflow/scripts/run\_python\_service.sh {PROJECT\_ROOT}/2\_stream\_processing/alert\_service/email\_alert\_service.py \&",

&nbsp;   )



&nbsp;   start\_dashboard = BashOperator(

&nbsp;       task\_id="start\_dashboard",

&nbsp;       bash\_command=f"streamlit run {PROJECT\_ROOT}/2\_stream\_processing/dashboard/dashboard.py \&",

&nbsp;   )



&nbsp;   run\_clean\_stream >> \[start\_predictor, start\_alert\_service, start\_dashboard]

#### **D:\\data\_engineering\\fraud\_detection\\infrastructure\\docker\\airflow\\docker-compose.yml**

 

**this works , so dont change anything , it also shows dags , but there are errors when we run dags , you know this** 



version: "3.8"



services:

&nbsp; airflow:

&nbsp;   container\_name: airflow\_container

&nbsp;   image: apache/airflow:2.9.2

&nbsp;   environment:

&nbsp;     AIRFLOW\_\_DATABASE\_\_SQL\_ALCHEMY\_CONN: sqlite:////opt/airflow/airflow.db

&nbsp;     AIRFLOW\_\_CORE\_\_LOAD\_EXAMPLES: "false"

&nbsp;   ports:

&nbsp;     - "8080:8080"

&nbsp;   volumes:

&nbsp;     - ./dags:/opt/airflow/dags

&nbsp;   command: >

&nbsp;     bash -c "airflow db migrate \&\&

&nbsp;              airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@example.com \&\&

&nbsp;              airflow standalone"



#### **D:\\data\_engineering\\fraud\_detection\\infrastructure\\docker\\airflow\\.env**

AIRFLOW\_\_CORE\_\_LOAD\_EXAMPLES=False

AIRFLOW\_\_WEBSERVER\_\_WORKERS=1

AIRFLOW\_UID=50000



