import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer

# ----------------------------------------------------
# EMAIL CONFIG
# ----------------------------------------------------
EMAIL_ADDRESS = "dummy@gmail.com"
EMAIL_PASSWORD = "nxqh ggvg vvgv fgyfvgf"
TO_EMAIL = "dummy@gmail.com"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# ----------------------------------------------------
# KAFKA CONSUMER (LISTEN TO FRAUD EVENTS)
# ----------------------------------------------------
consumer = KafkaConsumer(
    "fraud-alerts",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📡 Email Alert Service Started — Listening for FRAUD alerts...")


# ----------------------------------------------------
# EMAIL SENDER
# ----------------------------------------------------
def send_email_alert(amount, prob, timestamp, record):
    subject = f"🚨 FRAUD ALERT — ${amount}"
    
    body = f"""
    <h2>FRAUD ALERT DETECTED 🚨</h2>
    <p><b>Timestamp:</b> {timestamp}</p>
    <p><b>Amount:</b> ${amount}</p>
    <p><b>Fraud Probability:</b> {prob:.4f}</p>
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

        print(f"📧 Email sent successfully → {TO_EMAIL}")

    except Exception as e:
        print("❌ Email sending failed:", e)


# ----------------------------------------------------
# MAIN LOOP — LISTEN FOR FRAUD EVENTS
# ----------------------------------------------------
for msg in consumer:
    alert = msg.value
    print("\n🚨 Fraud Alert Received:", alert)

    send_email_alert(
        amount=alert["Amount"],
        prob=alert["probability"],
        timestamp=alert["timestamp"],
        record=alert["record"]
    )
