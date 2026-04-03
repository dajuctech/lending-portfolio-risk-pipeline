"""
LOAN EVENT PRODUCER
Reads loan data from BigQuery and sends each loan as a
JSON message to the Redpanda topic "loan-events".

How to run:
    cd "08 project"
    export GOOGLE_APPLICATION_CREDENTIALS="credentials/gcp-service-account.json"
    export GCP_PROJECT_ID=$(grep ^GCP_PROJECT_ID .env | cut -d= -f2)
    uv run streaming/producer_loans.py
"""

import os
import json
from time import time, sleep
from google.cloud import bigquery
from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"       # Redpanda address
TOPIC           = "loan-events"          # Topic name
PROJECT_ID      = os.environ["GCP_PROJECT_ID"]

# ── Connect to Redpanda ───────────────────────────────────────
# KafkaProducer works with Redpanda because Redpanda is
# 100% Kafka-compatible — same protocol, same client library
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# ── Read loans from BigQuery ──────────────────────────────────
client = bigquery.Client(project=PROJECT_ID)

query = f"""
    SELECT
        loan_id,
        grade,
        loan_amount,
        interest_rate,
        loan_status,
        issue_date,
        mths_since_last_delinq,
        total_payment,
        outstanding_principal,
        risk_tier
    FROM `{PROJECT_ID}.lending_club_staging.stg_loans`
    LIMIT 5000
"""

print("Fetching loans from BigQuery...")
rows = list(client.query(query).result())
print(f"Fetched {len(rows):,} loans. Sending to Redpanda...")

# ── Send each loan as a message ───────────────────────────────
t0 = time()
for i, row in enumerate(rows):
    # Convert BigQuery row to plain dictionary
    message = dict(row)

    # Send to Redpanda topic
    producer.send(TOPIC, value=message)

    # Print progress every 1000 messages
    if (i + 1) % 1000 == 0:
        print(f"  Sent {i + 1:,} / {len(rows):,} messages...")

# Make sure all messages are actually sent before exiting
producer.flush()

t1 = time()
print(f"Done! Sent {len(rows):,} loan events in {t1 - t0:.2f} seconds")
print(f"Topic: {TOPIC} on {KAFKA_BOOTSTRAP}")
