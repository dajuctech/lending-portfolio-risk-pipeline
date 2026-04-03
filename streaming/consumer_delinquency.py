"""
DELINQUENCY EVENT CONSUMER
Reads loan events from Redpanda topic "loan-events" and
counts how many loans show a delinquency signal.

This is a simple consumer to verify messages are flowing
before running the full PyFlink jobs.

How to run:
    cd "08 project"
    uv run streaming/consumer_delinquency.py

Press Ctrl+C to stop.
"""

import json
from kafka import KafkaConsumer

# ── Configuration ─────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC           = "loan-events"

# ── Connect to Redpanda ───────────────────────────────────────
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    auto_offset_reset="earliest",       # start from the very first message
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# ── Process each message ──────────────────────────────────────
print(f"Listening to topic: {TOPIC}")
print("Press Ctrl+C to stop.\n")

total          = 0
delinquent     = 0
grade_counts   = {}

try:
    for message in consumer:
        loan = message.value
        total += 1

        # Count delinquent loans
        months = loan.get("mths_since_last_delinq")
        if months is not None and months > 0:
            delinquent += 1

        # Count by grade
        grade = loan.get("grade", "Unknown")
        grade_counts[grade] = grade_counts.get(grade, 0) + 1

        # Print summary every 500 messages
        if total % 500 == 0:
            rate = round(delinquent / total * 100, 1) if total > 0 else 0
            print(f"Processed: {total:,} | Delinquent: {delinquent:,} ({rate}%)")
            print(f"  By grade: {dict(sorted(grade_counts.items()))}")

except KeyboardInterrupt:
    print("\n── Final Summary ──────────────────────────────")
    print(f"Total loans processed : {total:,}")
    print(f"Delinquent loans      : {delinquent:,}")
    if total > 0:
        print(f"Delinquency rate      : {delinquent/total*100:.1f}%")
    print(f"Loans by grade        : {dict(sorted(grade_counts.items()))}")
