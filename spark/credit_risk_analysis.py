"""
PYSPARK CREDIT RISK ANALYSIS
Reads loan data from the GCS parquet file and runs
large-scale batch aggregations using Apache Spark.

Why Spark instead of pandas?
- pandas loads everything into RAM on one machine
- Spark splits data across CPU cores and processes in parallel
- For 2M+ row datasets, Spark is significantly faster

How to run:
    cd "08 project"
    export JAVA_HOME=/usr/local/sdkman/candidates/java/21.0.9-ms
    export PATH=$JAVA_HOME/bin:$PATH
    export GOOGLE_APPLICATION_CREDENTIALS="credentials/gcp-service-account.json"
    export GCP_PROJECT_ID=$(grep ^GCP_PROJECT_ID .env | cut -d= -f2)
    uv run spark/credit_risk_analysis.py

NOTE on Java version:
    Spark requires Java 17 or 21. Java 25 (the codespace default) removed
    the getSubject API that Spark uses internally, causing an error.
    Always set JAVA_HOME to the 21.0.9-ms version before running Spark.
"""

import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Configuration ─────────────────────────────────────────────
PROJECT_ID   = os.environ.get("GCP_PROJECT_ID", "de-zoomcamp-2025-12345")
PARQUET_DIR  = "spark/data/"
PARQUET_FILE = f"{PARQUET_DIR}loans_2007_2011.parquet"
GCS_BUCKET   = f"{PROJECT_ID}-lending-club-raw"

# ── Create Spark Session ──────────────────────────────────────
# local[*] = use all available CPU cores on this machine
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("LendingClub Credit Risk Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("LENDING CLUB CREDIT RISK ANALYSIS — PYSPARK")
print("=" * 60)

# ── Download parquet from GCS if not already local ────────────
# We download once and cache locally to avoid repeated GCS calls
os.makedirs(PARQUET_DIR, exist_ok=True)

if not os.path.exists(PARQUET_FILE):
    print(f"\nDownloading parquet from GCS...")
    subprocess.run([
        "gcloud", "storage", "cp",
        f"gs://{GCS_BUCKET}/raw/loans_2007_2011.parquet",
        PARQUET_FILE
    ], check=True)
    print("Download complete.")
else:
    print(f"\nUsing cached parquet: {PARQUET_FILE}")

# ── Load parquet into Spark ───────────────────────────────────
print("Loading data into Spark...")
df = spark.read.parquet(PARQUET_FILE)

# cache() keeps data in memory — avoids re-reading for every query
df.cache()
total = df.count()
print(f"Loaded {total:,} rows")

# ── Clean int_rate — remove % sign before any aggregation ────
# The raw parquet has values like "10.65%" — Spark cannot average strings
# regexp_replace removes the % character, then cast to double
df = df.withColumn(
    "int_rate",
    F.regexp_replace(F.col("int_rate").cast("string"), "%", "").cast("double")
)

# ── Add derived columns ───────────────────────────────────────
# is_defaulted: 1 if loan went bad, 0 otherwise
df = df.withColumn(
    "is_defaulted",
    F.when(
        F.lower(F.col("loan_status")).isin(
            "charged off", "default",
            "does not meet the credit policy. status:charged off"
        ), 1
    ).otherwise(0)
)

# risk_tier: group grades into Low / Medium / High Risk
df = df.withColumn(
    "risk_tier",
    F.when(F.col("grade").isin("A", "B"), "Low Risk")
     .when(F.col("grade").isin("C", "D"), "Medium Risk")
     .when(F.col("grade").isin("E", "F", "G"), "High Risk")
     .otherwise("Unknown")
)

# ── Q1: Default Rate by Grade ─────────────────────────────────
print("\n=== Q1: Default Rate by Grade ===")
q1 = df.groupBy("grade") \
    .agg(
        F.count("*").alias("total_loans"),
        F.sum("is_defaulted").alias("total_defaults"),
        F.round(F.avg("is_defaulted") * 100, 2).alias("default_rate_pct"),
        F.round(F.avg("int_rate"), 2).alias("avg_interest_rate")
    ) \
    .orderBy("grade")
q1.show()

# ── Q2: Monthly Loan Volume ───────────────────────────────────
print("=== Q2: Monthly Loan Volume by Ingest Month ===")
# The raw parquet has `issue_date` (all null) — the original `issue_d`
# string column was not ingested. We use `ingest_ts` (the timestamp
# added by Kestra when the data was loaded) to group by month instead.
# This shows loan volume distribution by when records were ingested.
q2 = df.withColumn(
        "issue_month",
        F.date_trunc("month", F.col("ingest_ts").cast("timestamp"))
    ) \
    .groupBy("issue_month") \
    .agg(
        F.count("*").alias("total_loans"),
        F.round(F.sum("loan_amnt"), 2).alias("total_volume_usd"),
        F.round(F.avg("int_rate"), 2).alias("avg_interest_rate")
    ) \
    .orderBy("issue_month")
q2.show(20)

# ── Q3: DPD Bucket Distribution using RDD ────────────────────
# RDD = low-level Spark API — demonstrates map/reduce processing
print("=== Q3: DPD Bucket Distribution (RDD API) ===")

def dpd_bucket(months):
    """Classify loan by days past due bucket"""
    try:
        m = float(months) if months is not None else 0
    except:
        m = 0
    if m == 0:    return "1_Current"
    elif m <= 30: return "2_DPD 1-30"
    elif m <= 60: return "3_DPD 31-60"
    elif m <= 90: return "4_DPD 61-90"
    else:         return "5_DPD 90+"

rdd = df.select("mths_since_last_delinq").rdd \
    .map(lambda row: (dpd_bucket(row[0]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[0])

print(f"{'DPD Bucket':<15} {'Count':>10} {'Percentage':>12}")
print("-" * 40)
for bucket, count in rdd.collect():
    label = bucket[2:]
    pct = round(count / total * 100, 1)
    print(f"{label:<15} {count:>10,} {pct:>11.1f}%")

# ── Q4: Risk Tier Summary ─────────────────────────────────────
print("\n=== Q4: Risk Tier Summary ===")
q4 = df.groupBy("risk_tier") \
    .agg(
        F.count("*").alias("total_loans"),
        F.round(F.avg("is_defaulted") * 100, 2).alias("default_rate_pct"),
        F.round(F.sum("loan_amnt"), 2).alias("total_volume_usd")
    ) \
    .orderBy("risk_tier")
q4.show()

# ── Save results to Parquet ───────────────────────────────────
os.makedirs("spark/output", exist_ok=True)
print("Saving results to spark/output/...")
q1.write.mode("overwrite").parquet("spark/output/default_by_grade/")
q2.write.mode("overwrite").parquet("spark/output/monthly_volume/")
q4.write.mode("overwrite").parquet("spark/output/risk_tier_summary/")
print("Results saved to spark/output/")

spark.stop()
print("\nSpark analysis complete!")
