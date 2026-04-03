"""
FLINK JOB Q4 — 5-MINUTE TUMBLING WINDOW BY GRADE
Reads loan events from Redpanda topic "loan-events".
Groups them into 5-minute tumbling windows.
Counts total loans and volume per grade per window.
Writes results to PostgreSQL table: grade_5min_window

How to submit this job to the Flink cluster:
    docker exec -it lending_jobmanager flink run \
        -py /opt/src/job/q4_tumbling_grade.py

Monitor at: http://localhost:8081
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# ── Set up Flink execution environment ───────────────────────
# StreamExecutionEnvironment = the entry point for all Flink jobs
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)          # 1 partition on our local topic
env.enable_checkpointing(10000) # checkpoint every 10 seconds (fault tolerance)

# StreamTableEnvironment = lets us use SQL syntax in PyFlink
t_env = StreamTableEnvironment.create(env)

# ── Create source table (reads from Redpanda) ─────────────────
# This is NOT a real database table — it is a connector definition
# Flink reads from the Redpanda topic as if it were a table
t_env.execute_sql("""
    CREATE TABLE loan_events_source (
        loan_id                 VARCHAR,
        grade                   VARCHAR,
        loan_amount             DOUBLE,
        interest_rate           DOUBLE,
        loan_status             VARCHAR,
        issue_date              VARCHAR,
        mths_since_last_delinq  INT,
        risk_tier               VARCHAR,
        event_time AS           PROCTIME()
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = 'loan-events',
        'properties.bootstrap.servers'  = 'redpanda:29092',
        'properties.group.id'           = 'flink-grade-consumer',
        'scan.startup.mode'             = 'earliest-offset',
        'format'                        = 'json',
        'json.ignore-parse-errors'      = 'true'
    )
""")

# ── Create sink table (writes to PostgreSQL) ──────────────────
# Results of each 5-minute window are written here
t_env.execute_sql("""
    CREATE TABLE grade_5min_window (
        window_start    TIMESTAMP(3),
        window_end      TIMESTAMP(3),
        grade           VARCHAR,
        total_loans     BIGINT,
        total_volume    DOUBLE,
        avg_interest    DOUBLE
    ) WITH (
        'connector'  = 'jdbc',
        'url'        = 'jdbc:postgresql://postgres:5432/lending_stream',
        'table-name' = 'grade_5min_window',
        'username'   = 'postgres',
        'password'   = 'postgres',
        'driver'     = 'org.postgresql.Driver'
    )
""")

# ── Run the tumbling window aggregation ───────────────────────
print("Starting Q4 tumbling window job...")

t_env.execute_sql("""
    INSERT INTO grade_5min_window
    SELECT
        TUMBLE_START(event_time, INTERVAL '5' MINUTE)  AS window_start,
        TUMBLE_END(event_time,   INTERVAL '5' MINUTE)  AS window_end,
        grade,
        COUNT(*)                                        AS total_loans,
        SUM(loan_amount)                                AS total_volume,
        ROUND(AVG(interest_rate), 2)                   AS avg_interest
    FROM loan_events_source
    GROUP BY
        TUMBLE(event_time, INTERVAL '5' MINUTE),
        grade
""").wait()
