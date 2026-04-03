"""
FLINK JOB Q5 — SESSION WINDOW FOR DELINQUENCY EVENTS
Reads loan events from Redpanda topic "loan-events".
Filters only delinquent/defaulted loans.
Groups them into session windows (5-minute gap threshold).
Writes results to PostgreSQL table: delinquency_sessions

How to submit this job to the Flink cluster:
    docker exec -it lending_jobmanager flink run \
        -py /opt/src/job/q5_session_window.py

Monitor at: http://localhost:8081
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# ── Set up Flink execution environment ───────────────────────
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(10000)

t_env = StreamTableEnvironment.create(env)

# ── Create source table (reads from Redpanda) ─────────────────
# Same source as Q4 — reads from the same loan-events topic
# Different consumer group ID so it reads independently from Q4
t_env.execute_sql("""
    CREATE TABLE loan_events_source (
        loan_id                 VARCHAR,
        grade                   VARCHAR,
        loan_amount             DOUBLE,
        interest_rate           DOUBLE,
        loan_status             VARCHAR,
        mths_since_last_delinq  INT,
        risk_tier               VARCHAR,
        event_time AS           PROCTIME()
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = 'loan-events',
        'properties.bootstrap.servers'  = 'redpanda:29092',
        'properties.group.id'           = 'flink-delinquency-consumer',
        'scan.startup.mode'             = 'earliest-offset',
        'format'                        = 'json',
        'json.ignore-parse-errors'      = 'true'
    )
""")

# ── Create sink table (writes to PostgreSQL) ──────────────────
t_env.execute_sql("""
    CREATE TABLE delinquency_sessions (
        session_start       TIMESTAMP(3),
        session_end         TIMESTAMP(3),
        grade               VARCHAR,
        events_in_session   BIGINT,
        total_volume        DOUBLE
    ) WITH (
        'connector'  = 'jdbc',
        'url'        = 'jdbc:postgresql://postgres:5432/lending_stream',
        'table-name' = 'delinquency_sessions',
        'username'   = 'postgres',
        'password'   = 'postgres',
        'driver'     = 'org.postgresql.Driver'
    )
""")

# ── Run the session window aggregation ────────────────────────
# Only processes loans that are delinquent or defaulted
# SESSION gap = 5 minutes: if no event for 5 mins, close session
print("Starting Q5 session window job...")

t_env.execute_sql("""
    INSERT INTO delinquency_sessions
    SELECT
        SESSION_START(event_time, INTERVAL '5' MINUTE)  AS session_start,
        SESSION_END(event_time,   INTERVAL '5' MINUTE)  AS session_end,
        grade,
        COUNT(*)                                         AS events_in_session,
        SUM(loan_amount)                                 AS total_volume
    FROM loan_events_source
    WHERE loan_status IN (
        'Charged Off',
        'Default',
        'Does not meet the credit policy. Status:Charged Off'
    )
    OR mths_since_last_delinq > 0
    GROUP BY
        SESSION(event_time, INTERVAL '5' MINUTE),
        grade
""").wait()
