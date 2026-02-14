
import sqlite3
from datetime import datetime, timedelta
import random

# Connect/Create DB
conn = sqlite3.connect("local_simulation.db")
cursor = conn.cursor()

# Pipelines and configuration
pipelines = ["PIPE_SALES", "PIPE_INVENTORY", "PIPE_CUSTOMERS", "PIPE_FINANCE"]
tables_map = {
    "PIPE_SALES": ("SRC_SALES", "FACT_SALES"),
    "PIPE_INVENTORY": ("SRC_INV", "FACT_INVENTORY"),
    "PIPE_CUSTOMERS": ("SRC_CUST", "DIM_CUSTOMERS"),
    "PIPE_FINANCE": ("SRC_FIN", "FACT_FINANCE")
}

# ----------------------------
# 1. JOB TIMELINESS
# ----------------------------
cursor.execute("DROP TABLE IF EXISTS DIM_PIPELINE_JOB_TIMELINESS")
cursor.execute("""
CREATE TABLE DIM_PIPELINE_JOB_TIMELINESS (
    PIPELINE_NAME TEXT,
    RUN_ID INTEGER,
    JOB_NAME TEXT,
    JOB_START_TIME TEXT,
    END_TIME TEXT,
    EXECUTION_STATUS TEXT,
    PIPELINE_START_TIME TEXT
)
""")

# ----------------------------
# 2. CONTROL SOURCE
# ----------------------------
cursor.execute("DROP TABLE IF EXISTS DIM_PIPELINE_CONTROL_SOURCE")
cursor.execute("""
CREATE TABLE DIM_PIPELINE_CONTROL_SOURCE (
    RUN_ID INTEGER,
    PIPELINE_NAME TEXT,
    SOURCE_TABLE TEXT,
    PIPELINE_START_TIME TEXT,
    ROW_COUNT INTEGER,
    BYTES INTEGER
)
""")

# ----------------------------
# 3. CONTROL OUTPUT
# ----------------------------
cursor.execute("DROP TABLE IF EXISTS DIM_PIPELINE_CONTROL_OUTPUT_COMPLETENESS")
cursor.execute("""
CREATE TABLE DIM_PIPELINE_CONTROL_OUTPUT_COMPLETENESS (
    RUN_ID INTEGER,
    PIPELINE_NAME TEXT,
    SINK_TABLE TEXT,
    ROW_COUNT INTEGER
)
""")

# ----------------------------
# 4. CONTROL UNIQUENESS
# ----------------------------
cursor.execute("DROP TABLE IF EXISTS DIM_PIPELINE_CONTROL_UNIQUENESS")
cursor.execute("""
CREATE TABLE DIM_PIPELINE_CONTROL_UNIQUENESS (
    RUN_ID INTEGER,
    PIPELINE_NAME TEXT,
    PIPELINE_START_TIME TEXT,
    SINK_TABLE TEXT,
    DUPLICATE_COUNT INTEGER,
    DUPLICATE_PERCENTAGE REAL,
    DUPLICATE_THRESHOLD REAL
)
""")

# ----------------------------
# 5. CONTROL INTEGRITY
# ----------------------------
cursor.execute("DROP TABLE IF EXISTS DIM_PIPELINE_CONTROL_INTEGRITY")
cursor.execute("""
CREATE TABLE DIM_PIPELINE_CONTROL_INTEGRITY (
    RUN_ID INTEGER,
    PIPELINE_NAME TEXT,
    PIPELINE_START_TIME TEXT,
    NULL_COUNT INTEGER
)
""")

# Generate Data
now = datetime.utcnow()
run_counter = 1000

for i in range(50):
    pipeline = random.choice(pipelines)
    run_id = run_counter + i
    
    # Time logic
    days_ago = random.randint(0, 60)
    pipeline_start = now - timedelta(days=days_ago)
    job_start = pipeline_start + timedelta(minutes=random.randint(1, 10))
    
    # Status & Duration
    is_fail = random.random() < 0.15 # 15% fail rate
    status = "FAILED" if is_fail else "SUCCESS"
    
    # SLA Breach Logic
    duration_mins = random.randint(20, 90) if not is_fail else random.randint(5, 120)
    job_end = job_start + timedelta(minutes=duration_mins)
    
    job_name = f"LOAD_{pipeline.split('_')[1]}"
    
    # 1. Insert Timeliness
    cursor.execute("""
    INSERT INTO DIM_PIPELINE_JOB_TIMELINESS VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline, run_id, job_name, 
        job_start.isoformat(), job_end.isoformat(), 
        status, pipeline_start.isoformat()
    ))
    
    # 2. Insert Source
    src_table, sink_table = tables_map[pipeline]
    row_count = random.randint(10000, 500000)
    bytes_count = row_count * 100
    
    cursor.execute("""
    INSERT INTO DIM_PIPELINE_CONTROL_SOURCE VALUES (?, ?, ?, ?, ?, ?)
    """, (
        run_id, pipeline, src_table, pipeline_start.isoformat(), row_count, bytes_count
    ))
    
    # 3. Insert Output (If success)
    if status == "SUCCESS":
        out_rows = row_count - random.randint(0, 100) # Slight diff
        cursor.execute("""
        INSERT INTO DIM_PIPELINE_CONTROL_OUTPUT_COMPLETENESS VALUES (?, ?, ?, ?)
        """, (
            run_id, pipeline, sink_table, out_rows
        ))
        
        # 4. Insert Uniqueness
        dupes = random.randint(0, 500)
        dupe_pct = round((dupes / out_rows) * 100, 2)
        cursor.execute("""
        INSERT INTO DIM_PIPELINE_CONTROL_UNIQUENESS VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id, pipeline, pipeline_start.isoformat(), sink_table, dupes, dupe_pct, 1.0
        ))
        
        # 5. Insert Integrity
        nulls = random.randint(0, 200)
        cursor.execute("""
        INSERT INTO DIM_PIPELINE_CONTROL_INTEGRITY VALUES (?, ?, ?, ?)
        """, (
            run_id, pipeline, pipeline_start.isoformat(), nulls
        ))

conn.commit()
conn.close()
print("Local SQLite Simulation DB Created Successfully with 5 Tables.")
