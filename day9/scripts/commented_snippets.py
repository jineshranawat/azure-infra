"""Heavily commented PySpark snippets for Day 9 notebook cells."""

TYPED_SPLIT_CODE = '''# =============================================================================
# Cast bronze strings → double, then split GOOD vs QUARANTINE
# =============================================================================
# FinLedger rule:
#   silver_good  → amount IS NOT NULL AND amount > 0
#   silver_bad   → amount IS NULL OR amount <= 0  (goes to audit container)
# =============================================================================

# Step 1 — CAST (lazy transformation — no data moved yet)
typed = df.withColumn(
    "amount",
    F.col("amount_gbp").cast("double"),  # "INVALID" text becomes null
)

# Step 2 — Demo bad row (watch-only lab: quarantine folder must not be empty)
# In production, bad rows arrive naturally from messy source files.
bad_row = spark.createDataFrame(
    [("TXN-BAD", "ACC-1", "INVALID", "2026-06-02", "wire", "pending")],
    "transaction_id string, account_id string, amount_gbp string, "
    "value_date string, channel string, status string",
)
bronze_demo = bronze.unionByName(bad_row, allowMissingColumns=True)
typed = bronze_demo.withColumn("amount", F.col("amount_gbp").cast("double"))

# Step 3 — SPLIT (two lazy filters — pipeline never fails on bad data)
silver_good = typed.filter(
    F.col("amount").isNotNull() & (F.col("amount") > 0)
)
silver_bad = typed.filter(
    F.col("amount").isNull() | (F.col("amount") <= 0)
)

# Step 4 — ACTION: count triggers cluster execution for both branches
print("good:", silver_good.count(), "| quarantine:", silver_bad.count())'''

WRITE_SILVER_CODE = '''# =============================================================================
# WRITE silver — ACTION (data physically lands on ADLS)
# =============================================================================
# .format("delta")  → Parquet files + _delta_log (ACID) — see Session 10
# .mode("overwrite") → replace this run= folder snapshot (batch default)
# .save(path)       → ACTION — runs all lazy steps above, then writes
# =============================================================================

(
    silver_good.write
    .format("delta")       # Delta Lake format (not plain CSV)
    .mode("overwrite")     # idempotent re-run for same RUN_ID folder
    .save(SILVER_PATH)     # abfss://silver@.../transactions/run=session3-lab
)

print("Silver written to:", SILVER_PATH)'''

VERIFY_SILVER_CODE = '''# =============================================================================
# VERIFY — read Delta back (proves round-trip to the lake)
# =============================================================================
back = spark.read.format("delta").load(SILVER_PATH)
print("Read back rows:", back.count())
back.select("transaction_id", "channel", "amount", "status").show(truncate=False)'''

WRITE_QUARANTINE_CODE = '''# =============================================================================
# WRITE quarantine — bad rows to audit container (pipeline still succeeded)
# =============================================================================
# WHY separate container: governance team reviews audit/ without touching silver/
# =============================================================================

(
    silver_bad.write
    .format("delta")
    .mode("overwrite")
    .save(QUARANTINE_PATH)  # abfss://audit@.../quarantine/run=session3-lab
)

print("Quarantine written to:", QUARANTINE_PATH)
silver_bad.show(truncate=False)  # trainer: show TXN-BAD row landed here'''

CAST_PREVIEW_CODE = '''# Preview cast — see string vs double side by side
typed_preview = df.withColumn("amount", F.col("amount_gbp").cast("double"))
typed_preview.select("transaction_id", "amount_gbp", "amount").show(truncate=False)'''

BRIDGE_PRINT = '''print("Day 8 = kitchen prep (transform in memory).")
print("Session 9 = pack boxes and ship to warehouse (write to ADLS).")'''
