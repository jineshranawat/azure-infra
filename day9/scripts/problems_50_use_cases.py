"""Plain-English use cases for each of the 50 data-engineering problems (FinLedger UK)."""

USE_CASES: dict[str, str] = {
    "01": (
        "**FinLedger example:** Nightly ADF copy runs twice after a retry — silver has duplicate "
        "`transaction_id` rows and channel totals are doubled.\n\n"
        "**You run this when:** Before every silver write; after any pipeline re-run."
    ),
    "02": (
        "**FinLedger example:** Mobile card payments sync hours later; a 6pm aggregation must not "
        "miss them or count them twice.\n\n"
        "**You run this when:** Streaming aggregations or windowed batch jobs with late events."
    ),
    "03": (
        "**FinLedger example:** Core banking sends updates to existing payments; gold must apply "
        "only changes, not reload the full history.\n\n"
        "**You run this when:** Source publishes inserts/updates/deletes and you need incremental downstream."
    ),
    "04": (
        "**FinLedger example:** 90% of rows are `channel=card` — one Spark executor does all the work.\n\n"
        "**You run this when:** `groupBy` or join on a skewed key (channel, country, status)."
    ),
    "05": (
        "**FinLedger example:** Hourly micro-batches land 500 tiny files in silver — morning query takes 20 min.\n\n"
        "**You run this when:** Many small writes; after backfill loops; before heavy read jobs."
    ),
    "06": (
        "**FinLedger example:** Source adds `source_system` column mid-sprint; pipeline must not fail.\n\n"
        "**You run this when:** Upstream schema evolves; new optional columns appear."
    ),
    "07": (
        "**FinLedger example:** 3 rows have `amount_gbp='N/A'` — quarantine them; post the other 10,000.\n\n"
        "**You run this when:** Bad rows must not fail the whole batch; ops reviews audit folder."
    ),
    "08": (
        "**FinLedger example:** Compliance needs proof: null IDs = 0, negative amounts flagged before gold.\n\n"
        "**You run this when:** Every production load; metrics stored for auditors."
    ),
    "09": (
        "**FinLedger example:** `channel` is null on wire transfers — default to `UNKNOWN` so rollups work.\n\n"
        "**You run this when:** Nullable dimensions would break groupBy or joins."
    ),
    "10": (
        "**FinLedger example:** Source sends `card`, `CARD`, ` card ` — standardize before joining to dim.\n\n"
        "**You run this when:** Mixed casing, whitespace, or date formats in bronze."
    ),
    "11": (
        "**FinLedger example:** 5 years of transactions in one folder — filter one month still scans everything.\n\n"
        "**You run this when:** Table grows beyond single-folder performance; partition by `value_date`."
    ),
    "12": (
        "**FinLedger example:** Analyst queries `WHERE value_date = '2026-06-01'` — must skip other months.\n\n"
        "**You run this when:** Designing queries and verifying `partitionFilters` in explain plan."
    ),
    "13": (
        "**FinLedger example:** Same bronze→silver transform used in 5 cells — cache after first pass.\n\n"
        "**You run this when:** One DataFrame is reused for multiple actions in same job."
    ),
    "14": (
        "**FinLedger example:** Nightly job only processes rows since last successful watermark date.\n\n"
        "**You run this when:** Avoiding full bronze replay every night."
    ),
    "15": (
        "**FinLedger example:** Partner feed sends 200 updated amounts — MERGE into silver, not full replace.\n\n"
        "**You run this when:** Incremental upserts on `transaction_id`."
    ),
    "16": (
        "**FinLedger example:** Trainer sets `run_id` widget; ADF passes same param — no manual path edits.\n\n"
        "**You run this when:** Moving from laptop demo to scheduled Databricks Job."
    ),
    "17": (
        "**FinLedger example:** Row count and status logged to Delta — alert if bronze < expected.\n\n"
        "**You run this when:** Every job; morning check script reads metrics table."
    ),
    "18": (
        "**FinLedger example:** Logic fix requires reprocessing June 1–7 only — loop dates, not full history.\n\n"
        "**You run this when:** Backfill after bug fix; idempotent per-day writes."
    ),
    "19": (
        "**FinLedger example:** Streaming job restarts — checkpoint on ADLS resumes offset, no duplicates.\n\n"
        "**You run this when:** Structured Streaming; batch idempotency via MERGE keys."
    ),
    "20": (
        "**FinLedger example:** Events processed by arrival time skew totals — use `value_date` as event time.\n\n"
        "**You run this when:** Out-of-order payments across time zones."
    ),
    "21": (
        "**FinLedger example:** Micro-batch backlog grows — switch trigger to `availableNow` to catch up.\n\n"
        "**You run this when:** Streaming lag exceeds SLA."
    ),
    "22": (
        "**FinLedger example:** `collect()` on millions of rows kills driver — aggregate first, `show(20)`.\n\n"
        "**You run this when:** OOM errors; reviewing notebook anti-patterns."
    ),
    "23": (
        "**FinLedger example:** Enable AQE so Spark coalesces partitions after skewed shuffle.\n\n"
        "**You run this when:** Slow wide transformations; shuffle-heavy jobs."
    ),
    "24": (
        "**FinLedger example:** Join 1M facts to 5-row channel lookup — `broadcast(dim)`.\n\n"
        "**You run this when:** Dimension table fits in memory (<~10 MB)."
    ),
    "25": (
        "**FinLedger example:** Two huge fact tables on `channel` — co-partition or bucket to cut shuffle.\n\n"
        "**You run this when:** Both sides of join are large."
    ),
    "26": (
        "**FinLedger example:** Interactive cluster left on overnight — use job clusters + auto-terminate.\n\n"
        "**You run this when:** Cost review; designing production scheduling."
    ),
    "27": (
        "**FinLedger example:** Lab cluster autoscales 1–4 workers; terminates after 30 min idle.\n\n"
        "**You run this when:** Variable load; preventing idle DBU burn."
    ),
    "28": (
        "**FinLedger example:** SLA job cannot wait 8 min cold start — instance pool keeps VMs warm.\n\n"
        "**You run this when:** Sub-2-minute job start is required (trade-off: idle cost)."
    ),
    "29": (
        "**FinLedger example:** Only analysts with gold access see channel totals — UC GRANT on schema.\n\n"
        "**You run this when:** Onboarding users; separating bronze/silver/gold access."
    ),
    "30": (
        "**FinLedger example:** Analysts see aggregates only — masked view hides `transaction_id`.\n\n"
        "**You run this when:** PII columns must not appear in self-service SQL."
    ),
    "31": (
        "**FinLedger example:** Storage key in `.env` + scope `finledger` — notebooks never paste secrets.\n\n"
        "**You run this when:** Once per laptop; `session-3\\orchestrate.cmd --setup-secrets`."
    ),
    "32": (
        "**FinLedger example:** Compliance asks which report uses `silver.transactions` — UC lineage tab.\n\n"
        "**You run this when:** Impact analysis before schema change; Purview registration."
    ),
    "33": (
        "**FinLedger example:** New engineer reads UC column comment on `transaction_id` — no Slack hunt.\n\n"
        "**You run this when:** Every gold table; link to repo README."
    ),
    "34": (
        "**FinLedger example:** Team A uses `amount`, Team B `amount_gbp` — gold uses one canonical name.\n\n"
        "**You run this when:** Publishing gold layer; data dictionary alignment."
    ),
    "35": (
        "**FinLedger example:** Today's average payment 3× last week — drift profile flags upstream bug.\n\n"
        "**You run this when:** Append run metrics; compare week-over-week."
    ),
    "36": (
        "**FinLedger example:** Customer channel changes — SCD2 keeps history with `effective_from/to`.\n\n"
        "**You run this when:** Point-in-time reporting on slowly changing dimensions."
    ),
    "37": (
        "**FinLedger example:** FX rate table refreshed daily — small dim full overwrite.\n\n"
        "**You run this when:** Reference data < 100k rows; daily refresh."
    ),
    "38": (
        "**FinLedger example:** Partner API timeout — retry 3× with exponential backoff before DLQ.\n\n"
        "**You run this when:** Any external HTTP/REST ingest from notebook or driver."
    ),
    "39": (
        "**FinLedger example:** API allows 100 req/min — throttle batches to avoid HTTP 429.\n\n"
        "**You run this when:** Calling external APIs from pipeline driver."
    ),
    "40": (
        "**FinLedger example:** Landing file truncated — checksum mismatch before bronze ingest.\n\n"
        "**You run this when:** File lands from partner SFTP; validate before parse."
    ),
    "41": (
        "**FinLedger example:** All `value_date` stored as UTC — reports consistent globally.\n\n"
        "**You run this when:** Multi-region bank; regulatory timestamps."
    ),
    "42": (
        "**FinLedger example:** Display London time to UK users from UTC storage.\n\n"
        "**You run this when:** Dashboards need local timezone; storage stays UTC."
    ),
    "43": (
        "**FinLedger example:** Payment references `channel` not in dim — left anti-join finds orphans.\n\n"
        "**You run this when:** FK validation before gold publish."
    ),
    "44": (
        "**FinLedger example:** ADF retries pipeline — same `run_id` MERGE does not double-count.\n\n"
        "**You run this when:** Any retry-safe pipeline design."
    ),
    "45": (
        "**FinLedger example:** ETL selects only needed columns early — 40% faster silver job.\n\n"
        "**You run this when:** Profiling slow notebooks; removing UDFs and collect()."
    ),
    "46": (
        "**FinLedger example:** Delta Parquet snappy compression cuts ADLS scan cost.\n\n"
        "**You run this when:** Storage bill review; writing large historical archives."
    ),
    "47": (
        "**FinLedger example:** Bronze raw kept 90 days then cool tier — GDPR + cost.\n\n"
        "**You run this when:** ADLS lifecycle policy design; Delta VACUUM after retention."
    ),
    "48": (
        "**FinLedger example:** UC tags `classification=internal` on payments table for Purview.\n\n"
        "**You run this when:** Governance sprint; audit prep."
    ),
    "49": (
        "**FinLedger example:** Analyst runs bad DELETE — `RESTORE TABLE` to version before mistake.\n\n"
        "**You run this when:** Operational recovery; DR drill."
    ),
    "50": (
        "**FinLedger example:** One table shows bronze rows, UC tables created, job status — morning dashboard.\n\n"
        "**You run this when:** End of this notebook; template for daily ops check."
    ),
}

# Group labels for navigation in the notebook
PROBLEM_GROUPS: list[tuple[str, str, list[str]]] = [
    ("A", "Data quality & cleansing (1–10)", [f"{i:02d}" for i in range(1, 11)]),
    ("B", "Scale & performance (11–15)", [f"{i:02d}" for i in range(11, 16)]),
    ("C", "Operations & reliability (16–20)", [f"{i:02d}" for i in range(16, 21)]),
    ("D", "Spark tuning (21–25)", [f"{i:02d}" for i in range(21, 26)]),
    ("E", "Cost & infrastructure (26–28)", [f"{i:02d}" for i in range(26, 29)]),
    ("F", "Security & governance (29–33)", [f"{i:02d}" for i in range(29, 34)]),
    ("G", "Analytics & dimensions (34–37)", [f"{i:02d}" for i in range(34, 38)]),
    ("H", "Integration & integrity (38–44)", [f"{i:02d}" for i in range(38, 45)]),
    ("I", "Optimization & lifecycle (45–47)", [f"{i:02d}" for i in range(45, 48)]),
    ("J", "Governance & ops (48–50)", [f"{i:02d}" for i in range(48, 51)]),
    ("K", "Medallion + SQL metadata + Purview capstone", ["capstone"]),
]
