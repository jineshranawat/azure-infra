# Databricks notebook source
# MAGIC %md
# MAGIC # FinLedger Event Hub Lab — step by step
# MAGIC
# MAGIC **For someone new to Event Hubs.** Read every markdown cell before you click Run.
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Workspace** | `/Shared/day9/eventhub_finledger_lab` |
# MAGIC | **Hub (Azure)** | `ehns-shared-finledger` / `finledger-payments` |
# MAGIC | **Secrets** | Databricks scope `finledger` → `eventhub-*` keys |
# MAGIC | **One-time setup** | From your laptop: `python scripts/ensure_eventhub_lab.py` |
# MAGIC | **Compute** | Classic cluster preferred; Serverless OK for this SDK path |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Big picture — one kitchen story
# MAGIC
# MAGIC Imagine **FinLedger Bank** at lunch rush.
# MAGIC
# MAGIC | Kitchen role | Event Hub idea | In this lab |
# MAGIC |--------------|----------------|-------------|
# MAGIC | Waiter writes a ticket | **Producer / SEND** | Our Python code builds a small JSON “payment pulse” |
# MAGIC | Ticket rail / spike | **Event Hub** | Azure holds tickets in order for a while |
# MAGIC | Kitchen cooks pick tickets | **Consumer / RECEIVE** | Our code reads tickets back |
# MAGIC | Head chef totals sales | **PROCESS (Spark)** | Clean + groupBy channel → chart |
# MAGIC
# MAGIC ```text
# MAGIC  WAITERS (apps / banks)          TICKET RAIL                 KITCHEN (Databricks)
# MAGIC  -----------------------         ------------                --------------------
# MAGIC  Write payment tickets    ──►    Event Hub keeps them  ──►   Read tickets
# MAGIC                                  for ~24 hours               Spark totals GBP
# MAGIC                                                              by channel
# MAGIC ```
# MAGIC
# MAGIC **One sentence to remember:**  
# MAGIC *Event Hub is the ticket rail. Spark is the kitchen. Secrets are the staff-only key to the door.*
# MAGIC
# MAGIC You do **not** phone the waiter after cooking — the rail remembers recent tickets (that memory is called **retention**).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning outcomes (say these out loud at the end)
# MAGIC
# MAGIC 1. Draw namespace → hub → partition on a whiteboard in 30 seconds  
# MAGIC 2. Explain why the **connection string** is a hotel key card, not a sticky note in git  
# MAGIC 3. Send a dummy FinLedger payment onto the hub  
# MAGIC 4. Receive it and turn it into a Spark table  
# MAGIC 5. Aggregate GBP by channel (same Spark you already know)  
# MAGIC 6. Name one thing you’d change for a real overnight bank job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Vocabulary (kitchen / hotel / highway)
# MAGIC
# MAGIC Read this table slowly. Every word appears again in code.
# MAGIC
# MAGIC ### 1. Namespace — the restaurant building
# MAGIC
# MAGIC **Word:** Namespace (`ehns-shared-finledger`)
# MAGIC
# MAGIC **Easy analogy:** The **whole restaurant building** on the street.  
# MAGIC Address of the kitchen, manager’s office, and ticket rails all start here.
# MAGIC
# MAGIC **Detail:** In Azure this is a **resource**. Cheap lab SKU = **Basic**.  
# MAGIC One namespace can hold several hubs (several ticket rails).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Event Hub — one ticket rail
# MAGIC
# MAGIC **Word:** Event Hub / hub (`finledger-payments`)
# MAGIC
# MAGIC **Easy analogy:** **One named rail** inside the restaurant: “Payments only.”  
# MAGIC Another rail might be “Refunds only” (we don’t create that in this lab).
# MAGIC
# MAGIC **Detail:** Producers **append** messages. Consumers **read** them.  
# MAGIC Messages are tiny JSON documents — our “kitchen tickets.”
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Partition — lanes on the highway
# MAGIC
# MAGIC **Word:** Partition (we use **2**)
# MAGIC
# MAGIC **Easy analogy:** A highway with **2 lanes**. Cars (messages) don’t jump lanes.  
# MAGIC Two cooks can watch two lanes and go faster without mixing order *inside* a lane.
# MAGIC
# MAGIC **Detail:** Order is guaranteed **per partition**, not across the whole hub.  
# MAGIC More partitions ⇒ more parallel readers. Lab keeps **2** to stay cheap and simple.
# MAGIC
# MAGIC ```text
# MAGIC   Hub "finledger-payments"
# MAGIC   ┌─────────────┬─────────────┐
# MAGIC   │ Partition 0 │ Partition 1 │   ← two lanes
# MAGIC   │  msg msg    │  msg msg    │
# MAGIC   └─────────────┴─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Connection string / SAS — hotel key card
# MAGIC
# MAGIC **Word:** SAS (Shared Access Signature) / connection string
# MAGIC
# MAGIC **Easy analogy:** A **hotel key card**.  
# MAGIC - **Send** right = allowed to drop tickets on the rail  
# MAGIC - **Listen** right = allowed to read tickets off the rail  
# MAGIC
# MAGIC Our lab rule `lab-sendlisten` has **both**.
# MAGIC
# MAGIC **Detail:** The long string starts with `Endpoint=sb://...`.  
# MAGIC Treat it like a password. Store it in **Databricks secrets**, never in chat, git, or screenshots.
# MAGIC
# MAGIC **Analogy stretch:** If you laminate the key card to the classroom wall (hard-code in notebook), anyone with the screenshot can cook for free in your restaurant — forever until you cancel the card.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Consumer group — named team of cooks
# MAGIC
# MAGIC **Word:** Consumer group (`$Default` in this lab)
# MAGIC
# MAGIC **Easy analogy:** A **sticky label on the clipboard**: “Team Red kitchen.”  
# MAGIC Team Red and Team Blue can each read the *same* tickets for different jobs  
# MAGIC (one builds silver tables, one builds fraud scores) without stealing each other’s place.
# MAGIC
# MAGIC **Detail:** Each group tracks “how far we have read” separately.  
# MAGIC Lab uses Azure’s built-in **`$Default`** group so we don’t create extras.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. Retention — how long tickets stay on the spike
# MAGIC
# MAGIC **Word:** Retention (lab ≈ **24 hours**)
# MAGIC
# MAGIC **Easy analogy:** Tickets stay on the rail until closing + a bit.  
# MAGIC Tomorrow morning the oldest tickets are cleared — you can’t re-cook last month’s lunch from the rail alone.
# MAGIC
# MAGIC **Detail:** For history forever you write to **Delta / lake**.  
# MAGIC Event Hub is a **buffer**, not the archive.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. Producer vs consumer — who writes vs who reads
# MAGIC
# MAGIC | Role | Kitchen | Code in this notebook |
# MAGIC |------|---------|------------------------|
# MAGIC | **Producer** | Waiter | Step 5 — `EventHubProducerClient` |
# MAGIC | **Consumer** | Cook | Step 6 — `EventHubConsumerClient` |
# MAGIC | **Processor** | Head chef with calculator | Step 7 — Spark DataFrame |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. Message / event / pulse — one ticket
# MAGIC
# MAGIC **Word:** Message / event / “payment pulse”
# MAGIC
# MAGIC **Easy analogy:** **One paper ticket** with fields: who paid, how much, which channel, when.
# MAGIC
# MAGIC **Detail:** We use JSON so both Python and Spark can read the same shape.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Guardrail (say with the class)
# MAGIC
# MAGIC > Secrets live in scope `finledger`. We print **names and lengths only**, never the key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Prerequisites (open the restaurant once)
# MAGIC
# MAGIC **WHAT:** Create Azure Event Hub + put the key card into Databricks.  
# MAGIC **WHY:** Students should not click the Azure portal for every class.  
# MAGIC **HOW:** One idempotent Windows command from the trainer laptop.
# MAGIC
# MAGIC ```cmd
# MAGIC cd d:\azure
# MAGIC python scripts\ensure_eventhub_lab.py
# MAGIC ```
# MAGIC
# MAGIC **Analogy:** Before service, the manager unlocks the building, hangs the payments rail,  
# MAGIC and puts spare key cards in the staff safe (`finledger` secret scope).  
# MAGIC Running the script again is safe — it won’t build a second building.
# MAGIC
# MAGIC Then: open this notebook → attach compute → read markdown → Run cell by cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Load secrets (open the staff safe)
# MAGIC
# MAGIC **WHAT:** Read the hotel key card + hub name into variables.  
# MAGIC **WHY:** Rotation and audits — trainers can replace the key without editing notebooks.  
# MAGIC **HOW:** `dbutils.secrets.get` from scope `finledger`.  
# MAGIC
# MAGIC **Analogy:** You unlock the safe, check the card is there, and put it in your pocket.  
# MAGIC You do **not** photograph the card or write it on the whiteboard.
# MAGIC
# MAGIC If the safe is empty, the notebook still teaches Steps 4–8 with an **in-memory bus**  
# MAGIC (like practicing tickets on a classroom spike before the real rail is installed).

# COMMAND ----------

# =============================================================================
# STEP 2 — Secrets
# =============================================================================
# WHAT: Load Event Hub door key from Databricks secret scope.
# WHY:  Never paste connection strings into notebook cells or git.
# HOW:  finledger/eventhub-connection-string (from ensure_eventhub_lab.py)

SECRET_SCOPE = "finledger"

dbutils.widgets.text("run_id", "session3-lab", "Lab run id (tag on each event)")
RUN_ID = dbutils.widgets.get("run_id").strip() or "session3-lab"

try:
    EH_CONN = dbutils.secrets.get(scope=SECRET_SCOPE, key="eventhub-connection-string").strip()
    EH_NAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="eventhub-name").strip()
    EH_NS = dbutils.secrets.get(scope=SECRET_SCOPE, key="eventhub-namespace").strip()
    print("Secrets OK")
    print("  namespace :", EH_NS)
    print("  hub       :", EH_NAME)
    print("  conn chars:", len(EH_CONN), "(value hidden)")
    print("  run_id    :", RUN_ID)
except Exception as exc:
    EH_CONN = None
    EH_NAME = "finledger-payments"
    EH_NS = "ehns-shared-finledger"
    print("MISSING SECRETS — run on laptop: python scripts/ensure_eventhub_lab.py")
    print("Detail:", str(exc)[:160])
    print("This notebook will still DEMO the workflow with an in-memory bus.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Install the kitchen radio (`azure-eventhub`)
# MAGIC
# MAGIC **WHAT:** A Python library that knows Azure’s Event Hub language.  
# MAGIC **WHY:** Without it, Databricks doesn’t speak “ticket rail.”  
# MAGIC **HOW:** Use **`%pip install`** so the radio is installed into **this** classroom laptop (Serverless kernel).
# MAGIC
# MAGIC **Analogy:** `%pip` = plugging a radio into *this* booth.  
# MAGIC `subprocess pip` on Serverless often plugs a radio into the *storage room* — you still hear silence in class.
# MAGIC
# MAGIC After `%pip`, Python may restart — treat it like a brief power cut: re-run **Step 2** (secrets) then import.

# COMMAND ----------

# MAGIC %pip install azure-eventhub --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3b — Import the SDK
# MAGIC
# MAGIC Run this **after** the `%pip` cell succeeds. If import still fails, detach/re-attach compute and re-run `%pip`.

# COMMAND ----------

# =============================================================================
# STEP 3b — Import SDK (after %pip cell above)
# =============================================================================
# WHAT: Bring producer/consumer classes into this kernel.
# WHY:  %pip installs into the notebook env; plain subprocess often does not on Serverless.
# HOW:  Import once; fail with a clear message if %pip was skipped.

try:
    from azure.eventhub import EventData, EventHubProducerClient, EventHubConsumerClient
    print("OK — azure.eventhub imported")
    print("  EventData, EventHubProducerClient, EventHubConsumerClient ready")
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "azure.eventhub still missing. Fix: run the %pip cell above, wait until it finishes, "
        "then run this cell again. On Serverless do NOT use subprocess pip."
    ) from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Design the ticket (JSON contract)
# MAGIC
# MAGIC **WHAT:** Agree the **fields** every payment ticket must have.  
# MAGIC **WHY:** Cooks hate tickets that say “the usual” with no price. Downstream Spark needs stable column names.  
# MAGIC **HOW:** One Python `dict` shape for every event.
# MAGIC
# MAGIC **Analogy:** The restaurant prints **one blank ticket form**. Every waiter fills the same boxes:
# MAGIC
# MAGIC | Field | Kitchen meaning |
# MAGIC |-------|-----------------|
# MAGIC | `event_type` | Kind of ticket (`payment_pulse`) |
# MAGIC | `transaction_id` | Order number |
# MAGIC | `amount_gbp` | Price |
# MAGIC | `channel` | How they paid (CARD / WIRE / …) |
# MAGIC | `event_ts` | Time stamped on the ticket |
# MAGIC | `run_id` | Which class/lab service (so demos don’t collide) |
# MAGIC | `seq` | Line number on the pad |
# MAGIC
# MAGIC If you change the form later, **tell the kitchen** (schema evolution) — don’t surprise them.

# COMMAND ----------

# =============================================================================
# STEP 4 — Message contract + dummy payloads
# =============================================================================
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

# FinLedger-shaped dummy pulses (enough to see a chart)
DUMMY_EVENTS = [
    {
        "event_type": "payment_pulse",
        "transaction_id": f"EH-{RUN_ID}-001",
        "amount_gbp": 1250.50,
        "channel": "WIRE",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "seq": 1,
    },
    {
        "event_type": "payment_pulse",
        "transaction_id": f"EH-{RUN_ID}-002",
        "amount_gbp": 89.99,
        "channel": "CARD",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "seq": 2,
    },
    {
        "event_type": "payment_pulse",
        "transaction_id": f"EH-{RUN_ID}-003",
        "amount_gbp": 40.00,
        "channel": "CARD",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "seq": 3,
    },
    {
        "event_type": "payment_pulse",
        "transaction_id": f"EH-{RUN_ID}-004",
        "amount_gbp": 500.00,
        "channel": "FPS",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "seq": 4,
    },
    {
        "event_type": "payment_pulse",
        "transaction_id": f"EH-{RUN_ID}-005",
        "amount_gbp": 12.40,
        "channel": "BACS",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "seq": 5,
    },
    {
        "event_type": "payment_pulse",
        "transaction_id": f"EH-{RUN_ID}-006",
        "amount_gbp": 220.00,
        "channel": "WIRE",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "seq": 6,
    },
]

print("Contract fields:", sorted(DUMMY_EVENTS[0].keys()))
print("Dummy count   :", len(DUMMY_EVENTS))
print("Sample JSON   :")
print(json.dumps(DUMMY_EVENTS[0], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — SEND (waiters drop tickets on the rail)
# MAGIC
# MAGIC **WHAT:** Push each JSON ticket onto Event Hub.  
# MAGIC **WHY:** Landing events is separate from cooking them — outages in Spark don’t lose the waiters’ work (while retention lasts).  
# MAGIC **HOW:** `EventHubProducerClient` → fill a **batch** of tickets → `send_batch`.
# MAGIC
# MAGIC **Analogy:** Waiters don’t walk into the kitchen for every order. They **spike tickets on the rail**.  
# MAGIC Batching = clipping several tickets together before walking to the rail (faster, fewer trips).
# MAGIC
# MAGIC If the real rail isn’t unlocked (no secrets), we practice on a **classroom spike** (in-memory list) with the **same tickets**.

# COMMAND ----------

# =============================================================================
# STEP 5 — SEND
# =============================================================================
# WHAT: Drop JSON messages onto Event Hub (or in-memory bus).
# WHY:  Separates "land events" from "process events".
# HOW:  Producer client + batch send.

EH_SENT = []
EH_MODE = "simulated"

if EH_CONN:
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EH_CONN,
            eventhub_name=EH_NAME,
        )
        with producer:
            batch = producer.create_batch()
            for ev in DUMMY_EVENTS:
                payload = json.dumps(ev)
                # EventData body = bytes/string of our JSON contract
                try:
                    batch.add(EventData(payload))
                except ValueError:
                    # Batch full — send and start a new one
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(EventData(payload))
                EH_SENT.append(ev)
            if len(batch) > 0:
                producer.send_batch(batch)
        EH_MODE = "azure_eventhub"
        print(f"SUCCESS — sent {len(EH_SENT)} events to hub '{EH_NAME}'")
        print("          namespace:", EH_NS)
    except Exception as exc:
        print("SEND failed — using in-memory bus. Detail:", str(exc)[:180])
        EH_SENT = list(DUMMY_EVENTS)
        EH_MODE = "simulated_after_error"
else:
    EH_SENT = list(DUMMY_EVENTS)
    print(f"SIMULATED — {len(EH_SENT)} events kept in memory (no connection string)")

print("EH_MODE =", EH_MODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — RECEIVE (cooks take tickets off the rail)
# MAGIC
# MAGIC **WHAT:** Listen for a short time and collect tickets into a Python list.  
# MAGIC **WHY:** Prove the round trip. Real banks keep a cook on duty 24/7; we listen ~15 seconds so **Run all** finishes in class.  
# MAGIC **HOW:** `EventHubConsumerClient.receive` + callback `on_event`.
# MAGIC
# MAGIC **Analogy:** A cook stands at the rail, unclips tickets, and stacks them on a tray.  
# MAGIC `consumer group $Default` = “this kitchen’s clipboard.”  
# MAGIC `starting_position="-1"` (lab) = “read from the oldest ticket still on the spike” so demos are easy.
# MAGIC
# MAGIC **Production note:** Real cooks use a **bookmark** (checkpoint) so after a break they continue from the next unread ticket — not from the beginning every time.

# COMMAND ----------

# =============================================================================
# STEP 6 — RECEIVE
# =============================================================================
# WHAT: Listen for a short window and collect JSON dicts.
# WHY:  Students see both sides of the belt without a 24/7 job.
# HOW:  Consumer group $Default; stop after max_wait_time seconds.

received = []

if EH_MODE.startswith("azure") and EH_CONN:
    try:

        def on_event(partition_context, event):
            """Called once per message. Keep logic tiny — parse only."""
            try:
                body = event.body_as_str(encoding="UTF-8")
                received.append(json.loads(body))
            except Exception as parse_exc:
                print("  skip bad body:", str(parse_exc)[:80])

        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=EH_CONN,
            consumer_group="$Default",
            eventhub_name=EH_NAME,
        )
        print("Listening up to 15 seconds ...")
        with consumer:
            consumer.receive(
                on_event=on_event,
                starting_position="-1",  # lab: from earliest available
                max_wait_time=15,
            )
        print(f"Received {len(received)} events from Azure Event Hub")
        if not received:
            print("Nothing received in window — falling back to what we just sent")
            received = list(EH_SENT)
    except Exception as exc:
        print("RECEIVE failed — using sent list. Detail:", str(exc)[:180])
        received = list(EH_SENT)
else:
    received = list(EH_SENT)
    print(f"Using in-memory bus — {len(received)} events")

print("First received (truncated):", json.dumps(received[0], indent=2) if received else None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — PROCESS (head chef + calculator = Spark)
# MAGIC
# MAGIC **WHAT:** Tickets → Spark table → clean → totals by channel.  
# MAGIC **WHY:** Event Hub only **moves** data. Business rules still live in Spark (or SQL) like every FinLedger lab.  
# MAGIC **HOW:** `createDataFrame` → trim/cast/filter → `groupBy` → save tables.
# MAGIC
# MAGIC **Analogy:**  
# MAGIC - **Raw** = tickets still greasy from the rail  
# MAGIC - **Silver** = tickets rewritten neatly (types fixed, blanks removed)  
# MAGIC - **KPI / gold-ish** = board on the wall: “CARD sold £X, WIRE sold £Y”
# MAGIC
# MAGIC You already know these Spark verbs from teach-all / 50-problems —
# MAGIC Event Hub was only the **door** the tickets came through.

# COMMAND ----------

# =============================================================================
# STEP 7 — PROCESS
# =============================================================================
# WHAT: Event Hub exit ramp → Spark DataFrame → silver + KPI.
# WHY:  Business logic lives here, not in the producer.
# HOW:  Standard DataFrame verbs; save to advanced_lab if allowed.

if not received:
    raise RuntimeError("No events to process — re-run Step 5 (send) then Step 6.")

rows = [
    (
        e.get("transaction_id"),
        float(e.get("amount_gbp", 0)),
        e.get("channel"),
        e.get("event_ts"),
        e.get("run_id"),
        int(e.get("seq", 0)),
    )
    for e in received
]

raw = spark.createDataFrame(
    rows,
    ["transaction_id", "amount_gbp", "channel", "event_ts", "run_id", "seq"],
)
print("Raw rows:", raw.count())
display(raw)

# Silver: types + quality (same idea as medallion Session 9)
silver = (
    raw.withColumn("event_ts_ts", F.to_timestamp("event_ts"))
    .withColumn("channel_std", F.upper(F.trim(F.col("channel"))))
    .filter(F.col("amount_gbp") > 0)
    .dropDuplicates(["transaction_id", "seq"])
)
print("Silver rows:", silver.count())
display(silver)

# Gold-ish KPI for the whiteboard
kpi = (
    silver.groupBy("channel_std")
    .agg(
        F.count("*").alias("pulses"),
        F.round(F.sum("amount_gbp"), 2).alias("total_gbp"),
    )
    .orderBy(F.desc("total_gbp"))
)
display(kpi)

# Persist for Catalog browser (soft)
spark.sql("CREATE DATABASE IF NOT EXISTS eventhub_lab")
try:
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("eventhub_lab.payment_pulses")
    )
    (
        kpi.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("eventhub_lab.channel_kpi")
    )
    print("Saved: eventhub_lab.payment_pulses , eventhub_lab.channel_kpi")
except Exception as exc:
    print("Table save skipped (permissions):", str(exc)[:140])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Visual check (bar chart)
# MAGIC
# MAGIC **WHAT:** One chart so the class *sees* channels.  
# MAGIC **WHY:** Dashboards start from the same KPI frame you’d give Power BI later.

# COMMAND ----------

# =============================================================================
# STEP 8 — CHART
# =============================================================================
import matplotlib.pyplot as plt

pdf = kpi.toPandas()
fig, ax = plt.subplots(figsize=(8, 4))
ax.bar(pdf["channel_std"], pdf["total_gbp"], color="#2E86AB")
ax.set_title(f"FinLedger Event Hub pulses — GBP by channel ({EH_MODE})")
ax.set_xlabel("channel")
ax.set_ylabel("total_gbp")
fig.tight_layout()
display(fig)
print("Chart done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Production map (read this — no code)
# MAGIC
# MAGIC What we did today vs what banks do overnight:
# MAGIC
# MAGIC | Lab (this notebook) | Production |
# MAGIC |---------------------|------------|
# MAGIC | SDK send + short receive | Continuous `readStream.format("eventhubs")` |
# MAGIC | In-memory / 15s listen | Checkpoint folder on ADLS (exactly-once offsets) |
# MAGIC | `saveAsTable` overwrite | MERGE / append silver by event time |
# MAGIC | Manual Run all | Databricks Job or ADF trigger |
# MAGIC | Basic SKU / 2 partitions | Sized TUs + more partitions for throughput |
# MAGIC
# MAGIC ```text
# MAGIC [Card network / FPS] → Event Hub → Spark Structured Streaming
# MAGIC                                       → Delta silver → gold KPI → dashboard
# MAGIC ```
# MAGIC
# MAGIC **Interview one-liner:** *“Event Hub is the durable buffer; Spark is the processor;
# MAGIC Delta is the system of record.”*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Troubleshooting
# MAGIC
# MAGIC | Symptom | Fix |
# MAGIC |---------|-----|
# MAGIC | Missing secrets | `python scripts\ensure_eventhub_lab.py` then re-run Step 2 |
# MAGIC | SEND / RECEIVE fail | Check hub name; re-run ensure; wait 1 min after create |
# MAGIC | Empty receive | Normal if retention/cursor odd — fallback uses sent list |
# MAGIC | `pip` / ModuleNotFoundError | Run the **`%pip install azure-eventhub`** cell (not subprocess), then re-run import |
# MAGIC | Want classic Spark streaming | Add Event Hubs Spark connector library + `readStream` (next lesson) |

# COMMAND ----------

# =============================================================================
# STEP 10 — EXIT SUMMARY (for Jobs / ADF later)
# =============================================================================
import json as _json

summary = {
    "lab": "eventhub_finledger_lab",
    "run_id": RUN_ID,
    "mode": EH_MODE,
    "namespace": EH_NS,
    "hub": EH_NAME,
    "sent": len(EH_SENT),
    "received_processed": len(received),
    "status": "Succeeded",
}
print(_json.dumps(summary, indent=2))
dbutils.notebook.exit(_json.dumps(summary))

# COMMAND ----------
