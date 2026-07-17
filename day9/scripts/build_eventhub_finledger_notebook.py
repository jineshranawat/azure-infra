#!/usr/bin/env python3
"""Generate nb_eventhub_finledger_lab.py — beginner-friendly Event Hub workflow.

Step-by-step WHAT / WHY / HOW / ANALOGY so a new person can read top-to-bottom.
Uses shared hub from scripts/ensure_eventhub_lab.py + finledger secrets.

Run:    python day9/scripts/build_eventhub_finledger_notebook.py
Deploy: python scripts/deploy_eventhub_finledger_lab.py  (or deploy-shared-lab after wire)
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_eventhub_finledger_lab.py"


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # FinLedger Event Hub Lab — step by step",
        "# MAGIC",
        "# MAGIC **For someone new to Event Hubs.** Read every markdown cell before you click Run.",
        "# MAGIC",
        "# MAGIC | | |",
        "# MAGIC |:---|:---|",
        "# MAGIC | **Workspace** | `/Shared/day9/eventhub_finledger_lab` |",
        "# MAGIC | **Hub (Azure)** | `ehns-shared-finledger` / `finledger-payments` |",
        "# MAGIC | **Secrets** | Databricks scope `finledger` → `eventhub-*` keys |",
        "# MAGIC | **One-time setup** | From your laptop: `python scripts/ensure_eventhub_lab.py` |",
        "# MAGIC | **Compute** | Classic cluster preferred; Serverless OK for this SDK path |",
        "",
        "# COMMAND ----------",
        "",
    ]

    md(
        parts,
        """## Big picture — one kitchen story

Imagine **FinLedger Bank** at lunch rush.

| Kitchen role | Event Hub idea | In this lab |
|--------------|----------------|-------------|
| Waiter writes a ticket | **Producer / SEND** | Our Python code builds a small JSON “payment pulse” |
| Ticket rail / spike | **Event Hub** | Azure holds tickets in order for a while |
| Kitchen cooks pick tickets | **Consumer / RECEIVE** | Our code reads tickets back |
| Head chef totals sales | **PROCESS (Spark)** | Clean + groupBy channel → chart |

```text
 WAITERS (apps / banks)          TICKET RAIL                 KITCHEN (Databricks)
 -----------------------         ------------                --------------------
 Write payment tickets    ──►    Event Hub keeps them  ──►   Read tickets
                                 for ~24 hours               Spark totals GBP
                                                             by channel
```

**One sentence to remember:**  
*Event Hub is the ticket rail. Spark is the kitchen. Secrets are the staff-only key to the door.*

You do **not** phone the waiter after cooking — the rail remembers recent tickets (that memory is called **retention**).""",
    )

    md(
        parts,
        """## Learning outcomes (say these out loud at the end)

1. Draw namespace → hub → partition on a whiteboard in 30 seconds  
2. Explain why the **connection string** is a hotel key card, not a sticky note in git  
3. Send a dummy FinLedger payment onto the hub  
4. Receive it and turn it into a Spark table  
5. Aggregate GBP by channel (same Spark you already know)  
6. Name one thing you’d change for a real overnight bank job""",
    )

    md(
        parts,
        """## Step 0 — Vocabulary (kitchen / hotel / highway)

Read this table slowly. Every word appears again in code.

### 1. Namespace — the restaurant building

**Word:** Namespace (`ehns-shared-finledger`)

**Easy analogy:** The **whole restaurant building** on the street.  
Address of the kitchen, manager’s office, and ticket rails all start here.

**Detail:** In Azure this is a **resource**. Cheap lab SKU = **Basic**.  
One namespace can hold several hubs (several ticket rails).

---

### 2. Event Hub — one ticket rail

**Word:** Event Hub / hub (`finledger-payments`)

**Easy analogy:** **One named rail** inside the restaurant: “Payments only.”  
Another rail might be “Refunds only” (we don’t create that in this lab).

**Detail:** Producers **append** messages. Consumers **read** them.  
Messages are tiny JSON documents — our “kitchen tickets.”

---

### 3. Partition — lanes on the highway

**Word:** Partition (we use **2**)

**Easy analogy:** A highway with **2 lanes**. Cars (messages) don’t jump lanes.  
Two cooks can watch two lanes and go faster without mixing order *inside* a lane.

**Detail:** Order is guaranteed **per partition**, not across the whole hub.  
More partitions ⇒ more parallel readers. Lab keeps **2** to stay cheap and simple.

```text
  Hub "finledger-payments"
  ┌─────────────┬─────────────┐
  │ Partition 0 │ Partition 1 │   ← two lanes
  │  msg msg    │  msg msg    │
  └─────────────┴─────────────┘
```

---

### 4. Connection string / SAS — hotel key card

**Word:** SAS (Shared Access Signature) / connection string

**Easy analogy:** A **hotel key card**.  
- **Send** right = allowed to drop tickets on the rail  
- **Listen** right = allowed to read tickets off the rail  

Our lab rule `lab-sendlisten` has **both**.

**Detail:** The long string starts with `Endpoint=sb://...`.  
Treat it like a password. Store it in **Databricks secrets**, never in chat, git, or screenshots.

**Analogy stretch:** If you laminate the key card to the classroom wall (hard-code in notebook), anyone with the screenshot can cook for free in your restaurant — forever until you cancel the card.

---

### 5. Consumer group — named team of cooks

**Word:** Consumer group (`$Default` in this lab)

**Easy analogy:** A **sticky label on the clipboard**: “Team Red kitchen.”  
Team Red and Team Blue can each read the *same* tickets for different jobs  
(one builds silver tables, one builds fraud scores) without stealing each other’s place.

**Detail:** Each group tracks “how far we have read” separately.  
Lab uses Azure’s built-in **`$Default`** group so we don’t create extras.

---

### 6. Retention — how long tickets stay on the spike

**Word:** Retention (lab ≈ **24 hours**)

**Easy analogy:** Tickets stay on the rail until closing + a bit.  
Tomorrow morning the oldest tickets are cleared — you can’t re-cook last month’s lunch from the rail alone.

**Detail:** For history forever you write to **Delta / lake**.  
Event Hub is a **buffer**, not the archive.

---

### 7. Producer vs consumer — who writes vs who reads

| Role | Kitchen | Code in this notebook |
|------|---------|------------------------|
| **Producer** | Waiter | Step 5 — `EventHubProducerClient` |
| **Consumer** | Cook | Step 6 — `EventHubConsumerClient` |
| **Processor** | Head chef with calculator | Step 7 — Spark DataFrame |

---

### 8. Message / event / pulse — one ticket

**Word:** Message / event / “payment pulse”

**Easy analogy:** **One paper ticket** with fields: who paid, how much, which channel, when.

**Detail:** We use JSON so both Python and Spark can read the same shape.

---

### Guardrail (say with the class)

> Secrets live in scope `finledger`. We print **names and lengths only**, never the key.""",
    )

    md(
        parts,
        """## Step 1 — Prerequisites (open the restaurant once)

**WHAT:** Create Azure Event Hub + put the key card into Databricks.  
**WHY:** Students should not click the Azure portal for every class.  
**HOW:** One idempotent Windows command from the trainer laptop.

```cmd
cd d:\\azure
python scripts\\ensure_eventhub_lab.py
```

**Analogy:** Before service, the manager unlocks the building, hangs the payments rail,  
and puts spare key cards in the staff safe (`finledger` secret scope).  
Running the script again is safe — it won’t build a second building.

Then: open this notebook → attach compute → read markdown → Run cell by cell.""",
    )

    md(
        parts,
        """## Step 2 — Load secrets (open the staff safe)

**WHAT:** Read the hotel key card + hub name into variables.  
**WHY:** Rotation and audits — trainers can replace the key without editing notebooks.  
**HOW:** `dbutils.secrets.get` from scope `finledger`.  

**Analogy:** You unlock the safe, check the card is there, and put it in your pocket.  
You do **not** photograph the card or write it on the whiteboard.

If the safe is empty, the notebook still teaches Steps 4–8 with an **in-memory bus**  
(like practicing tickets on a classroom spike before the real rail is installed).""",
    )

    code(
        parts,
        '''# =============================================================================
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
    print("This notebook will still DEMO the workflow with an in-memory bus.")''',
    )

    md(
        parts,
        """## Step 3 — Install the kitchen radio (`azure-eventhub`)

**WHAT:** A Python library that knows Azure’s Event Hub language.  
**WHY:** Without it, Databricks doesn’t speak “ticket rail.”  
**HOW:** Use **`%pip install`** so the radio is installed into **this** classroom laptop (Serverless kernel).

**Analogy:** `%pip` = plugging a radio into *this* booth.  
`subprocess pip` on Serverless often plugs a radio into the *storage room* — you still hear silence in class.

After `%pip`, Python may restart — treat it like a brief power cut: re-run **Step 2** (secrets) then import.""",
    )

    # Dedicated %pip cell — required on Serverless (subprocess pip hits wrong env)
    parts.extend(
        [
            "# MAGIC %pip install azure-eventhub --quiet",
            "",
            "# COMMAND ----------",
            "",
        ]
    )

    md(
        parts,
        """### Step 3b — Import the SDK

Run this **after** the `%pip` cell succeeds. If import still fails, detach/re-attach compute and re-run `%pip`.""",
    )

    code(
        parts,
        '''# =============================================================================
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
    ) from exc''',
    )

    md(
        parts,
        """## Step 4 — Design the ticket (JSON contract)

**WHAT:** Agree the **fields** every payment ticket must have.  
**WHY:** Cooks hate tickets that say “the usual” with no price. Downstream Spark needs stable column names.  
**HOW:** One Python `dict` shape for every event.

**Analogy:** The restaurant prints **one blank ticket form**. Every waiter fills the same boxes:

| Field | Kitchen meaning |
|-------|-----------------|
| `event_type` | Kind of ticket (`payment_pulse`) |
| `transaction_id` | Order number |
| `amount_gbp` | Price |
| `channel` | How they paid (CARD / WIRE / …) |
| `event_ts` | Time stamped on the ticket |
| `run_id` | Which class/lab service (so demos don’t collide) |
| `seq` | Line number on the pad |

If you change the form later, **tell the kitchen** (schema evolution) — don’t surprise them.""",
    )

    code(
        parts,
        '''# =============================================================================
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
print(json.dumps(DUMMY_EVENTS[0], indent=2))''',
    )

    md(
        parts,
        """## Step 5 — SEND (waiters drop tickets on the rail)

**WHAT:** Push each JSON ticket onto Event Hub.  
**WHY:** Landing events is separate from cooking them — outages in Spark don’t lose the waiters’ work (while retention lasts).  
**HOW:** `EventHubProducerClient` → fill a **batch** of tickets → `send_batch`.

**Analogy:** Waiters don’t walk into the kitchen for every order. They **spike tickets on the rail**.  
Batching = clipping several tickets together before walking to the rail (faster, fewer trips).

If the real rail isn’t unlocked (no secrets), we practice on a **classroom spike** (in-memory list) with the **same tickets**.""",
    )

    code(
        parts,
        '''# =============================================================================
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

print("EH_MODE =", EH_MODE)''',
    )

    md(
        parts,
        """## Step 6 — RECEIVE (cooks take tickets off the rail)

**WHAT:** Listen for a short time and collect tickets into a Python list.  
**WHY:** Prove the round trip. Real banks keep a cook on duty 24/7; we listen ~15 seconds so **Run all** finishes in class.  
**HOW:** `EventHubConsumerClient.receive` + callback `on_event`.

**Analogy:** A cook stands at the rail, unclips tickets, and stacks them on a tray.  
`consumer group $Default` = “this kitchen’s clipboard.”  
`starting_position="-1"` (lab) = “read from the oldest ticket still on the spike” so demos are easy.

**Production note:** Real cooks use a **bookmark** (checkpoint) so after a break they continue from the next unread ticket — not from the beginning every time.""",
    )

    code(
        parts,
        '''# =============================================================================
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

print("First received (truncated):", json.dumps(received[0], indent=2) if received else None)''',
    )

    md(
        parts,
        """## Step 7 — PROCESS (head chef + calculator = Spark)

**WHAT:** Tickets → Spark table → clean → totals by channel.  
**WHY:** Event Hub only **moves** data. Business rules still live in Spark (or SQL) like every FinLedger lab.  
**HOW:** `createDataFrame` → trim/cast/filter → `groupBy` → save tables.

**Analogy:**  
- **Raw** = tickets still greasy from the rail  
- **Silver** = tickets rewritten neatly (types fixed, blanks removed)  
- **KPI / gold-ish** = board on the wall: “CARD sold £X, WIRE sold £Y”

You already know these Spark verbs from teach-all / 50-problems —
Event Hub was only the **door** the tickets came through.""",
    )

    code(
        parts,
        '''# =============================================================================
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
    print("Table save skipped (permissions):", str(exc)[:140])''',
    )

    md(
        parts,
        """## Step 8 — Visual check (bar chart)

**WHAT:** One chart so the class *sees* channels.  
**WHY:** Dashboards start from the same KPI frame you’d give Power BI later.""",
    )

    code(
        parts,
        '''# =============================================================================
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
print("Chart done")''',
    )

    md(
        parts,
        """## Step 9 — Production map (read this — no code)

What we did today vs what banks do overnight:

| Lab (this notebook) | Production |
|---------------------|------------|
| SDK send + short receive | Continuous `readStream.format("eventhubs")` |
| In-memory / 15s listen | Checkpoint folder on ADLS (exactly-once offsets) |
| `saveAsTable` overwrite | MERGE / append silver by event time |
| Manual Run all | Databricks Job or ADF trigger |
| Basic SKU / 2 partitions | Sized TUs + more partitions for throughput |

```text
[Card network / FPS] → Event Hub → Spark Structured Streaming
                                      → Delta silver → gold KPI → dashboard
```

**Interview one-liner:** *“Event Hub is the durable buffer; Spark is the processor;
Delta is the system of record.”*""",
    )

    md(
        parts,
        """## Step 10 — Troubleshooting

| Symptom | Fix |
|---------|-----|
| Missing secrets | `python scripts\\ensure_eventhub_lab.py` then re-run Step 2 |
| SEND / RECEIVE fail | Check hub name; re-run ensure; wait 1 min after create |
| Empty receive | Normal if retention/cursor odd — fallback uses sent list |
| `pip` / ModuleNotFoundError | Run the **`%pip install azure-eventhub`** cell (not subprocess), then re-run import |
| Want classic Spark streaming | Add Event Hubs Spark connector library + `readStream` (next lesson) |""",
    )

    code(
        parts,
        '''# =============================================================================
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
dbutils.notebook.exit(_json.dumps(summary))''',
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
