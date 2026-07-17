# FinLedger Event Hub Lab — trainer guide (easy analogies)

**Notebook:** `/Shared/day9/eventhub_finledger_lab`  
**Open:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/eventhub_finledger_lab  

This guide mirrors the notebook. Teach with the **restaurant / kitchen** story — new learners remember food better than “ingress topology.”

---

## The one story (repeat every 10 minutes)

| Kitchen | Azure / Databricks |
|---------|-------------------|
| Restaurant building | **Namespace** `ehns-shared-finledger` |
| Payments ticket rail | **Event Hub** `finledger-payments` |
| Highway lanes | **Partitions** (lab = 2) |
| Hotel key card | **Connection string / SAS** in secrets |
| Clipboard labeled “Team Red” | **Consumer group** `$Default` |
| How long tickets stay on the spike | **Retention** (~24 hours) |
| Waiter | **Producer / SEND** |
| Cook | **Consumer / RECEIVE** |
| Head chef with till | **Spark PROCESS** |
| Staff safe | **Databricks secret scope** `finledger` |

**One sentence:** *Event Hub is the ticket rail. Spark is the kitchen. Secrets are the staff key.*

---

## Vocabulary (say → draw → then code)

### Namespace
The **building**. Cheap Basic SKU for class. Holds one or more hubs.

### Event Hub
One **named rail** inside the building. We only create `finledger-payments`.

### Partition
**Lanes.** Order is safe inside a lane. More lanes = more parallel cooks.

### Connection string (SAS)
**Key card** with Send and/or Listen. Never photo / git / chat. Print length only.

### Consumer group
Named **team of cooks** sharing a reading bookmark. Lab uses `$Default`.

### Retention
Tickets disappear from the rail after the window. Forever history ⇒ **Delta lake**, not Event Hub.

### Message / event / pulse
One **JSON ticket**: id, amount, channel, time, run_id, seq.

---

## Setup (once)

```cmd
python scripts\ensure_eventhub_lab.py
python scripts\deploy_eventhub_finledger_lab.py
```

Manager unlocks the building, hangs the rail, puts key cards in the safe.

---

## Notebook steps

| Step | Teach in one breath |
|------|---------------------|
| 0 | Vocabulary with kitchen drawings |
| 1 | Unlock restaurant (`ensure_eventhub_lab.py`) |
| 2 | Open staff safe (secrets) — no photos of the key |
| 3 | Plug in kitchen radio (`%pip install azure-eventhub`) then import |
| 4 | Print blank ticket form (JSON fields) |
| 5 | Waiters spike tickets (**SEND**) |
| 6 | Cooks unclip tickets for ~15s (**RECEIVE**) |
| 7 | Head chef totals by channel (**Spark**) |
| 8 | Wall board chart |
| 9 | Real banks keep cooks 24/7 + bookmarks (checkpoints) |
| 10 | Troubleshooting |

---

## Fix: `ModuleNotFoundError: azure.eventhub`

Radio was plugged into the storage room (subprocess pip).  

1. Refresh notebook  
2. Run **`%pip install azure-eventhub --quiet`**  
3. Wait; Python may restart  
4. Re-run secrets + import  
5. Continue SEND → RECEIVE → PROCESS  

---

## Interview one-liner for students

> “Event Hub is a durable buffer. Spark processes. Delta stores. Connection strings stay in Key Vault or Databricks secrets.”
