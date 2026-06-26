# Session 3 — Trainer guide (2 hours)

Maps to [azure.html](../azure.html) **Day 3: Databricks** (Hours 25–32, compressed).

**Learner handout:** [SESSION3-STUDENT-GUIDE.md](SESSION3-STUDENT-GUIDE.md)  
**Theory + graphs:** [UI-OVERVIEW.md](UI-OVERVIEW.md)  
**UI detail:** [MANUAL-LAB.md](MANUAL-LAB.md) · [LINK-MAP.md](LINK-MAP.md)

---

## The one picture

### Before class — YOU run the script

```text
cd session-3
orchestrate.cmd
```

| Phase | Script | What students find |
|---|---|---|
| 1 | `databricks_rbac.py` | Storage RBAC for access connector (if any) |
| 2 | `bronze_prep.py` | `bronze/loaded/run=session3-lab/sample_transactions.csv` |

Students work in **Databricks UI** for 90+ minutes.

### In class — STUDENTS run notebooks

| Block | Time | Trainer | Students |
|---|---|---|---|
| **1** | 20 min | Whiteboard ADF vs Databricks | [lab-a](MANUAL-LAB.md#lab-a) → [lab-c](MANUAL-LAB.md#lab-c) |
| **2** | 30 min | Demo abfss path on screen | [lab-e](MANUAL-LAB.md#lab-e) nb_01 |
| **3** | 30 min | Delta + quarantine talking point | [lab-f](MANUAL-LAB.md#lab-f) nb_02 |
| **4** | 25 min | Gold = reporting layer | [lab-g](MANUAL-LAB.md#lab-g) nb_03 |
| **5** | 15 min | Cost + terminate cluster | [lab-i](MANUAL-LAB.md#lab-i) |

**Talking point:** TXN-10003 — £50k pending — fraud queue in gold `pending_count`.

---

## Pre-class checklist

- [ ] Class-1 + Databricks workspace deployed
- [ ] Session 2 bronze optional but Session 3 script uploads its own copy
- [ ] `session-3\orchestrate.cmd` exit 0
- [ ] Students have [SESSION3-STUDENT-GUIDE.md](SESSION3-STUDENT-GUIDE.md)
- [ ] MPN quota: if cluster blocked, code walkthrough + trainer demo

---

## Block 1 — Why Databricks (20 min)

**Objective:** Decision rule — ADF moves, Databricks transforms.

| Min | Trainer |
|-----|---------|
| 5 | Draw bronze → silver → gold on whiteboard |
| 10 | Portal → open Databricks → tour Compute vs Workspace |
| 5 | Everyone creates cluster with 30 min auto-terminate |

---

## Block 2 — Read bronze (30 min)

**Objective:** `abfss://` read, lazy vs action, 5 rows.

Walk through `nb_01_read_bronze.py` on projector — then learners **Run all**.

---

## Block 3 — Silver (30 min)

**Objective:** Cast, quarantine, Delta `_delta_log`.

Show messy row TXN-20003 if `transactions_messy.csv` uploaded.

---

## Block 4 — Gold (25 min)

**Objective:** `groupBy` aggregates; fraud filter cell.

---

## Block 5 — Wrap (15 min)

- Storage portal: silver/gold `_delta_log`
- **Mandatory:** terminate clusters
- Cost Management filter RG
- Preview Session 4 Purview

---

## MPN / quota fallbacks

| Issue | Workaround |
|-------|------------|
| Cannot create cluster | Trainer shared workspace screen-share |
| Quota 0 | Review notebook code + Storage paths only |
| Auth errors on abfss | Confirm Class-1 Storage Blob Data Contributor on learner user |

---

## Verification command

```text
orchestrate.cmd --verify-storage
```

Both `silver_transactions` and `gold_channel_summary` should print **OK**.
