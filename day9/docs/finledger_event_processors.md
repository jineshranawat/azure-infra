# Databricks concepts â€” Zach Wilson patterns for FinLedger

**Inspired by:** [EcZachly (Zach Wilson)](https://github.com/eczachly) and
[personal-health-analytics](https://github.com/EcZachly/personal-health-analytics)  
**Lab notebook:** `/Shared/day9/finledger_event_processors` (**v2** — identical Cell 0 + write helpers as `50_data_engineering_problems`)  
(`day9/notebooks/nb_finledger_event_processors.py` ← `build_finledger_event_processors_notebook.py`)  
**Deploy:** `deploy-shared-lab.cmd`  
**Verify:** `python scripts/run_finledger_event_processors.py`

---

## A. WHAT & WHY

Zachâ€™s teaching theme (from the [Data Engineer Handbook](https://github.com/DataExpert-io/data-engineer-handbook) community and his repos):

> Youâ€™re a data engineer â€” you donâ€™t â€œuse Spark and Kafkaâ€. Focus on **business outcomes**.

His [personal-health-analytics](https://github.com/EcZachly/personal-health-analytics) repo shows the **engineering** pattern:

1. **One Event schema** (`namedtuple`) every source must produce  
2. **One processor per source** (`processors/fitbit`, `linkedin`, â€¦)  
3. **One runner** that unions everything â†’ timeline â†’ daily aggregates â†’ **pivot**  

We map that to **FinLedger** on Databricks + Delta (batch, syllabus-aligned).

---

## B. Pattern map (Zach â†’ FinLedger)

| Zach (health analytics) | FinLedger (this lab) |
|-------------------------|----------------------|
| Fitbit / LinkedIn / Spotify | `transactions` + `returns` CSVs |
| `Event(source, metric_name, timestamp, metric_value, content)` | Same 5 columns (Spark StructType) |
| `processors/*.py` | `txn_processor`, `returns_processor` |
| `runner.py` â†’ `all_events_on_timeline.csv` | `run_everything()` â†’ silver `zach_events` Delta |
| `do_daily_aggregates` | groupBy source/metric/date â†’ `zach_daily_aggregates` |
| `pivoted.csv` for Tableau | `zach_pivoted` Delta + `net_cash_gbp` |
| CUTOFF_DATE filter | Quality filter `metric_value > 0` |

**Analogy:** Event is the **shipping label**. Processors are **factories**. Runner is the **warehouse that piles everything onto one truck**, then pivots for the CFO view.

---

## C. Learning roadmap (Zach â†’ syllabus sessions)

| Zach step | Syllabus | Lab proof |
|-----------|----------|-----------|
| Metrics / SQL mindset | Session 15 | `%sql` / temp view on pivoted gold |
| Data modeling | Sessions 13â€“14 | Event contract before joins |
| Python processors | Day 8 / 50-problems | Processor functions in notebook |
| Data quality | Sessions 9, 27 | Filter bad amounts; `assert_quality` |
| Distributed compute | Sessions 10â€“14 | Delta writes on ADLS |
| Orchestration | Sessions 18â€“19 | Widget `run_id` â†’ Job / ADF later |
| Tools (Spark/Delta) | Day 9+ | Means, not the goal |

---

## D. Lake paths written by the notebook

```text
bronze/loaded/run={run_id}/sample_transactions.csv   â† input
bronze/incoming/returns/returns_raw.csv              â† input
silver/zach_events/run={run_id}/                     â† Event timeline (Delta)
gold/zach_daily_aggregates/run={run_id}/             â† daily sums
gold/zach_pivoted/run={run_id}/                      â† wide analyst table
```

---

## E. How to teach (45â€“60 min)

1. **Whiteboard:** â€œTwo messy CSVs â†’ one Event contractâ€  
2. **Run** `/Shared/day9/finledger_event_processors` (**Run all**) â€” widget `run_id=session3-lab`  
3. **Show** silver Event rows (`source`, `metric_name`, â€¦)  
4. **Show** pivoted gold + `net_cash_gbp` (payments âˆ’ refunds)  
5. **Contrast** with medallion tables (`nb_session13` / `nb_medallion_governance_e2e`)  
   - Medallion = **layer** story (bronze/silver/gold tables)  
   - Zach Event = **metric** story (one schema for many sources)  
6. **Hook to ADF:** same notebook path can be an ADF Notebook activity with `run_id`

---

## F. Re-run

```cmd
deploy-shared-lab.cmd
```

Then open:

`https://adb-<workspace>.azuredatabricks.net/#workspace/Shared/day9/finledger_event_processors`

Idempotent: Delta overwrite on silver/gold zach_* paths.

---

## G. Related lab assets

| Asset | Role |
|-------|------|
| 50 problems + Part K | Pattern catalog (quality, MERGE, cost, UC) |
| `nb_medallion_governance_e2e` | Classic bronzeâ†’silverâ†’gold layers |
| `pl_gov_06` / `pl_db_14` | ADF orchestration of medallion |
| This notebook | **Cross-source Event model** (eczachly style) |

---

## H. Attribution

Patterns adapted from open educational material by Zach Wilson ([@EcZachly](https://github.com/eczachly)).  
FinLedger data, ADLS paths, and Databricks wiring are original to this training repo.  
Do **not** ship production PII through the public health-analytics sample style â€” use synthetic FinLedger CSVs only.

---

*Aligned with `nb_finledger_event_processors.py` and `docs/data-engineering-course.html` Sessions 15â€“19 / 30.*

