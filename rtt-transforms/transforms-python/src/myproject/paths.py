"""Canonical Foundry dataset paths for the RTT-Programme pipeline (single source of truth)."""
BASE = "/JINESH-161f69/Training/RTT-Programme"

# --- sources (reference + raw landing) ---
PROVIDER_REFERENCE = f"{BASE}/reference/provider_reference"
SPECIALTY_REFERENCE = f"{BASE}/reference/specialty_reference"
SAE_COLUMN_RULES = f"{BASE}/reference/sae_column_rules"
RTT_PATHWAYS_RAW = f"{BASE}/raw/trust_fdp_rtt_pathways_raw"

# --- clean (silver) ---
RTT_PATHWAYS_TYPED = f"{BASE}/clean/rtt_pathways_typed"
RTT_PATHWAYS_CLEAN = f"{BASE}/clean/rtt_pathways_clean"
RTT_PATHWAYS_MINIMISED = f"{BASE}/clean/rtt_pathways_minimised"
RTT_PATHWAYS_METRICS = f"{BASE}/clean/rtt_pathways_metrics"

# --- incremental / streaming ---
INC_PATHWAYS_APPENDED = f"{BASE}/clean/rtt_pathways_appended"
CDC_CHANGE_FEED = f"{BASE}/clean/rtt_pathways_cdc_feed"
STATUS_HISTORY = f"{BASE}/clean/rtt_status_history"
STATUS_SCD2 = f"{BASE}/clean/rtt_status_scd2"
LONGEST_WAITERS_BY_TRUST = f"{BASE}/marts/rtt_longest_waiters_by_trust"

# --- marts (gold) ---
RTT_TRUST_SPECIALTY_PERF = f"{BASE}/marts/rtt_trust_specialty_perf"
RTT_PTL = f"{BASE}/marts/rtt_ptl"

# --- performance demonstrations ---
PERF_TRUST_REGION = f"{BASE}/marts/rtt_perf_trust_region"          # partitioned by region
PERF_PATHWAY_BUCKETED = f"{BASE}/marts/rtt_perf_pathway_bucketed"  # bucketed by trust_code
PERF_SPECIALTY_DUCKDB = f"{BASE}/marts/rtt_perf_specialty_duckdb"  # DuckDB lightweight agg
COST_REGION_SUMMARY = f"{BASE}/marts/rtt_cost_region_summary"      # right-sized @lightweight agg

# --- quarantine / audit ---
RTT_PATHWAYS_REJECTS = f"{BASE}/quarantine/rtt_pathways_rejects"
SCHEMA_DRIFT_REPORT = f"{BASE}/quarantine/schema_drift_report"
SAE_AUDIT_LOG = f"{BASE}/audit/sae_audit_log"
RECON_CONTROL_TOTALS = f"{BASE}/audit/recon_control_totals"
FRESHNESS_REPORT = f"{BASE}/audit/freshness_report"

# --- ontology-backing ---
PATIENT_PSEUDONYMISED = f"{BASE}/ontology-backing/rtt_patient_pseudonymised"
