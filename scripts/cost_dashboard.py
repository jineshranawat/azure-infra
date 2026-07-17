#!/usr/bin/env python3
"""Shared-lab cost & metrics dashboard — subscription + RG + ADF pipelines.

Works on any student Windows machine with:
  - Python 3.10+ OR the repo .venv
  - Azure CLI logged in (`az login`) OR .env SPN credentials

Produces:
  - Console tables (service → resource → daily)
  - ADF pipeline run inventory + estimated $ share of factory spend
  - HTML dashboard (open in browser) + CSV exports
  - Azure Portal Cost Analysis deep links

Re-run:  cost-dashboard.cmd
         cost-dashboard.cmd --days 14
         cost-dashboard.cmd --open
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import webbrowser
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "docs" / "cost-dashboard-out"
DEFAULT_RG = "rg-shared-class1"
DEFAULT_ADF = "adf-shared-qgr7mj"

logger = logging.getLogger(__name__)


def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    dotenv = REPO_ROOT / ".env"
    if dotenv.is_file():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return {**env, **{k: v for k, v in os.environ.items() if v}}


def _credential(file_env: dict[str, str]):
    """Prefer az-login identity; fall back to SPN from .env (shared lab)."""
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    tid = file_env.get("AZURE_TENANT_ID", "").strip()
    cid = file_env.get("AZURE_CLIENT_ID", "").strip()
    sec = file_env.get("AZURE_CLIENT_SECRET", "").strip()
    if tid and cid and sec:
        logger.info("Auth: service principal from .env")
        return ClientSecretCredential(tid, cid, sec)
    logger.info("Auth: DefaultAzureCredential (az login / VS Code / MI)")
    return DefaultAzureCredential(exclude_interactive_browser_credential=False)


def _subscription_id(file_env: dict[str, str]) -> str:
    sub = file_env.get("AZURE_SUBSCRIPTION_ID", "").strip()
    if sub:
        return sub
    try:
        out = subprocess.check_output(
            ["az", "account", "show", "--query", "id", "-o", "tsv"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if out:
            return out
    except Exception:
        pass
    raise SystemExit("Set AZURE_SUBSCRIPTION_ID in .env or run: az login && az account set --subscription <id>")


def _scope_rg(sub: str, rg: str) -> str:
    return f"/subscriptions/{sub}/resourceGroups/{rg}"


def _scope_sub(sub: str) -> str:
    return f"/subscriptions/{sub}"


def _cost_query(
    credential,
    scope: str,
    *,
    timeframe: str = "MonthToDate",
    granularity: str | None = None,
    group_by: list[str] | None = None,
    filter_resource_id: str | None = None,
    retries: int = 5,
) -> list[list[Any]]:
    """Call Cost Management Query API. Returns raw rows. Retries on 429."""
    import time

    from azure.core.exceptions import HttpResponseError
    from azure.mgmt.costmanagement import CostManagementClient
    from azure.mgmt.costmanagement.models import (
        QueryAggregation,
        QueryDataset,
        QueryDefinition,
        QueryFilter,
        QueryGrouping,
        QueryComparisonExpression,
        QueryTimePeriod,
    )

    client = CostManagementClient(credential)
    grouping = [QueryGrouping(type="Dimension", name=g) for g in (group_by or [])]
    aggregation = {"totalCost": QueryAggregation(name="Cost", function="Sum")}

    filt = None
    if filter_resource_id:
        filt = QueryFilter(
            dimensions=QueryComparisonExpression(
                name="ResourceId",
                operator="In",
                values=[filter_resource_id],
            )
        )

    dataset = QueryDataset(
        granularity=granularity,
        aggregation=aggregation,
        grouping=grouping or None,
        filter=filt,
    )

    time_period = None
    tf = timeframe
    if timeframe.startswith("Custom|"):
        # "|" separator — ISO timestamps contain ":" so we cannot split on that
        _, start, end = timeframe.split("|", 2)
        tf = "Custom"
        time_period = QueryTimePeriod(
            from_property=datetime.fromisoformat(start.replace("Z", "+00:00")),
            to=datetime.fromisoformat(end.replace("Z", "+00:00")),
        )

    definition = QueryDefinition(
        type="ActualCost",
        timeframe=tf,
        time_period=time_period,
        dataset=dataset,
    )
    delay = 8.0
    for attempt in range(1, retries + 1):
        try:
            result = client.query.usage(scope=scope, parameters=definition)
            return list(result.rows or [])
        except HttpResponseError as e:
            code = getattr(e, "status_code", None) or ""
            msg = str(e)
            if "429" in msg or code == 429:
                logger.warning("Cost API 429 — sleep %.0fs (attempt %d/%d)", delay, attempt, retries)
                time.sleep(delay)
                delay = min(delay * 1.6, 60)
                continue
            logger.warning("Cost query failed on %s: %s", scope, msg[:200])
            return []
        except Exception as e:
            logger.warning("Cost query failed on %s: %s", scope, str(e)[:200])
            return []
    logger.warning("Cost query exhausted retries on %s", scope)
    return []


def _days_window(days: int) -> str:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(days - 1, 0))
    return f"Custom|{start.isoformat()}T00:00:00Z|{end.isoformat()}T23:59:59Z"


def _list_rg_resources(credential, sub: str, rg: str) -> list[dict[str, Any]]:
    from azure.mgmt.resource import ResourceManagementClient

    client = ResourceManagementClient(credential, sub)
    out = []
    for r in client.resources.list_by_resource_group(rg):
        out.append(
            {
                "name": r.name,
                "type": r.type,
                "location": r.location,
                "id": r.id,
            }
        )
    return out


def _adf_pipeline_runs(credential, sub: str, rg: str, adf: str, days: int) -> list[dict[str, Any]]:
    """List ADF pipeline runs in the lookback window (activity-level timing)."""
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.datafactory.models import RunFilterParameters

    client = DataFactoryManagementClient(credential, sub)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    filt = RunFilterParameters(last_updated_after=start, last_updated_before=end)
    runs: list[dict[str, Any]] = []
    try:
        resp = client.pipeline_runs.query_by_factory(rg, adf, filt)
        # SDK returns PipelineRunsQueryResponse with .value, not a bare list
        batch = list(getattr(resp, "value", None) or [])
        continuation = getattr(resp, "continuation_token", None)
        while True:
            for run in batch:
                dur_ms = None
                if run.duration_in_ms is not None:
                    dur_ms = int(run.duration_in_ms)
                elif run.run_start and run.run_end:
                    dur_ms = int((run.run_end - run.run_start).total_seconds() * 1000)
                runs.append(
                    {
                        "pipeline": run.pipeline_name,
                        "run_id": run.run_id,
                        "status": run.status,
                        "start": run.run_start.isoformat() if run.run_start else "",
                        "end": run.run_end.isoformat() if run.run_end else "",
                        "duration_ms": dur_ms or 0,
                        "duration_min": round((dur_ms or 0) / 60000.0, 3),
                        "message": (run.message or "")[:120],
                    }
                )
            if not continuation:
                break
            filt2 = RunFilterParameters(
                last_updated_after=start,
                last_updated_before=end,
                continuation_token=continuation,
            )
            resp = client.pipeline_runs.query_by_factory(rg, adf, filt2)
            batch = list(getattr(resp, "value", None) or [])
            continuation = getattr(resp, "continuation_token", None)
    except Exception as e:
        logger.warning("ADF pipeline runs query failed: %s", str(e)[:200])
    return runs


# Azure Monitor metric names per resource type — the performance side of NFRs.
# aggregation: Total for counters, Average for gauges.
METRIC_MAP: dict[str, list[tuple[str, str]]] = {
    "Microsoft.DataFactory/factories": [
        ("PipelineSucceededRuns", "Total"),
        ("PipelineFailedRuns", "Total"),
        ("ActivityFailedRuns", "Total"),
        ("TriggerSucceededRuns", "Total"),
    ],
    "Microsoft.Storage/storageAccounts": [
        ("UsedCapacity", "Average"),
        ("Transactions", "Total"),
        ("Ingress", "Total"),
        ("Egress", "Total"),
    ],
    "Microsoft.EventHub/namespaces": [
        ("IncomingMessages", "Total"),
        ("OutgoingMessages", "Total"),
        ("ThrottledRequests", "Total"),
    ],
    "Microsoft.KeyVault/vaults": [
        ("ServiceApiHit", "Total"),
        ("ServiceApiLatency", "Average"),
    ],
    "Microsoft.Sql/servers/databases": [
        ("dtu_consumption_percent", "Average"),
        ("storage_percent", "Average"),
        ("connection_successful", "Total"),
    ],
}


def _monitor_metrics(
    credential, resources: list[dict[str, Any]], days: int
) -> list[dict[str, Any]]:
    """Pull key Azure Monitor metrics per resource (performance NFR evidence).

    Uses the plain Metrics REST API so no extra SDK package is needed.
    Every metric is soft — a denied/unknown metric logs and continues.
    """
    import urllib.parse
    import urllib.request

    token = credential.get_token("https://management.azure.com/.default").token
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    timespan = f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    rows: list[dict[str, Any]] = []
    for res in resources:
        wanted = METRIC_MAP.get(res["type"])
        if not wanted:
            continue
        names = ",".join(m for m, _ in wanted)
        aggs = ",".join(sorted({a for _, a in wanted}))
        # Storage account metrics (UsedCapacity) only allow PT1H granularity
        interval = "PT1H" if res["type"] == "Microsoft.Storage/storageAccounts" else "P1D"
        url = (
            f"https://management.azure.com{res['id']}/providers/microsoft.insights/metrics"
            f"?api-version=2018-01-01&metricnames={urllib.parse.quote(names)}"
            f"&timespan={urllib.parse.quote(timespan)}&interval={interval}&aggregation={aggs}"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("Metrics failed for %s: %s", res["name"], str(e)[:120])
            continue
        agg_of = dict(wanted)
        for metric in payload.get("value", []):
            mname = metric.get("name", {}).get("value", "?")
            agg = agg_of.get(mname, "Total").lower()
            points = [
                p.get(agg)
                for ts in metric.get("timeseries", [])
                for p in ts.get("data", [])
                if p.get(agg) is not None
            ]
            if not points:
                continue
            value = (sum(points) / len(points)) if agg == "average" else sum(points)
            rows.append(
                {
                    "resource": res["name"],
                    "type": res["type"].split("/")[-1],
                    "metric": mname,
                    "aggregation": agg,
                    "value": round(float(value), 2),
                    "unit": metric.get("unit", ""),
                    "days": days,
                }
            )
    return rows


def _adf_factory_resource_id(sub: str, rg: str, adf: str) -> str:
    return (
        f"/subscriptions/{sub}/resourceGroups/{rg}"
        f"/providers/Microsoft.DataFactory/factories/{adf}"
    )


def _attribute_adf_cost(
    runs: list[dict[str, Any]], factory_cost: float
) -> list[dict[str, Any]]:
    """Estimate $ per pipeline from share of total successful run duration.

    Azure does NOT bill per ADF pipeline — only the factory. This is a transparent
    teaching estimate: cost_share = factory_mtd × (pipeline_ms / total_ms).
    """
    by_pipe: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"runs": 0, "ok": 0, "fail": 0, "duration_ms": 0}
    )
    for r in runs:
        p = r["pipeline"] or "(unknown)"
        by_pipe[p]["runs"] += 1
        by_pipe[p]["duration_ms"] += int(r["duration_ms"] or 0)
        st = (r["status"] or "").lower()
        if st == "succeeded":
            by_pipe[p]["ok"] += 1
        elif st in ("failed", "cancelled"):
            by_pipe[p]["fail"] += 1

    total_ms = sum(v["duration_ms"] for v in by_pipe.values()) or 1
    rows = []
    for name, v in sorted(by_pipe.items(), key=lambda kv: -kv[1]["duration_ms"]):
        share = v["duration_ms"] / total_ms
        rows.append(
            {
                "pipeline": name,
                "runs": v["runs"],
                "succeeded": v["ok"],
                "failed": v["fail"],
                "duration_min": round(v["duration_ms"] / 60000.0, 2),
                "est_cost_usd": round(factory_cost * share, 4),
                "share_pct": round(share * 100, 1),
            }
        )
    return rows


def _portal_links(sub: str, rg: str) -> dict[str, str]:
    # Cost Analysis deep links (Azure Portal)
    base = "https://portal.azure.com/#blade/Microsoft_Azure_CostManagement/Menu/costanalysis"
    return {
        "cost_analysis_rg": (
            f"https://portal.azure.com/#@/resource/subscriptions/{sub}"
            f"/resourceGroups/{rg}/costanalysis"
        ),
        "cost_analysis_sub": (
            f"https://portal.azure.com/#view/Microsoft_Azure_CostManagement"
            f"/Menu/~/costanalysis/scope/subscriptions%2F{sub}"
        ),
        "budgets": "https://portal.azure.com/#view/Microsoft_Azure_CostManagement/Menu/~/budgets",
        "adf_monitor": (
            f"https://adf.azure.com/en/monitoring/pipelineruns"
            f"?factory=/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.DataFactory/factories/{DEFAULT_ADF}"
        ),
        "cost_mgmt_home": base,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _html_escape(s: Any) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _table_html(title: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return f"<h2>{_html_escape(title)}</h2><p><em>No rows (cost data can lag 24–48h).</em></p>"
    cols = list(rows[0].keys())
    head = "".join(f"<th>{_html_escape(c)}</th>" for c in cols)
    body = []
    for r in rows:
        body.append("<tr>" + "".join(f"<td>{_html_escape(r.get(c, ''))}</td>" for c in cols) + "</tr>")
    return (
        f"<h2>{_html_escape(title)}</h2>"
        f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


def _render_html(
    *,
    rg: str,
    sub: str,
    mtd_services_rg: list[dict[str, Any]],
    mtd_resources: list[dict[str, Any]],
    daily: list[dict[str, Any]],
    mtd_services_sub: list[dict[str, Any]],
    adf_rows: list[dict[str, Any]],
    adf_runs: list[dict[str, Any]],
    resources: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    links: dict[str, str],
    factory_cost: float,
    generated: str,
) -> str:
    rg_total = sum(float(r.get("cost", 0) or 0) for r in mtd_services_rg)
    sub_total = sum(float(r.get("cost", 0) or 0) for r in mtd_services_sub)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>FinLedger Cost Dashboard — { _html_escape(rg) }</title>
<style>
  :root {{ --ink:#1a1a1a; --muted:#555; --line:#ddd; --accent:#0b5fff; --bg:#f7f8fa; }}
  body {{ font-family: "Segoe UI", system-ui, sans-serif; margin:0; background:var(--bg); color:var(--ink); }}
  header {{ background:#0b1f3a; color:#fff; padding:1.5rem 2rem; }}
  header h1 {{ margin:0 0 .4rem; font-size:1.5rem; }}
  header p {{ margin:0; opacity:.85; }}
  main {{ max-width:1100px; margin:1.5rem auto; padding:0 1rem 3rem; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:1rem; margin:1rem 0 2rem; }}
  .card {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:1rem 1.2rem; }}
  .card .n {{ font-size:1.6rem; font-weight:700; }}
  .card .l {{ color:var(--muted); font-size:.85rem; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; margin:0 0 2rem; font-size:.9rem; }}
  th, td {{ border:1px solid var(--line); padding:.45rem .6rem; text-align:left; }}
  th {{ background:#eef2f8; }}
  tr:hover td {{ background:#f0f6ff; }}
  h2 {{ margin:1.5rem 0 .6rem; font-size:1.15rem; }}
  .links a {{ display:inline-block; margin:.3rem .6rem .3rem 0; color:var(--accent); }}
  .note {{ background:#fff8e6; border:1px solid #f0d78c; padding:.8rem 1rem; border-radius:6px; margin:1rem 0; }}
  footer {{ color:var(--muted); font-size:.8rem; margin-top:2rem; }}
</style>
</head>
<body>
<header>
  <h1>FinLedger Cost &amp; Metrics Dashboard</h1>
  <p>Resource group <strong>{_html_escape(rg)}</strong> · Subscription {_html_escape(sub[:8])}… · Generated { _html_escape(generated) }</p>
</header>
<main>
  <div class="cards">
    <div class="card"><div class="n">${rg_total:,.2f}</div><div class="l">RG month-to-date</div></div>
    <div class="card"><div class="n">${sub_total:,.2f}</div><div class="l">Subscription MTD (all RGs)</div></div>
    <div class="card"><div class="n">${factory_cost:,.4f}</div><div class="l">ADF factory MTD (actual)</div></div>
    <div class="card"><div class="n">{len(adf_runs)}</div><div class="l">ADF pipeline runs (window)</div></div>
  </div>

  <div class="note">
    <strong>Granularity note:</strong> Azure Cost Management bills at <em>daily</em> resolution
    (not true per-minute $). Drill-down path: <b>Subscription → Service → Resource → Day</b>.
    ADF pipeline $ below is an <em>estimate</em> = factory cost × (pipeline duration ÷ all durations).
  </div>

  <p class="links">
    <a href="{_html_escape(links['cost_analysis_rg'])}" target="_blank">Azure Cost Analysis (RG)</a>
    <a href="{_html_escape(links['cost_analysis_sub'])}" target="_blank">Cost Analysis (Subscription)</a>
    <a href="{_html_escape(links['budgets'])}" target="_blank">Budgets</a>
    <a href="{_html_escape(links['adf_monitor'])}" target="_blank">ADF Monitor — pipeline runs</a>
  </p>

  {_table_html("1. RG cost by Azure service (MTD)", mtd_services_rg)}
  {_table_html("2. RG cost by resource (MTD) — click Azure portal for more", mtd_resources)}
  {_table_html("3. RG daily spend (lookback)", daily)}
  {_table_html("4. Subscription cost by service (MTD)", mtd_services_sub)}
  {_table_html("5. ADF pipeline estimated cost (duration share of factory)", adf_rows)}
  {_table_html("6. ADF pipeline runs (minute-level duration)", adf_runs[:80])}
  {_table_html("7. Azure Monitor metrics — performance NFRs (lookback window)", metrics)}
  {_table_html("8. Resources in RG (inventory)", resources)}

  <footer>
    Re-run: <code>cost-dashboard.cmd</code> · Notebook twin: <code>/Shared/day9/cost_management_master</code>
  </footer>
</main>
</body>
</html>
"""


def _print_table(title: str, rows: list[dict[str, Any]], limit: int = 20) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)
    if not rows:
        print("  (no rows — cost data can lag 24–48h on sponsorship subs)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in rows[:limit])) for c in cols}
    print("  " + " | ".join(c.ljust(widths[c]) for c in cols))
    print("  " + "-+-".join("-" * widths[c] for c in cols))
    for r in rows[:limit]:
        print("  " + " | ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))
    if len(rows) > limit:
        print(f"  … {len(rows) - limit} more rows (see CSV / HTML)")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    # Quiet chatty Azure HTTP loggers
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("azure.identity").setLevel(logging.WARNING)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser(description="FinLedger shared-lab cost dashboard")
    ap.add_argument("--resource-group", default=DEFAULT_RG)
    ap.add_argument("--adf-name", default=DEFAULT_ADF)
    ap.add_argument("--days", type=int, default=14, help="Lookback days for daily + ADF runs")
    ap.add_argument("--open", action="store_true", help="Open HTML dashboard in browser")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = ap.parse_args()

    file_env = _load_env()
    sub = _subscription_id(file_env)
    rg = args.resource_group
    adf = args.adf_name
    credential = _credential(file_env)

    OUT_DIR.mkdir(parents=True, exist_ok=True) if False else None
    args.out_dir.mkdir(parents=True, exist_ok=True)

    scope_rg = _scope_rg(sub, rg)
    scope_sub = _scope_sub(sub)
    custom = _days_window(args.days)
    links = _portal_links(sub, rg)

    print(f"Subscription : {sub}")
    print(f"Resource group: {rg}")
    print(f"ADF factory  : {adf}")
    print(f"Lookback     : {args.days} days")

    # --- Cost queries (spaced to avoid Cost Management 429) ---
    import time as _time

    logger.info("Querying Cost Management (RG by ServiceName)…")
    raw_svc = _cost_query(credential, scope_rg, group_by=["ServiceName"])
    mtd_services_rg = [
        {"cost": float(r[0]), "service": r[1], "currency": r[2] if len(r) > 2 else "USD"}
        for r in sorted(raw_svc, key=lambda x: -float(x[0]))
    ]

    _time.sleep(3)
    logger.info("Querying Cost Management (RG by ResourceId)…")
    raw_res = _cost_query(credential, scope_rg, group_by=["ResourceId", "ServiceName"])
    mtd_resources = []
    for r in sorted(raw_res, key=lambda x: -float(x[0])):
        rid = str(r[1])
        mtd_resources.append(
            {
                "cost": float(r[0]),
                "resource": rid.split("/")[-1],
                "service": r[2] if len(r) > 2 else "",
                "resource_id": rid,
            }
        )

    _time.sleep(3)
    logger.info("Querying Cost Management (RG daily)…")
    # Prefer MonthToDate+Daily (most reliable). Fall back to custom window.
    raw_daily = _cost_query(credential, scope_rg, timeframe="MonthToDate", granularity="Daily")
    if not raw_daily:
        raw_daily = _cost_query(credential, scope_rg, timeframe=custom, granularity="Daily")
    daily = []
    for r in raw_daily:
        cost = float(r[0])
        day = str(r[1])[:10]
        daily.append({"date": day, "cost": cost})
    daily.sort(key=lambda x: x["date"])

    _time.sleep(3)
    logger.info("Querying Cost Management (Subscription by ServiceName)…")
    raw_sub = _cost_query(credential, scope_sub, group_by=["ServiceName"])
    mtd_services_sub = [
        {"cost": float(r[0]), "service": r[1], "currency": r[2] if len(r) > 2 else "USD"}
        for r in sorted(raw_sub, key=lambda x: -float(x[0]))
    ][:25]

    # --- ADF ---
    factory_id = _adf_factory_resource_id(sub, rg, adf)
    _time.sleep(3)
    logger.info("Querying ADF factory cost…")
    raw_adf_cost = _cost_query(
        credential, scope_rg, group_by=["ResourceId"], filter_resource_id=factory_id
    )
    factory_cost = 0.0
    if raw_adf_cost:
        factory_cost = sum(float(r[0]) for r in raw_adf_cost)
    else:
        for row in mtd_resources:
            if adf.lower() in row["resource_id"].lower():
                factory_cost += float(row["cost"])

    logger.info("Listing ADF pipeline runs…")
    adf_runs = _adf_pipeline_runs(credential, sub, rg, adf, args.days)
    adf_rows = _attribute_adf_cost(adf_runs, factory_cost)

    logger.info("Listing RG resources…")
    try:
        resources = _list_rg_resources(credential, sub, rg)
    except Exception as e:
        logger.warning("Resource list failed: %s", e)
        resources = []

    logger.info("Querying Azure Monitor metrics (performance NFRs)…")
    try:
        metrics = _monitor_metrics(credential, resources, args.days)
    except Exception as e:
        logger.warning("Metrics collection failed: %s", e)
        metrics = []

    # --- Console ---
    _print_table("RG — cost by service (MTD)", mtd_services_rg)
    _print_table("RG — cost by resource (MTD)", mtd_resources)
    _print_table(f"RG — daily spend (last {args.days}d)", daily)
    _print_table("Subscription — top services (MTD)", mtd_services_sub)
    _print_table(f"ADF — estimated $ per pipeline (factory MTD ${factory_cost:.4f})", adf_rows)
    _print_table("ADF — recent runs (minute duration)", [
        {
            "pipeline": r["pipeline"],
            "status": r["status"],
            "duration_min": r["duration_min"],
            "start": r["start"][:19],
        }
        for r in adf_runs[:25]
    ])
    _print_table(f"Azure Monitor metrics (last {args.days}d — performance NFRs)", metrics, limit=30)

    print()
    print("Azure Portal deep links:")
    for k, v in links.items():
        print(f"  {k}: {v}")

    # --- Exports ---
    _write_csv(args.out_dir / "rg_by_service.csv", mtd_services_rg)
    _write_csv(args.out_dir / "rg_by_resource.csv", mtd_resources)
    _write_csv(args.out_dir / "rg_daily.csv", daily)
    _write_csv(args.out_dir / "sub_by_service.csv", mtd_services_sub)
    _write_csv(args.out_dir / "adf_pipeline_cost_estimate.csv", adf_rows)
    _write_csv(args.out_dir / "adf_pipeline_runs.csv", adf_runs)
    _write_csv(args.out_dir / "rg_resources.csv", resources)
    _write_csv(args.out_dir / "monitor_metrics.csv", metrics)

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = _render_html(
        rg=rg,
        sub=sub,
        mtd_services_rg=mtd_services_rg,
        mtd_resources=mtd_resources,
        daily=daily,
        mtd_services_sub=mtd_services_sub,
        adf_rows=adf_rows,
        adf_runs=adf_runs,
        resources=resources,
        metrics=metrics,
        links=links,
        factory_cost=factory_cost,
        generated=generated,
    )
    html_path = args.out_dir / "index.html"
    html_path.write_text(html, encoding="utf-8")
    print()
    print(f"HTML dashboard : {html_path}")
    print(f"CSV folder     : {args.out_dir}")

    if args.open:
        webbrowser.open(html_path.resolve().as_uri())

    # Summary JSON for automation
    summary = {
        "rg": rg,
        "subscription_id": sub,
        "rg_mtd_total": round(sum(r["cost"] for r in mtd_services_rg), 4),
        "sub_mtd_total": round(sum(r["cost"] for r in mtd_services_sub), 4),
        "adf_factory_mtd": round(factory_cost, 4),
        "adf_runs": len(adf_runs),
        "monitor_metrics": len(metrics),
        "html": str(html_path),
        "status": "Succeeded",
    }
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    # Ensure packages
    try:
        import azure.identity  # noqa: F401
        import azure.mgmt.costmanagement  # noqa: F401
        import azure.mgmt.datafactory  # noqa: F401
        import azure.mgmt.resource  # noqa: F401
    except ImportError:
        print("Installing Azure SDK packages into current Python…")
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-q",
                "azure-identity",
                "azure-mgmt-costmanagement",
                "azure-mgmt-datafactory",
                "azure-mgmt-resource",
            ]
        )
    raise SystemExit(main())
