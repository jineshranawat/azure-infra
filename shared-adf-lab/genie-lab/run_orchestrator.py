"""Genie question orchestrator — ask suite + multi-turn plan; write results."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from _common import OUT, api, must_ok, pick_warehouse, start_warehouse, write_json
from questions import ORCHESTRATOR_PLAN, QUESTION_SUITE

STATE_FILE = OUT / "genie_agent_state.json"


def _load_state() -> dict:
    if not STATE_FILE.exists():
        raise SystemExit("Missing genie_agent_state.json — run create_genie_agent.py first")
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def _extract_answer(message: dict) -> dict[str, Any]:
    """Normalize Genie message payload into a compact result."""
    out: dict[str, Any] = {
        "status": message.get("status"),
        "conversation_id": message.get("conversation_id"),
        "message_id": message.get("id") or message.get("message_id"),
        "content": None,
        "sql": None,
        "description": None,
        "attachments": [],
        "errors": message.get("error") or message.get("errors"),
        "user_prompt": message.get("content"),
    }
    attachments = message.get("attachments") or []
    for att in attachments:
        item: dict[str, Any] = {"raw_keys": list(att.keys()) if isinstance(att, dict) else []}
        if not isinstance(att, dict):
            out["attachments"].append(att)
            continue
        if "text" in att:
            item["text"] = att.get("text")
            text = att.get("text")
            if isinstance(text, dict) and text.get("content"):
                out["content"] = text["content"]
            elif isinstance(text, str):
                out["content"] = text
        if "query" in att:
            q = att["query"]
            if isinstance(q, dict):
                item["query"] = q.get("query") or q.get("sql") or q
                item["description"] = q.get("description")
                out["sql"] = out["sql"] or item["query"]
                out["description"] = out["description"] or item["description"]
            else:
                item["query"] = q
                out["sql"] = out["sql"] or q
        if "query_result" in att:
            item["query_result"] = att.get("query_result")
        out["attachments"].append(item)
    if not out["content"] and out.get("description"):
        out["content"] = out["description"]
    return out


def _start_conversation(space_id: str, content: str, timeout_sec: int = 300) -> dict:
    print(f"    -> start-conversation…", flush=True)
    status, payload = api(
        "POST",
        f"/api/2.0/genie/spaces/{space_id}/start-conversation",
        {"content": content},
        timeout=60,
    )
    must_ok(status, payload, "start-conversation")
    assert isinstance(payload, dict)
    conversation_id = payload.get("conversation_id")
    message_id = None
    message = payload.get("message") if isinstance(payload.get("message"), dict) else payload
    if isinstance(message, dict):
        message_id = message.get("id") or message.get("message_id")
        conversation_id = conversation_id or message.get("conversation_id")
    if not conversation_id or not message_id:
        # Some responses nest differently
        conversation_id = conversation_id or payload.get("conversation", {}).get("id")
        message_id = message_id or payload.get("message_id")
    if not conversation_id or not message_id:
        return {"status": "UNKNOWN", "raw": payload, "content": str(payload)[:500]}

    return _poll_message(space_id, conversation_id, message_id, timeout_sec)


def _followup(space_id: str, conversation_id: str, content: str, timeout_sec: int = 300) -> dict:
    status, payload = api(
        "POST",
        f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
        {"content": content},
        timeout=60,
    )
    must_ok(status, payload, "create message")
    assert isinstance(payload, dict)
    message_id = payload.get("id") or payload.get("message_id")
    if isinstance(payload.get("message"), dict):
        message_id = message_id or payload["message"].get("id")
    if not message_id:
        return {"status": "UNKNOWN", "raw": payload}
    return _poll_message(space_id, conversation_id, message_id, timeout_sec)


def _poll_message(space_id: str, conversation_id: str, message_id: str, timeout_sec: int) -> dict:
    deadline = time.time() + timeout_sec
    last: dict = {}
    while time.time() < deadline:
        status, payload = api(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
            timeout=60,
        )
        if status >= 400:
            return {"status": "ERROR", "http": status, "raw": payload}
        assert isinstance(payload, dict)
        last = payload
        st = (payload.get("status") or "").upper()
        if st in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            return _extract_answer(payload)
        time.sleep(3)
    return {"status": "TIMEOUT", "raw": last}


def run_suite() -> dict:
    state = _load_state()
    space_id = state["space_id"]
    print("=" * 68)
    print("GENIE QUESTION ORCHESTRATOR")
    print("=" * 68)
    print(f"  space_id: {space_id}")
    print(f"  questions: {len(QUESTION_SUITE)} + orchestrator plan {len(ORCHESTRATOR_PLAN)}")

    wh = pick_warehouse()
    start_warehouse(wh["id"])

    results: list[dict] = []
    for item in QUESTION_SUITE:
        qid = item["id"]
        question = item["question"]
        print(f"\n>>> [{qid}] {item['category']}: {question}")
        try:
            ans = _start_conversation(space_id, question)
            entry = {
                "id": qid,
                "category": item["category"],
                "question": question,
                "answer": ans,
                "followups": [],
            }
            print(f"    status={ans.get('status')} content={(str(ans.get('content') or ''))[:160]}")
            conv = ans.get("conversation_id")
            for fu in item.get("followups") or []:
                if not conv:
                    break
                print(f"    follow-up: {fu}")
                fu_ans = _followup(space_id, conv, fu)
                entry["followups"].append({"question": fu, "answer": fu_ans})
                print(f"    status={fu_ans.get('status')} content={(str(fu_ans.get('content') or ''))[:120]}")
            results.append(entry)
        except Exception as exc:  # noqa: BLE001 — keep suite running
            print(f"    ERROR: {exc}")
            results.append(
                {
                    "id": qid,
                    "category": item["category"],
                    "question": question,
                    "error": str(exc),
                }
            )

    # Multi-step orchestrator conversation
    print("\n>>> ORCHESTRATOR multi-step plan")
    orch: dict[str, Any] = {"steps": []}
    try:
        first = ORCHESTRATOR_PLAN[0]
        ans = _start_conversation(space_id, first)
        orch["steps"].append({"question": first, "answer": ans})
        conv = ans.get("conversation_id")
        print(f"    step1 status={ans.get('status')}")
        for step in ORCHESTRATOR_PLAN[1:]:
            if not conv:
                break
            print(f"    step: {step[:80]}…")
            ans = _followup(space_id, conv, step)
            orch["steps"].append({"question": step, "answer": ans})
            print(f"    status={ans.get('status')}")
    except Exception as exc:  # noqa: BLE001
        orch["error"] = str(exc)

    summary = {
        "space_id": space_id,
        "ui_url": state.get("ui_url"),
        "suite_count": len(QUESTION_SUITE),
        "answered": sum(1 for r in results if r.get("answer")),
        "errors": sum(1 for r in results if r.get("error")),
        "by_category": {},
        "results": results,
        "orchestrator": orch,
    }
    for r in results:
        cat = r.get("category") or "unknown"
        summary["by_category"].setdefault(cat, {"ok": 0, "err": 0})
        if r.get("error"):
            summary["by_category"][cat]["err"] += 1
        else:
            summary["by_category"][cat]["ok"] += 1

    path = write_json("orchestrator_results.json", summary)
    # Compact markdown report
    lines = [
        "# Genie orchestrator results",
        "",
        f"- space_id: `{space_id}`",
        f"- UI: {state.get('ui_url')}",
        f"- suite: {summary['answered']}/{summary['suite_count']} answered ({summary['errors']} errors)",
        "",
    ]
    for r in results:
        lines.append(f"## {r['id']} — {r.get('category')}")
        lines.append(f"**Q:** {r['question']}")
        if r.get("error"):
            lines.append(f"**Error:** {r['error']}")
        else:
            ans = r.get("answer") or {}
            lines.append(f"**Status:** {ans.get('status')}")
            lines.append(f"**A:** {ans.get('content')}")
        lines.append("")
    (OUT / "orchestrator_results.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n" + "=" * 68)
    print("ORCHESTRATOR COMPLETE")
    print(f"  Results JSON: {path}")
    print(f"  Results MD  : {OUT / 'orchestrator_results.md'}")
    print("=" * 68)
    return summary


if __name__ == "__main__":
    run_suite()
