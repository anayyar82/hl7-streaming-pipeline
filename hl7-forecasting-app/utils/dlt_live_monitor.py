"""
DLT / Lakeflow pipeline live monitoring via Workspace Pipelines API.

Uses get_update for coarse update state and list_pipeline_events for flow_progress
rows (per-flow status, row counts). See:
https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd


def _client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _as_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except json.JSONDecodeError:
            return {}
    # Dataclass-like SDK objects
    out: dict[str, Any] = {}
    for k in dir(obj):
        if k.startswith("_"):
            continue
        try:
            v = getattr(obj, k)
        except Exception:
            continue
        if callable(v):
            continue
        out[k] = v
    return out


def _fmt_ts(ms: Any) -> str:
    if ms is None:
        return "—"
    try:
        m = int(ms) / 1000.0
        return datetime.fromtimestamp(m, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError, OSError):
        return str(ms)


def fetch_update_row(pipeline_id: str, update_id: str) -> dict[str, Any]:
    """Single-row summary from get_update (best-effort field extraction)."""
    if not pipeline_id.strip() or not update_id.strip():
        return {"error": "pipeline_id and update_id required"}
    try:
        w = _client()
        resp = w.pipelines.get_update(pipeline_id=pipeline_id.strip(), update_id=update_id.strip())
    except Exception as e:
        return {"error": str(e)[:500]}

    upd_obj = getattr(resp, "update", None)
    if upd_obj is not None:
        d = _as_dict(upd_obj)
    else:
        d = _as_dict(resp)
        inner = d.get("update")
        if inner is not None:
            d = _as_dict(inner)

    row: dict[str, Any] = {}
    for key in (
        "state",
        "cause",
        "cluster_id",
        "creation_time",
        "end_time",
        "last_modified",
        "validate_only",
    ):
        if key in d and d[key] is not None:
            row[key] = d[key]
    if "creation_time" in row:
        row["creation_time_fmt"] = _fmt_ts(row["creation_time"])
    if "end_time" in row:
        row["end_time_fmt"] = _fmt_ts(row["end_time"])
    return row


def fetch_pipeline_events(
    pipeline_id: str,
    *,
    max_results: int = 120,
    update_id_hint: Optional[str] = None,
) -> list[Any]:
    """Recent pipeline events (newest first when order_by is supported)."""
    w = _client()
    out: list[Any] = []
    kwargs: dict[str, Any] = {
        "pipeline_id": pipeline_id.strip(),
        "max_results": min(max(1, max_results), 100),
    }
    for extra in ({"order_by": ["timestamp desc"]}, {}):
        try:
            it = w.pipelines.list_pipeline_events(**kwargs, **extra)
            for ev in it:
                out.append(ev)
            break
        except Exception:
            continue

    if update_id_hint and out:
        hint = update_id_hint.strip()
        filtered = []
        for ev in out:
            ed = _event_details_dict(ev)
            top_uid = getattr(ev, "update_id", None) or ed.get("update_id")
            if top_uid and str(top_uid) == hint:
                filtered.append(ev)
            elif hint in json.dumps(ed, default=str):
                filtered.append(ev)
        if filtered:
            return filtered
    return out


def _event_details_dict(ev: Any) -> dict[str, Any]:
    details = getattr(ev, "details", None)
    if details is None and isinstance(ev, dict):
        details = ev.get("details")
    if isinstance(details, str):
        return _as_dict(details)
    return _as_dict(details)


def _event_type(ev: Any) -> str:
    et = getattr(ev, "event_type", None)
    if et is None and isinstance(ev, dict):
        et = ev.get("event_type")
    return str(et or "")


def events_to_dataframes(events: list[Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build (flow_progress_latest, events_compact).

    flow_progress: one row per flow_id keeping the newest event (status + row metrics).
    events_compact: recent audit trail (timestamp, type, level, message snippet).
    """
    flow_rows: list[dict[str, Any]] = []
    compact: list[dict[str, Any]] = []

    for ev in events:
        ts = getattr(ev, "timestamp", None)
        if ts is None and isinstance(ev, dict):
            ts = ev.get("timestamp")
        ts_fmt = _fmt_ts(ts)

        et = _event_type(ev)
        lvl = getattr(ev, "level", None) or (ev.get("level") if isinstance(ev, dict) else None)
        msg = getattr(ev, "message", None) or (ev.get("message") if isinstance(ev, dict) else None)
        mid = str(msg or "")[:240]

        compact.append(
            {
                "Time (UTC)": ts_fmt,
                "Type": et or "—",
                "Level": str(lvl or "—"),
                "Message": mid or "—",
            }
        )

        if et != "flow_progress":
            continue

        det = _event_details_dict(ev)
        metrics = det.get("metrics") if isinstance(det.get("metrics"), dict) else _as_dict(det.get("metrics"))
        flow_rows.append(
            {
                "_ts": ts,
                "Time (UTC)": ts_fmt,
                "Flow": str(det.get("flow_name") or det.get("name") or det.get("flow_id") or "—"),
                "Flow id": str(det.get("flow_id") or "—"),
                "Status": str(det.get("status") or "—"),
                "Output rows": metrics.get("num_output_rows"),
                "Upserted": metrics.get("num_upserted_rows"),
                "Deleted": metrics.get("num_deleted_rows"),
                "Output bytes": metrics.get("num_output_bytes"),
            }
        )

    flow_df = pd.DataFrame(flow_rows)
    if not flow_df.empty and "_ts" in flow_df.columns:
        flow_df = flow_df.sort_values("_ts", ascending=True)
        flow_df = flow_df.drop_duplicates(subset=["Flow id", "Flow"], keep="last")
        flow_df = flow_df.drop(columns=["_ts"], errors="ignore")

    comp_df = pd.DataFrame(compact)
    return flow_df, comp_df
