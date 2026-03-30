"""
Fetch Databricks job and DLT pipeline status for the live activity monitor.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import pandas as pd


def _client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _run_state(run: Any) -> tuple[str, str, str]:
    st = getattr(run, "state", None)
    if st is None:
        return "UNKNOWN", "", ""
    life = getattr(st, "life_cycle_state", None) or ""
    res = getattr(st, "result_state", None) or ""
    msg = getattr(st, "state_message", None) or ""
    return str(life), str(res), str(msg)


def collect_active_runs(jobs: Iterable[tuple[str, Optional[int]]]) -> pd.DataFrame:
    """jobs: (label, job_id or None)."""
    rows: list[dict[str, Any]] = []
    w = _client()
    for label, jid in jobs:
        if jid is None:
            continue
        try:
            found = False
            for run in w.jobs.list_runs(job_id=jid, active_only=True, limit=10):
                found = True
                life, res, msg = _run_state(run)
                rows.append(
                    {
                        "Job": label,
                        "Job ID": jid,
                        "Run ID": getattr(run, "run_id", ""),
                        "Lifecycle": life,
                        "Result": res,
                        "Message": (msg[:200] + "…") if len(msg) > 200 else msg,
                    }
                )
            if not found:
                rows.append(
                    {
                        "Job": label,
                        "Job ID": jid,
                        "Run ID": "—",
                        "Lifecycle": "IDLE",
                        "Result": "",
                        "Message": "No active run",
                    }
                )
        except Exception as e:
            rows.append(
                {
                    "Job": label,
                    "Job ID": jid,
                    "Run ID": "—",
                    "Lifecycle": "ERROR",
                    "Result": "",
                    "Message": str(e)[:300],
                }
            )
    return pd.DataFrame(rows)


def collect_recent_runs(
    jobs: Iterable[tuple[str, Optional[int]]],
    *,
    limit_per_job: int = 6,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    w = _client()
    for label, jid in jobs:
        if jid is None:
            continue
        try:
            for run in w.jobs.list_runs(job_id=jid, limit=limit_per_job):
                life, res, _msg = _run_state(run)
                st_ms = getattr(run, "start_time", None)
                started = ""
                if st_ms is not None:
                    try:
                        started = datetime.fromtimestamp(st_ms / 1000, tz=timezone.utc).strftime(
                            "%Y-%m-%d %H:%M UTC"
                        )
                    except (TypeError, ValueError, OSError):
                        started = str(st_ms)
                rows.append(
                    {
                        "Job": label,
                        "Job ID": jid,
                        "Run ID": getattr(run, "run_id", ""),
                        "Started": started,
                        "Lifecycle": life,
                        "Result": res,
                    }
                )
        except Exception as e:
            rows.append(
                {
                    "Job": label,
                    "Job ID": jid,
                    "Run ID": "—",
                    "Started": "",
                    "Lifecycle": "ERROR",
                    "Result": str(e)[:200],
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@dataclass
class PipelineSnapshot:
    pipeline_id: str
    name: str
    state: str
    latest_update_state: str
    update_id: str
    cluster_id: str
    error: str


def get_pipeline_snapshot(pipeline_id: str) -> PipelineSnapshot:
    if not pipeline_id.strip():
        return PipelineSnapshot("", "", "—", "", "", "", "", "Pipeline ID not configured")
    try:
        w = _client()
        p = w.pipelines.get(pipeline_id)
        name = getattr(p, "name", None) or ""
        if not name:
            spec = getattr(p, "spec", None)
            name = getattr(spec, "name", None) or ""
        state = str(getattr(p, "state", "") or "")
        latest = getattr(p, "latest_updates", None)
        if latest is None:
            lu: list[Any] = []
        elif isinstance(latest, (list, tuple)):
            lu = list(latest)
        else:
            lu = [latest]
        upd_state = ""
        uid = ""
        cid = ""
        if lu:
            u0 = lu[0]
            upd_state = str(getattr(u0, "state", "") or "")
            uid = str(getattr(u0, "update_id", "") or "")
            cid = str(getattr(u0, "cluster_id", "") or "")
        return PipelineSnapshot(
            pipeline_id=pipeline_id,
            name=name,
            state=state,
            latest_update_state=upd_state,
            update_id=uid,
            cluster_id=cid,
            error="",
        )
    except Exception as e:
        return PipelineSnapshot(
            pipeline_id=pipeline_id,
            name="",
            state="ERROR",
            latest_update_state="",
            update_id="",
            cluster_id="",
            error=str(e)[:400],
        )


def configured_job_list() -> list[tuple[str, Optional[int]]]:
    from utils.databricks_trigger import parse_job_id

    def pid(key: str) -> Optional[int]:
        return parse_job_id(os.environ.get(key))

    return [
        ("Sample → volume", pid("HL7_JOB_SAMPLE_DATA")),
        ("AutoML training", pid("HL7_JOB_AUTOML")),
        ("Model inference", pid("HL7_JOB_INFERENCE")),
        ("Lakebase load", pid("HL7_JOB_LAKEBASE_LOAD")),
        ("Lakebase sync", pid("HL7_JOB_LAKEBASE_SYNC")),
    ]
