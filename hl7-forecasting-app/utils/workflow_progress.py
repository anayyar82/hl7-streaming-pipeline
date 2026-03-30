"""
Summarize multi-task Databricks job runs for workflow progress in the Streamlit app.

Uses task lifecycle states for step counts / percentage; enriches pipeline tasks with
Pipelines API update metadata when an update id is present (best-effort across SDK versions).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


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


def _task_state(t: Any) -> tuple[str, str, str]:
    st = getattr(t, "state", None)
    if st is None:
        return "UNKNOWN", "", ""
    life = getattr(st, "life_cycle_state", None) or ""
    res = getattr(st, "result_state", None) or ""
    msg = getattr(st, "state_message", None) or ""
    return str(life), str(res), str(msg)


def _task_kind(t: Any) -> str:
    if getattr(t, "pipeline_task", None) is not None:
        return "pipeline"
    if getattr(t, "run_job_task", None) is not None:
        return "run_job"
    if getattr(t, "notebook_task", None) is not None:
        return "notebook"
    if getattr(t, "spark_python_task", None) is not None:
        return "python"
    return "other"


def _pipeline_update_extras(w: Any, t: Any) -> str:
    spec = getattr(t, "pipeline_task", None)
    pid = str(getattr(spec, "pipeline_id", "") or "").strip() if spec else ""
    pto = getattr(t, "pipeline_task_run_output", None)
    if not pto or not pid:
        return ""
    uid = (
        getattr(pto, "pipeline_update_id", None)
        or getattr(pto, "update_id", None)
        or getattr(pto, "latest_update_id", None)
    )
    if not uid:
        return ""
    uid = str(uid).strip()
    upd = None
    for kwargs in (
        {"pipeline_id": pid, "update_id": uid},
        {"pipeline_id": pid, "update_id": uid, "include_metrics": True},
    ):
        try:
            upd = w.pipelines.get_update(**kwargs)
            break
        except TypeError:
            continue
        except Exception:
            break
    if upd is None:
        try:
            upd = w.pipelines.get_update(pid, uid)
        except Exception:
            return f"update_id={uid[:8]}…"
    parts: list[str] = []
    st = getattr(upd, "state", None) or getattr(upd, "update_state", None)
    if st:
        parts.append(f"DLT update: {st}")
    for attr in (
        "progress",
        "rows_written",
        "bytes_processed",
        "total_processing_time_ms",
        "data_processing_units",
    ):
        v = getattr(upd, attr, None)
        if v is not None:
            parts.append(f"{attr}={v}")
    # Some SDKs expose nested metrics
    m = getattr(upd, "metrics", None)
    if m is not None:
        parts.append(f"metrics={m!s}"[:200])
    return "; ".join(parts) if parts else f"update_id={uid[:8]}…"


def _run_job_extras(w: Any, t: Any) -> str:
    rjo = getattr(t, "run_job_task_run_output", None)
    if not rjo:
        return ""
    child_rid = getattr(rjo, "run_id", None)
    child_jid = getattr(rjo, "job_id", None)
    bits = []
    if child_jid:
        bits.append(f"child job {child_jid}")
    if not child_rid:
        return "; ".join(bits) if bits else ""
    try:
        cr = w.jobs.get_run(int(child_rid))
        clife, cres, cmsg = _run_state(cr)
        bits.append(f"child run {clife}")
        if cres:
            bits.append(cres.lower())
        if cmsg and len(cmsg) < 120:
            bits.append(cmsg)
        # Single-task child jobs: surface that task state
        ctasks = getattr(cr, "tasks", None) or []
        if len(ctasks) == 1:
            tk = getattr(ctasks[0], "task_key", "") or "task"
            tl, tr, _ = _task_state(ctasks[0])
            bits.append(f"{tk}: {tl}" + (f" ({tr})" if tr else ""))
    except Exception as e:
        bits.append(str(e)[:80])
    return "; ".join(bits)


@dataclass
class WorkflowTaskRow:
    task_key: str
    kind: str
    lifecycle: str
    result: str
    state_message: str
    detail: str


@dataclass
class WorkflowRunSummary:
    run_id: int
    job_id: int
    life_cycle_state: str
    result_state: str
    state_message: str
    tasks: list[WorkflowTaskRow] = field(default_factory=list)
    tasks_finished: int = 0
    tasks_total: int = 0
    progress_pct: float = 0.0
    active_task_keys: list[str] = field(default_factory=list)
    error: str = ""


_TERMINAL_LIFECYCLES = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})


def summarize_workflow_run(run_id: int) -> WorkflowRunSummary:
    """Build a snapshot of a job run (multi-task workflows supported)."""
    out = WorkflowRunSummary(
        run_id=run_id,
        job_id=0,
        life_cycle_state="UNKNOWN",
        result_state="",
        state_message="",
    )
    try:
        w = _client()
        run = w.jobs.get_run(run_id)
    except Exception as e:
        out.error = str(e)[:500]
        return out

    out.job_id = int(getattr(run, "job_id", 0) or 0)
    life, res, msg = _run_state(run)
    out.life_cycle_state = life
    out.result_state = res
    out.state_message = msg

    tasks = getattr(run, "tasks", None)
    if not tasks:
        out.tasks_total = 0
        out.progress_pct = 100.0 if life == "TERMINATED" else 0.0
        return out

    rows: list[WorkflowTaskRow] = []
    finished = 0
    active: list[str] = []

    for t in tasks:
        tk = str(getattr(t, "task_key", "") or "?")
        kind = _task_kind(t)
        tl, tr, tm = _task_state(t)
        detail = ""
        if kind == "pipeline":
            detail = _pipeline_update_extras(w, t)
        elif kind == "run_job":
            detail = _run_job_extras(w, t)
        rows.append(
            WorkflowTaskRow(
                task_key=tk,
                kind=kind,
                lifecycle=tl,
                result=tr,
                state_message=(tm[:300] + "…") if len(tm) > 300 else tm,
                detail=detail,
            )
        )
        if tl in _TERMINAL_LIFECYCLES:
            finished += 1
        if tl == "RUNNING":
            active.append(tk)

    n = len(rows)
    out.tasks = rows
    out.tasks_total = n
    out.tasks_finished = finished
    out.active_task_keys = active
    out.progress_pct = (100.0 * finished / n) if n else 0.0
    if life == "TERMINATED":
        out.progress_pct = 100.0
    return out


def format_task_completion_line(s: WorkflowRunSummary) -> str:
    """Active step + DLT metrics line (step counts shown separately in the UI)."""
    if s.error:
        return s.error
    if not s.tasks_total:
        return "No tasks on this run (check job definition)."
    parts: list[str] = []
    if s.active_task_keys:
        parts.append(f"Running now: **{', '.join(s.active_task_keys)}**.")
    for row in s.tasks:
        if row.kind == "pipeline" and row.detail:
            parts.append(row.detail)
            break
    return (
        " ".join(parts)
        if parts
        else "Waiting or between steps — DLT row/byte metrics appear when the pipeline task exposes an update id."
    )
