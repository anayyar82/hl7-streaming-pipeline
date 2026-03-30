"""
Start Databricks Jobs and Delta Live Tables updates using the Workspace API.

The Databricks App service principal must have permission to run each job and
update the pipeline (typically CAN RUN on jobs, appropriate pipeline permission).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass
class TriggerResult:
    ok: bool
    message: str
    url: Optional[str] = None
    run_id: Optional[int] = None
    job_id: Optional[int] = None


def _workspace_host() -> str:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    return (w.config.host or "").rstrip("/")


def _org_prefix() -> str:
    oid = (os.environ.get("DATABRICKS_ORG_ID") or "").strip()
    if oid:
        return f"/?o={oid}"
    return ""


def job_run_url(job_id: int, run_id: int) -> str:
    h = _workspace_host().rstrip("/")
    op = _org_prefix()
    if op:
        return f"{h}{op}#job/{job_id}/run/{run_id}"
    return f"{h}/#job/{job_id}/run/{run_id}"


def pipeline_update_url(pipeline_id: str, update_id: str) -> str:
    h = _workspace_host().rstrip("/")
    op = _org_prefix()
    if op:
        return f"{h}{op}#joblist/pipelines/{pipeline_id}/updates/{update_id}"
    return f"{h}/#joblist/pipelines/{pipeline_id}/updates/{update_id}"


def trigger_job(
    job_id: int,
    notebook_params: Optional[Mapping[str, str]] = None,
) -> TriggerResult:
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        params: dict[str, str] = (
            {k: str(v) for k, v in notebook_params.items()} if notebook_params else {}
        )
        if params:
            resp = w.jobs.run_now(job_id=job_id, notebook_params=params)
        else:
            resp = w.jobs.run_now(job_id=job_id)
        rid = getattr(resp, "run_id", None)
        if rid is None:
            return TriggerResult(False, "Job started but no run_id in API response.")
        rid_int = int(rid)
        return TriggerResult(
            True,
            f"Job **{job_id}** started — run id **{rid_int}**.",
            job_run_url(job_id, rid_int),
            run_id=rid_int,
            job_id=job_id,
        )
    except Exception as e:
        err = getattr(e, "message", None) or str(e)
        if "DatabricksError" in type(e).__name__ or "PERMISSION_DENIED" in err or "403" in err:
            err += " Check the app service principal has **Can Run** (or manage) on this job."
        return TriggerResult(False, err)


def trigger_pipeline_update(
    pipeline_id: str,
    *,
    full_refresh: bool = False,
) -> TriggerResult:
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        # Some SDK versions accept cause=; full_refresh only when True.
        if full_refresh:
            try:
                resp = w.pipelines.start_update(
                    pipeline_id=pipeline_id,
                    full_refresh=True,
                    cause="API_CALL",
                )
            except TypeError:
                resp = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
        else:
            try:
                resp = w.pipelines.start_update(pipeline_id=pipeline_id, cause="API_CALL")
            except TypeError:
                resp = w.pipelines.start_update(pipeline_id=pipeline_id)
        uid = getattr(resp, "update_id", None)
        if not uid:
            return TriggerResult(False, "Pipeline update started but no update_id returned.")
        return TriggerResult(
            True,
            f"Pipeline **{pipeline_id[:8]}…** update started.",
            pipeline_update_url(pipeline_id, str(uid)),
        )
    except Exception as e:
        err = str(e)
        if "403" in err or "PERMISSION" in err.upper():
            err += " Grant the app SP permission to run this pipeline."
        return TriggerResult(False, err)


def parse_job_id(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    s = str(raw).strip()
    if not s.isdigit():
        return None
    return int(s)
