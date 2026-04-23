"""
SLO-style freshness helpers — Lakebase max timestamps + optional DLT snapshot.
Thresholds line up with `STATUS_MONITOR_SPECS` + `_status_label` on 0_status.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
import streamlit as st

from utils import queries
from utils.db import run_query_batch
from utils.hl7_env import hl7_pipeline_id

PIPELINE_ID = hl7_pipeline_id()

# Bumped to invalidate st.cache_data when DLT one-line / humanize behavior changes.
_DLT_CACHE_BUMP = 3


def _normalize_dlt_line(s: str) -> str:
    """
    Strips legacy "error: …" and re-applies humanize (helps after deploy; cache may
    have held the raw API string).
    """
    t = (s or "").strip()
    if not t:
        return t
    if re.match(r"(?i)^error:\s*", t) or "the specified pipeline" in t.lower():
        from utils.databricks_activity import humanize_pipeline_error

        return humanize_pipeline_error(t)
    return t


def _dlt_metric_short(dlt_line: str) -> str:
    """Short label for the DLT st.metric value (full text in help=)."""
    t = (dlt_line or "").strip()
    lo = t.lower()
    if not t or t in ("—",):
        return "—"
    if "not found in this workspace" in lo or (
        "the specified pipeline" in lo and "not found" in lo
    ):
        return "ID not in workspace"
    if "not configured" in lo or "pipeline id not configured" in lo:
        return "Not configured"
    if "not permitted" in lo or "not permitted to read" in lo:
        return "No access"
    if re.search(r"unavailable|unknown pipeline error", lo):
        return "Unavailable"
    if "·" in t and len(t) <= 64:
        return t
    if len(t) <= 52:
        return t
    return t[:50] + "…"


def _hours_since(ts: Any) -> float | None:
    if ts is None:
        return None
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return None
    t = t.tz_localize(None) if getattr(t, "tzinfo", None) else t
    return (pd.Timestamp.now() - t).total_seconds() / 3600.0


def slo_tier(age_h: float | None, stale_after: float | None) -> str:
    """ok | stale | critical | —  (2× band, same as pages/0_status._status_label)"""
    if age_h is None or stale_after is None:
        return "—"
    if age_h <= stale_after:
        return "ok"
    if age_h <= stale_after * 2:
        return "stale"
    return "critical"


def _format_age(age_h: float | None) -> str:
    if age_h is None:
        return "—"
    if age_h < 1.0:
        return f"{int(age_h * 60)}m ago"
    return f"{age_h:.1f}h ago"


def _tier_emoji(tier: str) -> str:
    return {"ok": "🟢", "stale": "🟡", "critical": "🔴"}.get(tier, "➖")


def _cell_ts(df: pd.DataFrame) -> Any:
    if df is None or df.empty or "t" not in df.columns:
        return None
    v = df["t"].iloc[0]
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return v


@st.cache_data(ttl=45, show_spinner=False)
def _dlt_snapshot_cached_impl(pipeline_id: str, _bump: int) -> str:
    if not (pipeline_id or "").strip():
        return "not configured"
    try:
        from utils.databricks_activity import get_pipeline_snapshot

        snap = get_pipeline_snapshot(pipeline_id)
        if snap.error:
            return _normalize_dlt_line(snap.error)
        parts = [p for p in (snap.state, snap.latest_update_state) if p]
        return _normalize_dlt_line(" · ".join(parts) if parts else "—")
    except Exception as e:
        return _normalize_dlt_line(f"unavailable ({str(e)[:120]})")


def dlt_snapshot_cached(pipeline_id: str) -> str:
    """
    One-line DLT string for the home / status SLO row. Public wrapper so callers
    use a stable name; second arg _bump invalidates cache when DLT text logic changes.
    """
    return _dlt_snapshot_cached_impl(pipeline_id, _DLT_CACHE_BUMP)


def health_freshness_queries() -> dict[str, str]:
    return {
        "h_msg": queries.HEALTH_FRESHNESS_MSG,
        "h_enc": queries.HEALTH_FRESHNESS_ENC,
        "h_ml": queries.HEALTH_FRESHNESS_ML,
    }


def render_freshness_metrics_row(home_batch: dict[str, pd.DataFrame] | None = None) -> None:
    """
    Four columns: messages, encounters, ML, DLT; then a narrow link column.
    If `home_batch` is None, runs a small `run_query_batch` for the three SQL keys.
    """
    if home_batch is None:
        b = run_query_batch(health_freshness_queries(), quiet=True)
    else:
        b = home_batch

    slo = queries.HEALTH_SLO_HOURS
    m1, m2, m3, m4, m5 = st.columns([1, 1, 1, 1.1, 0.75])
    any_crit = False
    max_age: float = 0.0

    for col, key, label in (
        (m1, "h_msg", "Messages (stream)"),
        (m2, "h_enc", "Encounters (gold)"),
        (m3, "h_ml", "ML predictions"),
    ):
        df = b.get(key, pd.DataFrame()) if b else pd.DataFrame()
        ts = _cell_ts(df)
        age = _hours_since(ts)
        if age is not None:
            max_age = max(max_age, float(age))
        tier = slo_tier(age, slo.get(key))
        if tier == "critical":
            any_crit = True
        with col:
            d = f"{_tier_emoji(tier)} {tier}" if tier != "—" else "no timestamp"
            h = f"Last activity: {ts}" if ts is not None else "No last timestamp in table"
            st.metric(label, _format_age(age), delta=d, help=h)

    dlt_line = dlt_snapshot_cached(PIPELINE_ID)
    dlt_sh = _dlt_metric_short(dlt_line)
    dlt_help = f"HL7_PIPELINE_ID: {PIPELINE_ID or 'unset'}\n\n{dlt_line}"
    with m4:
        st.metric("DLT pipeline", dlt_sh, help=dlt_help)
        if not PIPELINE_ID:
            st.caption("Set `HL7_PIPELINE_ID` in app")
    with m5:
        st.write("")
        st.page_link(
            "pages/z_run_jobs.py",
            label="Run jobs",
            icon="🚀",
            help="Bundled DLT → inference → Lakebase when `HL7_JOB_REFRESH_WORKFLOW` is set",
            use_container_width=True,
        )

    st.caption(
        f"SLO: **ok** within the target window · **stale** up to 2× that · **critical** beyond 2× "
        f"(stream {int(slo['h_msg'])}h, encounters {int(slo['h_enc'])}h, ML {int(slo['h_ml'])}h). "
        "Per-table list: **Status** (sidebar)."
    )
    dlt_l = dlt_line.lower()
    dlt_broken = "not found" in dlt_l or "id not in workspace" in dlt_l or dlt_sh == "Not configured"
    if (any_crit or max_age >= 48.0) and dlt_broken:
        st.caption(
            "If this environment is meant to be live, **set `HL7_PIPELINE_ID` to a pipeline in this workspace** "
            "(Pipelines list / `databricks pipelines list`) and run **Run jobs** → the **DLT → Inference → Lakebase** job so Lakebase and gold see new data."
        )


def render_status_slo_banner() -> None:
    """Compact SLO row for 0_status — its own small batch (page already hits many queries)."""
    st.subheader("SLO snapshot (Lakebase + DLT)")
    st.caption(
        "Same **ok / stale / critical** rules as the matrix below, on three headline signals. "
        "For the default refresh path, use **Run jobs & workflow** and the **DLT → Inference → Lakebase (bundle)** job when configured."
    )
    b = run_query_batch(health_freshness_queries(), quiet=True)
    r1, r2, r3, r4 = st.columns(4, gap="medium")
    dlt = dlt_snapshot_cached(PIPELINE_ID)
    slo = queries.HEALTH_SLO_HOURS

    for col, key, label in (
        (r1, "h_msg", "Message stream (max `last_message_at`)"),
        (r2, "h_enc", "Encounters (max `created_at`)"),
        (r3, "h_ml", "ML (max `forecast_generated_at`)"),
    ):
        df = b.get(key, pd.DataFrame())
        ts = _cell_ts(df)
        age = _hours_since(ts)
        tier = slo_tier(age, slo.get(key))
        with col:
            st.metric(label, _format_age(age), delta=f"{_tier_emoji(tier)} {tier}" if tier != "—" else None)
    with r4:
        dlt_s = _dlt_metric_short(dlt)
        dlt_h = f"HL7_PIPELINE_ID: {PIPELINE_ID or 'unset'}\n\n{dlt}"
        st.metric("DLT (workspace)", dlt_s, help=dlt_h)

    c1, c2 = st.columns([3, 1], gap="small")
    with c1:
        st.page_link("pages/z_run_jobs.py", label="Open Run jobs & workflow", icon="🚀", use_container_width=True)
    with c2:
        st.page_link("pages/0b_live_activity.py", label="Live activity", icon="⚡", use_container_width=True)
    st.markdown("---")
