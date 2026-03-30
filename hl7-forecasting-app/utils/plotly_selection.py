"""
Normalize st.plotly_chart(on_select=...) selection for drill-down tables.

Streamlit versions differ slightly in return shape; we accept dict-like objects
and fall back to matching x-axis values when point_index is unreliable.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _points_from_event(event: Any) -> list[dict]:
    if event is None:
        return []
    if isinstance(event, dict):
        sel = event.get("selection")
        if isinstance(sel, dict) and "points" in sel:
            pts = sel.get("points") or []
        elif "points" in event:
            pts = event.get("points") or []
        else:
            pts = []
        out = []
        for p in pts:
            if isinstance(p, dict):
                out.append(p)
            else:
                d = {}
                for k in ("x", "y", "point_index", "pointIndex", "curve_number", "curveNumber"):
                    v = getattr(p, k, None)
                    if v is not None:
                        d[k.replace("pointIndex", "point_index").replace("curveNumber", "curve_number")] = v
                if "point_index" not in d and "pointIndex" in dir(p):
                    try:
                        d["point_index"] = getattr(p, "pointIndex", None)
                    except Exception:
                        pass
                out.append(d)
        return out
    sel = getattr(event, "selection", None)
    if sel is None:
        return []
    pts = getattr(sel, "points", None) or []
    result = []
    for p in pts:
        if isinstance(p, dict):
            result.append(p)
        else:
            x, y = getattr(p, "x", None), getattr(p, "y", None)
            pi = getattr(p, "point_index", None)
            if pi is None:
                pi = getattr(p, "pointIndex", None)
            result.append({"x": x, "y": y, "point_index": pi})
    return result


def selected_row_indices(df: pd.DataFrame, event: Any, x_column: str) -> list[int]:
    """
    Map Plotly selection to df row positions (0..len-1).

    Prefer point_index when present and in range; else match x_column to each point's x.
    """
    points = _points_from_event(event)
    if not points:
        return []

    n = len(df)
    by_index: list[int] = []
    for p in points:
        pi = p.get("point_index")
        if pi is None:
            pi = p.get("pointIndex")
        if pi is not None:
            try:
                i = int(pi)
                if 0 <= i < n:
                    by_index.append(i)
            except (TypeError, ValueError):
                pass

    if by_index:
        return sorted(set(by_index))

    xs = df[x_column]
    try:
        xs_dt = pd.to_datetime(xs, utc=False, errors="coerce")
    except Exception:
        xs_dt = xs

    by_x: list[int] = []
    for p in points:
        raw = p.get("x")
        if raw is None:
            continue
        try:
            target = pd.to_datetime(raw, utc=False, errors="coerce")
        except Exception:
            target = raw
        for pos in range(n):
            try:
                if pd.isna(target) and pd.isna(xs_dt.iloc[pos]):
                    by_x.append(pos)
                elif not pd.isna(target) and xs_dt.iloc[pos] == target:
                    by_x.append(pos)
            except Exception:
                if str(xs.iloc[pos]) == str(raw):
                    by_x.append(pos)
    return sorted(set(by_x))


def selection_state_from_chart(event: Any, state_key: str, session_state) -> Any:
    """Prefer plotly_chart return value; fall back to session_state[key]."""
    if event is not None:
        return event
    return session_state.get(state_key)
