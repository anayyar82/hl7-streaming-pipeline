"""
Periodic refresh for Lakebase-backed UI (Streamlit ≥ 1.33).

Without this, multipage apps only re-query Postgres on full reruns (navigation,
widgets, manual browser refresh). Wrapping data panels in ``st.fragment(run_every=…)``
re-runs that slice on a timer so metrics and charts stay current after DLT / Lakebase loads.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Callable

import streamlit as st


def periodic_fragment(interval_seconds: int):
    """Return ``st.fragment(run_every=…)`` or ``None`` if the runtime is too old."""
    try:
        return st.fragment(run_every=timedelta(seconds=interval_seconds))
    except (TypeError, AttributeError):
        return None


def run_live_dashboard(
    body: Callable[[], None],
    *,
    interval_seconds: int = 20,
    manual_key: str = "hl7_manual_data_refresh",
) -> None:
    """
    Run ``body`` on a timer inside a fragment when supported.

    ``manual_key`` must be unique per page (Streamlit widget key).
    """
    frag = periodic_fragment(interval_seconds)
    if frag is not None:

        @frag
        def _wrapped() -> None:
            body()
            st.caption(f"Data auto-refreshes about every **{interval_seconds}** seconds.")

        _wrapped()
        return

    body()
    st.warning(
        "Live auto-refresh needs **Streamlit ≥ 1.33** with ``st.fragment(run_every=…)``. "
        "This app pins ``streamlit>=1.36`` — redeploy the app image if you see this."
    )
    if st.button("Refresh data now", type="primary", key=manual_key):
        st.rerun()
