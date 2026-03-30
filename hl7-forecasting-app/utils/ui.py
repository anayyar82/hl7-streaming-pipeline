"""
Small interactive UI helpers (Streamlit) — use with apply_theme() for consistent look.
"""

from __future__ import annotations

from typing import Sequence

import streamlit as st


def home_focus_picker() -> str:
    """
    Segmented control (Streamlit ≥1.33) with radio fallback.
    Returns one of: clinical | platform | genie
    """
    options = [
        ("Clinical views", "clinical"),
        ("Platform & jobs", "platform"),
        ("Ask Genie", "genie"),
    ]
    labels = [o[0] for o in options]
    values = [o[1] for o in options]
    try:
        raw = st.segmented_control(
            "Start here",
            options=labels,
            default=labels[0],
            key="hl7_home_focus_seg",
            help="Choose a lane, then use the quick links below.",
        )
    except (TypeError, AttributeError):
        sel = st.radio(
            "Start here",
            labels,
            horizontal=True,
            key="hl7_home_focus_radio",
            help="Choose a lane, then use the quick links below.",
        )
        return values[labels.index(sel)]
    if raw is None:
        return values[0]
    return values[labels.index(raw)]


def home_quick_links(focus: str) -> None:
    """Render 3 page_link columns for the selected focus."""
    clinical = [
        ("pages/1_realtime.py", "Real-time ops", "📊"),
        ("pages/2_trends.py", "Trends", "📈"),
        ("pages/3_forecasting.py", "ML forecasts", "🔮"),
    ]
    platform = [
        ("pages/0b_live_activity.py", "Live activity", "⚡"),
        ("pages/z_run_jobs.py", "Run jobs", "🚀"),
        ("pages/9_platform_pulse.py", "Platform pulse", "🛰️"),
    ]
    genie = [
        ("pages/8_genie_chat.py", "Open Genie chat", "💬"),
        ("pages/0_status.py", "System status", "📡"),
        ("pages/6_combined_forecast.py", "Combined forecast", "⚖️"),
    ]
    m = {"clinical": clinical, "platform": platform, "genie": genie}
    links: Sequence[tuple[str, str, str]] = m.get(focus, clinical)
    c1, c2, c3 = st.columns(3, gap="medium")
    for col, (path, label, icon) in zip((c1, c2, c3), links):
        with col:
            st.page_link(path, label=label, icon=icon, use_container_width=True)
