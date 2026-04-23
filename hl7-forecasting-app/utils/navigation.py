"""
Categorized sidebar and home navigation — clinical vs platform vs Genie.
"""

from __future__ import annotations

import streamlit as st

from utils.theme import sidebar_product_context

# (path, label, icon) — order within each section is display order. icon None = no emoji.
def _sidebar_page_link(path: str, label: str, icon: str | None) -> None:
    if icon:
        st.sidebar.page_link(path, label=label, icon=icon)
    else:
        st.sidebar.page_link(path, label=label)


_CLINICAL = [
    ("pages/1_realtime.py", "Real-time ops", "📊"),
    ("pages/2_trends.py", "Trends & heatmaps", "📈"),
    ("pages/3_forecasting.py", "ML forecasting", "🔮"),
    ("pages/4_model_perf.py", "Model performance", "🎯"),
    ("pages/5_patient_clinical.py", "Patient & clinical", "🧬"),
    ("pages/6_combined_forecast.py", "Combined forecast", "⚖️"),
    ("pages/7_operations.py", "HL7 operations", "📨"),
]

_PLATFORM = [
    ("pages/0_status.py", "System status", "📡"),
    ("pages/0a_sample_to_volume.py", "Sample → volume", "📤"),
    ("pages/0b_live_activity.py", "Live activity", "⚡"),
    ("pages/0c_dlt_update_live.py", "DLT update live", "🔄"),
    ("pages/0d_load_test.py", "Load test", "🧪"),
    ("pages/z_run_jobs.py", "Run jobs & workflow", "🚀"),
    ("pages/9_platform_pulse.py", "Platform pulse", "🛰️"),
]

_GENIE = [
    ("pages/8_genie_chat.py", "Ask your data (Genie)", "💬"),
]


def render_sidebar_nav() -> None:
    """Call right after apply_theme() on Home and every multipage script."""
    st.sidebar.markdown(
        """
<div class="hl7-sidebar-brand">
  <span class="hl7-sidebar-brand-mark">🏥</span>
  <span class="hl7-sidebar-brand-text">HL7App</span>
</div>
<p class="hl7-sidebar-meta">Lakebase · <code>ankur_nayyar</code> · <code>ankurhlsproject</code></p>
        """,
        unsafe_allow_html=True,
    )
    st.sidebar.markdown("---")

    # Main script is not under pages/ — default Streamlit nav is hidden, so link Home explicitly.
    st.sidebar.markdown('<p class="hl7-sidebar-cat">App</p>', unsafe_allow_html=True)
    st.sidebar.page_link("app.py", label="Home", icon="🏠")

    st.sidebar.markdown('<p class="hl7-sidebar-cat">Clinical intelligence</p>', unsafe_allow_html=True)
    for path, label, icon in _CLINICAL:
        _sidebar_page_link(path, label, icon)

    st.sidebar.markdown('<p class="hl7-sidebar-cat hl7-sidebar-cat-platform">Platform</p>', unsafe_allow_html=True)
    for path, label, icon in _PLATFORM:
        _sidebar_page_link(path, label, icon)

    st.sidebar.markdown('<p class="hl7-sidebar-cat">Ask your data</p>', unsafe_allow_html=True)
    for path, label, icon in _GENIE:
        _sidebar_page_link(path, label, icon)

    st.sidebar.markdown("---")
    sidebar_product_context()


def render_home_footer() -> None:
    """Minimal home footer: avoid duplicating the sidebar catalog."""
    st.markdown("---")
    st.caption("All dashboards and platform tools are in the **left sidebar**.")
    st.markdown(
        '<p class="hl7-powered-foot">HL7App · Databricks · Lakebase</p>',
        unsafe_allow_html=True,
    )
