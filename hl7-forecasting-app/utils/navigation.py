"""
Categorized sidebar and home navigation — clinical vs platform vs Genie.
"""

from __future__ import annotations

import streamlit as st

from utils.theme import sidebar_product_context

# (path, label, icon) — order within each section is display order.
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

    st.sidebar.markdown('<p class="hl7-sidebar-cat">Clinical intelligence</p>', unsafe_allow_html=True)
    for path, label, icon in _CLINICAL:
        st.sidebar.page_link(path, label=label, icon=icon)

    st.sidebar.markdown('<p class="hl7-sidebar-cat hl7-sidebar-cat-platform">Platform</p>', unsafe_allow_html=True)
    for path, label, icon in _PLATFORM:
        st.sidebar.page_link(path, label=label, icon=icon)

    st.sidebar.markdown('<p class="hl7-sidebar-cat">Ask your data</p>', unsafe_allow_html=True)
    for path, label, icon in _GENIE:
        st.sidebar.page_link(path, label=label, icon=icon)

    st.sidebar.markdown("---")
    sidebar_product_context()


def render_home_navigation() -> None:
    """Bento-style home: clinical grid + platform stack + Genie spotlight."""
    st.markdown(
        """
<div class="hl7-nav-intro">
  <h2>Where to next</h2>
  <p>Clinical views read from <strong>Lakebase</strong>. Platform pages control <strong>DLT, jobs, and the Databricks stack</strong>.</p>
</div>
        """,
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.15, 1], gap="large")

    with left:
        st.markdown(
            """
<div class="hl7-clinical-banner">
  <div class="hl7-banner-inner">
    <span class="hl7-banner-ico">🏥</span>
    <div>
      <h3>Clinical intelligence</h3>
      <p>Census, trends, ML forecasts, and patient analytics — all from Lakebase gold.</p>
    </div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
        r1 = st.columns(4)
        _home_card(
            r1[0],
            "01",
            "Real-time ops",
            "Live ED/ICU census and hourly flow",
            "pages/1_realtime.py",
            "📊",
            "green",
        )
        _home_card(
            r1[1],
            "02",
            "Trends",
            "Daily rollups, heatmaps, ED vs ICU",
            "pages/2_trends.py",
            "📈",
            "blue",
        )
        _home_card(
            r1[2],
            "03",
            "ML forecasting",
            "Horizons, bands, predicted vs actual",
            "pages/3_forecasting.py",
            "🔮",
            "purple",
        )
        _home_card(
            r1[3],
            "04",
            "Model performance",
            "MAE, MAPE, coverage, comparisons",
            "pages/4_model_perf.py",
            "🎯",
            "orange",
        )
        r2 = st.columns(4)
        _home_card(
            r2[0],
            "05",
            "Patient & clinical",
            "Demographics, dx, labs, orders",
            "pages/5_patient_clinical.py",
            "🧬",
            "teal",
        )
        _home_card(
            r2[1],
            "06",
            "Combined forecast",
            "ED+ICU pressure and ratios",
            "pages/6_combined_forecast.py",
            "⚖️",
            "red",
        )
        _home_card(
            r2[2],
            "07",
            "HL7 operations",
            "Throughput, message types, facilities",
            "pages/7_operations.py",
            "📨",
            "indigo",
        )
        with r2[3]:
            st.markdown(
                """
<div class="hl7-nav-more-hint">
  <span class="hl7-nav-more-arrow">→</span>
  <p><strong>Platform &amp; Genie</strong> live in the right column — DLT, jobs, stack pulse, and NL queries.</p>
</div>
                """,
                unsafe_allow_html=True,
            )

    with right:
        st.markdown(
            """
<div class="hl7-platform-banner">
  <div class="hl7-banner-inner hl7-banner-inner--dark">
    <span class="hl7-banner-ico">⚙️</span>
    <div>
      <h3>Platform</h3>
      <p>Pipelines, jobs, and workspace health — kept apart from clinical dashboards on purpose.</p>
    </div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
        for path, label, icon in _PLATFORM:
            st.page_link(path, label=label, icon=icon)

        st.markdown(
            """
<div class="hl7-genie-spotlight">
  <div class="hl7-genie-row">
    <span class="hl7-genie-emoji">✨</span>
    <div>
      <strong>Genie</strong>
      <p>Natural language on your Unity Catalog space</p>
    </div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
        st.page_link("pages/8_genie_chat.py", label="Open Genie chat", icon="💬")


def _home_card(col, num: str, title: str, desc: str, path: str, icon: str, accent: str) -> None:
    with col:
        st.markdown(
            f"""
<div class="hl7-nav-card hl7-nav-card-v2 {accent}">
  <span class="hl7-nav-num">{num}</span>
  <h4>{icon} {title}</h4>
  <p>{desc}</p>
</div>
            """,
            unsafe_allow_html=True,
        )
        st.page_link(path, label=f"Open · {title}", icon="➜")
