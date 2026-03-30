"""
Shared Streamlit styling — call apply_theme() immediately after st.set_page_config on every page.
"""

import streamlit as st

PROFESSIONAL_CSS = """
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:ital,wght@0,400;0,500;0,600;0,700;1,400&display=swap');

html, body, .stApp, [data-testid="stAppViewContainer"] {
  font-family: 'IBM Plex Sans', -apple-system, BlinkMacSystemFont, sans-serif !important;
}

/* Main content breathing room */
.block-container {
  padding-top: 1.25rem !important;
  padding-bottom: 2rem !important;
  max-width: 1400px !important;
}

h1, h2, h3 {
  color: #0f172a !important;
  font-weight: 600 !important;
  letter-spacing: -0.02em !important;
}

/* Metrics — card-like */
div[data-testid="stMetric"] {
  background: linear-gradient(145deg, #ffffff 0%, #f8fafc 100%);
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  padding: 14px 18px;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}
div[data-testid="stMetric"] label {
  color: #64748b !important;
  font-size: 0.8rem !important;
  font-weight: 500 !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
  color: #0f172a !important;
  font-weight: 700 !important;
}

/* Primary buttons — Databricks-adjacent accent */
.stButton > button[kind="primary"] {
  background: linear-gradient(180deg, #ff4c00 0%, #e04200 100%) !important;
  border: none !important;
  font-weight: 600 !important;
}

/* Expanders */
.streamlit-expanderHeader {
  font-weight: 600 !important;
  color: #334155 !important;
}

/* Home & nav cards */
.hl7-hero {
  background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 55%, #0c4a6e 100%);
  color: #f8fafc;
  border-radius: 16px;
  padding: 28px 32px;
  margin-bottom: 1.5rem;
  box-shadow: 0 8px 32px rgba(15, 23, 42, 0.15);
}
.hl7-hero h1 {
  color: #f8fafc !important;
  font-size: 1.85rem !important;
  margin: 0 0 8px 0 !important;
  border: none !important;
}
.hl7-hero p {
  margin: 0;
  opacity: 0.92;
  font-size: 1.05rem;
  line-height: 1.5;
  max-width: 820px;
}
.hl7-hero .hl7-badge-row {
  margin-top: 16px;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.hl7-badge {
  display: inline-block;
  background: rgba(255,255,255,0.12);
  border: 1px solid rgba(255,255,255,0.2);
  border-radius: 999px;
  padding: 4px 12px;
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.02em;
}

.hl7-arch {
  display: flex;
  flex-wrap: wrap;
  align-items: stretch;
  gap: 10px;
  margin: 1rem 0 1.5rem 0;
}
.hl7-arch-step {
  flex: 1 1 120px;
  min-width: 100px;
  background: #f1f5f9;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  padding: 12px 14px;
  text-align: center;
}
.hl7-arch-step strong {
  display: block;
  color: #0f172a;
  font-size: 0.8rem;
  margin-bottom: 4px;
}
.hl7-arch-step span {
  font-size: 0.72rem;
  color: #64748b;
  line-height: 1.35;
}
.hl7-arch-arrow {
  align-self: center;
  color: #94a3b8;
  font-weight: 700;
  font-size: 1.1rem;
}

.hl7-nav-card {
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 14px;
  padding: 18px 18px 16px 18px;
  margin-bottom: 12px;
  min-height: 130px;
  box-shadow: 0 2px 8px rgba(15, 23, 42, 0.04);
  transition: box-shadow 0.15s ease, border-color 0.15s ease;
  border-left: 4px solid var(--hl7-accent, #0ea5e9);
}
.hl7-nav-card:hover {
  box-shadow: 0 6px 20px rgba(15, 23, 42, 0.08);
  border-color: #cbd5e1;
}
.hl7-nav-card h4 {
  margin: 0 0 8px 0 !important;
  font-size: 1rem !important;
  color: #0f172a !important;
}
.hl7-nav-card p {
  margin: 0 !important;
  font-size: 0.88rem !important;
  color: #475569 !important;
  line-height: 1.45 !important;
}

.hl7-nav-card.green { --hl7-accent: #059669; }
.hl7-nav-card.blue { --hl7-accent: #2563eb; }
.hl7-nav-card.purple { --hl7-accent: #7c3aed; }
.hl7-nav-card.orange { --hl7-accent: #ea580c; }
.hl7-nav-card.teal { --hl7-accent: #0d9488; }
.hl7-nav-card.red { --hl7-accent: #dc2626; }
.hl7-nav-card.indigo { --hl7-accent: #4f46e5; }
.hl7-nav-card.genie { --hl7-accent: #6b21a8; }

.conn-box {
  background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  padding: 18px 20px;
  font-size: 0.88rem;
  line-height: 1.65;
  color: #334155;
}
.conn-box code, .conn-box b {
  color: #0f172a;
}

/* Sidebar polish */
[data-testid="stSidebar"] {
  border-right: 1px solid #e2e8f0 !important;
}
[data-testid="stSidebar"] .stMarkdown {
  font-size: 0.9rem;
}
"""


def apply_theme() -> None:
    st.markdown(f"<style>{PROFESSIONAL_CSS}</style>", unsafe_allow_html=True)


def sidebar_product_context() -> None:
    """Optional: concise product context under navigation."""
    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Medallion pipeline on **Databricks**: ingest → **DLT** gold → **Lakebase** → this app. "
        "**AutoML** + jobs for forecasts; **Genie** for NL queries over UC."
    )
