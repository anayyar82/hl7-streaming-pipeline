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
  background: linear-gradient(125deg, #0f172a 0%, #1e3a5f 40%, #312e81 72%, #0c4a6e 100%);
  background-size: 180% 180%;
  animation: hl7-hero-flow 20s ease-in-out infinite;
  color: #f8fafc;
  border-radius: 16px;
  padding: 28px 32px;
  margin-bottom: 1.5rem;
  box-shadow: 0 8px 32px rgba(15, 23, 42, 0.2), 0 0 0 1px rgba(255,255,255,0.06) inset;
}
@keyframes hl7-hero-flow {
  0%, 100% { background-position: 0% 40%; }
  50% { background-position: 100% 60%; }
}
@media (prefers-reduced-motion: reduce) {
  .hl7-hero { animation: none; background-size: 100% 100%; }
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
  background: linear-gradient(180deg, #fafbfc 0%, #f1f5f9 100%) !important;
}
[data-testid="stSidebar"] .stMarkdown {
  font-size: 0.9rem;
}

/* Categorized sidebar */
.hl7-sidebar-brand {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 6px;
}
.hl7-sidebar-brand-mark {
  font-size: 1.75rem;
  line-height: 1;
}
.hl7-sidebar-brand-text {
  font-size: 1.35rem;
  font-weight: 700;
  letter-spacing: -0.03em;
  color: #0f172a;
}
.hl7-sidebar-meta {
  font-size: 0.78rem !important;
  color: #64748b !important;
  margin: 0 0 4px 0 !important;
  line-height: 1.45 !important;
}
.hl7-sidebar-meta code {
  font-size: 0.72rem !important;
  background: #e2e8f0;
  padding: 1px 5px;
  border-radius: 4px;
}
.hl7-sidebar-cat {
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.14em !important;
  text-transform: uppercase !important;
  color: #94a3b8 !important;
  margin: 14px 0 6px 0 !important;
  padding-bottom: 4px;
  border-bottom: 1px solid #e2e8f0;
}
.hl7-sidebar-cat-platform {
  color: #6366f1 !important;
  border-bottom-color: #c7d2fe;
}

/* Home navigation */
.hl7-nav-intro {
  margin: 0 0 1.5rem 0;
  padding: 0 4px;
}
.hl7-nav-intro h2 {
  margin: 0 0 6px 0 !important;
  font-size: 1.5rem !important;
  background: linear-gradient(90deg, #0f172a, #4338ca);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
.hl7-nav-intro p {
  margin: 0 !important;
  color: #475569;
  font-size: 1rem;
  line-height: 1.5;
}

.hl7-clinical-banner, .hl7-platform-banner {
  border-radius: 18px;
  margin-bottom: 16px;
  overflow: hidden;
}
.hl7-clinical-banner {
  background: linear-gradient(135deg, #eff6ff 0%, #ecfdf5 50%, #f0f9ff 100%);
  border: 1px solid #bfdbfe;
  box-shadow: 0 4px 24px rgba(37, 99, 235, 0.08);
}
.hl7-platform-banner {
  background: linear-gradient(145deg, #1e1b4b 0%, #312e81 40%, #0f172a 100%);
  border: 1px solid #4338ca;
  box-shadow: 0 8px 32px rgba(49, 46, 129, 0.35);
}
.hl7-banner-inner {
  display: flex;
  gap: 14px;
  align-items: flex-start;
  padding: 18px 20px;
}
.hl7-banner-inner--dark h3 {
  color: #f8fafc !important;
  margin: 0 0 4px 0 !important;
  font-size: 1.15rem !important;
  border: none !important;
}
.hl7-banner-inner--dark p {
  margin: 0 !important;
  color: rgba(248, 250, 252, 0.85) !important;
  font-size: 0.88rem !important;
  line-height: 1.45 !important;
}
.hl7-banner-inner:not(.hl7-banner-inner--dark) h3 {
  margin: 0 0 4px 0 !important;
  font-size: 1.15rem !important;
  color: #0f172a !important;
  border: none !important;
}
.hl7-banner-inner:not(.hl7-banner-inner--dark) p {
  margin: 0 !important;
  color: #475569 !important;
  font-size: 0.88rem !important;
}
.hl7-banner-ico {
  font-size: 1.75rem;
  line-height: 1;
  flex-shrink: 0;
}

.hl7-nav-card-v2 {
  position: relative;
  min-height: 118px;
  padding-top: 22px !important;
}
.hl7-nav-num {
  position: absolute;
  top: 10px;
  right: 12px;
  font-size: 0.65rem;
  font-weight: 800;
  letter-spacing: 0.06em;
  color: #94a3b8;
  background: #f1f5f9;
  border-radius: 6px;
  padding: 2px 7px;
}
.hl7-nav-more-hint {
  background: linear-gradient(145deg, #faf5ff 0%, #f5f3ff 100%);
  border: 1px dashed #c4b5fd;
  border-radius: 14px;
  padding: 16px 14px;
  min-height: 118px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 8px;
}
.hl7-nav-more-arrow {
  font-size: 1.5rem;
  color: #7c3aed;
  font-weight: 700;
}
.hl7-nav-more-hint p {
  margin: 0 !important;
  font-size: 0.82rem !important;
  color: #5b21b6 !important;
  line-height: 1.4 !important;
}

.hl7-genie-spotlight {
  margin-top: 20px;
  padding: 18px 18px;
  border-radius: 16px;
  background: linear-gradient(120deg, #fef3c7 0%, #fde68a 35%, #fcd34d 100%);
  border: 1px solid #f59e0b;
  box-shadow: 0 6px 20px rgba(245, 158, 11, 0.2);
}
.hl7-genie-row {
  display: flex;
  gap: 12px;
  align-items: center;
}
.hl7-genie-emoji {
  font-size: 2rem;
  line-height: 1;
}
.hl7-genie-spotlight strong {
  display: block;
  color: #78350f;
  font-size: 1rem;
  margin-bottom: 2px;
}
.hl7-genie-spotlight p {
  margin: 0 !important;
  font-size: 0.85rem !important;
  color: #92400e !important;
}

/* ---- Interactive polish ---- */
div[data-testid="stMetric"] {
  transition: transform 0.18s ease, box-shadow 0.18s ease;
}
div[data-testid="stMetric"]:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 16px rgba(15, 23, 42, 0.08) !important;
}

.hl7-arch-step {
  transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
}
.hl7-arch-step:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 14px rgba(37, 99, 235, 0.12);
  border-color: #93c5fd;
}

.hl7-nav-card-v2 {
  transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease !important;
}
.hl7-nav-card-v2:hover {
  transform: translateY(-3px);
  box-shadow: 0 10px 28px rgba(15, 23, 42, 0.1) !important;
}

[data-testid="stSidebar"] a[data-testid="stPageLink-NavLink"] {
  border-radius: 10px !important;
  transition: background 0.15s ease, padding-left 0.15s ease;
}
[data-testid="stSidebar"] a[data-testid="stPageLink-NavLink"]:hover {
  background: rgba(99, 102, 241, 0.08) !important;
}

.stButton > button[kind="secondary"] {
  border-radius: 10px !important;
  font-weight: 500 !important;
  border: 1px solid #e2e8f0 !important;
  transition: background 0.15s ease, border-color 0.15s ease !important;
}
.stButton > button[kind="secondary"]:hover {
  border-color: #cbd5e1 !important;
  background: #f8fafc !important;
}

[data-testid="stVerticalBlock"] > div:has([data-testid="stPlotlyChart"]) {
  border-radius: 14px;
  border: 1px solid #e2e8f0;
  padding: 8px 8px 4px 8px;
  background: linear-gradient(180deg, #ffffff 0%, #fafbfc 100%);
  box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
}

textarea[data-testid="stChatInputTextArea"],
div[data-testid="stChatInput"] {
  border-radius: 14px !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
  box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.25) !important;
}

div[data-testid="stSegmentedControl"] {
  background: #f1f5f9 !important;
  border-radius: 12px !important;
  padding: 4px !important;
}

.hl7-panel-eyebrow {
  font-size: 0.7rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.12em !important;
  text-transform: uppercase !important;
  color: #6366f1 !important;
  margin: 0 0 4px 0 !important;
}

@media (prefers-reduced-motion: reduce) {
  div[data-testid="stMetric"],
  .hl7-arch-step,
  .hl7-nav-card-v2 {
    transition: none !important;
  }
  div[data-testid="stMetric"]:hover,
  .hl7-arch-step:hover,
  .hl7-nav-card-v2:hover {
    transform: none !important;
  }
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
