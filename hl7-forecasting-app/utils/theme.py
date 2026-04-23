"""
Shared Streamlit styling — call apply_theme() immediately after st.set_page_config on every page.

Type and color tokens follow the Databricks AppKit / brand palette used in
https://github.com/mkgs-databricks-demos/dbxWearables (e.g. zeroBus/dbxW_zerobus_app/src/app/client/src/index.css).
Streamlit cannot load AppKit UI, but this CSS approximates the same look (DM Sans, Lava primary, Oat surfaces, Navy text).
"""

import streamlit as st

PROFESSIONAL_CSS = """
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:ital@0;1&family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap');

:root {
  --dbx-lava-600: #FF3621;
  --dbx-lava-500: #FF5F46;
  --dbx-navy-800: #1B3139;
  --dbx-navy-900: #0B2026;
  --dbx-oat-light: #F9F7F4;
  --dbx-oat-medium: #EEEDE9;
  --dbx-gray-text: #5A6F77;
  --dbx-gray-lines: #DCE0E2;
  --dbx-green-600: #00A972;
  --dbx-blue-600: #2272B4;
  --dbx-radius: 0.625rem;
}

html, body, .stApp, [data-testid="stAppViewContainer"] {
  font-family: "DM Sans", "Inter", system-ui, -apple-system, sans-serif !important;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  background: var(--dbx-oat-light) !important;
  color: var(--dbx-navy-800) !important;
}

/* Main content breathing room */
.block-container {
  padding-top: 1.25rem !important;
  padding-bottom: 2rem !important;
  max-width: 1400px !important;
}

h1, h2, h3 {
  color: var(--dbx-navy-800) !important;
  font-weight: 600 !important;
  letter-spacing: -0.02em !important;
  line-height: 1.2 !important;
}

/* Slightly tighter page titles than Streamlit default */
.block-container h1 {
  font-size: 1.5rem !important;
  line-height: 1.2 !important;
}
.block-container h2,
.block-container h3 {
  font-size: 1.15rem !important;
  line-height: 1.25 !important;
}

/* Metrics — card on brand surfaces (dbxWearables) */
div[data-testid="stMetric"] {
  background: #ffffff !important;
  border: 1px solid var(--dbx-gray-lines) !important;
  border-radius: var(--dbx-radius) !important;
  padding: 14px 18px !important;
  box-shadow: 0 1px 2px rgba(11, 32, 38, 0.06) !important;
}
div[data-testid="stMetric"] label {
  color: var(--dbx-gray-text) !important;
  font-size: 0.8rem !important;
  font-weight: 500 !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
  color: var(--dbx-navy-800) !important;
  font-weight: 700 !important;
  font-size: 1.05rem !important;
}

/* DLT live — compact update summary row (replaces oversized metrics on that page) */
.hl7-dlt-kpi-row {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px 14px;
  margin: 0 0 12px 0;
}
@media (max-width: 900px) {
  .hl7-dlt-kpi-row {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
.hl7-dlt-kpi-cell {
  background: #ffffff;
  border: 1px solid var(--dbx-gray-lines);
  border-radius: 10px;
  padding: 8px 12px;
  min-width: 0;
}
.hl7-dlt-kpi-label {
  display: block;
  font-size: 0.68rem !important;
  font-weight: 600 !important;
  color: var(--dbx-gray-text) !important;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  margin-bottom: 2px;
}
.hl7-dlt-kpi-value {
  display: block;
  font-size: 0.88rem !important;
  font-weight: 600 !important;
  color: var(--dbx-navy-800) !important;
  line-height: 1.35;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: help;
  max-width: 100%;
}
.hl7-dlt-kpi-value:hover {
  overflow: visible;
  white-space: normal;
  word-break: break-word;
  position: relative;
  z-index: 3;
  background: #ffffff;
  box-shadow: 0 4px 14px rgba(15, 23, 42, 0.1);
  border-radius: 6px;
  margin: -2px -4px;
  padding: 2px 4px;
}

/* Primary — Databricks Lava (dbxWearables) */
.stButton > button[kind="primary"] {
  background: linear-gradient(180deg, var(--dbx-lava-600) 0%, var(--dbx-lava-500) 100%) !important;
  border: none !important;
  font-weight: 600 !important;
  color: #fff !important;
  border-radius: var(--dbx-radius) !important;
}
.stButton > button[kind="primary"]:hover {
  filter: brightness(0.95);
}

/* Expanders */
.streamlit-expanderHeader {
  font-weight: 600 !important;
  color: #334155 !important;
}

/* Home — header (white on oat page; Lava left rule like dbx demo cards) */
.hl7-app-header {
  background: #ffffff;
  border: 1px solid var(--dbx-gray-lines);
  border-left: 4px solid var(--dbx-lava-600);
  border-radius: var(--dbx-radius);
  padding: 22px 24px 20px;
  margin: 0 0 1.25rem 0;
  box-shadow: 0 1px 3px rgba(11, 32, 38, 0.06);
}
.hl7-app-eyebrow {
  display: block;
  font-size: 0.65rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--dbx-gray-text) !important;
  margin: 0 0 8px 0 !important;
}
.hl7-app-header h1 {
  color: var(--dbx-navy-800) !important;
  font-size: 1.6rem !important;
  line-height: 1.2 !important;
  margin: 0 0 8px 0 !important;
  font-weight: 600 !important;
  letter-spacing: -0.03em !important;
  border: none !important;
}
.hl7-app-lead {
  margin: 0 !important;
  font-size: 0.95rem !important;
  line-height: 1.5;
  color: var(--dbx-gray-text) !important;
  max-width: 720px;
}
.hl7-app-header--tight h1 {
  font-size: 1.75rem !important;
  margin-bottom: 0.4rem !important;
}
/* Architecture card: Sankey + doc links (home) */
.hl7-arch-sankey-shell {
  margin: 0 0 0.5rem 0;
}
.hl7-doc-pills {
  background: linear-gradient(145deg, #f8fafc 0%, #f1f5f9 100%);
  border: 1px solid var(--dbx-gray-lines);
  border-radius: 12px;
  padding: 14px 16px 12px;
  min-height: 200px;
  animation: hl7-arch-breathe 6s ease-in-out infinite;
}
@keyframes hl7-arch-breathe {
  0%, 100% { box-shadow: 0 0 0 0 rgba(34, 114, 180, 0.04); }
  50% { box-shadow: 0 0 0 1px rgba(34, 114, 180, 0.08), 0 6px 20px rgba(15, 23, 42, 0.04); }
}
.hl7-doc-pills-title {
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--dbx-gray-text);
  margin: 0 0 10px 0;
}
.hl7-doc-pills-list {
  list-style: none;
  margin: 0;
  padding: 0;
}
.hl7-doc-pills-list li {
  font-size: 0.88rem;
  line-height: 1.45;
  margin: 0 0 10px 0;
  color: #334155;
  padding-left: 0;
}
.hl7-doc-pills-list a {
  color: var(--dbx-blue-600) !important;
  font-weight: 600;
  text-decoration: none;
  border-bottom: 1px solid rgba(34, 114, 180, 0.25);
}
.hl7-doc-pills-list a:hover {
  border-bottom-color: var(--dbx-blue-600);
}
.hl7-doc-pills-hint {
  font-size: 0.75rem;
  color: #94a3b8;
  margin: 12px 0 0 0;
  line-height: 1.4;
}
/* Home — system health (no st.metric) */
.hl7-app-header a {
  color: var(--dbx-blue-600) !important;
  font-weight: 600;
  text-decoration: none;
  border-bottom: 1px solid rgba(34, 114, 180, 0.3);
}
.hl7-app-header a:hover {
  border-bottom-color: var(--dbx-blue-600);
}
.hl7-sys-health {
  background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
  border: 1px solid var(--dbx-gray-lines);
  border-radius: 16px;
  padding: 0 0 8px 0;
  margin: 0;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.05);
}
.hl7-sys-health-head {
  padding: 18px 20px 12px;
  border-bottom: 1px solid #e2e8f0;
  background: rgba(255, 255, 255, 0.7);
  border-radius: 16px 16px 0 0;
}
.hl7-sys-health-title {
  margin: 0 0 6px 0 !important;
  font-size: 1.35rem !important;
  font-weight: 700 !important;
  color: var(--dbx-navy-800) !important;
  letter-spacing: -0.02em !important;
  border: none !important;
}
.hl7-sys-health-deck {
  margin: 0;
  font-size: 0.9rem;
  line-height: 1.5;
  color: var(--dbx-gray-text);
  max-width: 720px;
}
.hl7-sys-health-list {
  list-style: none;
  margin: 0;
  padding: 0 4px 0 0;
}
.hl7-sys-health-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 10px 16px;
  padding: 14px 20px;
  border-bottom: 1px solid #e8eef3;
  transition: background 0.15s ease;
}
.hl7-sys-health-row:hover {
  background: rgba(34, 114, 180, 0.03);
}
.hl7-sys-health-row--dlt {
  background: rgba(11, 79, 122, 0.03);
  border-bottom: none;
  border-radius: 0 0 12px 12px;
}
.hl7-sys-health-main {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  min-width: 0;
  flex: 1 1 200px;
}
.hl7-sys-health-label {
  font-size: 0.88rem;
  font-weight: 600;
  color: #0f172a;
  letter-spacing: -0.01em;
}
.hl7-sys-health-age {
  font-family: "DM Mono", ui-monospace, monospace;
  font-size: 0.9rem;
  color: #475569;
  font-weight: 500;
}
.hl7-hs-tier {
  font-size: 0.82rem;
  font-weight: 700;
  letter-spacing: 0.02em;
  white-space: nowrap;
  padding: 4px 10px;
  border-radius: 8px;
  background: #f1f5f9;
  border: 1px solid #e2e8f0;
}
.hl7-hs-tier--ok {
  color: #0f766e;
  background: #ecfdf5;
  border-color: #6ee7b7;
}
.hl7-hs-tier--stale {
  color: #9a3412;
  background: #fffbeb;
  border-color: #fcd34d;
}
.hl7-hs-tier--critical {
  color: #991b1b;
  background: #fef2f2;
  border-color: #fca5a5;
}
.hl7-hs-tier--na {
  color: #475569;
  background: #f8fafc;
  border-color: #e2e8f0;
}
.hl7-sys-dlt-pill {
  display: inline-block;
  font-size: 0.8rem;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  padding: 2px 8px;
  background: #fff;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  margin-top: 4px;
  color: #0f172a;
}
.hl7-sys-dlt-hint {
  margin: 0;
  padding: 10px 20px 4px;
  font-size: 0.75rem;
  line-height: 1.4;
  color: #94a3b8;
  word-break: break-word;
}
.hl7-sys-dlt-full {
  display: block;
  font-size: 0.72rem;
  color: #64748b;
  line-height: 1.4;
  font-weight: 400;
  background: transparent;
  border: none;
  padding: 0;
  white-space: pre-wrap;
}
/* Home — more shortcuts (inside st.container) */
.hl7-pro-quickstart-title {
  margin: 0 0 2px 0 !important;
  font-size: 0.9rem !important;
  font-weight: 600 !important;
  color: var(--dbx-navy-800) !important;
  border: none !important;
  letter-spacing: -0.02em !important;
}
.hl7-pro-quickstart-hint {
  margin: 0 0 12px 0 !important;
  font-size: 0.8rem !important;
  color: var(--dbx-gray-text) !important;
  line-height: 1.45;
}

/* Home — reference architecture (blueprint on oat; dbxWearables diagram tone) */
.hl7-architect-panel {
  background-color: var(--dbx-oat-medium);
  background-image:
    linear-gradient(rgba(90, 111, 119, 0.1) 1px, transparent 1px),
    linear-gradient(90deg, rgba(90, 111, 119, 0.1) 1px, transparent 1px);
  background-size: 20px 20px;
  background-position: -1px -1px;
  border: 1px solid var(--dbx-gray-lines);
  border-radius: var(--dbx-radius);
  padding: 22px 20px 16px;
  margin: 0 0 1.35rem 0;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
}
.hl7-architect-foot {
  margin: 16px 0 0 0 !important;
  font-size: 0.8rem !important;
  line-height: 1.5 !important;
  color: var(--dbx-gray-text) !important;
  padding-top: 12px;
  border-top: 1px dashed var(--dbx-gray-lines);
  max-width: 900px;
}
.hl7-arch-badge {
  display: inline-block;
  font-family: "DM Mono", ui-monospace, monospace;
  font-size: 0.6rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  color: #00875c;
  background: #e6faf4;
  border: 1px solid #9ed6c4;
  border-radius: 4px;
  padding: 2px 6px;
  margin: 0 0 6px 0;
}
.hl7-arch-step--app {
  border-color: var(--dbx-blue-600) !important;
  background: #f0f7fc !important;
  box-shadow: 0 0 0 1px rgba(34, 114, 180, 0.2);
}

/* Home — data flow card (pipeline strip) — also used for titles inside architect panel */
.hl7-dataflow-wrap {
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 18px;
  padding: 20px 22px 18px;
  margin: 0 0 1.5rem 0;
  box-shadow: 0 2px 14px rgba(15, 23, 42, 0.05);
}
.hl7-home-eyebrow {
  display: block;
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--dbx-gray-text) !important;
  margin: 0 0 4px 0 !important;
}
.hl7-dataflow-title {
  margin: 0 0 6px 0 !important;
  font-size: 1.2rem !important;
  font-weight: 700 !important;
  color: var(--dbx-navy-800) !important;
  letter-spacing: -0.02em !important;
  border: none !important;
}
.hl7-dataflow-deck {
  font-size: 0.9rem;
  line-height: 1.5;
  color: var(--dbx-gray-text);
  margin: 0 0 1rem 0;
}

/* Home — live / charts / observability / Lakebase section headers (injected in app.py) */
.hl7-live-head,
.hl7-charts-head,
.hl7-observability-top,
.hl7-lakebase-head {
  margin: 0 0 8px 0;
  padding: 0 2px 10px 0;
  border-bottom: 1px solid #e2e8f0;
}
.hl7-live-head h2,
.hl7-charts-head h2,
.hl7-observability-top h2,
.hl7-lakebase-head h2,
h2.hl7-section-h2 {
  margin: 0 0 4px 0 !important;
  font-size: 1.28rem !important;
  font-weight: 700 !important;
  color: var(--dbx-navy-800) !important;
  letter-spacing: -0.02em !important;
  border: none !important;
  line-height: 1.25 !important;
}
h3.hl7-section-h3 {
  margin: 0 0 4px 0 !important;
  font-size: 1.12rem !important;
  font-weight: 600 !important;
  color: var(--dbx-navy-800) !important;
  letter-spacing: -0.02em !important;
  border: none !important;
  line-height: 1.3 !important;
}
.hl7-charts-head h2 { margin-top: 4px !important; }
.hl7-ops-snapshot-deck {
  font-size: 0.9rem;
  line-height: 1.5;
  color: var(--dbx-gray-text);
  margin: 0;
  max-width: 820px;
}
.hl7-live-deck, .hl7-charts-deck {
  font-size: 0.9rem;
  line-height: 1.5;
  color: var(--dbx-gray-text);
  margin: 0;
  max-width: 820px;
}

/* Lakebase connection strip on home */
.hl7-conn-surface {
  background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
  border: 1px solid #e2e8f0;
  border-radius: 16px;
  padding: 4px 8px 12px 8px;
  margin-top: 8px;
}
.hl7-powered-foot {
  text-align: center;
  font-size: 0.8rem;
  color: #94a3b8;
  margin-top: 0.5rem;
}
.hl7-home-demo-row { margin-bottom: 1rem; }

.hl7-arch {
  display: flex;
  flex-wrap: wrap;
  align-items: stretch;
  justify-content: center;
  gap: 8px 6px;
  margin: 14px 0 0 0;
}
.hl7-arch-step {
  flex: 1 1 108px;
  min-width: 96px;
  max-width: 160px;
  background: #ffffff;
  border: 1px solid var(--dbx-gray-lines);
  border-top: 3px solid var(--dbx-green-600);
  border-radius: 6px;
  padding: 10px 10px 12px;
  text-align: left;
  box-shadow: 0 1px 2px rgba(11, 32, 38, 0.07);
}
.hl7-arch-step strong {
  display: block;
  color: var(--dbx-navy-800);
  font-size: 0.78rem;
  font-weight: 600;
  line-height: 1.3;
  margin-bottom: 4px;
  letter-spacing: -0.02em;
}
.hl7-arch-step > span:not(.hl7-arch-badge) {
  font-size: 0.68rem;
  color: var(--dbx-gray-text);
  line-height: 1.35;
  display: block;
}
.hl7-arch-arrow {
  align-self: center;
  color: var(--dbx-gray-text);
  font-weight: 600;
  font-size: 1rem;
  padding: 0 1px;
  user-select: none;
}
@media (max-width: 1100px) {
  .hl7-arch { gap: 10px; }
  .hl7-arch-arrow { display: none; }
  .hl7-arch-step { max-width: none; flex: 1 1 45%; }
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
  background: #ffffff;
  border: 1px solid var(--dbx-gray-lines);
  border-radius: var(--dbx-radius);
  padding: 18px 20px;
  font-size: 0.88rem;
  line-height: 1.6;
  color: var(--dbx-gray-text);
  font-family: "DM Sans", system-ui, sans-serif;
}
.conn-box code, .conn-box b {
  color: var(--dbx-navy-800);
  font-family: "DM Mono", ui-monospace, monospace;
  font-size: 0.82em;
}

/* Sidebar — white on oat (dbx app shell) */
[data-testid="stSidebar"] {
  border-right: 1px solid var(--dbx-gray-lines) !important;
  background: #ffffff !important;
}
[data-testid="stSidebar"] .stMarkdown {
  font-size: 0.9rem;
  color: var(--dbx-navy-800) !important;
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
  color: var(--dbx-navy-800);
}
.hl7-sidebar-meta {
  font-size: 0.78rem !important;
  color: var(--dbx-gray-text) !important;
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
  box-shadow: 0 4px 14px rgba(255, 95, 70, 0.2);
  border-color: var(--dbx-lava-500) !important;
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
  background: rgba(255, 54, 33, 0.08) !important;
}

.stButton > button[kind="secondary"] {
  border-radius: var(--dbx-radius) !important;
  font-weight: 500 !important;
  border: 1px solid var(--dbx-gray-lines) !important;
  transition: background 0.15s ease, border-color 0.15s ease !important;
  color: var(--dbx-navy-800) !important;
}
.stButton > button[kind="secondary"]:hover {
  border-color: var(--dbx-gray-text) !important;
  background: #ffffff !important;
}

[data-testid="stVerticalBlock"] > div:has([data-testid="stPlotlyChart"]) {
  border-radius: 12px;
  border: 1px solid var(--dbx-gray-lines);
  padding: 8px 8px 4px 8px;
  background: #ffffff;
  box-shadow: 0 2px 10px rgba(11, 32, 38, 0.05);
}

textarea[data-testid="stChatInputTextArea"],
div[data-testid="stChatInput"] {
  border-radius: 14px !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
  box-shadow: 0 0 0 2px rgba(255, 54, 33, 0.3) !important;
}

div[data-testid="stSegmentedControl"] {
  background: var(--dbx-oat-medium) !important;
  border-radius: var(--dbx-radius) !important;
  padding: 4px !important;
  border: 1px solid var(--dbx-gray-lines) !important;
}

.hl7-panel-eyebrow {
  font-size: 0.7rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.12em !important;
  text-transform: uppercase !important;
  color: var(--dbx-lava-600) !important;
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

/* Bordered panels (e.g. Quick start, connection card) */
[data-testid="stAppViewContainer"] [data-testid="stVerticalBlockBorderWrapper"] {
  border-radius: 12px !important;
  background: #ffffff !important;
  border: 1px solid var(--dbx-gray-lines) !important;
  box-shadow: 0 1px 8px rgba(11, 32, 38, 0.05) !important;
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
