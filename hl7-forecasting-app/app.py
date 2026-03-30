"""
HL7App - ED & ICU Operations Dashboard

Real-time operations dashboard and ML forecasting for Emergency Department
and ICU arrivals/discharges, powered by Lakebase Postgres.
"""

import streamlit as st
from utils.db import run_query

st.set_page_config(
    page_title="HL7App - ED & ICU Operations",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 1.5rem; }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #667eea11, #764ba211);
        border: 1px solid #e0e0e0;
        border-radius: 10px;
        padding: 12px 16px;
    }
    .card {
        background: linear-gradient(135deg, #f5f7fa, #c3cfe2);
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 12px;
        border-left: 4px solid;
        min-height: 140px;
    }
    .card h4 { margin: 0 0 8px 0; }
    .card p { margin: 0; font-size: 0.9em; color: #444; }
    .card-green { border-left-color: #4CAF50; }
    .card-blue { border-left-color: #2196F3; }
    .card-purple { border-left-color: #9C27B0; }
    .card-orange { border-left-color: #FF9800; }
    .card-teal { border-left-color: #009688; }
    .card-red { border-left-color: #F44336; }
    .card-indigo { border-left-color: #3F51B5; }
    .card-genie { border-left-color: #6a1b9a; }
    .conn-box {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 16px;
        font-size: 0.85em;
    }
    .conn-box code { color: #0d6efd; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.sidebar.title("HL7App")
st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    **Data Source:** Lakebase Postgres  
    **Project:** `ankurhlsproject`  
    **Schema:** `ankur_nayyar`
    """
)
st.sidebar.markdown("---")
st.sidebar.page_link(
    "pages/8_genie_chat.py",
    label="Ask your data (Genie)",
    icon="💬",
)

st.title("HL7 ED & ICU Operations Dashboard")
st.markdown(
    "Use the sidebar (or the link below) for **8. Ask your data (Genie)** — natural-language "
    "questions over your Genie space — plus real-time operations, trends, forecasts, "
    "clinical analytics, and pipeline health."
)

# ---- Lakebase Connection & Data Summary ----
st.markdown("### Lakebase Connection")

TABLE_COUNT_QUERY = """
SELECT
    COUNT(*) AS table_count,
    string_agg(tablename, ', ' ORDER BY tablename) AS tables
FROM pg_tables
WHERE schemaname = 'ankur_nayyar'
"""

try:
    info_df = run_query(TABLE_COUNT_QUERY)
    if not info_df.empty:
        tbl_count = int(info_df["table_count"].iloc[0])
        tbl_list = info_df["tables"].iloc[0] or ""

        col_conn, col_stats = st.columns([1, 1])

        with col_conn:
            st.markdown(
                '<div class="conn-box">'
                "<b>Endpoint:</b> <code>ep-wandering-meadow-d1tdkigo</code><br>"
                "<b>Database:</b> <code>databricks_postgres</code><br>"
                "<b>Schema:</b> <code>ankur_nayyar</code><br>"
                "<b>Region:</b> <code>us-west-2</code><br>"
                f"<b>Tables:</b> <code>{tbl_count}</code>"
                "</div>",
                unsafe_allow_html=True,
            )

        with col_stats:
            from utils.db import PGHOST, PGDATABASE, ENDPOINT_NAME
            st.metric("Gold Tables", tbl_count)
            if tbl_list:
                with st.expander("View Table Names"):
                    for t in sorted(tbl_list.split(", ")):
                        st.code(t, language=None)
    else:
        st.info("Connected to Lakebase but no tables found in schema.")
except Exception as e:
    st.warning(f"Could not query Lakebase metadata: {e}")

st.markdown("---")

st.markdown("### Dashboard Pages")

st.page_link(
    "pages/8_genie_chat.py",
    label="8. Ask your data (Genie) — AI/BI natural language",
    icon="💬",
)

row1_c1, row1_c2, row1_c3, row1_c4 = st.columns(4)

with row1_c1:
    st.markdown(
        '<div class="card card-green">'
        "<h4>1. Real-Time Ops</h4>"
        "<p>Live ED &amp; ICU census, hourly arrivals &amp; discharges, "
        "department-level status.<br><b>Filters:</b> facility, department, "
        "time window, weekday/weekend</p>"
        "</div>",
        unsafe_allow_html=True,
    )

with row1_c2:
    st.markdown(
        '<div class="card card-blue">'
        "<h4>2. Trends</h4>"
        "<p>Daily summaries, hour-of-day heatmaps, ED vs ICU comparison, "
        "LOS analytics.<br><b>Filters:</b> date range, facility, weekday/weekend</p>"
        "</div>",
        unsafe_allow_html=True,
    )

with row1_c3:
    st.markdown(
        '<div class="card card-purple">'
        "<h4>3. ML Forecasting</h4>"
        "<p>Predicted vs actual, confidence intervals, forecast horizons, "
        "model outputs.<br><b>Filters:</b> department, metric, horizon</p>"
        "</div>",
        unsafe_allow_html=True,
    )

with row1_c4:
    st.markdown(
        '<div class="card card-orange">'
        "<h4>4. Model Performance</h4>"
        "<p>Accuracy metrics, MAE/MAPE trends, model comparison, "
        "coverage analysis.<br><b>Filters:</b> model selection</p>"
        "</div>",
        unsafe_allow_html=True,
    )

row2_c1, row2_c2, row2_c3, row2_c4 = st.columns(4)

with row2_c1:
    st.markdown(
        '<div class="card card-teal">'
        "<h4>5. Patient &amp; Clinical</h4>"
        "<p>Demographics, top diagnoses, lab results, allergies, "
        "orders &amp; provider activity.<br><b>Filters:</b> coding system, severity, "
        "priority, provider search</p>"
        "</div>",
        unsafe_allow_html=True,
    )

with row2_c2:
    st.markdown(
        '<div class="card card-red">'
        "<h4>6. Combined Forecast</h4>"
        "<p>ED + ICU system pressure, cross-department analysis, "
        "rolling averages, feature heatmaps.<br><b>Filters:</b> date range, "
        "weekday/weekend</p>"
        "</div>",
        unsafe_allow_html=True,
    )

with row2_c3:
    st.markdown(
        '<div class="card card-indigo">'
        "<h4>7. Operations</h4>"
        "<p>Message throughput, pipeline health, data freshness, "
        "patient activity by class.<br><b>Filters:</b> date range, message type, "
        "facility, patient class</p>"
        "</div>",
        unsafe_allow_html=True,
    )

with row2_c4:
    st.markdown(
        '<div class="card card-genie">'
        "<h4>8. Ask your data (Genie)</h4>"
        "<p>Natural-language Q&amp;A via Databricks AI/BI Genie over tables "
        "in your Genie space.<br><b>Requires:</b> <code>GENIE_SPACE_ID</code> "
        "and UC/warehouse grants.</p>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.page_link(
        "pages/8_genie_chat.py",
        label="Open Genie chat",
        icon="💬",
    )

st.markdown("---")
st.caption("Powered by Databricks Unity Catalog + DLT + MLflow AutoML + Lakebase")
