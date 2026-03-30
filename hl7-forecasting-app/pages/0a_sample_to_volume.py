"""
Generate HL7 sample files into the Unity Catalog landing volume (Workflows job).
"""

from __future__ import annotations

import os

import streamlit as st

from utils.theme import apply_theme
from utils.databricks_trigger import parse_job_id, trigger_job

st.set_page_config(page_title="Sample Data → Volume", page_icon="📤", layout="wide")
apply_theme()

JOB_SAMPLE = parse_job_id(os.environ.get("HL7_JOB_SAMPLE_DATA"))
DEFAULT_CATALOG = (os.environ.get("HL7_UC_CATALOG") or "users").strip()
DEFAULT_SCHEMA = (os.environ.get("HL7_UC_SCHEMA") or "ankur_nayyar").strip()
DEFAULT_VOLUME = (os.environ.get("HL7_LANDING_VOLUME") or "landing").strip()

st.title("Sample data → landing volume")
st.caption(
    "Runs the **HL7 sample generator** notebook as a Workflow job. Output is written to "
    f"`/Volumes/<catalog>/<schema>/<volume>/` (default **`{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{DEFAULT_VOLUME}`**). "
    "Then run **DLT** (and the rest of the stack) from **Run Databricks jobs** or **Live activity**."
)

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
Push **synthetic HL7** into the **UC volume** so the medallion pipeline has something to ingest—without using the Databricks UI.

**What happens**  
The configured **`HL7_JOB_SAMPLE_DATA`** job runs `06_generate_sample_data.py`, which writes `.hl7` files to the landing volume. Parameters below are passed as **notebook widgets** for that run.

**Typical next steps**  
1. Wait for this job to finish (**Live activity**).  
2. Start **DLT** → **Inference** (optional) → **Lakebase load**.

**Permissions**  
The app service principal needs **Can run** on the sample-data job.
        """
    )

if JOB_SAMPLE is None:
    st.error(
        "Set **`HL7_JOB_SAMPLE_DATA`** to the numeric Workflow job id (Apps → Environment or `app.yaml`), then redeploy."
    )
    st.stop()

st.markdown("### Generator parameters")

with st.form("sample_vol_form"):
    c1, c2, c3 = st.columns(3)
    with c1:
        catalog = st.text_input("Catalog", value=DEFAULT_CATALOG)
    with c2:
        schema = st.text_input("Schema", value=DEFAULT_SCHEMA)
    with c3:
        volume = st.text_input("Volume name", value=DEFAULT_VOLUME)

    r1, r2, r3 = st.columns(3)
    with r1:
        num_days = st.number_input("Days of data", min_value=7, max_value=365, value=60, step=1)
    with r2:
        num_patients = st.number_input("Patients", min_value=20, max_value=500, value=150, step=10)
    with r3:
        clear_existing = st.selectbox(
            "Clear existing .hl7 files first",
            options=["yes", "no"],
            index=0,
            help="If yes, existing .hl7 files in the volume path are removed before write.",
        )

    start_date = st.text_input(
        "Start date (YYYY-MM-DD), optional",
        value="",
        help="Leave empty to use rolling window from notebook defaults.",
    )

    submitted = st.form_submit_button("Generate & write to volume", type="primary")

if submitted:
    params = {
        "catalog": catalog.strip(),
        "schema": schema.strip(),
        "volume": volume.strip(),
        "num_days": str(int(num_days)),
        "num_patients": str(int(num_patients)),
        "clear_existing": clear_existing,
        "start_date": (start_date or "").strip(),
    }
    with st.spinner("Starting sample-data job…"):
        r = trigger_job(JOB_SAMPLE, notebook_params=params)
    if r.ok:
        st.success(r.message)
        if r.url:
            st.markdown(f"[Open job run in workspace]({r.url})")
        st.info("Monitor progress on **0b · Live activity**, then refresh **0 · System status** after DLT + Lakebase load.")
    else:
        st.error(r.message)

st.markdown("---")
st.markdown("### Volume path preview")
st.code(
    f"/Volumes/{catalog.strip() or DEFAULT_CATALOG}/"
    f"{schema.strip() or DEFAULT_SCHEMA}/"
    f"{volume.strip() or DEFAULT_VOLUME}/",
    language=None,
)

st.page_link("pages/0b_live_activity.py", label="Open Live activity monitor", icon="📡")
st.page_link("pages/z_run_jobs.py", label="Open Run Databricks jobs", icon="🚀")
