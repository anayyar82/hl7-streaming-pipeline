"""
Load test — parallel Lakebase queries to measure latency under concurrent connections.
"""

from __future__ import annotations

import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st

from utils import queries
from utils.db import execute_load_probe, ENDPOINT_NAME, PGHOST, PGUSER
from utils.navigation import render_sidebar_nav
from utils.theme import apply_theme

_PRESETS: dict[str, str] = {
    "SELECT 1 (no table IO)": "SELECT 1 AS one",
    "Count encounters (7d)": queries.HOME_ENCOUNTER_COUNT_7D.strip(),
    "Message volume (24h)": queries.HOME_MESSAGE_VOLUME_24H.strip(),
}

st.set_page_config(
    page_title="HL7App – Load test",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_theme()
render_sidebar_nav()

st.title("Load test")
st.caption(
    "Driver **Lakebase** with parallel short-lived connections. The rest of the app serializes "
    "DB access; this page uses `execute_load_probe` so threads can run in parallel."
)

with st.expander("Limits & how to read this", expanded=False):
    st.markdown(
        """
- **One app replica** = one Python process. This measures **thread-parallel** Lakebase calls, not
  many separate users on different app instances.
- For **HTTP**-level load, use your Databricks **App URL** with a tool such as k6, Locust, or
  `hey`, or many browser clients.
- Start with **4** workers; avoid huge totals to protect shared Lakebase.
        """
    )

c1, c2, c3, c4 = st.columns(4)
with c1:
    workers = st.slider("Parallel workers (threads)", min_value=1, max_value=32, value=4, step=1)
with c2:
    rounds = st.number_input("Batches (each batch runs all workers once)", min_value=1, max_value=200, value=1, step=1)
with c3:
    preset = st.selectbox("Preset SQL", options=list(_PRESETS.keys()), index=0)
with c4:
    h = (PGHOST or "—")
    st.metric("Postgres host", h[:20] + "…" if len(h) > 20 else h)

st.code(_PRESETS[preset], language="sql")

if st.button("Run load test", type="primary", use_container_width=True):
    sql = _PRESETS[preset]
    total = int(workers * rounds)
    if total > 2000:
        st.error("Maximum 2,000 requests per run. Reduce workers or batches.")
    else:
        latencies: list[float] = []
        errors: list[str] = []
        t0 = time.perf_counter()

        def _one(_: int) -> tuple[bool, float, str | None]:
            return execute_load_probe(sql, None)

        with st.spinner(f"Running {total} request(s) ({workers} workers × {rounds} batch(es))…"):
            for _ in range(rounds):
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futs = [ex.submit(_one, i) for i in range(workers)]
                    for f in as_completed(futs):
                        ok, sec, err = f.result()
                        if ok:
                            latencies.append(sec * 1000.0)
                        else:
                            errors.append(err or "unknown error")

        wall = time.perf_counter() - t0
        st.session_state["lt_result"] = {
            "latencies": latencies,
            "errors": errors,
            "wall": wall,
            "sql_label": preset,
        }

r = st.session_state.get("lt_result")
if r is not None:
    latencies = r["latencies"]
    errors = r["errors"]
    wall = r["wall"]
    oks, fail = len(latencies), len(errors)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("OK", f"{oks}")
    m2.metric("Failed", f"{fail}")
    m3.metric("Wall time (s)", f"{wall:.2f}")
    m4.metric("OK / wall (req/s)", f"{(oks / wall):.2f}" if wall > 0 and oks else "0")

    if errors:
        with st.expander(f"Errors ({len(errors)} total, sample below)", expanded=bool(errors and oks == 0)):
            for e in errors[:8]:
                st.text(e)

    if latencies:
        latencies.sort()
        p50 = statistics.median(latencies)
        p95 = latencies[int(0.95 * (len(latencies) - 1))]
        p99 = latencies[min(len(latencies) - 1, int(0.99 * (len(latencies) - 1)))]
        st.subheader("Latency (ms) — per successful request")
        t1, t2, t3, t4, t5 = st.columns(5)
        t1.metric("min", f"{latencies[0]:.1f}")
        t2.metric("p50", f"{p50:.1f}")
        t3.metric("p95", f"{p95:.1f}")
        t4.metric("p99", f"{p99:.1f}")
        t5.metric("max", f"{latencies[-1]:.1f}")
        st.bar_chart({"ms": latencies}, height=240)
else:
    st.info("Configure options above and click **Run load test**.")

st.markdown("---")
st.markdown(
    f"**`PGUSER`:** `{PGUSER or '—'}`  \n**`ENDPOINT_NAME`:** `{ENDPOINT_NAME}`  \n"
    "*(Dedicated ingest-style UIs often expose a `/load-test` path; Streamlit uses this file’s "
    "name in the sidebar: **0d · Load test** — your Apps URL + navigation.)*"
)
