"""
Ask your data with Databricks AI/BI Genie (natural language → SQL).

Requires a curated Genie space over your Unity Catalog tables and GENIE_SPACE_ID
configured for this app. Similar in spirit to embedded Genie / standalone chat apps.
"""

import streamlit as st

from utils.genie_client import get_genie_space_id
from utils.theme import apply_theme

st.set_page_config(
    page_title="Ask your data (Genie)",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()

st.markdown(
    """
    <style>
    .genie-setup { background: #f0f4ff; border-radius: 10px; padding: 16px; border: 1px solid #c5d4ff; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Ask your data")
st.caption("Powered by Databricks AI/BI Genie — ask questions in plain English about tables in your Genie space.")

with st.expander("What this page does", expanded=False):
    st.markdown(
        """
**Purpose**  
A **natural-language** layer on top of the tables and metrics curated in a **Databricks Genie space** (AI/BI). You type a question; Genie proposes SQL and explanations against that space—not against arbitrary Postgres tables unless they are exposed there.

**What you will see**  
- A simple **chat** UI when `GENIE_SPACE_ID` (or app secrets) is configured.  
- **Setup instructions** if the space id is missing or the iframe cannot load.

**What it is not**  
- Not a replacement for the other pages: those read **Lakebase** directly with fixed queries.  
- Not guaranteed to match raw HL7; answers depend on **which UC tables** were added to the Genie space and their permissions.

**Requirements**  
Workspace auth, Genie space UUID, and UC/SQL warehouse grants for the app’s service principal (see `notebooks/12_genie_uc_grants.py` in the repo).
        """
    )


def _resolve_space_id():
    sid = get_genie_space_id()
    if sid:
        return sid
    try:
        if "GENIE_SPACE_ID" in st.secrets:
            raw = st.secrets["GENIE_SPACE_ID"]
            if raw:
                return str(raw).strip().strip('"').strip("'") or None
    except Exception:
        pass
    return None


SPACE_ID = _resolve_space_id()

if "genie_conversation_id" not in st.session_state:
    st.session_state.genie_conversation_id = None
if "genie_messages" not in st.session_state:
    st.session_state.genie_messages = []

if not SPACE_ID:
    st.markdown('<div class="genie-setup">', unsafe_allow_html=True)
    st.error("**GENIE_SPACE_ID is not set.** The app does not see a Genie space id at runtime.")
    st.markdown(
        """
**Fix (pick one)**

1. **`app.yaml` (default in repo)**  
   Set **`GENIE_SPACE_ID`** to the UUID from the Genie URL (`.../genie/rooms/<SPACE_ID>`), then **redeploy**
   the app from Git / bundle so the runtime receives the env var.

2. **Apps → Environment variables**  
   Set **`GENIE_SPACE_ID`** there if you prefer not to bake it into `app.yaml`.

3. **Unity Catalog + warehouse**  
   Grant the app service principal `USE CATALOG` / `USE SCHEMA` / `SELECT` on the schema Genie uses, and
   `CAN USE` on the Genie SQL warehouse. Run **`notebooks/12_genie_uc_grants.py`** (or job **`hl7_genie_uc_grants`**).

Project **README** → *Databricks App (HL7App)* → *Ask your data (Databricks Genie)* has the full checklist.

Docs: [Genie in Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/genie),
[Conversation API](https://docs.databricks.com/aws/en/genie/conversation-api).
        """
    )
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()


def _render_parts(parts: list) -> None:
    for part in parts:
        pt = part.get("type")
        if pt == "text":
            st.markdown(part.get("body", ""))
        elif pt == "sql":
            st.caption(part.get("description", "Generated SQL"))
            st.code(part.get("sql", ""), language="sql")
        elif pt == "table":
            st.dataframe(part.get("df"), use_container_width=True)


with st.sidebar:
    st.subheader("Conversation")
    if st.button("New conversation", use_container_width=True):
        st.session_state.genie_conversation_id = None
        st.session_state.genie_messages = []
        st.rerun()
    st.caption("Follow-up questions stay in context until you start a new conversation.")

SUGGESTED = [
    "How many encounters do we have by patient class?",
    "What are the top 10 diagnosis codes by volume?",
    "Show ED vs ICU arrival trends by day.",
]

with st.expander("Suggested questions", expanded=False):
    for i, q in enumerate(SUGGESTED):
        if st.button(q, key=f"sq_{i}", use_container_width=True):
            st.session_state["_pending_prompt"] = q

for role, content in st.session_state.genie_messages:
    with st.chat_message(role):
        if isinstance(content, list):
            _render_parts(content)
        else:
            st.markdown(str(content))

prompt = st.chat_input("Ask a question about your data…")
if "_pending_prompt" in st.session_state:
    prompt = st.session_state.pop("_pending_prompt") or prompt

if prompt:
    from utils.genie_client import (
        ask_genie,
        extract_conversation_id,
        message_to_ui_parts,
        workspace_client,
    )

    st.session_state.genie_messages.append(("user", prompt))
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Genie is thinking…"):
            try:
                client = workspace_client()
                msg = ask_genie(
                    client,
                    SPACE_ID,
                    prompt,
                    conversation_id=st.session_state.genie_conversation_id,
                )
                cid = extract_conversation_id(msg)
                if cid:
                    st.session_state.genie_conversation_id = cid
                parts = message_to_ui_parts(client, msg)
                st.session_state.genie_messages.append(("assistant", parts))
                _render_parts(parts)
            except Exception as e:
                err = f"**Genie error:** `{e}`\n\nCheck Genie space permissions, warehouse access, and that the space is curated for your tables."
                st.error(err)
                st.session_state.genie_messages.append(("assistant", err))

    st.rerun()
