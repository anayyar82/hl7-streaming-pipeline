"""
Databricks AI/BI Genie Conversation API helpers.

Intended to run inside a Databricks App: WorkspaceClient() uses the app's
service principal (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET).

Resolve the space id (first match): ``GENIE_SPACE_ID``, then ``DATABRICKS_GENIE_SPACE_ID``.

Typical wiring: in ``app.yaml``, ``GENIE_SPACE_ID`` with ``valueFrom: genie-space`` where
``genie-space`` is the **resource key** from Apps → hl7app → Resources (not the Genie space
display title). Or set ``GENIE_SPACE_ID`` in the app environment to the UUID from the Genie URL
(``.../rooms/<SPACE_ID>``).
"""

from __future__ import annotations

import os
from typing import Any, Optional

import pandas as pd


def get_genie_space_id() -> Optional[str]:
    """Return trimmed Genie space UUID from environment, or None if unset."""
    for key in ("GENIE_SPACE_ID", "DATABRICKS_GENIE_SPACE_ID"):
        raw = os.getenv(key)
        if not raw:
            continue
        sid = raw.strip().strip('"').strip("'")
        if sid:
            return sid
    return None


def workspace_client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def ask_genie(client, space_id: str, prompt: str, conversation_id: Optional[str] = None):
    """
    Start a new conversation or append a user message. Returns the latest
    Genie message object (SDK shape may vary slightly by version).
    """
    g = client.genie
    if conversation_id:
        return g.create_message_and_wait(space_id, conversation_id, prompt)
    return g.start_conversation_and_wait(space_id, prompt)


def extract_conversation_id(message) -> Optional[str]:
    cid = getattr(message, "conversation_id", None)
    if cid:
        return str(cid)
    return None


def message_to_ui_parts(client, message) -> list[dict[str, Any]]:
    """Turn a Genie message into renderable chunks for Streamlit."""
    parts: list[dict[str, Any]] = []

    for att in getattr(message, "attachments", None) or []:
        txt = getattr(att, "text", None)
        if txt is not None:
            content = getattr(txt, "content", None)
            if content is not None:
                body = "".join(content) if isinstance(content, list) else str(content)
                parts.append({"type": "text", "body": body})
        q = getattr(att, "query", None)
        if q is not None:
            desc = getattr(q, "description", None) or ""
            sql = getattr(q, "query", None)
            if isinstance(sql, list):
                sql = "".join(sql)
            parts.append({"type": "sql", "description": str(desc), "sql": (sql or "").strip()})

    qr = getattr(message, "query_result", None)
    stmt_id = getattr(qr, "statement_id", None) if qr else None
    if stmt_id:
        df = statement_to_dataframe(client, str(stmt_id))
        if df is not None and not df.empty:
            parts.append({"type": "table", "df": df})
        elif df is not None:
            parts.append({"type": "text", "body": "_Query returned no rows._"})

    if not parts:
        parts.append({"type": "text", "body": "_No answer text was returned. Try rephrasing._"})
    return parts


def statement_to_dataframe(client, statement_id: str) -> Optional[pd.DataFrame]:
    try:
        res = client.statement_execution.get_statement(statement_id)
    except Exception:
        return None
    result = getattr(res, "result", None)
    data = getattr(result, "data_array", None) if result else None
    manifest = getattr(res, "manifest", None)
    schema = getattr(manifest, "schema", None) if manifest else None
    cols = getattr(schema, "columns", None) if schema else None
    if not data or not cols:
        return None
    names = [getattr(c, "name", f"col_{i}") for i, c in enumerate(cols)]
    return pd.DataFrame(data, columns=names)
