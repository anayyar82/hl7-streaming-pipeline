"""
Databricks AI/BI Genie Conversation API helpers.

Intended to run inside a Databricks App: WorkspaceClient() uses the app's
service principal (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET).

Resolve the space id (first match): ``GENIE_SPACE_ID``, then ``DATABRICKS_GENIE_SPACE_ID``.

Typical wiring: in ``app.yaml``, set ``GENIE_SPACE_ID`` to the UUID from the Genie URL
(``.../genie/rooms/<SPACE_ID>``), or use ``valueFrom: <resource-key>`` if the space is attached
under Apps → Resources.
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

import pandas as pd


def workspace_host_url() -> Optional[str]:
    """HTTPS workspace root for deep links (Apps / notebooks set DATABRICKS_HOST)."""
    raw = (os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_WORKSPACE_URL") or "").strip()
    if not raw:
        return None
    if not raw.startswith("http"):
        return "https://" + raw.lstrip("/")
    return raw.rstrip("/")


def genie_room_url(space_id: str) -> Optional[str]:
    base = workspace_host_url()
    if not base or not space_id:
        return None
    return f"{base}/genie/rooms/{space_id.strip()}"


def _format_message_error_obj(err: Any) -> Optional[str]:
    if err is None:
        return None
    parts: list[str] = []
    t = getattr(err, "type", None)
    if t is not None:
        parts.append(f"type: {t}")
    msg = getattr(err, "error", None)
    if msg:
        parts.append(str(msg).strip())
    return "\n".join(parts) if parts else None


def last_failed_assistant_detail(client, space_id: str, conversation_id: str) -> Optional[str]:
    """
    After create_message_and_wait / start_conversation_and_wait raises on FAILED, the SDK often omits
    the real reason. The same failure is usually visible on the latest assistant GenieMessage.
    """
    try:
        g = client.genie
        resp = g.list_conversation_messages(space_id, conversation_id, page_size=50)
        messages = getattr(resp, "messages", None) or []
    except Exception:
        return None
    for m in reversed(messages):
        st = getattr(m, "status", None)
        stv = getattr(st, "value", None) if st is not None else (str(st) if st else "")
        if stv and "FAILED" not in str(stv).upper():
            continue
        bits: list[str] = []
        if stv:
            bits.append(f"status: {stv}")
        me = _format_message_error_obj(getattr(m, "error", None))
        if me:
            bits.append(me)
        mid = getattr(m, "message_id", None) or getattr(m, "id", None)
        if mid:
            bits.append(f"message_id: {mid}")
        if bits:
            return "Genie message detail:\n" + "\n".join(bits)
    return None


def format_genie_error(exc: BaseException) -> str:
    """
    Surface SDK / API detail for failed Genie turns (MessageStatus.FAILED, etc.).
    """
    parts: list[str] = [str(exc).strip() or type(exc).__name__]
    det = getattr(exc, "details", None)
    if det:
        parts.append(str(det))
    inner = getattr(exc, "__cause__", None) or getattr(exc, "inner_exception", None)
    if inner is not None:
        parts.append(f"Cause: {inner}")
    resp = getattr(exc, "response", None)
    if resp is not None:
        parts.append(f"response: {resp!r}"[:1200])
    return "\n".join(p for p in parts if p)


def _conversation_id_from_error_text(text: str) -> Optional[str]:
    # UUID v4-ish (Genie conversation ids are often UUIDs)
    for m in re.finditer(
        r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
        text,
        flags=re.I,
    ):
        return m.group(0)
    return None


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

    On MessageStatus.FAILED, enriches the exception with ``GenieMessage.error`` when possible
    (SDK issue: https://github.com/databricks/databricks-sdk-py/issues/939).
    """
    g = client.genie
    try:
        if conversation_id:
            return g.create_message_and_wait(space_id, conversation_id, prompt)
        return g.start_conversation_and_wait(space_id, prompt)
    except Exception as e:
        cid = conversation_id
        if not cid:
            cid = _conversation_id_from_error_text(str(e))
        if cid:
            detail = last_failed_assistant_detail(client, space_id, str(cid))
            if detail:
                raise RuntimeError(f"{e}\n\n{detail}") from e
        raise


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
