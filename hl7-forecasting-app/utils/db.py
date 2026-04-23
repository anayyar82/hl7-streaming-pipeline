"""
Lakebase Postgres via OAuth (Databricks Apps service principal).

Uses POST /api/2.0/postgres/credentials and WorkspaceClient.config.authenticate().

This module intentionally avoids psycopg_pool: under Streamlit + fragments + many
sessions, pool checkout produced "couldn't get a connection after 30.00 sec" even
with raised limits. A process-wide lock and short-lived connections (with a small
token cache) removes pool contention and stale-pooled-token edge cases.

Prerequisite (Lakebase SQL):

    CREATE EXTENSION IF NOT EXISTS databricks_auth;
    SELECT databricks_create_role('<DATABRICKS_CLIENT_ID>'::text, 'service_principal'::text);
"""

from __future__ import annotations

import os
import threading
import time

import pandas as pd
import psycopg
import requests
import streamlit as st
from psycopg.rows import dict_row

PGHOST = os.environ.get(
    "PGHOST",
    "ep-wandering-meadow-d1tdkigo.database.us-west-2.cloud.databricks.com",
)
PGDATABASE = os.environ.get("PGDATABASE", "databricks_postgres")
PGPORT = os.environ.get("PGPORT", "5432")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")
PGUSER = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID", "")
ENDPOINT_NAME = os.environ.get(
    "ENDPOINT_NAME",
    "projects/ankurhlsproject/branches/production/endpoints/primary",
)

# Serialize Lakebase access in this Python process (Streamlit may overlap fragment runs).
_db_lock = threading.Lock()

_token_lock = threading.Lock()
_cached_token: str | None = None
_cached_at: float = 0.0
# Reuse the same DB token for a short window to avoid hammering /postgres/credentials.
_TOKEN_TTL_SEC = float(os.environ.get("LAKEBASE_TOKEN_CACHE_SEC", "50"))

_workspace_client = None


def _workspace_for_auth():
    global _workspace_client
    if _workspace_client is None:
        from databricks.sdk import WorkspaceClient

        _workspace_client = WorkspaceClient()
    return _workspace_client


def _generate_db_credential() -> str:
    """Fresh Lakebase OAuth token via workspace REST API."""
    w = _workspace_for_auth()
    api_url = f"{w.config.host}/api/2.0/postgres/credentials"
    headers = dict(w.config.authenticate())
    resp = requests.post(
        api_url,
        headers=headers,
        json={"endpoint": ENDPOINT_NAME},
        timeout=60,
    )
    resp.raise_for_status()
    token = (resp.json().get("token") or "").strip()
    if not token:
        raise RuntimeError("Empty token from /api/2.0/postgres/credentials")
    return token


def _invalidate_token_cache() -> None:
    global _cached_token, _cached_at
    with _token_lock:
        _cached_token = None
        _cached_at = 0.0


def _lakebase_password() -> str:
    global _cached_token, _cached_at
    now = time.monotonic()
    with _token_lock:
        if _cached_token is not None and (now - _cached_at) < _TOKEN_TTL_SEC:
            return _cached_token
    tok = _generate_db_credential()
    with _token_lock:
        _cached_token = tok
        _cached_at = time.monotonic()
    return tok


def _conninfo_base() -> str:
    """Libpq keyword connection string (password supplied separately to connect())."""
    if not PGUSER:
        raise RuntimeError("PGUSER / DATABRICKS_CLIENT_ID is required.")
    return (
        f"dbname={PGDATABASE} user={PGUSER} host={PGHOST} port={PGPORT} "
        f"sslmode={PGSSLMODE}"
    )


def _connect_timeout_sec() -> int:
    return max(5, int(os.environ.get("PGCONNECT_TIMEOUT", "25")))


def _lakebase_auth_hint(exc: BaseException) -> str:
    """Actionable text when Lakebase rejects OAuth (usually missing databricks_create_role)."""
    msg = str(exc).lower()
    if "password authentication failed" not in msg:
        return ""
    uid = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or "<PGUSER>"
    return (
        f" **Lakebase:** In the Lakebase **SQL Editor**, as a user who can administer the database, run "
        f"`CREATE EXTENSION IF NOT EXISTS databricks_auth;` then "
        f"`SELECT databricks_create_role('{uid}'::text, 'service_principal'::text);` "
        f"then GRANT CONNECT on the database and USAGE/SELECT on schema (see **notebooks/11_lakebase_grants.py**). "
        f"The UUID must match **PGUSER** / `databricks apps get hl7app` → `service_principal_client_id`."
    )


def _connect():
    """New connection; caller must hold _db_lock for the connection's lifetime."""
    pwd = _lakebase_password()
    try:
        return psycopg.connect(
            _conninfo_base(),
            password=pwd,
            connect_timeout=_connect_timeout_sec(),
            row_factory=dict_row,
        )
    except psycopg.OperationalError:
        _invalidate_token_cache()
        pwd = _lakebase_password()
        return psycopg.connect(
            _conninfo_base(),
            password=pwd,
            connect_timeout=_connect_timeout_sec(),
            row_factory=dict_row,
        )


def execute_load_probe(query: str, params=None) -> tuple[bool, float, str | None]:
    """
    One Lakebase round-trip without the process-wide _db_lock so threads can run in parallel.

    Use only from the Load test page. Normal pages keep using run_query / run_query_batch to
    avoid connection storms under Streamlit fragments.
    """
    t0 = time.perf_counter()
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description is not None:
                    cur.fetchall()
        return True, time.perf_counter() - t0, None
    except Exception as e:
        _invalidate_token_cache()
        return False, time.perf_counter() - t0, (str(e) or "")[:500]


def run_query(query: str, params=None, *, quiet: bool = False) -> pd.DataFrame:
    try:
        with _db_lock:
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
    except Exception as e:
        _invalidate_token_cache()
        if not quiet:
            st.error(f"Database connection error: {e}{_lakebase_auth_hint(e)}")
        return pd.DataFrame()


def run_query_batch(named_sql: dict[str, str], *, quiet: bool = True) -> dict[str, pd.DataFrame]:
    """
    Run several independent SELECTs on one connection (single lock hold).

    Preserves key order in the returned dict for stable UI.
    """
    if not named_sql:
        return {}
    out: dict[str, pd.DataFrame] = {}
    try:
        with _db_lock:
            with _connect() as conn:
                with conn.cursor() as cur:
                    for name, sql in named_sql.items():
                        try:
                            cur.execute(sql)
                            rows = cur.fetchall()
                            if not rows:
                                out[name] = pd.DataFrame()
                            else:
                                out[name] = pd.DataFrame(rows)
                        except Exception as e:
                            conn.rollback()
                            if not quiet:
                                st.error(f"Database connection error: {e}{_lakebase_auth_hint(e)}")
                            out[name] = pd.DataFrame()
    except Exception as e:
        _invalidate_token_cache()
        if not quiet:
            st.error(f"Database connection error: {e}{_lakebase_auth_hint(e)}")
        return {k: pd.DataFrame() for k in named_sql}
    return out
