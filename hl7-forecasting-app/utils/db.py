"""
Lakebase Postgres via OAuth (Databricks Apps service principal).

Aligned with the working pattern: mint DB tokens with POST /api/2.0/postgres/credentials
and WorkspaceClient.config.authenticate() headers (same as a plain requests client).

Conninfo matches the minimal form (no search_path options); all SQL uses schema-qualified
identifiers in utils/queries.py.

Prerequisite (Lakebase SQL):

    CREATE EXTENSION IF NOT EXISTS databricks_auth;
    SELECT databricks_create_role('<DATABRICKS_CLIENT_ID>'::text, 'service_principal'::text);
"""

import os
from concurrent.futures import ThreadPoolExecutor

import streamlit as st
import psycopg
import requests
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
import pandas as pd

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

_workspace_client = None


def _workspace_for_auth():
    global _workspace_client
    if _workspace_client is None:
        from databricks.sdk import WorkspaceClient

        _workspace_client = WorkspaceClient()
    return _workspace_client


def _generate_db_credential() -> str:
    """Fresh Lakebase OAuth token via workspace REST API (same as working app)."""
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


def _conninfo_base() -> str:
    """Libpq keyword connection string (no password)."""
    if not PGUSER:
        raise RuntimeError("PGUSER / DATABRICKS_CLIENT_ID is required.")
    return (
        f"dbname={PGDATABASE} user={PGUSER} host={PGHOST} port={PGPORT} "
        f"sslmode={PGSSLMODE}"
    )


class OAuthConnection(psycopg.Connection):
    """Postgres connection that uses a freshly minted Lakebase DB credential."""

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        kwargs["password"] = _generate_db_credential()
        return super().connect(conninfo, **kwargs)


@st.cache_resource
def get_pool() -> ConnectionPool:
    return ConnectionPool(
        conninfo=_conninfo_base(),
        connection_class=OAuthConnection,
        min_size=1,
        max_size=10,
        open=True,
        kwargs={"row_factory": dict_row},
    )


def run_query(query: str, params=None, *, quiet: bool = False) -> pd.DataFrame:
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                if not rows:
                    return pd.DataFrame()
                return pd.DataFrame(rows)
    except Exception as e:
        if not quiet:
            st.error(f"Database connection error: {e}")
        try:
            get_pool.clear()
        except Exception:
            pass
        return pd.DataFrame()


def run_query_batch(named_sql: dict[str, str], *, quiet: bool = True) -> dict[str, pd.DataFrame]:
    """
    Run several independent SELECTs in parallel (faster home / pulse pages).

    Preserves key order in the returned dict for stable UI.
    """
    if not named_sql:
        return {}
    keys = list(named_sql.keys())
    n = len(keys)
    workers = min(8, max(1, n))

    def _one(name: str, sql: str) -> tuple[str, pd.DataFrame]:
        return name, run_query(sql, quiet=quiet)

    out: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_one, k, named_sql[k]) for k in keys]
        for fut in futs:
            name, df = fut.result()
            out[name] = df
    return out
