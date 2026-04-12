from __future__ import annotations

import time
import uuid
from typing import Any

import bcrypt
import jwt
import pyodbc

from shared import config


def _sql_connection_string() -> str:
    cs = config.AZURE_SQL_CONNECTION_STRING
    if not cs:
        raise RuntimeError(
            "AZURE_SQL_CONNECTION_STRING is not set. Add your Azure SQL ODBC connection string "
            "to local.settings.json (local) or Function App Configuration (Azure)."
        )
    if "driver=" not in cs.lower():
        cs = "Driver={ODBC Driver 18 for SQL Server};" + cs
    return cs


def _connect() -> pyodbc.Connection:
    return pyodbc.connect(_sql_connection_string(), timeout=30)


def _rowdict(cursor: pyodbc.Cursor, row: tuple | None) -> dict[str, Any] | None:
    if row is None:
        return None
    cols = [c[0] for c in cursor.description]
    return dict(zip(cols, row))


def init_db() -> None:
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users' AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        CREATE TABLE dbo.users (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NULL,
            display_name NVARCHAR(255) NOT NULL,
            provider VARCHAR(32) NOT NULL CONSTRAINT DF_users_provider DEFAULT ('local'),
            created_at BIGINT NOT NULL
        );
    END
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close()
    finally:
        conn.close()


def register_user(email: str, password: str, display_name: str) -> dict[str, Any]:
    init_db()
    email_norm = email.strip().lower()
    if not email_norm or not password:
        raise ValueError("Email and password are required")
    pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    uid = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO dbo.users (id, email, password_hash, display_name, provider, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (uid, email_norm, pw_hash, display_name or email_norm, "local", int(time.time())),
            )
            conn.commit()
        except pyodbc.IntegrityError:
            raise ValueError("Email already registered") from None
        finally:
            cur.close()
    finally:
        conn.close()
    return {
        "id": uid,
        "email": email_norm,
        "display_name": display_name or email_norm,
        "provider": "local",
    }


def verify_user(email: str, password: str) -> dict[str, Any]:
    init_db()
    email_norm = email.strip().lower()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, email, password_hash, display_name, provider FROM dbo.users WHERE email = ?",
            (email_norm,),
        )
        row = _rowdict(cur, cur.fetchone())
        cur.close()
    finally:
        conn.close()
    if not row or not row.get("password_hash"):
        raise ValueError("Invalid credentials")
    if not bcrypt.checkpw(password.encode("utf-8"), str(row["password_hash"]).encode("utf-8")):
        raise ValueError("Invalid credentials")
    return {
        "id": row["id"],
        "email": row["email"],
        "display_name": row["display_name"],
        "provider": row["provider"],
    }


def upsert_oauth_user(email: str, display_name: str, provider: str) -> dict[str, Any]:
    init_db()
    email_norm = email.strip().lower()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, email, display_name FROM dbo.users WHERE email = ?", (email_norm,))
        row = _rowdict(cur, cur.fetchone())
        if row:
            cur.execute(
                "UPDATE dbo.users SET display_name = ?, provider = ? WHERE email = ?",
                (display_name, provider, email_norm),
            )
            conn.commit()
            cur.close()
            return {"id": row["id"], "email": email_norm, "display_name": display_name, "provider": provider}
        uid = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO dbo.users (id, email, password_hash, display_name, provider, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (uid, email_norm, None, display_name, provider, int(time.time())),
        )
        conn.commit()
        cur.close()
        return {"id": uid, "email": email_norm, "display_name": display_name, "provider": provider}
    finally:
        conn.close()


def get_user_by_id(user_id: str) -> dict[str, Any] | None:
    init_db()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, email, display_name, provider FROM dbo.users WHERE id = ?",
            (user_id,),
        )
        row = _rowdict(cur, cur.fetchone())
        cur.close()
    finally:
        conn.close()
    return row


def issue_token(user: dict[str, Any]) -> str:
    now = int(time.time())
    payload = {
        "sub": user["id"],
        "email": user["email"],
        "name": user.get("display_name") or user["email"],
        "provider": user.get("provider", "local"),
        "iat": now,
        "exp": now + config.JWT_EXPIRE_HOURS * 3600,
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm=config.JWT_ALG)


def decode_token(token: str) -> dict[str, Any]:
    return jwt.decode(token, config.JWT_SECRET, algorithms=[config.JWT_ALG])


def parse_bearer(req) -> str | None:
    auth = req.headers.get("Authorization") or req.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def issue_oauth_state(provider: str) -> str:
    now = int(time.time())
    payload = {
        "typ": "oauth_state",
        "provider": provider,
        "iat": now,
        "exp": now + 600,
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm=config.JWT_ALG)


def verify_oauth_state(token: str, provider: str) -> None:
    payload = jwt.decode(token, config.JWT_SECRET, algorithms=[config.JWT_ALG])
    if payload.get("typ") != "oauth_state" or payload.get("provider") != provider:
        raise ValueError("Invalid OAuth state")
