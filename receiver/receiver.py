"""Receives events from the generator and buffers them in SQLite until the ETL pulls them."""
import json
import os
import secrets
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

INGEST_TOKEN = os.environ["INGEST_TOKEN"]
DB_PATH = os.getenv("BUFFER_DB", "/data/buffer.db")

app = FastAPI(title="receiver")


class Event(BaseModel):
    terminal: Optional[str] = None
    productionLine: Optional[str] = None
    packer: Optional[str] = None
    presentation: Optional[str] = None
    uniqueN: Optional[int] = None
    registeredOn: datetime


class Employee(BaseModel):
    id: str
    name: Optional[str] = None
    lastName: Optional[str] = None
    sLastName: Optional[str] = None
    groupName: Optional[str] = None
    isPacker: Optional[int] = None


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)")
    conn.execute("CREATE TABLE IF NOT EXISTS employees (id INTEGER PRIMARY KEY CHECK (id = 1), version TEXT, payload TEXT)")
    # seq restarts if this buffer is wiped, so each buffer gets an id the ETL keeps its cursor under
    conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
    conn.execute("INSERT OR IGNORE INTO meta VALUES ('buffer_id', ?)", (uuid.uuid4().hex,))
    return conn


def check_key(key: Optional[str]) -> None:
    if not key or not secrets.compare_digest(key, INGEST_TOKEN):
        raise HTTPException(401, "invalid api key")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=202)
def post_events(events: list[Event], x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    with db() as conn:
        conn.executemany("INSERT INTO events (payload) VALUES (?)", [(e.model_dump_json(),) for e in events])
    return {"buffered": len(events)}


def buffer_id(conn: sqlite3.Connection) -> str:
    return conn.execute("SELECT v FROM meta WHERE k = 'buffer_id'").fetchone()[0]


@app.get("/buffer")
def get_buffer(x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    with db() as conn:
        return {"id": buffer_id(conn)}


@app.get("/events")
def pull_events(buffer: str, after: int = Query(0, ge=0), limit: int = Query(5000, ge=1, le=10000),
                x_api_key: Optional[str] = Header(None)):
    """Returns events with seq > after. `after` acknowledges that everything up to it is loaded, so those rows are dropped."""
    check_key(x_api_key)
    with db() as conn:
        if buffer != buffer_id(conn):
            raise HTTPException(409, "buffer id changed; resync cursor")
        conn.execute("DELETE FROM events WHERE seq <= ?", (after,))
        rows = conn.execute("SELECT seq, payload FROM events WHERE seq > ? ORDER BY seq LIMIT ?", (after, limit)).fetchall()
        pending = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    return {"events": [{"seq": seq, **json.loads(p)} for seq, p in rows], "pending": pending}


@app.put("/employees")
def put_employees(employees: list[Employee], x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    version = datetime.now(timezone.utc).isoformat()
    payload = json.dumps([e.model_dump() for e in employees])
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO employees (id, version, payload) VALUES (1, ?, ?)", (version, payload))
    return {"stored": len(employees), "version": version}


@app.get("/employees")
def get_employees(x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    with db() as conn:
        row = conn.execute("SELECT version, payload FROM employees WHERE id = 1").fetchone()
    return {"version": row[0], "employees": json.loads(row[1])} if row else {"version": None, "employees": []}
