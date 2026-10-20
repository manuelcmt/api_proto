"""Pulls batches from the receiver, tidies them and loads them into BigQuery."""
import os
import time
from datetime import datetime, timezone

import requests

import bq

RECEIVER_URL = os.environ["RECEIVER_URL"]
HEADERS = {"X-Api-Key": os.environ["INGEST_TOKEN"]}
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))

UNKNOWN = "UNKNOWN"
KNOWN_TERMINALS = {"1", "2", "3"}
KNOWN_LINES = {"A", "B"}


def code(value) -> str | None:
    return value.strip().upper() if isinstance(value, str) and value.strip() else None


def full_name(emp: dict) -> str | None:
    return " ".join(p.strip() for p in (emp.get("name"), emp.get("lastName"), emp.get("sLastName")) if p and p.strip()) or None


def tidy(event: dict, employees: dict[str, dict], buffer_id: str, loaded_at: str) -> dict:
    registered = datetime.fromisoformat(event["registeredOn"])
    registered = registered if registered.tzinfo else registered.replace(tzinfo=timezone.utc)  # source times are naive
    terminal, line = code(event.get("terminal")), code(event.get("productionLine"))
    packer = code(event.get("packer"))
    emp = employees.get(packer, {})
    return {
        "buffer_id": buffer_id,
        "event_id": event["seq"],
        "registered_on": registered.isoformat(),
        "event_date": registered.date().isoformat(),
        "terminal": terminal if terminal in KNOWN_TERMINALS else UNKNOWN,
        "production_line": line if line in KNOWN_LINES else UNKNOWN,
        "presentation": code(event.get("presentation")) or UNKNOWN,
        "unique_n": event.get("uniqueN"),
        "packer_id": packer,
        "packer_name": full_name(emp),
        "packer_group": code(emp.get("groupName")),
        "loaded_at": loaded_at,
    }


def get(path: str, **params) -> dict:
    r = requests.get(RECEIVER_URL + path, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


class Etl:
    def __init__(self):
        bq.ensure_tables()
        self.buffer_id = None
        self.cursor = 0
        self.employees: dict[str, dict] = {}
        self.employees_version = None

    def sync_cursor(self) -> None:
        # cursor lives in BigQuery, so a restart resumes after the last loaded event of the current buffer
        self.buffer_id = get("/buffer")["id"]
        self.cursor = bq.query(f"SELECT COALESCE(MAX(event_id), 0) AS c FROM `{bq.EVENTS}` WHERE buffer_id = @b",
                               [bq.bigquery.ScalarQueryParameter("b", "STRING", self.buffer_id)])[0]["c"]
        print(f"buffer {self.buffer_id}, cursor={self.cursor}", flush=True)

    def run_once(self) -> int:
        if self.buffer_id is None:
            self.sync_cursor()
        emp = get("/employees")
        if emp["version"] != self.employees_version:
            self.employees = {code(e["id"]): e for e in emp["employees"]}
            self.employees_version = emp["version"]
            print(f"employees loaded: {len(self.employees)}", flush=True)

        try:
            batch = get("/events", buffer=self.buffer_id, after=self.cursor, limit=BATCH_SIZE)["events"]
        except requests.HTTPError as e:
            if e.response.status_code == 409:  # receiver buffer was replaced
                self.buffer_id = None
                return 0
            raise
        if not batch:
            return 0
        loaded_at = datetime.now(timezone.utc).isoformat()
        errors = bq.client.insert_rows_json(bq.EVENTS, [tidy(e, self.employees, self.buffer_id, loaded_at) for e in batch])
        if errors:
            raise RuntimeError(f"BigQuery insert failed: {errors[:3]}")  # cursor not advanced -> batch is retried
        self.cursor = batch[-1]["seq"]
        return len(batch)


def main() -> None:
    while True:
        try:
            etl = Etl()
            break
        except Exception as e:  # BigQuery not up yet
            print(f"waiting for BigQuery: {e}", flush=True)
            time.sleep(3)
    while True:
        try:
            n = etl.run_once()
            if n:
                print(f"loaded {n} events (cursor={etl.cursor})", flush=True)
            if n < BATCH_SIZE:
                time.sleep(POLL_SECONDS)
        except Exception as e:
            print(f"etl error, retrying: {e}", flush=True)
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
