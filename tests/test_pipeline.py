"""receiver -> etl -> BigQuery -> dashboard queries. Needs a live bigquery-emulator (see README)."""
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / d) for d in ("common", "receiver", "etl", "dashboard")]
os.environ.setdefault("INGEST_TOKEN", "test-key")
os.environ.setdefault("RECEIVER_URL", "http://receiver")
os.environ["BUFFER_DB"] = str(Path(tempfile.mkdtemp()) / "buffer.db")

from fastapi.testclient import TestClient  # noqa: E402

import app as dashboard  # noqa: E402
import bq  # noqa: E402
import etl  # noqa: E402
import receiver  # noqa: E402

KEY = {"X-Api-Key": "test-key"}
EMPLOYEES = [{"id": "7888", "name": "Ana", "lastName": "Ruiz", "sLastName": "Paz", "groupName": "02", "isPacker": 1}]
EVENTS = [
    {"terminal": "1", "productionLine": " b ", "packer": "7888", "presentation": "rpc", "uniqueN": 107, "registeredOn": "2026-10-19 13:47:35"},
    {"terminal": "99", "productionLine": "1", "packer": "4242", "presentation": "C3", "uniqueN": 5, "registeredOn": "2026-10-18 08:00:00"},
]


def test_tidy_normalizes_and_resolves_packer():
    emps = {"7888": EMPLOYEES[0]}
    a = etl.tidy({**EVENTS[0], "seq": 1}, emps, "buf", "2026-10-19T00:00:00+00:00")
    assert (a["production_line"], a["presentation"], a["terminal"]) == ("B", "RPC", "1")
    assert (a["packer_name"], a["packer_group"]) == ("Ana Ruiz Paz", "02")
    assert (a["registered_on"], a["event_date"]) == ("2026-10-19T13:47:35+00:00", "2026-10-19")
    b = etl.tidy({**EVENTS[1], "seq": 2}, emps, "buf", "x")
    assert (b["terminal"], b["production_line"], b["packer_id"], b["packer_name"]) == ("UNKNOWN", "UNKNOWN", "4242", None)


def test_pipeline():
    r = TestClient(receiver.app)
    assert r.post("/events", json=EVENTS).status_code == 401
    assert r.get("/buffer").status_code == 401
    assert r.post("/events", json=[{"packer": "1"}], headers=KEY).status_code == 422  # registeredOn required
    assert r.put("/employees", json=EMPLOYEES, headers=KEY).status_code == 200
    assert r.post("/events", json=EVENTS, headers=KEY).json() == {"buffered": 2}

    def get(path, **params):  # route the ETL's HTTP pulls to the test client
        resp = r.get(path, params=params, headers=KEY)
        resp.raise_for_status()
        return resp.json()
    etl.get = get

    # every run uses a fresh buffer (new temp db) against a persistent BigQuery: events must still load
    job = etl.Etl()
    assert job.run_once() == 2
    assert job.run_once() == 0  # acked rows are not served again
    assert r.get("/events", params={"buffer": job.buffer_id, "after": job.cursor}, headers=KEY).json()["pending"] == 0
    assert r.get("/events", params={"buffer": "other", "after": 99}, headers=KEY).status_code == 409
    restarted = etl.Etl()
    restarted.sync_cursor()
    assert (restarted.buffer_id, restarted.cursor) == (job.buffer_id, job.cursor)  # restart resumes from BigQuery

    loaded = bq.query(f"SELECT * FROM `{bq.EVENTS}` WHERE buffer_id = @b ORDER BY event_id",
                      [bq.bigquery.ScalarQueryParameter("b", "STRING", job.buffer_id)])
    assert [e["packer_name"] for e in loaded] == ["Ana Ruiz Paz", None]

    s = dashboard.stats("2026-10-19", "2026-10-19", ["B"], [])
    assert s["totals"]["events"] >= 1 and {r["production_line"] for r in s["by_line"]} == {"B"}
    assert "Ana Ruiz Paz" in [p["packer"] for p in s["top_packers"]]
    assert dashboard.stats("2026-10-18", "2026-10-19", [], ["C3"])["top_packers"][0]["packer"] == "Unknown #4242"
    o = dashboard.options()
    assert "UNKNOWN" in o["lines"] and "RPC" in o["pres"]
