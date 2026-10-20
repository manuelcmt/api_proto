import asyncio
import os
import sqlite3
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp


@dataclass(frozen=True)
class ProductionEntry:
    terminal: Optional[str]
    productionLine: Optional[str]
    packer: Optional[str]
    presentation: Optional[str]
    uniqueN: Optional[int]
    registeredOn: str

    @property
    def registered_on_dt(self) -> datetime:
        return datetime.fromisoformat(self.registeredOn)


@dataclass(frozen=True)
class EmployeeEntry:
    id: str
    name: Optional[str]
    lastName: Optional[str]
    sLastName: Optional[str]
    groupName: Optional[str]
    isPacker: Optional[int]


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "metrics_anonymized.sqlite"
PRODUCTION_TABLE = "ProductionLog"
EMPLOYEES_TABLE = "EmployeesEmployee"
ENV_PATH = BASE_DIR.parent / ".env"
INGEST_URL = os.getenv("INGEST_URL", "http://127.0.0.1:8001")
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "10"))
# Source day replayed live as "today"; earlier days are preloaded, later rows (one stray 2024-11-12 event) are skipped.
REPLAY_DAY = date.fromisoformat(os.getenv("REPLAY_DAY", "2024-07-10"))
PRELOAD_CHUNK = 5000


def load_entries(db_path: Path, table_name: str, limit: Optional[int] = None) -> list[ProductionEntry]:
    query = (
        f"SELECT terminal, productionLine, packer, presentation, uniqueN, registeredOn "
        f"FROM \"{table_name}\" "
        f"WHERE registeredOn IS NOT NULL "
        f"ORDER BY registeredOn ASC, rowid ASC"
    )
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query).fetchall()

    return [
        ProductionEntry(
            terminal=row["terminal"],
            productionLine=row["productionLine"],
            packer=row["packer"],
            presentation=row["presentation"],
            uniqueN=row["uniqueN"],
            registeredOn=row["registeredOn"],
        )
        for row in rows
    ]


def load_employees(db_path: Path, table_name: str) -> list[EmployeeEntry]:
    query = (
        f"SELECT id, name, lastName, sLastName, groupName, isPacker "
        f"FROM \"{table_name}\" "
        f"ORDER BY CAST(id AS INTEGER), id"
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query).fetchall()

    return [
        EmployeeEntry(
            id=row["id"],
            name=row["name"],
            lastName=row["lastName"],
            sLastName=row["sLastName"],
            groupName=row["groupName"],
            isPacker=row["isPacker"],
        )
        for row in rows
    ]


async def send(session: aiohttp.ClientSession, method: str, path: str, payload, headers: dict) -> None:
    while True:  # retry instead of dropping events while ingest is down/restarting
        try:
            async with session.request(method, INGEST_URL + path, json=payload, headers=headers) as r:
                if r.status < 300:
                    return
                body = await r.text()
                if r.status == 401:
                    raise SystemExit("ingest rejected INGEST_TOKEN")
                if r.status == 422:
                    print(f"skipping invalid payload: {body}")
                    return
                print(f"ingest {r.status}: {body[:200]}")
        except aiohttp.ClientError as e:
            print(f"ingest unreachable ({e}), retrying")
        await asyncio.sleep(2)


async def replay_to_http(session: aiohttp.ClientSession, entries: list[ProductionEntry], speed: float, headers: dict, shift: timedelta) -> int:
    if speed <= 0:
        raise ValueError("speed must be > 0")

    if not entries:
        return 0

    base_time = entries[0].registered_on_dt
    loop = asyncio.get_running_loop()
    start = loop.time()

    for sent, entry in enumerate(entries, 1):
        # schedule against the wall clock so time spent in POSTs doesn't accumulate as drift
        offset_seconds = (entry.registered_on_dt - base_time).total_seconds() / speed
        wait_seconds = start + offset_seconds - loop.time()
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        await send(session, "POST", "/events", [shifted(entry, shift)], headers)
        if sent % 100 == 0:
            print(f"events sent: {sent}", flush=True)

    return len(entries)


def ingest_token() -> str:
    if "INGEST_TOKEN" in os.environ:
        return os.environ["INGEST_TOKEN"]
    for line in ENV_PATH.read_text().splitlines():
        if line.startswith("INGEST_TOKEN="):
            return line.split("=", 1)[1]
    raise SystemExit(f"INGEST_TOKEN not set and not found in {ENV_PATH}")


def shifted(entry: ProductionEntry, shift: timedelta) -> dict:
    return {**asdict(entry), "registeredOn": (entry.registered_on_dt + shift).isoformat(sep=" ")}


async def main() -> None:
    headers = {"X-Api-Key": ingest_token()}
    shift = timedelta(days=(date.today() - REPLAY_DAY).days)  # REPLAY_DAY lands on today's date
    entries = load_entries(DB_PATH, PRODUCTION_TABLE, limit=None)
    history = [e for e in entries if e.registered_on_dt.date() < REPLAY_DAY]
    today = [e for e in entries if e.registered_on_dt.date() == REPLAY_DAY]

    async with aiohttp.ClientSession() as session:
        employees = load_employees(DB_PATH, EMPLOYEES_TABLE)
        await send(session, "PUT", "/employees", [asdict(e) for e in employees], headers)
        print(f"employees sent: {len(employees)}", flush=True)

        for i in range(0, len(history), PRELOAD_CHUNK):
            await send(session, "POST", "/events", [shifted(e, shift) for e in history[i:i + PRELOAD_CHUNK]], headers)
        print(f"history preloaded: {len(history)} events", flush=True)

        print(f"replaying {len(today)} events of {REPLAY_DAY} as {date.today()} at {REPLAY_SPEED}x", flush=True)
        replayed = await replay_to_http(session, today, REPLAY_SPEED, headers, shift)
        print(f"replay completed: {replayed} rows")


if __name__ == "__main__":
    asyncio.run(main())
