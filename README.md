# Demo ETL

```
 this PC                       docker network "etl"
┌─────────────────────┐       ┌────────────────────────────────────────────────────────────┐
│ production_replay.py│ HTTP  │ receiver :8001 ◄─pull── etl ──load──► bigquery (emulator)   │
│  PUT /employees     ├──────►│ (API key, SQLite     (tidy +        goccy/bigquery-emulator │
│  POST /events       │ :8001 │  buffer)              resolve packer)        │              │
└─────────────────────┘       │                              dashboard :8050 ◄┘              │
                              └──────────────────────────────────┬─────────────────────────┘
                    browser ── http://localhost:8050 ◄────────────┘
```

| Container | Role |
|---|---|
| `receiver` | Data input (only published port besides the dashboard). Validates events and buffers them in SQLite. |
| `etl` | Pulls batches of up to 5,000 events plus the employee list from the receiver. Tidies them (normalized codes, UTC timestamps, `event_date`, unknown terminal/line → `UNKNOWN`), resolves each packer's name and group, and loads one `events` table into BigQuery. |
| `bigquery` | BigQuery emulator (the LocalStack equivalent for GCP). Data persists in the `bqdata` volume. |
| `dashboard` | Dash app. Reads BigQuery and offers filters by date range, production line and presentation. |

## Data

`data_generator/metrics_anonymized.sqlite` holds 24 days of packing events (2024-06-14 → 2024-07-10).
The generator shifts all dates so 2024-07-10 becomes **today**. It preloads the 23 earlier days in one go,
then replays today at 10x (`REPLAY_SPEED`, `REPLAY_DAY` env vars).
Today starts with 103 events at 00:00 and resumes at 08:00, which is about 48 minutes into a 10x replay.

## Run

Secrets are in `.env` (`INGEST_TOKEN`, random, git-ignored).

```bash
docker compose down -v && docker compose up -d --build           # fresh stack
cd data_generator && ../.venv/bin/python production_replay.py   # preload + live replay
```

Open http://localhost:8050.

Rerunning the generator re-sends the history, so reset the stack first (`down -v`).

## BigQuery table `metrics.events`

`buffer_id, event_id` (ETL cursor) · `registered_on` TIMESTAMP · `event_date` DATE · `terminal` · `production_line` ·
`presentation` · `unique_n` · `packer_id` · `packer_name` · `packer_group` · `loaded_at`

## Tests

Need a local emulator binary (https://github.com/goccy/bigquery-emulator/releases):

```bash
bigquery-emulator --project=demo-etl --dataset=metrics --port=19050 &
BQ_PROJECT=demo-etl BQ_DATASET=metrics BQ_ENDPOINT=http://localhost:19050 .venv/bin/python -m pytest -q tests
```

## Real BigQuery

Remove `BQ_ENDPOINT` and set `BQ_PROJECT`/`BQ_DATASET` to a real project. The services then use Google default credentials.
