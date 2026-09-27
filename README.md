# Resilient API Ingestion

A production-style pipeline that pulls data from an **unreliable, rate-limited API**, lands it safely, and loads it into SQL, ending **exactly** in sync with the source, and proving it.

It is a clean-room rebuild of the ingestion platform I built for a franchise healthcare brand. That brand's operating platform only exposed data through an API with tight limits, so I staged everything on a self-built SQL Server with Python-automated pipelines, which fed analytics and franchisee reporting. Everything here runs against a local **mock API on synthetic data**.

## The problem

Clinic and salon platforms often expose data only through APIs that fight you:

| API behavior | Why it hurts |
| --- | --- |
| Rate limits (HTTP 429) | Naive scripts get throttled, then banned |
| Random 5xx errors and slow responses | Nightly jobs die halfway through |
| Tokens that expire mid-run, sometimes earlier than advertised | Long backfills fail after 20 minutes |
| Report endpoints that only accept 7-day date ranges | A six-month backfill needs dozens of windowed calls |
| No "changed since" filter on bookings | A booking made last week that completes today is invisible to a date-based pull |
| Unstable pagination | The same record shows up on two pages |
| New fields appearing without notice | Rigid loaders crash or silently drop data |

## What the pipeline does

```mermaid
flowchart LR
    API[(Unreliable API)] -->|rate-limited, retried,<br/>token-refreshing client| X[Extract<br/>7-day windows, cursors,<br/>dedupe, completeness check]
    X --> L[Landing zone<br/>raw JSONL + manifest<br/>per run]
    L --> V{Validate}
    V -- bad records --> R[(ingest_rejects)]
    V -- new fields --> D[(schema_drift_events<br/>+ extra_json)]
    V --> U[Upsert into SQL<br/>last-write-wins<br/>on updated_at]
    U --> C[Post-load checks<br/>completeness, freshness,<br/>orphans]
    C --> A[Alerts<br/>webhook + run log]
```

- **Resilient client.** A client-side rate limiter stays under the API's limit and honors `Retry-After` when it doesn't. Server errors and timeouts get exponential backoff with jitter, while rate limits use a separate budget, because "slow down" isn't "broken." Tokens refresh before they expire, and once more if the API kills them early. Errors that retrying can't fix fail fast.
- **Three extraction modes.** Small reference tables are pulled whole. Endpoints with a change filter use a watermark with a clock-skew overlap. Date-only report endpoints are split into 7-day windows, with a **lookback window re-pulled every run** to catch late changes.
- **Landing before loading.** Raw pages are written to disk with a manifest before the database is touched. That means a load can be re-run or audited without calling the API again. The extractor also checks that the number of unique records received equals the total the API reported.
- **Safe loading.** Upserts are last-write-wins on the source's own `updated_at`, so a stale copy from an overlapping page never overwrites a newer one. Bad records go to a rejects table with a reason instead of failing the run. Unknown fields are kept in `extra_json`, logged once, and raise one alert.
- **Isolation and recovery.** Each endpoint runs independently. If one fails, the others still load, its watermark doesn't advance, and the next run picks up where it left off.
- **Everything is measured.** Every run records pages, records inserted, updated and unchanged, rejects, requests, retries, 429s, 5xx errors, timeouts and token refreshes in `ingest_runs`.

## Proof: exact sync under chaos

`python demo.py` starts the mock API with chaos turned on:

- a 12 requests/second limit
- 5% server errors
- 2% responses slower than the client timeout
- overlapping pages
- tokens that die 10 seconds before they claim to
- a new field appearing mid-history

It then runs a six-month backfill, simulates three days passing, runs an incremental load, and compares the database to the API record by record.

In the [recorded run](docs/demo_run.md):

- **Backfill:** about 19,700 records across 12 centers. The pipeline survived 19 server errors, 3 timeouts, rate limiting and a mid-run token expiry, and dropped duplicate records from overlapping pages.
- **Three days pass:** 147 bookings resolved into completed, cancelled or no-show; 14 visits were discounted after the fact; 146 new bookings arrived.
- **Incremental run:** 36 requests in 8 seconds picked up every one of those changes.
- **Result:** the database matches the API exactly, with 0 missing, 0 extra and 0 mismatched records.

Numbers vary slightly between runs because the chaos is timing-dependent. The exact match doesn't.

## What's tested

15 tests run against the mock API, with no external services:

- **Client:** gives up after its retry budget; honors `Retry-After` and still succeeds; refreshes a token that dies early; fails fast on bad requests and bad credentials.
- **Extraction:** date windows never exceed the cap and cover every day; overlapping pages are deduplicated and the result is complete; raw landing keeps exactly what the API sent.
- **Loading:** a stale record never overwrites a newer one; bad records are rejected with a reason; schema drift is stored and alerted only once; reference rows update only when content changes.
- **Pipeline:** backfill and incremental runs are exact under heavy chaos; a rerun with no changes writes nothing; one failing endpoint doesn't block the others and recovers on the next run.
- **The lookback trade-off:** a correction older than the lookback window is missed by a normal run and caught by a deep refresh. That's why lookback is a setting, and why production schedules a weekly deep refresh.

## Quick start

```bash
git clone https://github.com/<quanti-nomad>/resilient-api-ingestion.git
cd resilient-api-ingestion
pip install -r requirements.txt

pytest -q                 # about 30 seconds
python demo.py --quick    # a few seconds
python demo.py            # full demo, about a minute; writes docs/demo_run.md
```

Against a real API:

```bash
export API_CLIENT_ID=... API_CLIENT_SECRET=...
export ALERT_WEBHOOK_URL=...          # optional: Slack or Teams incoming webhook
python -m ingest --base-url https://api.example.com \
  --db "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+18+for+SQL+Server"
```

The loader uses SQLAlchemy, so the same code targets SQLite (tests) and Microsoft SQL Server (production). The command exits with code 1 if any endpoint or check fails, so any scheduler can flag the run: cron, Windows Task Scheduler, SQL Server Agent or Airflow. The test suite runs on SQLite; point `--db` at a SQL Server instance to run it there.

## Project structure

```
ingest/client.py      rate limiter, retries, backoff, Retry-After, token refresh
ingest/extract.py     endpoint modes, 7-day windows, pagination, landing, completeness
ingest/load.py        typed tables, validation, rejects, drift, last-write-wins upserts
ingest/checks.py      completeness, freshness and orphan checks
ingest/pipeline.py    incremental scope, per-endpoint isolation, run log, alerts
ingest/verify.py      record-by-record comparison with the source
mock_api/             the unreliable API and its ground-truth data
demo.py               end-to-end demo that writes docs/demo_run.md
tests/                client, extraction, loading and pipeline tests
```

## Design decisions

- **Land raw first.** The database is never written from a half-finished extraction, and every load is replayable.
- **Trust the source's timestamps, not arrival order.** Pages overlap and retries reorder things; `updated_at` decides.
- **Count what you received against what the API promised.** A silent short read is the most dangerous failure.
- **Make the lookback window an explicit trade-off.** Short keeps nightly runs fast; a periodic deep refresh catches late corrections.
- **Alert on what a person must act on.** Failures, rejects, stale data and new fields alert; routine retries only get logged.

## About

Built by **Miguel Tomada**, Data, Analytics & AI leader. I build data platforms, reporting governance and AI automation for multi-location healthcare and franchise organizations.
