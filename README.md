# API → SQL Sync

An incremental synchronisation job: it pulls paginated data from a REST API, validates it, and keeps a local SQLite database up to date. It only inserts new rows, updates rows that changed, and leaves everything else alone.

It uses the free public demo API [JSONPlaceholder](https://jsonplaceholder.typicode.com) (users and posts). No account or API key is needed.

## The problem

Business systems expose data through REST APIs, but reporting teams need it in a database they can query with SQL. A naive script that re-downloads and re-inserts everything is slow, creates duplicates, breaks on the first network hiccup and leaves no record of what happened.

## What it does

```
REST API  (users, posts; paginated)
   │
   ▼
 1. Fetch every page (retry on 429/5xx/timeouts, exponential backoff)
 2. Flatten nested JSON (address.city, company.name)
 3. Validate (missing id/name, bad e-mail, posts pointing to unknown users)
 4. Incremental upsert: content hash per row → insert / update / unchanged
 5. Commit everything together, or roll back and record the failure
   │
   ▼
data/sync.db   tables: users, posts, sync_runs (audit trail)
```

## Quick start

Requires Python 3.10+.

```bash
git clone <this-repo-url>
cd api-to-sql-sync

python -m venv .venv
# Windows:      .venv\Scripts\activate
# macOS/Linux:  source .venv/bin/activate

pip install -r requirements.txt

python sync_api.py             # first run: inserts everything
python sync_api.py             # second run: everything reported as unchanged
```

Each run prints how many rows were inserted, updated, unchanged and skipped, plus a sample report. More queries to try are in [`queries.sql`](queries.sql).

Options: `--base-url`, `--db` (default `data/sync.db`) and `--page-size`.

Run the tests with `pytest`.

## Design notes

- **Incremental by content hash.** Each row stores a SHA-256 of its normalised content. Unchanged rows cost one comparison and no write, and the same run can be repeated safely (idempotent).
- **Real HTTP in the tests.** The test suite starts a local HTTP server that mimics the API's pagination and can fail on demand. It proves, over real HTTP, that retries work, that a second run changes nothing, that edits and new rows are picked up, and that a permanent failure rolls back.
- **All-or-nothing.** Users and posts are written in one transaction. If anything fails after retries, nothing is committed and the failure is recorded in `sync_runs`.
- **Bad data is skipped and logged**, never loaded and never silently lost.
- **Does not retry what cannot succeed.** Network errors, timeouts, 429 and 5xx are retried. Other 4xx errors fail immediately.

## Known simplifications

- Rows deleted at the source are not removed from the database. A real job would need a soft-delete or reconciliation step.
- It re-reads the whole dataset each run. APIs that support `updated_since` filters would allow truly incremental fetching.
- SQLite keeps the demo dependency-free. The same SQL approach applies to PostgreSQL or SQL Server.

## Project structure

```
sync_api.py          client, validation, upsert, audit trail, CLI
queries.sql          example reporting queries
tests/test_sync.py   tests against a local fake API server
```

## About

Built by José Roberto Melo, a Python and RPA developer focused on business process automation (Python, UiPath, Power Automate, Automation Anywhere, REST/SOAP integrations, OCR).
