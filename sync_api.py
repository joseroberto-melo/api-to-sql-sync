#!/usr/bin/env python3
"""Incremental REST API -> SQLite synchronisation.

Pulls paginated JSON from a REST API (default: the public JSONPlaceholder demo
API), validates and flattens the records, and upserts them into SQLite.

* Pagination: follows ``_page`` / ``_limit`` until the last page.
* Resilience: retries 429/5xx responses and network errors with backoff.
* Incremental: each row stores a content hash. Re-running only inserts new rows,
  updates changed rows and leaves unchanged rows alone.
* Auditable: every run is recorded in ``sync_runs``; bad records are skipped
  and logged, never silently dropped.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger("api_sync")

DEFAULT_BASE_URL = "https://jsonplaceholder.typicode.com"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
RETRY_STATUSES = {429, 500, 502, 503, 504}

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    username TEXT,
    email TEXT NOT NULL,
    city TEXT,
    company TEXT,
    row_hash TEXT NOT NULL,
    synced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title TEXT NOT NULL,
    body TEXT,
    row_hash TEXT NOT NULL,
    synced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    details TEXT
);
CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# REST client
# --------------------------------------------------------------------------- #
class ApiClient:
    def __init__(self, base_url: str = DEFAULT_BASE_URL, session: requests.Session | None = None,
                 max_attempts: int = 4, backoff_seconds: float = 1.0, timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.timeout = timeout

    def get_json(self, path: str, params: dict | None = None):
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_error = "unknown error"
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                if response.status_code in RETRY_STATUSES:
                    last_error = f"HTTP {response.status_code}"
                else:
                    response.raise_for_status()  # other 4xx: retrying will not help
                    return response.json()
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = type(exc).__name__
            except ValueError as exc:  # body was not valid JSON
                last_error = f"invalid JSON ({exc})"
            if attempt < self.max_attempts:
                delay = self.backoff_seconds * 2 ** (attempt - 1)
                log.warning("GET %s failed (%s), attempt %d/%d, retrying in %.1fs",
                            path, last_error, attempt, self.max_attempts, delay)
                time.sleep(delay)
        raise RuntimeError(f"GET {path} failed after {self.max_attempts} attempts: {last_error}")

    def fetch_all(self, path: str, page_size: int = 25) -> list[dict]:
        """Walk every page of a ``_page``/``_limit`` paginated endpoint."""
        records: list[dict] = []
        page = 1
        while True:
            batch = self.get_json(path, {"_page": page, "_limit": page_size})
            if not isinstance(batch, list):
                raise RuntimeError(f"Expected a JSON list from {path}, got {type(batch).__name__}")
            records.extend(batch)
            log.info("Fetched %s page %d (%d records)", path, page, len(batch))
            if len(batch) < page_size:
                return records
            page += 1


# --------------------------------------------------------------------------- #
# Transform + validate
# --------------------------------------------------------------------------- #
def flatten_user(raw: dict) -> dict:
    return {
        "id": raw.get("id"),
        "name": (raw.get("name") or "").strip(),
        "username": (raw.get("username") or "").strip() or None,
        "email": (raw.get("email") or "").strip().lower(),
        "city": (raw.get("address") or {}).get("city"),
        "company": (raw.get("company") or {}).get("name"),
    }


def flatten_post(raw: dict) -> dict:
    return {
        "id": raw.get("id"),
        "user_id": raw.get("userId"),
        "title": (raw.get("title") or "").strip(),
        "body": (raw.get("body") or "").strip(),
    }


def user_problem(row: dict) -> str | None:
    if not isinstance(row["id"], int):
        return "missing id"
    if not row["name"]:
        return "missing name"
    if not EMAIL_RE.match(row["email"]):
        return "invalid email"
    return None


def post_problem(row: dict, known_user_ids: set[int]) -> str | None:
    if not isinstance(row["id"], int):
        return "missing id"
    if not row["title"]:
        return "missing title"
    if row["user_id"] not in known_user_ids:
        return f"unknown user_id {row['user_id']}"
    return None


def row_hash(row: dict) -> str:
    return hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


# --------------------------------------------------------------------------- #
# Load (incremental upsert)
# --------------------------------------------------------------------------- #
@dataclass
class Counts:
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped: int = 0
    problems: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in ("fetched", "inserted", "updated", "unchanged", "skipped")}


def upsert(conn: sqlite3.Connection, table: str, columns: list[str], rows: list[dict]) -> Counts:
    counts = Counts(fetched=len(rows))
    now = utc_now()
    existing = dict(conn.execute(f"SELECT id, row_hash FROM {table}"))
    for row in rows:
        digest = row_hash(row)
        values = [row[c] for c in columns]
        if row["id"] not in existing:
            marks = ",".join("?" * (len(columns) + 2))
            conn.execute(f"INSERT INTO {table} ({','.join(columns)},row_hash,synced_at) VALUES ({marks})",
                         [*values, digest, now])
            counts.inserted += 1
        elif existing[row["id"]] != digest:
            sets = ",".join(f"{c}=?" for c in columns if c != "id")
            conn.execute(f"UPDATE {table} SET {sets},row_hash=?,synced_at=? WHERE id=?",
                         [*[row[c] for c in columns if c != "id"], digest, now, row["id"]])
            counts.updated += 1
        else:
            counts.unchanged += 1
    return counts


def sync(client: ApiClient, db_path: str | Path, page_size: int = 25) -> dict:
    """Run one full sync. Everything is committed together, or nothing is."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    run_id = conn.execute("INSERT INTO sync_runs (started_at, status) VALUES (?, 'running')",
                          (utc_now(),)).lastrowid
    conn.commit()
    log.info("Sync run %d started | api=%s | db=%s", run_id, client.base_url, db_path)

    try:
        raw_users = client.fetch_all("users", page_size)
        raw_posts = client.fetch_all("posts", page_size)

        users, user_counts = [], Counts(fetched=len(raw_users))
        for raw in map(flatten_user, raw_users):
            if problem := user_problem(raw):
                user_counts.skipped += 1
                user_counts.problems.append(f"user {raw['id']}: {problem}")
                log.warning("Skipping user %s: %s", raw["id"], problem)
            else:
                users.append(raw)

        known = {u["id"] for u in users} | {r[0] for r in conn.execute("SELECT id FROM users")}
        posts, post_counts = [], Counts(fetched=len(raw_posts))
        for raw in map(flatten_post, raw_posts):
            if problem := post_problem(raw, known):
                post_counts.skipped += 1
                post_counts.problems.append(f"post {raw['id']}: {problem}")
                log.warning("Skipping post %s: %s", raw["id"], problem)
            else:
                posts.append(raw)

        u = upsert(conn, "users", ["id", "name", "username", "email", "city", "company"], users)
        p = upsert(conn, "posts", ["id", "user_id", "title", "body"], posts)
        for c, src in ((u, user_counts), (p, post_counts)):
            c.fetched, c.skipped, c.problems = src.fetched, src.skipped, src.problems

        details = {"users": u.as_dict(), "posts": p.as_dict()}
        conn.execute("UPDATE sync_runs SET finished_at=?, status='success', details=? WHERE run_id=?",
                     (utc_now(), json.dumps(details), run_id))
        conn.commit()
        log.info("Sync run %d finished | users %s | posts %s", run_id, u.as_dict(), p.as_dict())
        return {"run_id": run_id, **details}
    except Exception as exc:
        conn.rollback()
        conn.execute("UPDATE sync_runs SET finished_at=?, status='failed', details=? WHERE run_id=?",
                     (utc_now(), json.dumps({"error": str(exc)}), run_id))
        conn.commit()
        log.error("Sync run %d failed: %s", run_id, exc)
        raise
    finally:
        conn.close()


def top_authors(db_path: str | Path, limit: int = 5) -> list[tuple]:
    """Example report: authors with the most posts (a JOIN + GROUP BY)."""
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            """SELECT u.name, u.company, COUNT(p.id) AS posts
               FROM users u LEFT JOIN posts p ON p.user_id = u.id
               GROUP BY u.id ORDER BY posts DESC, u.name LIMIT ?""", (limit,)).fetchall()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--db", default="data/sync.db")
    parser.add_argument("--page-size", type=int, default=25)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    try:
        result = sync(ApiClient(args.base_url), args.db, args.page_size)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("\n=== Sync result ===")
    for resource in ("users", "posts"):
        print(f"{resource:6}", result[resource])
    print("\nTop authors by number of posts:")
    for name, company, posts in top_authors(args.db):
        print(f"  {posts:3}  {name} ({company})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
