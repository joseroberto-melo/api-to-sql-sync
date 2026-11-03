"""Tests run against a real local HTTP server (no mocking of `requests`).

The fake server mimics JSONPlaceholder's `_page`/`_limit` pagination and can be
told to fail the next N requests, so retry behaviour is exercised over real HTTP.
"""
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

import sync_api


class FakeApi:
    def __init__(self):
        self.users = [{"id": i, "name": f"User {i}", "username": f"user{i}",
                       "email": f"user{i}@example.com", "address": {"city": "Testville"},
                       "company": {"name": f"Company {i}"}} for i in range(1, 6)]
        self.posts = [{"id": i, "userId": (i % 5) + 1, "title": f"Post {i}", "body": f"Body {i}"}
                      for i in range(1, 24)]
        self.fail_next = 0       # number of upcoming requests that return 503
        self.request_count = 0


@pytest.fixture
def api():
    state = FakeApi()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state.request_count += 1
            if state.fail_next > 0:
                state.fail_next -= 1
                self.send_response(503)
                self.end_headers()
                return
            url = urlparse(self.path)
            data = {"/users": state.users, "/posts": state.posts}.get(url.path)
            if data is None:
                self.send_response(404)
                self.end_headers()
                return
            q = parse_qs(url.query)
            page, limit = int(q["_page"][0]), int(q["_limit"][0])
            body = json.dumps(data[(page - 1) * limit: page * limit]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # keep test output quiet
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield state
    server.shutdown()


def client(api, **kwargs):
    return sync_api.ApiClient(api.url, backoff_seconds=0, **kwargs)


def table_count(db, table):
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_pagination_collects_every_record(api):
    posts = client(api).fetch_all("posts", page_size=10)   # 23 records -> 3 pages
    assert len(posts) == 23


def test_first_sync_inserts_everything(api, tmp_path):
    db = tmp_path / "sync.db"
    result = sync_api.sync(client(api), db, page_size=10)
    assert result["users"] == {"fetched": 5, "inserted": 5, "updated": 0, "unchanged": 0, "skipped": 0}
    assert result["posts"]["inserted"] == 23
    assert table_count(db, "posts") == 23


def test_second_sync_changes_nothing(api, tmp_path):
    db = tmp_path / "sync.db"
    sync_api.sync(client(api), db)
    again = sync_api.sync(client(api), db)
    assert again["users"]["unchanged"] == 5 and again["posts"]["unchanged"] == 23
    assert again["posts"]["inserted"] == again["posts"]["updated"] == 0


def test_incremental_sync_inserts_new_and_updates_changed(api, tmp_path):
    db = tmp_path / "sync.db"
    sync_api.sync(client(api), db)
    api.posts[0]["title"] = "Edited title"
    api.posts.append({"id": 24, "userId": 1, "title": "Brand new", "body": "x"})
    result = sync_api.sync(client(api), db)
    assert (result["posts"]["inserted"], result["posts"]["updated"], result["posts"]["unchanged"]) == (1, 1, 22)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT title FROM posts WHERE id=1").fetchone()[0] == "Edited title"


def test_invalid_records_are_skipped_not_loaded(api, tmp_path):
    api.users[0]["email"] = "not-an-email"
    api.posts.append({"id": 99, "userId": 777, "title": "Orphan", "body": ""})
    db = tmp_path / "sync.db"
    result = sync_api.sync(client(api), db)
    assert result["users"]["skipped"] == 1 and result["users"]["inserted"] == 4
    # user 1 was rejected, so its posts (and the orphan) are skipped too
    assert result["posts"]["skipped"] > 0
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM posts WHERE user_id IN (1, 777)").fetchone()[0] == 0


def test_transient_errors_are_retried(api, tmp_path):
    api.fail_next = 2
    result = sync_api.sync(client(api), tmp_path / "sync.db")
    assert result["users"]["inserted"] == 5


def test_permanent_failure_rolls_back_and_is_logged(api, tmp_path):
    db = tmp_path / "sync.db"
    api.fail_next = 10_000
    with pytest.raises(RuntimeError, match="failed after 4 attempts"):
        sync_api.sync(client(api), db)
    assert table_count(db, "users") == 0
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT status FROM sync_runs").fetchone()[0] == "failed"


def test_top_authors_report(api, tmp_path):
    db = tmp_path / "sync.db"
    sync_api.sync(client(api), db)
    rows = sync_api.top_authors(db, limit=2)
    assert len(rows) == 2 and rows[0][2] >= rows[1][2]
