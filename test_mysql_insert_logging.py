import importlib
import logging
import sys
from datetime import datetime, timezone
from unittest import mock

import pytest


def _load_index():
    with mock.patch("pymongo.MongoClient", return_value=mock.MagicMock()):
        sys.modules.pop("index", None)
        return importlib.import_module("index")


index = _load_index()

pytestmark = pytest.mark.skipif(index.pymysql is None, reason="PyMySQL not installed")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        if sql.lstrip().upper().startswith("INSERT"):
            if self.conn.duplicate:
                raise index.pymysql.err.IntegrityError(1062, "Duplicate entry")
            self.rowcount = 1
            self.conn.inserts += 1
        elif "COUNT(*)" in sql:
            self.conn.count_calls += 1
            if self.conn.count_raises:
                raise RuntimeError("count boom")
            self._row = (self.conn.inserts,)

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self):
        self.duplicate = False
        self.count_raises = False
        self.inserts = 0
        self.count_calls = 0

    def cursor(self):
        return FakeCursor(self)


@pytest.fixture
def conn(monkeypatch):
    c = FakeConn()
    monkeypatch.setattr(index, "_get_mysql_conn", lambda: c)
    monkeypatch.setattr(index, "_ensure_mysql_table", lambda _conn: None)
    monkeypatch.setattr(index, "_mysql_insert_count", 0)
    monkeypatch.setattr(index, "MYSQL_DATABASE", "testdb")
    return c


def _doc(n=1, embedding=True):
    now = datetime.now(timezone.utc)
    return {
        "message_id": f"reddit_serp_{n}", "platform": "reddit", "post_url": f"https://x/{n}",
        "text": "secret post body", "username": "u", "subreddit_or_channel": "s",
        "posted_at": None, "fetched_at": now, "google_rank": 3, "search_keyword": "kw",
        "client_id": "c", "created_at": now,
        "embedding": [0.1] * 1536 if embedding else None,
    }


def test_successful_insert_logs_inserted(conn, caplog):
    caplog.set_level(logging.INFO, logger="flintel")
    assert index._save_to_mysql(_doc(7)) is True
    msgs = [r.getMessage() for r in caplog.records if "[MYSQL] INSERTED" in r.getMessage()]
    assert len(msgs) == 1
    assert "message_id=reddit_serp_7" in msgs[0]
    assert "db=testdb" in msgs[0]
    assert "table=flintel_signals" in msgs[0]
    assert "embedding_bytes=6144" in msgs[0]
    assert "secret post body" not in msgs[0]


def test_duplicate_returns_false_and_no_inserted_log(conn, caplog):
    caplog.set_level(logging.INFO, logger="flintel")
    conn.duplicate = True
    assert index._save_to_mysql(_doc(1)) is False
    assert not any("[MYSQL] INSERTED" in r.getMessage() for r in caplog.records)


def test_total_logged_exactly_once_after_25_inserts(conn, caplog):
    caplog.set_level(logging.INFO, logger="flintel")
    for i in range(25):
        assert index._save_to_mysql(_doc(i)) is True
    totals = [r.getMessage() for r in caplog.records if "[MYSQL] TOTAL rows" in r.getMessage()]
    assert len(totals) == 1
    assert "= 25 (db=testdb)" in totals[0]
    assert conn.count_calls == 1


def test_count_failure_does_not_fail_insert(conn, caplog):
    caplog.set_level(logging.INFO, logger="flintel")
    conn.count_raises = True
    results = [index._save_to_mysql(_doc(i)) for i in range(25)]
    assert all(r is True for r in results)
    assert any(r.levelno == logging.WARNING and "COUNT(*) failed" in r.getMessage() for r in caplog.records)
