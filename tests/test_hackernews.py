"""
tests/test_hackernews.py
------------------------
Hacker News module ke hermetic tests (test_mysql_insert_logging.py wala style):
  * pymongo.MongoClient mock karke `index` import hota hai - koi real Mongo nahi.
  * requests.get monkeypatch - koi real network nahi.
  * hackernews.database.* functions fake - koi DB call nahi.
"""

import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
import requests

# Repo root ko sys.path mein daalo (tests/ ke andar se `index` / `hackernews` import ke liye).
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_index():
    with mock.patch("pymongo.MongoClient", return_value=mock.MagicMock()):
        sys.modules.pop("index", None)
        return importlib.import_module("index")


index = _load_index()

from hackernews import config, database, fetcher  # noqa: E402  (index ke baad import)


# ─────────────────────────────────────────────────────────────────────────────
# helpers / fixtures
# ─────────────────────────────────────────────────────────────────────────────

class _Stop(BaseException):
    """Always-on loop ko test mein todne ke liye. BaseException hai, isliye loop ka
    `except Exception` ise nahi pakadta."""


class FakeResp:
    def __init__(self, payload=None, ok=True):
        self._payload = payload if payload is not None else {}
        self._ok = ok

    def raise_for_status(self):
        if not self._ok:
            raise requests.HTTPError("500 Server Error")

    def json(self):
        return self._payload


def story_hit(oid, title="", story_text=None, author="alice", ts=1700000000):
    return {"objectID": str(oid), "title": title, "story_text": story_text,
            "author": author, "created_at_i": ts}


def comment_hit(oid, comment_text, author="bob", ts=1700000000):
    return {"objectID": str(oid), "comment_text": comment_text,
            "author": author, "created_at_i": ts}


def make_item(text, ts, oid="1", username="alice", post_url=None, keyword="crm tool"):
    """fetch_hackernews_page ke output jaisa item (checkpoint key `_created_at_i` ke saath)."""
    return {
        "message_id": f"hackernews_{oid}", "platform": "hackernews", "text": text,
        "username": username, "subreddit_or_channel": "",
        "post_url": post_url or f"https://news.ycombinator.com/item?id={oid}",
        "posted_at": None, "search_keyword": keyword, "google_rank": None,
        "_created_at_i": ts,
    }


@pytest.fixture(autouse=True)
def hermetic(monkeypatch):
    """Har test: koi real sleep nahi, .env dobara load nahi hota, config defaults saaf."""
    monkeypatch.setattr(fetcher, "time", SimpleNamespace(sleep=lambda *_a, **_k: None))
    monkeypatch.setattr(config, "load_dotenv", lambda *a, **k: False)
    for name in ("HACKERNEWS_ENABLED", "HN_FETCH_STORIES", "HN_FETCH_COMMENTS", "HN_STRICT_MATCH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config, "HN_MAX_PAGES", 5)
    monkeypatch.setattr(config, "HN_EXACT_PHRASE", False)
    monkeypatch.setattr(config, "HN_KEYWORD_GAP_SECONDS", 0.0)
    monkeypatch.setattr(config, "HN_POLL_INTERVAL_SECONDS", 0)


@pytest.fixture
def http(monkeypatch):
    """requests.get ko fake karta hai. Test `http.handler = fn(url, params) -> FakeResp`
    set karta hai; saari calls `http.calls` mein record hoti hain."""
    state = SimpleNamespace(calls=[], handler=lambda url, params: FakeResp({"hits": [], "nbPages": 0}))

    def fake_get(url, params=None, headers=None, timeout=None, **_kw):
        state.calls.append({"url": url, "params": dict(params or {}), "headers": headers})
        return state.handler(url, params or {})

    monkeypatch.setattr(fetcher.requests, "get", fake_get)
    return state


@pytest.fixture
def state_db(monkeypatch):
    """database.get_last_seen_ts / set_last_seen_ts ko fake karta hai."""
    st = SimpleNamespace(last_seen=None, set_calls=[])
    monkeypatch.setattr(database, "get_last_seen_ts", lambda kw, kind: st.last_seen)
    monkeypatch.setattr(database, "set_last_seen_ts",
                        lambda kw, kind, ts: st.set_calls.append((kw, kind, ts)))
    return st


# ─────────────────────────────────────────────────────────────────────────────
# test_1 / test_2 / test_3 — item conversion
# ─────────────────────────────────────────────────────────────────────────────

def test_1_story_hit_item_shape(http):
    http.handler = lambda url, params: FakeResp({
        "hits": [story_hit(123, title="Best CRM tool",
                           story_text="<p>We use <a href=\"http://x\">HubSpot</a></p>",
                           author="bob", ts=1700000000)],
        "nbPages": 1,
    })

    items, nb_pages = fetcher.fetch_hackernews_page("crm tool", "story", None, 0)

    assert nb_pages == 1
    assert len(items) == 1
    item = items[0]
    assert item["message_id"] == "hackernews_123"
    assert item["platform"] == "hackernews"
    assert item["text"] == "Best CRM tool\n\nWe use HubSpot"
    assert item["username"] == "bob"
    assert item["post_url"] == "https://news.ycombinator.com/item?id=123"
    assert item["posted_at"] == datetime.fromtimestamp(1700000000, tz=timezone.utc).isoformat()
    assert item["posted_at"].endswith("+00:00")
    assert item["google_rank"] is None
    assert item["search_keyword"] == "crm tool"


def test_2_comment_hit_strips_tags(http):
    http.handler = lambda url, params: FakeResp({
        "hits": [comment_hit(456, "<p>Try <a href=\"http://x\">this</a> tool</p><p>Second</p>")],
        "nbPages": 1,
    })

    items, _ = fetcher.fetch_hackernews_page("crm tool", "comment", None, 0)

    assert len(items) == 1
    item = items[0]
    assert item["message_id"] == "hackernews_comment_456"
    assert item["platform"] == "hackernews_comment"
    assert "<" not in item["text"] and ">" not in item["text"]
    assert item["text"] == "Try this tool Second"
    assert item["post_url"] == "https://news.ycombinator.com/item?id=456"


@pytest.mark.parametrize("failure", ["timeout", "http_error"])
def test_3_api_error_returns_empty_without_raising(http, failure):
    if failure == "timeout":
        def boom(url, params):
            raise requests.Timeout("timed out")
        http.handler = boom
    else:
        http.handler = lambda url, params: FakeResp({}, ok=False)

    result = fetcher.fetch_hackernews_page("crm tool", "story", None, 0)

    assert result == ([], 0)


# ─────────────────────────────────────────────────────────────────────────────
# test_4 — keyword_in_post
# ─────────────────────────────────────────────────────────────────────────────

def test_4_keyword_in_post():
    # poora phrase
    assert fetcher.keyword_in_post("residential graphic designer",
                                   {"text": "We need a residential graphic designer today"})
    # saare lafz alag-alag (phrase nahi, lekin har significant word maujood)
    assert fetcher.keyword_in_post("residential graphic designer",
                                   {"text": "Looking for a designer. Graphic work, residential projects."})
    # ek lafz missing => False
    assert not fetcher.keyword_in_post("residential graphic designer",
                                       {"text": "residential designer only"})
    # case-insensitive (keyword bhi, text bhi)
    assert fetcher.keyword_in_post("HVAC Technician", {"text": "hvac TECHNICIAN wanted"})
    assert fetcher.keyword_in_post("hvac technician", {"text": "Need an HVAC Technician"})
    # khali text
    assert not fetcher.keyword_in_post("crm tool", {"text": ""})
    assert not fetcher.keyword_in_post("crm tool", {})


# ─────────────────────────────────────────────────────────────────────────────
# test_5 / test_6 — strict match + checkpoint
# ─────────────────────────────────────────────────────────────────────────────

def _two_items():
    good = make_item("Anyone tried this crm tool?", ts=100, oid="1")
    # keyword sirf URL / author mein hai, text mein nahi
    bad = make_item("Totally unrelated text", ts=200, oid="2",
                    username="crm tool", post_url="https://example.com/crm-tool")
    return good, bad


def test_5_strict_match_on_and_off(monkeypatch, state_db):
    good, bad = _two_items()
    monkeypatch.setattr(fetcher, "fetch_hackernews_page",
                        lambda kw, kind, since, page: ([good, bad], 1))

    # strict ON: sirf text-match wala item save_fn tak jata hai
    monkeypatch.setattr(config, "is_hn_strict_match_enabled", lambda: True)
    saved = []
    count = fetcher.process_keyword_kind("crm tool", "story",
                                         lambda it: saved.append(it["message_id"]) or True, None)
    assert saved == ["hackernews_1"]
    assert count == 1

    # strict OFF: sab save_fn ko jate hain
    monkeypatch.setattr(config, "is_hn_strict_match_enabled", lambda: False)
    saved.clear()
    count = fetcher.process_keyword_kind("crm tool", "story",
                                         lambda it: saved.append(it["message_id"]) or True, None)
    assert saved == ["hackernews_1", "hackernews_2"]
    assert count == 2


def test_6_checkpoint_advances_past_skipped_posts(monkeypatch, state_db):
    items = [
        make_item("Anyone tried this crm tool?", ts=100, oid="1"),   # match
        make_item("nothing relevant here", ts=300, oid="2"),         # skip, sab se naya
        make_item("also not relevant", ts=200, oid="3"),             # skip
    ]
    monkeypatch.setattr(fetcher, "fetch_hackernews_page",
                        lambda kw, kind, since, page: (items, 1))
    monkeypatch.setattr(config, "is_hn_strict_match_enabled", lambda: True)

    saved = []
    fetcher.process_keyword_kind("crm tool", "story", lambda it: saved.append(it) or True, None)

    assert len(saved) == 1                                  # sirf match save hua
    assert state_db.set_calls == [("crm tool", "story", 300)]  # checkpoint skip hui posts ke baad bhi max par


# ─────────────────────────────────────────────────────────────────────────────
# test_7 — pagination
# ─────────────────────────────────────────────────────────────────────────────

def _paged_handler(http, nb_pages=3):
    counter = {"n": 0}

    def handler(url, params):
        counter["n"] += 1
        hit = story_hit(1000 + counter["n"], title=f"crm tool post {counter['n']}", ts=1700000000 + counter["n"])
        return FakeResp({"hits": [hit], "nbPages": nb_pages})

    http.handler = handler


def test_7_pagination_and_max_pages(monkeypatch, http, state_db):
    _paged_handler(http, nb_pages=3)

    # nbPages=3, HN_MAX_PAGES=5 -> 3 pages
    monkeypatch.setattr(config, "HN_MAX_PAGES", 5)
    fetcher.process_keyword_kind("crm tool", "story", lambda it: True, None)
    assert [c["params"]["page"] for c in http.calls] == [0, 1, 2]

    # HN_MAX_PAGES=2 -> 2 pe ruk jaye
    http.calls.clear()
    _paged_handler(http, nb_pages=3)
    monkeypatch.setattr(config, "HN_MAX_PAGES", 2)
    fetcher.process_keyword_kind("crm tool", "story", lambda it: True, None)
    assert [c["params"]["page"] for c in http.calls] == [0, 1]


# ─────────────────────────────────────────────────────────────────────────────
# test_8 — ek keyword fail ho to baqi chalte rahein
# ─────────────────────────────────────────────────────────────────────────────

def test_8_one_keyword_failing_does_not_stop_others(monkeypatch, http, state_db):
    monkeypatch.setattr(database, "load_hn_keywords",
                        lambda: ["alpha tool", "bad tool", "gamma tool"])
    monkeypatch.setattr(config, "is_hn_comments_enabled", lambda: False)  # sirf stories

    def handler(url, params):
        if params["query"] == "bad tool":
            raise requests.ConnectionError("network down")
        return FakeResp({"hits": [story_hit(abs(hash(params["query"])) % 10_000,
                                            title=f"{params['query']} found")],
                         "nbPages": 1})

    http.handler = handler

    saved_keywords = []
    total = fetcher.run_hackernews_cycle(lambda it: saved_keywords.append(it["search_keyword"]) or True)

    assert saved_keywords == ["alpha tool", "gamma tool"]
    assert total == 2
    assert [c["params"]["query"] for c in http.calls] == ["alpha tool", "bad tool", "gamma tool"]


# ─────────────────────────────────────────────────────────────────────────────
# test_9 — loop switches
# ─────────────────────────────────────────────────────────────────────────────

def _run_loop_once(monkeypatch, sinks_ok):
    """run_hackernews_loop ko ek iteration chala kar _Stop se tod deta hai
    (pehli time.sleep par). Return: kitni dafa cycle chala."""
    cycles = []
    monkeypatch.setattr(fetcher, "run_hackernews_cycle", lambda save_fn: cycles.append(1) or 0)

    def stop(*_a, **_k):
        raise _Stop()

    monkeypatch.setattr(fetcher, "time", SimpleNamespace(sleep=stop))
    with pytest.raises(_Stop):
        fetcher.run_hackernews_loop(lambda it: True, sinks_ok)
    return len(cycles)


def test_9_loop_respects_enabled_and_sinks(monkeypatch, http, state_db):
    # HACKERNEWS_ENABLED=false -> cycle nahi
    monkeypatch.setenv("HACKERNEWS_ENABLED", "false")
    assert _run_loop_once(monkeypatch, lambda: True) == 0

    # enabled lekin sinks_ok False -> cycle nahi
    monkeypatch.setenv("HACKERNEWS_ENABLED", "true")
    assert _run_loop_once(monkeypatch, lambda: False) == 0

    # in dono surat mein na fetch hua, na checkpoint aage badha
    assert http.calls == []
    assert state_db.set_calls == []

    # sanity: dono ok ho to cycle chalta hai
    assert _run_loop_once(monkeypatch, lambda: True) == 1


def test_9b_loop_skip_does_not_touch_checkpoint_with_real_cycle(monkeypatch, http, state_db):
    """Asli run_hackernews_cycle ke saath: sinks_ok False par keywords bhi load nahi hote."""
    loaded = []
    monkeypatch.setattr(database, "load_hn_keywords", lambda: loaded.append(1) or ["crm tool"])

    def stop(*_a, **_k):
        raise _Stop()

    monkeypatch.setattr(fetcher, "time", SimpleNamespace(sleep=stop))
    with pytest.raises(_Stop):
        fetcher.run_hackernews_loop(lambda it: True, lambda: False)

    assert loaded == []
    assert http.calls == []
    assert state_db.set_calls == []


# ─────────────────────────────────────────────────────────────────────────────
# test_10 — set_last_seen_ts
# ─────────────────────────────────────────────────────────────────────────────

class FakeStateColl:
    """flintel_hn_state ka chhota fake jo `$max` + upsert ka asal behaviour nibhata hai."""

    def __init__(self):
        self.store = {}
        self.calls = []

    def update_one(self, flt, update, upsert=False):
        self.calls.append((flt, update, upsert))
        key = (flt["keyword"], flt["kind"])
        for field, val in update["$max"].items():
            cur = self.store.get(key)
            self.store[key] = val if cur is None else max(cur, val)


def test_10_set_last_seen_ts_keeps_max_and_never_raises(monkeypatch):
    coll = FakeStateColl()
    monkeypatch.setattr(database, "_get_state_collection", lambda: coll)

    database.set_last_seen_ts("crm tool", "story", 500)
    database.set_last_seen_ts("crm tool", "story", 300)   # purana ts - wapas nahi aana chahiye
    database.set_last_seen_ts("crm tool", "story", "700")  # int mein cast hota hai

    assert coll.store[("crm tool", "story")] == 700
    for flt, update, upsert in coll.calls:
        assert flt == {"keyword": "crm tool", "kind": "story"}
        assert list(update.keys()) == ["$max"]
        assert upsert is True

    # DB down (koi collection nahi) -> raise nahi
    monkeypatch.setattr(database, "_get_state_collection", lambda: None)
    assert database.set_last_seen_ts("crm tool", "story", 900) is None

    # DB down (collection lena hi fail) -> raise nahi
    def boom():
        raise RuntimeError("mongo down")
    monkeypatch.setattr(database, "_get_state_collection", boom)
    assert database.set_last_seen_ts("crm tool", "story", 900) is None

    # DB down (update_one fail) -> raise nahi
    class BrokenColl:
        def update_one(self, *a, **k):
            raise RuntimeError("write failed")
    monkeypatch.setattr(database, "_get_state_collection", lambda: BrokenColl())
    assert database.set_last_seen_ts("crm tool", "story", 900) is None


# ─────────────────────────────────────────────────────────────────────────────
# test_11 — load_hn_keywords
# ─────────────────────────────────────────────────────────────────────────────

class FakeKeywordColl:
    def __init__(self, docs):
        self.docs = docs
        self.find_args = None

    def find(self, *args, **kwargs):
        self.find_args = (args, kwargs)
        return iter(self.docs)


class FakeDb2(dict):
    """db["flintel_keywords"] style access."""


def test_11_load_hn_keywords_dedup_ordered_ignores_fetched(monkeypatch):
    docs = [
        {"keyword": "Alpha Tool", "fetched": True},     # fetched flag ignore hona chahiye
        {"keyword": "beta", "fetched": False},
        {"keyword": "  alpha tool "},                   # duplicate (case + whitespace)
        {"keyword": "BETA"},                            # duplicate
        {"keyword": ""},                                # khali
        {"keyword": "   "},                             # sirf spaces
        {"keyword": None},                              # None
        {},                                             # keyword field hi nahi
        {"keyword": "Gamma", "fetched": True},
    ]
    coll = FakeKeywordColl(docs)
    monkeypatch.setattr(database, "get_db2", lambda: FakeDb2({"flintel_keywords": coll}))

    assert database.load_hn_keywords() == ["Alpha Tool", "beta", "Gamma"]

    # `fetched` par filter nahi lagta: find({}, ...) saare docs maangta hai
    args, _kwargs = coll.find_args
    assert args[0] == {}

    # db2 na mile -> [] (raise nahi)
    monkeypatch.setattr(database, "get_db2", lambda: None)
    assert database.load_hn_keywords() == []


# ─────────────────────────────────────────────────────────────────────────────
# test_12 — item index.save_signal ke contract par poora utarta hai
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_KEYS = {"message_id", "platform", "text", "username", "post_url", "posted_at", "search_keyword"}


@pytest.mark.parametrize("kind,platform", [("story", "hackernews"), ("comment", "hackernews_comment")])
def test_12_save_fn_item_has_save_signal_keys(monkeypatch, http, state_db, kind, platform):
    if kind == "story":
        hit = story_hit(77, title="crm tool launch", story_text="<p>details</p>", author="carol")
    else:
        hit = comment_hit(77, "<p>we love this crm tool</p>", author="carol")
    http.handler = lambda url, params: FakeResp({"hits": [hit], "nbPages": 1})

    received = []
    fetcher.process_keyword_kind("crm tool", kind, lambda it: received.append(it) or True, None)

    assert len(received) == 1
    item = received[0]
    assert REQUIRED_KEYS <= set(item.keys())
    assert item["platform"] == platform
    assert item["username"] == "carol"
    assert item["search_keyword"] == "crm tool"

    # Isi item ko asal index.save_signal se guzaro (fake Mongo sink, koi embedding/MySQL nahi).
    fake_db = mock.MagicMock()
    fake_db.flintel_signals.find_one.return_value = None
    monkeypatch.setattr(index, "db", fake_db)
    monkeypatch.setattr(index, "_is_mongodb_data_enabled", lambda: True)
    monkeypatch.setattr(index, "_is_mysql_data_enabled", lambda: False)
    monkeypatch.setattr(index, "_is_embedding_enabled", lambda: False)

    assert index.save_signal(item) is True

    inserted = fake_db.flintel_signals.insert_one.call_args[0][0]
    for key in REQUIRED_KEYS:
        assert inserted[key] == item[key]
    assert "_created_at_i" not in inserted   # checkpoint key save_signal ignore karta hai
