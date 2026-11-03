"""
tests/test_gpt_reddit.py
------------------------
Hermetic tests for the 24h GPT-Web-Search Reddit ingestion flow
(gpt_reddit_ingest.py + its wiring in index.py).

No real Mongo / MySQL / OpenAI: pymongo.MongoClient is mocked on import, the
Mongo collection, MySQL layer and both OpenAI clients are fakes. The REAL
index.save_signal / index.generate_embedding / index._embedding_to_blob /
index.is_post_already_signaled run, so reuse of the existing implementations
is actually exercised.
"""

import importlib
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_index():
    with mock.patch("pymongo.MongoClient", return_value=mock.MagicMock()):
        sys.modules.pop("index", None)
        return importlib.import_module("index")


index = _load_index()
import gpt_reddit_ingest as g  # noqa: E402

NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)
DIM = 1536


# ─────────────────────────────────────────────────────────────────────────────
# fakes
# ─────────────────────────────────────────────────────────────────────────────

def _utc(dt):
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


class FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, key, direction):
        self.docs = sorted(self.docs, key=lambda d: _utc(d[key]), reverse=direction < 0)
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def __iter__(self):
        return iter(self.docs)


class FakeCollection:
    """Applies the two filter conditions like Mongo does. Stores datetimes the
    way pymongo returns them (naive UTC) unless a test stores aware ones."""

    def __init__(self, docs, events):
        self.docs = docs
        self.events = events
        self.last_query = None
        self.fail_update = False

    def find(self, query):
        self.last_query = query
        cut = _utc(query["discovered_at"]["$gte"])
        return FakeCursor([d for d in self.docs
                           if d["reddit_fetched"] == query["reddit_fetched"]
                           and _utc(d["discovered_at"]) >= cut])

    def update_one(self, flt, upd):
        if self.fail_update:
            raise RuntimeError("mongo down")
        n = 0
        for d in self.docs:
            if d["post_url"] == flt["post_url"]:
                d.update(upd["$set"])
                n += 1
        self.events.append(("mark", flt["post_url"]))
        return SimpleNamespace(matched_count=n)


def post(pid, hours_ago=1, fetched=False, naive=True, subreddit="smallbusiness"):
    disc = NOW - timedelta(hours=hours_ago)
    if naive:
        disc = disc.replace(tzinfo=None)
    return {
        "post_url": f"https://www.reddit.com/r/{subreddit}/comments/{pid}/some_slug/",
        "subreddit": subreddit, "search_keyword": "plumber", "google_rank": 3,
        "reddit_fetched": fetched, "discovered_at": disc, "retry_count": 0, "next_retry_at": None,
    }


def gpt_reply(pid, title="Need a plumber", body="Pipe burst in my kitchen, who do I call?",
              found=True, cite=True, author="u/bob"):
    import json
    txt = json.dumps({"found": found, "title": title, "body": body, "author": author, "reason": ""})
    ann = [{"type": "url_citation", "url": f"https://www.reddit.com/r/smallbusiness/comments/{pid}/some_slug/"}] if cite else []
    return SimpleNamespace(
        output_text=txt,
        output=[SimpleNamespace(type="message", content=[SimpleNamespace(type="output_text", annotations=ann)])],
    )


class FakeResponses:
    def __init__(self, replies):
        self.replies = replies          # pid -> response or Exception
        self.calls = []

    def create(self, **kw):
        self.calls.append(kw)
        for pid, r in self.replies.items():
            if f"/comments/{pid}/" in kw["input"]:
                if isinstance(r, Exception):
                    raise r
                return r
        raise AssertionError("GPT called with unexpected URL")


class Env:
    """Wires fakes into index and exposes counters/event log."""

    def __init__(self, monkeypatch, docs, replies, embed_ok=True, mysql_fail=False, preexisting=()):
        self.events = []
        self.coll = FakeCollection(docs, self.events)
        self.responses = FakeResponses(replies)
        self.embed_calls = []
        self.mysql_ids = set(preexisting)
        self.mysql_docs = {}
        self.mysql_fail = mysql_fail

        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("EMBEDDING_ENABLED", "true")
        monkeypatch.setenv("MYSQL_DATA", "true")
        monkeypatch.setenv("MONGODB_DATA", "false")
        monkeypatch.setattr(index, "load_dotenv", lambda *a, **k: None)
        monkeypatch.setattr(index, "db2", SimpleNamespace(flintel_google_posts=self.coll))
        monkeypatch.setattr(index, "_get_gpt_reddit_client",
                            lambda: SimpleNamespace(responses=self.responses))

        # the REAL generate_embedding runs; only the OpenAI embeddings client is faked
        def create(model, input):
            self.embed_calls.append((model, input))
            if not embed_ok:
                raise RuntimeError("embedding api down")
            return SimpleNamespace(data=[SimpleNamespace(embedding=[0.01] * DIM)])
        monkeypatch.setattr(index, "_get_openai_client",
                            lambda: SimpleNamespace(embeddings=SimpleNamespace(create=create)))

        # MySQL layer faked at the lowest existing seam; save_signal is real
        monkeypatch.setattr(index, "_mysql_post_exists", lambda mid: mid in self.mysql_ids)
        monkeypatch.setattr(index, "_mysql_post_url_exists",
                            lambda url: any(d["post_url"] == url for d in self.mysql_docs.values()))

        def save_mysql(doc):
            if self.mysql_fail:
                return False
            if doc["message_id"] in self.mysql_ids:
                return False
            self.mysql_ids.add(doc["message_id"])
            self.mysql_docs[doc["message_id"]] = doc
            self.events.append(("mysql_insert", doc["post_url"]))
            return True
        monkeypatch.setattr(index, "_save_to_mysql", save_mysql)

        g._retry_after.clear()

    def run(self, now=NOW):
        return g.run_cycle(index._gpt_reddit_deps(), now=now)


@pytest.fixture
def mk(monkeypatch):
    def _mk(docs, replies, **kw):
        return Env(monkeypatch, docs, replies, **kw)
    return _mk


# ─────────────────────────────────────────────────────────────────────────────
# A. eligibility / UTC
# ─────────────────────────────────────────────────────────────────────────────

def test_only_unfetched_within_24h_are_eligible(mk):
    docs = [post("aaa111", hours_ago=2), post("bbb222", hours_ago=25),
            post("ccc333", hours_ago=3, fetched=True), post("ddd444", hours_ago=23.9)]
    env = mk(docs, {})
    got = g.get_eligible_posts(env.coll, now=NOW)
    ids = sorted(d["post_url"].split("/comments/")[1].split("/")[0] for d in got)
    assert ids == ["aaa111", "ddd444"]


def test_window_is_relative_to_now_not_a_calendar_date(mk):
    docs = [post("aaa111", hours_ago=2)]
    env = mk(docs, {})
    later = NOW + timedelta(hours=23)          # doc is now 25h old
    assert g.get_eligible_posts(env.coll, now=later) == []
    assert len(g.get_eligible_posts(env.coll, now=NOW)) == 1


def test_utc_handling_naive_and_aware_and_non_utc_now(mk):
    est = timezone(timedelta(hours=-5))
    docs = [post("aaa111", hours_ago=2, naive=True), post("bbb222", hours_ago=2, naive=False),
            post("ccc333", hours_ago=30, naive=True), post("ddd444", hours_ago=30, naive=False)]
    env = mk(docs, {})
    got = g.get_eligible_posts(env.coll, now=NOW.astimezone(est))   # same instant, other tz
    assert sorted(d["post_url"].split("/")[6] for d in got) == ["aaa111", "bbb222"]
    q = env.coll.last_query
    assert q["reddit_fetched"] is False
    cutoff = q["discovered_at"]["$gte"]
    assert cutoff.tzinfo is not None and cutoff.utcoffset() == timedelta(0)
    assert cutoff == NOW - timedelta(hours=24)


def test_python_recheck_blocks_old_doc_even_if_query_lets_it_through():
    class Leaky(FakeCollection):
        def find(self, query):
            return FakeCursor(list(self.docs))      # ignores the filter entirely
    coll = Leaky([post("old111", hours_ago=48), post("new222", hours_ago=1),
                  post("done33", hours_ago=1, fetched=True)], [])
    got = g.get_eligible_posts(coll, now=NOW)
    assert [d["post_url"].split("/")[6] for d in got] == ["new222"]


# ─────────────────────────────────────────────────────────────────────────────
# B. GPT web search
# ─────────────────────────────────────────────────────────────────────────────

def test_gpt_receives_original_url_with_web_search_and_real_text_is_stored(mk):
    p = post("aaa111")
    env = mk([p], {"aaa111": gpt_reply("aaa111")})
    s = env.run()
    call = env.responses.calls[0]
    assert p["post_url"] in call["input"]
    assert call["tools"] == [{"type": "web_search"}]
    stored = env.mysql_docs["reddit_serp_aaa111"]
    assert stored["text"] == "Need a plumber\n\nPipe burst in my kitchen, who do I call?"
    assert stored["post_url"] == p["post_url"]
    assert stored["subreddit_or_channel"] == "smallbusiness"
    assert stored["username"] == "bob"
    assert stored["platform"] == "reddit"
    assert stored["google_rank"] == 3 and stored["search_keyword"] == "plumber"
    assert s["new_messages"] == 1


@pytest.mark.parametrize("reply", [
    gpt_reply("aaa111", found=False, title="", body=""),            # cannot access
    gpt_reply("aaa111", cite=False),                                # unverifiable: no citation
    gpt_reply("aaa111", title="", body="orphan body"),              # no title
    SimpleNamespace(output_text="I could not open it, sorry.", output=[]),   # not JSON
    gpt_reply("zzz999"),                                            # cites a DIFFERENT post
])
def test_missing_or_unverifiable_content_is_not_inserted_or_marked(mk, reply):
    env = mk([post("aaa111")], {"aaa111": reply})
    s = env.run()
    assert env.mysql_docs == {} and env.embed_calls == []
    assert env.coll.docs[0]["reddit_fetched"] is False
    assert (s["retrieved"], s["failed"], s["new_messages"], s["marked_fetched"]) == (0, 1, 0, 0)


def test_gpt_api_error_leaves_record_unfetched(mk):
    env = mk([post("aaa111")], {"aaa111": RuntimeError("boom")})
    s = env.run()
    assert env.mysql_docs == {} and env.coll.docs[0]["reddit_fetched"] is False
    assert s["failed"] == 1 and s["submitted"] == 1


def test_removed_body_is_dropped_but_title_still_valid(mk):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111", body="[removed]")})
    env.run()
    assert env.mysql_docs["reddit_serp_aaa111"]["text"] == "Need a plumber"


# ─────────────────────────────────────────────────────────────────────────────
# embeddings / MySQL / ordering
# ─────────────────────────────────────────────────────────────────────────────

def test_existing_embedding_implementation_is_reused_exactly_once(mk):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")})
    env.run()
    assert len(env.embed_calls) == 1                       # save_signal did NOT embed again
    model, text = env.embed_calls[0]
    assert model == index.EMBEDDING_MODEL
    assert text == env.mysql_docs["reddit_serp_aaa111"]["text"]
    assert len(env.mysql_docs["reddit_serp_aaa111"]["embedding"]) == DIM
    index._embedding_to_blob(env.mysql_docs["reddit_serp_aaa111"]["embedding"])  # existing format OK


def test_mysql_insert_happens_before_mongo_mark(mk):
    p = post("aaa111")
    env = mk([p], {"aaa111": gpt_reply("aaa111")})
    s = env.run()
    assert env.events == [("mysql_insert", p["post_url"]), ("mark", p["post_url"])]
    assert env.coll.docs[0]["reddit_fetched"] is True and s["marked_fetched"] == 1


def test_embedding_failure_leaves_record_unfetched(mk):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")}, embed_ok=False)
    s = env.run()
    assert env.mysql_docs == {} and env.events == []
    assert env.coll.docs[0]["reddit_fetched"] is False
    assert s["embedding_failed"] == 1 and s["new_messages"] == 0 and s["marked_fetched"] == 0


def test_wrong_dimension_embedding_is_an_embedding_failure(mk, monkeypatch):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")})
    monkeypatch.setattr(index, "generate_embedding", lambda t: [0.1] * 10)
    s = env.run()
    assert env.mysql_docs == {} and s["embedding_failed"] == 1
    assert env.coll.docs[0]["reddit_fetched"] is False


def test_mysql_failure_leaves_record_unfetched(mk):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")}, mysql_fail=True)
    s = env.run()
    assert env.coll.docs[0]["reddit_fetched"] is False and env.events == []
    assert s["mysql_failed"] == 1 and s["new_messages"] == 0 and s["marked_fetched"] == 0


def test_mysql_disabled_touches_nothing(mk, monkeypatch):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")})
    monkeypatch.setenv("MYSQL_DATA", "false")
    s = env.run()
    assert env.responses.calls == [] and env.coll.docs[0]["reddit_fetched"] is False and s["eligible"] == 0


def test_mark_failure_after_insert_is_counted_not_new_twice(mk):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")})
    env.coll.fail_update = True
    s = env.run()
    assert s["new_messages"] == 1 and s["marked_fetched"] == 0 and s["mark_failed"] == 1
    env.coll.fail_update = False
    g._retry_after.clear()
    s2 = env.run()      # next cycle: positively-confirmed duplicate gets marked, no 2nd insert
    assert s2["new_messages"] == 0 and s2["duplicates"] == 1 and s2["marked_fetched"] == 1
    assert env.responses.calls.__len__() == 1


# ─────────────────────────────────────────────────────────────────────────────
# duplicates + new_messages accounting
# ─────────────────────────────────────────────────────────────────────────────

def test_duplicate_is_skipped_without_gpt_or_embedding_and_marked(mk):
    p = post("aaa111")
    env = mk([p], {}, preexisting={"reddit_serp_aaa111"})
    env.mysql_docs["reddit_serp_aaa111"] = {"post_url": p["post_url"]}
    s = env.run()
    assert env.responses.calls == [] and env.embed_calls == []
    assert s["duplicates"] == 1 and s["new_messages"] == 0 and s["marked_fetched"] == 1
    assert env.coll.docs[0]["reddit_fetched"] is True


def test_same_url_processed_twice_inserts_once(mk):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")})
    env.run()
    env.coll.docs[0]["reddit_fetched"] = False      # simulate it being picked up again
    s = env.run()
    assert len(env.mysql_docs) == 1 and s["new_messages"] == 0 and s["duplicates"] == 1


def test_message_id_duplicate_with_different_url_form_is_not_new(mk):
    # same submission id already stored under another URL form: pre-check by URL misses it,
    # save_signal's message_id dedupe returns False, MySQL has it => duplicate, not new.
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111")}, preexisting={"reddit_serp_aaa111"})
    s = env.run()
    assert s["new_messages"] == 0 and s["duplicates"] == 1 and s["marked_fetched"] == 1
    assert env.mysql_docs == {}


def test_failed_url_is_not_retried_every_cycle(mk):
    env = mk([post("aaa111")], {"aaa111": gpt_reply("aaa111", found=False)})
    env.run()
    s2 = env.run()
    assert len(env.responses.calls) == 1 and s2["skipped_cooldown"] == 1


def test_new_messages_counts_only_genuine_new_inserts_and_summary_log(mk, caplog):
    docs = [post("new001"), post("new002"), post("dup003"), post("bad004"),
            post("emb005"), post("old006", hours_ago=30), post("done07", fetched=True)]
    replies = {"new001": gpt_reply("new001"), "new002": gpt_reply("new002", title="T2"),
               "bad004": gpt_reply("bad004", found=False)}
    env = mk(docs, replies, preexisting={"reddit_serp_dup003"})
    env.mysql_docs["reddit_serp_dup003"] = {"post_url": docs[2]["post_url"]}
    # emb005 -> GPT ok, but embedding fails for it only
    replies["emb005"] = gpt_reply("emb005", title="EMBFAIL")
    real = index.generate_embedding
    with mock.patch.object(index, "generate_embedding",
                           lambda t: None if t.startswith("EMBFAIL") else real(t)):
        with caplog.at_level(logging.INFO, logger="flintel"):
            s = env.run()
    assert s == dict(eligible=5, submitted=4, retrieved=3, new_messages=2, duplicates=1, failed=2,
                     embedding_failed=1, mysql_failed=0, marked_fetched=3, mark_failed=0,
                     skipped_cooldown=0)
    assert len(env.mysql_docs) == 3                  # dup003 (pre-seeded) + 2 new
    line = [r.getMessage() for r in caplog.records if "Cycle complete" in r.getMessage()][-1]
    assert "[GPT-REDDIT] Cycle complete" in line
    assert "new_messages=2" in line and "eligible=5" in line and "duplicates=1" in line
    assert "marked_fetched=3" in line
    msgs = [r.getMessage() for r in caplog.records]
    assert any("Cycle start" in m for m in msgs)
    assert any("EMBEDDING FAILED" in m for m in msgs)
    assert any("FAILED retrieval" in m for m in msgs)
    assert any("MySQL INSERTED" in m for m in msgs)


def test_missing_openai_client_touches_nothing(mk, monkeypatch):
    env = mk([post("aaa111")], {})
    monkeypatch.setattr(index, "_get_gpt_reddit_client", lambda: None)
    s = env.run()
    assert s["eligible"] == 1 and s["submitted"] == 0 and env.coll.docs[0]["reddit_fetched"] is False


# ─────────────────────────────────────────────────────────────────────────────
# existing behaviour preserved
# ─────────────────────────────────────────────────────────────────────────────

def test_save_signal_without_embedding_key_still_embeds_itself(monkeypatch):
    calls = []
    monkeypatch.setattr(index, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setenv("MYSQL_DATA", "true")
    monkeypatch.setenv("MONGODB_DATA", "false")
    monkeypatch.setenv("OPENAI_API_KEY", "sk")
    monkeypatch.setattr(index, "_mysql_post_exists", lambda m: False)
    monkeypatch.setattr(index, "_save_to_mysql", lambda d: calls.append(d) or True)
    monkeypatch.setattr(index, "generate_embedding", lambda t: [0.5] * DIM)
    assert index.save_signal({"message_id": "m1", "text": "hello", "post_url": "u"}) is True
    assert calls[0]["embedding"] == [0.5] * DIM
