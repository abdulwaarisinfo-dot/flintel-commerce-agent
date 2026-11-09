"""Hermetic tests for the archive-first batch fetch (no network, no DB)."""
import importlib, sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

def _load():
    with mock.patch("pymongo.MongoClient", return_value=mock.MagicMock()):
        sys.modules.pop("index", None)
        return importlib.import_module("index")

index = _load()

def _doc(sid, slug="some_title"):
    return {"post_url": f"https://www.reddit.com/r/test/comments/{sid}/{slug}/",
            "search_keyword": "kw", "google_rank": 1, "subreddit": "test"}

def _resp(rows, status=200):
    m = mock.MagicMock(); m.status_code = status; m.json.return_value = {"data": rows}; return m

def test_batch_hits_and_misses():
    index._archive_miss_until.clear()
    docs = [_doc("aaa111"), _doc("bbb222")]
    rows = [{"id": "aaa111", "title": "Hello", "selftext": "Body text", "author": "bob", "created_utc": 1700000000}]
    with mock.patch.object(index.requests, "get", return_value=_resp(rows)) as g:
        out = index.fetch_reddit_posts_via_archive(docs)
    assert g.call_count == 1                      # ONE call for the whole batch
    assert list(out) == [docs[0]["post_url"]]
    item = out[docs[0]["post_url"]]
    assert item["message_id"] == "reddit_serp_aaa111"
    assert item["text"] == "Hello\n\nBody text" and item["username"] == "bob"
    assert item["post_url"] == docs[0]["post_url"] and item["platform"] == "reddit"
    assert "bbb222" in index._archive_miss_until  # miss remembered -> not re-asked every pass

def test_miss_not_requeried():
    with mock.patch.object(index.requests, "get") as g:
        assert index.fetch_reddit_posts_via_archive([_doc("bbb222")]) == {}
    g.assert_not_called()

def test_removed_body_and_error_fallback():
    index._archive_miss_until.clear()
    rows = [{"id": "ccc333", "title": "T", "selftext": "[removed]", "author": "[deleted]"}]
    with mock.patch.object(index.requests, "get", return_value=_resp(rows)):
        item = index.fetch_reddit_posts_via_archive([_doc("ccc333")])[_doc("ccc333")["post_url"]]
    assert item["text"] == "T" and item["username"] == "unknown"
    with mock.patch.object(index.requests, "get", return_value=_resp([], 500)):
        assert index.fetch_reddit_posts_via_archive([_doc("ddd444")]) == {}
    with mock.patch.object(index.requests, "get", side_effect=Exception("boom")):
        assert index.fetch_reddit_posts_via_archive([_doc("eee555")]) == {}
