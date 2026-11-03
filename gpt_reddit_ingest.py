"""
gpt_reddit_ingest.py
--------------------
24-hour Google-discovered Reddit post ingestion via OpenAI GPT Web Search.

Flow (one cycle, see run_cycle()):
  1. Read flintel_google_posts docs with reddit_fetched=False AND
     discovered_at >= (now_utc - 24h).
  2. For each, ask the OpenAI Responses API (web_search tool) to open the
     EXISTING post_url and return the post's real title/body. No Google
     search, no new URLs. Anything unverifiable is a failure (never inserted).
  3. Generate the embedding with the repo's existing generate_embedding().
  4. Insert via the repo's existing save_signal() (MySQL flintel_signals).
  5. ONLY after MySQL confirms the row exists (new insert, or positively
     confirmed duplicate) flip reddit_fetched=True in flintel_google_posts.

This module never talks to Mongo/MySQL/OpenAI directly except through the
callables in `Deps`, which index.py wires to its existing implementations.
Nothing in the existing Reddit RSS / SERP / Hacker News pipelines is touched.
"""

import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional

log = logging.getLogger("flintel")
TAG = "[GPT-REDDIT]"

ELIGIBILITY_WINDOW_HOURS = 24

# In-memory (per-process) retry cooldown for URLs this flow failed on. Nothing
# is written to Mongo on failure, so flintel_google_posts' own retry/cooldown
# fields (used by the existing RSS loop) are left exactly as they are.
_retry_after: dict = {}
_retry_lock = threading.Lock()
_cycle_lock = threading.Lock()   # one cycle at a time

_REMOVED_BODIES = {"[removed]", "[deleted]", "removed", "deleted"}


@dataclass
class Deps:
    """Everything the cycle needs, injected so tests can mock it all."""
    get_posts_collection: Callable            # () -> flintel_google_posts collection
    get_client: Callable                      # () -> OpenAI client or None
    model: str
    generate_embedding: Callable              # existing index.generate_embedding
    embedding_to_blob: Callable               # existing index._embedding_to_blob (dim validation)
    embedding_enabled: Callable               # () -> bool
    save_signal: Callable                     # existing index.save_signal
    is_post_already_signaled: Callable        # existing duplicate check (by post_url)
    mysql_post_exists: Callable               # existing index._mysql_post_exists (by message_id)
    mysql_enabled: Callable                   # () -> bool
    extract_submission_id: Callable           # existing index._extract_reddit_submission_id
    pass_limit: int = 50
    retry_cooldown_seconds: int = 1800


# ─────────────────────────────────────────────────────────────────────────────
# A. Mongo eligibility (timezone-safe UTC)
# ─────────────────────────────────────────────────────────────────────────────

def _as_utc(dt):
    """pymongo returns naive datetimes (already UTC) unless tz_aware=True."""
    if not isinstance(dt, datetime):
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def utc_cutoff(now: Optional[datetime] = None) -> datetime:
    now = _as_utc(now) if now is not None else datetime.now(timezone.utc)
    return now - timedelta(hours=ELIGIBILITY_WINDOW_HOURS)


def get_eligible_posts(collection, now: Optional[datetime] = None, limit: int = 50) -> list:
    """reddit_fetched=False AND discovered_at within the last 24h (relative to
    `now`, UTC). Oldest first so posts nearest to leaving the window go first.
    The window is re-checked in Python so a naive/aware mismatch can never let
    an old record through."""
    cutoff = utc_cutoff(now)
    cursor = collection.find({
        "reddit_fetched": False,
        "discovered_at": {"$gte": cutoff},
    }).sort("discovered_at", 1).limit(limit)
    out = []
    for doc in cursor:
        disc = _as_utc(doc.get("discovered_at"))
        if doc.get("reddit_fetched") is False and disc is not None and disc >= cutoff \
                and doc.get("post_url"):
            out.append(doc)
    return out


def mark_fetched(collection, post_url: str) -> bool:
    """Same fields as index.mark_google_post_fetched(), but reports success so
    marked_fetched is an honest count. Never raises."""
    try:
        res = collection.update_one(
            {"post_url": post_url},
            {"$set": {
                "reddit_fetched": True,
                "fetched_at":     datetime.now(timezone.utc),
                "fuzzy_matched":  True,
            }},
        )
        return getattr(res, "matched_count", 0) >= 1
    except Exception as exc:
        log.error(f"{TAG} mark reddit_fetched failed for {post_url}: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# B. GPT Web Search — retrieve the actual content of the supplied URL
# ─────────────────────────────────────────────────────────────────────────────

def build_prompt(post_url: str) -> str:
    return (
        "Use web search to open EXACTLY this Reddit post URL and read the post:\n"
        f"{post_url}\n\n"
        "Rules:\n"
        "- Only retrieve this one URL. Do not search for other posts, related discussions, "
        "or alternative sources, and do not run a general Google-style search for the topic.\n"
        "- Copy the post's title and its original body text (selftext) VERBATIM. "
        "Do not summarize, paraphrase, translate, or add anything. Do not include comments.\n"
        "- If you cannot open this exact URL, or cannot see the actual post text, "
        "do NOT guess or reconstruct it: return found=false with empty strings.\n"
        "- If the post only has a title (link/image post), return the title and an empty body.\n\n"
        "Respond with ONLY a JSON object, no markdown fences, of this exact shape:\n"
        '{"found": true|false, "title": "...", "body": "...", "author": "..." or null, '
        '"reason": "short note if found is false"}'
    )


def _get(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _citation_urls(response) -> list:
    urls = []
    for item in (_get(response, "output") or []):
        if _get(item, "type") != "message":
            continue
        for part in (_get(item, "content") or []):
            for ann in (_get(part, "annotations") or []):
                if _get(ann, "type") == "url_citation" and _get(ann, "url"):
                    urls.append(str(_get(ann, "url")))
    return urls


def _parse_json_object(text: str):
    if not text:
        return None
    t = text.strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.I).strip()
    try:
        obj = json.loads(t)
    except ValueError:
        m = re.search(r"\{.*\}", t, flags=re.S)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except ValueError:
            return None
    return obj if isinstance(obj, dict) else None


def fetch_post_content_via_gpt(client, model: str, post_url: str, submission_id: str) -> tuple:
    """Returns ({"title","body","author"}, "ok") or (None, reason).
    reason: api_error | no_json | not_found | unverified | empty"""
    try:
        response = client.responses.create(
            model=model,
            tools=[{"type": "web_search"}],
            tool_choice="required",
            input=build_prompt(post_url),
        )
    except Exception as exc:
        return None, f"api_error:{type(exc).__name__}:{str(exc)[:120]}"

    data = _parse_json_object(_get(response, "output_text") or "")
    if data is None:
        return None, "no_json"
    if data.get("found") is not True:
        return None, "not_found"

    # Verification: the web search must have actually cited THIS post
    # (…/comments/<submission_id>…). No citation of the target => unverified.
    needle = f"/comments/{submission_id}".lower()
    if not any(needle in u.lower() and "reddit.com" in u.lower() for u in _citation_urls(response)):
        return None, "unverified"

    title = str(data.get("title") or "").strip()
    body = str(data.get("body") or "").strip()
    if body.lower() in _REMOVED_BODIES:
        body = ""
    if not title:
        return None, "empty"
    author = str(data.get("author") or "").strip()
    author = re.sub(r"^/?u/", "", author).strip() or "unknown"
    return {"title": title, "body": body, "author": author}, "ok"


def _compose_text(title: str, body: str) -> str:
    # same shape the existing RSS path produces: "title\n\nbody"
    return title if (not body or body.lower() == title.lower()) else f"{title}\n\n{body}"


# ─────────────────────────────────────────────────────────────────────────────
# one cycle
# ─────────────────────────────────────────────────────────────────────────────

def _cooling(url: str) -> bool:
    with _retry_lock:
        return _retry_after.get(url, 0) > time.monotonic()


def _set_cooldown(url: str, seconds: int):
    with _retry_lock:
        _retry_after[url] = time.monotonic() + seconds


def _clear_cooldown(url: str):
    with _retry_lock:
        _retry_after.pop(url, None)


def run_cycle(deps: Deps, now: Optional[datetime] = None) -> dict:
    """Runs one ingestion cycle and returns the counters it logged."""
    s = dict(eligible=0, submitted=0, retrieved=0, new_messages=0, duplicates=0, failed=0,
             embedding_failed=0, mysql_failed=0, marked_fetched=0, mark_failed=0,
             skipped_cooldown=0)

    if not _cycle_lock.acquire(blocking=False):
        log.warning(f"{TAG} previous cycle still running — skipping this one")
        return s
    try:
        log.info(f"{TAG} Cycle start | window={ELIGIBILITY_WINDOW_HOURS}h (UTC, relative to now) | "
                 f"pass_limit={deps.pass_limit} | model={deps.model}")

        if not deps.mysql_enabled():
            log.warning(f"{TAG} MYSQL_DATA is false — this flow stores to MySQL, nothing done, "
                        f"no records touched")
            return s

        coll = deps.get_posts_collection()
        try:
            docs = get_eligible_posts(coll, now=now, limit=deps.pass_limit)
        except Exception as exc:
            log.error(f"{TAG} could not read eligible records from flintel_google_posts: {exc}")
            return s
        s["eligible"] = len(docs)
        log.info(f"{TAG} Eligible MongoDB records found: {s['eligible']} "
                 f"(reddit_fetched=False, discovered_at >= {utc_cutoff(now).isoformat()})")
        if not docs:
            _log_summary(s)
            return s

        client = deps.get_client()
        if client is None:
            log.error(f"{TAG} OpenAI client unavailable (OPENAI_API_KEY missing / openai not installed) — "
                      f"no records touched")
            _log_summary(s)
            return s

        embedding_on = deps.embedding_enabled()
        if not embedding_on:
            log.warning(f"{TAG} embeddings disabled (EMBEDDING_ENABLED/OPENAI_API_KEY) — every record "
                        f"will be treated as an embedding failure and left unfetched")

        for doc in docs:
            url = doc["post_url"]
            subreddit = doc.get("subreddit", "") or ""

            def fail(kind=None):
                s["failed"] += 1
                if kind:
                    s[kind] += 1
                _set_cooldown(url, deps.retry_cooldown_seconds)

            if _cooling(url):
                s["skipped_cooldown"] += 1
                continue

            sid = deps.extract_submission_id(url)
            if not sid:
                log.warning(f"{TAG} FAILED retrieval | reason=invalid_url | {url}")
                fail()
                continue
            message_id = f"reddit_serp_{sid}"   # same id scheme as the RSS path => shared dedup

            # duplicate pre-check (existing mechanism) — no GPT/embedding spend
            if deps.is_post_already_signaled(url):
                s["duplicates"] += 1
                log.info(f"{TAG} DUPLICATE skipped (already stored) | {url}")
                if mark_fetched(coll, url):
                    s["marked_fetched"] += 1
                else:
                    s["mark_failed"] += 1
                continue

            # ── B. GPT Web Search
            s["submitted"] += 1
            content, reason = fetch_post_content_via_gpt(client, deps.model, url, sid)
            if content is None:
                log.warning(f"{TAG} FAILED retrieval | reason={reason} | {url}")
                fail()
                continue
            s["retrieved"] += 1
            text = _compose_text(content["title"], content["body"])
            log.info(f"{TAG} Retrieved post content | chars={len(text)} | subreddit={subreddit!r} | "
                     f"discovered_at={_as_utc(doc.get('discovered_at')).isoformat()} | {url}")

            # ── embedding (existing implementation; failure => nothing inserted)
            emb = deps.generate_embedding(text) if embedding_on else None
            if emb is not None:
                try:
                    deps.embedding_to_blob(emb)   # same dim/format check the MySQL insert applies
                except Exception as exc:
                    log.warning(f"{TAG} embedding rejected by existing format check: {exc}")
                    emb = None
            if emb is None:
                log.warning(f"{TAG} EMBEDDING FAILED — not inserted, left unfetched | {url}")
                fail("embedding_failed")
                continue

            # ── C. MySQL insert through existing save_signal()
            item = {
                "message_id":           message_id,
                "platform":             "reddit",
                "text":                 text,
                "username":             content["author"],
                "subreddit_or_channel": subreddit,
                "post_url":             url,
                "posted_at":            None,
                "search_keyword":       doc.get("search_keyword", ""),
                "google_rank":          doc.get("google_rank"),
                "embedding":            emb,   # reused by save_signal; no second embedding call
            }
            existed_before = deps.mysql_post_exists(message_id)
            try:
                saved = deps.save_signal(item)
            except Exception as exc:
                log.error(f"{TAG} MYSQL INSERT FAILED | {url} | {exc}")
                fail("mysql_failed")
                continue
            in_mysql = deps.mysql_post_exists(message_id)   # positive confirmation

            if not in_mysql:
                log.error(f"{TAG} MYSQL INSERT FAILED (row not present after save) | {url}")
                fail("mysql_failed")
                continue

            if saved and not existed_before:
                s["new_messages"] += 1
                log.info(f"{TAG} MySQL INSERTED | message_id={message_id} | {url}")
            else:
                s["duplicates"] += 1
                log.info(f"{TAG} DUPLICATE skipped (already in MySQL) | message_id={message_id} | {url}")

            # ── D. only now, with MySQL confirmed, mark Mongo
            if mark_fetched(coll, url):
                s["marked_fetched"] += 1
                _clear_cooldown(url)
            else:
                s["mark_failed"] += 1   # row is stored; next cycle's duplicate check will mark it

        _log_summary(s)
        return s
    finally:
        _cycle_lock.release()


def _log_summary(s: dict):
    log.info(
        f"{TAG} Cycle complete | eligible={s['eligible']} | submitted={s['submitted']} | "
        f"retrieved={s['retrieved']} | new_messages={s['new_messages']} | duplicates={s['duplicates']} | "
        f"failed={s['failed']} | embedding_failed={s['embedding_failed']} | mysql_failed={s['mysql_failed']} | "
        f"marked_fetched={s['marked_fetched']} | mark_failed={s['mark_failed']} | "
        f"skipped_cooldown={s['skipped_cooldown']}"
    )
