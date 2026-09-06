"""
FLINTEL — main.py  (STRIPPED-DOWN SIGNAL PIPELINE, based on v9.12)
=====================================================================
WHAT THIS FILE IS:
  A simplified version of the v9.12 service. The SERP-discovery ->
  flintel_google_posts -> Reddit-RSS-fetch pipeline is kept 100% AS-IS
  (same Google RapidAPI SERP call, same flintel_keywords fetch-once
  cache, same flintel_google_posts collection/schema, same fuzzy-keyword
  generation/matching, same Reddit RSS smart-retry fetch). Everything
  else has been REMOVED per request:

  REMOVED:
    - Claude / Anthropic scoring layer entirely (no intent_score,
      is_relevant, reply_draft, no system prompt, no batch scorer, no
      rescore processor).
    - search_volume (RapidAPI keyword-volume lookups + random fallback +
      flintel_keywords volume fields/seeding).
    - upvotes / comments (and their random-fallback generation) — Reddit
      RSS doesn't expose real engagement counts, so this build simply
      doesn't fabricate or store them anymore.
    - Twitter/X entirely (tweepy poller, Twitter queue, Twitter keyword
      list) — this build is Reddit-only.
    - The whole batching/queue system (reddit_queue, flintel_pending_batch,
      flintel_queue_messages, flintel_batch_seconds, flintel_seen_ids) —
      no longer needed since there's no Claude call to batch for.

  KEPT AS-IS:
    1. SERP DISCOVERY (run_serp_discovery_loop / process_one_keyword) —
       reads REDDIT_SEARCH_KEYWORDS (python list) + flintel_keywords
       fetch-once-forever cache, calls search_google_for_keyword() (the
       same single RapidAPI SERP call, site:reddit.com, unchanged), and
       for every result saves it into `flintel_google_posts`
       (post_url + google_rank + search_keyword + subreddit +
       Python-generated fuzzy_keywords) via save_google_post() —
       insert-only, so an already-tracked post_url is never overwritten.
       depth (SERP_RESULTS_PER_KEYWORD) and lookback (SERP_MONTHS_BACK)
       are still read from .env exactly as before.

    2. REDDIT FETCH (run_reddit_fetch_loop) — a fully separate thread
       that reads `flintel_google_posts` directly (reddit_fetched ==
       False), fetches each due post_url's public per-post RSS feed
       (fetch_reddit_post_by_url — same smart-retry + old.reddit.com
       fallback, credential-free, no .json endpoint anywhere), and
       checks the fetched text against that post's own stored
       fuzzy_keywords + original search_keyword via
       passes_fuzzy_filter().

    3. SIGNAL STORAGE — the ONLY behavioral change in this step: instead
       of pushing a match onto a queue for Claude to batch-score later,
       a match is saved DIRECTLY into `flintel_signals` right there in
       the fetch loop, tagged with its search_keyword, platform="reddit",
       post_url, text, username, subreddit, posted_at — no score, no
       queue, no batch, no Claude call anywhere in this file.

Run:
    pip install fastapi uvicorn pymongo python-dotenv httpx requests \
                feedparser
    python main.py
"""

import asyncio
import logging
import os
import time
import random
import re
import html
import threading
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

import requests
import feedparser
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN
import uvicorn

# ─────────────────────────────────────────────────────────────────────────────
# ENV / LOGGING
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("flintel")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — UNCHANGED shape from v9.12 for everything kept.
# ─────────────────────────────────────────────────────────────────────────────

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")
CLIENT_ID   = os.getenv("CLIENT_ID", "Flintel")

# ── RapidAPI — SOLE provider for Google SERP rank/discovery. UNCHANGED.
RAPIDAPI_KEY          = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_SEARCH_HOST  = "google-search116.p.rapidapi.com"

DATAFORSEO_SERP_TIMEOUT_SECONDS = int(os.getenv("DATAFORSEO_SERP_TIMEOUT_SECONDS", "120"))
REDDIT_JSON_TIMEOUT_SECONDS     = int(os.getenv("REDDIT_JSON_TIMEOUT_SECONDS", "15"))  # used for the RSS fetch

# ── SERP DISCOVERY CONFIG — UNCHANGED. This Python list's ONLY job is to
# seed brand-new keyword documents into flintel_keywords (insert-only).
REDDIT_SEARCH_KEYWORDS = [


]

# ── PER-KEYWORD "FETCH ONCE, EVER" CACHE CONFIG — UNCHANGED.
KEYWORD_CHECK_INTERVAL_SECONDS = int(os.getenv("KEYWORD_CHECK_INTERVAL_SECONDS", "60"))
REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS = int(os.getenv("REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS", "1800"))

# depth + lookback — read from .env exactly as before. Depth default
# raised to 100 per request.
SERP_RESULTS_PER_KEYWORD = int(os.getenv("SERP_RESULTS_PER_KEYWORD", "100"))
SERP_MONTHS_BACK         = int(os.getenv("SERP_MONTHS_BACK", "6"))
SERP_FETCH_SLEEP_SECONDS = float(os.getenv("SERP_FETCH_SLEEP_SECONDS", "1.5"))

# ── REDDIT "SMART FETCH" CONFIG — UNCHANGED v9.6 retry logic.
REDDIT_FETCH_MAX_RETRIES     = int(os.getenv("REDDIT_FETCH_MAX_RETRIES", "3"))
REDDIT_FETCH_BACKOFF_BASE    = float(os.getenv("REDDIT_FETCH_BACKOFF_BASE", "2.0"))
REDDIT_FETCH_JITTER_MIN      = float(os.getenv("REDDIT_FETCH_JITTER_MIN", "0.4"))
REDDIT_FETCH_JITTER_MAX      = float(os.getenv("REDDIT_FETCH_JITTER_MAX", "1.6"))
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "python:flintel-signal-bot:v1.0 (by /u/flintel_signals)",
)

# ── flintel_google_posts CONFIG — UNCHANGED.
REDDIT_FETCH_CHECK_INTERVAL_SECONDS = int(os.getenv("REDDIT_FETCH_CHECK_INTERVAL_SECONDS", "30"))
REDDIT_POST_RETRY_COOLDOWN_SECONDS  = int(os.getenv("REDDIT_POST_RETRY_COOLDOWN_SECONDS", "1800"))

REDDIT_ENABLED = os.getenv("REDDIT_ENABLED", "True").strip().lower() in ("1", "true", "yes", "on")

# ─────────────────────────────────────────────────────────────────────────────
# API KEY AUTH — unchanged shape, only used to protect read-only endpoints.
# ─────────────────────────────────────────────────────────────────────────────

API_KEY = os.getenv("API_KEY", "")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query  = APIKeyQuery(name="api_key",    auto_error=False)


async def verify_api_key(
    key_header: str = Security(api_key_header),
    key_query:  str = Security(api_key_query),
):
    if not API_KEY:
        return
    if key_header == API_KEY or key_query == API_KEY:
        return
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Invalid or missing API key.")


def _working(flag: bool) -> str:
    return "✅ Working" if flag else "❌ Not Working"


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC JSON FIELD-EXTRACTION HELPERS — UNCHANGED. Used ONLY by the
# Google SERP RapidAPI code below.
# ─────────────────────────────────────────────────────────────────────────────

def _dig_value(obj, candidate_keys: list):
    if obj is None:
        return None

    def _try_dict(d):
        if not isinstance(d, dict):
            return None
        for key in candidate_keys:
            if key in d and d[key] is not None:
                return d[key]
        return None

    if isinstance(obj, dict):
        val = _try_dict(obj)
        if val is not None:
            return val
        for v in obj.values():
            if isinstance(v, dict):
                val = _try_dict(v)
                if val is not None:
                    return val
            elif isinstance(v, list) and v:
                first = v[0]
                if isinstance(first, dict):
                    val = _try_dict(first)
                    if val is not None:
                        return val

    elif isinstance(obj, list) and obj:
        first = obj[0]
        if isinstance(first, dict):
            val = _try_dict(first)
            if val is not None:
                return val

    return None


def _dig_list(obj, candidate_list_keys: list) -> list:
    if isinstance(obj, list):
        return obj
    if not isinstance(obj, dict):
        return []
    for key in candidate_list_keys:
        val = obj.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            for inner_key in candidate_list_keys:
                inner_val = val.get(inner_key)
                if isinstance(inner_val, list):
                    return inner_val
    return []


RANK_FIELD_CANDIDATES = [
    "rank_absolute", "rank", "position", "google_rank",
    "serp_position", "rank_group", "index", "pos",
]

RESULT_LIST_KEY_CANDIDATES = [
    "results", "organic_results", "organic", "items", "data", "response", "hits",
]

# ─────────────────────────────────────────────────────────────────────────────
# FUZZY KEYWORD GENERATION + MATCHING — UNCHANGED from v9.12. This is the
# only relevance filter left in the whole file: a fetched Reddit post is
# only saved into flintel_signals if it matches its own stored
# search_keyword / fuzzy_keywords.
# ─────────────────────────────────────────────────────────────────────────────

_FUZZY_STOPWORDS = {
    "a", "an", "the", "to", "for", "of", "in", "on", "my", "our", "is",
    "are", "and", "or", "with", "from", "at", "by", "your", "their",
}


def generate_fuzzy_keywords(search_keyword: str) -> list:
    """UNCHANGED — Python auto-generates a small set of fuzzy variants for
    one Google search_keyword (full phrase, stopword-stripped phrase,
    individual significant words, bigrams, naive singular/plural
    variants). Called once per SERP result, at save_google_post() time."""
    kw = (search_keyword or "").lower().strip()
    words = re.findall(r"[a-z0-9']+", kw)
    variants = set()
    if kw:
        variants.add(kw)

    sig_words = [w for w in words if w not in _FUZZY_STOPWORDS and len(w) > 2]

    if sig_words:
        variants.add(" ".join(sig_words))

    for w in sig_words:
        variants.add(w)

    for i in range(len(sig_words) - 1):
        variants.add(f"{sig_words[i]} {sig_words[i + 1]}")

    plural_variants = set()
    for v in variants:
        if v.endswith("s") and len(v) > 3:
            plural_variants.add(v[:-1])
        else:
            plural_variants.add(v + "s")
    variants |= plural_variants

    variants.discard("")
    return sorted(variants)


def passes_fuzzy_filter(text: str, search_keyword: str, fuzzy_keywords: list) -> bool:
    """UNCHANGED — checks fetched Reddit post text against the ORIGINAL
    search_keyword and that post's own stored fuzzy_keywords list, both
    read straight off the flintel_google_posts document."""
    if not text:
        return False
    t = text.lower()
    if search_keyword and search_keyword.lower() in t:
        return True
    for kw in (fuzzy_keywords or []):
        if kw and kw in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB — flintel_signals + flintel_keywords (fetch-once cache) +
# flintel_google_posts. Batch/queue collections REMOVED entirely — there
# is no batching or Claude step left to persist state for.
# ─────────────────────────────────────────────────────────────────────────────

def get_database():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        db.flintel_signals.create_index([("message_id", ASCENDING)], unique=True, name="message_id_unique")
        db.flintel_signals.create_index([("post_url", ASCENDING)], name="post_url_lookup")
        for field in ["search_keyword", "platform", "created_at"]:
            db.flintel_signals.create_index([(field, ASCENDING)])

        # flintel_keywords — fetch-once-forever cache. UNCHANGED shape,
        # minus the search_volume fields (removed — no longer used).
        db.flintel_keywords.create_index([("keyword", ASCENDING)], unique=True, name="keyword_unique")
        db.flintel_keywords.create_index([("fetched", ASCENDING)], name="keyword_fetched_idx")
        db.flintel_keywords.create_index([("next_retry_at", ASCENDING)], name="keyword_retry_cooldown_idx")

        # flintel_google_posts — UNCHANGED schema/indexes from v9.12.
        db.flintel_google_posts.create_index(
            [("post_url", ASCENDING)], unique=True, name="google_post_url_unique"
        )
        db.flintel_google_posts.create_index(
            [("reddit_fetched", ASCENDING)], name="google_post_fetched_idx"
        )
        db.flintel_google_posts.create_index(
            [("next_retry_at", ASCENDING)], name="google_post_retry_cooldown_idx"
        )
        db.flintel_google_posts.create_index(
            [("subreddit", ASCENDING)], name="google_post_subreddit_idx"
        )
        db.flintel_google_posts.create_index(
            [("search_keyword", ASCENDING)], name="google_post_search_keyword_idx"
        )

        log.info("MongoDB connected.")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()


def log_operator_alert(title: str, detail: str, level: str = "ERROR"):
    log.log(
        logging.CRITICAL if level == "CRITICAL" else logging.ERROR,
        f"[OPERATOR ALERT] {title} — {detail}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD CACHE — flintel_keywords. Same fetch-once-forever behavior as
# v9.12, minus every search_volume-related field/function (removed).
# ─────────────────────────────────────────────────────────────────────────────

def sync_keywords_to_db(keywords: list):
    now = datetime.now(timezone.utc)
    for kw in keywords:
        try:
            db.flintel_keywords.update_one(
                {"keyword": kw},
                {"$setOnInsert": {
                    "keyword":         kw,
                    "fetched":         False,
                    "last_fetched_at": None,
                    "next_retry_at":   None,
                    "created_at":      now,
                }},
                upsert=True,
            )
        except Exception as exc:
            log.error(f"[KEYWORD-CACHE] sync error for {kw!r}: {exc}")


def get_due_keywords() -> list:
    try:
        now = datetime.now(timezone.utc)
        cursor = db.flintel_keywords.find({
            "fetched": False,
            "$or": [
                {"next_retry_at": None},
                {"next_retry_at": {"$exists": False}},
                {"next_retry_at": {"$lte": now}},
            ],
        })
        return list(cursor)
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] get_due_keywords error: {exc}")
        return []


def mark_keyword_fetched(keyword: str):
    now = datetime.now(timezone.utc)
    try:
        db.flintel_keywords.update_one(
            {"keyword": keyword},
            {"$set": {"fetched": True, "last_fetched_at": now}},
        )
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] mark_keyword_fetched error for {keyword!r}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — SOLE discovery mechanism: RapidAPI SERP search
# (site:reddit.com). UNCHANGED from v9.12 — same single RapidAPI call,
# same independent host, same try/except.
# ─────────────────────────────────────────────────────────────────────────────

def search_google_for_keyword(keyword: str, months_back: int = SERP_MONTHS_BACK) -> list:
    if not RAPIDAPI_KEY:
        log.warning("[SERP] RapidAPI key not set — skipping SERP search.")
        return []

    today = datetime.now(timezone.utc)
    date_from = today - timedelta(days=months_back * 30)
    cd_min = date_from.strftime("%m/%d/%Y")
    cd_max = today.strftime("%m/%d/%Y")

    query = f'site:reddit.com "{keyword}"'
    try:
        url = "https://google-search116.p.rapidapi.com/"

        querystring = {"query": query}

        headers = {
            "x-rapidapi-key": RAPIDAPI_KEY,  # .env
            "x-rapidapi-host": RAPIDAPI_SEARCH_HOST,
            "Content-Type": "application/json",
        }

        r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_SERP_TIMEOUT_SECONDS)

        try:
            result_data = r.json()
        except ValueError:
            log.error(f"[SERP] Non-JSON response for {keyword!r} | status:{r.status_code}")
            return []

        raw_items = _dig_list(result_data, RESULT_LIST_KEY_CANDIDATES)
        results = []
        rank_misses = 0
        for pos, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                continue
            item_url = item.get("url", "") or item.get("link", "")
            if "reddit.com" not in item_url:
                continue
            rank = _dig_value(item, RANK_FIELD_CANDIDATES)
            if rank is None:
                rank = pos
                rank_misses += 1
            results.append({
                "url":   item_url,
                "rank":  rank,
                "title": item.get("title", ""),
            })

        if rank_misses and rank_misses == len(results) and results:
            log.warning(
                f"[SERP] '{keyword}' — no explicit rank field found in any result "
                f"(tried {RANK_FIELD_CANDIDATES}); used result order as rank fallback."
            )

        log.info(
            f"[SERP] '{keyword}' → {len(results)} Reddit result(s) "
            f"(last {months_back} months: {cd_min} to {cd_max})"
        )
        return results

    except Exception as exc:
        log.error(f"[SERP] RapidAPI search error for {keyword!r}: {exc}")
        return []


def is_post_already_signaled(post_url: str) -> bool:
    """UNCHANGED — checks `flintel_signals` directly by post_url before
    any Reddit fetch happens."""
    if not post_url:
        return False
    try:
        existing = db.flintel_signals.find_one({"post_url": post_url}, {"_id": 1})
        return existing is not None
    except Exception as exc:
        log.error(f"[DEDUP] is_post_already_signaled error for {post_url}: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# flintel_google_posts HELPERS — UNCHANGED from v9.12.
# ─────────────────────────────────────────────────────────────────────────────

def save_google_post(post_url: str, google_rank, search_keyword: str, subreddit: str, fuzzy_keywords: list) -> bool:
    """Insert-only upsert — a post_url already tracked here is NEVER
    overwritten. Returns True only when this call genuinely inserted a
    brand-new document."""
    now = datetime.now(timezone.utc)
    try:
        result = db.flintel_google_posts.update_one(
            {"post_url": post_url},
            {"$setOnInsert": {
                "post_url":        post_url,
                "google_rank":     google_rank,
                "search_keyword":  search_keyword,
                "subreddit":       subreddit,
                "fuzzy_keywords":  fuzzy_keywords,
                "reddit_fetched":  False,
                "fuzzy_matched":   None,
                "next_retry_at":   None,
                "discovered_at":   now,
                "fetched_at":      None,
            }},
            upsert=True,
        )
        return result.upserted_id is not None
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] save_google_post error for {post_url}: {exc}")
        return False


def get_due_google_posts() -> list:
    """Returns every flintel_google_posts document still
    reddit_fetched=False AND not currently in a retry cooldown."""
    try:
        now = datetime.now(timezone.utc)
        cursor = db.flintel_google_posts.find({
            "reddit_fetched": False,
            "$or": [
                {"next_retry_at": None},
                {"next_retry_at": {"$exists": False}},
                {"next_retry_at": {"$lte": now}},
            ],
        })
        return list(cursor)
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] get_due_google_posts error: {exc}")
        return []


def mark_google_post_fetched(post_url: str, fuzzy_matched):
    """Flips reddit_fetched=True PERMANENTLY for this post_url."""
    now = datetime.now(timezone.utc)
    try:
        db.flintel_google_posts.update_one(
            {"post_url": post_url},
            {"$set": {
                "reddit_fetched": True,
                "fetched_at":     now,
                "fuzzy_matched":  fuzzy_matched,
            }},
        )
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] mark_google_post_fetched error for {post_url}: {exc}")


def set_google_post_retry_cooldown(post_url: str, cooldown_seconds: int = REDDIT_POST_RETRY_COOLDOWN_SECONDS):
    """Called when a specific post_url's Reddit RSS fetch genuinely
    failed. Keeps reddit_fetched=False but stamps next_retry_at."""
    now = datetime.now(timezone.utc)
    next_retry = now + timedelta(seconds=cooldown_seconds)
    try:
        db.flintel_google_posts.update_one(
            {"post_url": post_url},
            {"$set": {"next_retry_at": next_retry}},
        )
        log.info(
            f"[GOOGLE-POSTS] '{post_url}' cooldown set | next_retry_at:{next_retry.isoformat()} "
            f"({cooldown_seconds}s from now) — will not be re-attempted before then"
        )
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] set_google_post_retry_cooldown error for {post_url}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT POST FETCH — public, credential-free per-post RSS feed ONLY.
# UNCHANGED retry/backoff/parsing behavior from v9.12/v9.11. The ONLY
# change: no more random-fallback upvotes/comments — those fields are
# simply not generated or stored anymore.
# ─────────────────────────────────────────────────────────────────────────────

def _reddit_get_with_retry(url: str) -> requests.Response | None:
    headers = {
        "User-Agent": REDDIT_USER_AGENT,
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    }

    last_status = None
    for attempt in range(1, REDDIT_FETCH_MAX_RETRIES + 1):
        time.sleep(random.uniform(REDDIT_FETCH_JITTER_MIN, REDDIT_FETCH_JITTER_MAX))
        try:
            r = requests.get(url, headers=headers, timeout=REDDIT_JSON_TIMEOUT_SECONDS)
            last_status = r.status_code
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                log.debug(f"[REDDIT-FETCH] 404 (gone) for {url} — not retrying.")
                return None
            if r.status_code in (403, 429) or r.status_code >= 500:
                wait = (REDDIT_FETCH_BACKOFF_BASE ** attempt) + random.uniform(0, 1.0)
                log.warning(
                    f"[REDDIT-FETCH] Reddit fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                    f"got {r.status_code} for {url} — backing off {wait:.1f}s..."
                )
                time.sleep(wait)
                continue
            log.error(f"[REDDIT-FETCH] Unexpected status {r.status_code} for {url}")
            return None
        except requests.RequestException as exc:
            log.warning(
                f"[REDDIT-FETCH] Reddit fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                f"network error for {url}: {exc}"
            )
            time.sleep((REDDIT_FETCH_BACKOFF_BASE ** attempt))

    log.error(f"[REDDIT-FETCH] Reddit fetch exhausted {REDDIT_FETCH_MAX_RETRIES} attempts for {url} "
              f"(last_status:{last_status})")
    return None


def _extract_reddit_submission_id(post_url: str) -> str | None:
    match = re.search(r"/comments/([a-zA-Z0-9]+)", post_url)
    return match.group(1) if match else None


def _extract_reddit_subreddit_from_url(post_url: str) -> str:
    match = re.search(r"reddit\.com/r/([^/]+)/", post_url)
    return match.group(1) if match else ""


def fetch_reddit_post_by_url(post_url: str, keyword: str, rank: int) -> dict | None:
    """UNCHANGED retry/fallback behavior. No more upvotes/comments —
    those fields are gone; only real, fetched content is returned."""
    if not post_url:
        return None

    primary_url = post_url.rstrip("/") + ".rss"
    r = _reddit_get_with_retry(primary_url)

    if r is None and "old.reddit.com" not in post_url:
        fallback_url = (
            post_url.rstrip("/")
            .replace("https://www.reddit.com", "https://old.reddit.com")
            .replace("https://reddit.com", "https://old.reddit.com")
            + ".rss"
        )
        if fallback_url != primary_url:
            log.info(f"[REDDIT-FETCH] Retrying via old.reddit.com fallback: {fallback_url}")
            r = _reddit_get_with_retry(fallback_url)

    if r is None:
        log.error(f"[REDDIT-FETCH] fetch_reddit_post_by_url gave up for {post_url}")
        return None

    try:
        feed = feedparser.parse(r.content)
        if not feed.entries:
            log.error(f"[REDDIT-FETCH] fetch_reddit_post_by_url: RSS feed had no entries for {post_url}")
            return None

        entry = feed.entries[0]

        title = (entry.get("title", "") or "").strip()
        raw_summary = entry.get("summary", "") or ""
        if not raw_summary and entry.get("content"):
            raw_summary = entry["content"][0].get("value", "") or ""
        summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(raw_summary)).strip()

        text = title
        if summary_plain and summary_plain.lower() != title.lower():
            text = f"{title}\n\n{summary_plain}"

        author = (entry.get("author", "") or "unknown").lstrip("u/").lstrip("/u/").strip() or "unknown"
        subreddit = _extract_reddit_subreddit_from_url(post_url)

        posted_at = None
        published = entry.get("published") or entry.get("updated")
        if published:
            try:
                posted_at = datetime(*entry.get("published_parsed", entry.get("updated_parsed"))[:6],
                                      tzinfo=timezone.utc).isoformat()
            except (TypeError, ValueError):
                posted_at = published

        submission_id = _extract_reddit_submission_id(post_url)
        message_id = f"reddit_serp_{submission_id}" if submission_id else (
            f"reddit_serp_{re.sub(r'[^a-zA-Z0-9]', '_', post_url)[-40:]}"
        )

        return {
            "message_id":           message_id,
            "platform":             "reddit",
            "text":                 text,
            "username":             author,
            "subreddit_or_channel": subreddit,
            "post_url":             post_url,
            "posted_at":            posted_at,
            "search_keyword":       keyword,
            "google_rank":          rank,
        }
    except Exception as exc:
        log.error(f"[REDDIT-FETCH] fetch_reddit_post_by_url parse error for {post_url}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL STORAGE — direct save into flintel_signals, no batching, no
# scoring. This is what replaces the old queue -> Claude -> save flow.
# ─────────────────────────────────────────────────────────────────────────────

def save_signal(item: dict) -> bool:
    doc = {
        "message_id":           item["message_id"],
        "platform":             item.get("platform", "reddit"),
        "post_url":             item.get("post_url", ""),
        "text":                 item.get("text", ""),
        "username":             item.get("username", "unknown"),
        "subreddit_or_channel": item.get("subreddit_or_channel", ""),
        "posted_at":            item.get("posted_at"),
        "fetched_at":           datetime.now(timezone.utc),
        "google_rank":          item.get("google_rank"),
        "search_keyword":       item.get("search_keyword", ""),
        "client_id":            CLIENT_ID,
        "created_at":           datetime.now(timezone.utc),
    }
    try:
        db.flintel_signals.insert_one(doc)
        log.info(
            f"SAVED [{doc['platform'].upper()}] search_keyword={doc['search_keyword']!r} | "
            f"subreddit:{doc['subreddit_or_channel']!r} | google_rank:{doc['google_rank']} | "
            f"post_url:{doc['post_url']}"
        )
        return True
    except DuplicateKeyError:
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        log_operator_alert("MongoDB Write Failed", str(exc), level="CRITICAL")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SERP DISCOVERY — process_one_keyword() ONLY runs the Google SERP call
# and persists results into flintel_google_posts. Reddit is NEVER
# fetched here — this keyword's SERP job is done the moment this
# function returns.
# ─────────────────────────────────────────────────────────────────────────────

def process_one_keyword(keyword: str) -> tuple:
    results = search_google_for_keyword(keyword, months_back=SERP_MONTHS_BACK)

    new_posts_saved = 0
    for result in results[:SERP_RESULTS_PER_KEYWORD]:
        post_url = result["url"]
        subreddit = _extract_reddit_subreddit_from_url(post_url)
        fuzzy_keywords = generate_fuzzy_keywords(keyword)

        was_new = save_google_post(
            post_url=post_url,
            google_rank=result["rank"],
            search_keyword=keyword,
            subreddit=subreddit,
            fuzzy_keywords=fuzzy_keywords,
        )
        if was_new:
            new_posts_saved += 1

    return len(results), new_posts_saved


def run_serp_discovery_loop():
    sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

    log.info(
        f"[SERP] Discovery loop started | {len(REDDIT_SEARCH_KEYWORDS)} keyword(s) in python list | "
        f"check_interval:{KEYWORD_CHECK_INTERVAL_SECONDS}s | "
        f"months_back:{SERP_MONTHS_BACK} | depth:{SERP_RESULTS_PER_KEYWORD} | "
        f"KEYWORD CACHE: fetch-once-forever, restart-safe, no re-fetch ever | "
        f"REDDIT FETCH: fully decoupled — SERP results are only SAVED into "
        f"flintel_google_posts here, the actual Reddit RSS fetch happens in a separate loop"
    )

    while True:
        try:
            sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

            due = get_due_keywords()
            if not due:
                time.sleep(KEYWORD_CHECK_INTERVAL_SECONDS)
                continue

            total_results, total_new_posts = 0, 0
            for doc in due:
                keyword = doc["keyword"]
                results_count, new_posts_saved = process_one_keyword(keyword)
                total_results += results_count
                total_new_posts += new_posts_saved

                mark_keyword_fetched(keyword)
                log.info(
                    f"[SERP] '{keyword}' DONE | serp_results:{results_count} | "
                    f"new_google_posts_saved:{new_posts_saved} | marked fetched=True PERMANENTLY"
                )
                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[SERP] Pass complete | keywords_processed:{len(due)} | "
                f"total_serp_results:{total_results} | new_google_posts_saved:{total_new_posts}"
            )

        except Exception as exc:
            log.error(f"[SERP] discovery loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT FETCH LOOP — reads flintel_google_posts directly, fetches RSS,
# fuzzy-filters, and on a match SAVES DIRECTLY into flintel_signals. No
# queue, no batching, no Claude call anywhere in this loop.
# ─────────────────────────────────────────────────────────────────────────────

def run_reddit_fetch_loop():
    log.info(
        f"[REDDIT-FETCH] Loop started | reads directly from flintel_google_posts | "
        f"check_interval:{REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s | "
        f"retry_cooldown:{REDDIT_POST_RETRY_COOLDOWN_SECONDS}s | "
        f"fetch method: public per-post RSS only, credential-free "
        f"({REDDIT_FETCH_MAX_RETRIES}x backoff + old.reddit.com fallback, no OAuth/PRAW) | "
        f"on fuzzy match -> saved DIRECTLY into flintel_signals, no queue/batch/Claude"
    )

    while True:
        try:
            due_posts = get_due_google_posts()
            if not due_posts:
                time.sleep(REDDIT_FETCH_CHECK_INTERVAL_SECONDS)
                continue

            log.info(f"[REDDIT-FETCH] {len(due_posts)} post(s) due for Reddit RSS fetch this pass")

            saved_count, no_match_count, dupe_count, fail_count = 0, 0, 0, 0

            for doc in due_posts:
                post_url       = doc["post_url"]
                search_keyword = doc.get("search_keyword", "")
                fuzzy_keywords = doc.get("fuzzy_keywords", [])
                subreddit      = doc.get("subreddit", "")
                google_rank    = doc.get("google_rank")

                if is_post_already_signaled(post_url):
                    mark_google_post_fetched(post_url, fuzzy_matched=None)
                    dupe_count += 1
                    log.info(f"[REDDIT-FETCH] SKIP (already in flintel_signals) | {post_url}")
                    continue

                item = fetch_reddit_post_by_url(post_url, search_keyword, google_rank)
                if not item:
                    set_google_post_retry_cooldown(post_url)
                    fail_count += 1
                    log.warning(
                        f"[REDDIT-FETCH] fetch FAILED (retries exhausted) | {post_url} | "
                        f"left reddit_fetched=False — will retry after cooldown"
                    )
                    time.sleep(SERP_FETCH_SLEEP_SECONDS)
                    continue

                matched = passes_fuzzy_filter(item.get("text", ""), search_keyword, fuzzy_keywords)
                if not matched:
                    mark_google_post_fetched(post_url, fuzzy_matched=False)
                    no_match_count += 1
                    log.info(
                        f"[REDDIT-FETCH] fetched OK but NO fuzzy-keyword match | {post_url} | "
                        f"keyword:{search_keyword!r} | fuzzy_keywords_tried:{len(fuzzy_keywords)} | "
                        f"marked reddit_fetched=True (settled 'no', won't be retried)"
                    )
                    time.sleep(SERP_FETCH_SLEEP_SECONDS)
                    continue

                # ── MATCH — save straight into flintel_signals, tagged
                # with its search_keyword. No queue, no batch, no Claude.
                item["subreddit_or_channel"] = subreddit or item.get("subreddit_or_channel", "")
                saved = save_signal(item)
                mark_google_post_fetched(post_url, fuzzy_matched=True)
                if saved:
                    saved_count += 1

                log.info(
                    f"[REDDIT-FETCH] {'SAVED' if saved else 'DUPLICATE (already existed)'} | {post_url} | "
                    f"keyword:{search_keyword!r} | subreddit:{subreddit!r} | google_rank:{google_rank} | "
                    f"marked reddit_fetched=True PERMANENTLY"
                )
                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[REDDIT-FETCH] Pass complete | due:{len(due_posts)} | saved:{saved_count} | "
                f"no_fuzzy_match:{no_match_count} | already_signaled:{dupe_count} | "
                f"failed_will_retry:{fail_count}"
            )

        except Exception as exc:
            log.error(f"[REDDIT-FETCH] loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS — thread management + auto-restart
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    """Reddit runs on TWO independent threads:
      1. SERP discovery (run_serp_discovery_loop) — Google call, saves
         results into flintel_google_posts.
      2. Reddit fetch (run_reddit_fetch_loop) — reads
         flintel_google_posts directly, fetches RSS, fuzzy-filters,
         saves matches straight into flintel_signals.
    No batch/Claude thread anymore — nothing left to score."""
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED — skipping.")
        return
    if not RAPIDAPI_KEY:
        log.warning("Reddit not started — RAPIDAPI_KEY not set (required for SERP discovery).")
        return

    serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
    fetch_thread = threading.Thread(target=run_reddit_fetch_loop, daemon=True, name="Reddit-Fetch")
    serp_thread.start()
    fetch_thread.start()
    log.info("Reddit threads running: SERP-Discovery ✅ | Reddit-Fetch ✅")

    while True:
        await asyncio.sleep(60)
        if not serp_thread.is_alive():
            log.error("Reddit SERP thread died — restarting...")
            serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
            serp_thread.start()
        if not fetch_thread.is_alive():
            log.error("Reddit Fetch thread died — restarting...")
            fetch_thread = threading.Thread(target=run_reddit_fetch_loop, daemon=True, name="Reddit-Fetch")
            fetch_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI — read-only endpoints
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Flintel main.py — Reddit-only (Google SERP discovery -> flintel_google_posts -> Reddit RSS fetch -> flintel_signals, no Claude, no search_volume, no engagement, no Twitter)",
    version="1.0.0",
)


def _serialise(signals: list) -> list:
    for s in signals:
        s.pop("_id", None)
        for f in ["created_at", "fetched_at"]:
            if s.get(f):
                s[f] = s[f].isoformat()
    return signals


@app.get("/")
def root():
    total_keywords_tracked = db.flintel_keywords.count_documents({})
    due_now_count = db.flintel_keywords.count_documents({"fetched": False})

    total_google_posts = db.flintel_google_posts.count_documents({})
    pending_reddit_fetch = db.flintel_google_posts.count_documents({"reddit_fetched": False})
    fetched_reddit_posts = db.flintel_google_posts.count_documents({"reddit_fetched": True})
    fuzzy_matched_posts  = db.flintel_google_posts.count_documents({"fuzzy_matched": True})
    fuzzy_no_match_posts = db.flintel_google_posts.count_documents({"fuzzy_matched": False})

    return {
        "status":                  "running",
        "system":                  "Flintel main.py — Reddit-only signal pipeline (no Claude, no search_volume, no engagement, no Twitter)",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "reddit_fetch_method":     "public per-post RSS (credential-free, smart-retry + old.reddit.com fallback) — no OAuth/PRAW, no .json endpoint anywhere",
        "reddit_search_keywords":  len(REDDIT_SEARCH_KEYWORDS),
        "keyword_check_interval_seconds": KEYWORD_CHECK_INTERVAL_SECONDS,
        "keyword_cache":           "ENABLED — fetch-once-forever, restart-safe (flintel_keywords)",
        "google_posts_collection":            "flintel_google_posts",
        "google_posts_tracked":               total_google_posts,
        "google_posts_pending_reddit_fetch":  pending_reddit_fetch,
        "google_posts_reddit_fetched":        fetched_reddit_posts,
        "google_posts_fuzzy_matched":         fuzzy_matched_posts,
        "google_posts_fuzzy_no_match":        fuzzy_no_match_posts,
        "reddit_fetch_check_interval_seconds": REDDIT_FETCH_CHECK_INTERVAL_SECONDS,
        "reddit_post_retry_cooldown_seconds":  REDDIT_POST_RETRY_COOLDOWN_SECONDS,
        "keywords_tracked":        total_keywords_tracked,
        "keywords_due_now":        due_now_count,
        "serp_months_back":        SERP_MONTHS_BACK,
        "serp_results_per_kw":     SERP_RESULTS_PER_KEYWORD,
        "rapidapi_configured":     bool(RAPIDAPI_KEY),
        "auth_required":           bool(API_KEY),
        "claude_removed":          True,
        "search_volume_removed":   True,
        "engagement_removed":      True,
        "twitter_removed":         True,
        "batching_removed":        True,
        "signals_saved_directly":  True,
    }


@app.get("/health")
def health():
    try:
        db.command("ping")
        mongo = "connected"
    except Exception:
        mongo = "disconnected"

    return {
        "status":                  "ok",
        "mongodb":                 mongo,
        "reddit_working":          REDDIT_ENABLED and bool(RAPIDAPI_KEY),
        "reddit_indicator":        _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "google_posts_pending_reddit_fetch": db.flintel_google_posts.count_documents({"reddit_fetched": False}),
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


@app.get("/keywords", dependencies=[Depends(verify_api_key)])
def get_keywords_status():
    raw_docs = list(db.flintel_keywords.find({}, {"_id": 0}).sort("keyword", 1))
    due_count = 0
    docs = []
    for d in raw_docs:
        is_due = not d.get("fetched")
        if is_due:
            due_count += 1
        for f in ["last_fetched_at", "created_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()
        d["due_now"] = is_due
        docs.append(d)
    return {"total": len(docs), "due_now": due_count, "keywords": docs}


@app.get("/google-posts", dependencies=[Depends(verify_api_key)])
def get_google_posts_status(reddit_fetched: bool = None, fuzzy_matched: bool = None, limit: int = 200):
    q: dict = {}
    if reddit_fetched is not None:
        q["reddit_fetched"] = reddit_fetched
    if fuzzy_matched is not None:
        q["fuzzy_matched"] = fuzzy_matched

    docs = list(db.flintel_google_posts.find(q, {"_id": 0}).sort("discovered_at", -1).limit(limit))
    for d in docs:
        for f in ["discovered_at", "fetched_at", "next_retry_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()

    total = db.flintel_google_posts.count_documents({})
    pending = db.flintel_google_posts.count_documents({"reddit_fetched": False})
    fetched = db.flintel_google_posts.count_documents({"reddit_fetched": True})
    matched = db.flintel_google_posts.count_documents({"fuzzy_matched": True})
    no_match = db.flintel_google_posts.count_documents({"fuzzy_matched": False})

    return {
        "total": total,
        "pending_reddit_fetch": pending,
        "reddit_fetched": fetched,
        "fuzzy_matched": matched,
        "fuzzy_no_match": no_match,
        "returned": len(docs),
        "posts": docs,
    }


@app.get("/signals", dependencies=[Depends(verify_api_key)])
def get_signals(limit: int = 50, search_keyword: str = None, platform: str = None):
    q: dict = {"client_id": CLIENT_ID}
    if search_keyword:
        q["search_keyword"] = search_keyword
    if platform:
        q["platform"] = platform
    signals = list(db.flintel_signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    return {"count": len(signals), "signals": _serialise(signals)}


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    api_thread = threading.Thread(target=run_fastapi, daemon=True, name="FastAPI")
    api_thread.start()
    log.info("FastAPI running at http://0.0.0.0:8000")

    await asyncio.gather(
        start_reddit_listener(),
    )


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FLINTEL main.py — REDDIT-ONLY SIGNAL PIPELINE")
    log.info("  (Google SERP discovery -> flintel_google_posts -> Reddit RSS fetch")
    log.info("   -> flintel_signals, saved directly, tagged with search_keyword)")
    log.info("  Claude / search_volume / engagement / Twitter / batching: REMOVED")
    log.info("=" * 70)
    log.info(f"  Client                : {CLIENT_ID}")
    log.info(f"  Reddit                : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED and bool(RAPIDAPI_KEY))}")
    log.info(f"  Reddit fetch method   : public per-post RSS only — credential-free, no OAuth/PRAW, no .json anywhere")
    log.info(f"  Reddit keywords       : {len(REDDIT_SEARCH_KEYWORDS)} (used ONLY to seed brand-new flintel_keywords docs)")
    log.info(f"  Keyword cache         : flintel_keywords — fetch-once-forever")
    log.info(f"  Google SERP           : search_google_for_keyword() — unchanged single RapidAPI call")
    log.info(f"  flintel_google_posts  : stores post_url + google_rank + search_keyword + subreddit + auto fuzzy_keywords + reddit_fetched")
    log.info(f"  Reddit fetch interval : check every {REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s | retry cooldown {REDDIT_POST_RETRY_COOLDOWN_SECONDS}s on genuine fetch failure")
    log.info(f"  Fuzzy keywords        : Python auto-generated per SERP result at save time — used to filter fetched RSS content")
    log.info(f"  Signal storage        : direct save into flintel_signals on fuzzy match — NO queue, NO batch, NO Claude")
    log.info(f"  RapidAPI config       : {bool(RAPIDAPI_KEY)} (SOLE provider — Google SERP discovery only now)")
    log.info(f"  MongoDB DB            : {MONGODB_DB}")
    log.info(f"  API auth              : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info("=" * 70)

    asyncio.run(main())
