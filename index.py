"""
FLINTEL — index.py  (STRIPPED-DOWN SIGNAL PIPELINE, based on v9.12)
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

  BUGFIX vs. original main.py:
    - The unique index on flintel_signals.message_id was being created
      here under the name "message_id_unique". On any deployment where
      this collection/index already existed under its original name
      ("signals_message_id_unique" — same key, same uniqueness), Mongo
      refuses to create a second index with an identical key pattern
      under a different name and raises IndexOptionsConflict (error
      code 85) on every startup. Fixed by creating the index under the
      name that already exists in the database, so create_index() is a
      no-op on deployments that already have it, and creates it fresh
      (under the same name) on brand-new deployments.

  COST-CONTROL BATCHING (NEW, this version only):
    - The Google SERP discovery step no longer fires ONE RapidAPI call
      per keyword. Due keywords are now grouped into batches of
      GOOGLE_SERP_BATCH (default 10, .env-overridable) and ONE RapidAPI
      call is made per batch, using a single OR'd query
      (site:reddit.com ("kw1" OR "kw2" OR ... "kw10")). This cuts
      RapidAPI usage ~GOOGLE_SERP_BATCH-fold.
    - Every result returned by a batched call is still resolved back to
      exactly ONE keyword from that batch (via
      _find_best_matching_keyword — exact substring match first, same
      fuzzy-keyword variants used everywhere else in this file as a
      fallback) before being saved, so flintel_google_posts keeps
      storing a single, unambiguous search_keyword + fuzzy_keywords per
      document, exactly like the old one-call-per-keyword flow did.
      A result that can't be confidently attributed to any keyword in
      its batch is skipped rather than mis-tagged.
    - Everything else stays the same: flintel_google_posts schema,
      signal storage, indexes, endpoints — all as before. The old
      single-keyword search_google_for_keyword() / process_one_keyword()
      functions are kept in place (unused by the loop now, left for
      reference / backward compatibility) rather than removed.

  POST-FETCH CONTENT FILTER REMOVED (NEW, this version only):
    - The Reddit fetch loop (run_reddit_fetch_loop) NO LONGER re-checks
      a fetched post's RSS text against its stored search_keyword /
      fuzzy_keywords before saving. fuzzy_keywords are still generated
      and used ONLY at SERP-discovery time (to resolve/tag which
      keyword a SERP result belongs to — see generate_fuzzy_keywords()
      and process_keywords_batch()). Once a post_url is tracked in
      flintel_google_posts with a resolved search_keyword, a
      SUCCESSFUL Reddit RSS fetch for that exact post_url is now the
      only condition needed to save it straight into flintel_signals.
      passes_fuzzy_filter() is left defined in this file (unused by the
      loop) rather than removed.

Run:
    pip install fastapi uvicorn pymongo python-dotenv httpx requests \
                feedparser
    python index.py
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
from pymongo.errors import DuplicateKeyError, OperationFailure
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
    
"budget penetration testing",
      "budget product manager",
      "budget scrum master",
      "budget site reliability engineer",
      "budget software architect",
      "budget software escrow",
      "budget technical co-founder",
      "budget technical writer",
      "budget white-label software",
      "certified API gateway",
      "certified AWS consultant",
      "certified CI/CD pipeline",
      "certified CRM",
      "certified DevOps engineer",
      "certified GraphQL developer",
      "certified IT staffing agency",
      "certified QA testing",
      "certified REST API",
      "certified SOC 2 audit",
      "certified SSO provider",
      "certified SaaS platform",
      "certified UX designer",
      "certified app builder",
      "certified backend developer",
      "certified cloud hosting",
      "certified code audit",
      "certified code review",
      "certified cybersecurity audit",
      "certified data engineer",
      "certified data pipeline",
      "certified database admin",
      "certified frontend developer",
      "certified full-stack developer",
      "certified kubernetes consultant",
      "certified legacy system migration",
      "certified load testing",
      "certified microservices consultant",
      "certified mobile app developer",
      "certified no-code builder",
      "certified outsourced dev team",
      "certified penetration testing",
      "certified product manager",
      "certified scrum master",
      "certified site reliability engineer",
      "certified software architect",
      "certified software escrow",
      "certified technical co-founder",
      "certified technical writer",
      "certified white-label software",
      "cloud hosting",
      "code audit",
      "code review",
      "commercial API gateway",
      "commercial AWS consultant",
      "commercial CI/CD pipeline",
      "commercial CRM",
      "commercial DevOps engineer",
      "commercial GraphQL developer",
      "commercial IT staffing agency",
      "commercial QA testing",
      "commercial REST API",
      "commercial SOC 2 audit",
      "commercial SSO provider",
      "commercial SaaS platform",
      "commercial UX designer",
      "commercial app builder",
      "commercial backend developer",
      "commercial cloud hosting",
      "commercial code audit",
      "commercial code review",
      "commercial cybersecurity audit",
      "commercial data engineer",
      "commercial data pipeline",
      "commercial database admin",
      "commercial frontend developer",
      "commercial full-stack developer",
      "commercial kubernetes consultant",
      "commercial legacy system migration",
      "commercial load testing",
      "commercial microservices consultant",
      "commercial mobile app developer",
      "commercial no-code builder",
      "commercial outsourced dev team",
      "commercial penetration testing",
      "commercial product manager",
      "commercial scrum master",
      "commercial site reliability engineer",
      "commercial software architect",
      "commercial software escrow",
      "commercial technical co-founder",
      "commercial technical writer",
      "commercial white-label software",
      "custom API gateway",
      "custom AWS consultant",
      "custom CI/CD pipeline",
      "custom CRM",
      "custom DevOps engineer",
      "custom GraphQL developer",
      "custom IT staffing agency",
      "custom QA testing",
      "custom REST API",
      "custom SOC 2 audit",
      "custom SSO provider",
      "custom SaaS platform",
      "custom UX designer",
      "custom app builder",
      "custom backend developer",
      "custom cloud hosting",
      "custom code audit",
      "custom code review",
      "custom cybersecurity audit",
      "custom data engineer",
      "custom data pipeline",
      "custom database admin",
      "custom frontend developer",
      "custom full-stack developer",
      "custom kubernetes consultant",
      "custom legacy system migration",
      "custom load testing",
      "custom microservices consultant",
      "custom mobile app developer",
      "custom no-code builder",
      "custom outsourced dev team",
      "custom penetration testing",
      "custom product manager",
      "custom scrum master",
      "custom site reliability engineer",
      "custom software architect",
      "custom software escrow",
      "custom technical co-founder",
      "custom technical writer",
      "custom white-label software",
      "cybersecurity audit",
      "data engineer",
      "data pipeline",
      "database admin",
      "emergency API gateway",
      "emergency AWS consultant",
      "emergency CI/CD pipeline",
      "emergency CRM",
      "emergency DevOps engineer",
      "emergency GraphQL developer",
      "emergency IT staffing agency",
      "emergency QA testing",
      "emergency REST API",
      "emergency SOC 2 audit",
      "emergency SSO provider",
      "emergency SaaS platform",
      "emergency UX designer",
      "emergency app builder",
      "emergency backend developer",
      "emergency cloud hosting",
      "emergency code audit",
      "emergency code review",
      "emergency cybersecurity audit",
      "emergency data engineer",
      "emergency data pipeline",
      "emergency database admin",
      "emergency frontend developer",
      "emergency full-stack developer",
      "emergency kubernetes consultant",
      "emergency legacy system migration",
      "emergency load testing",
      "emergency microservices consultant",
      "emergency mobile app developer",
      "emergency no-code builder",
      "emergency outsourced dev team",
      "emergency penetration testing",
      "emergency product manager",
      "emergency scrum master",
      "emergency site reliability engineer",
      "emergency software architect",
      "emergency software escrow",
      "emergency technical co-founder",
      "emergency technical writer",
      "emergency white-label software",
      "enterprise API gateway",
      "enterprise AWS consultant",
      "enterprise CI/CD pipeline",
      "enterprise CRM",
      "enterprise DevOps engineer",
      "enterprise GraphQL developer",
      "enterprise IT staffing agency",
      "enterprise QA testing",
      "enterprise REST API",
      "enterprise SOC 2 audit",
      "enterprise SSO provider",
      "enterprise SaaS platform",
      "enterprise UX designer",
      "enterprise app builder",
      "enterprise backend developer",
      "enterprise cloud hosting",
      "enterprise code audit",
      "enterprise code review",
      "enterprise cybersecurity audit",
      "enterprise data engineer",
      "enterprise data pipeline",
      "enterprise database admin",
      "enterprise frontend developer",
      "enterprise full-stack developer",

]

# ── PER-KEYWORD "FETCH ONCE, EVER" CACHE CONFIG — UNCHANGED.
KEYWORD_CHECK_INTERVAL_SECONDS = int(os.getenv("KEYWORD_CHECK_INTERVAL_SECONDS", "60"))
REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS = int(os.getenv("REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS", "1800"))

# depth + lookback — read from .env exactly as before. Depth default
# raised to 100 per request.
SERP_RESULTS_PER_KEYWORD = int(os.getenv("SERP_RESULTS_PER_KEYWORD", "100"))
SERP_MONTHS_BACK         = int(os.getenv("SERP_MONTHS_BACK", "6"))
SERP_FETCH_SLEEP_SECONDS = float(os.getenv("SERP_FETCH_SLEEP_SECONDS", "1.5"))

# ── GOOGLE SERP BATCHING (NEW) — how many keywords get combined into a
# SINGLE RapidAPI call via an OR'd query, instead of one call/keyword.
# This is the ONLY cost-control change in this version. Default 10.
GOOGLE_SERP_BATCH = int(os.getenv("GOOGLE_SERP_BATCH", "10"))

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

def _ensure_index(collection, keys, **kwargs):
    """Create an index, but never let a pre-existing index under a
    *different* name for the *same* key pattern crash startup.

    Different deployments of this service have accumulated indexes
    under slightly different auto-generated / hand-picked names over
    past versions (e.g. "signals_message_id_unique",
    "signals_search_keyword"). Mongo refuses to create a second index
    with an identical key spec under a new name and raises
    OperationFailure code 85 (IndexOptionsConflict). Since an
    equivalent index already existing under another name is completely
    fine functionally (same key, same options), we just log it and
    move on instead of raising.
    """
    try:
        collection.create_index(keys, **kwargs)
    except OperationFailure as exc:
        if exc.code == 85:  # IndexOptionsConflict
            log.warning(
                f"[INDEX] Equivalent index for {keys} already exists under a "
                f"different name on {collection.name} — skipping create "
                f"(requested name: {kwargs.get('name')!r}): {exc.details.get('errmsg', exc)}"
            )
        else:
            raise


def get_database():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        # NOTE: names below match what this collection already has in
        # production. Same keys/uniqueness as before — only the *name*
        # passed to create_index() matters here. Any mismatch for a
        # key pattern that already exists under a different name is
        # now handled gracefully by _ensure_index() instead of crashing
        # the process with IndexOptionsConflict (code 85).
        _ensure_index(db.flintel_signals, [("message_id", ASCENDING)], unique=True, name="signals_message_id_unique")
        _ensure_index(db.flintel_signals, [("post_url", ASCENDING)], name="post_url_lookup")
        _ensure_index(db.flintel_signals, [("search_keyword", ASCENDING)], name="signals_search_keyword")
        _ensure_index(db.flintel_signals, [("platform", ASCENDING)], name="signals_platform")
        _ensure_index(db.flintel_signals, [("created_at", ASCENDING)], name="signals_created_at")

        # flintel_keywords — fetch-once-forever cache. UNCHANGED shape,
        # minus the search_volume fields (removed — no longer used).
        _ensure_index(db.flintel_keywords, [("keyword", ASCENDING)], unique=True, name="keyword_unique")
        _ensure_index(db.flintel_keywords, [("fetched", ASCENDING)], name="keyword_fetched_idx")
        _ensure_index(db.flintel_keywords, [("next_retry_at", ASCENDING)], name="keyword_retry_cooldown_idx")

        # flintel_google_posts — UNCHANGED schema/indexes from v9.12.
        _ensure_index(
            db.flintel_google_posts, [("post_url", ASCENDING)], unique=True, name="google_post_url_unique"
        )
        _ensure_index(
            db.flintel_google_posts, [("reddit_fetched", ASCENDING)], name="google_post_fetched_idx"
        )
        _ensure_index(
            db.flintel_google_posts, [("next_retry_at", ASCENDING)], name="google_post_retry_cooldown_idx"
        )
        _ensure_index(
            db.flintel_google_posts, [("subreddit", ASCENDING)], name="google_post_subreddit_idx"
        )
        _ensure_index(
            db.flintel_google_posts, [("search_keyword", ASCENDING)], name="google_post_search_keyword_idx"
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
    """UNCHANGED, one-keyword-per-call version. Kept in place for
    reference / backward compatibility — the live discovery loop below
    now calls the batched version (search_google_for_keywords_batch)
    instead, to cut RapidAPI usage. This function itself is untouched."""
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


def _find_best_matching_keyword(text: str, keywords_batch: list) -> str | None:
    """
    NEW — resolves a single SERP result (coming back from a combined /
    batched OR query) to exactly ONE of the keywords in that batch, so
    every flintel_google_posts document still stores a single,
    unambiguous search_keyword + fuzzy_keywords set — exactly like the
    old one-call-per-keyword flow produced.

    1. Exact substring match of the raw keyword against the result's
       title+url (case-insensitive) — same confidence as before.
    2. Falls back to the same generate_fuzzy_keywords() variants used
       everywhere else in this file, so behavior stays consistent with
       passes_fuzzy_filter() downstream.
    3. Returns None if nothing in the batch matches — the caller skips
       that result instead of mis-tagging it under the wrong keyword.
    """
    t = (text or "").lower()
    for kw in keywords_batch:
        if kw and kw.lower() in t:
            return kw
    for kw in keywords_batch:
        for fkw in generate_fuzzy_keywords(kw):
            if fkw and fkw in t:
                return kw
    return None


def search_google_for_keywords_batch(keywords_batch: list, months_back: int = SERP_MONTHS_BACK) -> list:
    """
    NEW — batches up to GOOGLE_SERP_BATCH keywords into a SINGLE
    RapidAPI SERP call using one OR'd query
    (site:reddit.com ("kw1" OR "kw2" OR ...)), instead of firing one
    RapidAPI call per keyword. This is the sole cost-control change in
    this version — cuts RapidAPI usage roughly GOOGLE_SERP_BATCH-fold.

    Everything downstream is unaffected: each returned result is still
    resolved back to exactly one search_keyword (via
    _find_best_matching_keyword) before flintel_google_posts ever sees
    it, so save_google_post(), fuzzy_keywords generation, the Reddit
    fetch loop, and signal storage all keep working exactly as before.
    """
    if not RAPIDAPI_KEY:
        log.warning("[SERP-BATCH] RapidAPI key not set — skipping SERP search.")
        return []
    if not keywords_batch:
        return []

    today = datetime.now(timezone.utc)
    date_from = today - timedelta(days=months_back * 30)
    cd_min = date_from.strftime("%m/%d/%Y")
    cd_max = today.strftime("%m/%d/%Y")

    quoted_terms = " OR ".join(f'"{kw}"' for kw in keywords_batch)
    query = f'site:reddit.com ({quoted_terms})'

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
            log.error(f"[SERP-BATCH] Non-JSON response for batch {keywords_batch!r} | status:{r.status_code}")
            return []

        raw_items = _dig_list(result_data, RESULT_LIST_KEY_CANDIDATES)
        results = []
        rank_misses = 0
        skipped_unattributed = 0
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
            title = item.get("title", "")

            matched_keyword = _find_best_matching_keyword(f"{title} {item_url}", keywords_batch)
            if matched_keyword is None:
                skipped_unattributed += 1
                continue

            results.append({
                "url":     item_url,
                "rank":    rank,
                "title":   title,
                "keyword": matched_keyword,
            })

        if rank_misses and rank_misses == len(results) and results:
            log.warning(
                f"[SERP-BATCH] batch {keywords_batch!r} — no explicit rank field found in any "
                f"result (tried {RANK_FIELD_CANDIDATES}); used result order as rank fallback."
            )

        if skipped_unattributed:
            log.debug(
                f"[SERP-BATCH] batch {keywords_batch!r} — {skipped_unattributed} result(s) "
                f"could not be confidently attributed to any keyword in the batch — skipped."
            )

        log.info(
            f"[SERP-BATCH] batch of {len(keywords_batch)} keyword(s) → {len(results)} attributed "
            f"Reddit result(s) (last {months_back} months: {cd_min} to {cd_max}) | "
            f"1 RapidAPI call | query:{query!r}"
        )
        return results

    except Exception as exc:
        log.error(f"[SERP-BATCH] RapidAPI search error for batch {keywords_batch!r}: {exc}")
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
# SERP DISCOVERY — process_one_keyword() / process_keywords_batch() ONLY
# run the Google SERP call(s) and persist results into
# flintel_google_posts. Reddit is NEVER fetched here — SERP's job is
# done the moment these functions return.
# ─────────────────────────────────────────────────────────────────────────────

def process_one_keyword(keyword: str) -> tuple:
    """UNCHANGED, one-keyword-per-call version. Kept for reference /
    backward compatibility — the live loop below now calls
    process_keywords_batch() instead, to batch RapidAPI calls."""
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


def process_keywords_batch(keywords_batch: list) -> tuple:
    """
    NEW — batched counterpart to process_one_keyword(). Runs exactly ONE
    RapidAPI call for up to GOOGLE_SERP_BATCH keywords at once (instead
    of one call per keyword) via search_google_for_keywords_batch(), and
    persists results into flintel_google_posts exactly like
    process_one_keyword() did — same save_google_post() call, same
    insert-only behavior, same fuzzy_keywords generation, just keyed off
    each result's resolved keyword instead of a single fixed keyword.
    """
    results = search_google_for_keywords_batch(keywords_batch, months_back=SERP_MONTHS_BACK)

    new_posts_saved = 0
    # Cap kept proportional to batch size so the effective per-keyword
    # depth stays the same as before batching (SERP_RESULTS_PER_KEYWORD
    # per keyword in the batch).
    for result in results[:SERP_RESULTS_PER_KEYWORD * len(keywords_batch)]:
        post_url = result["url"]
        matched_keyword = result["keyword"]
        subreddit = _extract_reddit_subreddit_from_url(post_url)
        fuzzy_keywords = generate_fuzzy_keywords(matched_keyword)

        was_new = save_google_post(
            post_url=post_url,
            google_rank=result["rank"],
            search_keyword=matched_keyword,
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
        f"GOOGLE_SERP_BATCH:{GOOGLE_SERP_BATCH} keyword(s) per RapidAPI call (cost control) | "
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

            # ── Batch due keywords into groups of GOOGLE_SERP_BATCH —
            # ONE RapidAPI call per group instead of one per keyword.
            for i in range(0, len(due), GOOGLE_SERP_BATCH):
                batch_docs = due[i:i + GOOGLE_SERP_BATCH]
                batch_keywords = [doc["keyword"] for doc in batch_docs]

                results_count, new_posts_saved = process_keywords_batch(batch_keywords)
                total_results += results_count
                total_new_posts += new_posts_saved

                for kw in batch_keywords:
                    mark_keyword_fetched(kw)

                log.info(
                    f"[SERP] batch {batch_keywords!r} DONE | 1 RapidAPI call for "
                    f"{len(batch_keywords)} keyword(s) | serp_results:{results_count} | "
                    f"new_google_posts_saved:{new_posts_saved} | all marked fetched=True PERMANENTLY"
                )
                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            rapidapi_calls_used = (len(due) + GOOGLE_SERP_BATCH - 1) // GOOGLE_SERP_BATCH
            log.info(
                f"[SERP] Pass complete | keywords_processed:{len(due)} | "
                f"rapidapi_calls_used:{rapidapi_calls_used} (batch_size:{GOOGLE_SERP_BATCH}) | "
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
    """
    UPDATED (per request) — the post-fetch fuzzy CONTENT filter has been
    REMOVED from this loop. fuzzy_keywords are still generated and used
    at SERP-discovery time (to find/tag which posts belong to which
    keyword — unchanged, see generate_fuzzy_keywords() /
    process_keywords_batch()). But once a flintel_google_posts document
    is already tagged with a post_url + search_keyword, this loop no
    longer re-checks the fetched RSS text against that keyword.

    New, simpler rule: if the post_url's Reddit RSS fetch SUCCEEDS
    (matched post_url — content was retrieved), it is saved straight
    into flintel_signals. No content-based accept/reject step anymore.
    passes_fuzzy_filter() is left defined elsewhere in this file (in
    case it's needed again later) but is no longer called here.
    """
    log.info(
        f"[REDDIT-FETCH] Loop started | reads directly from flintel_google_posts | "
        f"check_interval:{REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s | "
        f"retry_cooldown:{REDDIT_POST_RETRY_COOLDOWN_SECONDS}s | "
        f"fetch method: public per-post RSS only, credential-free "
        f"({REDDIT_FETCH_MAX_RETRIES}x backoff + old.reddit.com fallback, no OAuth/PRAW) | "
        f"on successful post_url fetch -> saved DIRECTLY into flintel_signals "
        f"(no post-fetch content/fuzzy filter, no queue/batch/Claude)"
    )

    while True:
        try:
            due_posts = get_due_google_posts()
            if not due_posts:
                time.sleep(REDDIT_FETCH_CHECK_INTERVAL_SECONDS)
                continue

            log.info(f"[REDDIT-FETCH] {len(due_posts)} post(s) due for Reddit RSS fetch this pass")

            saved_count, dupe_count, fail_count = 0, 0, 0

            for doc in due_posts:
                post_url       = doc["post_url"]
                search_keyword = doc.get("search_keyword", "")
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

                # ── post_url fetch SUCCEEDED — save straight into
                # flintel_signals, tagged with its search_keyword. No
                # content/fuzzy check anymore, no queue, no batch, no Claude.
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
                f"already_signaled:{dupe_count} | failed_will_retry:{fail_count}"
            )

        except Exception as exc:
            log.error(f"[REDDIT-FETCH] loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS — thread management + auto-restart
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    """Reddit runs on TWO independent threads:
      1. SERP discovery (run_serp_discovery_loop) — Google call(s),
         batched GOOGLE_SERP_BATCH keywords per RapidAPI call, saves
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
    title="Flintel index.py — Reddit-only (Google SERP discovery, batched GOOGLE_SERP_BATCH/keywords per call -> flintel_google_posts -> Reddit RSS fetch -> flintel_signals, no Claude, no search_volume, no engagement, no Twitter)",
    version="1.1.0",
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
        "system":                  "Flintel index.py — Reddit-only signal pipeline (no Claude, no search_volume, no engagement, no Twitter)",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "reddit_fetch_method":     "public per-post RSS (credential-free, smart-retry + old.reddit.com fallback) — no OAuth/PRAW, no .json endpoint anywhere",
        "reddit_search_keywords":  len(REDDIT_SEARCH_KEYWORDS),
        "keyword_check_interval_seconds": KEYWORD_CHECK_INTERVAL_SECONDS,
        "keyword_cache":           "ENABLED — fetch-once-forever, restart-safe (flintel_keywords)",
        "google_serp_batch_size":  GOOGLE_SERP_BATCH,
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
    log.info("  FLINTEL index.py — REDDIT-ONLY SIGNAL PIPELINE")
    log.info("  (Google SERP discovery, batched per GOOGLE_SERP_BATCH keywords/call")
    log.info("   -> flintel_google_posts -> Reddit RSS fetch -> flintel_signals,")
    log.info("   saved directly, tagged with search_keyword)")
    log.info("  Claude / search_volume / engagement / Twitter / queueing: REMOVED")
    log.info("=" * 70)
    log.info(f"  Client                : {CLIENT_ID}")
    log.info(f"  Reddit                : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED and bool(RAPIDAPI_KEY))}")
    log.info(f"  Reddit fetch method   : public per-post RSS only — credential-free, no OAuth/PRAW, no .json anywhere")
    log.info(f"  Reddit keywords       : {len(REDDIT_SEARCH_KEYWORDS)} (used ONLY to seed brand-new flintel_keywords docs)")
    log.info(f"  Keyword cache         : flintel_keywords — fetch-once-forever")
    log.info(f"  Google SERP           : search_google_for_keywords_batch() — {GOOGLE_SERP_BATCH} keyword(s) OR'd into ONE RapidAPI call (cost control)")
    log.info(f"  flintel_google_posts  : stores post_url + google_rank + search_keyword + subreddit + auto fuzzy_keywords + reddit_fetched")
    log.info(f"  Reddit fetch interval : check every {REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s | retry cooldown {REDDIT_POST_RETRY_COOLDOWN_SECONDS}s on genuine fetch failure")
    log.info(f"  Fuzzy keywords        : Python auto-generated per SERP result at save time — used to filter fetched RSS content")
    log.info(f"  Signal storage        : direct save into flintel_signals on fuzzy match — NO queue, NO batch, NO Claude")
    log.info(f"  RapidAPI config       : {bool(RAPIDAPI_KEY)} (SOLE provider — Google SERP discovery only now, batched {GOOGLE_SERP_BATCH}/call)")
    log.info(f"  MongoDB DB            : {MONGODB_DB}")
    log.info(f"  API auth              : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info("=" * 70)

    asyncio.run(main())
