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

  COST-CONTROL BATCHING (this version only):
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

  POST-FETCH CONTENT FILTER REMOVED (this version only):
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

  DUAL-MONGODB SPLIT (this version only):
    - This build now connects to TWO separate MongoDB targets instead
      of one:
        * MONGODB_URI  (db)  — holds ONLY `flintel_signals`. This is
          the sole collection that is READ from and WRITTEN to on this
          connection (is_post_already_signaled() + save_signal() +
          the /signals endpoint).
        * MONGODB2_URI (db2) — holds `flintel_keywords` and
          `flintel_google_posts`. Every place in this file that used to
          read/write those two collections on `db` now reads/writes
          them on `db2` instead (sync_keywords_to_db, get_due_keywords,
          mark_keyword_fetched, save_google_post, get_due_google_posts,
          mark_google_post_fetched, set_google_post_retry_cooldown, and
          the /keywords + /google-posts endpoints + the counts shown on
          "/").
    - Nothing else about the logic, schema, indexes, retry/cooldown
      behavior, fuzzy matching, SERP batching, or Reddit RSS fetching
      changed — only WHICH MongoDB connection each collection lives on.
    - If MONGODB2_URI is not set, it falls back to MONGODB_URI so the
      service still runs against a single Mongo target (both `db` and
      `db2` simply point at the same cluster/DB in that case).

  ── EMBEDDINGS (ported as-is from flintel.py) ──
    - The moment a signal's raw text is about to be saved into
      `flintel_signals` (i.e. right before the very first insert of that
      document, inside save_signal() — duplicates never re-run this),
      this service generates ONE vector embedding from that document's
      own `text` field and stores it on the SAME document under the
      `embedding` field. Nothing else about the save path changed.
    - One embedding is generated from a document's own `text` field,
      once, at the moment that document is first saved. It is stored on
      that same document under `embedding` (a plain list of floats).
    - Embeddings are NEVER shared between documents — each document's
      embedding comes only from that document's own `text`.
    - Duplicates (a post_url/message_id already saved before) never
      reach the embedding call at all (NOTE: this is now actually TRUE —
      see "MYSQL SINK / BUG FIX" below; before, the embedding was
      generated before the duplicate was detected).
    - If embedding generation fails or is disabled (`EMBEDDING_ENABLED` =
      False, or no API key configured), the document is still saved
      exactly as before — `embedding` is simply set to `None` on that
      document rather than blocking the save.
    - `backfill_missing_embeddings()` is a one-time, on-demand helper
      (run manually via `python index.py --backfill-embeddings`) that
      scans EXISTING documents in `flintel_signals` that already have a
      `text` field but no `embedding` (or `embedding: None`), and
      generates an embedding for each straight from that already-stored
      `text` — it never re-fetches anything from Reddit. This does not
      run automatically on every startup; it only runs when explicitly
      invoked. (Mongo only — there is NO MySQL backfill.)
    - Nothing else — no query embeddings, no vector index creation, no
      vector search, no ranking/retrieval changes.

  ── REDDIT FETCH FIXES (THIS VERSION — every change is marked "# FIX:") ──
    1. URL NORMALIZATION (root cause of "https://old.reddit.com.rss" and
       "RSS feed had no entries"): _normalize_reddit_post_url() accepts
       ONLY real post URLs (…/r/<sub>/comments/<id>[/<slug>]) and returns
       the canonical "https://www.reddit.com/r/<sub>/comments/<id>/<slug>/"
       with no query string / fragment. Used in both SERP search
       functions (non-post URLs are skipped, ?tl=xx variants are deduped),
       in save_google_post(), and in fetch_reddit_post_by_url(), which now
       builds the RSS URL as canonical + ".rss" (".../slug/.rss").
       cleanup_google_posts() runs at startup: deletes unfetched
       non-post documents and rewrites/merges non-canonical URLs.
    2. RATE-LIMIT HANDLING: _reddit_get_with_retry() returns
       (response, reason) with reason in ok / 429 / blocked / not_found /
       empty / network. A global limiter enforces REDDIT_MIN_GAP_SECONDS
       (+ jitter) between ANY two Reddit requests. A circuit breaker
       pauses the fetch loop for REDDIT_CIRCUIT_BREAK_SECONDS after 3
       consecutive 429/403 responses. Per-post cooldown is exponential
       (retry_count stored in flintel_google_posts), "empty" responses
       get a longer floor, and old.reddit.com fallback is skipped after a
       429/403 (same IP, same limit).
    3. OPTIONAL PROXY: REDDIT_PROXY_URL is applied to every Reddit request.
    4. DIAGNOSTICS: GET /reddit-test?url=... (API-key protected).

  ── MYSQL SINK (added in this version — everything else is AS-IS) ──
    - Two live on/off switches (checked via load_dotenv(override=True) +
      _env_bool on every save, no restart needed — same pattern as
      EMBEDDING_ENABLED):
          MONGODB_DATA = true/false  (default TRUE)  -> save signals into
                                                        Mongo `flintel_signals`
          MYSQL_DATA   = true/false  (default FALSE) -> save signals into
                                                        MySQL `flintel_signals`
      With the defaults, behaviour is identical to before (Mongo only).
      If BOTH are false nothing is saved; because posts are marked
      reddit_fetched=True right after a fetch, the Reddit fetch loop
      PAUSES (warning every pass) instead of burning tracked posts.
    - ONLY `flintel_signals` gets the MySQL sink. flintel_keywords /
      flintel_google_posts / jobs stay on Mongo (db2), untouched.
    - MySQL is used purely as a CLIENT (PyMySQL). The server runs
      separately (Render Private Service + Persistent Disk). Nothing is
      written to SQLite/local files.
    - Table `flintel_signals` is created with CREATE TABLE IF NOT EXISTS
      only (never DROP/ALTER) — at startup via init_mysql() and lazily on
      the first save. UNIQUE key is `message_id` only, exactly like Mongo.
    - Embedding is stored as a BLOB: float32, little-endian, flat bytes
      (1536 * 4 = 6144 bytes) + `embedding_dim` + `embedding_model`.
      Never JSON/TEXT. Mongo's embedding (list of floats) is unchanged.
      Helpers: _embedding_to_blob() / _blob_to_embedding().
    - One PyMySQL connection PER THREAD (threading.local), ping(reconnect)
      before every use, one retry on a dropped connection. A MySQL
      failure never blocks the Mongo save and never crashes a thread.

    BUG FIX (duplicate embeddings): save_signal() used to generate the
    OpenAI embedding BEFORE insert_one, so every duplicate still cost one
    embedding call. Now an existence check runs first in every enabled
    sink; the embedding is generated only if at least one enabled sink
    does NOT have the post yet, at most ONCE, shared by both sinks; and if
    Mongo already has the post WITH an embedding, that vector is reused
    for MySQL (zero OpenAI calls). DuplicateKeyError / MySQL 1062 are
    still caught on insert (thread races).
    is_post_already_signaled() now returns True only if the post exists in
    EVERY enabled sink.

Run:
    pip install fastapi uvicorn pymongo python-dotenv httpx requests \
                feedparser openai PyMySQL
    python index.py
    python index.py --backfill-embeddings   # one-time historical backfill
    python index.py --reset-keywords        # FIX: one-time recovery of keywords burned by SERP failures
"""

import array
import asyncio
import json
import logging
import os
import sys
import time
import random
import re
import html
import threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse  # FIX: used by _normalize_reddit_post_url()
from dotenv import load_dotenv

import requests
import feedparser
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN
import uvicorn

try:  # PyMySQL is only needed when MYSQL_DATA is used
    import pymysql
    import pymysql.err
except ImportError:  # pragma: no cover
    pymysql = None

try:  # numpy is optional; array.array fallback is used if missing
    import numpy as _np
except ImportError:  # pragma: no cover
    _np = None

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

# ── SECOND MONGODB CONNECTION — holds flintel_keywords +
# flintel_google_posts. flintel_signals stays on MONGODB_URI/MONGODB_DB
# above. If MONGODB2_URI isn't set, falls back to MONGODB_URI so the
# service still runs fine against a single Mongo target.
MONGODB2_URI = os.getenv("MONGODB2_URI", MONGODB_URI)
MONGODB2_DB  = os.getenv("MONGODB2_DB", MONGODB_DB)

# ── RapidAPI — SOLE provider for Google SERP rank/discovery. UNCHANGED.
RAPIDAPI_KEY          = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_SEARCH_HOST  = "google-search116.p.rapidapi.com"

DATAFORSEO_SERP_TIMEOUT_SECONDS = int(os.getenv("DATAFORSEO_SERP_TIMEOUT_SECONDS", "120"))
REDDIT_JSON_TIMEOUT_SECONDS     = int(os.getenv("REDDIT_JSON_TIMEOUT_SECONDS", "15"))  # used for the RSS fetch

# ── SERP DISCOVERY CONFIG — UNCHANGED. This Python list's ONLY job is to
# seed brand-new keyword documents into flintel_keywords (insert-only).
REDDIT_SEARCH_KEYWORDS = [

"residential graphic designer",
      "residential growth marketer",
      "residential influencer marketing",
      "residential keyword research tool",
      "residential landing page designer",
      "residential lead magnet",
      "residential link building service",
      "residential marketing automation",
      "residential marketing consultant",
      "residential marketing intern",
      "residential media buyer",
      "residential podcast producer",
      "residential social media manager",
      "residential video editor",
      "residential webinar platform",
      "small business ABM platform",
      "small business CRM marketing",
      "small business PPC specialist",
      "small business PR agency",
      "small business SEO audit",
      "small business SEO consultant",
      "small business TikTok marketing",
      "small business ad agency",
      "small business affiliate marketing",
      "small business analytics dashboard",
      "small business brand designer",
      "small business brand strategist",
      "small business content strategist",
      "small business content writer",
      "small business copywriter",
      "small business creative agency",
      "small business email marketing",
      "small business fractional CMO",
      "small business graphic designer",
      "small business growth marketer",
      "small business influencer marketing",
      "small business lead magnet",
      "small business marketing automation",
      "small business marketing consultant",
      "small business marketing intern",
      "small business media buyer",
      "small business podcast producer",
      "small business video editor",
      "small business webinar platform",
      "social media manager",
      "video editor",
      "webinar platform",
      "HVAC technician",
      "affordable HVAC technician",
      "affordable appliance repair",
      "affordable asbestos removal",
      "affordable carpet cleaner",
      "affordable chimney sweep",
      "affordable concrete contractor",
      "affordable deck builder",
      "affordable drywall contractor",
      "affordable electrician",
      "affordable fence installer",
      "affordable flooring contractor",
      "affordable foundation repair",
      "affordable garage door repair",
      "affordable general contractor",
      "affordable generator installer",
      "affordable gutter cleaning",
      "affordable handyman",
      "affordable home inspector",
      "affordable house cleaner",
      "affordable insulation contractor",
      "affordable junk removal",
      "affordable landscaper",
      "affordable locksmith",
      "affordable mold remediation",
      "affordable moving company",
      "affordable painter",
      "affordable pest control",
      "affordable plumber",
      "affordable pool maintenance",
      "affordable pressure washing",
      "affordable roofer",
      "affordable septic tank service",
]

# ── PER-KEYWORD "FETCH ONCE, EVER" CACHE CONFIG — UNCHANGED.
KEYWORD_CHECK_INTERVAL_SECONDS = int(os.getenv("KEYWORD_CHECK_INTERVAL_SECONDS", "60"))
REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS = int(os.getenv("REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS", "20"))

# depth + lookback — read from .env exactly as before. Depth default
# raised to 100 per request.
SERP_RESULTS_PER_KEYWORD = int(os.getenv("SERP_RESULTS_PER_KEYWORD", "100"))
SERP_MONTHS_BACK         = int(os.getenv("SERP_MONTHS_BACK", "6"))
SERP_FETCH_SLEEP_SECONDS = float(os.getenv("SERP_FETCH_SLEEP_SECONDS", "1.5"))

# ── GOOGLE SERP BATCHING — how many keywords get combined into a
# SINGLE RapidAPI call via an OR'd query, instead of one call/keyword.
# This is the ONLY cost-control change in this version. Default 10.
GOOGLE_SERP_BATCH = int(os.getenv("GOOGLE_SERP_BATCH", "10"))

# ── How many results to ask RapidAPI for, PER keyword in a batch, via
# its "limit" query parameter (confirmed supported by this exact
# provider — ScraperLink/google-search116 — "you can specify limit to
# fetch more results per page"). A batch of 3 keywords now requests
# limit = GOOGLE_SERP_BASE_RESULTS_PER_KEYWORD * 3, so per-keyword
# result depth stays consistent regardless of batch size, instead of
# every batch just getting Google's bare default (~10) no matter how
# many keywords were OR'd together.
GOOGLE_SERP_BASE_RESULTS_PER_KEYWORD = int(os.getenv("GOOGLE_SERP_BASE_RESULTS_PER_KEYWORD", "10"))

# ── REDDIT "SMART FETCH" CONFIG — v9.6 retry logic (now only used for
# transient network/5xx errors — see FIX notes below).
REDDIT_FETCH_MAX_RETRIES     = int(os.getenv("REDDIT_FETCH_MAX_RETRIES", "3"))
REDDIT_FETCH_BACKOFF_BASE    = float(os.getenv("REDDIT_FETCH_BACKOFF_BASE", "2.0"))
REDDIT_FETCH_JITTER_MIN      = float(os.getenv("REDDIT_FETCH_JITTER_MIN", "0.4"))
REDDIT_FETCH_JITTER_MAX      = float(os.getenv("REDDIT_FETCH_JITTER_MAX", "1.6"))

# FIX: realistic browser User-Agent by default (a custom "bot-style" UA is
# one of the most common reasons Reddit answers 403/429 to RSS requests).
# Override with REDDIT_USER_AGENT in .env if you prefer something else.
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)

# FIX: global rate limiter — minimum seconds between ANY two Reddit
# requests (shared by the fetch loop and /reddit-test), plus the random
# jitter above (REDDIT_FETCH_JITTER_MIN..MAX) on top of it.
REDDIT_MIN_GAP_SECONDS = float(os.getenv("REDDIT_MIN_GAP_SECONDS", "4"))

# FIX: circuit breaker — after REDDIT_CIRCUIT_BREAK_THRESHOLD consecutive
# 429/403 responses, the whole fetch loop pauses for this many seconds.
REDDIT_CIRCUIT_BREAK_SECONDS   = int(os.getenv("REDDIT_CIRCUIT_BREAK_SECONDS", "300"))
REDDIT_CIRCUIT_BREAK_THRESHOLD = int(os.getenv("REDDIT_CIRCUIT_BREAK_THRESHOLD", "3"))

# FIX: exponential per-post cooldown: next_retry_at = now +
# min(REDDIT_POST_RETRY_COOLDOWN_SECONDS * 2^retry_count, REDDIT_POST_MAX_COOLDOWN_SECONDS).
# After REDDIT_POST_MAX_FAILED_ATTEMPTS failures the post stays
# reddit_fetched=False but is pinned to the max (6h) cooldown.
REDDIT_POST_MAX_COOLDOWN_SECONDS = int(os.getenv("REDDIT_POST_MAX_COOLDOWN_SECONDS", str(6 * 3600)))
REDDIT_POST_MAX_FAILED_ATTEMPTS  = int(os.getenv("REDDIT_POST_MAX_FAILED_ATTEMPTS", "8"))
# FIX: a 200 response with zero feed entries (or a 404) gets AT LEAST this
# long a cooldown — it is not a transient network blip.
REDDIT_EMPTY_COOLDOWN_SECONDS    = int(os.getenv("REDDIT_EMPTY_COOLDOWN_SECONDS", "600"))

# FIX: optional proxy (e.g. http://user:pass@host:port). Applied to every
# Reddit requests.get when set; behaves exactly as before when unset.
REDDIT_PROXY_URL = os.getenv("REDDIT_PROXY_URL", "").strip()

# ── flintel_google_posts CONFIG — UNCHANGED.
REDDIT_FETCH_CHECK_INTERVAL_SECONDS = int(os.getenv("REDDIT_FETCH_CHECK_INTERVAL_SECONDS", "30"))
REDDIT_POST_RETRY_COOLDOWN_SECONDS  = int(os.getenv("REDDIT_POST_RETRY_COOLDOWN_SECONDS", "20"))

REDDIT_ENABLED = os.getenv("REDDIT_ENABLED", "True").strip().lower() in ("1", "true", "yes", "on")

# ── EMBEDDINGS — ported as-is from flintel.py. Everything here is
# additive; none of the settings above were touched. ──
#
# Master ON/OFF switch, same live-checked pattern as other *_ENABLED
# switches in this file. EMBEDDING_ENABLED=True (default) -> every
# newly saved signal gets an embedding generated from its own text.
# EMBEDDING_ENABLED=False -> save_signal() still saves documents exactly
# as before, just with embedding=None — fetching/matching/saving never
# stops or breaks because of this switch.
def _env_bool(name: str, default: bool) -> bool:
    """Parses a True/False on-off switch from an env var. Accepts
    true/false/1/0/yes/no (case-insensitive). Falls back to `default` if
    the var isn't set."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _is_embedding_enabled() -> bool:
    load_dotenv(override=True)
    return _env_bool("EMBEDDING_ENABLED", True) and bool(os.getenv("OPENAI_API_KEY", ""))


# ── SAVE-TARGET SWITCHES for flintel_signals (live-checked, no restart).
# MONGODB_DATA (default True)  -> save into Mongo `flintel_signals`.
# MYSQL_DATA   (default False) -> save into MySQL `flintel_signals`.
def _is_mongodb_data_enabled() -> bool:
    load_dotenv(override=True)
    return _env_bool("MONGODB_DATA", True)


def _is_mysql_data_enabled() -> bool:
    load_dotenv(override=True)
    return _env_bool("MYSQL_DATA", False)


# ── MySQL connection settings (client only). Password is NEVER logged. ──
MYSQL_HOST            = os.getenv("MYSQL_HOST", "")
MYSQL_PORT            = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER            = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD        = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE        = os.getenv("MYSQL_DATABASE", "")
MYSQL_SSL             = _env_bool("MYSQL_SSL", False)
MYSQL_SSL_CA          = os.getenv("MYSQL_SSL_CA", "")  # optional CA file when MYSQL_SSL=true
MYSQL_CONNECT_TIMEOUT = int(os.getenv("MYSQL_CONNECT_TIMEOUT", "10"))
MYSQL_READ_TIMEOUT    = int(os.getenv("MYSQL_READ_TIMEOUT", "30"))
MYSQL_WRITE_TIMEOUT   = int(os.getenv("MYSQL_WRITE_TIMEOUT", "30"))


EMBEDDING_PROVIDER  = os.getenv("EMBEDDING_PROVIDER", "openai")
EMBEDDING_MODEL     = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
OPENAI_API_KEY      = os.getenv("OPENAI_API_KEY", "")
EMBEDDING_TIMEOUT   = int(os.getenv("EMBEDDING_TIMEOUT", "20"))
# Max characters of a document's text sent to the embedding model per call
# (keeps a single unusually long post from blowing past the model's token
# limit). Purely a safety truncation, does not change what gets stored as
# `text` on the document itself.
EMBEDDING_MAX_CHARS = int(os.getenv("EMBEDDING_MAX_CHARS", "8000"))
# How many documents backfill_missing_embeddings() updates per DB batch.
EMBEDDING_BACKFILL_BATCH_SIZE = int(os.getenv("EMBEDDING_BACKFILL_BATCH_SIZE", "100"))
# Politeness delay between individual embedding calls during backfill, so
# a large historical backlog doesn't hammer the embedding API all at once.
EMBEDDING_BACKFILL_GAP_SECONDS = float(os.getenv("EMBEDDING_BACKFILL_GAP_SECONDS", "0.2"))
# Expected vector length for the MySQL BLOB sanity check (1536 floats for
# text-embedding-3-small -> 6144 bytes).
EMBEDDING_EXPECTED_DIM = int(os.getenv("EMBEDDING_EXPECTED_DIM", "1536"))

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
# FIX: REDDIT POST URL NORMALIZATION — the root-cause fix.
# Google SERP returns all sorts of reddit.com URLs (homepage, /search,
# /policies, wiki pages, subreddit roots, business.reddit.com, user
# profiles, and ?tl=fil / ?utm_* variants of the same post). The old code
# accepted anything containing "reddit.com" and then blindly appended
# ".rss", producing garbage such as "https://old.reddit.com.rss" or
# "…/slug/?tl=da.rss". This function accepts ONLY real post URLs and
# returns ONE canonical form per post.
# ─────────────────────────────────────────────────────────────────────────────

# FIX: hosts allowed — reddit.com, www., old., np. only (NOT business.reddit.com,
# NOT m., NOT redd.it, etc.).
_REDDIT_ALLOWED_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "np.reddit.com"}

# FIX: /r/<sub>/comments/<id>[/<slug>][/anything-else-ignored]
_REDDIT_POST_PATH_RE = re.compile(
    r"^/r/([A-Za-z0-9_]+)/comments/([A-Za-z0-9]+)(?:/([^/]+))?(?:/.*)?$"
)

# FIX: loose "is this even a post URL?" regex used ONLY by the startup
# cleanup query (same pattern the request specified).
_REDDIT_POST_LOOSE_RE = re.compile(r"reddit\.com/r/[^/]+/comments/")

# FIX: shape of an already-canonical URL — used by cleanup to find
# documents that still need rewriting.
_REDDIT_CANONICAL_RE = re.compile(
    r"^https://www\.reddit\.com/r/[^/?#]+/comments/[^/?#]+/[^/?#]+/$"
)


def _normalize_reddit_post_url(url: str) -> str | None:
    """FIX: Returns the canonical post URL
        https://www.reddit.com/r/<sub>/comments/<id>/<slug>/
    (no query string, no fragment, always www.reddit.com, always with a
    trailing slash) for any http(s) URL of a Reddit POST on reddit.com /
    www. / old. / np. — or None for everything else (homepage, /search,
    /policies, wiki pages, subreddit roots, business.reddit.com, user
    profiles, non-reddit hosts, garbage).

    If the input has no slug (…/comments/<id>), the canonical form is
    https://www.reddit.com/r/<sub>/comments/<id>/ — Reddit resolves that
    fine, and it is still a stable, dedupable key."""
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url.strip())
    except ValueError:
        return None

    if parsed.scheme not in ("http", "https"):
        return None
    host = (parsed.hostname or "").lower()
    if host not in _REDDIT_ALLOWED_HOSTS:
        return None

    m = _REDDIT_POST_PATH_RE.match(parsed.path or "")
    if not m:
        return None

    sub, post_id, slug = m.groups()
    base = f"https://www.reddit.com/r/{sub}/comments/{post_id}/"
    return f"{base}{slug}/" if slug else base


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
# FUZZY KEYWORD GENERATION + MATCHING — UNCHANGED from v9.12. Fuzzy
# variants are used at SERP-discovery time to resolve which keyword a
# batched SERP result belongs to.
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
# MONGODB — TWO connections:
#   db  (MONGODB_URI / MONGODB_DB)   -> flintel_signals ONLY
#   db2 (MONGODB2_URI / MONGODB2_DB) -> flintel_keywords + flintel_google_posts
# Batch/queue collections REMOVED entirely — there is no batching or
# Claude step left to persist state for.
# (MySQL is NOT handled here — see init_mysql().)
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
    """
    Connects to TWO MongoDB targets instead of one:
      * `db`  (MONGODB_URI/MONGODB_DB)   — holds ONLY flintel_signals.
      * `db2` (MONGODB2_URI/MONGODB2_DB) — holds flintel_keywords and
        flintel_google_posts.
    If MONGODB2_URI was not set, it defaults to MONGODB_URI (see the
    CONFIGURATION section above), so a single-Mongo deployment still
    works exactly as before — `db` and `db2` just point at the same
    cluster/DB in that case.

    Returns (db, db2). Indexes for each collection are created on
    whichever connection now owns that collection. (`db` is connected at
    startup even when MONGODB_DATA=false, so flipping it to true live
    just works.)
    """
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        # flintel_signals — lives on the PRIMARY connection (db). Same
        # names/keys/uniqueness as before — only the *name* passed to
        # create_index() matters here. Any mismatch for a key pattern
        # that already exists under a different name is now handled
        # gracefully by _ensure_index() instead of crashing the
        # process with IndexOptionsConflict (code 85).
        _ensure_index(db.flintel_signals, [("message_id", ASCENDING)], unique=True, name="signals_message_id_unique")
        _ensure_index(db.flintel_signals, [("post_url", ASCENDING)], name="post_url_lookup")
        _ensure_index(db.flintel_signals, [("search_keyword", ASCENDING)], name="signals_search_keyword")
        _ensure_index(db.flintel_signals, [("platform", ASCENDING)], name="signals_platform")
        _ensure_index(db.flintel_signals, [("created_at", ASCENDING)], name="signals_created_at")

        log.info("MongoDB (primary — flintel_signals) connected.")

        # ── SECOND CONNECTION — flintel_keywords + flintel_google_posts.
        client2 = MongoClient(MONGODB2_URI, serverSelectionTimeoutMS=5000)
        client2.server_info()
        db2 = client2[MONGODB2_DB]

        # flintel_keywords — fetch-once-forever cache. UNCHANGED shape,
        # minus the search_volume fields (removed — no longer used).
        _ensure_index(db2.flintel_keywords, [("keyword", ASCENDING)], unique=True, name="keyword_unique")
        _ensure_index(db2.flintel_keywords, [("fetched", ASCENDING)], name="keyword_fetched_idx")
        _ensure_index(db2.flintel_keywords, [("next_retry_at", ASCENDING)], name="keyword_retry_cooldown_idx")

        # flintel_google_posts — UNCHANGED schema/indexes from v9.12.
        _ensure_index(
            db2.flintel_google_posts, [("post_url", ASCENDING)], unique=True, name="google_post_url_unique"
        )
        _ensure_index(
            db2.flintel_google_posts, [("reddit_fetched", ASCENDING)], name="google_post_fetched_idx"
        )
        _ensure_index(
            db2.flintel_google_posts, [("next_retry_at", ASCENDING)], name="google_post_retry_cooldown_idx"
        )
        _ensure_index(
            db2.flintel_google_posts, [("subreddit", ASCENDING)], name="google_post_subreddit_idx"
        )
        _ensure_index(
            db2.flintel_google_posts, [("search_keyword", ASCENDING)], name="google_post_search_keyword_idx"
        )

        log.info("MongoDB2 (secondary — flintel_keywords + flintel_google_posts) connected.")

        return db, db2
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db, db2 = get_database()


def log_operator_alert(title: str, detail: str, level: str = "ERROR"):
    log.log(
        logging.CRITICAL if level == "CRITICAL" else logging.ERROR,
        f"[OPERATOR ALERT] {title} — {detail}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD CACHE — flintel_keywords (now on db2). Same fetch-once-forever
# behavior as v9.12, minus every search_volume-related field/function
# (removed).
# ─────────────────────────────────────────────────────────────────────────────

def sync_keywords_to_db(keywords: list):
    now = datetime.now(timezone.utc)
    for kw in keywords:
        try:
            db2.flintel_keywords.update_one(
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
        cursor = db2.flintel_keywords.find({
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
        db2.flintel_keywords.update_one(
            {"keyword": keyword},
            {"$set": {"fetched": True, "last_fetched_at": now}},
        )
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] mark_keyword_fetched error for {keyword!r}: {exc}")


def reset_burned_keywords() -> int:
    """FIX: ONE-TIME / ON-DEMAND recovery helper (run via
    `python index.py --reset-keywords`). Finds keywords in
    db2.flintel_keywords with fetched == True that have ZERO documents in
    db2.flintel_google_posts with that search_keyword — i.e. keywords that
    were marked fetched but produced nothing (typically burned by an earlier
    SERP failure) — and sets fetched=False, next_retry_at=None on them so the
    discovery loop searches them again. Returns how many were reset.

    Note: a keyword that genuinely had zero Reddit results also matches this
    rule; it will simply be searched once more and re-marked fetched."""
    try:
        fetched_docs = list(db2.flintel_keywords.find({"fetched": True}, {"keyword": 1}))
        burned = []
        for d in fetched_docs:
            kw = d.get("keyword")
            if not kw:
                continue
            if db2.flintel_google_posts.count_documents({"search_keyword": kw}, limit=1) == 0:
                burned.append(kw)

        reset_count = 0
        for i in range(0, len(burned), 500):
            chunk = burned[i:i + 500]
            res = db2.flintel_keywords.update_many(
                {"keyword": {"$in": chunk}, "fetched": True},
                {"$set": {"fetched": False, "next_retry_at": None}},
            )
            reset_count += res.modified_count

        log.info(
            f"[KEYWORD-RESET] scanned {len(fetched_docs)} fetched keyword(s) | "
            f"burned (0 google_posts) found:{len(burned)} | reset to fetched=False:{reset_count}"
        )
        return reset_count
    except Exception as exc:
        log.error(f"[KEYWORD-RESET] reset_burned_keywords error: {exc}")
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — SOLE discovery mechanism: RapidAPI SERP search
# (site:reddit.com). Same single RapidAPI call, same independent host,
# same try/except. FIX: result URLs are now validated + canonicalized +
# deduped via _normalize_reddit_post_url() instead of a bare
# `"reddit.com" in url` check.
# ─────────────────────────────────────────────────────────────────────────────

def search_google_for_keyword(keyword: str, months_back: int = SERP_MONTHS_BACK) -> list:
    """One-keyword-per-call version. Kept in place for reference /
    backward compatibility — the live discovery loop below now calls the
    batched version (search_google_for_keywords_batch) instead.
    FIX: now also uses _normalize_reddit_post_url() + canonical dedupe."""
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
        non_post_count = 0   # FIX: non-post reddit / non-reddit URLs skipped
        dupe_count = 0       # FIX: same post under a different ?tl=/utm variant
        seen_canonical = set()  # FIX: dedupe by canonical URL
        for pos, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                continue
            item_url = item.get("url", "") or item.get("link", "")

            # FIX: was `if "reddit.com" not in item_url: continue`
            canonical_url = _normalize_reddit_post_url(item_url)
            if canonical_url is None:
                non_post_count += 1
                continue
            if canonical_url in seen_canonical:
                dupe_count += 1
                continue
            seen_canonical.add(canonical_url)

            rank = _dig_value(item, RANK_FIELD_CANDIDATES)
            if rank is None:
                rank = pos
                rank_misses += 1
            results.append({
                "url":   canonical_url,  # FIX: canonical, never the raw SERP URL
                "rank":  rank,
                "title": item.get("title", ""),
            })

        if rank_misses and rank_misses == len(results) and results:
            log.warning(
                f"[SERP] '{keyword}' — no explicit rank field found in any result "
                f"(tried {RANK_FIELD_CANDIDATES}); used result order as rank fallback."
            )

        if non_post_count or dupe_count:
            log.info(
                f"[SERP] '{keyword}' — skipped {non_post_count} non-post URL(s), "
                f"deduped {dupe_count} variant URL(s) of the same post."
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
    Resolves a single SERP result (coming back from a combined /
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
    # FIX: fuzzy fallback now only accepts variants containing a space (2+ words).
    # Single-word variants like "business", "affordable", "video" are far too
    # generic and were tagging results to the wrong keyword in a batch.
    # (generate_fuzzy_keywords() itself is unchanged — stored fuzzy_keywords stay the same.)
    for kw in keywords_batch:
        for fkw in generate_fuzzy_keywords(kw):
            if fkw and " " in fkw and fkw in t:
                return kw
    return None


# FIX: return contract — returns None on a REAL failure (missing key, non-200,
# non-JSON, API error payload, exception) and [] ONLY when the call succeeded
# (HTTP 200 + valid JSON) with genuinely zero results (or an empty batch).
def search_google_for_keywords_batch(keywords_batch: list, months_back: int = SERP_MONTHS_BACK) -> list | None:
    """
    Batches up to GOOGLE_SERP_BATCH keywords into a SINGLE
    RapidAPI SERP call using one OR'd query
    (site:reddit.com ("kw1" OR "kw2" OR ...)), instead of firing one
    RapidAPI call per keyword. This is the sole cost-control change in
    this version — cuts RapidAPI usage roughly GOOGLE_SERP_BATCH-fold.

    Everything downstream is unaffected: each returned result is still
    resolved back to exactly one search_keyword (via
    _find_best_matching_keyword) before flintel_google_posts ever sees
    it, so save_google_post(), fuzzy_keywords generation, the Reddit
    fetch loop, and signal storage all keep working exactly as before.

    FIX: result URLs are validated/canonicalized with
    _normalize_reddit_post_url() (non-post URLs are skipped and counted
    as non-post) and deduped by canonical URL, so ?tl=fil / ?tl=da
    variants of the same post become ONE result.
    """
    if not RAPIDAPI_KEY:
        log.warning("[SERP-BATCH] RapidAPI key not set — skipping SERP search.")
        return None  # FIX: real failure -> None (was []), so keywords are NOT marked fetched
    if not keywords_batch:
        return []

    today = datetime.now(timezone.utc)
    date_from = today - timedelta(days=months_back * 30)
    cd_min = date_from.strftime("%m/%d/%Y")
    cd_max = today.strftime("%m/%d/%Y")

    quoted_terms = " OR ".join(f'"{kw}"' for kw in keywords_batch)
    query = f'site:reddit.com ({quoted_terms})'

    # Scale the requested result count with batch size, so a 3-keyword
    # batch asks for ~3x the results a single keyword would, instead of
    # every batch just getting capped at the provider's bare default
    # (~10) regardless of how many keywords were combined into the
    # query.
    requested_limit = GOOGLE_SERP_BASE_RESULTS_PER_KEYWORD * len(keywords_batch)

    try:
        url = "https://google-search116.p.rapidapi.com/"

        querystring = {"query": query, "limit": str(requested_limit)}

        headers = {
            "x-rapidapi-key": RAPIDAPI_KEY,  # .env
            "x-rapidapi-host": RAPIDAPI_SEARCH_HOST,
            "Content-Type": "application/json",
        }

        r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_SERP_TIMEOUT_SECONDS)

        # FIX: HTTP status check right after the request. Anything but 200
        # (quota exceeded, bad subscription, provider outage, ...) is a
        # real failure -> None, so the caller does NOT burn the keywords.
        if r.status_code != 200:
            log.error(
                f"[SERP-BATCH] HTTP {r.status_code} for batch {keywords_batch!r} — treating as "
                f"FAILURE (batch will be retried) | first 200 chars of body: {r.text[:200]!r}"
            )
            return None

        try:
            result_data = r.json()
        except ValueError:
            log.error(f"[SERP-BATCH] Non-JSON response for batch {keywords_batch!r} | status:{r.status_code}")
            return None  # FIX: real failure -> None (was [])

        raw_items = _dig_list(result_data, RESULT_LIST_KEY_CANDIDATES)

        # FIX: RapidAPI quota / subscription errors often come back as HTTP 200-ish JSON
        # like {"message": "..."} or {"error": "..."} with no result list at all.
        # That is a failure, NOT a genuine zero-result search. (A payload that
        # DOES contain a result list — even an empty one — is a genuine success.)
        if (
            isinstance(result_data, dict)
            and ("message" in result_data or "error" in result_data)
            and not raw_items
            and not any(isinstance(result_data.get(k), list) for k in RESULT_LIST_KEY_CANDIDATES)
        ):
            log.error(
                f"[SERP-BATCH] API error payload (no result list) for batch {keywords_batch!r} — "
                f"treating as FAILURE (batch will be retried) | "
                f"message:{str(result_data.get('message'))[:200]!r} error:{str(result_data.get('error'))[:200]!r}"
            )
            return None

        # ── DIAGNOSTIC (always at INFO) — shows the raw count RapidAPI
        # actually returned for this combined/OR'd query, BEFORE any
        # local filtering. Without this, "serp_results:0" in the pass
        # log is ambiguous: it could mean RapidAPI itself returned 0
        # hits for the batched query, OR it could mean RapidAPI returned
        # results but every single one got filtered out locally (either
        # not a reddit post URL, or couldn't be attributed back to one
        # of the batch's keywords). This line tells you which.
        log.info(
            f"[SERP-BATCH] RAW response for batch {keywords_batch!r} → "
            f"{len(raw_items)} raw item(s) from RapidAPI (requested limit:{requested_limit}, "
            f"before reddit-post-URL / keyword-attribution filtering) | query:{query!r}"
        )
        if len(raw_items) == 0:
            log.warning(
                f"[SERP-BATCH] RapidAPI returned ZERO raw items for this batch's combined "
                f"query — this is NOT a local filtering issue, the SERP call itself found "
                f"nothing. Possible causes: the OR'd query "
                f"(site:reddit.com (\"kw1\" OR \"kw2\" OR ...)) may be too long / not "
                f"supported the same way as a single-keyword query by this RapidAPI host, "
                f"or the response shape changed (result_data keys tried: "
                f"{RESULT_LIST_KEY_CANDIDATES}) — raw response top-level keys: "
                f"{list(result_data.keys()) if isinstance(result_data, dict) else type(result_data).__name__}"
            )

        results = []
        rank_misses = 0
        non_reddit_count = 0   # FIX: now means "not a real reddit POST url" (was "not reddit.com")
        dupe_count = 0         # FIX: canonical-URL duplicates (?tl=fil / ?tl=da / utm variants)
        skipped_unattributed = 0
        unattributed_samples = []
        seen_canonical = set()  # FIX: dedupe by canonical URL
        for pos, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                continue
            item_url = item.get("url", "") or item.get("link", "")

            # FIX: was `if "reddit.com" not in item_url: ...continue`
            # Now: only real post URLs survive, in canonical form.
            canonical_url = _normalize_reddit_post_url(item_url)
            if canonical_url is None:
                non_reddit_count += 1
                continue
            if canonical_url in seen_canonical:
                dupe_count += 1
                continue

            rank = _dig_value(item, RANK_FIELD_CANDIDATES)
            if rank is None:
                rank = pos
                rank_misses += 1
            title = item.get("title", "")

            matched_keyword = _find_best_matching_keyword(f"{title} {canonical_url}", keywords_batch)
            if matched_keyword is None:
                skipped_unattributed += 1
                if len(unattributed_samples) < 5:
                    unattributed_samples.append({"title": title, "url": canonical_url})
                continue

            seen_canonical.add(canonical_url)
            results.append({
                "url":     canonical_url,  # FIX: canonical, never the raw SERP URL
                "rank":    rank,
                "title":   title,
                "keyword": matched_keyword,
            })

        if rank_misses and rank_misses == len(results) and results:
            log.warning(
                f"[SERP-BATCH] batch {keywords_batch!r} — no explicit rank field found in any "
                f"result (tried {RANK_FIELD_CANDIDATES}); used result order as rank fallback."
            )

        if non_reddit_count:
            log.info(
                f"[SERP-BATCH] batch {keywords_batch!r} — {non_reddit_count} raw item(s) were "
                f"not real reddit POST URLs (homepage/search/wiki/profile/subreddit root/other host) "
                f"— filtered out."
            )

        if dupe_count:
            log.info(
                f"[SERP-BATCH] batch {keywords_batch!r} — {dupe_count} raw item(s) were duplicate "
                f"variants (?tl=/utm/old./np.) of a post already in this batch — deduped."
            )

        if skipped_unattributed:
            log.warning(
                f"[SERP-BATCH] batch {keywords_batch!r} — {skipped_unattributed} reddit.com "
                f"result(s) COULD NOT be attributed to any keyword in this batch (title/url "
                f"didn't contain the keyword or any fuzzy variant) — skipped, not saved. "
                f"Sample: {unattributed_samples}"
            )

        log.info(
            f"[SERP-BATCH] batch of {len(keywords_batch)} keyword(s) → {len(results)} attributed "
            f"Reddit result(s) out of {len(raw_items)} raw item(s) "
            f"(last {months_back} months: {cd_min} to {cd_max}) | 1 RapidAPI call"
        )
        return results

    except Exception as exc:
        log.error(f"[SERP-BATCH] RapidAPI search error for batch {keywords_batch!r}: {exc}")
        return None  # FIX: real failure -> None (was []), so keywords are NOT marked fetched


def is_post_already_signaled(post_url: str) -> bool:
    """Checks the signal sink(s) by post_url before any Reddit fetch
    happens. Original behaviour: look in Mongo `flintel_signals` (db).
    Now: True ONLY if the post already exists in EVERY enabled sink
    (MONGODB_DATA -> Mongo, MYSQL_DATA -> MySQL), so a post that is in
    Mongo but missing from a freshly enabled MySQL still gets fetched and
    saved into MySQL. Any lookup error counts as "not signaled" (same as
    before). Both sinks off -> False (the fetch loop pauses in that case)."""
    if not post_url:
        return False

    mongo_on = _is_mongodb_data_enabled()
    mysql_on = _is_mysql_data_enabled()
    if not mongo_on and not mysql_on:
        return False

    if mongo_on:
        try:
            existing = db.flintel_signals.find_one({"post_url": post_url}, {"_id": 1})
            if existing is None:
                return False
        except Exception as exc:
            log.error(f"[DEDUP] is_post_already_signaled error for {post_url}: {exc}")
            return False

    if mysql_on:
        if not _mysql_post_url_exists(post_url):
            return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# flintel_google_posts HELPERS — same schema/collection/indexes as v9.12
# (on db2). FIX: save_google_post() canonicalizes, retry cooldown is now
# exponential and stores retry_count, startup cleanup added.
# ─────────────────────────────────────────────────────────────────────────────

def save_google_post(post_url: str, google_rank, search_keyword: str, subreddit: str, fuzzy_keywords: list) -> bool:
    """Insert-only upsert — a post_url already tracked here is NEVER
    overwritten. Returns True only when this call genuinely inserted a
    brand-new document.

    FIX: post_url is canonicalized first (belt-and-braces — the SERP
    functions already do this), and anything that isn't a real Reddit
    post URL is refused instead of being stored."""
    canonical_url = _normalize_reddit_post_url(post_url)  # FIX
    if canonical_url is None:                             # FIX
        log.debug(f"[GOOGLE-POSTS] refused non-post URL: {post_url!r}")
        return False
    post_url = canonical_url                              # FIX

    now = datetime.now(timezone.utc)
    try:
        result = db2.flintel_google_posts.update_one(
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
                "retry_count":     0,  # FIX: exponential-backoff counter
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
        cursor = db2.flintel_google_posts.find({
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
        db2.flintel_google_posts.update_one(
            {"post_url": post_url},
            {"$set": {
                "reddit_fetched": True,
                "fetched_at":     now,
                "fuzzy_matched":  fuzzy_matched,
            }},
        )
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] mark_google_post_fetched error for {post_url}: {exc}")


def _compute_post_cooldown_seconds(reason: str, retry_count: int) -> int:
    """FIX: exponential per-post cooldown.

        cooldown = min(REDDIT_POST_RETRY_COOLDOWN_SECONDS * 2^retry_count,
                       REDDIT_POST_MAX_COOLDOWN_SECONDS)          # 20s * 2^n, cap 6h

    - `retry_count` is the number of failed attempts BEFORE this one.
    - "empty" (200 with zero entries) and "not_found" get a longer floor
      (REDDIT_EMPTY_COOLDOWN_SECONDS, default 10 min) because retrying
      them in 20s is pointless — they are not transient network blips.
    - Once this failure makes it REDDIT_POST_MAX_FAILED_ATTEMPTS (8)
      failures, the post is pinned to the max (6h) cooldown. It stays
      reddit_fetched=False (never permanently abandoned)."""
    if retry_count + 1 >= REDDIT_POST_MAX_FAILED_ATTEMPTS:
        return REDDIT_POST_MAX_COOLDOWN_SECONDS

    cooldown = REDDIT_POST_RETRY_COOLDOWN_SECONDS * (2 ** min(retry_count, 20))
    if reason in ("empty", "not_found"):
        cooldown = max(cooldown, REDDIT_EMPTY_COOLDOWN_SECONDS)
    return int(min(cooldown, REDDIT_POST_MAX_COOLDOWN_SECONDS))


def set_google_post_retry_cooldown(post_url: str, reason: str = "network", retry_count: int = 0):
    """Called when a specific post_url's Reddit RSS fetch genuinely
    failed. Keeps reddit_fetched=False but stamps next_retry_at.

    FIX: was a flat 20s. Now exponential (see _compute_post_cooldown_seconds)
    and the new attempt count is persisted in `retry_count` on the same
    flintel_google_posts document."""
    now = datetime.now(timezone.utc)
    cooldown_seconds = _compute_post_cooldown_seconds(reason, retry_count)
    new_retry_count = retry_count + 1
    next_retry = now + timedelta(seconds=cooldown_seconds)
    try:
        db2.flintel_google_posts.update_one(
            {"post_url": post_url},
            {"$set": {"next_retry_at": next_retry, "retry_count": new_retry_count}},
        )
        log.info(
            f"[GOOGLE-POSTS] '{post_url}' cooldown set | reason:{reason} | "
            f"retry_count:{new_retry_count} | next_retry_at:{next_retry.isoformat()} "
            f"({cooldown_seconds}s from now) — will not be re-attempted before then"
        )
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] set_google_post_retry_cooldown error for {post_url}: {exc}")


def cleanup_google_posts():
    """FIX: startup cleanup for flintel_google_posts. Runs once, before the
    worker threads start. No schema/index changes — only deletes junk and
    rewrites post_url values to their canonical form.

      1. DELETE documents with reddit_fetched == False whose post_url is
         not a Reddit post (doesn't match reddit\\.com/r/[^/]+/comments/):
         homepage, /search, /policies, wiki pages, subreddit roots, etc.
      2. REWRITE / MERGE documents whose post_url is a real post but not
         in canonical form (?tl=fil, ?utm_*, old./np./bare host, http://,
         no trailing slash …):
           - if no document with the canonical URL exists yet -> the
             URL is rewritten in place;
           - if one already exists -> the duplicate is deleted and one
             is kept (preferring an already-fetched document, so a
             finished post never gets re-fetched)."""
    try:
        # ── 1. delete unfetched non-post junk ────────────────────────────
        del_result = db2.flintel_google_posts.delete_many({
            "reddit_fetched": False,
            "post_url": {"$not": _REDDIT_POST_LOOSE_RE},
        })
        junk_deleted = del_result.deleted_count

        # ── 2. rewrite / merge non-canonical URLs ─────────────────────────
        candidates = list(db2.flintel_google_posts.find(
            {"post_url": {"$not": _REDDIT_CANONICAL_RE}},
            {"_id": 1, "post_url": 1, "reddit_fetched": 1},
        ))

        rewritten, merged_dupes, skipped = 0, 0, 0
        for doc in candidates:
            old_url = doc.get("post_url", "")
            canonical = _normalize_reddit_post_url(old_url)
            if canonical is None:
                skipped += 1   # not a post URL but already fetched — leave it alone
                continue
            if canonical == old_url:
                continue

            existing = db2.flintel_google_posts.find_one(
                {"post_url": canonical}, {"_id": 1, "reddit_fetched": 1}
            )
            if existing is None:
                db2.flintel_google_posts.update_one(
                    {"_id": doc["_id"]}, {"$set": {"post_url": canonical}}
                )
                rewritten += 1
            elif doc.get("reddit_fetched") and not existing.get("reddit_fetched"):
                # the variant is already done, the canonical one isn't -> keep the finished one
                db2.flintel_google_posts.delete_one({"_id": existing["_id"]})
                db2.flintel_google_posts.update_one(
                    {"_id": doc["_id"]}, {"$set": {"post_url": canonical}}
                )
                merged_dupes += 1
            else:
                db2.flintel_google_posts.delete_one({"_id": doc["_id"]})
                merged_dupes += 1

        log.info(
            f"[CLEANUP] flintel_google_posts | deleted_unfetched_non_post:{junk_deleted} | "
            f"urls_rewritten_to_canonical:{rewritten} | duplicate_variants_merged:{merged_dupes} | "
            f"non_post_but_already_fetched_left_alone:{skipped}"
        )
    except Exception as exc:
        log.error(f"[CLEANUP] cleanup_google_posts error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT POST FETCH — public, credential-free per-post RSS feed ONLY.
# No more random-fallback upvotes/comments — those fields are simply not
# generated or stored anymore.
#
# FIX: rewritten request layer —
#   * every request goes through ONE global rate limiter
#   * optional proxy (REDDIT_PROXY_URL)
#   * realistic browser headers
#   * _reddit_get_with_retry() returns (response, reason) instead of just
#     None, reason in: "ok" | "429" | "blocked" | "not_found" | "empty" | "network"
#   * 429/403 are NOT retried in-place (retrying the same IP immediately
#     only makes it worse) — the caller decides (cooldown + circuit breaker)
#   * only transient "network" failures (timeouts, 5xx) are retried here
# ─────────────────────────────────────────────────────────────────────────────

# FIX: global rate limiter state (shared by the fetch loop AND /reddit-test)
_reddit_rate_lock = threading.Lock()
_reddit_last_request_at = float("-inf")

# FIX: log the first bytes of an "empty" response body ONCE per process so
# you can tell whether it's a block page or a genuinely empty feed.
_reddit_empty_body_logged = False


def _reddit_rate_limit_wait():
    """FIX: blocks until at least REDDIT_MIN_GAP_SECONDS (+ random jitter of
    REDDIT_FETCH_JITTER_MIN..MAX) has elapsed since the previous Reddit
    request from ANY thread. The sleep happens while holding the lock, so
    concurrent callers queue up and are spaced out correctly."""
    global _reddit_last_request_at
    with _reddit_rate_lock:
        gap = REDDIT_MIN_GAP_SECONDS + random.uniform(REDDIT_FETCH_JITTER_MIN, REDDIT_FETCH_JITTER_MAX)
        wait = gap - (time.monotonic() - _reddit_last_request_at)
        if wait > 0:
            time.sleep(wait)
        _reddit_last_request_at = time.monotonic()


def _reddit_proxies() -> dict | None:
    """FIX: proxies dict for requests.get when REDDIT_PROXY_URL is set,
    otherwise None (requests behaves exactly as before)."""
    if REDDIT_PROXY_URL:
        return {"http": REDDIT_PROXY_URL, "https": REDDIT_PROXY_URL}
    return None


def _reddit_headers() -> dict:
    """FIX: browser-like headers with an RSS/Atom-friendly Accept."""
    return {
        "User-Agent": REDDIT_USER_AGENT,
        "Accept": "application/atom+xml, application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
        "Accept-Language": "en-US,en;q=0.9",
    }


def _reddit_request_once(url: str) -> tuple:
    """FIX: ONE rate-limited (optionally proxied) GET. Never retries.
    Returns (response_or_None, reason, status_code_or_None, error_or_None).

    reason:
      "ok"        HTTP 200 with a non-empty body
      "empty"     HTTP 200 with a completely empty body
      "not_found" HTTP 404 / 410
      "429"       HTTP 429 (rate limited)
      "blocked"   HTTP 401 / 403 / 451 (IP or UA blocked)
      "network"   timeout / connection error / 5xx / any unexpected status
    """
    _reddit_rate_limit_wait()
    try:
        r = requests.get(
            url,
            headers=_reddit_headers(),
            timeout=REDDIT_JSON_TIMEOUT_SECONDS,
            proxies=_reddit_proxies(),
        )
    except requests.RequestException as exc:
        return None, "network", None, str(exc)

    status = r.status_code
    if status == 200:
        return r, ("ok" if r.content and r.content.strip() else "empty"), status, None
    if status in (404, 410):
        return r, "not_found", status, None
    if status == 429:
        return r, "429", status, None
    if status in (401, 403, 451):
        return r, "blocked", status, None
    # 5xx and anything unexpected -> treat as transient
    return r, "network", status, None


def _reddit_get_with_retry(url: str) -> tuple:
    """FIX: returns (response_or_None, reason) — see _reddit_request_once()
    for the reason values. Retries ONLY transient "network" failures (up to
    REDDIT_FETCH_MAX_RETRIES, exponential backoff). 429 / blocked /
    not_found / empty are returned immediately: retrying them in place just
    burns requests against a limit that won't lift for minutes."""
    last_reason = "network"
    last_resp = None
    for attempt in range(1, REDDIT_FETCH_MAX_RETRIES + 1):
        r, reason, status, err = _reddit_request_once(url)
        last_reason, last_resp = reason, r

        if reason != "network":
            if reason in ("429", "blocked"):
                retry_after = r.headers.get("Retry-After") if r is not None else None
                log.warning(
                    f"[REDDIT-FETCH] {reason.upper()} (HTTP {status}) for {url}"
                    f"{' | Retry-After:' + retry_after if retry_after else ''} — not retrying in place."
                )
            elif reason == "not_found":
                log.debug(f"[REDDIT-FETCH] 404 (gone) for {url} — not retrying.")
            return r, reason

        # transient: network error / 5xx
        if attempt < REDDIT_FETCH_MAX_RETRIES:
            wait = (REDDIT_FETCH_BACKOFF_BASE ** attempt) + random.uniform(0, 1.0)
            log.warning(
                f"[REDDIT-FETCH] attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} transient failure "
                f"(status:{status} err:{err}) for {url} — backing off {wait:.1f}s..."
            )
            time.sleep(wait)

    log.error(f"[REDDIT-FETCH] exhausted {REDDIT_FETCH_MAX_RETRIES} attempts for {url} (last_reason:{last_reason})")
    return last_resp, last_reason


def _extract_reddit_submission_id(post_url: str) -> str | None:
    match = re.search(r"/comments/([a-zA-Z0-9]+)", post_url)
    return match.group(1) if match else None


def _extract_reddit_subreddit_from_url(post_url: str) -> str:
    match = re.search(r"reddit\.com/r/([^/]+)/", post_url)
    return match.group(1) if match else ""


def _fetch_and_parse_rss(rss_url: str) -> tuple:
    """FIX: one _reddit_get_with_retry() + feedparser parse.
    Returns (feed_or_None, reason). A 200 that parses to ZERO entries is
    reported as "empty" (and the first 200 chars of the body are logged
    once per process so you can see if it's a block/consent page)."""
    global _reddit_empty_body_logged

    r, reason = _reddit_get_with_retry(rss_url)
    if reason not in ("ok", "empty") or r is None:
        return None, reason

    feed = feedparser.parse(r.content)
    if reason == "empty" or not feed.entries:
        if not _reddit_empty_body_logged:
            _reddit_empty_body_logged = True
            preview = r.content[:200].decode("utf-8", errors="replace")
            log.warning(
                f"[REDDIT-FETCH] EMPTY feed (HTTP {r.status_code}, 0 entries) for {rss_url} | "
                f"content-type:{r.headers.get('Content-Type')!r} | first 200 chars of body "
                f"(logged once per process): {preview!r}"
            )
        return None, "empty"

    return feed, "ok"


def fetch_reddit_post_by_url(post_url: str, keyword: str, rank: int) -> tuple:
    """Fetches one post's public RSS feed.

    FIX: now returns (item_or_None, reason) instead of just item_or_None,
    so the fetch loop can react differently to 429 / blocked / empty /
    not_found / network. reason is "ok" on success, or one of
    "429" | "blocked" | "not_found" | "empty" | "network" | "invalid_url" | "parse_error".

    FIX: the RSS URL is built from the CANONICAL post URL as
    canonical + ".rss" (i.e. ".../slug/.rss") — never appended after a
    query string. The old.reddit.com fallback is built the same way, and
    is SKIPPED when the primary failed with 429/blocked (same IP, same
    limit) — it is only tried on empty / not_found / network."""
    canonical_url = _normalize_reddit_post_url(post_url)
    if canonical_url is None:
        log.error(f"[REDDIT-FETCH] not a Reddit post URL, refusing to fetch: {post_url!r}")
        return None, "invalid_url"

    primary_url = canonical_url + ".rss"
    feed, reason = _fetch_and_parse_rss(primary_url)

    if feed is None and reason in ("empty", "not_found", "network"):
        fallback_url = canonical_url.replace("https://www.reddit.com", "https://old.reddit.com") + ".rss"
        if fallback_url != primary_url:
            log.info(f"[REDDIT-FETCH] primary failed ({reason}) — retrying via old.reddit.com fallback: {fallback_url}")
            feed, reason = _fetch_and_parse_rss(fallback_url)
    elif feed is None:
        log.info(f"[REDDIT-FETCH] primary failed with {reason} — skipping old.reddit.com fallback (same IP, same limit)")

    if feed is None:
        log.error(f"[REDDIT-FETCH] fetch_reddit_post_by_url gave up for {post_url} (reason:{reason})")
        return None, reason

    try:
        entry = feed.entries[0]

        title = (entry.get("title", "") or "").strip()
        raw_summary = entry.get("summary", "") or ""
        if not raw_summary and entry.get("content"):
            raw_summary = entry["content"][0].get("value", "") or ""
        summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(raw_summary)).strip()

        text = title
        if summary_plain and summary_plain.lower() != title.lower():
            text = f"{title}\n\n{summary_plain}"

        # FIX: .lstrip("u/") strips every leading 'u' or '/' CHARACTER ("/u/username" -> "sername").
        # Remove only the literal "/u/" or "u/" prefix instead.
        author = (entry.get("author", "") or "unknown").strip()
        author = re.sub(r"^/?u/", "", author).strip() or "unknown"
        subreddit = _extract_reddit_subreddit_from_url(canonical_url)

        posted_at = None
        published = entry.get("published") or entry.get("updated")
        if published:
            try:
                posted_at = datetime(*entry.get("published_parsed", entry.get("updated_parsed"))[:6],
                                      tzinfo=timezone.utc).isoformat()
            except (TypeError, ValueError):
                posted_at = published

        submission_id = _extract_reddit_submission_id(canonical_url)
        message_id = f"reddit_serp_{submission_id}" if submission_id else (
            f"reddit_serp_{re.sub(r'[^a-zA-Z0-9]', '_', canonical_url)[-40:]}"
        )

        return {
            "message_id":           message_id,
            "platform":             "reddit",
            "text":                 text,
            "username":             author,
            "subreddit_or_channel": subreddit,
            "post_url":             post_url,   # keep the URL exactly as stored in flintel_google_posts
            "posted_at":            posted_at,
            "search_keyword":       keyword,
            "google_rank":          rank,
        }, "ok"
    except Exception as exc:
        log.error(f"[REDDIT-FETCH] fetch_reddit_post_by_url parse error for {post_url}: {exc}")
        return None, "parse_error"


# ─────────────────────────────────────────────────────────────────────────────
# EMBEDDINGS — ported as-is from flintel.py. One function that turns a
# document's own text into one vector, called from exactly one place
# (save_signal, right before insert), plus one manually-triggered
# backfill helper for historical documents. Nothing else in the file
# calls these, and these never touch fetching/matching/SERP/keyword logic.
# ─────────────────────────────────────────────────────────────────────────────

_openai_client = None


def _get_openai_client():
    """Lazily creates (once) and reuses a single OpenAI client for the
    lifetime of the process. Returns None (never raises) if the `openai`
    package isn't installed or OPENAI_API_KEY isn't set — callers treat
    that as "embeddings unavailable right now" and just store
    embedding=None rather than failing the save."""
    global _openai_client
    if _openai_client is not None:
        return _openai_client

    if not OPENAI_API_KEY:
        return None

    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY, timeout=EMBEDDING_TIMEOUT)
        return _openai_client
    except Exception as exc:
        log.warning(f"[EMBEDDING] could not initialise OpenAI client: {exc}")
        return None


def generate_embedding(text: str):
    """Generates ONE embedding vector from ONE piece of text, using the
    configured embedding model (EMBEDDING_MODEL, default
    "text-embedding-3-small"). This is the ONLY function in the whole
    service that talks to the embedding API.

    - One call in, one embedding out — never given more than one
      document's text at a time, and never mixes text from more than one
      document into a single embedding call, so embeddings are never
      shared across posts.
    - Returns a plain list[float] on success, or None on any failure
      (missing/invalid key, network error, empty text, provider outage,
      etc.) — it NEVER raises, so a failed embedding call can never break
      or block the fetch/match/save pipeline that calls it.
    """
    if not text or not text.strip():
        return None

    client = _get_openai_client()
    if client is None:
        return None

    # Simple safety truncation — keeps one unusually long document from
    # exceeding the embedding model's input limit. Does not affect what
    # is stored as the document's own `text` field, only what is sent to
    # the embedding call.
    payload_text = text.strip()[:EMBEDDING_MAX_CHARS]

    try:
        response = client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=payload_text,
        )
        return response.data[0].embedding
    except Exception as exc:
        log.warning(f"[EMBEDDING] generation failed | model={EMBEDDING_MODEL} | {exc}")
        return None


def backfill_missing_embeddings():
    """ONE-TIME / ON-DEMAND helper — NOT called automatically anywhere in
    the normal startup path. Run it manually when you want to generate
    embeddings for documents that were saved to flintel_signals BEFORE
    this embedding layer existed (or that were saved while
    EMBEDDING_ENABLED was False):

        python index.py --backfill-embeddings

    What it does, and nothing more:
      1. Finds documents in flintel_signals that already have a `text`
         field but no usable `embedding` (missing OR None OR empty list).
      2. For each one, generates an embedding from that document's own
         ALREADY-STORED `text` — it never re-fetches anything from
         Reddit, and never touches any other field on the document.
      3. Writes the embedding onto that same document.

    Documents that already have a real embedding are left completely
    untouched (never regenerated). Processes in batches
    (EMBEDDING_BACKFILL_BATCH_SIZE at a time) with a small politeness
    delay between embedding calls (EMBEDDING_BACKFILL_GAP_SECONDS).
    (Mongo only — there is no MySQL backfill.)"""
    if not _is_embedding_enabled():
        log.warning(
            "[EMBEDDING-BACKFILL] EMBEDDING_ENABLED is False or OPENAI_API_KEY is not "
            "set — nothing to do. Set both and re-run."
        )
        return

    query = {
        "text": {"$exists": True, "$ne": ""},
        "$or": [
            {"embedding": {"$exists": False}},
            {"embedding": None},
            {"embedding": []},
        ],
    }

    total_scanned = 0
    total_updated = 0
    total_failed = 0

    log.info("[EMBEDDING-BACKFILL] starting one-time backfill of missing embeddings...")

    while True:
        batch = list(
            db.flintel_signals.find(query, {"_id": 1, "text": 1}).limit(EMBEDDING_BACKFILL_BATCH_SIZE)
        )
        if not batch:
            break

        for doc in batch:
            total_scanned += 1
            embedding = generate_embedding(doc.get("text", ""))

            if embedding is not None:
                try:
                    db.flintel_signals.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"embedding": embedding}},
                    )
                    total_updated += 1
                except Exception as exc:
                    total_failed += 1
                    log.error(f"[EMBEDDING-BACKFILL] update failed | _id={doc['_id']} | {exc}")
            else:
                total_failed += 1
                log.warning(f"[EMBEDDING-BACKFILL] embedding generation failed | _id={doc['_id']}")

            time.sleep(EMBEDDING_BACKFILL_GAP_SECONDS)

        log.info(
            f"[EMBEDDING-BACKFILL] progress | scanned={total_scanned} | "
            f"updated={total_updated} | failed={total_failed}"
        )

    log.info(
        f"[EMBEDDING-BACKFILL] done | scanned={total_scanned} | "
        f"updated={total_updated} | failed={total_failed}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# MYSQL SINK — client only (PyMySQL). The MySQL server runs elsewhere.
# Only `flintel_signals` is mirrored into MySQL; everything else stays on
# Mongo.
# ─────────────────────────────────────────────────────────────────────────────

_MYSQL_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS flintel_signals (
    id                    BIGINT        NOT NULL AUTO_INCREMENT,
    message_id            VARCHAR(191)  NOT NULL,
    platform              VARCHAR(32)   NOT NULL,
    post_url              VARCHAR(1024) NULL,
    text                  MEDIUMTEXT    NULL,
    username              VARCHAR(255)  NULL,
    subreddit_or_channel  VARCHAR(128)  NULL,
    posted_at             DATETIME(3)   NULL,
    fetched_at            DATETIME(3)   NOT NULL,
    google_rank           INT           NULL,
    search_keyword        VARCHAR(255)  NULL,
    client_id             VARCHAR(128)  NULL,
    created_at            DATETIME(3)   NOT NULL,
    embedding             BLOB          NULL,
    embedding_dim         SMALLINT      NULL,
    embedding_model       VARCHAR(64)   NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_signals_message_id (message_id),
    KEY idx_signals_post_url (post_url(191)),
    KEY idx_signals_search_keyword (search_keyword),
    KEY idx_signals_platform (platform),
    KEY idx_signals_client_id (client_id),
    KEY idx_signals_created_at (created_at),
    KEY idx_signals_fetched_at (fetched_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# Column order used for INSERT (params must follow this order).
_MYSQL_COLUMNS = [
    "message_id", "platform", "post_url", "text", "username",
    "subreddit_or_channel", "posted_at", "fetched_at", "google_rank",
    "search_keyword", "client_id", "created_at", "embedding",
    "embedding_dim", "embedding_model",
]
_MYSQL_INSERT_SQL = (
    "INSERT INTO flintel_signals ("
    + ", ".join(f"`{c}`" for c in _MYSQL_COLUMNS)
    + ") VALUES (" + ", ".join(["%s"] * len(_MYSQL_COLUMNS)) + ")"
)

_mysql_local = threading.local()          # one connection PER THREAD
_mysql_table_ready = False
_mysql_table_lock = threading.Lock()


# ── Embedding <-> BLOB (float32, little-endian, flat bytes) ──────────────────

def _embedding_to_blob(vec):
    """list[float] -> float32 little-endian bytes. Returns None for a
    None/empty vector. Raises ValueError if the byte length isn't
    EMBEDDING_EXPECTED_DIM * 4 (1536 * 4 = 6144 by default)."""
    if vec is None or len(vec) == 0:
        return None
    if _np is not None:
        raw = _np.asarray(vec, dtype="<f4").tobytes()
    else:
        arr = array.array("f", vec)
        if sys.byteorder == "big":
            arr.byteswap()
        raw = arr.tobytes()
    expected = EMBEDDING_EXPECTED_DIM * 4
    if len(raw) != expected:
        raise ValueError(
            f"embedding blob length {len(raw)} != expected {expected} "
            f"(dim={len(vec)}, expected_dim={EMBEDDING_EXPECTED_DIM})"
        )
    return raw


def _blob_to_embedding(blob):
    """float32 little-endian bytes -> list[float] (float32 precision).
    Returns None for None/empty. Raises ValueError on a wrong length."""
    if blob is None or len(blob) == 0:
        return None
    expected = EMBEDDING_EXPECTED_DIM * 4
    if len(blob) != expected:
        raise ValueError(f"embedding blob length {len(blob)} != expected {expected}")
    if _np is not None:
        return _np.frombuffer(bytes(blob), dtype="<f4").astype(float).tolist()
    arr = array.array("f")
    arr.frombytes(bytes(blob))
    if sys.byteorder == "big":
        arr.byteswap()
    return list(arr)


# ── connection handling ──────────────────────────────────────────────────────

def _mysql_connect():
    """Opens ONE new PyMySQL connection (utf8mb4, autocommit). Password is
    never logged."""
    if pymysql is None:
        raise RuntimeError("PyMySQL is not installed (pip install PyMySQL)")
    kwargs = dict(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=MYSQL_CONNECT_TIMEOUT,
        read_timeout=MYSQL_READ_TIMEOUT,
        write_timeout=MYSQL_WRITE_TIMEOUT,
    )
    if MYSQL_SSL:
        kwargs["ssl"] = {"ca": MYSQL_SSL_CA} if MYSQL_SSL_CA else {"check_hostname": False}
    return pymysql.connect(**kwargs)


def _reset_mysql_conn():
    conn = getattr(_mysql_local, "conn", None)
    _mysql_local.conn = None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def _get_mysql_conn():
    """This thread's own connection (never shared across threads),
    ping(reconnect=True)-ed before every use."""
    conn = getattr(_mysql_local, "conn", None)
    if conn is not None:
        try:
            conn.ping(reconnect=True)
            return conn
        except Exception:
            _reset_mysql_conn()
    conn = _mysql_connect()
    _mysql_local.conn = conn
    return conn


def _mysql_retry_errors() -> tuple:
    if pymysql is None:
        return (OSError,)
    return (pymysql.err.OperationalError, pymysql.err.InterfaceError, OSError)


def _with_mysql(fn):
    """Runs fn(conn) on this thread's connection. If the connection is
    dropped, reconnects and retries ONCE; if it still fails, raises (the
    callers catch it, log, and move on)."""
    last = None
    for _ in range(2):
        try:
            return fn(_get_mysql_conn())
        except _mysql_retry_errors() as exc:
            last = exc
            _reset_mysql_conn()
    raise last


def _ensure_mysql_table(conn):
    """CREATE TABLE IF NOT EXISTS (never DROP/ALTER). Runs once per
    process, lazily on first use if startup init didn't manage it."""
    global _mysql_table_ready
    if _mysql_table_ready:
        return
    with _mysql_table_lock:
        if _mysql_table_ready:
            return
        with conn.cursor() as cur:
            cur.execute(_MYSQL_CREATE_TABLE_SQL)
        _mysql_table_ready = True
        log.info("[MYSQL] table flintel_signals ready")


def init_mysql() -> bool:
    """Startup helper: if MYSQL_DATA is true, connect and make sure the
    table exists. Never raises — if MySQL is down the service still
    starts (warning) and the next save reconnects by itself."""
    if not _is_mysql_data_enabled():
        return False
    try:
        _with_mysql(_ensure_mysql_table)
        log.info(f"[MYSQL] connected | host={MYSQL_HOST} db={MYSQL_DATABASE}")
        return True
    except Exception as exc:
        log.warning(
            f"[MYSQL] startup connect/table init failed (service continues, will retry on next save) | {exc}"
        )
        return False


# ── helpers for rows ─────────────────────────────────────────────────────────

def _to_utc_naive(dt: datetime) -> datetime:
    """tz-aware -> UTC naive (what MySQL DATETIME(3) wants)."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _parse_posted_at(value):
    """Mongo keeps `posted_at` as an ISO-8601 STRING (or None, or — in a
    rare parse-failure case — Reddit's raw date string). MySQL gets a UTC
    naive DATETIME(3); anything that can't be parsed becomes NULL."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return _to_utc_naive(value)
    if isinstance(value, str):
        try:
            return _to_utc_naive(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _clip(value, n: int):
    """Truncate to a VARCHAR width so strict-mode MySQL never rejects a row
    over 'Data too long'."""
    if value is None:
        return None
    return str(value)[:n]


def _to_int_or_none(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _build_mysql_row(doc: dict) -> tuple:
    emb = doc.get("embedding")
    blob = dim = model = None
    if emb is not None and len(emb) > 0:
        try:
            blob = _embedding_to_blob(emb)
            dim = len(emb)
            model = _clip(EMBEDDING_MODEL, 64)
        except ValueError as exc:
            log.warning(f"[MYSQL] embedding rejected, saving row with NULL embedding | {exc}")
            blob = dim = model = None

    return (
        _clip(doc["message_id"], 191),
        _clip(doc.get("platform", "reddit"), 32),
        _clip(doc.get("post_url", ""), 1024),
        doc.get("text"),
        _clip(doc.get("username"), 255),
        _clip(doc.get("subreddit_or_channel", ""), 128),
        _parse_posted_at(doc.get("posted_at")),
        _to_utc_naive(doc["fetched_at"]),
        _to_int_or_none(doc.get("google_rank")),
        _clip(doc.get("search_keyword"), 255),
        _clip(doc.get("client_id"), 128),
        _to_utc_naive(doc["created_at"]),
        blob,
        dim,
        model,
    )


def _mysql_exists_query(sql: str, value: str, what: str) -> bool:
    """Cheap existence check. On any MySQL failure returns False (the
    insert attempt will then log the real error)."""
    def op(conn):
        _ensure_mysql_table(conn)
        with conn.cursor() as cur:
            cur.execute(sql, (value,))
            return cur.fetchone() is not None
    try:
        return bool(_with_mysql(op))
    except Exception as exc:
        log.warning(f"[MYSQL] existence check failed | {what}={value} | {exc}")
        return False


def _mysql_post_exists(message_id: str) -> bool:
    return _mysql_exists_query(
        "SELECT 1 FROM flintel_signals WHERE message_id=%s LIMIT 1", message_id, "message_id"
    )


def _mysql_post_url_exists(post_url: str) -> bool:
    return _mysql_exists_query(
        "SELECT 1 FROM flintel_signals WHERE post_url=%s LIMIT 1", post_url, "post_url"
    )


def _save_to_mongo(doc: dict) -> bool:
    """True only if a NEW Mongo document was inserted."""
    try:
        db.flintel_signals.insert_one(doc)
        return True
    except DuplicateKeyError:
        # Already saved this post before (same message_id) — not an error.
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        log_operator_alert("MongoDB Write Failed", str(exc), level="CRITICAL")
        return False


def _save_to_mysql(doc: dict) -> bool:
    """True only if a NEW MySQL row was inserted. Never raises."""
    try:
        row = _build_mysql_row(doc)

        def op(conn):
            _ensure_mysql_table(conn)
            with conn.cursor() as cur:
                cur.execute(_MYSQL_INSERT_SQL, row)
            return True

        return bool(_with_mysql(op))
    except Exception as exc:
        if pymysql is not None and isinstance(exc, pymysql.err.IntegrityError) \
                and exc.args and exc.args[0] == 1062:
            return False  # duplicate message_id (race) — not an error
        log.error(f"[MYSQL] save_signal error | message_id={doc.get('message_id')} | {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL STORAGE — direct save into flintel_signals (Mongo on db and/or
# MySQL, per MONGODB_DATA / MYSQL_DATA), no batching, no scoring. This is
# what replaces the old queue -> Claude -> save flow. Also generates one
# embedding from this document's own `text` — but ONLY when at least one
# enabled sink does not have the post yet (see BUG FIX in module
# docstring).
# ─────────────────────────────────────────────────────────────────────────────

_NO_SINK_MSG = "MONGODB_DATA aur MYSQL_DATA dono false: kuch save nahi ho raha"
_last_no_sink_warn = 0.0


def _warn_no_sinks(force: bool = False):
    """Logs the 'both sinks off' warning. The fetch loop forces it once per
    pass; save_signal() calls it throttled (max once/60s)."""
    global _last_no_sink_warn
    now = time.time()
    if force or now - _last_no_sink_warn >= 60:
        log.warning(_NO_SINK_MSG)
        _last_no_sink_warn = now


def save_signal(item: dict) -> bool:
    """Returns True if a NEW row was inserted in at least one enabled sink,
    else False.

    Order:
      1. cheap existence check (by message_id) in each enabled sink;
      2. every enabled sink already has it -> return False, NO embedding;
      3. otherwise ONE embedding (or Mongo's existing one, reused) shared
         by every sink that still needs the row;
      4. insert per sink, each with its own try/except; DuplicateKeyError /
         MySQL 1062 still caught for thread races."""
    mongo_on = _is_mongodb_data_enabled()
    mysql_on = _is_mysql_data_enabled()

    if not mongo_on and not mysql_on:
        _warn_no_sinks()
        return False

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
    mid = doc["message_id"]

    # ── 1. existence checks (cheap, no embedding yet) ──
    mongo_exists = False
    mongo_embedding = None
    if mongo_on:
        try:
            found = db.flintel_signals.find_one({"message_id": mid}, {"_id": 1, "embedding": 1})
            if found is not None:
                mongo_exists = True
                emb = found.get("embedding")
                if isinstance(emb, list) and emb:
                    mongo_embedding = emb
        except Exception as exc:
            log.error(f"[MONGO] existence check failed | message_id={mid} | {exc}")

    mysql_exists = _mysql_post_exists(mid) if mysql_on else False

    need_mongo = mongo_on and not mongo_exists
    need_mysql = mysql_on and not mysql_exists

    # ── 2. already everywhere -> NO embedding call ──
    if not need_mongo and not need_mysql:
        return False

    # ── 3. one embedding, shared. Reuse Mongo's if it already has one. ──
    if mongo_embedding is not None:
        embedding = mongo_embedding
    elif _is_embedding_enabled():
        embedding = generate_embedding(doc["text"])
    else:
        embedding = None

    # ── 4. per-sink inserts, each isolated ──
    saved_to = []

    if need_mongo:
        mongo_doc = dict(doc)
        mongo_doc["embedding"] = embedding
        if _save_to_mongo(mongo_doc):
            saved_to.append("mongo")

    if need_mysql:
        mysql_doc = dict(doc)
        mysql_doc["embedding"] = embedding
        if _save_to_mysql(mysql_doc):
            saved_to.append("mysql")

    if saved_to:
        log.info(
            f"SAVED [{doc['platform'].upper()}] search_keyword={doc['search_keyword']!r} | "
            f"subreddit:{doc['subreddit_or_channel']!r} | google_rank:{doc['google_rank']} | "
            f"embedding:{'yes' if embedding is not None else 'none'} | "
            f"sinks:{'+'.join(saved_to)} | "
            f"post_url:{doc['post_url']}"
        )
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# SERP DISCOVERY — process_one_keyword() / process_keywords_batch() ONLY
# run the Google SERP call(s) and persist results into
# flintel_google_posts. Reddit is NEVER fetched here — SERP's job is
# done the moment these functions return. UNCHANGED (URLs coming out of
# the search functions are already canonical — see FIX notes above).
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
    Batched counterpart to process_one_keyword(). Runs exactly ONE
    RapidAPI call for up to GOOGLE_SERP_BATCH keywords at once (instead
    of one call per keyword) via search_google_for_keywords_batch(), and
    persists results into flintel_google_posts exactly like
    process_one_keyword() did — same save_google_post() call, same
    insert-only behavior, same fuzzy_keywords generation, just keyed off
    each result's resolved keyword instead of a single fixed keyword.
    """
    results = search_google_for_keywords_batch(keywords_batch, months_back=SERP_MONTHS_BACK)

    # FIX: None means the SERP call FAILED (not "zero results") — signal that to the
    # caller with (None, 0) so it does not mark these keywords as fetched.
    if results is None:
        return None, 0

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
            failed_batches = 0  # FIX: batches whose SERP call failed this pass (keywords left un-fetched)

            # ── Batch due keywords into groups of GOOGLE_SERP_BATCH —
            # ONE RapidAPI call per group instead of one per keyword.
            for i in range(0, len(due), GOOGLE_SERP_BATCH):
                batch_docs = due[i:i + GOOGLE_SERP_BATCH]
                batch_keywords = [doc["keyword"] for doc in batch_docs]

                results_count, new_posts_saved = process_keywords_batch(batch_keywords)

                # FIX: results_count is None => the SERP call FAILED (RapidAPI down / quota /
                # non-JSON / missing key / exception). Do NOT mark these keywords fetched —
                # they stay fetched=False and are retried on the next pass. The 30s sleep
                # also prevents a tight retry loop while the provider is down.
                if results_count is None:
                    failed_batches += 1
                    log.error(
                        f"[SERP] batch {batch_keywords!r} FAILED — SERP call did not succeed. "
                        f"Keywords NOT marked fetched; will be retried next pass. "
                        f"Sleeping 30s before continuing."
                    )
                    time.sleep(30)
                    continue

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
                f"total_serp_results:{total_results} | new_google_posts_saved:{total_new_posts} | "
                f"failed_batches_will_retry:{failed_batches}"  # FIX
            )

        except Exception as exc:
            log.error(f"[SERP] discovery loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT FETCH LOOP — reads flintel_google_posts directly, fetches RSS,
# and on a successful fetch SAVES DIRECTLY into flintel_signals. No
# queue, no batching, no Claude call anywhere in this loop.
#
# FIX: reacts to the fetch reason —
#   * circuit breaker: REDDIT_CIRCUIT_BREAK_THRESHOLD (3) consecutive
#     429/403 responses -> the WHOLE loop pauses for
#     REDDIT_CIRCUIT_BREAK_SECONDS (300s) instead of hammering Reddit
#     through every remaining post
#   * exponential per-post cooldown with retry_count
#   * "empty"/"not_found" get a longer cooldown than network errors
#   * the old extra SERP_FETCH_SLEEP_SECONDS sleep is gone — the global
#     rate limiter in _reddit_rate_limit_wait() now spaces requests
#
# MYSQL SINK: the ONLY addition here is a guard at the top of each pass —
# if MONGODB_DATA and MYSQL_DATA are BOTH false the pass is skipped (with
# a warning). Otherwise posts would be fetched, marked reddit_fetched=True
# permanently, and never saved anywhere.
# ─────────────────────────────────────────────────────────────────────────────

def run_reddit_fetch_loop():
    """
    The post-fetch fuzzy CONTENT filter has been REMOVED from this loop.
    fuzzy_keywords are still generated and used at SERP-discovery time
    (to find/tag which posts belong to which keyword — unchanged, see
    generate_fuzzy_keywords() / process_keywords_batch()). But once a
    flintel_google_posts document is already tagged with a post_url +
    search_keyword, this loop no longer re-checks the fetched RSS text
    against that keyword.

    Rule: if the post_url's Reddit RSS fetch SUCCEEDS (matched post_url —
    content was retrieved), it is saved straight into flintel_signals
    (which now also generates an embedding from that document's own text
    at save time — see save_signal()). No content-based accept/reject
    step anymore. passes_fuzzy_filter() is left defined elsewhere in this
    file (in case it's needed again later) but is no longer called here.
    """
    log.info(
        f"[REDDIT-FETCH] Loop started | reads directly from flintel_google_posts | "
        f"check_interval:{REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s | "
        f"per-post cooldown: exponential {REDDIT_POST_RETRY_COOLDOWN_SECONDS}s*2^n "
        f"(cap {REDDIT_POST_MAX_COOLDOWN_SECONDS}s, pinned after {REDDIT_POST_MAX_FAILED_ATTEMPTS} failures, "
        f"empty/404 floor {REDDIT_EMPTY_COOLDOWN_SECONDS}s) | "
        f"rate limit: min gap {REDDIT_MIN_GAP_SECONDS}s + jitter | "
        f"circuit breaker: {REDDIT_CIRCUIT_BREAK_THRESHOLD} consecutive 429/403 -> pause {REDDIT_CIRCUIT_BREAK_SECONDS}s | "
        f"proxy: {'ON' if REDDIT_PROXY_URL else 'off'} | "
        f"fetch method: public per-post RSS only, credential-free "
        f"(canonical URL + '.rss', old.reddit.com fallback only on empty/404/network, no OAuth/PRAW) | "
        f"on successful post_url fetch -> saved DIRECTLY into flintel_signals "
        f"(no post-fetch content/fuzzy filter, no queue/batch/Claude) | "
        f"embeddings: {'ENABLED — model=' + EMBEDDING_MODEL if _is_embedding_enabled() else 'DISABLED'} "
        f"(one per newly saved signal, generated inside save_signal())"
    )

    # FIX: circuit-breaker state. Lives outside the while-loop so a run of
    # 429s that straddles two passes still counts as "consecutive".
    consecutive_blocks = 0

    while True:
        try:
            # MYSQL SINK guard: both save targets off -> don't burn tracked posts.
            if not _is_mongodb_data_enabled() and not _is_mysql_data_enabled():
                _warn_no_sinks(force=True)
                time.sleep(REDDIT_FETCH_CHECK_INTERVAL_SECONDS)
                continue

            due_posts = get_due_google_posts()
            if not due_posts:
                time.sleep(REDDIT_FETCH_CHECK_INTERVAL_SECONDS)
                continue

            log.info(f"[REDDIT-FETCH] {len(due_posts)} post(s) due for Reddit RSS fetch this pass")

            saved_count, dupe_count, fail_count, invalid_count = 0, 0, 0, 0
            breaker_tripped = False

            for doc in due_posts:
                post_url       = doc["post_url"]
                search_keyword = doc.get("search_keyword", "")
                subreddit      = doc.get("subreddit", "")
                google_rank    = doc.get("google_rank")
                retry_count    = int(doc.get("retry_count") or 0)  # FIX

                if is_post_already_signaled(post_url):
                    mark_google_post_fetched(post_url, fuzzy_matched=None)
                    dupe_count += 1
                    log.info(f"[REDDIT-FETCH] SKIP (already in flintel_signals) | {post_url}")
                    continue

                item, reason = fetch_reddit_post_by_url(post_url, search_keyword, google_rank)  # FIX: (item, reason)

                if not item:
                    # FIX: a URL that isn't a Reddit post can never succeed — don't retry it forever.
                    if reason == "invalid_url":
                        mark_google_post_fetched(post_url, fuzzy_matched=None)
                        invalid_count += 1
                        continue

                    # FIX: exponential cooldown + retry_count, reason-aware
                    set_google_post_retry_cooldown(post_url, reason=reason, retry_count=retry_count)
                    fail_count += 1
                    log.warning(
                        f"[REDDIT-FETCH] fetch FAILED | reason:{reason} | {post_url} | "
                        f"left reddit_fetched=False — will retry after cooldown"
                    )

                    # FIX: circuit breaker bookkeeping
                    if reason in ("429", "blocked"):
                        consecutive_blocks += 1
                    elif reason in ("empty", "not_found"):
                        consecutive_blocks = 0   # Reddit answered normally — we're not being throttled
                    # "network"/"parse_error": leave the counter untouched

                    if consecutive_blocks >= REDDIT_CIRCUIT_BREAK_THRESHOLD:
                        breaker_tripped = True
                        break
                    continue  # FIX: no extra sleep — the global rate limiter spaces requests

                # ── post_url fetch SUCCEEDED — save straight into
                # flintel_signals, tagged with its search_keyword. No
                # content/fuzzy check anymore, no queue, no batch, no
                # Claude. save_signal() itself now also generates an
                # embedding from this document's own text.
                consecutive_blocks = 0  # FIX: a good response resets the breaker
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

            log.info(
                f"[REDDIT-FETCH] Pass {'ABORTED (circuit breaker)' if breaker_tripped else 'complete'} | "
                f"due:{len(due_posts)} | saved:{saved_count} | already_signaled:{dupe_count} | "
                f"invalid_url_skipped:{invalid_count} | failed_will_retry:{fail_count}"
            )

            # FIX: circuit breaker — pause the whole loop instead of hammering Reddit
            if breaker_tripped:
                log.error(
                    f"[REDDIT-FETCH] ⛔ CIRCUIT BREAKER TRIPPED — {consecutive_blocks} consecutive "
                    f"429/403 responses. Reddit is rate-limiting/blocking this IP"
                    f"{' (proxy in use: ' + 'yes)' if REDDIT_PROXY_URL else ' (no proxy configured — consider REDDIT_PROXY_URL)'}. "
                    f"Pausing the ENTIRE Reddit fetch loop for {REDDIT_CIRCUIT_BREAK_SECONDS}s "
                    f"instead of continuing through the remaining posts."
                )
                consecutive_blocks = 0
                time.sleep(REDDIT_CIRCUIT_BREAK_SECONDS)
                log.info("[REDDIT-FETCH] circuit breaker pause over — resuming.")

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
         flintel_google_posts directly, fetches RSS, saves successful
         fetches straight into flintel_signals (embedding generated
         inside save_signal()).
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
# FASTAPI — read-only endpoints (+ FIX: /reddit-test diagnostic)
# (/signals and the counts on "/" still read the Mongo copy of
# flintel_signals — they are unchanged.)
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Flintel index.py — Reddit-only (Google SERP discovery, batched GOOGLE_SERP_BATCH/keywords per call -> flintel_google_posts -> Reddit RSS fetch -> flintel_signals + embeddings, no Claude, no search_volume, no engagement, no Twitter)",
    version="1.3.0",
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
    total_keywords_tracked = db2.flintel_keywords.count_documents({})
    due_now_count = db2.flintel_keywords.count_documents({"fetched": False})

    total_google_posts = db2.flintel_google_posts.count_documents({})
    pending_reddit_fetch = db2.flintel_google_posts.count_documents({"reddit_fetched": False})
    fetched_reddit_posts = db2.flintel_google_posts.count_documents({"reddit_fetched": True})
    fuzzy_matched_posts  = db2.flintel_google_posts.count_documents({"fuzzy_matched": True})
    fuzzy_no_match_posts = db2.flintel_google_posts.count_documents({"fuzzy_matched": False})

    signals_with_embedding = db.flintel_signals.count_documents({"embedding": {"$ne": None}})
    signals_without_embedding = db.flintel_signals.count_documents(
        {"$or": [{"embedding": None}, {"embedding": {"$exists": False}}]}
    )

    return {
        "status":                  "running",
        "system":                  "Flintel index.py — Reddit-only signal pipeline (no Claude, no search_volume, no engagement, no Twitter) + per-signal embeddings",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "reddit_fetch_method":     "public per-post RSS (credential-free, canonical URL + '.rss', rate-limited, circuit-breaker, old.reddit.com fallback only on empty/404/network) — no OAuth/PRAW, no .json endpoint anywhere",
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
        "reddit_min_gap_seconds":              REDDIT_MIN_GAP_SECONDS,        # FIX
        "reddit_circuit_break_seconds":        REDDIT_CIRCUIT_BREAK_SECONDS,  # FIX
        "reddit_proxy_configured":             bool(REDDIT_PROXY_URL),        # FIX
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
        "dual_mongodb":            True,
        "mongodb2_configured":     bool(MONGODB2_URI),
        "mongodb_data_enabled":    _is_mongodb_data_enabled(),  # MYSQL SINK
        "mysql_data_enabled":      _is_mysql_data_enabled(),    # MYSQL SINK
        "embedding_enabled":       _is_embedding_enabled(),
        "embedding_model":         EMBEDDING_MODEL,
        "signals_with_embedding":     signals_with_embedding,
        "signals_without_embedding": signals_without_embedding,
    }


@app.get("/health")
def health():
    try:
        db.command("ping")
        mongo = "connected"
    except Exception:
        mongo = "disconnected"

    try:
        db2.command("ping")
        mongo2 = "connected"
    except Exception:
        mongo2 = "disconnected"

    return {
        "status":                  "ok",
        "mongodb":                 mongo,
        "mongodb2":                mongo2,
        "reddit_working":          REDDIT_ENABLED and bool(RAPIDAPI_KEY),
        "reddit_indicator":        _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "google_posts_pending_reddit_fetch": db2.flintel_google_posts.count_documents({"reddit_fetched": False}),
        "embedding_working":       _is_embedding_enabled(),
        "embedding_indicator":     _working(_is_embedding_enabled()),
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


@app.get("/keywords", dependencies=[Depends(verify_api_key)])
def get_keywords_status():
    raw_docs = list(db2.flintel_keywords.find({}, {"_id": 0}).sort("keyword", 1))
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

    docs = list(db2.flintel_google_posts.find(q, {"_id": 0}).sort("discovered_at", -1).limit(limit))
    for d in docs:
        for f in ["discovered_at", "fetched_at", "next_retry_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()

    total = db2.flintel_google_posts.count_documents({})
    pending = db2.flintel_google_posts.count_documents({"reddit_fetched": False})
    fetched = db2.flintel_google_posts.count_documents({"reddit_fetched": True})
    matched = db2.flintel_google_posts.count_documents({"fuzzy_matched": True})
    no_match = db2.flintel_google_posts.count_documents({"fuzzy_matched": False})

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
def get_signals(limit: int = 50, search_keyword: str = None, platform: str = None, has_embedding: bool = None):
    q: dict = {"client_id": CLIENT_ID}
    if search_keyword:
        q["search_keyword"] = search_keyword
    if platform:
        q["platform"] = platform
    if has_embedding is True:
        q["embedding"] = {"$ne": None}
    elif has_embedding is False:
        q["$or"] = [{"embedding": None}, {"embedding": {"$exists": False}}]
    signals = list(db.flintel_signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    return {"count": len(signals), "signals": _serialise(signals)}


@app.get("/reddit-test", dependencies=[Depends(verify_api_key)])
def reddit_test(url: str, old: bool = False):
    """FIX: diagnostic endpoint. Fetches ONE Reddit post's RSS feed using
    the SAME request layer as the fetch loop (canonical URL + '.rss',
    global rate limiter, browser headers, optional proxy) — but a single
    attempt with no retries, and without touching the circuit breaker or
    any collection — so you can tell from the deployed server whether
    Reddit is blocking your IP.

        GET /reddit-test?url=https://www.reddit.com/r/<sub>/comments/<id>/<slug>/
        GET /reddit-test?url=...&old=true      # test the old.reddit.com host instead

    Only Reddit POST URLs are accepted (anything else -> 400)."""
    canonical = _normalize_reddit_post_url(url)
    if canonical is None:
        raise HTTPException(
            status_code=400,
            detail="url must be a Reddit post URL like https://www.reddit.com/r/<sub>/comments/<id>/<slug>/",
        )

    target = canonical
    if old:
        target = canonical.replace("https://www.reddit.com", "https://old.reddit.com")
    rss_url = target + ".rss"

    started = time.monotonic()
    r, reason, status, err = _reddit_request_once(rss_url)
    elapsed_ms = int((time.monotonic() - started) * 1000)

    entries = 0
    body_preview = ""
    content_type = None
    final_url = None
    retry_after = None
    if r is not None:
        content_type = r.headers.get("Content-Type")
        retry_after = r.headers.get("Retry-After")
        final_url = r.url
        body_preview = r.content[:300].decode("utf-8", errors="replace")
        try:
            entries = len(feedparser.parse(r.content).entries)
        except Exception:
            entries = 0
        if reason == "ok" and entries == 0:
            reason = "empty"

    return {
        "input_url":               url,
        "canonical_url":           canonical,
        "final_url_requested":     rss_url,
        "final_url_after_redirects": final_url,
        "status_code":             status,
        "reason":                  reason,
        "feed_entries":            entries,
        "body_first_300_chars":    body_preview,
        "content_type":            content_type,
        "retry_after":             retry_after,
        "network_error":           err,
        "proxy_used":              bool(REDDIT_PROXY_URL),
        "user_agent":              REDDIT_USER_AGENT,
        "elapsed_ms":              elapsed_ms,
        "note":                    "Single attempt, rate-limited via the shared limiter (may wait up to "
                                   f"~{REDDIT_MIN_GAP_SECONDS + REDDIT_FETCH_JITTER_MAX:.0f}s). "
                                   "429 or 403 here => Reddit is blocking/throttling this server's IP.",
    }


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    api_thread = threading.Thread(target=run_fastapi, daemon=True, name="FastAPI")
    api_thread.start()
    log.info("FastAPI running at http://0.0.0.0:8000")

    # FIX: startup cleanup of flintel_google_posts (junk non-post URLs +
    # ?tl=/utm duplicate variants) BEFORE the worker threads start.
    cleanup_google_posts()

    # MYSQL SINK: no-op unless MYSQL_DATA is true; never raises.
    init_mysql()

    await asyncio.gather(
        start_reddit_listener(),
    )


if __name__ == "__main__":
    # Optional one-time backfill mode. Running with this flag does NOT
    # start the SERP/Reddit threads or the FastAPI server — it only
    # generates embeddings for existing flintel_signals documents that
    # have text but no embedding yet, then exits. Normal
    # `python index.py` (no flag) starts everything exactly as before.
    # FIX: optional one-time recovery mode — `python index.py --reset-keywords`.
    # Un-burns keywords that were marked fetched=True but have zero
    # flintel_google_posts (earlier SERP failures), logs the count, then exits.
    # Does NOT start the SERP/Reddit threads or the FastAPI server.
    if "--reset-keywords" in sys.argv:
        log.info("=" * 70)
        log.info("  FLINTEL index.py — ONE-TIME KEYWORD RESET (recover keywords burned by SERP failures)")
        log.info("=" * 70)
        n_reset = reset_burned_keywords()
        log.info(f"[KEYWORD-RESET] done — {n_reset} keyword(s) reset to fetched=False.")
        sys.exit(0)

    if "--backfill-embeddings" in sys.argv:
        log.info("=" * 70)
        log.info("  FLINTEL index.py — ONE-TIME EMBEDDING BACKFILL (historical documents only)")
        log.info("=" * 70)
        backfill_missing_embeddings()
        sys.exit(0)

    log.info("=" * 70)
    log.info("  FLINTEL index.py — REDDIT-ONLY SIGNAL PIPELINE")
    log.info("  (Google SERP discovery, batched per GOOGLE_SERP_BATCH keywords/call")
    log.info("   -> flintel_google_posts -> Reddit RSS fetch -> flintel_signals,")
    log.info("   saved directly, tagged with search_keyword, + per-signal embedding)")
    log.info("  Claude / search_volume / engagement / Twitter / queueing: REMOVED")
    log.info("  DUAL MONGODB: flintel_signals on MONGODB_URI | flintel_keywords +")
    log.info("  flintel_google_posts on MONGODB2_URI")
    log.info("=" * 70)
    log.info(f"  Client                : {CLIENT_ID}")
    log.info(f"  Reddit                : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED and bool(RAPIDAPI_KEY))}")
    log.info(f"  Reddit fetch method   : public per-post RSS only — credential-free, no OAuth/PRAW, no .json anywhere")
    # FIX: new startup log lines
    log.info(f"  Reddit URL handling   : canonical post URLs only (…/comments/<id>/<slug>/ + '.rss'); non-post URLs skipped, ?tl=/utm variants deduped")
    log.info(f"  Reddit rate limit     : min gap {REDDIT_MIN_GAP_SECONDS}s + {REDDIT_FETCH_JITTER_MIN}-{REDDIT_FETCH_JITTER_MAX}s jitter, shared by every Reddit request")
    log.info(f"  Circuit breaker       : {REDDIT_CIRCUIT_BREAK_THRESHOLD} consecutive 429/403 -> pause fetch loop {REDDIT_CIRCUIT_BREAK_SECONDS}s")
    log.info(f"  Post retry cooldown   : exponential {REDDIT_POST_RETRY_COOLDOWN_SECONDS}s*2^retry_count, cap {REDDIT_POST_MAX_COOLDOWN_SECONDS}s, pinned after {REDDIT_POST_MAX_FAILED_ATTEMPTS} failures; empty/404 floor {REDDIT_EMPTY_COOLDOWN_SECONDS}s")
    log.info(f"  Reddit proxy          : {'ON (REDDIT_PROXY_URL set)' if REDDIT_PROXY_URL else 'off (set REDDIT_PROXY_URL to route Reddit requests through a proxy)'}")
    log.info(f"  Reddit keywords       : {len(REDDIT_SEARCH_KEYWORDS)} (used ONLY to seed brand-new flintel_keywords docs)")
    log.info(f"  Keyword cache         : flintel_keywords (MONGODB2) — fetch-once-forever")
    log.info(f"  Google SERP           : search_google_for_keywords_batch() — {GOOGLE_SERP_BATCH} keyword(s) OR'd into ONE RapidAPI call (cost control)")
    log.info(f"  flintel_google_posts  : (MONGODB2) stores post_url + google_rank + search_keyword + subreddit + auto fuzzy_keywords + reddit_fetched + retry_count")
    log.info(f"  Reddit fetch interval : check every {REDDIT_FETCH_CHECK_INTERVAL_SECONDS}s")
    log.info(f"  Fuzzy keywords        : Python auto-generated per SERP result at save time — used at SERP time to attribute batched results to a keyword")
    log.info(f"  Signal storage        : direct save into flintel_signals on successful fetch — NO queue, NO batch, NO Claude")
    log.info(f"  Mongo signals save    : {'ENABLED' if _is_mongodb_data_enabled() else 'DISABLED'} (MONGODB_DATA)")
    log.info(f"  MySQL signals save    : {'ENABLED' if _is_mysql_data_enabled() else 'DISABLED'} (MYSQL_DATA) | host={MYSQL_HOST or '-'} db={MYSQL_DATABASE or '-'}")
    log.info(f"  Embeddings            : {'ENABLED — model=' + EMBEDDING_MODEL if _is_embedding_enabled() else 'DISABLED (set OPENAI_API_KEY + EMBEDDING_ENABLED=True to enable)'} (checked live from .env, one embedding per newly saved signal, generated inside save_signal())")
    log.info(f"  RapidAPI config       : {bool(RAPIDAPI_KEY)} (SOLE provider — Google SERP discovery only now, batched {GOOGLE_SERP_BATCH}/call)")
    log.info(f"  MongoDB DB (primary)  : {MONGODB_DB}")
    log.info(f"  MongoDB2 DB (secondary): {MONGODB2_DB}")
    log.info(f"  API auth              : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info(f"  Diagnostics           : GET /reddit-test?url=<reddit post url> (API-key protected)")
    log.info(f"  Embedding backfill    : run with --backfill-embeddings for historical docs missing an embedding")
    log.info("=" * 70)

    asyncio.run(main())
